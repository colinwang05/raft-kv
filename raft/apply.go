package raft

import (
	"context"
	"time"
)

// applyTickInterval is how often runApplyLoop checks whether commitIndex
// has moved past lastApplied. It isn't leader-only and isn't tied to
// cfg.HeartbeatInterval — it just needs to notice new commits promptly.
const applyTickInterval = 10 * time.Millisecond

// waitAppliedPollInterval is how often WaitApplied re-checks lastApplied.
const waitAppliedPollInterval = 5 * time.Millisecond

// Applier applies one committed command to a state machine. storage.KVStore
// implements this (raft can't import storage — storage already imports
// raft — so this interface is how Node stays decoupled from it).
type Applier interface {
	Apply(cmd Command)
}

// Operation is a KV state-machine command type (design doc section 10).
type Operation int

const (
	PUT Operation = iota
	DELETE
)

// Command is a single replicated state-machine operation. ClientID and
// RequestID are carried through the log so retried client requests can
// later be deduplicated at the logical API level.
type Command struct {
	Op        Operation
	Key       string
	Value     string
	ClientID  string
	RequestID uint64
}

// runApplyLoop applies committed log entries (index > lastApplied, up to
// commitIndex) to the KV state machine, strictly in increasing log-index
// order (design doc sections 10 and 21). Exits when ctx is cancelled.
func (n *Node) runApplyLoop(ctx context.Context) {
	ticker := time.NewTicker(applyTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.applyPending()
		}
	}
}

// applyPending applies every log entry in (lastApplied, commitIndex],
// strictly in increasing index order, then advances lastApplied to match.
// Locks n.mu internally (briefly, and not for the duration of applying).
func (n *Node) applyPending() {
	n.mu.Lock()
	if n.commitIndex <= n.lastApplied {
		n.mu.Unlock()
		return
	}
	// n.log is 0-indexed by (Index-1); entries (lastApplied, commitIndex]
	// are a contiguous slice.
	pending := make([]LogEntry, n.commitIndex-n.lastApplied)
	copy(pending, n.log[n.lastApplied:n.commitIndex])
	applier := n.applier
	n.mu.Unlock()

	for _, e := range pending {
		if applier != nil {
			applier.Apply(e.Command)
		}
		n.mu.Lock()
		n.lastApplied = e.Index
		n.mu.Unlock()
	}
}

// WaitApplied blocks (simple polling, design doc's "start simple") until
// lastApplied >= index, or ctx is done — returning whether it was applied
// in time. The caller (kvServer) passes through the incoming RPC's ctx, so
// a client-side deadline naturally bounds this.
func (n *Node) WaitApplied(ctx context.Context, index uint64) bool {
	n.mu.Lock()
	applied := n.lastApplied >= index
	n.mu.Unlock()
	if applied {
		return true
	}

	ticker := time.NewTicker(waitAppliedPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			n.mu.Lock()
			applied := n.lastApplied >= index
			n.mu.Unlock()
			if applied {
				return true
			}
		}
	}
}
