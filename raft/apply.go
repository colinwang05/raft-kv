package raft

import "context"

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
	// TODO: implement.
}
