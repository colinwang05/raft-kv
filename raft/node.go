// Package raft implements the Raft consensus algorithm: leader election,
// log replication, and commit/apply of a replicated command log.
package raft

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/colinwang05/raft-kv/internal/config"
)

// State is a node's role in the Raft protocol.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// LogEntry is one entry in the replicated Raft log.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command Command
}

// Node holds all Raft state for one server (design doc section 5).
type Node struct {
	mu sync.Mutex

	id          int
	state       State
	currentTerm uint64
	votedFor    int // 0 means "no vote this term"; valid node IDs start at 1.
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64
	leaderID    int // 0 means "no leader observed yet"; last leader seen via becomeLeader or AppendEntries.
	peers       map[int]RaftClient

	// applier receives committed commands from runApplyLoop. May be nil
	// (e.g. in unit tests that don't care about the KV state machine).
	applier Applier

	// persister durably records currentTerm/votedFor/log. May be nil (e.g.
	// in unit tests that don't care about crash recovery).
	persister Persister

	// Leader-only volatile state.
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	cfg *config.Config

	// resetElectionC signals the election loop to restart its timeout,
	// e.g. after granting a vote or (from M2) receiving a valid heartbeat.
	resetElectionC chan struct{}
	cancel         context.CancelFunc
}

// NewNode constructs a Node from configuration. It does not start any
// background loops or persistence recovery; call Start for that. Callers
// that need crash recovery should call SetPersister and RestoreState
// (loading state from a WAL themselves) before Start — see cmd/server/main.go.
func NewNode(cfg *config.Config) *Node {
	return &Node{
		id:             cfg.ID,
		state:          Follower,
		peers:          make(map[int]RaftClient),
		nextIndex:      make(map[int]uint64),
		matchIndex:     make(map[int]uint64),
		cfg:            cfg,
		resetElectionC: make(chan struct{}, 1),
	}
}

// Start connects to peers, logs the node's initial state, and launches the
// election loop. Heartbeat/apply loops are not started yet (design doc
// section 12) — those land with heartbeats (M2) and replication (M3).
func (n *Node) Start() error {
	peers, err := dialPeers(n.cfg)
	if err != nil {
		return fmt.Errorf("connect to peers: %w", err)
	}

	n.mu.Lock()
	n.peers = peers
	state, term := n.state, n.currentTerm
	n.mu.Unlock()

	log.Printf("[node=%d term=%d state=%s] started; peers=%v", n.id, term, state, n.cfg.Peers)

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	go n.runElectionLoop(ctx)
	go n.runHeartbeatLoop(ctx)
	go n.runApplyLoop(ctx)
	return nil
}

// SetApplier wires the state machine that runApplyLoop delivers committed
// commands to. Call once before Start().
func (n *Node) SetApplier(a Applier) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applier = a
}

// SetPersister wires the durable store that Node persists currentTerm/
// votedFor/log to. Call once before Start().
func (n *Node) SetPersister(p Persister) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.persister = p
}

// RestoreState installs Raft state loaded from a WAL by the caller (e.g.
// cmd/server/main.go) before this node starts participating in any Raft
// traffic (design doc section 11). Call once before Start(), after
// SetPersister.
func (n *Node) RestoreState(term uint64, votedFor int, log []LogEntry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = term
	n.votedFor = votedFor
	n.log = log
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// LeaderID returns the most recently observed leader's ID, or 0 if none
// has been observed yet.
func (n *Node) LeaderID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// resetElectionTimer signals the election loop to restart its randomized
// timeout without blocking. Safe to call while holding n.mu.
func (n *Node) resetElectionTimer() {
	select {
	case n.resetElectionC <- struct{}{}:
	default:
	}
}

// becomeFollowerLocked steps down to Follower, updating currentTerm and
// clearing votedFor if newTerm is newer (design doc section 21: a server
// observing a higher term immediately steps down). Caller must hold n.mu.
func (n *Node) becomeFollowerLocked(newTerm uint64) {
	if newTerm > n.currentTerm {
		n.currentTerm = newTerm
		n.votedFor = 0
		if n.persister != nil {
			// Best-effort: no external RPC response is riding on this
			// specific call succeeding, and threading a rollback through
			// becomeFollowerLocked's several call sites isn't worth it.
			if err := n.persister.SaveState(newTerm, 0); err != nil {
				log.Printf("[node=%d term=%d state=%s] persist state failed: %v", n.id, newTerm, n.state, err)
			}
		}
	}
	n.state = Follower
}
