// Package raft implements the Raft consensus algorithm: leader election,
// log replication, and commit/apply of a replicated command log.
package raft

import (
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
	votedFor    int
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64
	peers       map[int]RaftClient

	// Leader-only volatile state.
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	cfg *config.Config
}

// NewNode constructs a Node from configuration. It does not start any
// background loops or persistence recovery; call Start for that.
func NewNode(cfg *config.Config) *Node {
	// TODO: load persistent state (currentTerm, votedFor, log) from the WAL
	// before this node participates in any Raft traffic (design doc section 11).
	return &Node{
		id:         cfg.ID,
		state:      Follower,
		peers:      make(map[int]RaftClient),
		nextIndex:  make(map[int]uint64),
		matchIndex: make(map[int]uint64),
		cfg:        cfg,
	}
}

// Start launches the node's long-running goroutines: election timing,
// heartbeats/replication, and application of committed entries (design doc
// section 12). It does not block.
func (n *Node) Start() {
	// TODO: go n.runElectionLoop(ctx)
	// TODO: go n.runHeartbeatLoop(ctx)
	// TODO: go n.runApplyLoop(ctx)
}
