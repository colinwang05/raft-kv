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
	peers       map[int]RaftClient

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
// background loops or persistence recovery; call Start for that.
func NewNode(cfg *config.Config) *Node {
	// TODO: load persistent state (currentTerm, votedFor, log) from the WAL
	// before this node participates in any Raft traffic (design doc section 11).
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
	// TODO: go n.runApplyLoop(ctx)
	return nil
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
		// TODO(M5): persist currentTerm before sending/responding further.
	}
	n.state = Follower
}
