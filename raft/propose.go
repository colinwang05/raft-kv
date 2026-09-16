package raft

import "log"

// Propose appends cmd to the leader's own log as a new entry (design doc
// section 8: "Leader appends [index, term, cmd] locally"). Returns the
// entry's index and term so the caller (later, M4's KV service) can track
// it until commitIndex reaches it. isLeader is false, and nothing is
// modified, if this node is not currently the leader — the caller must
// redirect the client elsewhere.
func (n *Node) Propose(cmd Command) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, 0, false
	}

	index = n.lastLogIndex() + 1
	term = n.currentTerm
	n.log = append(n.log, LogEntry{Index: index, Term: term, Command: cmd})

	log.Printf("[node=%d term=%d state=%s] append index=%d command=%q", n.id, n.currentTerm, n.state, index, cmd)

	// A lone leader with no peers (majority=1) should commit its own
	// append immediately, rather than wait for the next heartbeat tick to
	// notice via replicateTo.
	n.maybeAdvanceCommitIndexLocked()

	return index, term, true
}
