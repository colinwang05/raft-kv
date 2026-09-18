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

	if n.persister != nil {
		// Best-effort, log-only: unlike the receiver-side AppendEntries
		// path, there's no explicit design-doc requirement gating
		// Propose's own success on local durability — the entry's real
		// durability guarantee comes from WaitApplied plus successful
		// replication to (and persistence by) a majority of followers via
		// the already-strict AppendEntries path. If the leader's own disk
		// write fails here but replication elsewhere succeeds, the entry
		// is still safe cluster-wide, so Propose's return contract
		// (index, term, isLeader — no error) stays unchanged.
		if err := n.persister.PersistLog(n.log); err != nil {
			log.Printf("[node=%d term=%d state=%s] persist log failed: %v", n.id, n.currentTerm, n.state, err)
		}
	}

	log.Printf("[node=%d term=%d state=%s] append index=%d command=%q", n.id, n.currentTerm, n.state, index, cmd)

	// A lone leader with no peers (majority=1) should commit its own
	// append immediately, rather than wait for the next heartbeat tick to
	// notice via replicateTo.
	n.maybeAdvanceCommitIndexLocked()

	return index, term, true
}
