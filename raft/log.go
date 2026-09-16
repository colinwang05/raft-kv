package raft

// lastLogIndex returns the index of the last entry in the log, or 0 if empty.
func (n *Node) lastLogIndex() uint64 {
	// TODO: implement.
	return 0
}

// lastLogTerm returns the term of the last entry in the log, or 0 if empty.
func (n *Node) lastLogTerm() uint64 {
	// TODO: implement.
	return 0
}

// isLogUpToDate reports whether a candidate log described by
// (candidateLastIndex, candidateLastTerm) is at least as up-to-date as this
// node's log, per the Raft election-restriction rule (design doc section 6).
func (n *Node) isLogUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	// TODO: implement.
	return false
}

// findConflict reports whether the entry at prevIndex does not have term
// prevTerm (i.e. AppendEntries' consistency check fails), so the leader
// must back up nextIndex (design doc sections 6 and 9).
func (n *Node) findConflict(prevIndex, prevTerm uint64) bool {
	// TODO: implement.
	return false
}
