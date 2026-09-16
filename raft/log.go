package raft

// lastLogIndex returns the index of the last entry in the log, or 0 if
// empty. Caller must hold n.mu.
func (n *Node) lastLogIndex() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// lastLogTerm returns the term of the last entry in the log, or 0 if empty.
// Caller must hold n.mu.
func (n *Node) lastLogTerm() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// isLogUpToDate reports whether a candidate log described by
// (candidateLastIndex, candidateLastTerm) is at least as up-to-date as this
// node's log, per the Raft election-restriction rule (design doc section
// 6): higher last-log term wins outright; on a tie, the longer log wins.
// Caller must hold n.mu.
func (n *Node) isLogUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	ourLastTerm := n.lastLogTerm()
	if candidateLastTerm != ourLastTerm {
		return candidateLastTerm > ourLastTerm
	}
	return candidateLastIndex >= n.lastLogIndex()
}

// findConflict reports whether the entry at prevIndex does not have term
// prevTerm (i.e. AppendEntries' consistency check fails), so the leader
// must back up nextIndex (design doc sections 6 and 9). prevIndex == 0
// always matches (no previous entry to check).
//
// Assumes the log has no gaps and starts at index 1 (n.log[i] has
// Index == i+1); this holds until snapshot compaction is added (M8).
// Caller must hold n.mu.
func (n *Node) findConflict(prevIndex, prevTerm uint64) bool {
	if prevIndex == 0 {
		return false
	}
	if prevIndex > uint64(len(n.log)) {
		return true // we don't have an entry there at all
	}
	return n.log[prevIndex-1].Term != prevTerm
}
