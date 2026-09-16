// Package storage implements V1 persistence: a metadata file for
// currentTerm/votedFor, an append-only log file for Raft log entries, and
// the in-memory KV state machine (design doc section 11).
package storage

import "github.com/colinwang05/raft-kv/raft"

// Metadata is the small, frequently-updated persistent Raft state.
type Metadata struct {
	CurrentTerm uint64
	VotedFor    int
}

// WAL is the write-ahead log for one node's data directory:
//
//	<dataDir>/metadata.json
//	<dataDir>/raft.log
type WAL struct {
	dataDir string
}

// NewWAL returns a WAL rooted at dataDir. It does not read or create files.
func NewWAL(dataDir string) *WAL {
	return &WAL{dataDir: dataDir}
}

// LoadMetadata reads currentTerm/votedFor from disk, or zero values if no
// metadata file exists yet (first boot).
func (w *WAL) LoadMetadata() (Metadata, error) {
	// TODO: implement.
	return Metadata{}, nil
}

// SaveTerm durably persists currentTerm. Must be called before sending any
// message for a newer term (design doc section 11).
func (w *WAL) SaveTerm(term uint64) error {
	// TODO: implement.
	return nil
}

// SaveVote durably persists votedFor for the current term. Must be called
// before returning vote_granted=true (design doc section 11).
func (w *WAL) SaveVote(candidateID int) error {
	// TODO: implement.
	return nil
}

// AppendLogEntries durably appends entries to raft.log. Must be called
// before an AppendEntries RPC returns success (design doc section 11).
func (w *WAL) AppendLogEntries(entries []raft.LogEntry) error {
	// TODO: implement.
	return nil
}

// LoadLog reads the full persisted Raft log from disk, in index order.
func (w *WAL) LoadLog() ([]raft.LogEntry, error) {
	// TODO: implement.
	return nil, nil
}
