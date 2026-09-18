package raft

// Persister durably records the Raft state that must survive a restart
// (design doc §11: currentTerm, votedFor, and the log). storage.WAL
// implements this — raft can't import storage (storage already imports
// raft for Command/LogEntry), so this interface is how Node stays
// decoupled from it, same as Applier for the KV state machine.
type Persister interface {
	SaveState(term uint64, votedFor int) error
	PersistLog(entries []LogEntry) error
}
