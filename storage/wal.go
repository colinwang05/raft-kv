// Package storage implements V1 persistence: a metadata file for
// currentTerm/votedFor, an append-only log file for Raft log entries, and
// the in-memory KV state machine (design doc section 11).
package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/colinwang05/raft-kv/raft"
)

// Metadata is the small, frequently-updated persistent Raft state.
type Metadata struct {
	CurrentTerm uint64
	VotedFor    int
}

// WAL is the write-ahead log for one node's data directory:
//
//	<dataDir>/metadata.json
//	<dataDir>/raft.log
//
// No internal locking: every call site in raft.Node already runs under
// Node.mu (calls are already serialized per-node), so an additional mutex
// here wouldn't earn its keep.
type WAL struct {
	dataDir string
}

// NewWAL returns a WAL rooted at dataDir. It does not read or create files.
func NewWAL(dataDir string) *WAL {
	return &WAL{dataDir: dataDir}
}

func (w *WAL) metadataPath() string {
	return filepath.Join(w.dataDir, "metadata.json")
}

func (w *WAL) logPath() string {
	return filepath.Join(w.dataDir, "raft.log")
}

// LoadMetadata reads currentTerm/votedFor from disk, or the zero value if
// no metadata file exists yet (first boot). A real read/decode error is
// returned rather than swallowed.
func (w *WAL) LoadMetadata() (Metadata, error) {
	data, err := os.ReadFile(w.metadataPath())
	if err != nil {
		if os.IsNotExist(err) {
			return Metadata{}, nil
		}
		return Metadata{}, fmt.Errorf("read metadata: %w", err)
	}
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return Metadata{}, fmt.Errorf("decode metadata: %w", err)
	}
	return meta, nil
}

// SaveState durably persists currentTerm and votedFor together, in a
// single atomic write.
//
// term and votedFor are combined into one call (rather than separate
// SaveTerm/SaveVote methods) because votedFor is only ever meaningful in
// the context of the currentTerm it was cast in, and nothing in the
// on-disk format ties a saved vote to its term. Two separate writes would
// let a crash between them leave a stale, mismatched pair on disk (e.g. a
// new term persisted but the old term's vote still there) — and reloading
// that pair on restart would make the node believe it already voted in
// the new term when it did not, needlessly refusing to vote for anyone
// else that term (a liveness bug). Writing both fields atomically together
// makes that impossible.
//
// Must be called before sending any RequestVote for a newer term, and
// before returning vote_granted=true (design doc section 11).
func (w *WAL) SaveState(term uint64, votedFor int) error {
	data, err := json.Marshal(Metadata{CurrentTerm: term, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	return atomicWriteFile(w.dataDir, w.metadataPath(), data)
}

// PersistLog durably replaces the entire on-disk log with entries, via a
// temp-file-plus-rename (atomic on POSIX): LoadLog will only ever observe
// either the fully-old file or the fully-new one, never a torn write.
//
// This fully rewrites the file on every call rather than appending
// incrementally. A true append-only format can't cleanly express "discard
// the conflicting suffix after index N and append these instead" without
// tracking per-index byte offsets — real complexity that belongs with
// M8's log-compaction work, not V1. Rewriting the whole log on every call
// is less storage/IO-efficient (an accepted V1 tradeoff), but it's simple
// and correct, and callers (raft.Node) already avoid calling this when the
// log hasn't actually changed (e.g. steady-state heartbeats).
//
// Must be called before an AppendEntries RPC returns success (design doc
// section 11).
func (w *WAL) PersistLog(entries []raft.LogEntry) error {
	var buf bytes.Buffer
	for _, e := range entries {
		var entryBuf bytes.Buffer
		if err := gob.NewEncoder(&entryBuf).Encode(e); err != nil {
			return fmt.Errorf("encode log entry: %w", err)
		}
		var lenPrefix [4]byte
		binary.BigEndian.PutUint32(lenPrefix[:], uint32(entryBuf.Len()))
		buf.Write(lenPrefix[:])
		buf.Write(entryBuf.Bytes())
	}
	return atomicWriteFile(w.dataDir, w.logPath(), buf.Bytes())
}

// LoadLog reads the full persisted Raft log from disk, in index order, or
// nil if no log file exists yet (first boot). A real read/decode error is
// returned rather than swallowed. Because PersistLog always atomically
// replaces the whole file, a crash mid-write can never leave a truncated
// trailing record here — no such handling is needed.
func (w *WAL) LoadLog() ([]raft.LogEntry, error) {
	data, err := os.ReadFile(w.logPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log: %w", err)
	}

	var entries []raft.LogEntry
	r := bytes.NewReader(data)
	for {
		var lenPrefix [4]byte
		_, err := io.ReadFull(r, lenPrefix[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read log entry length: %w", err)
		}
		n := binary.BigEndian.Uint32(lenPrefix[:])
		entryBytes := make([]byte, n)
		if _, err := io.ReadFull(r, entryBytes); err != nil {
			return nil, fmt.Errorf("read log entry: %w", err)
		}
		var entry raft.LogEntry
		if err := gob.NewDecoder(bytes.NewReader(entryBytes)).Decode(&entry); err != nil {
			return nil, fmt.Errorf("decode log entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// atomicWriteFile durably writes data to path: write to a temp file in
// dir, fsync it, close it, then os.Rename over path (atomic on POSIX).
// Used by both SaveState and PersistLog.
func atomicWriteFile(dir, path string, data []byte) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename below succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}
