package storage

import (
	"testing"

	"github.com/colinwang05/raft-kv/raft"
)

func TestLoadMetadata_EmptyDirReturnsZeroValue(t *testing.T) {
	w := NewWAL(t.TempDir())

	meta, err := w.LoadMetadata()
	if err != nil {
		t.Fatalf("LoadMetadata: unexpected error: %v", err)
	}
	if meta != (Metadata{}) {
		t.Errorf("LoadMetadata on empty dir = %+v, want zero value", meta)
	}
}

func TestLoadLog_EmptyDirReturnsNil(t *testing.T) {
	w := NewWAL(t.TempDir())

	log, err := w.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: unexpected error: %v", err)
	}
	if log != nil {
		t.Errorf("LoadLog on empty dir = %v, want nil", log)
	}
}

func TestSaveState_RoundTripsAcrossFreshWALInstance(t *testing.T) {
	dir := t.TempDir()

	w1 := NewWAL(dir)
	if err := w1.SaveState(7, 3); err != nil {
		t.Fatalf("SaveState: unexpected error: %v", err)
	}

	// Simulate a restart: a brand new WAL instance pointed at the same dir.
	w2 := NewWAL(dir)
	meta, err := w2.LoadMetadata()
	if err != nil {
		t.Fatalf("LoadMetadata: unexpected error: %v", err)
	}
	if meta.CurrentTerm != 7 || meta.VotedFor != 3 {
		t.Errorf("LoadMetadata = %+v, want {CurrentTerm:7 VotedFor:3}", meta)
	}
}

func TestSaveState_SecondCallOverwritesFirst(t *testing.T) {
	dir := t.TempDir()

	w := NewWAL(dir)
	if err := w.SaveState(1, 1); err != nil {
		t.Fatalf("SaveState: unexpected error: %v", err)
	}
	if err := w.SaveState(2, 0); err != nil {
		t.Fatalf("SaveState: unexpected error: %v", err)
	}

	w2 := NewWAL(dir)
	meta, err := w2.LoadMetadata()
	if err != nil {
		t.Fatalf("LoadMetadata: unexpected error: %v", err)
	}
	if meta.CurrentTerm != 2 || meta.VotedFor != 0 {
		t.Errorf("LoadMetadata = %+v, want {CurrentTerm:2 VotedFor:0} (the second, latest write)", meta)
	}
}

func TestPersistLog_RoundTripsAcrossFreshWALInstance(t *testing.T) {
	dir := t.TempDir()

	entries := []raft.LogEntry{
		{Index: 1, Term: 1, Command: raft.Command{Op: raft.PUT, Key: "x", Value: "10", ClientID: "c1", RequestID: 1}},
		{Index: 2, Term: 1, Command: raft.Command{Op: raft.DELETE, Key: "y", ClientID: "c2", RequestID: 2}},
		{Index: 3, Term: 2, Command: raft.Command{Op: raft.PUT, Key: "z", Value: "some longer value with spaces"}},
	}

	w1 := NewWAL(dir)
	if err := w1.PersistLog(entries); err != nil {
		t.Fatalf("PersistLog: unexpected error: %v", err)
	}

	w2 := NewWAL(dir)
	got, err := w2.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: unexpected error: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("LoadLog returned %d entries, want %d", len(got), len(entries))
	}
	for i, want := range entries {
		if got[i] != want {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want)
		}
	}
}

func TestPersistLog_SecondCallWithFewerEntriesFullyReplaces(t *testing.T) {
	// Simulates a conflicting-suffix truncation: the log shrinks between
	// calls. A fresh WAL's LoadLog must reflect only the second call's
	// entries, never a stale mix of both (PersistLog fully replaces the
	// log on every call, it doesn't append incrementally).
	dir := t.TempDir()

	w := NewWAL(dir)
	first := []raft.LogEntry{
		{Index: 1, Term: 1, Command: raft.Command{Op: raft.PUT, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Command: raft.Command{Op: raft.PUT, Key: "b", Value: "2"}},
		{Index: 3, Term: 1, Command: raft.Command{Op: raft.PUT, Key: "c", Value: "3"}},
	}
	if err := w.PersistLog(first); err != nil {
		t.Fatalf("PersistLog(first): unexpected error: %v", err)
	}

	second := []raft.LogEntry{
		{Index: 1, Term: 1, Command: raft.Command{Op: raft.PUT, Key: "a", Value: "1"}},
	}
	if err := w.PersistLog(second); err != nil {
		t.Fatalf("PersistLog(second): unexpected error: %v", err)
	}

	w2 := NewWAL(dir)
	got, err := w2.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("LoadLog returned %d entries, want 1 (only the second call's entries)", len(got))
	}
	if got[0] != second[0] {
		t.Errorf("entry = %+v, want %+v", got[0], second[0])
	}
}
