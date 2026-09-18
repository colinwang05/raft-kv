// Package raft_test (external test package, not `raft`) is required here
// because this test needs to import both raft and storage, and storage
// already imports raft — only raft_test avoids that import cycle. It uses
// only raft's exported API plus storage.NewWAL/storage.NewKVStore, exactly
// as cmd/server/main.go does.
package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/colinwang05/raft-kv/internal/config"
	"github.com/colinwang05/raft-kv/raft"
	"github.com/colinwang05/raft-kv/storage"
)

func waitUntilLeader(t *testing.T, n *raft.Node) {
	t.Helper()
	deadline := time.After(1 * time.Second)
	for !n.IsLeader() {
		select {
		case <-deadline:
			t.Fatal("node never became leader within deadline")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func waitApplied(t *testing.T, n *raft.Node, index uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if !n.WaitApplied(ctx, index) {
		t.Fatalf("index %d was not applied within deadline", index)
	}
}

func testConfig(id int, dataDir string) *config.Config {
	return &config.Config{
		ID:                 id,
		DataDir:            dataDir,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
	}
}

// TestRestart_RecoversPersistedLogAndAppliesItAlongsideNewEntries builds a
// real single-node (zero peers) Node backed by a real WAL+KVStore, proposes
// one command, then simulates a process restart (new Node, new WAL over the
// same directory, new empty KVStore), and confirms that after the restarted
// node re-elects itself and a *new* command is proposed and applied, the
// KVStore ends up containing both the pre-restart and post-restart data.
//
// It deliberately does NOT assert that the old entry auto-commits right
// after restart, before any new proposal: it structurally can't. The
// restored entry is from the old term; the restarted node re-elects itself
// into a new, higher term; and maybeAdvanceCommitIndexLocked (design doc's
// Raft §5.4.2 / Figure-8 rule) correctly refuses to directly commit an
// older-term entry without a current-term entry also reaching majority
// first. With zero new proposals, that never happens — by design, not a
// bug. Proposing a second, new command is what correctly and implicitly
// commits everything before it too.
func TestRestart_RecoversPersistedLogAndAppliesItAlongsideNewEntries(t *testing.T) {
	dataDir := t.TempDir()

	// --- First "process": propose one command, then stop caring about it. ---
	wal1 := storage.NewWAL(dataDir)
	store1 := storage.NewKVStore()
	node1 := raft.NewNode(testConfig(1, dataDir))
	node1.SetPersister(wal1)
	node1.SetApplier(store1)
	if err := node1.Start(); err != nil {
		t.Fatalf("node1.Start() = %v", err)
	}
	waitUntilLeader(t, node1)

	index1, _, isLeader := node1.Propose(raft.Command{Op: raft.PUT, Key: "before", Value: "restart"})
	if !isLeader {
		t.Fatal("node1.Propose: isLeader = false, want true")
	}
	waitApplied(t, node1, index1)

	if v, ok := store1.Get("before"); !ok || v != "restart" {
		t.Fatalf("store1.Get(before) = (%q, %v), want (\"restart\", true) before restart", v, ok)
	}

	// --- Simulate a restart: new Node, new WAL over the same dir, new
	// empty KVStore, loading persisted state exactly as cmd/server/main.go
	// does. ---
	wal2 := storage.NewWAL(dataDir)
	meta, err := wal2.LoadMetadata()
	if err != nil {
		t.Fatalf("LoadMetadata: %v", err)
	}
	restoredLog, err := wal2.LoadLog()
	if err != nil {
		t.Fatalf("LoadLog: %v", err)
	}
	if len(restoredLog) != 1 {
		t.Fatalf("restoredLog = %v, want 1 entry persisted from before the restart", restoredLog)
	}

	store2 := storage.NewKVStore()
	node2 := raft.NewNode(testConfig(1, dataDir))
	node2.RestoreState(meta.CurrentTerm, meta.VotedFor, restoredLog)
	node2.SetPersister(wal2)
	node2.SetApplier(store2)
	if err := node2.Start(); err != nil {
		t.Fatalf("node2.Start() = %v", err)
	}
	waitUntilLeader(t, node2)

	// Propose a *new*, current-term command. This both applies itself and
	// (per the Figure-8 rule above) implicitly commits the restored
	// pre-restart entry too, since it's now ahead of it in the same log.
	index2, _, isLeader := node2.Propose(raft.Command{Op: raft.PUT, Key: "after", Value: "restart"})
	if !isLeader {
		t.Fatal("node2.Propose: isLeader = false, want true")
	}
	waitApplied(t, node2, index2)

	if v, ok := store2.Get("before"); !ok || v != "restart" {
		t.Errorf("store2.Get(before) = (%q, %v), want (\"restart\", true): the pre-restart entry must survive and get applied", v, ok)
	}
	if v, ok := store2.Get("after"); !ok || v != "restart" {
		t.Errorf("store2.Get(after) = (%q, %v), want (\"restart\", true): the post-restart entry must apply too", v, ok)
	}
}
