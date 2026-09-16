package raft

import (
	"context"
	"testing"
	"time"
)

// fakeApplier records the Commands it receives, in the order Apply was
// called, for assertions about ordering. Safe for the pattern used in
// these tests: the test goroutine only reads commands after synchronizing
// with the applying goroutine via n.mu (see the lastApplied poll below),
// which establishes the necessary happens-before edge.
type fakeApplier struct {
	commands []Command
}

func newFakeApplier() *fakeApplier {
	return &fakeApplier{}
}

func (f *fakeApplier) Apply(cmd Command) {
	f.commands = append(f.commands, cmd)
}

func TestRunApplyLoop_AppliesInOrderAndAdvancesLastApplied(t *testing.T) {
	n := newTestNode(1)
	applier := newFakeApplier()
	n.applier = applier

	n.log = []LogEntry{
		{Index: 1, Term: 1, Command: Command{Op: PUT, Key: "a", Value: "1"}},
		{Index: 2, Term: 1, Command: Command{Op: PUT, Key: "b", Value: "2"}},
		{Index: 3, Term: 1, Command: Command{Op: DELETE, Key: "a"}},
	}
	n.mu.Lock()
	n.commitIndex = 3
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runApplyLoop(ctx)

	deadline := time.After(1 * time.Second)
	for {
		n.mu.Lock()
		applied := n.lastApplied
		n.mu.Unlock()
		if applied == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("lastApplied = %d, want 3 within deadline", applied)
		case <-time.After(2 * time.Millisecond):
		}
	}

	if len(applier.commands) != 3 {
		t.Fatalf("applier got %d commands, want 3", len(applier.commands))
	}
	wantKeys := []string{"a", "b", "a"}
	for i, cmd := range applier.commands {
		if cmd.Key != wantKeys[i] {
			t.Errorf("commands[%d].Key = %q, want %q (applied out of order)", i, cmd.Key, wantKeys[i])
		}
	}
	if applier.commands[0].Op != PUT || applier.commands[2].Op != DELETE {
		t.Errorf("commands = %+v, want PUT,PUT,DELETE in that order", applier.commands)
	}
}

func TestRunApplyLoop_NilApplierStillAdvancesLastApplied(t *testing.T) {
	n := newTestNode(1)
	// n.applier left nil deliberately.

	n.log = []LogEntry{
		{Index: 1, Term: 1, Command: Command{Op: PUT, Key: "a", Value: "1"}},
	}
	n.mu.Lock()
	n.commitIndex = 1
	n.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runApplyLoop(ctx)

	if !n.WaitApplied(context.Background(), 1) {
		t.Fatal("WaitApplied(1) = false, want true: lastApplied should advance even with a nil applier")
	}
}

func TestWaitApplied_ReturnsTruePromptlyOnceApplied(t *testing.T) {
	n := newTestNode(1)
	n.mu.Lock()
	n.lastApplied = 5
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if !n.WaitApplied(ctx, 5) {
		t.Error("WaitApplied(5) = false, want true when lastApplied is already 5")
	}
}

func TestWaitApplied_ReturnsFalseWhenDeadlineExpiresFirst(t *testing.T) {
	n := newTestNode(1)
	// lastApplied stays 0 forever: nothing ever advances it.

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if n.WaitApplied(ctx, 1) {
		t.Error("WaitApplied(1) = true, want false: index is never applied")
	}
}
