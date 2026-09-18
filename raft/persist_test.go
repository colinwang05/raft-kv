package raft

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	pb "github.com/colinwang05/raft-kv/proto"
)

// fakePersister is a Persister test double: it records every SaveState/
// PersistLog call, and can be made to fail either method on demand.
type fakePersister struct {
	mu sync.Mutex

	saveStateCalls  []savedState
	persistLogCalls [][]LogEntry

	failSaveState  bool
	failPersistLog bool
}

type savedState struct {
	Term     uint64
	VotedFor int
}

func (f *fakePersister) SaveState(term uint64, votedFor int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveStateCalls = append(f.saveStateCalls, savedState{Term: term, VotedFor: votedFor})
	if f.failSaveState {
		return errors.New("fake SaveState failure")
	}
	return nil
}

func (f *fakePersister) PersistLog(entries []LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]LogEntry(nil), entries...)
	f.persistLogCalls = append(f.persistLogCalls, cp)
	if f.failPersistLog {
		return errors.New("fake PersistLog failure")
	}
	return nil
}

func (f *fakePersister) saveStateCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.saveStateCalls)
}

func (f *fakePersister) persistLogCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.persistLogCalls)
}

func (f *fakePersister) lastPersistedLog() []LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.persistLogCalls) == 0 {
		return nil
	}
	return f.persistLogCalls[len(f.persistLogCalls)-1]
}

// --- RequestVote: persist-before-grant, rollback on failure ---

func TestRequestVote_PersistsStateBeforeGranting(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1 // same term as the request, so becomeFollowerLocked's own
	// (separate) persist path isn't also triggered here — isolates the
	// vote-granting persist call this test is about.
	fp := &fakePersister{}
	n.persister = fp

	resp, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{Term: 1, CandidateId: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatal("VoteGranted = false, want true")
	}
	if fp.saveStateCallCount() != 1 {
		t.Fatalf("SaveState called %d times, want 1", fp.saveStateCallCount())
	}
	got := fp.saveStateCalls[0]
	if got.Term != 1 || got.VotedFor != 2 {
		t.Errorf("SaveState called with %+v, want {Term:1 VotedFor:2}", got)
	}
}

func TestRequestVote_RollsBackVoteAndDeniesOnPersistFailure(t *testing.T) {
	n := newTestNode(1)
	fp := &fakePersister{failSaveState: true}
	n.persister = fp

	resp, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{Term: 1, CandidateId: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VoteGranted {
		t.Error("VoteGranted = true, want false when persistence fails")
	}
	if n.votedFor != 0 {
		t.Errorf("votedFor = %d, want 0 (rolled back after failed persist)", n.votedFor)
	}

	// No permanent lockout: a later RequestVote for a *different* candidate,
	// with persistence now succeeding, must still be granted.
	fp.failSaveState = false
	resp2, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{Term: 1, CandidateId: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp2.VoteGranted {
		t.Error("VoteGranted = false, want true: a rolled-back failed vote must not permanently lock out other candidates")
	}
	if n.votedFor != 3 {
		t.Errorf("votedFor = %d, want 3", n.votedFor)
	}
}

// --- AppendEntries: persist-on-change only, rollback on failure ---

func TestAppendEntries_PersistsLogOnlyWhenChanged(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1
	fp := &fakePersister{}
	n.persister = fp

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*pb.LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}},
	})
	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if fp.persistLogCallCount() != 1 {
		t.Fatalf("PersistLog called %d times, want 1 after appending new entries", fp.persistLogCallCount())
	}
	if got := fp.lastPersistedLog(); len(got) != 2 {
		t.Fatalf("PersistLog called with %d entries, want 2 (the full post-merge log)", len(got))
	}

	// A plain heartbeat matching prevLogIndex, with no entries, must not
	// trigger another full rewrite+fsync.
	resp2 := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1,
	})
	if !resp2.Success {
		t.Fatal("Success = false, want true for a matching heartbeat")
	}
	if fp.persistLogCallCount() != 1 {
		t.Errorf("PersistLog called %d times, want still 1 (heartbeat must not persist)", fp.persistLogCallCount())
	}
}

func TestAppendEntries_ConflictOnlyTruncationPersistsEvenWithNoNewEntries(t *testing.T) {
	// A probing heartbeat against a follower with a divergent, uncommitted
	// tail: zero new entries carried, but the conflicting suffix is still
	// discarded, so this must persist.
	n := newTestNode(1)
	n.currentTerm = 3
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	fp := &fakePersister{}
	n.persister = fp

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 3, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
	})
	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if fp.persistLogCallCount() != 1 {
		t.Errorf("PersistLog called %d times, want 1 (truncation must persist even with no new entries)", fp.persistLogCallCount())
	}
	if len(n.log) != 1 {
		t.Errorf("log = %v, want 1 entry (conflicting suffix truncated)", n.log)
	}
}

func TestAppendEntries_RollsBackLogAndReturnsFailureOnPersistFailure(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1
	n.log = []LogEntry{{Index: 1, Term: 1}}
	fp := &fakePersister{failPersistLog: true}
	n.persister = fp

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*pb.LogEntry{{Index: 2, Term: 1}},
	})
	if resp.Success {
		t.Error("Success = true, want false when persisting the log fails")
	}
	if len(n.log) != 1 {
		t.Errorf("log = %v, want rolled back to the original 1 entry", n.log)
	}
}

// TestAppendEntries_RollbackSurvivesBackingArrayReuse guards against a
// specific Go-slices footgun: capturing "oldLog := n.log" before mutating
// n.log is NOT a safe rollback snapshot, because `append(n.log[:prevIndex],
// newEntries...)` reuses n.log's backing array in place whenever it has
// spare capacity — silently overwriting the very elements oldLog still
// points at. This only manifests when there's spare capacity to reuse, so
// it builds the log via repeated appends (which over-allocate capacity)
// rather than a single slice literal (which wouldn't have any spare
// capacity, and would mask the bug by forcing every append to reallocate).
func TestAppendEntries_RollbackSurvivesBackingArrayReuse(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1
	for i := uint64(1); i <= 5; i++ {
		n.log = append(n.log, LogEntry{Index: i, Term: 1, Command: Command{Key: fmt.Sprintf("k%d", i)}})
	}
	if cap(n.log) == len(n.log) {
		t.Fatalf("test setup invariant violated: need spare capacity (cap=%d, len=%d) to exercise the aliasing bug", cap(n.log), len(n.log))
	}
	original := append([]LogEntry(nil), n.log...)

	fp := &fakePersister{failPersistLog: true}
	n.persister = fp

	// Truncate the last 3 entries and replace with 2 new ones — a
	// conflicting-suffix case with room for append to write in place.
	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []*pb.LogEntry{{Index: 3, Term: 1}, {Index: 4, Term: 1}},
	})
	if resp.Success {
		t.Fatal("Success = true, want false when persisting the log fails")
	}
	if !reflect.DeepEqual(n.log, original) {
		t.Errorf("log after failed-persist rollback = %+v, want exactly the original %+v (backing-array corruption)", n.log, original)
	}
}

// --- startElection: bumps term/vote even on persist failure, sends no RPCs ---

// countingClient wraps a loopbackClient and counts RequestVote calls, to
// prove startElection sent no RPCs when local persistence failed.
type countingClient struct {
	peer              *Node
	requestVoteCalled int32
}

func (c *countingClient) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	atomic.AddInt32(&c.requestVoteCalled, 1)
	return c.peer.RequestVote(ctx, req)
}

func (c *countingClient) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return c.peer.AppendEntries(ctx, req)
}

func TestStartElection_BumpsTermOnPersistFailureButSendsNoRPCs(t *testing.T) {
	candidate := newTestNode(1)
	peerNode := newTestNode(2)

	cc := &countingClient{peer: peerNode}
	candidate.peers[2] = cc

	fp := &fakePersister{failSaveState: true}
	candidate.persister = fp

	candidate.startElection(context.Background())

	if candidate.currentTerm != 1 {
		t.Errorf("currentTerm = %d, want 1 (bumped in memory even though persistence failed)", candidate.currentTerm)
	}
	if candidate.votedFor != 1 {
		t.Errorf("votedFor = %d, want 1 (voted for self in memory)", candidate.votedFor)
	}
	if candidate.state != Candidate {
		t.Errorf("state = %s, want CANDIDATE", candidate.state)
	}
	if got := atomic.LoadInt32(&cc.requestVoteCalled); got != 0 {
		t.Errorf("RequestVote called %d times, want 0: must not send RPCs claiming an unpersisted term/vote", got)
	}
}
