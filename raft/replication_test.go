package raft

import (
	"context"
	"testing"

	pb "github.com/colinwang05/raft-kv/proto"
)

// These tests define the contract for AppendEntries (still a `nil, nil`
// stub) ahead of implementing it — heartbeat semantics (M2) and log-
// replication consistency (M3, design doc section 18: "AppendEntries
// rejects wrong prevLogTerm", "conflicting suffix is removed correctly",
// "commit index never decreases"). They are expected to fail until those
// milestones land; mustAppendEntries fails with a clear message instead of
// a nil-pointer panic in the meantime.

func mustAppendEntries(t *testing.T, n *Node, req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	t.Helper()
	resp, err := n.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("AppendEntries returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("AppendEntries returned a nil response (not yet implemented)")
	}
	return resp
}

// --- M2: heartbeats ---

func TestAppendEntries_RejectsStaleTerm(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 5

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{Term: 3, LeaderId: 2})
	if resp.Success {
		t.Error("Success = true, want false for a stale term")
	}
	if resp.Term != 5 {
		t.Errorf("resp.Term = %d, want 5 (unchanged)", resp.Term)
	}
}

func TestAppendEntries_EmptyEntriesIsHeartbeatAndSucceedsOnEmptyLog(t *testing.T) {
	n := newTestNode(1)

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
	})
	if !resp.Success {
		t.Error("Success = false, want true for a heartbeat matching an empty log")
	}
	if n.currentTerm != 1 {
		t.Errorf("currentTerm = %d, want 1", n.currentTerm)
	}
}

func TestAppendEntries_ResetsElectionTimer(t *testing.T) {
	n := newTestNode(1)

	mustAppendEntries(t, n, &pb.AppendEntriesRequest{Term: 1, LeaderId: 2})

	select {
	case <-n.resetElectionC:
		// Good: a valid AppendEntries must reset the election timer so
		// followers don't start a needless election against a live leader
		// (design doc section 7).
	default:
		t.Error("valid AppendEntries did not signal an election-timer reset")
	}
}

func TestAppendEntries_HigherTermCausesLeaderStepDown(t *testing.T) {
	n := newTestNode(1)
	n.state = Leader
	n.currentTerm = 2

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{Term: 5, LeaderId: 3})
	if !resp.Success {
		t.Error("Success = false, want true")
	}
	if n.state != Follower {
		t.Errorf("state = %s, want FOLLOWER: a higher term must force step-down", n.state)
	}
	if n.currentTerm != 5 {
		t.Errorf("currentTerm = %d, want 5", n.currentTerm)
	}
}

func TestAppendEntries_CandidateStepsDownOnSameTermLeader(t *testing.T) {
	// Raft correctness rule (not spelled out in the design doc, but
	// required so the cluster converges): a candidate that hears from a
	// legitimate leader for its own current term must revert to follower
	// rather than keep competing in that term.
	n := newTestNode(1)
	n.state = Candidate
	n.currentTerm = 3

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{Term: 3, LeaderId: 2})
	if !resp.Success {
		t.Error("Success = false, want true")
	}
	if n.state != Follower {
		t.Errorf("state = %s, want FOLLOWER", n.state)
	}
}

// --- M3: log replication consistency ---

func TestAppendEntries_RejectsWhenPrevLogTermMismatch(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 2
	n.log = []LogEntry{{Index: 1, Term: 1}}

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 2, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 2, // we have term 1 at index 1, not 2
	})
	if resp.Success {
		t.Error("Success = true, want false: prevLogTerm does not match")
	}
}

func TestAppendEntries_RejectsWhenPrevLogIndexBeyondLog(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 2

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 2, LeaderId: 2, PrevLogIndex: 5, PrevLogTerm: 2, // our log is empty
	})
	if resp.Success {
		t.Error("Success = true, want false: prevLogIndex is beyond our log")
	}
}

func TestAppendEntries_AppendsNewEntriesOnMatchingPrevLog(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1
	n.log = []LogEntry{{Index: 1, Term: 1}}

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*pb.LogEntry{
			{Index: 2, Term: 1},
			{Index: 3, Term: 1},
		},
	})
	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if got := n.lastLogIndex(); got != 3 {
		t.Errorf("lastLogIndex() = %d, want 3 after appending 2 new entries", got)
	}
	if resp.MatchIndex != 3 {
		t.Errorf("MatchIndex = %d, want 3", resp.MatchIndex)
	}
}

func TestAppendEntries_RemovesConflictingSuffixAndAppendsLeaderEntries(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 3
	// Follower has a stale, uncommitted suffix from an old term.
	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1}, // conflicts with the leader's term-2 entry below
		{Index: 3, Term: 1},
	}

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 3, LeaderId: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*pb.LogEntry{
			{Index: 2, Term: 2},
		},
	})
	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if len(n.log) != 2 {
		t.Fatalf("log = %v, want exactly 2 entries (conflicting suffix removed)", n.log)
	}
	if n.log[1].Term != 2 {
		t.Errorf("log[1].Term = %d, want 2 (leader's entry, not the stale term-1 one)", n.log[1].Term)
	}
}

func TestAppendEntries_CommitIndexAdvancesToMinLeaderCommitAndLastNewEntry(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
		},
		LeaderCommit: 5, // ahead of what we actually have
	})
	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 (min(leaderCommit=5, lastNewEntry=2))", n.commitIndex)
	}
}

func TestAppendEntries_CommitIndexNeverDecreases(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 1
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	n.commitIndex = 2

	resp := mustAppendEntries(t, n, &pb.AppendEntriesRequest{
		Term: 1, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1,
		LeaderCommit: 0, // a stale/reordered heartbeat must not roll commitIndex back
	})
	if !resp.Success {
		t.Fatal("Success = false, want true")
	}
	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 (must never decrease)", n.commitIndex)
	}
}
