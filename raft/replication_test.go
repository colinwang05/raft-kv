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

// --- M3: leader-side replication (Propose, majority commit) ---

func TestPropose_NonLeaderReturnsFalseAndLeavesLogUnchanged(t *testing.T) {
	n := newTestNode(1)
	n.state = Follower
	n.currentTerm = 3

	_, _, isLeader := n.Propose(Command{Op: PUT, Key: "x", Value: "10"})
	if isLeader {
		t.Error("isLeader = true, want false: node is not the leader")
	}
	if len(n.log) != 0 {
		t.Errorf("log = %v, want unchanged (empty)", n.log)
	}
}

func TestPropose_LeaderAppendsWithCorrectIndexAndTerm(t *testing.T) {
	n := newTestNode(1)
	n.state = Leader
	n.currentTerm = 4
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 3}}

	index, term, isLeader := n.Propose(Command{Op: PUT, Key: "x", Value: "10"})
	if !isLeader {
		t.Fatal("isLeader = false, want true")
	}
	if index != 3 {
		t.Errorf("index = %d, want 3 (lastLogIndex+1)", index)
	}
	if term != 4 {
		t.Errorf("term = %d, want 4 (currentTerm)", term)
	}
	if len(n.log) != 3 {
		t.Fatalf("log length = %d, want 3", len(n.log))
	}
	last := n.log[2]
	if last.Index != 3 || last.Term != 4 || last.Command.Key != "x" || last.Command.Value != "10" {
		t.Errorf("appended entry = %+v, want {Index:3 Term:4 Command:{PUT x 10}}", last)
	}
}

func TestCommand_RoundTripsThroughEncodeDecode(t *testing.T) {
	want := Command{Op: DELETE, Key: "foo", Value: "bar", ClientID: "client-1", RequestID: 42}
	got := decodeCommand(encodeCommand(want))
	if got != want {
		t.Errorf("decodeCommand(encodeCommand(cmd)) = %+v, want %+v", got, want)
	}
}

func TestCommand_DecodeEmptyBytesIsZeroCommand(t *testing.T) {
	got := decodeCommand(nil)
	if got != (Command{}) {
		t.Errorf("decodeCommand(nil) = %+v, want zero Command", got)
	}
}

func TestMaybeAdvanceCommitIndex_AdvancesOnMajorityCurrentTerm(t *testing.T) {
	n := newTestNode(1)
	n.state = Leader
	n.currentTerm = 2
	n.log = []LogEntry{{Index: 1, Term: 2}, {Index: 2, Term: 2}}
	n.peers = map[int]RaftClient{2: nil, 3: nil, 4: nil} // total=4, majority=3
	n.matchIndex = map[int]uint64{2: 2, 3: 2, 4: 0}      // leader(1) + 2 + 3 = 3 => majority

	n.maybeAdvanceCommitIndexLocked()

	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 (majority replicated a current-term entry)", n.commitIndex)
	}
}

// TestMaybeAdvanceCommitIndex_DoesNotAdvanceToOlderTermEntry is the
// Figure-8-style safety case the design doc calls out (Raft paper §5.4.2):
// a leader must never directly commit an entry from an older term, even
// when a majority of matchIndex already covers it, because a future leader
// could still overwrite it.
func TestMaybeAdvanceCommitIndex_DoesNotAdvanceToOlderTermEntry(t *testing.T) {
	n := newTestNode(1)
	n.state = Leader
	n.currentTerm = 1
	n.log = []LogEntry{{Index: 1, Term: 1}}
	n.peers = map[int]RaftClient{2: nil, 3: nil} // total=3, majority=2
	n.matchIndex = map[int]uint64{2: 1, 3: 1}    // leader(1) + 2 + 3 = 3 => majority already covers index 1

	// Bump currentTerm to 2 without any term-2 entry yet appended (e.g. this
	// node just won a new election). Index 1 is still term-1.
	n.currentTerm = 2

	n.maybeAdvanceCommitIndexLocked()

	if n.commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0: must not directly commit an older-term entry even with majority replication", n.commitIndex)
	}

	// Once a current-term entry is appended and reaches majority, it commits
	// and implicitly commits the older-term entry before it too.
	n.log = append(n.log, LogEntry{Index: 2, Term: 2})
	n.matchIndex[2] = 2
	n.matchIndex[3] = 2

	n.maybeAdvanceCommitIndexLocked()

	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2: a current-term entry commits and implicitly commits everything before it", n.commitIndex)
	}
}

func TestMaybeAdvanceCommitIndex_NeverRegressesAndRespectsExactMajority(t *testing.T) {
	n := newTestNode(1)
	n.state = Leader
	n.currentTerm = 1
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	n.peers = map[int]RaftClient{2: nil, 3: nil, 4: nil} // total=4, majority=3
	n.commitIndex = 2

	// Only one peer (plus the leader) has index 3: leader(1) + peer2(1) = 2,
	// which is short of majority=3, so commitIndex must not advance past 2.
	n.matchIndex = map[int]uint64{2: 3, 3: 1, 4: 0}

	n.maybeAdvanceCommitIndexLocked()

	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2 (unchanged: no majority for index 3, and must never regress)", n.commitIndex)
	}
}

func TestReplication_E2E_ProposeReplicatesAndCommitsAcrossLoopbackCluster(t *testing.T) {
	nodes := newLoopbackCluster(3)
	leader := nodes[0]

	// Manually install leader state (mirrors becomeLeader) rather than
	// running a full election, for a deterministic test.
	leader.mu.Lock()
	leader.state = Leader
	leader.currentTerm = 1
	lastIndex := leader.lastLogIndex()
	for peerID := range leader.peers {
		leader.nextIndex[peerID] = lastIndex + 1
		leader.matchIndex[peerID] = 0
	}
	leader.mu.Unlock()

	cmd := Command{Op: PUT, Key: "x", Value: "10", ClientID: "c1", RequestID: 1}
	index, term, isLeader := leader.Propose(cmd)
	if !isLeader {
		t.Fatal("Propose: isLeader = false, want true")
	}
	if index != 1 || term != 1 {
		t.Fatalf("Propose returned index=%d term=%d, want index=1 term=1", index, term)
	}

	ctx := context.Background()
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		leader.replicateTo(ctx, n.id)
	}

	leader.mu.Lock()
	commitIndex := leader.commitIndex
	leader.mu.Unlock()
	if commitIndex != 1 {
		t.Fatalf("leader commitIndex = %d, want 1 after replicating to a majority", commitIndex)
	}

	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		n.mu.Lock()
		gotLog := append([]LogEntry(nil), n.log...)
		n.mu.Unlock()
		if len(gotLog) != 1 {
			t.Fatalf("follower %d log = %v, want 1 entry", n.id, gotLog)
		}
		if gotLog[0].Command != cmd {
			t.Errorf("follower %d decoded command = %+v, want %+v", n.id, gotLog[0].Command, cmd)
		}
	}
}
