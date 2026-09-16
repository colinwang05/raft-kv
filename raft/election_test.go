package raft

import (
	"context"
	"testing"
	"time"

	"github.com/colinwang05/raft-kv/internal/config"
	pb "github.com/colinwang05/raft-kv/proto"
)

func newTestNode(id int) *Node {
	cfg := &config.Config{
		ID:                 id,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
	}
	return NewNode(cfg)
}

// --- RequestVote handler: unit tests, one Node in isolation ---

func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 5

	resp, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term: 3, CandidateId: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VoteGranted {
		t.Error("VoteGranted = true, want false for a stale term")
	}
	if resp.Term != 5 {
		t.Errorf("resp.Term = %d, want 5 (unchanged)", resp.Term)
	}
	if n.currentTerm != 5 || n.votedFor != 0 {
		t.Errorf("node state = {term:%d, votedFor:%d}, want unchanged {5, 0}", n.currentTerm, n.votedFor)
	}
}

func TestRequestVote_GrantsWhenUpToDateAndUnvoted(t *testing.T) {
	n := newTestNode(1)

	resp, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term: 1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.VoteGranted {
		t.Error("VoteGranted = false, want true")
	}
	if n.currentTerm != 1 {
		t.Errorf("currentTerm = %d, want 1", n.currentTerm)
	}
	if n.votedFor != 2 {
		t.Errorf("votedFor = %d, want 2", n.votedFor)
	}
	if n.state != Follower {
		t.Errorf("state = %s, want FOLLOWER", n.state)
	}
}

func TestRequestVote_DeniesSecondVoteDifferentCandidate(t *testing.T) {
	n := newTestNode(1)
	ctx := context.Background()

	if _, err := n.RequestVote(ctx, &pb.RequestVoteRequest{Term: 1, CandidateId: 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resp, err := n.RequestVote(ctx, &pb.RequestVoteRequest{Term: 1, CandidateId: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VoteGranted {
		t.Error("VoteGranted = true, want false: only one vote per term")
	}
	if n.votedFor != 2 {
		t.Errorf("votedFor = %d, want 2 (unchanged)", n.votedFor)
	}
}

func TestRequestVote_GrantsSameCandidateAgainSameTerm(t *testing.T) {
	// A retried RequestVote from the same candidate in the same term (e.g.
	// after an RPC timeout) must still be granted.
	n := newTestNode(1)
	ctx := context.Background()

	if _, err := n.RequestVote(ctx, &pb.RequestVoteRequest{Term: 1, CandidateId: 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resp, err := n.RequestVote(ctx, &pb.RequestVoteRequest{Term: 1, CandidateId: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.VoteGranted {
		t.Error("VoteGranted = false, want true for a retry from the already-voted-for candidate")
	}
}

func TestRequestVote_HigherTermStepsDownAndGrantsVote(t *testing.T) {
	n := newTestNode(1)
	n.state = Leader
	n.currentTerm = 2

	resp, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term: 5, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.VoteGranted {
		t.Error("VoteGranted = false, want true")
	}
	if n.state != Follower {
		t.Errorf("state = %s, want FOLLOWER: a higher term must force step-down", n.state)
	}
	if n.currentTerm != 5 {
		t.Errorf("currentTerm = %d, want 5", n.currentTerm)
	}
	if n.votedFor != 3 {
		t.Errorf("votedFor = %d, want 3", n.votedFor)
	}
}

func TestRequestVote_HigherTermStepsDownButDeniesVoteWhenLogBehind(t *testing.T) {
	n := newTestNode(1)
	n.currentTerm = 2
	n.log = []LogEntry{{Index: 1, Term: 2}, {Index: 2, Term: 2}}

	resp, err := n.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term: 5, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 0, // behind our log
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VoteGranted {
		t.Error("VoteGranted = true, want false: candidate log is behind")
	}
	// The term bump happens unconditionally on seeing a higher term, even
	// when the vote itself is denied (design doc section 21).
	if n.currentTerm != 5 {
		t.Errorf("currentTerm = %d, want 5 (term updates regardless of vote outcome)", n.currentTerm)
	}
	if n.votedFor != 0 {
		t.Errorf("votedFor = %d, want 0 (vote not granted to anyone)", n.votedFor)
	}
}

// --- Multi-node election safety, via an in-memory loopback transport ---

// loopbackClient implements RaftClient by calling a peer Node's RPC
// handlers directly in-process, without gRPC. This lets tests build a
// small cluster of real Nodes exercising the real concurrent election
// path, deterministically and fast (no ports, no serialization).
type loopbackClient struct {
	peer *Node
}

func (c *loopbackClient) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return c.peer.RequestVote(ctx, req)
}

func (c *loopbackClient) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return c.peer.AppendEntries(ctx, req)
}

func newLoopbackCluster(n int) []*Node {
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = newTestNode(i + 1)
	}
	for _, node := range nodes {
		for _, peer := range nodes {
			if peer.id == node.id {
				continue
			}
			node.peers[peer.id] = &loopbackClient{peer: peer}
		}
	}
	return nodes
}

func snapshotState(n *Node) (State, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

func TestElectionSafety_AtMostOneLeaderPerTerm(t *testing.T) {
	nodes := newLoopbackCluster(3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		go n.runElectionLoop(ctx)
	}

	leadersByTerm := make(map[uint64]map[int]bool)
	sawLeader := false

	deadline := time.After(1 * time.Second)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

pollLoop:
	for {
		select {
		case <-deadline:
			break pollLoop
		case <-ticker.C:
			for _, n := range nodes {
				state, term := snapshotState(n)
				if state != Leader {
					continue
				}
				sawLeader = true
				if leadersByTerm[term] == nil {
					leadersByTerm[term] = make(map[int]bool)
				}
				leadersByTerm[term][n.id] = true
			}
		}
	}

	if !sawLeader {
		t.Fatal("no node was ever observed as LEADER within the test window")
	}
	for term, leaders := range leadersByTerm {
		if len(leaders) > 1 {
			t.Errorf("term %d had multiple simultaneous leaders: %v", term, leaders)
		}
	}
}
