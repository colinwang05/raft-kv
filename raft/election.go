package raft

import (
	"context"

	pb "github.com/colinwang05/raft-kv/proto"
)

// runElectionLoop owns the randomized election timer (design doc section 7).
// It resets whenever a valid AppendEntries is received, and triggers
// startElection on timeout. Exits when ctx is cancelled.
func (n *Node) runElectionLoop(ctx context.Context) {
	// TODO: randomized timeout in [ElectionTimeoutMin, ElectionTimeoutMax);
	// reset on valid AppendEntries; on fire, call startElection().
}

// startElection transitions the node to Candidate, increments currentTerm,
// votes for itself, persists term/vote, and requests votes from all peers
// concurrently. Becomes Leader on a majority (design doc section 7).
func (n *Node) startElection() {
	// TODO: implement.
}

// RequestVote implements the RaftService RPC handler (design doc section 6).
//
// Invariants this must uphold (design doc section 21):
//   - never decrease currentTerm
//   - grant at most one vote per term
//   - grant only if the candidate's log is at least as up-to-date as ours
//   - persist votedFor before returning vote_granted=true
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// TODO: implement.
	return nil, nil
}
