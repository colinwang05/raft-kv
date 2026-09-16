package raft

import (
	"context"
	"log"
	"math/rand"
	"time"

	pb "github.com/colinwang05/raft-kv/proto"
)

// randomElectionTimeout returns a random duration in
// [cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax) (design doc section 7).
func (n *Node) randomElectionTimeout() time.Duration {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int63n(int64(hi-lo)))
}

// runElectionLoop owns the randomized election timer (design doc section 7).
// It resets whenever a valid AppendEntries is received or a vote is
// granted, and triggers startElection on timeout. Exits when ctx is
// cancelled.
func (n *Node) runElectionLoop(ctx context.Context) {
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-n.resetElectionC:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(n.randomElectionTimeout())

		case <-timer.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if !isLeader {
				n.startElection(ctx)
			}
			timer.Reset(n.randomElectionTimeout())
		}
	}
}

// startElection transitions the node to Candidate, increments currentTerm,
// votes for itself, and requests votes from all peers concurrently.
// Becomes Leader on a majority (design doc section 7).
func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
		return
	}
	n.state = Candidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	lastIndex := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	peers := make(map[int]RaftClient, len(n.peers))
	for id, c := range n.peers {
		peers[id] = c
	}
	n.mu.Unlock()

	// TODO(M5): persist currentTerm and votedFor before sending RequestVote RPCs.

	log.Printf("[node=%d term=%d state=%s] election timeout; starting election", n.id, term, Candidate)

	total := len(peers) + 1
	majority := total/2 + 1
	votes := 1 // vote for self

	if votes >= majority {
		n.becomeLeader(term, votes, total)
		return
	}

	type result struct {
		peerID int
		resp   *pb.RequestVoteResponse
	}
	resultsCh := make(chan result, len(peers))

	for peerID, client := range peers {
		go func(peerID int, client RaftClient) {
			rctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
			defer cancel()
			resp, err := client.RequestVote(rctx, &pb.RequestVoteRequest{
				Term:         term,
				CandidateId:  int32(n.id),
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				log.Printf("[node=%d term=%d state=%s] RequestVote to peer=%d failed: %v", n.id, term, Candidate, peerID, err)
				resultsCh <- result{peerID: peerID}
				return
			}
			resultsCh <- result{peerID: peerID, resp: resp}
		}(peerID, client)
	}

	for i := 0; i < len(peers); i++ {
		select {
		case <-ctx.Done():
			return

		case r := <-resultsCh:
			if r.resp == nil {
				continue // RPC failed; doesn't count as a vote either way.
			}

			n.mu.Lock()
			if r.resp.Term > n.currentTerm {
				n.becomeFollowerLocked(r.resp.Term)
				n.mu.Unlock()
				return
			}
			stillCandidate := n.state == Candidate && n.currentTerm == term
			n.mu.Unlock()
			if !stillCandidate {
				return // term/state moved on since we started (e.g. saw a higher term, or already won/lost).
			}

			if r.resp.VoteGranted {
				votes++
				if votes >= majority {
					n.becomeLeader(term, votes, total)
					return
				}
			}
		}
	}
	// Fell through without a majority: stay Candidate until the election
	// timer fires again and starts a new term (design doc section 7,
	// "split vote").
}

// becomeLeader transitions a Candidate to Leader for the given term,
// initializing leader-only volatile state (design doc section 7).
func (n *Node) becomeLeader(term uint64, votes, total int) {
	n.mu.Lock()
	if n.state != Candidate || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	n.state = Leader
	n.leaderID = n.id
	lastIndex := n.lastLogIndex()
	for peerID := range n.peers {
		n.nextIndex[peerID] = lastIndex + 1
		n.matchIndex[peerID] = 0
	}
	n.mu.Unlock()

	log.Printf("[node=%d term=%d state=%s] won election votes=%d/%d", n.id, term, Leader, votes, total)
	// TODO(M2): start heartbeat loop to establish authority and prevent new elections.
}

// RequestVote implements the RaftService RPC handler (design doc section 6).
//
// Invariants this must uphold (design doc section 21):
//   - never decrease currentTerm
//   - grant at most one vote per term
//   - grant only if the candidate's log is at least as up-to-date as ours
//   - persist votedFor before returning vote_granted=true
func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
	}

	candidateID := int(req.CandidateId)
	canVote := n.votedFor == 0 || n.votedFor == candidateID
	upToDate := n.isLogUpToDate(req.LastLogIndex, req.LastLogTerm)

	if !canVote || !upToDate {
		return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}

	n.votedFor = candidateID
	// TODO(M5): persist votedFor before returning vote_granted=true.
	n.resetElectionTimer()

	log.Printf("[node=%d term=%d state=%s] vote granted candidate=%d", n.id, n.currentTerm, n.state, candidateID)
	return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
}
