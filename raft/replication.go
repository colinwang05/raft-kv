package raft

import (
	"context"
	"time"

	pb "github.com/colinwang05/raft-kv/proto"
)

// runHeartbeatLoop is the leader-only loop that sends periodic AppendEntries
// (empty = heartbeat) to every peer at HeartbeatInterval (design doc
// section 7). No-ops on ticks where this node is not Leader. Exits when
// ctx is cancelled.
func (n *Node) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			peerIDs := make([]int, 0, len(n.peers))
			for id := range n.peers {
				peerIDs = append(peerIDs, id)
			}
			n.mu.Unlock()

			if !isLeader {
				continue
			}
			for _, peerID := range peerIDs {
				go n.replicateTo(ctx, peerID)
			}
		}
	}
}

// replicateTo sends pending log entries (or a heartbeat, if there are none
// past nextIndex[peerID]) to one follower, advancing nextIndex/matchIndex
// on success and backing off nextIndex on log-mismatch rejection (design
// doc section 9). becomeLeader guarantees nextIndex/matchIndex are
// initialized for every current peer before this is ever called.
func (n *Node) replicateTo(ctx context.Context, peerID int) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	client, ok := n.peers[peerID]
	if !ok {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	prevLogIndex := n.nextIndex[peerID] - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	entries := make([]*pb.LogEntry, 0, uint64(len(n.log))-prevLogIndex)
	for _, e := range n.log[prevLogIndex:] {
		entries = append(entries, &pb.LogEntry{Index: e.Index, Term: e.Term})
	}
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := client.AppendEntries(rctx, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     int32(n.id),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return // will retry on the next heartbeat tick
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.becomeFollowerLocked(resp.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return // stale response; a newer term/role change already happened
	}
	if resp.Success {
		n.matchIndex[peerID] = resp.MatchIndex
		n.nextIndex[peerID] = resp.MatchIndex + 1
	} else if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

// AppendEntries implements the RaftService RPC handler (design doc
// section 6).
//
// Invariants this must uphold (design doc section 21):
//   - reject stale terms
//   - verify the entry at prevLogIndex has prevLogTerm before accepting
//   - remove any conflicting uncommitted suffix before appending
//   - advance commitIndex to min(leaderCommit, index of last new entry)
//   - persist received entries before returning success
func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	// A valid AppendEntries from a leader whose term is at least ours:
	// adopt the term if it's higher, and always step down to follower —
	// this also covers a candidate hearing from the winner of its own
	// current term (never two leaders in the same term).
	n.becomeFollowerLocked(req.Term)
	n.resetElectionTimer()

	if n.findConflict(req.PrevLogIndex, req.PrevLogTerm) {
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	// TODO(M5): persist appended entries before returning success.
	n.log = append(n.log[:req.PrevLogIndex], toLogEntries(req.Entries)...)

	lastNewIndex := n.lastLogIndex()
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, lastNewIndex)
	}

	return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: lastNewIndex}, nil
}

// toLogEntries converts wire-format log entries to internal LogEntry
// values. Only Index/Term matter for log-consistency checks until the KV
// API defines Command's wire format and starts producing real commands.
//
// TODO(M4): decode Command from entry.Command.
func toLogEntries(entries []*pb.LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{Index: e.Index, Term: e.Term}
	}
	return out
}
