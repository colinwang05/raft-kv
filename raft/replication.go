package raft

import (
	"context"
	"log"
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
		entries = append(entries, &pb.LogEntry{Index: e.Index, Term: e.Term, Command: encodeCommand(e.Command)})
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
		n.maybeAdvanceCommitIndexLocked()
	} else if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

// maybeAdvanceCommitIndexLocked recomputes commitIndex from matchIndex
// across a majority of the cluster, honoring the Raft rule that a leader
// only ever directly commits an entry from its own current term (Raft
// paper §5.4.2 / design doc section 8) — never an older-term entry, even
// if a majority already has it, since a future leader could still
// overwrite it. Directly committing a current-term entry implicitly
// commits every entry before it too. Caller must hold n.mu and this node
// must currently be Leader.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	total := len(n.peers) + 1
	majority := total/2 + 1
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		count := 1 // the leader's own log always has everything up to lastLogIndex
		for peerID := range n.peers {
			if n.matchIndex[peerID] >= idx {
				count++
			}
		}
		if count >= majority {
			if n.log[idx-1].Term == n.currentTerm {
				old := n.commitIndex
				n.commitIndex = idx
				log.Printf("[node=%d term=%d state=%s] commit advanced old=%d new=%d", n.id, n.currentTerm, n.state, old, idx)
			}
			return // idx is the highest majority-replicated index — whether or not we committed it, no smaller idx can be committed either
		}
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
	n.leaderID = int(req.LeaderId)

	if n.findConflict(req.PrevLogIndex, req.PrevLogTerm) {
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
	}

	// Compute whether the log will actually change *before* mutating it: a
	// steady-state heartbeat (no new entries, prevLogIndex already at the
	// end of our log) is by far the most common AppendEntries call and
	// must not trigger a full rewrite+fsync every ~100ms. A conflict-only
	// truncation (a stale uncommitted suffix discarded even with zero new
	// entries carried in this call) does still need persisting.
	logChanged := req.PrevLogIndex < uint64(len(n.log)) || len(req.Entries) > 0
	// A shallow "oldLog := n.log" is not a safe rollback value: Go's append
	// below reuses n.log's backing array in place whenever it has spare
	// capacity, which silently overwrites the very elements oldLog would
	// still be pointing at. Only a deep copy survives as a true snapshot,
	// and it's only needed when we might actually roll back.
	var oldLog []LogEntry
	if logChanged && n.persister != nil {
		oldLog = append([]LogEntry(nil), n.log...)
	}
	n.log = append(n.log[:req.PrevLogIndex], toLogEntries(req.Entries)...)

	if logChanged && n.persister != nil {
		if err := n.persister.PersistLog(n.log); err != nil {
			// Don't let this node's in-memory log claim durability it
			// doesn't have: roll back so a future RequestVote freshness
			// check or candidacy from this node only ever reflects what's
			// actually on disk.
			n.log = oldLog
			log.Printf("[node=%d term=%d state=%s] persist log failed: %v", n.id, n.currentTerm, n.state, err)
			return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}, nil
		}
	}

	lastNewIndex := n.lastLogIndex()
	if req.LeaderCommit > n.commitIndex {
		old := n.commitIndex
		n.commitIndex = min(req.LeaderCommit, lastNewIndex)
		log.Printf("[node=%d term=%d state=%s] commit advanced old=%d new=%d", n.id, n.currentTerm, n.state, old, n.commitIndex)
	}

	return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: lastNewIndex}, nil
}

// toLogEntries converts wire-format log entries to internal LogEntry
// values, decoding each entry's Command payload.
func toLogEntries(entries []*pb.LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	for i, e := range entries {
		out[i] = LogEntry{Index: e.Index, Term: e.Term, Command: decodeCommand(e.Command)}
	}
	return out
}
