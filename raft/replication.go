package raft

import (
	"context"

	pb "github.com/colinwang05/raft-kv/proto"
)

// runHeartbeatLoop is the leader-only loop that sends periodic AppendEntries
// (empty = heartbeat) to every peer at HeartbeatInterval (design doc
// section 7). Exits (or no-ops) when this node is not Leader, or when ctx
// is cancelled.
func (n *Node) runHeartbeatLoop(ctx context.Context) {
	// TODO: implement.
}

// replicateTo sends pending log entries (or a heartbeat) to one follower,
// advancing nextIndex/matchIndex on success and backing off nextIndex on
// log-mismatch rejection (design doc section 9).
func (n *Node) replicateTo(ctx context.Context, peerID int) {
	// TODO: implement.
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
	// TODO: implement.
	return nil, nil
}
