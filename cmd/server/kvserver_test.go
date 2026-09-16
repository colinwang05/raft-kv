package main

import (
	"context"
	"testing"
	"time"

	"github.com/colinwang05/raft-kv/internal/config"
	pb "github.com/colinwang05/raft-kv/proto"
	"github.com/colinwang05/raft-kv/raft"
	"github.com/colinwang05/raft-kv/storage"
)

// newTestKVServer builds a real single-node *raft.Node (zero peers) wired
// to a real *storage.KVStore, starts it, and waits for it to elect itself
// leader (0 peers => majority=1 => it wins immediately once its election
// timer fires; mirrors the polling pattern in raft/election_test.go's
// TestElectionSafety_AtMostOneLeaderPerTerm).
func newTestKVServer(t *testing.T) *kvServer {
	t.Helper()

	cfg := &config.Config{
		ID:                 1,
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		RPCTimeout:         100 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
	}
	node := raft.NewNode(cfg)
	store := storage.NewKVStore()
	node.SetApplier(store)

	if err := node.Start(); err != nil {
		t.Fatalf("node.Start() = %v", err)
	}
	t.Cleanup(func() {
		// Nothing exported to stop the node's background loops, but the
		// process/test binary teardown reclaims goroutines; that's fine
		// for this short-lived test.
	})

	deadline := time.After(1 * time.Second)
	for !node.IsLeader() {
		select {
		case <-deadline:
			t.Fatal("node never became leader within deadline")
		case <-time.After(2 * time.Millisecond):
		}
	}

	return &kvServer{node: node, store: store}
}

func TestKVServer_PutThenGetRoundTrips(t *testing.T) {
	s := newTestKVServer(t)
	ctx := context.Background()

	putResp, err := s.Put(ctx, &pb.PutRequest{Key: "name", Value: "Colin"})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if !putResp.Success {
		t.Fatalf("Put success = false, leaderId=%d, want true", putResp.LeaderId)
	}

	getResp, err := s.Get(ctx, &pb.GetRequest{Key: "name"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !getResp.Found {
		t.Fatalf("Get found = false, want true")
	}
	if getResp.Value != "Colin" {
		t.Errorf("Get value = %q, want %q", getResp.Value, "Colin")
	}
}

func TestKVServer_DeleteThenGetReturnsNotFound(t *testing.T) {
	s := newTestKVServer(t)
	ctx := context.Background()

	if _, err := s.Put(ctx, &pb.PutRequest{Key: "k", Value: "v"}); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	delResp, err := s.Delete(ctx, &pb.DeleteRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if !delResp.Success {
		t.Fatalf("Delete success = false, leaderId=%d, want true", delResp.LeaderId)
	}

	getResp, err := s.Get(ctx, &pb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if getResp.Found {
		t.Error("Get found = true after Delete, want false")
	}
	if getResp.LeaderId != 0 {
		t.Errorf("Get leaderId = %d, want 0: a real not-found, not a redirect", getResp.LeaderId)
	}
}

func TestKVServer_NotLeaderRedirects(t *testing.T) {
	s := newTestKVServer(t)
	ctx := context.Background()

	// Force this node to step down to Follower and record leader=2, via
	// the real (exported) AppendEntries RPC handler — exactly what a
	// legitimate peer's heartbeat would do — rather than reaching into
	// raft.Node's unexported fields from this other package. A term far
	// beyond anything reached by self-election guarantees it's accepted
	// as "at least as new" and forces the step-down.
	if _, err := s.node.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term: 1_000_000, LeaderId: 2,
	}); err != nil {
		t.Fatalf("AppendEntries returned error: %v", err)
	}
	if s.node.IsLeader() {
		t.Fatal("node.IsLeader() = true after a higher-term AppendEntries, want false")
	}
	if got := s.node.LeaderID(); got != 2 {
		t.Fatalf("node.LeaderID() = %d, want 2", got)
	}

	putResp, err := s.Put(ctx, &pb.PutRequest{Key: "k", Value: "v"})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if putResp.Success {
		t.Error("Put success = true, want false: node is not leader")
	}
	if putResp.LeaderId != 2 {
		t.Errorf("Put leaderId = %d, want 2", putResp.LeaderId)
	}

	getResp, err := s.Get(ctx, &pb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if getResp.Found {
		t.Error("Get found = true, want false: node is not leader")
	}
	if getResp.LeaderId != 2 {
		t.Errorf("Get leaderId = %d, want 2 (redirect)", getResp.LeaderId)
	}
}
