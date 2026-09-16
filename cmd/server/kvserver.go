package main

import (
	"context"

	pb "github.com/colinwang05/raft-kv/proto"
	"github.com/colinwang05/raft-kv/raft"
	"github.com/colinwang05/raft-kv/storage"
)

// kvServer adapts a *raft.Node and *storage.KVStore to the generated
// pb.KVServiceServer interface: PUT/DELETE go through Raft (Propose +
// WaitApplied); GET is served directly from the local state machine, but
// only when this node believes it is the leader (design doc's "leader
// GET" — a follower's local copy of the state machine can be stale).
//
// Redirect contract: LeaderId != 0 in any response always means "go ask
// that node instead," regardless of Found/Success. Only Found: false with
// LeaderId == 0 means a real not-found.
type kvServer struct {
	pb.UnimplementedKVServiceServer
	node  *raft.Node
	store *storage.KVStore
}

func (s *kvServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	index, _, isLeader := s.node.Propose(raft.Command{
		Op: raft.PUT, Key: req.Key, Value: req.Value,
		ClientID: req.ClientId, RequestID: req.RequestId,
	})
	if !isLeader {
		return &pb.PutResponse{Success: false, LeaderId: int32(s.node.LeaderID())}, nil
	}
	if !s.node.WaitApplied(ctx, index) {
		return &pb.PutResponse{Success: false, LeaderId: int32(s.node.LeaderID())}, nil
	}
	return &pb.PutResponse{Success: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	index, _, isLeader := s.node.Propose(raft.Command{
		Op: raft.DELETE, Key: req.Key,
		ClientID: req.ClientId, RequestID: req.RequestId,
	})
	if !isLeader {
		return &pb.DeleteResponse{Success: false, LeaderId: int32(s.node.LeaderID())}, nil
	}
	if !s.node.WaitApplied(ctx, index) {
		return &pb.DeleteResponse{Success: false, LeaderId: int32(s.node.LeaderID())}, nil
	}
	return &pb.DeleteResponse{Success: true}, nil
}

func (s *kvServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if !s.node.IsLeader() {
		return &pb.GetResponse{Found: false, LeaderId: int32(s.node.LeaderID())}, nil
	}
	value, found := s.store.Get(req.Key)
	return &pb.GetResponse{Found: found, Value: value}, nil
}
