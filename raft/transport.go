package raft

import (
	"context"

	"github.com/colinwang05/raft-kv/internal/config"
	pb "github.com/colinwang05/raft-kv/proto"
)

// RaftClient is the outbound-RPC interface a Node uses to talk to a peer.
// It wraps the generated gRPC client stub so the rest of the raft package
// depends on an interface, not a concrete transport.
type RaftClient interface {
	RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

// server adapts a *Node to the generated pb.RaftServiceServer interface,
// delegating each RPC to the corresponding Node method.
type server struct {
	pb.UnimplementedRaftServiceServer
	node *Node
}

// NewServer returns a pb.RaftServiceServer backed by node.
func NewServer(node *Node) pb.RaftServiceServer {
	return &server{node: node}
}

func (s *server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.node.RequestVote(ctx, req)
}

func (s *server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.node.AppendEntries(ctx, req)
}

// dialPeers opens a gRPC connection and RaftClient to every peer in cfg.Peers.
func dialPeers(cfg *config.Config) (map[int]RaftClient, error) {
	// TODO: for each peer, grpc.Dial(addr) and wrap pb.NewRaftServiceClient(conn).
	return nil, nil
}
