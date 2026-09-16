package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/colinwang05/raft-kv/internal/config"
	pb "github.com/colinwang05/raft-kv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
// Connections are lazy (gRPC connects on first RPC), so this returns
// immediately without requiring peers to already be up.
func dialPeers(cfg *config.Config) (map[int]RaftClient, error) {
	clients := make(map[int]RaftClient, len(cfg.Peers))
	for peerID, addr := range cfg.Peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial peer %d (%s): %w", peerID, addr, err)
		}
		clients[peerID] = &raftClient{
			client:  pb.NewRaftServiceClient(conn),
			timeout: cfg.RPCTimeout,
		}
	}
	return clients, nil
}

// raftClient implements RaftClient over a real gRPC connection, applying
// cfg.RPCTimeout to every call so stuck RPCs don't hang indefinitely
// (design doc section 14).
type raftClient struct {
	client  pb.RaftServiceClient
	timeout time.Duration
}

func (c *raftClient) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.RequestVote(ctx, req)
}

func (c *raftClient) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.AppendEntries(ctx, req)
}
