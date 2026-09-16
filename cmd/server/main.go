// Command server runs one Raft KV node: config -> WAL/KV -> Node -> gRPC.
package main

import (
	"log"
	"net"

	"github.com/colinwang05/raft-kv/internal/config"
	"github.com/colinwang05/raft-kv/raft"
	"github.com/colinwang05/raft-kv/storage"
	pb "github.com/colinwang05/raft-kv/proto"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	_ = storage.NewWAL(cfg.DataDir)
	store := storage.NewKVStore()
	// TODO: load persisted metadata/log from the WAL before serving traffic
	// (design doc section 11).

	node := raft.NewNode(cfg)
	node.SetApplier(store)
	if err := node.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, raft.NewServer(node))
	pb.RegisterKVServiceServer(grpcServer, &kvServer{node: node, store: store})

	log.Printf("[node=%d] listening on %s", cfg.ID, cfg.Addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
