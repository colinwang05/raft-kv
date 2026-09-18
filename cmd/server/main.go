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

	wal := storage.NewWAL(cfg.DataDir)
	meta, err := wal.LoadMetadata()
	if err != nil {
		log.Fatalf("load metadata: %v", err)
	}
	restoredLog, err := wal.LoadLog()
	if err != nil {
		log.Fatalf("load log: %v", err)
	}
	store := storage.NewKVStore()

	node := raft.NewNode(cfg)
	node.RestoreState(meta.CurrentTerm, meta.VotedFor, restoredLog)
	node.SetPersister(wal)
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
