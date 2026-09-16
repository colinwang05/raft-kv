// Command client is a minimal CLI for exercising the KV API:
//
//	raftctl put <key> <value>
//	raftctl get <key>
//	raftctl delete <key>
package main

import (
	"context"
	"fmt"
	"os"

	pb "github.com/colinwang05/raft-kv/proto"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: raftctl <put|get|delete> ...")
		os.Exit(1)
	}

	// TODO: accept a --addr flag (or try each configured node and follow
	// leader_id redirects on failure, per design doc section 22's demo).
	conn, err := grpc.NewClient("localhost:8001", grpc.WithInsecure())
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)
	ctx := context.Background()

	switch os.Args[1] {
	case "put":
		// TODO: client.Put(ctx, &pb.PutRequest{Key: os.Args[2], Value: os.Args[3]})
		_ = client
		_ = ctx
	case "get":
		// TODO: client.Get(ctx, &pb.GetRequest{Key: os.Args[2]})
	case "delete":
		// TODO: client.Delete(ctx, &pb.DeleteRequest{Key: os.Args[2]})
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(1)
	}
}
