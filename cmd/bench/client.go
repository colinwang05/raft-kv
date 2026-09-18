// client.go implements a small reusable KV client for the benchmark tool.
//
// Unlike raftctl (cmd/client), which dials fresh per CLI invocation,
// benchmark workers issue many requests per second: connections to every
// known node are dialed once and cached, and the last node that answered
// without a redirect is remembered so steady-state traffic goes straight
// to the leader instead of re-probing every node on each call.
package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	pb "github.com/colinwang05/raft-kv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// rpcAttemptTimeout bounds a single RPC attempt against one node.
const rpcAttemptTimeout = 2 * time.Second

// kvClient follows the KVService redirect contract (LeaderId != 0 always
// means "ask that node instead," per cmd/server/kvserver.go) while caching
// gRPC connections and the last known leader address across calls.
type kvClient struct {
	seedAddr string
	peers    map[int32]string // peer id -> addr; only used to build the probe order

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	leader string // last address that answered without a redirect; "" if unknown
}

func newKVClient(seedAddr string, peers map[int]string) *kvClient {
	p := make(map[int32]string, len(peers))
	for id, addr := range peers {
		p[int32(id)] = addr
	}
	return &kvClient{seedAddr: seedAddr, peers: p, conns: map[string]*grpc.ClientConn{}}
}

func (c *kvClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}

// candidateOrder returns addresses to try, most-likely-correct first: the
// last known leader, then the seed address, then every known peer in
// ascending-id order — deduplicated.
func (c *kvClient) candidateOrder() []string {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()

	seen := map[string]bool{}
	var order []string
	add := func(addr string) {
		if addr != "" && !seen[addr] {
			seen[addr] = true
			order = append(order, addr)
		}
	}
	add(leader)
	add(c.seedAddr)

	ids := make([]int32, 0, len(c.peers))
	for id := range c.peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		add(c.peers[id])
	}
	return order
}

func (c *kvClient) connFor(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *kvClient) setLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

// attemptFunc is one RPC call against a single node: ok is Success/Found,
// leaderID is nonzero exactly when the response is a redirect, value is
// only meaningful for Get.
type attemptFunc func(ctx context.Context, cl pb.KVServiceClient) (ok bool, leaderID int32, value string, err error)

// do runs attempt against candidates in order until one returns a
// non-redirect answer. It returns an error only when every known address
// was exhausted without a definitive answer — e.g. the cluster has no
// leader right now, which is exactly the window the failover benchmark
// measures.
func (c *kvClient) do(ctx context.Context, attempt attemptFunc) (value string, ok bool, err error) {
	order := c.candidateOrder()
	var lastErr error

	for _, target := range order {
		conn, dialErr := c.connFor(target)
		if dialErr != nil {
			lastErr = fmt.Errorf("%s: %w", target, dialErr)
			continue
		}

		rctx, cancel := context.WithTimeout(ctx, rpcAttemptTimeout)
		respOK, leaderID, respValue, rpcErr := attempt(rctx, pb.NewKVServiceClient(conn))
		cancel()

		if rpcErr != nil {
			lastErr = fmt.Errorf("%s: %w", target, rpcErr)
			continue
		}
		if leaderID == 0 {
			c.setLeader(target)
			return respValue, respOK, nil
		}
		lastErr = fmt.Errorf("%s: redirected to leader %d", target, leaderID)
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no addresses to try")
	}
	return "", false, fmt.Errorf("exhausted all %d known addresses: %w", len(order), lastErr)
}

func (c *kvClient) Put(ctx context.Context, key, value string) error {
	_, ok, err := c.do(ctx, func(rctx context.Context, cl pb.KVServiceClient) (bool, int32, string, error) {
		resp, err := cl.Put(rctx, &pb.PutRequest{Key: key, Value: value})
		if err != nil {
			return false, 0, "", err
		}
		return resp.Success, resp.LeaderId, "", nil
	})
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("put not confirmed committed")
	}
	return nil
}

func (c *kvClient) Get(ctx context.Context, key string) (value string, found bool, err error) {
	return c.do(ctx, func(rctx context.Context, cl pb.KVServiceClient) (bool, int32, string, error) {
		resp, err := cl.Get(rctx, &pb.GetRequest{Key: key})
		if err != nil {
			return false, 0, "", err
		}
		return resp.Found, resp.LeaderId, resp.Value, nil
	})
}
