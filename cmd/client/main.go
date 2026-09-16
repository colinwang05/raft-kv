// Command client is a minimal CLI for exercising the KV API. Flags must
// come before the positional verb/key/value (standard Go flag-package
// behavior: FlagSet.Parse stops at the first non-flag argument):
//
//	raftctl [--addr=host:port] [--peers=id=addr,id=addr,...] put <key> <value>
//	raftctl [--addr=host:port] [--peers=id=addr,id=addr,...] get <key>
//	raftctl [--addr=host:port] [--peers=id=addr,id=addr,...] delete <key>
//
// --addr is where the first attempt goes (default localhost:8001). If
// that node isn't the leader, it (per the KV API's redirect contract)
// tells us who is via leader_id; --peers maps peer IDs to addresses so we
// can retry against the real leader without the caller needing to guess.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/colinwang05/raft-kv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// rpcAttemptTimeout bounds a single RPC attempt against one address. It
// flows through to the server's WaitApplied, so it also bounds how long
// the server waits for a Put/Delete to commit.
const rpcAttemptTimeout = 2 * time.Second

// kvResult is the outcome of one successful (transport-wise) RPC attempt,
// normalized across Put/Delete/Get so the retry loop can be shared.
type kvResult struct {
	ok       bool   // Success (put/delete) or Found (get)
	value    string // get only
	leaderID int32  // per the KV API contract: nonzero always means "ask this node instead"
}

func main() {
	fs := flag.NewFlagSet("raftctl", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8001", "address of the node to try first, e.g. localhost:8001")
	peersFlag := fs.String("peers", "", "comma-separated peerID=addr pairs, e.g. 2=localhost:8002,3=localhost:8003")
	fs.Parse(os.Args[1:])

	args := fs.Args()
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: raftctl [--addr=host:port] [--peers=id=addr,...] <put|get|delete> <key> [value]")
		os.Exit(1)
	}
	verb, key := args[0], args[1]

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "--peers: %v\n", err)
		os.Exit(1)
	}

	var attempt func(ctx context.Context, client pb.KVServiceClient) (kvResult, error)
	switch verb {
	case "put":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: raftctl put <key> <value>")
			os.Exit(1)
		}
		value := args[2]
		attempt = func(ctx context.Context, client pb.KVServiceClient) (kvResult, error) {
			resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: value})
			if err != nil {
				return kvResult{}, err
			}
			return kvResult{ok: resp.Success, leaderID: resp.LeaderId}, nil
		}
	case "delete":
		attempt = func(ctx context.Context, client pb.KVServiceClient) (kvResult, error) {
			resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: key})
			if err != nil {
				return kvResult{}, err
			}
			return kvResult{ok: resp.Success, leaderID: resp.LeaderId}, nil
		}
	case "get":
		attempt = func(ctx context.Context, client pb.KVServiceClient) (kvResult, error) {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				return kvResult{}, err
			}
			return kvResult{ok: resp.Found, value: resp.Value, leaderID: resp.LeaderId}, nil
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", verb)
		os.Exit(1)
	}

	result, err := runWithRedirects(*addr, peers, attempt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	switch verb {
	case "put", "delete":
		if !result.ok {
			fmt.Fprintln(os.Stderr, "operation did not succeed")
			os.Exit(1)
		}
		fmt.Println("OK")
	case "get":
		if !result.ok {
			fmt.Fprintln(os.Stderr, "not found")
			os.Exit(1)
		}
		fmt.Println(result.value)
	}
}

// runWithRedirects dials addr first and, on a redirect (leader_id != 0)
// to a known peer, retries there; on an unknown/zero leader_id or a
// transport-level RPC failure, it falls back to any other --peers
// address it hasn't tried yet. Bounded to len(peers)+1 attempts total —
// one shot at each address we could possibly know about — so it can
// never loop forever.
//
// A returned kvResult with a nil error is always a real, final answer
// (ok may still be false, e.g. a genuine not-found) — running out of
// attempts without ever reaching a non-redirecting node is reported as
// an error instead.
func runWithRedirects(addr string, peers map[int]string, attempt func(ctx context.Context, client pb.KVServiceClient) (kvResult, error)) (kvResult, error) {
	tried := map[string]bool{}
	queue := []string{addr}
	maxAttempts := len(peers) + 1
	var lastErr error

	for i := 0; i < maxAttempts && len(queue) > 0; i++ {
		target := queue[0]
		queue = queue[1:]
		if tried[target] {
			continue
		}
		tried[target] = true

		res, rpcErr := tryOnce(target, attempt)
		if rpcErr != nil {
			lastErr = fmt.Errorf("%s: %w", target, rpcErr)
			enqueueUntried(&queue, peers, tried)
			continue
		}

		if res.leaderID == 0 {
			return res, nil
		}

		// Redirected (or leader unknown to this node with leaderID==0
		// handled above): prefer the address the response actually named,
		// if we know it and haven't tried it yet.
		lastErr = fmt.Errorf("%s: redirected to leader %d", target, res.leaderID)
		if redirectAddr, ok := peers[int(res.leaderID)]; ok && !tried[redirectAddr] {
			queue = append([]string{redirectAddr}, queue...)
		} else {
			enqueueUntried(&queue, peers, tried)
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no addresses to try")
	}
	return kvResult{}, fmt.Errorf("exhausted all known addresses: %w", lastErr)
}

// tryOnce dials target and runs one attempt against it, bounded by
// rpcAttemptTimeout.
func tryOnce(target string, attempt func(ctx context.Context, client pb.KVServiceClient) (kvResult, error)) (kvResult, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return kvResult{}, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcAttemptTimeout)
	defer cancel()

	return attempt(ctx, pb.NewKVServiceClient(conn))
}

// enqueueUntried appends every peers address not already tried or already
// queued, in a deterministic (ascending peer ID) order.
func enqueueUntried(queue *[]string, peers map[int]string, tried map[string]bool) {
	ids := make([]int, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	queued := map[string]bool{}
	for _, a := range *queue {
		queued[a] = true
	}
	for _, id := range ids {
		addr := peers[id]
		if !tried[addr] && !queued[addr] {
			*queue = append(*queue, addr)
			queued[addr] = true
		}
	}
}

// parsePeers parses "id=addr,id=addr,..." into a map. An empty string
// yields an empty (non-nil) map. This mirrors internal/config's private
// parsePeers format, but that helper isn't exported (and has its own test
// coverage) — duplicating this trivial split is cheaper than widening
// that package's public surface for one caller.
func parsePeers(s string) (map[int]string, error) {
	peers := make(map[int]string)
	if s == "" {
		return peers, nil
	}
	for _, pair := range strings.Split(s, ",") {
		idAddr := strings.SplitN(pair, "=", 2)
		if len(idAddr) != 2 {
			return nil, fmt.Errorf("malformed peer entry %q, want id=addr", pair)
		}
		id, err := strconv.Atoi(idAddr[0])
		if err != nil {
			return nil, fmt.Errorf("malformed peer id %q: %w", idAddr[0], err)
		}
		peers[id] = idAddr[1]
	}
	return peers, nil
}
