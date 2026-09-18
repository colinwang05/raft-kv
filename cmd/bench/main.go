// Command bench (raft-bench) drives load and failure-window measurements
// against a running raft-kv cluster (started locally or via
// docker compose) to get the numbers the design doc's resume target
// (§23) calls for: sustained ops/sec, and time to recover from a leader
// failure. It never starts or kills nodes itself — pair it with a cluster
// you already brought up (see repo root README) and, for the failover
// mode, a terminal where you kill/restart the leader yourself.
//
// Usage (mode comes first; flags after it, standard Go flag-package
// behavior — FlagSet.Parse stops at the first non-flag argument):
//
//	raft-bench throughput --peers=id=addr,... [--addr=host:port] [--workers=N] [--duration=10s] [--read-pct=0] [--value-size=64] [--key-space=1000]
//	raft-bench failover   --peers=id=addr,... [--addr=host:port] [--duration=30s] [--interval=20ms]
//
// throughput runs N concurrent workers issuing PUT (and, with
// --read-pct>0, a mix of GET) requests for --duration and reports
// ops/sec plus latency percentiles. failover continuously PUTs while you
// manually kill (or `docker compose kill`/`stop`) the current leader in
// another terminal, then reports the resulting client-visible
// unavailability window.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	mode := os.Args[1]

	fs := flag.NewFlagSet("raft-bench "+mode, flag.ExitOnError)
	addr := fs.String("addr", "localhost:8001", "address of the node to try first")
	peersFlag := fs.String("peers", "", "comma-separated peerID=addr pairs, e.g. 1=localhost:8001,2=localhost:8002,3=localhost:8003")

	switch mode {
	case "throughput":
		workers := fs.Int("workers", 10, "concurrent client workers")
		duration := fs.Duration("duration", 10*time.Second, "how long to generate load")
		readPct := fs.Int("read-pct", 0, "percentage (0-100) of ops that are GET instead of PUT")
		valueSize := fs.Int("value-size", 64, "size in bytes of each PUT's value")
		keySpace := fs.Int("key-space", 1000, "number of distinct keys cycled through")
		fs.Parse(os.Args[2:])

		peers := mustParsePeers(*peersFlag)
		if *readPct < 0 || *readPct > 100 {
			fmt.Fprintln(os.Stderr, "--read-pct must be between 0 and 100")
			os.Exit(1)
		}
		if *keySpace < 1 {
			fmt.Fprintln(os.Stderr, "--key-space must be at least 1")
			os.Exit(1)
		}
		runThroughput(throughputConfig{
			seedAddr: *addr, peers: peers, workers: *workers, duration: *duration,
			readPct: *readPct, valueSize: *valueSize, keySpace: *keySpace,
		})

	case "failover":
		duration := fs.Duration("duration", 30*time.Second, "how long to probe for")
		interval := fs.Duration("interval", 20*time.Millisecond, "delay between probes (bounds measurement resolution)")
		fs.Parse(os.Args[2:])

		peers := mustParsePeers(*peersFlag)
		runFailover(failoverConfig{seedAddr: *addr, peers: peers, duration: *duration, interval: *interval})

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raft-bench <throughput|failover> --peers=id=addr,... [--addr=host:port] [flags]")
}

func mustParsePeers(s string) map[int]string {
	peers, err := parsePeers(s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "--peers: %v\n", err)
		os.Exit(1)
	}
	return peers
}

// parsePeers parses "id=addr,id=addr,..." into a map. Mirrors cmd/client's
// identical private helper — small enough that sharing it isn't worth
// widening either package's public surface for one caller.
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
