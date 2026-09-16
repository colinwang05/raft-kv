// Package config parses per-node startup configuration (id, address, peers,
// data directory, and Raft timing) from CLI flags.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default timing values (design doc section 14).
const (
	DefaultHeartbeatInterval  = 100 * time.Millisecond
	DefaultElectionTimeoutMin = 300 * time.Millisecond
	DefaultElectionTimeoutMax = 600 * time.Millisecond
	DefaultRPCTimeout         = 300 * time.Millisecond
)

// Config holds one server process's startup parameters.
type Config struct {
	ID      int
	Addr    string
	Peers   map[int]string // peer node ID -> gRPC address
	DataDir string

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
}

// ParseFlags parses os.Args into a Config, matching the CLI shape:
//
//	--id=1 --addr=localhost:8001 --peers=2=localhost:8002,3=localhost:8003 --data=./data/node1
func ParseFlags() (*Config, error) {
	return parseArgs(os.Args[0], os.Args[1:])
}

// parseArgs is the testable core of ParseFlags: it takes an explicit
// argument slice and uses a fresh FlagSet per call, so (unlike registering
// flags on the package-level flag.CommandLine) it can be called repeatedly
// — e.g. once per test case — without panicking on redefined flags.
func parseArgs(progName string, args []string) (*Config, error) {
	fs := flag.NewFlagSet(progName, flag.ContinueOnError)
	id := fs.Int("id", 0, "this node's numeric ID (required)")
	addr := fs.String("addr", "", "address this node listens on, e.g. localhost:8001 (required)")
	peers := fs.String("peers", "", "comma-separated peerID=addr pairs, e.g. 2=localhost:8002,3=localhost:8003")
	dataDir := fs.String("data", "", "directory for this node's WAL/metadata (required)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if *id == 0 {
		return nil, fmt.Errorf("--id is required")
	}
	if *addr == "" {
		return nil, fmt.Errorf("--addr is required")
	}
	if *dataDir == "" {
		return nil, fmt.Errorf("--data is required")
	}

	peerMap, err := parsePeers(*peers)
	if err != nil {
		return nil, fmt.Errorf("--peers: %w", err)
	}

	return &Config{
		ID:      *id,
		Addr:    *addr,
		Peers:   peerMap,
		DataDir: *dataDir,

		HeartbeatInterval:  DefaultHeartbeatInterval,
		ElectionTimeoutMin: DefaultElectionTimeoutMin,
		ElectionTimeoutMax: DefaultElectionTimeoutMax,
		RPCTimeout:         DefaultRPCTimeout,
	}, nil
}

// parsePeers parses "id=addr,id=addr,..." into a map. An empty string
// yields an empty (non-nil) map.
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
		peerID, err := strconv.Atoi(idAddr[0])
		if err != nil {
			return nil, fmt.Errorf("malformed peer id %q: %w", idAddr[0], err)
		}
		peers[peerID] = idAddr[1]
	}
	return peers, nil
}
