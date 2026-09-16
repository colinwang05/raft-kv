// Package config parses per-node startup configuration (id, address, peers,
// data directory, and Raft timing) from CLI flags.
package config

import "time"

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
	// TODO: define flag.Int/flag.String flags for id, addr, data;
	// parse the comma-separated "id=addr" pairs in --peers into Peers;
	// fill in HeartbeatInterval/ElectionTimeout*/RPCTimeout defaults.
	return nil, nil
}
