package config

import "testing"

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    map[int]string
		wantErr bool
	}{
		{name: "empty", in: "", want: map[int]string{}},
		{
			name: "single",
			in:   "2=localhost:8002",
			want: map[int]string{2: "localhost:8002"},
		},
		{
			name: "multiple",
			in:   "2=localhost:8002,3=localhost:8003",
			want: map[int]string{2: "localhost:8002", 3: "localhost:8003"},
		},
		{name: "missing equals", in: "2-localhost:8002", wantErr: true},
		{name: "non-numeric id", in: "x=localhost:8002", wantErr: true},
		{name: "trailing comma", in: "2=localhost:8002,", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePeers(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePeers(%q) = %v, nil; want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePeers(%q) unexpected error: %v", tt.in, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parsePeers(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("parsePeers(%q)[%d] = %q, want %q", tt.in, k, got[k], v)
				}
			}
		})
	}
}

func TestParseArgs(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cfg, err := parseArgs("server", []string{
			"--id=1",
			"--addr=localhost:8001",
			"--peers=2=localhost:8002,3=localhost:8003",
			"--data=./data/node1",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ID != 1 {
			t.Errorf("ID = %d, want 1", cfg.ID)
		}
		if cfg.Addr != "localhost:8001" {
			t.Errorf("Addr = %q, want localhost:8001", cfg.Addr)
		}
		if cfg.DataDir != "./data/node1" {
			t.Errorf("DataDir = %q, want ./data/node1", cfg.DataDir)
		}
		if len(cfg.Peers) != 2 || cfg.Peers[2] != "localhost:8002" || cfg.Peers[3] != "localhost:8003" {
			t.Errorf("Peers = %v, want {2:localhost:8002 3:localhost:8003}", cfg.Peers)
		}
		if cfg.HeartbeatInterval != DefaultHeartbeatInterval {
			t.Errorf("HeartbeatInterval = %v, want default %v", cfg.HeartbeatInterval, DefaultHeartbeatInterval)
		}
		if cfg.ElectionTimeoutMin != DefaultElectionTimeoutMin || cfg.ElectionTimeoutMax != DefaultElectionTimeoutMax {
			t.Errorf("election timeouts = [%v,%v], want defaults [%v,%v]",
				cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax, DefaultElectionTimeoutMin, DefaultElectionTimeoutMax)
		}
		if cfg.RPCTimeout != DefaultRPCTimeout {
			t.Errorf("RPCTimeout = %v, want default %v", cfg.RPCTimeout, DefaultRPCTimeout)
		}
	})

	t.Run("missing id", func(t *testing.T) {
		if _, err := parseArgs("server", []string{"--addr=localhost:8001", "--data=./data"}); err == nil {
			t.Fatal("expected error for missing --id, got nil")
		}
	})

	t.Run("missing addr", func(t *testing.T) {
		if _, err := parseArgs("server", []string{"--id=1", "--data=./data"}); err == nil {
			t.Fatal("expected error for missing --addr, got nil")
		}
	})

	t.Run("missing data", func(t *testing.T) {
		if _, err := parseArgs("server", []string{"--id=1", "--addr=localhost:8001"}); err == nil {
			t.Fatal("expected error for missing --data, got nil")
		}
	})

	t.Run("malformed peers", func(t *testing.T) {
		_, err := parseArgs("server", []string{
			"--id=1", "--addr=localhost:8001", "--data=./data", "--peers=garbage",
		})
		if err == nil {
			t.Fatal("expected error for malformed --peers, got nil")
		}
	})

	t.Run("no peers is valid (single-node cluster)", func(t *testing.T) {
		cfg, err := parseArgs("server", []string{"--id=1", "--addr=localhost:8001", "--data=./data"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Peers) != 0 {
			t.Errorf("Peers = %v, want empty", cfg.Peers)
		}
	})

	t.Run("callable repeatedly without panicking", func(t *testing.T) {
		// Regression guard: ParseFlags must not register flags on the
		// package-level flag.CommandLine, or a second call in the same
		// process (as every subsequent test case here does) would panic
		// with "flag redefined".
		for i := 0; i < 3; i++ {
			if _, err := parseArgs("server", []string{"--id=1", "--addr=a", "--data=d"}); err != nil {
				t.Fatalf("call %d: unexpected error: %v", i, err)
			}
		}
	})
}
