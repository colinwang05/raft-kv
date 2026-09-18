// Package integration holds real-process, real-gRPC fault-injection tests
// (M6 per the design doc's milestone table). Unlike raft's loopback-based
// tests, everything here talks to actual `cmd/server` OS processes over a
// real TCP/gRPC connection, and drives them through the actual `cmd/client`
// (raftctl) binary rather than a hand-rolled gRPC client — see README.md's
// "## Testing" section for the rationale.
//
// This package is intentionally a fresh, black-box `package integration`:
// it needs no access to any unexported internals of raft/storage.
package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// serverBin and clientBin are populated by TestMain before any test runs,
// pointing at freshly built `cmd/server`/`cmd/client` binaries in a temp
// directory.
var (
	serverBin string
	clientBin string
)

// TestMain builds the two binaries these tests exercise exactly once (not
// `go run`, which recompiles per invocation and complicates process/PID
// tracking) before running any test in this package.
func TestMain(m *testing.M) {
	code, err := buildAndRun(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration TestMain:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func buildAndRun(m *testing.M) (int, error) {
	tmpDir, err := os.MkdirTemp("", "raftkv-integration-bin")
	if err != nil {
		return 0, fmt.Errorf("mkdtemp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	moduleRoot, err := findModuleRoot()
	if err != nil {
		return 0, err
	}

	// This environment doesn't reliably have `go` on PATH, and a test
	// subprocess isn't guaranteed to inherit whatever resolved `go test`
	// itself — locate the toolchain via GOROOT instead.
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(goBin); err != nil {
		return 0, fmt.Errorf("go toolchain not found at %s: %w", goBin, err)
	}

	serverBin = filepath.Join(tmpDir, "raft-kv-server")
	clientBin = filepath.Join(tmpDir, "raftctl")

	if err := buildBinary(goBin, moduleRoot, "./cmd/server", serverBin); err != nil {
		return 0, err
	}
	if err := buildBinary(goBin, moduleRoot, "./cmd/client", clientBin); err != nil {
		return 0, err
	}

	return m.Run(), nil
}

// buildBinary runs `go build -o outPath pkg` with cwd set to moduleRoot, so
// it works regardless of what directory `go test` happened to invoke this
// package from.
func buildBinary(goBin, moduleRoot, pkg, outPath string) error {
	cmd := exec.Command(goBin, "build", "-o", outPath, pkg)
	cmd.Dir = moduleRoot
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go build %s: %w\n%s", pkg, err, out)
	}
	return nil
}

// findModuleRoot walks up from the current working directory (the
// integration/ package dir, when run via `go test`) looking for go.mod.
func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found walking up from %s", dir)
		}
		dir = parent
	}
}
