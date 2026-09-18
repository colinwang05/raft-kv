package integration

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colinwang05/raft-kv/internal/config"
)

// clientTimeout bounds one raftctl subprocess invocation, including any
// internal redirect retries across every known peer. Down nodes fail fast
// (connection refused), so in practice this is only ever approached when
// something is genuinely wrong; it's set generously per the project's
// guidance to prefer generous deadlines over tight ones in real-process
// tests.
const clientTimeout = 8 * time.Second

// leaderProbeKey is used to ask "are you the leader?" via a cheap Get that
// doesn't depend on any key actually existing (see findLeader).
const leaderProbeKey = "__integration_leader_probe__"

var appliedIndexRe = regexp.MustCompile(`applied index=(\d+)`)

// nodeStatus tracks what the harness believes about one node process. It's
// harness-tracked truth (not queried from the process), since the harness
// is the one doing the killing/pausing.
type nodeStatus int

const (
	statusDown nodeStatus = iota
	statusUp
	statusPaused
)

// node is one simulated cluster member: a real `cmd/server` subprocess
// bound to a fixed address and data directory across restarts.
type node struct {
	id        int
	addr      string
	dataDir   string
	peersFlag string // this node's own --peers value (every *other* node)

	mu     sync.Mutex
	cmd    *exec.Cmd
	status nodeStatus
	logBuf *syncBuffer
}

func (n *node) logs() string {
	return n.logBuf.String()
}

func (n *node) getStatus() nodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status
}

// syncBuffer is a concurrency-safe io.Writer/String() buffer: exec.Cmd
// copies a child's stdout and stderr from two separate goroutines, so a
// plain bytes.Buffer isn't safe here.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// procTracker records the PID of every process this harness has ever
// started, and force-kills all of them unconditionally on cleanup — a test
// that fails partway through must never leave an orphaned server process
// running. Registered via t.Cleanup before any process is started.
type procTracker struct {
	mu   sync.Mutex
	pids []int
}

func (p *procTracker) add(pid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pids = append(p.pids, pid)
}

func (p *procTracker) killAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pid := range p.pids {
		if proc, err := os.FindProcess(pid); err == nil {
			_ = proc.Kill() // SIGKILL; ignore "already finished" errors
		}
	}
}

// cluster is a set of real server node processes sharing an addressing
// table, plus the bookkeeping needed to fault-inject and clean them up.
type cluster struct {
	t       *testing.T
	nodes   []*node
	byID    map[int]*node
	tracker *procTracker
}

// newCluster allocates n nodes (dynamic free ports, per-node t.TempDir()
// data dirs), registers cleanup, and starts every node. Cleanup is
// registered before any process is spawned, per this project's own
// documented lesson about leftover orphaned processes from a prior
// milestone's manual testing.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, byID: map[int]*node{}, tracker: &procTracker{}}

	// Register cleanup FIRST, before anything that could fail or spawn a
	// process, so a failure partway through setup still cleans up whatever
	// did get started.
	t.Cleanup(c.tracker.killAll)
	t.Cleanup(func() {
		if t.Failed() {
			for _, nd := range c.nodes {
				t.Logf("=== node %d captured output (tail) ===\n%s", nd.id, tail(nd.logs(), 4000))
			}
		}
	})

	for i := 1; i <= n; i++ {
		port := freePort(t)
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		nd := &node{id: i, addr: addr, dataDir: t.TempDir(), logBuf: &syncBuffer{}}
		c.nodes = append(c.nodes, nd)
		c.byID[i] = nd
	}

	for _, nd := range c.nodes {
		var parts []string
		for _, other := range c.nodes {
			if other.id == nd.id {
				continue
			}
			parts = append(parts, fmt.Sprintf("%d=%s", other.id, other.addr))
		}
		nd.peersFlag = strings.Join(parts, ",")
	}

	for _, nd := range c.nodes {
		if err := c.startNode(nd); err != nil {
			t.Fatalf("start node %d: %v", nd.id, err)
		}
	}
	return c
}

// freePort grabs a free TCP port by binding to :0 and immediately closing
// the listener (standard Go testing pattern; small TOCTOU window accepted).
func freePort(t testing.TB) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

// startNode launches (or relaunches, for a restart) the server process for
// nd, appending to its existing captured-output buffer (a restart's log
// lines land right after its pre-crash ones, which is what the
// "applied index=" catch-up checks rely on).
func (c *cluster) startNode(nd *node) error {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	cmd := exec.Command(serverBin,
		fmt.Sprintf("--id=%d", nd.id),
		fmt.Sprintf("--addr=%s", nd.addr),
		fmt.Sprintf("--peers=%s", nd.peersFlag),
		fmt.Sprintf("--data=%s", nd.dataDir),
	)
	cmd.Stdout = nd.logBuf
	cmd.Stderr = nd.logBuf
	if err := cmd.Start(); err != nil {
		return err
	}
	c.tracker.add(cmd.Process.Pid)
	nd.cmd = cmd
	nd.status = statusUp
	return nil
}

// restart is startNode under a clearer name for call sites that are
// conceptually "bring this back up after it was killed."
func (c *cluster) restart(nd *node) error {
	if nd.getStatus() != statusDown {
		return fmt.Errorf("node %d is not down (status=%v), refusing to restart", nd.id, nd.getStatus())
	}
	return c.startNode(nd)
}

// kill SIGKILLs nd's process and reaps it. Safe to call on an
// already-down node (no-op).
func (c *cluster) kill(nd *node) error {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.status == statusDown || nd.cmd == nil {
		return nil
	}
	proc := nd.cmd.Process
	if err := proc.Kill(); err != nil {
		return fmt.Errorf("kill node %d: %w", nd.id, err)
	}
	_, _ = proc.Wait() // reap; exit status from a SIGKILL isn't interesting
	nd.status = statusDown
	return nil
}

// pause SIGSTOPs nd's process, simulating an unreachable/very slow node:
// peers' RPCs to it will simply time out against RPCTimeout, approximating
// a network partition/delay without needing any network-namespace tooling.
func (c *cluster) pause(nd *node) error {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.status != statusUp {
		return fmt.Errorf("node %d is not up (status=%v), cannot pause", nd.id, nd.status)
	}
	if err := nd.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		return fmt.Errorf("SIGSTOP node %d: %w", nd.id, err)
	}
	nd.status = statusPaused
	return nil
}

// resume SIGCONTs a previously-paused node.
func (c *cluster) resume(nd *node) error {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nd.status != statusPaused {
		return fmt.Errorf("node %d is not paused (status=%v), cannot resume", nd.id, nd.status)
	}
	if err := nd.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		return fmt.Errorf("SIGCONT node %d: %w", nd.id, err)
	}
	nd.status = statusUp
	return nil
}

// upNodes returns every node the harness currently believes is up
// (running and not paused).
func (c *cluster) upNodes() []*node {
	var out []*node
	for _, nd := range c.nodes {
		if nd.getStatus() == statusUp {
			out = append(out, nd)
		}
	}
	return out
}

// cliResult is one raftctl invocation's outcome.
type cliResult struct {
	stdout   string
	stderr   string
	exitCode int
}

// runClient invokes the real raftctl binary as a subprocess: --addr, and
// --peers only if peersFlag is non-empty (an empty --peers means "don't
// follow redirects," which findLeader relies on), then the verb/args.
func runClient(addr, peersFlag string, args ...string) (cliResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	full := []string{"--addr=" + addr}
	if peersFlag != "" {
		full = append(full, "--peers="+peersFlag)
	}
	full = append(full, args...)

	cmd := exec.CommandContext(ctx, clientBin, full...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	runErr := cmd.Run()

	res := cliResult{stdout: outBuf.String(), stderr: errBuf.String()}
	if cmd.ProcessState != nil {
		res.exitCode = cmd.ProcessState.ExitCode()
	} else {
		res.exitCode = -1
	}
	if runErr != nil {
		if _, ok := runErr.(*exec.ExitError); ok {
			return res, nil // a normal nonzero exit is not a harness-level error
		}
		return res, runErr // failed to start, context deadline, etc.
	}
	return res, nil
}

// isLeaderResult reports whether a raftctl `get <leaderProbeKey>` result
// (run with an empty --peers, so it never follows a redirect) proves the
// probed node currently believes it is the leader: either it actually
// answered (found or a genuine not-found), as opposed to reporting a
// redirect or failing to reach the node at all.
func isLeaderResult(res cliResult) bool {
	if res.exitCode == 0 {
		return true
	}
	return res.exitCode == 1 && strings.Contains(res.stderr, "not found")
}

// leaderConfirmGap is how long findLeader waits before re-checking a
// candidate leader (see the comment inside findLeader for why this is
// needed, not just a "poll faster/slower" tuning knob).
var leaderConfirmGap = config.DefaultElectionTimeoutMax + 2*config.DefaultHeartbeatInterval

// findLeader polls every node the harness believes is up, per §7's
// technique (an --addr-only, no-peers probe Get), until one answers as
// leader, retrying with backoff since elections take real time (300-600ms
// timeout range) and don't complete instantly after startup or a leader
// crash.
//
// A single probe is not enough to trust: LeaderId==0 in a GetResponse
// means two different things depending on who sends it — "I am the
// leader" (the true-leader case §7 wants), or "I am a follower who simply
// hasn't observed any leader yet" (only possible in the brief window
// right after a fresh cluster start, before the first election completes
// and its first heartbeat propagates). Both cases produce an identical
// exit-1/"not found" raftctl result. So a positive probe is only accepted
// once confirmed by a second probe of the *same* node after a settle gap
// long enough that any election in progress would have concluded and its
// winner's first heartbeat would have reached every follower — a
// transient false positive from that startup window won't still hold
// after this gap, since a real leader will have emerged and told this
// follower who it is (moving its LeaderId off 0), whereas a genuine
// leader's answer is stable and reproduces.
func (c *cluster) findLeader(timeout time.Duration) (*node, error) {
	deadline := time.Now().Add(timeout)
	for {
		for _, nd := range c.nodes {
			if nd.getStatus() != statusUp {
				continue
			}
			if !probeIsLeader(nd) {
				continue
			}
			time.Sleep(leaderConfirmGap)
			if probeIsLeader(nd) {
				return nd, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("no leader found within %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// probeIsLeader issues one no-redirect-following probe Get against nd and
// reports whether it answered as leader (see findLeader's doc comment for
// why one probe alone isn't trustworthy).
func probeIsLeader(nd *node) bool {
	res, err := runClient(nd.addr, "", "get", leaderProbeKey)
	if err != nil {
		return false
	}
	return isLeaderResult(res)
}

// nudge issues a cheap write via leaderAddr. Raft's Figure-8 safety rule
// (also exercised by raft/restart_test.go) means an older-term entry
// already sitting in a (possibly new) leader's log isn't committed just
// because that node is leader — only once the leader commits an entry of
// its own current term, which then implicitly commits everything before
// it. A real client without a read-index/lease-read mechanism (out of
// scope here) gets this same behavior; a nudge write is the correct way
// to force read-visibility of already-acknowledged data after a
// leadership change, not a workaround for a bug.
func (c *cluster) nudge(leaderAddr string) bool {
	res, err := runClient(leaderAddr, "", "put", "__nudge__", fmt.Sprintf("%d", time.Now().UnixNano()))
	return err == nil && res.exitCode == 0
}

// putViaCurrentLeader resolves the current leader (via findLeader, which
// is immune to raftctl's own redirect-chase ambiguity — see below) and
// puts key/value directly against it.
//
// This deliberately does not use raftctl's --peers redirect-following
// against an arbitrary starting address: a freshly-restarted node (never
// yet observed any leader) answers a Put it's not leader for with
// LeaderId==0 — identical, to the client, to "I am the leader with an
// empty response." raftctl's redirect-chase treats that as final and
// gives up instead of trying another peer. Resolving the leader ourselves
// first (findLeader's own double-check already defends against exactly
// this ambiguity) and talking to it directly sidesteps that entirely.
func (c *cluster) putViaCurrentLeader(key, value string, timeout time.Duration) bool {
	leader, err := c.findLeader(timeout)
	if err != nil {
		return false
	}
	res, err := runClient(leader.addr, "", "put", key, value)
	return err == nil && res.exitCode == 0
}

// getFromCurrentLeader resolves the current leader and reads key directly
// from it, nudging first so an older-term acknowledged write isn't
// mistaken for lost data during the window described in nudge's doc
// comment.
func (c *cluster) getFromCurrentLeader(key string, timeout time.Duration) (string, bool) {
	leader, err := c.findLeader(timeout)
	if err != nil {
		return "", false
	}
	if !c.nudge(leader.addr) {
		return "", false
	}
	res, err := runClient(leader.addr, "", "get", key)
	if err != nil || res.exitCode != 0 {
		return "", false
	}
	return strings.TrimSpace(res.stdout), true
}

// maxAppliedIndex scans captured node output for every "applied index=N"
// line this node has logged so far (raft/apply.go's per-entry log line)
// and returns the highest N seen, or 0 if none yet.
func maxAppliedIndex(logs string) uint64 {
	var max uint64
	for _, m := range appliedIndexRe.FindAllStringSubmatch(logs, -1) {
		v, err := strconv.ParseUint(m[1], 10, 64)
		if err == nil && v > max {
			max = v
		}
	}
	return max
}

// waitForAppliedIndex polls nd's captured output until it has logged
// applying at least `target` (see maxAppliedIndex), or the deadline
// elapses. This is the black-box way to observe "did this specific
// follower catch up" without querying it via the (deliberately
// leader-only) KV Get API.
func waitForAppliedIndex(nd *node, target uint64, timeout time.Duration) bool {
	return waitFor(timeout, 100*time.Millisecond, func() bool {
		return maxAppliedIndex(nd.logs()) >= target
	})
}

// waitFor polls cond every interval until it returns true or timeout
// elapses.
func waitFor(timeout, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// tail returns the last n bytes of s (as a string), prefixed with "..." if
// truncated. Used only for failure diagnostics.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "...(truncated)...\n" + s[len(s)-n:]
}

// newRand returns a *rand.Rand seeded from the current time, for the
// chaos test's random node selection.
func newRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}
