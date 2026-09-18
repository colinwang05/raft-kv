package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Every test in this package spawns real OS processes and drives them
// through real gRPC with the project's real (unsped-up) election/heartbeat
// timing, so it's inherently slower than the rest of the suite. Gate it
// behind -short so routine `go test ./... -short` iteration stays fast.
func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("real-process integration test; skipped with -short")
	}
}

// TestThreeRealNodesElectExactlyOneLeader starts 3 real node processes and
// confirms exactly one of them believes it is leader within a reasonable
// deadline (design doc §18).
func TestThreeRealNodesElectExactlyOneLeader(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	leader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("findLeader: %v", err)
	}
	t.Logf("initial leader: node %d", leader.id)

	// Give the cluster a moment to settle, then check every node at once
	// so we're not just trusting findLeader's first hit.
	time.Sleep(500 * time.Millisecond)
	leaderCount := 0
	for _, nd := range c.nodes {
		res, err := runClient(nd.addr, "", "get", leaderProbeKey)
		if err != nil {
			t.Fatalf("probe node %d: %v", nd.id, err)
		}
		if isLeaderResult(res) {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 node to report itself leader, got %d", leaderCount)
	}
}

// TestPutSurvivesLeaderCrash: a PUT commits, the leader is SIGKILLed, a new
// leader is elected among the survivors, and the key is still readable
// from the new leader.
func TestPutSurvivesLeaderCrash(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	leader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("findLeader: %v", err)
	}

	if res, err := runClient(leader.addr, "", "put", "k1", "v1"); err != nil || res.exitCode != 0 {
		t.Fatalf("put k1: err=%v res=%+v", err, res)
	}
	if res, err := runClient(leader.addr, "", "get", "k1"); err != nil || res.exitCode != 0 || strings.TrimSpace(res.stdout) != "v1" {
		t.Fatalf("get k1 before crash: err=%v res=%+v", err, res)
	}

	oldLeaderID := leader.id
	if err := c.kill(leader); err != nil {
		t.Fatalf("kill leader: %v", err)
	}

	newLeader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("no new leader elected among survivors: %v", err)
	}
	if newLeader.id == oldLeaderID {
		t.Fatalf("findLeader returned the dead node %d", oldLeaderID)
	}
	t.Logf("new leader after crash: node %d", newLeader.id)

	// Raft's Figure-8 safety rule (already exercised by raft/restart_test.go
	// for the single-process case) means an entry from an *older* term
	// sitting in the new leader's log — k1 here — does not become committed
	// purely by virtue of a new leader existing; it's only implicitly
	// committed once the new leader commits an entry of its own current
	// term. A real client that needs to observe this without a read-index/
	// lease-read mechanism (out of scope here) issues a write first — so
	// this nudge write is the correct way to prove k1 survived, not a
	// workaround for a bug.
	if res, err := runClient(newLeader.addr, "", "put", "__nudge__", "1"); err != nil || res.exitCode != 0 {
		t.Fatalf("nudge put via new leader %d: err=%v res=%+v", newLeader.id, err, res)
	}

	if !waitFor(10*time.Second, 100*time.Millisecond, func() bool {
		res, err := runClient(newLeader.addr, "", "get", "k1")
		return err == nil && res.exitCode == 0 && strings.TrimSpace(res.stdout) == "v1"
	}) {
		t.Fatalf("k1 not readable from new leader %d after old leader %d crashed", newLeader.id, oldLeaderID)
	}
}

// TestFollowerCrashDoesNotBlockCommits: killing a follower (leader keeps a
// 2-of-3 majority) must not block new writes.
func TestFollowerCrashDoesNotBlockCommits(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	leader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("findLeader: %v", err)
	}
	follower := firstNonLeader(t, c, leader)

	if err := c.kill(follower); err != nil {
		t.Fatalf("kill follower %d: %v", follower.id, err)
	}

	if res, err := runClient(leader.addr, "", "put", "k2", "v2"); err != nil || res.exitCode != 0 {
		t.Fatalf("put k2 after follower %d crashed: err=%v res=%+v", follower.id, err, res)
	}
	if res, err := runClient(leader.addr, "", "get", "k2"); err != nil || res.exitCode != 0 || strings.TrimSpace(res.stdout) != "v2" {
		t.Fatalf("get k2 after follower %d crashed: err=%v res=%+v", follower.id, err, res)
	}
}

// TestRestartedFollowerCatchesUp: a follower is killed, writes continue via
// the remaining majority, and once the follower restarts against its same
// --data dir it catches up — proven via grepping its own captured stdout
// for "applied index=N" reaching the leader-confirmed index, per §5/§7's
// black-box observability technique (it never needs to become leader).
func TestRestartedFollowerCatchesUp(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	leader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("findLeader: %v", err)
	}
	follower := firstNonLeader(t, c, leader)

	if err := c.kill(follower); err != nil {
		t.Fatalf("kill follower %d: %v", follower.id, err)
	}

	for _, kv := range [][2]string{{"k3", "v3"}, {"k4", "v4"}} {
		if res, err := runClient(leader.addr, "", "put", kv[0], kv[1]); err != nil || res.exitCode != 0 {
			t.Fatalf("put %s while follower %d down: err=%v res=%+v", kv[0], follower.id, err, res)
		}
	}

	target := maxAppliedIndex(leader.logs())
	if target == 0 {
		t.Fatalf("leader %d has not logged applying anything yet", leader.id)
	}

	if err := c.restart(follower); err != nil {
		t.Fatalf("restart follower %d: %v", follower.id, err)
	}

	if !waitForAppliedIndex(follower, target, 10*time.Second) {
		t.Fatalf("follower %d did not catch up to applied index %d after restart", follower.id, target)
	}
}

// TestRestartedFormerLeaderBecomesFollowerAndConverges: the leader is
// killed, a new leader emerges and accepts a write, then the old leader is
// restarted against its same --data dir. A fresh restart always starts as
// Follower, and its restored term is behind the new leader's, so the first
// real AppendEntries it receives keeps it in step; we confirm it converges
// to the latest committed index via the same log-grepping technique.
func TestRestartedFormerLeaderBecomesFollowerAndConverges(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	leader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("findLeader: %v", err)
	}
	oldLeaderID := leader.id

	if err := c.kill(leader); err != nil {
		t.Fatalf("kill leader %d: %v", oldLeaderID, err)
	}

	newLeader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("no new leader elected: %v", err)
	}
	if newLeader.id == oldLeaderID {
		t.Fatalf("findLeader returned the dead node %d", oldLeaderID)
	}

	if res, err := runClient(newLeader.addr, "", "put", "k5", "v5"); err != nil || res.exitCode != 0 {
		t.Fatalf("put k5 via new leader %d: err=%v res=%+v", newLeader.id, err, res)
	}
	target := maxAppliedIndex(newLeader.logs())
	if target == 0 {
		t.Fatalf("new leader %d has not logged applying anything yet", newLeader.id)
	}

	oldLeaderNode := c.byID[oldLeaderID]
	if err := c.restart(oldLeaderNode); err != nil {
		t.Fatalf("restart old leader %d: %v", oldLeaderID, err)
	}

	if !waitForAppliedIndex(oldLeaderNode, target, 10*time.Second) {
		t.Fatalf("restarted former leader %d did not converge to applied index %d", oldLeaderID, target)
	}
}

// TestPausedNodeCatchesUpAfterResume: SIGSTOP simulates a follower going
// dark (peers' RPCs to it time out, approximating a network partition/
// delay without needing network-namespace tooling). Writes still commit
// via the other two while it's paused; once resumed it applies everything
// it missed.
func TestPausedNodeCatchesUpAfterResume(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	leader, err := c.findLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("findLeader: %v", err)
	}
	follower := firstNonLeader(t, c, leader)

	if res, err := runClient(leader.addr, "", "put", "k6", "v6"); err != nil || res.exitCode != 0 {
		t.Fatalf("put k6: err=%v res=%+v", err, res)
	}

	if err := c.pause(follower); err != nil {
		t.Fatalf("pause follower %d: %v", follower.id, err)
	}

	if res, err := runClient(leader.addr, "", "put", "k7", "v7"); err != nil || res.exitCode != 0 {
		t.Fatalf("put k7 while follower %d paused: err=%v res=%+v", follower.id, err, res)
	}
	target := maxAppliedIndex(leader.logs())
	if target == 0 {
		t.Fatalf("leader has not logged applying anything yet")
	}

	if err := c.resume(follower); err != nil {
		t.Fatalf("resume follower %d: %v", follower.id, err)
	}

	if !waitForAppliedIndex(follower, target, 10*time.Second) {
		t.Fatalf("paused-then-resumed follower %d did not catch up to applied index %d", follower.id, target)
	}
}

// TestRepeatedRandomKillRestartPreservesAcknowledgedWrites is the explicit
// chaos test from design doc §18: repeated random kill/restart cycles must
// never lose a write that raftctl actually reported as acknowledged.
//
// One deliberate deviation from a pure "always kill if up, always restart
// if down": the harness never lets the up-count drop below a majority
// (2 of 3). The design's failure scenarios (§16) are single-node-at-a-time
// (leader crash, follower crash) — a genuine simultaneous double failure
// would make the cluster correctly and expectedly unavailable, which would
// make the "previously-acknowledged key must stay readable" check flaky
// for a reason that has nothing to do with a real bug. Guarding against it
// is a root-cause fix for that flakiness, not a weakening of the test: it
// still does real repeated random kill/restart cycles across many rounds.
func TestRepeatedRandomKillRestartPreservesAcknowledgedWrites(t *testing.T) {
	skipIfShort(t)
	c := newCluster(t, 3)

	if _, err := c.findLeader(10 * time.Second); err != nil {
		t.Fatalf("findLeader (initial): %v", err)
	}

	rng := newRand()
	acknowledged := map[string]string{}
	const rounds = 10

	verifyAcknowledged := func(round int) {
		for k, v := range acknowledged {
			ok := waitFor(15*time.Second, 200*time.Millisecond, func() bool {
				got, ok := c.getFromCurrentLeader(k, 5*time.Second)
				return ok && got == v
			})
			if !ok {
				t.Fatalf("round %d: previously-acknowledged key %q=%q is no longer readable/correct", round, k, v)
			}
		}
	}

	for round := 0; round < rounds; round++ {
		var target *node
		if len(c.upNodes()) <= 2 {
			// Prioritize recovery over further attrition, to keep a
			// majority available (see doc comment above).
			target = pickByStatus(c, statusDown, rng)
		}
		if target == nil {
			target = c.nodes[rng.Intn(len(c.nodes))]
		}

		switch target.getStatus() {
		case statusUp:
			if err := c.kill(target); err != nil {
				t.Fatalf("round %d: kill node %d: %v", round, target.id, err)
			}
			t.Logf("round %d: killed node %d", round, target.id)
		case statusDown:
			if err := c.restart(target); err != nil {
				t.Fatalf("round %d: restart node %d: %v", round, target.id, err)
			}
			t.Logf("round %d: restarted node %d", round, target.id)
		case statusPaused:
			// Not used by this test's schedule, but handle it defensively.
			if err := c.resume(target); err != nil {
				t.Fatalf("round %d: resume node %d: %v", round, target.id, err)
			}
		}

		if round%2 == 0 {
			key := fmt.Sprintf("chaos-key-%d", round)
			value := fmt.Sprintf("chaos-value-%d", round)
			if c.putViaCurrentLeader(key, value, 5*time.Second) {
				acknowledged[key] = value
				t.Logf("round %d: acknowledged put %s=%s", round, key, value)
			} else {
				t.Logf("round %d: put %s not acknowledged (fine, not recorded)", round, key)
			}
		}

		verifyAcknowledged(round)
	}

	verifyAcknowledged(rounds)
	if len(acknowledged) == 0 {
		t.Fatalf("no key was ever acknowledged during the chaos run; test isn't exercising anything")
	}
	t.Logf("chaos run complete: %d acknowledged keys, all verified", len(acknowledged))
}

// firstNonLeader returns any node in c other than leader, failing the test
// if none exists.
func firstNonLeader(t *testing.T, c *cluster, leader *node) *node {
	t.Helper()
	for _, nd := range c.nodes {
		if nd.id != leader.id {
			return nd
		}
	}
	t.Fatalf("no non-leader node found (cluster of %d)", len(c.nodes))
	return nil
}

// pickByStatus returns a random node currently in status s, or nil if none.
func pickByStatus(c *cluster, s nodeStatus, rng interface{ Intn(int) int }) *node {
	var candidates []*node
	for _, nd := range c.nodes {
		if nd.getStatus() == s {
			candidates = append(candidates, nd)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[rng.Intn(len(candidates))]
}
