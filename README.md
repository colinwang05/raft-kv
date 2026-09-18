# raft-kv

A fault-tolerant replicated key-value store using Raft consensus.

M0 (config parsing, gRPC wiring), M1 (leader election), M2 (heartbeats +
the full `AppendEntries` receiver, which also satisfies M3's log-
consistency rules since it's the same RPC handler), M3 (leader-side
log replication: `Propose` appends a command to the leader's own log,
`replicateTo`/`AppendEntries` carry the real `Command` payload over the
wire via a small gob codec, and `maybeAdvanceCommitIndexLocked` advances
`commitIndex` from a majority of `matchIndex` — honoring the Raft rule
that a leader only ever directly commits an entry from its own current
term), and M4 (the KV API: `storage.KVStore` is a real in-memory map;
`raft.runApplyLoop` applies committed entries to it in order via the new
`Applier` interface; `cmd/server`'s `kvServer` wires `KVService`'s
Put/Delete through `Node.Propose` + `Node.WaitApplied`, and serves Get
directly from the leader's own store — a follower reports `leader_id` so
the caller can redirect; `raftctl` (`cmd/client`) now actually issues
these RPCs and follows `leader_id` redirects via a `--peers` address
table), and M5 (persistence: `storage.WAL` durably persists currentTerm/
votedFor (combined into one atomic `SaveState` write, since a vote is only
meaningful in the context of the term it was cast in) and the full Raft
log (`PersistLog` atomically rewrites the whole log file via temp-file +
fsync + rename on every call that actually changes it — a `raft.Persister`
interface keeps `raft` decoupled from `storage`, same pattern as
`Applier`); `cmd/server/main.go` loads persisted state and calls
`Node.RestoreState` before `Start()`, so a killed and restarted node
recovers its term/vote/log from disk), and M6 (fault testing: a new
`integration/` package drives real `cmd/server`/`raftctl` OS processes
over real gRPC — leader/follower crash, restart-and-catch-up, SIGSTOP/
SIGCONT-simulated network delay, and repeated random kill/restart cycles,
per the design doc's §16/§18 failure scenarios) are implemented and
covered by tests — `go test ./...` is fully green. M7 (packaging) is in
progress: `cmd/bench` (`raft-bench`), a standalone load-generating/
failover-timing client, is done — see "## Benchmarking" below for usage
and measured numbers; Dockerfile/Compose and the README demo walkthrough
are still to come.

## Generate protobuf/gRPC code

Required once before `go build ./...` will succeed, since `raft/transport.go`
and `cmd/*` reference generated types.

```sh
brew install go protobuf grpcurl
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc --go_out=. --go-grpc_out=. proto/raft.proto
```

## Run three local nodes

```sh
go run ./cmd/server --id=1 --addr=localhost:8001 \
  --peers=2=localhost:8002,3=localhost:8003 --data=./data/node1
go run ./cmd/server --id=2 --addr=localhost:8002 \
  --peers=1=localhost:8001,3=localhost:8003 --data=./data/node2
go run ./cmd/server --id=3 --addr=localhost:8003 \
  --peers=1=localhost:8001,2=localhost:8002 --data=./data/node3
```

## Benchmarking

`cmd/bench` (`raft-bench`) is a standalone client — it never starts or
kills nodes itself — that drives load against a cluster you already
brought up (the three local nodes above, or a `docker compose` cluster
once M7 adds one) to get real numbers instead of guessing them:

```sh
go build -o raft-bench ./cmd/bench

# sustained throughput + latency percentiles, 20 concurrent writers for 10s
./raft-bench throughput --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --workers=20 --duration=10s

# leader-failure recovery window: run this, then in another terminal kill
# (or docker compose kill/stop) whichever node is currently the leader
./raft-bench failover --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --duration=30s
```

`throughput` runs N concurrent workers issuing PUTs (add `--read-pct` for
a read mix) and reports ops/sec plus p50/p95/p99/max latency. `failover`
probes with PUT every `--interval` (default 20ms) and reports the longest
run of failed probes as a `[min, max]` bound on the actual outage — min is
the span between the first and last failed probe, max extends to the
nearest successful probes on either side, so true recovery time lies
in between, resolution-limited by `--interval`.

Measured on a single dev machine (all three nodes as local processes on
loopback, default `HeartbeatInterval`/election-timeout config, `--workers
20 --duration 10s`, values default 64B):

- **Throughput: ~110-120 ops/sec**, p50 latency ~170ms. A single writer
  alone gets ~100ms p50 — almost exactly `DefaultHeartbeatInterval`
  (100ms, `internal/config/config.go`). That's not a fluke: `raft/
  replication.go`'s replication loop only sends `AppendEntries` on a
  fixed heartbeat ticker rather than immediately when `Propose` appends a
  new entry, so a write's commit latency is dominated by waiting for the
  next tick rather than by the actual network/fsync cost. Triggering
  replication immediately on `Propose` (in addition to the periodic
  heartbeat, which still needs to exist for idle keepalive/step-down
  detection) would be the highest-leverage throughput fix, and is a good
  M8 candidate — out of scope for the M7 benchmark harness itself, which
  is deliberately just the measurement tool.
- **Failover: ~400-600ms** client-visible unavailability window after a
  hard `kill -9` of the leader, consistent with one election timeout plus
  the time for the new leader's first heartbeat round to reach quorum.

These are small-scale, single-machine numbers meant to characterize the
current implementation, not a production benchmark — rerun both commands
yourself for numbers that reflect your hardware and current code.

## Testing

```sh
go test ./... -race
```

- `internal/config`: flag parsing and validation (`--id`/`--addr`/`--peers`/`--data`).
- `raft`: `RequestVote` handler unit tests covering the design doc's
  invariants (§21) — vote granted at most once per term, log-freshness
  comparison, term never decreases, immediate step-down on a higher term —
  plus a multi-node election safety test (`TestElectionSafety_AtMostOneLeaderPerTerm`)
  using an in-memory loopback `RaftClient` (peer RPCs call the peer `Node`'s
  handlers directly, no gRPC/network) to run real concurrent elections
  deterministically and assert no term ever has two leaders.
- Real-process integration/chaos tests (multi-node kill/restart, network
  delay, failover) are implemented in `integration/` (M6) — see below;
  the loopback test above gives fast, deterministic unit-level coverage
  of the same election-safety property in the meantime/in addition.
- `raft/replication_test.go` (M2/M3, `TestAppendEntries_*`): was written
  ahead of the implementation as the spec for `AppendEntries` — stale-term
  rejection, heartbeat semantics, election-timer reset, step-down on a
  higher/same-term leader, prevLogTerm consistency checks, conflicting-
  suffix removal, and commit-index advancement (never decreasing, always
  `min(leaderCommit, lastNewEntryIndex)`). Now passing.
- `raft/replication_test.go` (M3, leader-side replication): `Propose` on a
  non-leader vs. a leader (index/term correctness, log growth), a
  round-trip test for the `encodeCommand`/`decodeCommand` gob codec, and
  `maybeAdvanceCommitIndexLocked` coverage including the Raft §5.4.2 safety
  case — a majority-replicated older-term entry must *not* become
  committed, only a current-term entry (which then implicitly commits
  everything before it) — plus an end-to-end test on a real 3-`Node`
  loopback cluster that proposes a command on a manually-installed leader,
  drives `replicateTo` to both followers, and asserts the entry reaches
  `commitIndex` with its `Command` correctly decoded on a follower's log.
- `storage/kv_test.go` and `raft/apply_test.go` (M4): `KVStore` Get/Put/
  Delete/Apply dispatch; `runApplyLoop` applies committed entries strictly
  in order and advances `lastApplied` (including with a `nil` `Applier`);
  `WaitApplied` returns promptly once applied, and `false` on a timed-out
  context.
- `cmd/server/kvserver_test.go` (M4): a real single-node `*raft.Node` (zero
  peers, so it elects itself immediately) exercised through the actual
  `kvServer.Put`/`Get`/`Delete` handlers — round-trip, delete-then-not-found,
  and the not-leader redirect path (forced via a real higher-term
  `AppendEntries` call, matching what a legitimate peer would send).
- `storage/wal_test.go` (M5): `SaveState`/`PersistLog` round-tripped
  through a *fresh* `WAL` instance pointed at the same directory
  (simulating a restart), including real `Command` payloads, not just
  Index/Term; empty-directory first-boot behavior (zero value/`nil`, no
  error); and a second `PersistLog` call with fewer entries than the first
  (simulating a conflicting-suffix truncation) fully replacing the log
  rather than leaving a stale mix.
- `raft/persist_test.go` (M5, persistence-behavior/rollback, using a
  `fakePersister` test double): `RequestVote` persists before granting,
  and rolls back `votedFor` + denies the vote (with no permanent lockout —
  proven by a subsequent grant to a different candidate) when persistence
  fails; `AppendEntries` persists the full post-merge log only when the
  log actually changed (asserted absent on a plain heartbeat, present on a
  conflict-only truncation with zero new entries) and rolls back the log +
  returns `Success: false` on a persist failure; `startElection` bumps
  term/vote in memory even when persistence fails, without rolling back,
  and sends no RequestVote RPCs in that case (observed via a counting
  loopback-style peer).
- `raft/restart_test.go` (M5, end-to-end, `package raft_test` to avoid the
  raft/storage import cycle): a real single-node `*raft.Node` backed by a
  real `*storage.WAL` and `*storage.KVStore` proposes and applies one
  command, then a simulated restart (new `Node`+`WAL` over the same
  directory, new empty `KVStore`, `RestoreState` loaded exactly as
  `cmd/server/main.go` does) re-elects itself, proposes a second command,
  and asserts the KVStore ends up with both the pre- and post-restart data
  — proving the old entry was durably persisted, correctly reloaded, and
  correctly gets implicitly committed (per the Raft §5.4.2/Figure-8 rule)
  once a current-term entry also commits, rather than auto-committing on
  restart alone (which would be incorrect).
- `integration/` (M6, `package integration`, real processes/real gRPC):
  unlike every test above, this drives actual `cmd/server` OS processes
  over real TCP/gRPC, through the real `raftctl` (`cmd/client`) binary as
  a subprocess (so it re-validates the actual CLI, not a hand-rolled test
  client) rather than the in-memory loopback transport. `TestMain` builds
  both binaries once into a temp dir; each test spins up a 3-node cluster
  on dynamic free ports with per-node `t.TempDir()` data dirs, and tracks
  every spawned PID so `t.Cleanup` can force-kill them even if a test
  fails partway through. It uses the project's real (unsped-up) timing
  defaults — no test-only config flags — so it's noticeably slower than
  the rest of the suite; run it on its own with:
  ```sh
  go test ./integration/... -v
  ```
  It's gated behind `testing.Short()` (skipped by `go test ./... -short`)
  so routine iteration stays fast; `go test ./...` (no `-short`) runs it.
  Covers design doc §18's list: exactly one stable leader among 3 real
  nodes; a PUT surviving a leader crash (readable from the new leader
  once it commits an entry of its own term, per the Figure-8 rule above);
  a follower crash not blocking commits; a killed-and-restarted follower
  catching up; a restarted former leader rejoining as a follower and
  converging; a `SIGSTOP`/`SIGCONT`-paused node (simulating an unreachable/
  slow peer without any network-namespace tooling) catching up once
  resumed; and repeated random kill/restart cycles across many rounds
  never losing a write `raftctl` actually reported as acknowledged. A
  dedicated real-process split-vote test is deliberately not included —
  `raft.TestElectionSafety_AtMostOneLeaderPerTerm` already proves that
  safety property deterministically over the loopback transport, and
  forcing a genuine split vote with real OS-level timing would be flaky
  for little additional coverage.
  Two small additive logging changes support this package's black-box
  observability technique — asking "did this specific node apply up to
  index N" by grepping its captured stdout, since `KVService.Get` is
  deliberately leader-only and no debug RPC was added for this: a follower's
  own `commitIndex` advance in `AppendEntries` (`raft/replication.go`) now
  logs `"commit advanced old=%d new=%d"`, matching the leader-side line
  that already existed; and the apply loop (`raft/apply.go`) now logs
  `"applied index=%d"` per entry it applies. Neither changes any return
  value or control flow.

## Milestones

| Milestone | Definition of done |
|---|---|
| M0 - Skeleton | Go module, config parsing, three server processes, gRPC connectivity, structured logs. |
| M1 - Election | Follower/candidate/leader states, randomized timeout, RequestVote, stable leader election. |
| M2 - Heartbeats | Leader sends empty AppendEntries; followers reset timeout; stale leaders step down. |
| M3 - Log replication | Append commands, prevLog consistency check, nextIndex/matchIndex, majority commit. |
| M4 - KV API | PUT/DELETE through Raft, leader GET, apply loop, client redirect/retry behavior. |
| M5 - Persistence | Durable term/vote/log, crash recovery, restart tests. **Done.** |
| M6 - Fault testing | Kill/restart scripts, delayed RPCs, repeated failover tests, invariants. **Done.** |
| M7 - Packaging | Dockerfile + Docker Compose, README demo, benchmark harness. **In progress** (benchmark harness done, see "## Benchmarking"; Dockerfile/Compose/demo still to come). |
| M8 - Stretch | Snapshots/log compaction, conflict-index optimization, metrics, sharding. |

Recommended immediate boundary: implement only M0-M2 first (see design doc
section 24).
