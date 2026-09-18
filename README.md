# raft-kv

A fault-tolerant replicated key-value store using Raft consensus. See
`raft_kv_design (1).docx` for the full design (architecture, RPC contract,
invariants, milestones).

This is a **framework scaffold**: package layout, types, and RPC contracts
are in place; Raft logic itself (`// TODO` markers throughout `raft/` and
`storage/`) is not yet implemented. Follow the milestone order below.

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
recovers its term/vote/log from disk) are implemented and covered by
tests — `go test ./...` is fully green.

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
- Real-process integration tests (multi-node kill/restart, network delay,
  failover) are deferred to M6 per the doc's own milestone split — the
  loopback tests above give fast unit-level coverage of the same safety
  properties in the meantime.
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

## Milestones

| Milestone | Definition of done |
|---|---|
| M0 - Skeleton | Go module, config parsing, three server processes, gRPC connectivity, structured logs. |
| M1 - Election | Follower/candidate/leader states, randomized timeout, RequestVote, stable leader election. |
| M2 - Heartbeats | Leader sends empty AppendEntries; followers reset timeout; stale leaders step down. |
| M3 - Log replication | Append commands, prevLog consistency check, nextIndex/matchIndex, majority commit. |
| M4 - KV API | PUT/DELETE through Raft, leader GET, apply loop, client redirect/retry behavior. |
| M5 - Persistence | Durable term/vote/log, crash recovery, restart tests. **Done.** |
| M6 - Fault testing | Kill/restart scripts, delayed RPCs, repeated failover tests, invariants. |
| M7 - Packaging | Dockerfile + Docker Compose, README demo, benchmark harness. |
| M8 - Stretch | Snapshots/log compaction, conflict-index optimization, metrics, sharding. |

Recommended immediate boundary: implement only M0-M2 first (see design doc
section 24).
