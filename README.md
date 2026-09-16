# raft-kv

A fault-tolerant replicated key-value store using Raft consensus. See
`raft_kv_design (1).docx` for the full design (architecture, RPC contract,
invariants, milestones).

This is a **framework scaffold**: package layout, types, and RPC contracts
are in place; Raft logic itself (`// TODO` markers throughout `raft/` and
`storage/`) is not yet implemented. Follow the milestone order below.

M0 (config parsing, gRPC wiring), M1 (leader election), M2 (heartbeats +
the full `AppendEntries` receiver, which also satisfies M3's log-
consistency rules since it's the same RPC handler), and M3 (leader-side
log replication: `Propose` appends a command to the leader's own log,
`replicateTo`/`AppendEntries` carry the real `Command` payload over the
wire via a small gob codec, and `maybeAdvanceCommitIndexLocked` advances
`commitIndex` from a majority of `matchIndex` — honoring the Raft rule
that a leader only ever directly commits an entry from its own current
term) are implemented and covered by tests — `go test ./...` is fully
green. Producing real commands from client writes and applying committed
entries to the KV state machine (M4's job) is still outstanding.

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

## Milestones

| Milestone | Definition of done |
|---|---|
| M0 - Skeleton | Go module, config parsing, three server processes, gRPC connectivity, structured logs. |
| M1 - Election | Follower/candidate/leader states, randomized timeout, RequestVote, stable leader election. |
| M2 - Heartbeats | Leader sends empty AppendEntries; followers reset timeout; stale leaders step down. |
| M3 - Log replication | Append commands, prevLog consistency check, nextIndex/matchIndex, majority commit. |
| M4 - KV API | PUT/DELETE through Raft, leader GET, apply loop, client redirect/retry behavior. |
| M5 - Persistence | Durable term/vote/log, crash recovery, restart tests. |
| M6 - Fault testing | Kill/restart scripts, delayed RPCs, repeated failover tests, invariants. |
| M7 - Packaging | Dockerfile + Docker Compose, README demo, benchmark harness. |
| M8 - Stretch | Snapshots/log compaction, conflict-index optimization, metrics, sharding. |

Recommended immediate boundary: implement only M0-M2 first (see design doc
section 24).
