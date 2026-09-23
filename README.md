# raft-kv

A distributed key-value store built on Raft consensus — three (soon 3-5)
independent Go processes that elect a leader, replicate a command log,
and apply it to an in-memory KV store. Kill the leader mid-write and the
cluster keeps serving correct data from whoever gets elected next.

It's a from-scratch implementation (no consensus library), built as a
learning project — but it's the real thing: real gRPC between nodes,
real disk persistence, and a test suite that actually kills and restarts
OS processes to prove failover works.

**Where it's at:** leader election, log replication, the KV API,
persistence, real fault-injection tests, and Docker packaging are all
done (M0-M7 below). Next up, if this keeps going, is snapshotting and a
throughput optimization — see "## Roadmap" for the full picture.

## Quick start

Easiest path is Docker — nothing else to install.

1. Bring up a 3-node cluster:
   ```sh
   docker compose up -d
   docker compose logs | grep LEADER   # see who won the election
   ```
2. Build the CLI and write something:
   ```sh
   go build -o raftctl ./cmd/client
   ./raftctl --addr=localhost:8001 --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
     put name Colin
   ```
3. Kill whichever node is currently leader, and read the value back from
   whoever wins the re-election:
   ```sh
   docker compose kill node2   # substitute the actual leader
   ./raftctl --addr=localhost:8001 --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
     get name
   # -> Colin
   ```
4. Bring the killed node back — it rejoins and catches up on its own:
   ```sh
   docker compose start node2
   ```

> **Heads up:** every so often, the node you restart wins its own
> re-election before it's heard from whoever's actually leading. That's
> correct Raft behavior, not a bug — but as a *freshly elected* leader it
> can briefly return "not found" for data that's already committed,
> until it commits one entry of its own term. If that happens, just
> write anything (`put nudge 1`) and the old data reappears.

### Running it without Docker

Same idea, just run the three processes yourself:

```sh
go run ./cmd/server --id=1 --addr=localhost:8001 \
  --peers=2=localhost:8002,3=localhost:8003 --data=./data/node1
go run ./cmd/server --id=2 --addr=localhost:8002 \
  --peers=1=localhost:8001,3=localhost:8003 --data=./data/node2
go run ./cmd/server --id=3 --addr=localhost:8003 \
  --peers=1=localhost:8001,2=localhost:8002 --data=./data/node3
```

One-time setup first — generate the protobuf/gRPC code:

```sh
brew install go protobuf grpcurl
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/raft.proto
```

(The `paths=source_relative` flags matter — without them `protoc-gen-go`
writes the generated files to the wrong directory and `go build` won't
find them.)

## Benchmarking

`raft-bench` drives load against a cluster you already have running
(local or Docker) and reports real numbers instead of guessed ones:

```sh
go build -o raft-bench ./cmd/bench

# throughput + latency, 20 concurrent writers for 10s
./raft-bench throughput --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --workers=20 --duration=10s

# how long a leader failure actually takes to recover from — kill the
# leader yourself in another terminal while this runs
./raft-bench failover --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --duration=30s
```

What I measured on a dev laptop:

| | Local processes | Docker Compose |
|---|---|---|
| Throughput | ~110-120 ops/sec | ~190 ops/sec |
| p50 write latency | ~170ms | ~102ms |
| Failover recovery | ~400-600ms | ~880ms-1.06s |

Small-scale, single-machine numbers — rerun them yourself for numbers
that reflect your own hardware. The write latency is mostly one thing:
replication only fires on a 100ms heartbeat tick instead of immediately
when you write, so commits are bottlenecked on that tick rather than
actual network or disk cost. Fixing that is probably the single highest-
leverage throughput improvement available, and a good next project.

## Testing

```sh
go test ./... -race
```

Everything's covered by fast unit tests, plus a slower `integration/`
suite that spins up real server processes and does actual kill/restart/
network-delay testing over real gRPC — the same techniques as the Docker
demo above, just scripted. That suite is gated behind `-short` so quick
iteration stays fast; run it on its own with `go test ./integration/... -v`
when you want the full fault-injection coverage.

## Roadmap

| Milestone | What it is | |
|---|---|---|
| M0 - Skeleton | Go module, config parsing, three server processes, gRPC wiring | ✅ |
| M1 - Election | Leader election with randomized timeouts | ✅ |
| M2 - Heartbeats | Leader keeps followers alive; stale leaders step down | ✅ |
| M3 - Log replication | Commands get appended, replicated, and committed by majority | ✅ |
| M4 - KV API | PUT/GET/DELETE through Raft, with client redirect to the leader | ✅ |
| M5 - Persistence | Durable term/vote/log — survives a crash and restart | ✅ |
| M6 - Fault testing | Real process kill/restart tests, repeated failover | ✅ |
| M7 - Packaging | Docker, Compose, README demo, benchmark harness | ✅ |
| M8 - Stretch | Snapshots, conflict-index optimization, metrics, sharding | — |
