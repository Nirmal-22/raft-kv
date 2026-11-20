# raft-kv — CLAUDE.md

## Purpose
A learning project for distributed systems in Go: Raft consensus, gRPC/protobuf, Gin, an LSM
storage engine (Pebble), linearizability testing, metrics and tracing.

Write production-style code: small packages with clear boundaries, typed errors, a test for
every behaviour change, and honest docs (the README states limits and unverified claims).
Machine-specific notes, if any, live in `CLAUDE.local.md`, which is not committed.

## Layout
```
cmd/kvd/                 node binary and flags
proto/kv.proto           Raft log entries + node-to-node gRPC service (regen: make proto)
internal/pb/             generated code, committed
internal/store/          Pebble state machine: Apply (idempotent by log index), TTL index,
                         snapshots, watch hub
internal/node/           Raft wiring, leader forwarding (route), join, leader loop (expiry,
                         self-registration), shutdown guard (enter/life), faultnet partitions
internal/httpapi/        Gin REST API, SSE watch, /metrics
internal/metrics/        Prometheus instruments (per-node registry)
internal/tracing/        OpenTelemetry setup (stdout / otlp)
internal/clustertest/    in-process multi-node tests, Porcupine linearizability check,
                         compare-and-swap counter under faults
scripts/cluster.sh       local 3-node cluster (HTTP 8001-8003, gRPC 9001-9003, Raft 7001-7003)
```

## Invariants — keep these true
- Only `FAILED_PRECONDITION` ("not the leader, did nothing") is retried by `route`. Any other
  error from a forwarded write is returned: it may have been applied.
- `Store.Apply` skips any index at or below the stored applied index, and writes the data change
  and the new index in one batch (no fsync).
- On start, the snapshot reload is skipped only when the store's applied index is at or past the
  newest snapshot. Never skip it unconditionally: unsynced store writes can be lost in a crash
  and their log entries may already be compacted.
- Nodes never consult their own clock to expire keys; the leader stamps `expires_at_ms` and
  proposes `Expire(now, max_keys)`. Limits that affect what a command does travel in the
  command, never in code constants read by the state machine.
- Loading a snapshot cuts off all watchers (no events exist for those changes).
- Background goroutines (leaderLoop, joinLoop) start only after both listeners are bound.
- Every public Node entry point holds `enter()` so nothing touches the store after Shutdown.
- Every write path keeps the metadata index (`v<key>`) in step with the data (`d<key>`) in the
  same batch. Read paths that only need a version or expiry use `stat` or iterate the index;
  they never load values.
- A leader removing itself sets `leaving` first so the leader loop does not re-register it.

## Commands
`make build` · `make test` · `make race` (what CI runs) · `make lint` · `make proto` ·
`scripts/cluster.sh start|status|leader|kill nX|up nX|stop|clean`

## Status (2026-11-20)
Done: single node, 3-node Raft cluster, gRPC forwarding, TTL (batched expiry), watch, snapshots,
restart without snapshot reload, membership (including the leader removing itself), metrics,
tracing, tests race-clean and CI green, README, MIT licence.
Known gaps (also in README "Limits"): no auth/TLS; one log entry per linearizable read; no
client-side write dedup; single Raft group (no sharding); watch has no replay; listen address =
advertised address; member
removal not forwarded; no benchmarks; OTLP export not run against a real collector. The suite
does not catch a leader that skips its read barrier (needs control over time).

Good next steps: benchmarks (`go test -bench` + a load generator), batched/lease reads, client
dedup (client id + sequence in state), sharding across Raft groups.
