# raft-kv — CLAUDE.md

## Purpose
A learning project for **distributed systems in Go**: Raft consensus, gRPC/protobuf, Gin, an LSM
storage engine (Pebble), linearizability testing, metrics and tracing. The user is learning Go
and backend/distributed-systems engineering for interviews. Keep explanations focused on those
concepts; handle environment chores directly and don't turn them into Linux lessons.

Write production-style code: small packages with clear boundaries, typed errors, tests for every
behaviour change, and honest docs (README states limits and unverified claims).

## Environment (WSL Ubuntu-24.04)
- Go 1.27.4 in `~/.local/go`, protoc 36.2 in `~/.local/protoc`, plugins in `~/go/bin`.
  Run `. ~/.local/goenv.sh` first in every shell (sets PATH and `GOFLAGS=-p=6`).
- WSL has ~7 GB RAM and has crashed from memory pressure before. Keep runs modest; the full test
  suite is fine (~100 MB). Don't start Docker-based tooling (Docker is not available in WSL).
- `gh` is logged in as **Nirmal-22**. Repo: https://github.com/Nirmal-22/raft-kv (private).
- Plain `git push` works for workflow files; `gh repo create --push` was refused for lacking the
  `workflow` scope.
- Never `pkill -f <pattern>` that also matches your own shell command. Background processes die
  when a Claude Code session ends; `scripts/cluster.sh` tracks node PIDs in `data/`.

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
internal/clustertest/    in-process multi-node tests + Porcupine linearizability check
scripts/cluster.sh       local 3-node cluster (HTTP 8001-8003, gRPC 9001-9003, Raft 7001-7003)
```

## Invariants — keep these true
- Only `FAILED_PRECONDITION` ("not the leader, did nothing") is retried by `route`. Any other
  error from a forwarded write is returned: it may have been applied.
- `Store.Apply` skips any index at or below the stored applied index, and writes the data change
  and the new index in one batch.
- Nodes never consult their own clock to expire keys; the leader stamps `expires_at_ms` and
  proposes `Expire(now)`.
- Background goroutines (leaderLoop, joinLoop) start only after both listeners are bound.
- Every public Node entry point holds `enter()` so nothing touches the store after Shutdown.

## Commands
`make build` · `make test` · `make race` (what CI runs) · `make lint` · `make proto` ·
`scripts/cluster.sh start|status|leader|kill nX|up nX|stop|clean`

## Status (2026-11-19)
Done: single node, 3-node Raft cluster, gRPC forwarding, TTL, watch, snapshots, membership,
metrics, tracing, 26 tests (race-clean, CI green), README.
Known gaps (also in README "Limits"): no auth/TLS; one log entry per linearizable read; no
client-side write dedup; single Raft group (no sharding); watch has no replay; listen address =
advertised address; member removal not forwarded; no benchmarks; OTLP export not run against a
real collector. The suite does not catch a leader that skips its read barrier (needs control
over time, e.g. a deterministic simulator).

Good next steps: benchmarks (`go test -bench` + a load generator), batched/lease reads, client
dedup (client id + sequence in state), sharding across several Raft groups.
