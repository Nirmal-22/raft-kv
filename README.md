# raft-kv

A small distributed key-value store in Go. Three (or more) nodes keep identical copies of the
data using the Raft consensus algorithm, so the cluster keeps working and loses nothing when a
node crashes or is cut off from the others.

It is a learning project built to production habits: a typed wire protocol, tests that break
the cluster on purpose and check the results, metrics, tracing and CI.

```
            PUT /v1/kv/greeting                 any node accepts any request
client ───────────────────────────► n2 (follower)
                                     │ gRPC: Apply(command)      forwards to the leader
                                     ▼
                                    n1 (leader) ── Raft log ──► n2, n3
                                     │            a write is acknowledged once a
                                     ▼            majority has stored it
                                 Pebble store     every node applies the same
                                                  commands in the same order
```

## Try it

Needs Go 1.27 or newer. Nothing else: no Docker, no external services.

```bash
scripts/cluster.sh start        # builds, starts n1 n2 n3 on localhost, waits for a leader
```

```
n1  state=Leader leader=n1 term=2 applied=7
n2  state=Follower leader=n1 term=2 applied=6
n3  state=Follower leader=n1 term=2 applied=6
```

Write through one follower, read through another:

```bash
curl -X PUT localhost:8002/v1/kv/greeting -d 'hello'
# {"key":"greeting","version":8}
curl -i localhost:8003/v1/kv/greeting
# HTTP/1.1 200 OK
# X-Kv-Version: 8
# hello
```

Kill the leader with `kill -9` and carry on:

```bash
scripts/cluster.sh kill "$(scripts/cluster.sh leader)"
scripts/cluster.sh status
# n1  down
# n2  state=Leader leader=n2 term=3 applied=11
# n3  state=Follower leader=n2 term=3 applied=11
curl localhost:8002/v1/kv/greeting                 # hello
curl -X PUT localhost:8002/v1/kv/after -d 'still-works'
scripts/cluster.sh up n1                           # comes back, catches up
```

Cut a node off from its peers without stopping it (a network partition), then heal it. This
endpoint exists only when a node is started with `--chaos`, which the script does:

```bash
curl -X POST 'localhost:8002/debug/isolate?on=true'
curl -X POST 'localhost:8002/debug/isolate?on=false'
```

`scripts/cluster.sh stop` stops the nodes; `scripts/cluster.sh clean` also deletes their data.

## API

Every node serves the same HTTP API on its `--http` address.

| Request | What it does |
|---|---|
| `PUT /v1/kv/{key}` | Store the request body as the value. Returns the key's new `version`. |
| `PUT /v1/kv/{key}?ttl=30s` | Same, and the key is removed after about that long. |
| `PUT /v1/kv/{key}?if_version=N` | Compare-and-swap: write only if the key's version is `N` (`0` = only if it does not exist). `409` with `current_version` otherwise. |
| `GET /v1/kv/{key}` | The value as the response body, version in `X-Kv-Version`. `404` if absent. |
| `GET /v1/kv/{key}?consistency=stale` | Answer from this node's own copy: faster, works without a leader, may be behind. |
| `DELETE /v1/kv/{key}` | Delete. Accepts `if_version` too. |
| `GET /v1/keys?prefix=&limit=` | List keys under a prefix, with versions. |
| `GET /v1/watch?prefix=` | Server-Sent Events stream of `put`, `delete` and `expire` events. A value that is not valid UTF-8 arrives as `value_base64`. A final `dropped` event means events were missed: re-read and reconnect. |
| `GET /v1/cluster` | Leader, term, log positions and members as this node sees them. |
| `DELETE /v1/cluster/members/{id}` | Remove a node. Must be sent to the leader (`421` otherwise, with the leader's address). |
| `GET /metrics`, `/healthz`, `/readyz` | Prometheus metrics; liveness; ready once a leader is known. |

Keys may contain `/` and are at most 1024 bytes; values are at most 1 MiB.

A key's **version** is the Raft log index of the last command that changed it. That gives
optimistic concurrency for free: read a value and its version, then write back with
`if_version`.

Errors on writes separate two cases a client must treat differently:

- `503 no leader available; nothing was written` : safe to retry.
- `503`/`504` ending in `the write may or may not have been applied` : the request reached the
  log and its fate is unknown (for example the leader lost its majority mid-write). Retry only
  with `if_version`, or after reading the key.

## How it works

**Write path.** A node that is not the leader forwards the command to the leader over gRPC
(`internal/node/grpc.go`). The leader appends it to its Raft log (BoltDB, fsynced), replicates
it, and once a majority has it, every node applies it to its Pebble store. A node asked to do
something it cannot do as a follower answers `FAILED_PRECONDITION` and does nothing; that is the
only error the caller retries, so a write is never sent twice by the store itself.

**Read path.** The default read is *linearizable*: it reflects every write that finished before
the read began. The danger is a leader that has been cut off, does not know it yet, and still
holds old data. Before reading, the leader commits a marker entry through Raft (`Barrier`): if
it has been replaced, the marker never reaches a majority and the read fails instead of
returning stale data (`getLocal` in `internal/node/node.go`). `consistency=stale` skips all of
this and reads the contacted node's own copy.

**Storage layout.** A key's value is stored with a 16-byte header (version and expiry time). The
same header is also kept in a small separate index, because Pebble stores a value next to its
key: anything that only needs versions (listing, compare-and-swap, expiry) would otherwise load
every value from disk.

**Applying a command exactly once per node.** Each command is applied in one Pebble batch
together with its log index (`Store.Apply` in `internal/store/store.go`). After a restart Raft
replays entries the store may already contain; anything at or below the stored index is
skipped. Without that, a replayed compare-and-swap would be judged against newer state and the
copies could drift apart. It also means the store does not need to fsync: if it loses its last
few commands in a crash, the node brings it forward again on restart.

**Restart.** Raft's default is to reload the newest snapshot into the state machine on every
start, which would wipe and rebuild the whole store and make restart time grow with the data.
The store is on disk and knows the last index it applied, so the node skips the reload when the
store is at or past the snapshot and just replays the log after it (`startRaft` in
`internal/node/node.go`). It still reloads when the store is behind the snapshot, because the
log entries that would fill the gap may already have been compacted away.

**TTL without clocks disagreeing.** Nodes never check their own clock to decide whether a key
has expired, because three clocks give three answers. The leader stamps the expiry time when it
proposes the write, and later proposes `Expire(now, max_keys)` commands; every node deletes the
same keys when it applies one. A sorted expiry index makes this touch only the keys that are
due, and each command removes at most 1,000 of them, so a large backlog is cleared in many small
steps instead of one that stalls every node. The costs: a key can outlive its TTL by up to one
tick (1 s by default), and after a change of leader a key can expire early or late by the two
leaders' clock difference.

**Snapshots.** The log is compacted into a snapshot of the store (a frozen Pebble view,
streamed out while writes continue). A node that is too far behind, or brand new, is sent the
snapshot and then the log entries after it.

**Membership.** A new node asks any existing node to add it; that node passes the request to
the leader. Each node's addresses are stored in the replicated state, which is how a follower
finds the leader's gRPC address. The leader can remove any node, including itself.

**Watch.** Events come from the node you are connected to, as it applies commands. When that
node loads a snapshot its state jumps without per-key events, so it ends every watch stream
with a `dropped` event rather than let a client believe it saw everything.

**Wire format.** Log entries and node-to-node calls are Protocol Buffers
(`proto/kv.proto`). The generated code is committed, so building needs only Go.

## Design decisions

| Decision | Why | Cost |
|---|---|---|
| `hashicorp/raft` for consensus, not a hand-written Raft | Elections, log repair and membership changes are where subtle bugs live; this library runs Consul and Nomad. The interesting work here is everything around it. | Less control: reads cannot use its internals for a cheaper scheme. |
| Linearizable reads by committing a marker (`Barrier`) | Correct without trusting clocks. | One log entry and one round trip to a majority per read. |
| Pebble for state, BoltDB for the Raft log | Pebble (an LSM tree) suits many small writes and gives cheap consistent snapshots. BoltDB is the log store `hashicorp/raft` is tested with. | Two storage engines to understand. |
| Followers forward to the leader | Clients need no cluster map and no redirect handling. | One extra hop for requests sent to a follower. |
| Version = log index | Compare-and-swap with no extra counter, and versions never repeat. | Versions are not consecutive per key. |
| gRPC only for forwarding and joining; Raft keeps its own TCP transport | Replacing a tested transport would add risk for no gain. | Two node-to-node ports. |

## Testing

`make test` runs 35 tests; `make race` runs them under Go's race detector, which is what CI
does.

- **Store** (12): put/get/delete, compare-and-swap, TTL and bounded expiry, listing without
  reading values, the metadata index staying in step with the data, snapshot and restore, watch,
  and that replayed log entries are skipped.
- **HTTP** (7): the real handler in front of a one-node cluster, including the event stream and
  `/metrics`.
- **Cluster** (16): real 3-node clusters inside the test process. Real Raft over TCP, real gRPC,
  real files; only the timeouts are shortened. They cover forwarding, a leader crash, loss of
  majority, a partitioned leader, restarts with and without a snapshot reload, a new node
  catching up from a snapshot, TTL on every node and across a change of leader, a 4.5 MiB list
  through a follower, and removing a member (including the leader removing itself).

**The fault-injection run** (`internal/clustertest/linearizability_test.go`). Six clients read,
write and delete for six seconds through randomly chosen nodes while the test repeatedly cuts
the leader off from the others and stops and restarts nodes. Every operation is recorded with
its start and end time. [Porcupine](https://github.com/anishathalye/porcupine) then searches for
a single order of all operations that explains every result a client saw; if there is none, the
store returned something a correct single-copy store never could. A typical run: about 3,000
completed operations, 8 faults, verified in under 7 seconds. Writes whose outcome the client
never learned are kept in the history as "may have happened at any later time", not dropped.

**Compare-and-swap under faults** (`TestCompareAndSwapCounterUnderFaults`). The Porcupine model
has no versions, so compare-and-swap gets its own check: five clients increment one counter with
read-then-swap during the same partitions and restarts. At the end the counter must lie between
the number of swaps the clients saw succeed and that number plus the swaps with an unknown
outcome. A swap applied twice, lost, or accepted against a stale version breaks that. One run:
380 acknowledged swaps, 14 unknown, counter at 384.

**Does the test catch real bugs?** I broke the code on purpose and re-ran it:

| Deliberate bug | Caught? |
|---|---|
| Serve every read from the contacted node's own copy | Yes, by both the partition test and the Porcupine run. |
| Leader skips the majority check before a read | **No** (0 failures in 8 runs of each test). |

The second result is a real limit, not a pass. Raft makes a cut-off leader step down after its
lease runs out, which here is before the others can elect a replacement, so the stale window is
closed as long as timers behave. The marker read protects against the cases where they do not
(a long pause, a clock jump), and this test suite cannot produce those. Showing that needs a
tool that controls time, such as Jepsen or a deterministic simulator.

**Bugs the tests found while building this:**

- The race detector caught a joining node announcing itself before its gRPC address was set, so
  it could have registered an empty address.
- The partition simulator itself leaked: a connection that was being dialed at the moment a node
  was cut off survived, and the "isolated" leader kept its majority. The fix is in
  `internal/node/faultnet.go`.

**Bugs found by reading the code afterwards**, each now fixed with a test that fails on the old
code where that was checked:

- A list sent to a follower failed once the reply passed gRPC's 4 MiB default, while the same
  request to the leader worked (checked: `ResourceExhausted` at 4.5 MiB without the fix).
- Listing read every value to get at the 16-byte header in front of it: 42 to 48 MiB allocated
  to list 40 MiB of values, now under 4 MiB. The first fix (stop copying) was not enough, because
  Pebble keeps a value next to its key and loads the whole value from disk either way. Versions
  and expiry times now also live in a small separate index that List and the write path read.
- One `Expire` command deleted every due key, however many.
- The leader removing itself took effect but reported an error and stayed in the member table.
  The first fix had a race of its own, which its test caught: the leader's background loop put
  the entry back before the removal committed.
- Every restart reloaded the whole snapshot.
- A watcher silently missed everything a snapshot changed, and binary values were altered in
  watch events.

## Observability

- **Metrics** at `/metrics`: request latency by route and status, time to apply a command, read
  latency by consistency level, forwarded requests, leadership changes, expired keys, open
  watchers, snapshot reloads, and Raft's term and log positions.
- **Tracing** with OpenTelemetry: `kvd --trace stdout` prints spans; `--trace otlp` sends them to
  a collector. A write sent to a follower is one trace across two nodes (the follower's HTTP
  span is the parent of the leader's gRPC span). The stdout mode is verified; the OTLP mode
  compiles but has not been run against a collector.
- **Logs**: structured, one logger per node, `--log-level debug|info|warn|error`.

## Limits

Things a real deployment would need, in rough order of importance:

- **No authentication or TLS.** The HTTP API is open and node-to-node traffic is plain TCP.
- **Reads are expensive.** One log entry per linearizable read. The standard fixes are batching
  many reads behind one marker, or lease-based reads.
- **Writes are not deduplicated.** A client that retries a write with an unknown outcome can
  apply it twice. Compare-and-swap avoids it today; the full fix is a client id and sequence
  number recorded in the replicated state.
- **One Raft group.** All data lives on every node, so capacity does not grow with nodes. Scaling
  out means sharding keys across several groups.
- **Watch does not replay.** A watcher that disconnects or is dropped misses events; there is no
  "resume from version N".
- **Listen address = advertised address.** Running behind NAT or in containers needs separate
  bind and advertise addresses.
- **Removing a member is not forwarded** to the leader like other requests are.
- **No benchmarks yet**, so this README makes no performance claims.

## Layout

```
cmd/kvd/                 the node binary and its flags
proto/kv.proto           log entries and the node-to-node gRPC service
internal/pb/             generated from the proto file (committed)
internal/store/          the state machine on Pebble: apply, expiry index, snapshots, watch
internal/node/           Raft wiring, leader forwarding, joining, expiry loop, partition switch
internal/httpapi/        the REST API (Gin)
internal/metrics/        Prometheus instruments
internal/tracing/        OpenTelemetry setup
internal/clustertest/    multi-node tests and the linearizability check
scripts/cluster.sh       run a 3-node cluster locally
```

`make proto` regenerates `internal/pb/` and needs `protoc` plus `protoc-gen-go` and
`protoc-gen-go-grpc`. Nothing else in the build does.

## Licence

MIT. See [LICENSE](LICENSE).
