#!/usr/bin/env bash
# Run a 3-node raft-kv cluster on this machine.
#
#   scripts/cluster.sh start     build, start n1 n2 n3, wait for a leader
#   scripts/cluster.sh status    one line per node
#   scripts/cluster.sh leader    print the leader's node id
#   scripts/cluster.sh kill n2   kill -9 one node (a crash, not a clean stop)
#   scripts/cluster.sh up n2     start one node again
#   scripts/cluster.sh stop      stop all nodes
#   scripts/cluster.sh clean     stop all nodes and delete their data
#
# HTTP on 8001-8003, gRPC on 9001-9003, Raft on 7001-7003.
# EXTRA_FLAGS is passed to every node, for example EXTRA_FLAGS="--trace stdout".
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DATA="$ROOT/data"
BIN="$ROOT/bin/kvd"
EXTRA_FLAGS=${EXTRA_FLAGS:-}

http_port() { echo $((8000 + ${1#n})); }

start_node() {
  local id=$1 i=${1#n}
  local args=(--id "$id" --data "$DATA/$id" --http "127.0.0.1:$((8000 + i))"
    --grpc "127.0.0.1:$((9000 + i))" --raft "127.0.0.1:$((7000 + i))" --chaos)
  if [ "$id" = n1 ]; then
    args+=(--bootstrap)
  else
    args+=(--join 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003)
  fi
  mkdir -p "$DATA"
  # shellcheck disable=SC2086
  nohup "$BIN" "${args[@]}" $EXTRA_FLAGS >>"$DATA/$id.log" 2>&1 </dev/null &
  echo $! >"$DATA/$id.pid"
  echo "started $id (pid $!, http 127.0.0.1:$((8000 + i)))"
}

stop_node() {
  local id=$1 signal=${2:-TERM}
  local pidfile="$DATA/$id.pid"
  [ -f "$pidfile" ] || return 0
  kill "-$signal" "$(cat "$pidfile")" 2>/dev/null || true
  rm -f "$pidfile"
}

status() {
  for id in n1 n2 n3; do
    local out
    if out=$(curl -sf -m 2 "127.0.0.1:$(http_port "$id")/v1/cluster" 2>/dev/null); then
      echo "$id  $(echo "$out" | sed -E 's/.*"state":"([^"]*)".*"leader_id":"([^"]*)".*"term":([0-9]+).*"applied_index":([0-9]+).*/state=\1 leader=\2 term=\3 applied=\4/')"
    else
      echo "$id  down"
    fi
  done
}

leader() {
  for id in n1 n2 n3; do
    local out
    out=$(curl -sf -m 2 "127.0.0.1:$(http_port "$id")/v1/cluster" 2>/dev/null) || continue
    echo "$out" | sed -E 's/.*"leader_id":"([^"]*)".*/\1/'
    return 0
  done
  return 1
}

case "${1:-}" in
  start)
    (cd "$ROOT" && go build -o "$BIN" ./cmd/kvd)
    for id in n1 n2 n3; do start_node "$id"; done
    for _ in $(seq 1 50); do
      if [ "$(status | grep -c 'leader=n')" = 3 ]; then break; fi
      sleep 0.2
    done
    status
    ;;
  status) status ;;
  leader) leader ;;
  kill) stop_node "${2:?node id, for example n2}" KILL && echo "killed $2" ;;
  up) start_node "${2:?node id, for example n2}" ;;
  stop) for id in n1 n2 n3; do stop_node "$id"; done ;;
  clean)
    for id in n1 n2 n3; do stop_node "$id"; done
    rm -rf "$DATA"
    ;;
  *)
    sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
