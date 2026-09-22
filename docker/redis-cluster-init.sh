#!/bin/sh
# Boot a 6-node Redis cluster (3 masters + 3 replicas) inside ONE container and
# block until it actually reports cluster_state:ok on every node.
#
# Why this exists: the previous inline entrypoint started all six servers with
# `--daemonize yes`, waited for port 30000 to answer PING, and went straight
# into `redis-cli --cluster create`. Two things could make that exit 1 and take
# the whole container down before a single test ran (see issue #398):
#
#   1. Only node 30000 was gated. `--daemonize yes` returns as soon as the
#      server forks, before it binds its port, so on a loaded runner nodes
#      30001-30005 could still be unbound when `--cluster create` dialled them.
#      redis-cli exits 1 on the first connection refusal.
#   2. Even with every node up, `--cluster create` gives up with
#      "Sorry, the cluster configuration is not consistent" (exit 1) when the
#      nodes have not finished gossiping over the cluster bus in time — the
#      classic noisy-neighbour failure on shared CI runners.
#
# Both are transient. So: gate on *every* node, then retry the create a bounded
# number of times (resetting the nodes in between so a half-formed cluster does
# not poison the next attempt), and dump every node's log before giving up so a
# real failure is diagnosable instead of a bare "exited (1)".
set -eu

PORTS='30000 30001 30002 30003 30004 30005'
LOG_DIR='/var/log/redis-cluster'
# Written only once formation is complete. The compose healthcheck requires it,
# so `docker compose up --wait` cannot return while this script is still
# retrying or still waiting for the topology to settle.
READY_MARKER='/run/redis-cluster-ready'

# All tunables are env-overridable so a slow machine can be given more room
# without editing this script.
NODE_READY_TIMEOUT="${NODE_READY_TIMEOUT:-60}"       # seconds, all nodes bound
CLUSTER_READY_TIMEOUT="${CLUSTER_READY_TIMEOUT:-60}" # seconds, per create attempt
CREATE_ATTEMPTS="${CREATE_ATTEMPTS:-5}"
# `redis-cli --cluster create` can sit in "Waiting for the cluster to join"
# indefinitely; without a timeout that hang is unrecoverable, with one it just
# becomes another retryable attempt.
CREATE_TIMEOUT="${CREATE_TIMEOUT:-120}"
# 5000ms flaps under CI load and is what makes the join check give up; 15s is
# still far below any test timeout and nothing here exercises failover.
CLUSTER_NODE_TIMEOUT="${CLUSTER_NODE_TIMEOUT:-15000}"

log() {
  echo "[cluster-init] $*"
}

now() {
  date +%s
}

dump_diagnostics() {
  log '===== diagnostics ====='
  for port in $PORTS; do
    log "--- CLUSTER INFO @ $port ---"
    redis-cli -p "$port" cluster info 2>&1 || log "(node $port unreachable)"
  done
  log '--- CLUSTER NODES @ 30000 ---'
  redis-cli -p 30000 cluster nodes 2>&1 || log '(node 30000 unreachable)'
  for port in $PORTS; do
    log "--- redis-server $port log (last 50 lines) ---"
    tail -n 50 "$LOG_DIR/$port.log" 2>&1 || log "(no log file for $port)"
  done
  log '===== end diagnostics ====='
}

start_nodes() {
  mkdir -p "$LOG_DIR"
  for port in $PORTS; do
    # --logfile is essential: a daemonized redis-server with an empty logfile
    # sends its log to /dev/null, which is the other half of why three CI
    # sightings of this bug produced zero diagnostic detail.
    redis-server \
      --port "$port" \
      --cluster-enabled yes \
      --cluster-config-file "nodes-$port.conf" \
      --cluster-node-timeout "$CLUSTER_NODE_TIMEOUT" \
      --cluster-announce-ip 127.0.0.1 \
      --cluster-announce-port "$port" \
      --appendonly no \
      --save '' \
      --logfile "$LOG_DIR/$port.log" \
      --daemonize yes
  done
  log "started 6 redis-server processes (logs in $LOG_DIR)"
}

# A node counts as ready only once it answers PING *and* reports that it came
# up in cluster mode — PING alone can succeed before the cluster subsystem is
# initialised.
node_ready() {
  [ "$(redis-cli -p "$1" ping 2>/dev/null)" = 'PONG' ] || return 1
  # cluster_enabled lives in INFO's cluster section, not in CLUSTER INFO.
  redis-cli -p "$1" info cluster 2>/dev/null | grep -q 'cluster_enabled:1'
}

wait_for_nodes() {
  deadline=$(($(now) + NODE_READY_TIMEOUT))
  for port in $PORTS; do
    until node_ready "$port"; do
      if [ "$(now)" -ge "$deadline" ]; then
        log "FATAL: node $port not ready after ${NODE_READY_TIMEOUT}s"
        dump_diagnostics
        exit 1
      fi
      sleep 0.2
    done
  done
  log 'all 6 nodes are up and cluster-enabled'
}

# The real readiness gate: every node must agree the cluster is formed, every
# slot must be covered, and all six nodes must know each other. A cluster where
# only 30000 says cluster_state:ok is still one clients can hang on.
cluster_formed() {
  for port in $PORTS; do
    info=$(redis-cli -p "$port" cluster info 2>/dev/null) || return 1
    echo "$info" | grep -q 'cluster_state:ok' || return 1
    echo "$info" | grep -q 'cluster_slots_assigned:16384' || return 1
    [ "$(echo "$info" | grep 'cluster_known_nodes:' | tr -dc '0-9')" = '6' ] || return 1

    # Roles settle by gossip a beat after `--cluster create` returns: for a
    # short window a node can report cluster_state:ok while still listing a
    # freshly-assigned replica as a slotless master. Wait that out too, or the
    # first client in sees a topology that is about to change under it.
    roles=$(redis-cli -p "$port" cluster nodes 2>/dev/null | awk '{print $3}') || return 1
    [ "$(echo "$roles" | grep -c 'master')" = '3' ] || return 1
    [ "$(echo "$roles" | grep -c 'slave')" = '3' ] || return 1
  done
}

wait_for_cluster() {
  deadline=$(($(now) + CLUSTER_READY_TIMEOUT))
  until cluster_formed; do
    [ "$(now)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}

# Between attempts, wipe whatever the failed attempt left behind. Without this a
# retry dies immediately on "Node 127.0.0.1:3000x is not empty".
reset_nodes() {
  for port in $PORTS; do
    redis-cli -p "$port" flushall >/dev/null 2>&1 || true
    redis-cli -p "$port" cluster reset hard >/dev/null 2>&1 || true
  done
}

create_cluster() {
  nodes=''
  for port in $PORTS; do
    nodes="$nodes 127.0.0.1:$port"
  done

  attempt=1
  while :; do
    log "forming cluster (attempt $attempt/$CREATE_ATTEMPTS)"
    # shellcheck disable=SC2086 # $nodes is a deliberately split argument list
    if timeout "$CREATE_TIMEOUT" redis-cli --cluster create $nodes --cluster-replicas 1 --cluster-yes; then
      if wait_for_cluster; then
        log 'cluster_state:ok on all 6 nodes, 16384/16384 slots, 3 masters + 3 replicas'
        return 0
      fi
      log "create succeeded but the cluster never settled within ${CLUSTER_READY_TIMEOUT}s"
    else
      log "redis-cli --cluster create failed (or timed out after ${CREATE_TIMEOUT}s) on attempt $attempt"
    fi

    if [ "$attempt" -ge "$CREATE_ATTEMPTS" ]; then
      log "FATAL: cluster did not form after $CREATE_ATTEMPTS attempts"
      dump_diagnostics
      exit 1
    fi

    attempt=$((attempt + 1))
    reset_nodes
    sleep 2
  done
}

rm -f "$READY_MARKER"

start_nodes
wait_for_nodes
create_cluster

touch "$READY_MARKER"
log 'cluster ready'
# Keep the container alive in the foreground; the redis-servers are daemonized.
# If one of them dies later the compose healthcheck (which re-checks
# cluster_state on every node) turns the container unhealthy.
exec tail -f /dev/null
