#!/bin/sh
# Boot a 6-node Redis cluster (3 masters + 3 replicas) inside ONE container and
# block until it has genuinely formed. Also serves as the container's
# healthcheck (`sh redis-cluster-init.sh check`) so the startup gate and the
# liveness probe can never drift apart.
#
# Why this exists: the previous inline entrypoint started all six servers with
# `--daemonize yes`, waited for port 30000 to answer PING, and went straight
# into `redis-cli --cluster create`. Two things could make that exit 1 and take
# the whole container down before a single test ran (see issue #398):
#
#   1. Only node 30000 was gated. `--daemonize yes` forks and returns exit 0
#      before the server binds its port — it returns 0 even when the bind then
#      fails — so on a loaded runner nodes 30001-30005 could still be unbound
#      when `--cluster create` dialled them. redis-cli exits 1 on the first
#      connection refusal.
#   2. Even with every node up, `--cluster create` gives up with
#      "Sorry, the cluster configuration is not consistent" (exit 1) when the
#      nodes have not finished gossiping over the cluster bus in time — the
#      classic noisy-neighbour failure on shared CI runners.
#
# Both are transient. So: gate on *every* node, then retry the create a bounded
# number of times (resetting the nodes in between so a half-formed cluster does
# not poison the next attempt), and dump every node's log before giving up so a
# real failure is diagnosable instead of a bare "exited (1)".
#
# ---------------------------------------------------------------------------
# TIMEOUT BUDGET — keep in sync with .github/workflows/ci.yml
# ---------------------------------------------------------------------------
# Worst case with the defaults below:
#
#   node gate    NODE_READY_TIMEOUT                              =  30s
#   per attempt  CREATE_TIMEOUT + CLUSTER_READY_TIMEOUT = 60 + 30 =  90s
#   attempts     CREATE_ATTEMPTS                                 =   3
#   backoff      2s x (CREATE_ATTEMPTS - 1)                      =   4s
#                                          30 + 3*90 + 4         = 304s
#
# plus a few seconds for the diagnostics dump, so call it ~315s.
#
# This script is deliberately the *authoritative* budget: it is meant to lose
# first. The workflow's `docker compose up --wait --wait-timeout` sits above it
# (420s) purely as an outer backstop, and the compose healthcheck's own budget
# sits above that again. Get this ordering wrong — as an earlier revision of
# this PR did, with a 968s script budget under a 240s wait-timeout — and every
# retry after the first, *and* the diagnostics dump that is the whole point of
# the exercise, become unreachable: compose kills the wait first and you are
# back to #398's original symptom with a nicer message. If you change a
# constant here, recheck ci.yml.
set -eu

PORTS='30000 30001 30002 30003 30004 30005'
LOG_DIR='/var/log/redis-cluster'
# Where each node's nodes-<port>.conf lives. /data is a declared VOLUME *and*
# the image's WORKDIR, so a relative --cluster-config-file lands in a volume
# that survives a container restart. A stale topology file there makes every
# node come up already believing it is in a cluster, which fails the create
# with "Node ... is not empty" and burns an attempt. Cleared on every boot.
CONFIG_DIR="${CONFIG_DIR:-/data}"
# Ready marker. It lives on /dev/shm, which Docker mounts as a fresh tmpfs on
# every container start (including `docker restart`), so a stale marker from a
# previous boot is structurally impossible rather than a small race against the
# `rm -f` below — /run is just the container's writable layer and does persist.
READY_MARKER='/dev/shm/redis-cluster-ready'

# All tunables are env-overridable so a slow machine can be given more room
# without editing this script. See the budget block above before raising any.
NODE_READY_TIMEOUT="${NODE_READY_TIMEOUT:-30}"       # seconds, all nodes bound
CLUSTER_READY_TIMEOUT="${CLUSTER_READY_TIMEOUT:-30}" # seconds, per create attempt
CREATE_ATTEMPTS="${CREATE_ATTEMPTS:-3}"
# `redis-cli --cluster create` can sit in "Waiting for the cluster to join"
# indefinitely; without a timeout that hang is unrecoverable, with one it just
# becomes another retryable attempt.
CREATE_TIMEOUT="${CREATE_TIMEOUT:-60}"
# 5000ms is what made bus links flap under CI load, and a flapping link is
# exactly what makes redis-cli's join consistency check give up. 15s buys that
# stability at a real cost, not for free: cluster-node-timeout is also the
# pfail -> fail promotion window, so a node the cluster has lost stays merely
# "pfail" — invisible to the flag check in cluster_formed() — for up to 15s
# instead of 5s, tripling the window in which such a node can slip past the
# gate. What limits the damage is that cluster_formed() also talks to all six
# nodes directly, so one that is simply dead fails the gate on the next probe
# regardless of this setting (measured: unhealthy 15s after SIGKILL, while
# cluster_state stayed ok throughout). The widened window therefore only covers
# nodes that still answer us but that the cluster considers failed — a
# partition, not a crash. Nothing here exercises failover, so the trade is
# worth it; it is a trade all the same.
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
    # Every node's own view, not just 30000's: disagreement between them is
    # precisely what the readiness gate refuses to accept.
    log "--- CLUSTER NODES @ $port ---"
    redis-cli -p "$port" cluster nodes 2>&1 || log "(node $port unreachable)"
  done
  for port in $PORTS; do
    log "--- redis-server $port log (last 50 lines) ---"
    tail -n 50 "$LOG_DIR/$port.log" 2>&1 || log "(no log file for $port)"
  done
  log '===== end diagnostics ====='
}

start_nodes() {
  mkdir -p "$LOG_DIR" "$CONFIG_DIR"
  rm -f "$CONFIG_DIR"/nodes-*.conf
  for port in $PORTS; do
    # --logfile is essential: a daemonized redis-server with an empty logfile
    # sends its log to /dev/null, which is the other half of why three CI
    # sightings of this bug produced zero diagnostic detail.
    redis-server \
      --port "$port" \
      --cluster-enabled yes \
      --dir "$CONFIG_DIR" \
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
# initialised, and `--daemonize yes` returns 0 even when the bind fails.
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

# Count nodes that are cleanly a master or cleanly a replica, from one node's
# point of view. Flags are a comma-separated set, so this matches whole tokens
# rather than substrings: a grep for "master" also matches "master,fail", and
# a grep for "fail" also matches the perfectly healthy "nofailover".
healthy_role_counts() {
  awk '
    {
      n = split($3, flags, ",")
      role = ""
      degraded = 0
      for (i = 1; i <= n; i++) {
        if (flags[i] == "master") role = "master"
        else if (flags[i] == "slave") role = "slave"
        else if (flags[i] == "fail" || flags[i] == "fail?" ||
                 flags[i] == "handshake" || flags[i] == "noaddr") degraded = 1
      }
      if (degraded || role == "") next
      if (role == "master") masters++; else replicas++
    }
    END { printf "%d %d", masters + 0, replicas + 0 }
  '
}

# The real readiness gate: every node must agree the cluster is formed, every
# slot must be covered, all six nodes must know each other, and the topology
# must have settled into three clean masters and three clean replicas.
#
# Each condition catches something the others miss. cluster_state:ok alone is
# not enough: a cluster whose slots are all covered stays "ok" with a dead
# *replica*, so without the flag check the ready marker gets written over a
# degraded cluster. Roles also settle a beat after `--cluster create` returns,
# so for a short window a node reports ok while still listing a freshly
# assigned replica as a slotless master — measured at 5 masters + 1 replica at
# the exact moment `up --wait` returned, before this check existed.
cluster_formed() {
  for port in $PORTS; do
    info=$(redis-cli -p "$port" cluster info 2>/dev/null) || return 1
    echo "$info" | grep -q 'cluster_state:ok' || return 1
    echo "$info" | grep -q 'cluster_slots_assigned:16384' || return 1
    [ "$(echo "$info" | grep 'cluster_known_nodes:' | tr -dc '0-9')" = '6' ] || return 1

    nodes_view=$(redis-cli -p "$port" cluster nodes 2>/dev/null) || return 1
    [ "$(echo "$nodes_view" | healthy_role_counts)" = '3 3' ] || return 1
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

# Healthcheck mode. docker-compose.test.yml points its probe here rather than
# inlining a shell one-liner, so the probe applies exactly the gate that
# startup applied — including the flag check, which a cluster_state:ok probe
# cannot see through.
if [ "${1:-}" = 'check' ]; then
  [ -f "$READY_MARKER" ] || exit 1
  cluster_formed || exit 1
  exit 0
fi

rm -f "$READY_MARKER"

start_nodes
wait_for_nodes
create_cluster

touch "$READY_MARKER"
log 'cluster ready'
# Keep the container alive in the foreground; the redis-servers are daemonized.
# If one of them dies later the compose healthcheck re-runs cluster_formed and
# turns the container unhealthy.
exec tail -f /dev/null
