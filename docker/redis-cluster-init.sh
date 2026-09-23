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
# TIMEOUT BUDGET — keep in sync with docker-compose.test.yml and ci.yml
# ---------------------------------------------------------------------------
# Every redis-cli call is bounded by CLI_TIMEOUT, and whenever all six nodes
# are queried they are queried IN PARALLEL (query_all), so a phase costs one
# CLI_TIMEOUT at most however many nodes are HUNG (accepting connections,
# never replying) rather than dead. Without that, one hung node would block the
# script before it ever reached its own deadline check. Worst case with the
# defaults below:
#
#   node gate    NODE_READY_TIMEOUT + one node_ready call in flight
#                                                         30 + 5        =  35s
#   per attempt  CREATE_TIMEOUT + CLUSTER_READY_TIMEOUT
#                + one cluster_formed probe in flight     45 + 30 + 5   =  80s
#   attempts     CREATE_ATTEMPTS = 3                           3 x 80   = 240s
#   between      reset_nodes + 2s backoff, x (CREATE_ATTEMPTS - 1)
#                                                         2 x (5 + 2)   =  14s
#   diagnostics  one parallel query_all; the log tails are local reads  =   5s
#                                                  35 + 240 + 14 + 5    = 294s
#
# This script is deliberately the *authoritative* budget: it is meant to lose
# first. The compose healthcheck's start_period (330s) sits above it so a
# slow-but-healthy boot can never exhaust the retry count, and the workflow's
# `docker compose up --wait --wait-timeout` (420s) sits above that purely as
# an outer backstop. Get this ordering wrong — as an earlier revision of this
# PR did, with a 968s script budget under a 240s wait-timeout — and every retry
# after the first, *and* the diagnostics dump that is the whole point of the
# exercise, become unreachable: compose gives up first and you are back to
# #398's original symptom with a nicer message. If you change a constant here,
# recheck both of those files.
set -eu

PORTS='30000 30001 30002 30003 30004 30005'
LOG_DIR='/var/log/redis-cluster'
# Each node gets its OWN working directory, $DATA_DIR/<port>, wiped on every
# boot. /data is a declared VOLUME *and* the image's WORKDIR, so anything a node
# writes there survives `docker restart`. Two kinds of file matter:
#   - nodes-<port>.conf: a stale topology makes the node boot already believing
#     it is in a cluster;
#   - dump.rdb: `--save ''` stops snapshots, but a REPLICA still writes the RDB
#     it receives during full sync to disk. With one shared dir, all six nodes
#     then LOAD that same file on the next boot, and any node holding keys is
#     "not empty" too.
# Either one fails the create with "[ERR] Node ... is not empty" and burns an
# attempt. Separate dirs stop the six nodes sharing one dump.rdb; the wipe
# stops a node reloading its own. (AOF is off, but an appendonlydir would land
# in the same per-node dir and be wiped with it.)
DATA_DIR="${DATA_DIR:-/data}"
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
# becomes another retryable attempt. A healthy create finishes in seconds.
CREATE_TIMEOUT="${CREATE_TIMEOUT:-45}"
# Per redis-cli call. redis-cli's own `-t` is only a CONNECT timeout, and a
# hung node still completes the TCP handshake from the kernel's listen backlog
# before never replying — so `-t` would not bound it. coreutils `timeout` does.
CLI_TIMEOUT="${CLI_TIMEOUT:-5}"
# 5000ms is what made bus links flap under CI load, and a flapping link is
# exactly what makes redis-cli's join consistency check give up. 15s buys that
# stability at a real cost, not for free. When a node stops responding, its
# peers go through three stages: no flag at all until cluster-node-timeout
# elapses, then `fail?` (PFAIL — this node's own suspicion), then `fail` once a
# majority of masters agree. The flag check in cluster_formed() rejects both
# `fail?` and `fail`, so the only blind stage is the first one — and its length
# IS cluster-node-timeout, so raising it grows the blind window from ~5s to
# ~15s. What limits the damage is that cluster_formed() also talks to all six
# nodes directly, so one that is simply dead or hung fails the gate on the next
# probe regardless of this setting (measured: unhealthy ~15s after SIGKILL,
# while cluster_state stayed ok throughout). The blind window therefore only
# matters for a node that still answers us but that its peers have lost — a
# partition, not a crash. Nothing here exercises failover, so the trade is
# worth it; it is a trade all the same.
CLUSTER_NODE_TIMEOUT="${CLUSTER_NODE_TIMEOUT:-15000}"

log() {
  echo "[cluster-init] $*"
}

now() {
  date +%s
}

# Every redis-cli call in this script goes through here.
rcli() {
  timeout "$CLI_TIMEOUT" redis-cli "$@"
}

# Send the same commands to all six nodes at once, pipelined into ONE redis-cli
# call per node, each node's reply landing in "$1/<port>". Running the nodes in
# parallel is what makes the timeout budget hold with a hung node: the whole
# round costs one CLI_TIMEOUT, not one per node.
#
# Two properties of redis-cli's non-tty stdin mode (checked against the image)
# shape how callers must read the result:
#   - an error reply mid-pipeline does NOT abort the commands after it;
#   - a refused connection prints to stderr and still EXITS 0, and a hung node
#     killed by `timeout` leaves the file empty. The exit status is therefore
#     worthless here — callers judge the reply content, never the status.
query_all() {
  dir=$1
  shift
  for port in $PORTS; do
    printf '%s\n' "$@" | rcli -p "$port" >"$dir/$port" 2>&1 &
  done
  wait
}

# The dump must itself survive a hung node, or the one failure mode it exists
# for would silence it — hence query_all, and the output still prints in port
# order rather than interleaved.
dump_diagnostics() {
  log '===== diagnostics ====='
  diag_dir=$(mktemp -d)
  query_all "$diag_dir" 'CLUSTER INFO' 'CLUSTER NODES'
  for port in $PORTS; do
    # Every node's own view, not just 30000's: disagreement between them is
    # precisely what the readiness gate refuses to accept.
    log "--- CLUSTER INFO + CLUSTER NODES @ $port ---"
    # Print whatever came back — for a refused connection that is the error
    # text — then judge by CONTENT, as judge_views does. File size is useless:
    # under QEMU every redis-cli prints jemalloc warnings to stderr, so even a
    # hung node's file is non-empty.
    cat "$diag_dir/$port"
    grep -q '^cluster_state:' "$diag_dir/$port" ||
      log "(no CLUSTER INFO reply from node $port within ${CLI_TIMEOUT}s: down or hung)"
  done
  rm -rf "$diag_dir"
  for port in $PORTS; do
    log "--- redis-server $port log (last 50 lines) ---"
    tail -n 50 "$LOG_DIR/$port.log" 2>&1 || log "(no log file for $port)"
  done
  log '===== end diagnostics ====='
}

start_nodes() {
  mkdir -p "$LOG_DIR"
  for port in $PORTS; do
    rm -rf "${DATA_DIR:?}/$port"
    mkdir -p "$DATA_DIR/$port"
  done
  for port in $PORTS; do
    # --logfile is essential: a daemonized redis-server with an empty logfile
    # sends its log to /dev/null, which is the other half of why three CI
    # sightings of this bug produced zero diagnostic detail.
    redis-server \
      --port "$port" \
      --cluster-enabled yes \
      --dir "$DATA_DIR/$port" \
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

# A node counts as ready once it answers INFO *and* reports that it came up in
# cluster mode. A reply at all proves the node is bound and serving (so no
# separate PING); cluster_enabled:1 proves the cluster subsystem is up, which a
# PING alone would not. Remember `--daemonize yes` returns 0 even when the bind
# fails, so this is the only real evidence a node exists.
node_ready() {
  # cluster_enabled lives in INFO's cluster section, not in CLUSTER INFO.
  rcli -p "$1" info cluster 2>/dev/null | grep -q 'cluster_enabled:1'
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

# Judge all six nodes' CLUSTER INFO + CLUSTER NODES replies in ONE awk pass.
# Run it inside the query_all directory with the ports as arguments, so each
# reply file's name IS its port. Prints "ok", or the first reason a node is
# not ready (which wait_for_cluster logs when a settle window runs out).
#
# Every node must independently report, from its own point of view:
#   cluster_state:ok, cluster_slots_assigned:16384, cluster_known_nodes:6,
#   and exactly 3 clean masters + 3 clean replicas.
#
# Flags are a comma-separated set, so roles match whole tokens rather than
# substrings: a grep for "master" also matches "master,fail", and a grep for
# "fail" also matches the perfectly healthy "nofailover". A node flagged
# fail / fail? / handshake / noaddr is not counted at all.
#
# CLUSTER INFO lines end in \r\n while CLUSTER NODES lines end in \n, so \r is
# stripped first. INFO lines are key:value (a single field); NODES lines have
# at least 8 fields, and only those reach the role logic. A node that never
# replied leaves an empty file, which awk reads no records from — so the END
# block walks the EXPECTED list rather than the files it happened to see.
judge_views() {
  awk -v expected_list="$*" '
    { sub(/\r$/, "") }
    /^cluster_state:/          { split($0, kv, ":"); state[FILENAME] = kv[2]; next }
    /^cluster_slots_assigned:/ { split($0, kv, ":"); slots[FILENAME] = kv[2]; next }
    /^cluster_known_nodes:/    { split($0, kv, ":"); known[FILENAME] = kv[2]; next }
    NF >= 8 {
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
      if (role == "master") masters[FILENAME]++; else replicas[FILENAME]++
    }
    END {
      nexpected = split(expected_list, expected, " ")
      for (i = 1; i <= nexpected; i++) {
        f = expected[i]
        if (state[f] == "")      { printf "node %s: no CLUSTER INFO reply", f; exit }
        if (state[f] != "ok")    { printf "node %s: cluster_state=%s", f, state[f]; exit }
        if (slots[f] != "16384") { printf "node %s: slots_assigned=%s", f, slots[f]; exit }
        if (known[f] != "6")     { printf "node %s: known_nodes=%s", f, known[f]; exit }
        if (masters[f] + 0 != 3 || replicas[f] + 0 != 3) {
          printf "node %s: sees %d clean masters + %d clean replicas", f, masters[f], replicas[f]
          exit
        }
      }
      printf "ok"
    }
  ' "$@" 2>/dev/null
}

# The real readiness gate. Each condition in judge_views catches something the
# others miss. cluster_state:ok alone is not enough: a cluster whose slots are
# all covered stays "ok" with a dead *replica*, so without the flag check the
# ready marker gets written over a degraded cluster. Roles also settle a beat
# after `--cluster create` returns, so for a short window a node reports ok
# while still listing a freshly assigned replica as a slotless master —
# measured at 5 masters + 1 replica at the exact moment `up --wait` returned,
# before this check existed.
#
# One probe is one parallel query_all plus one awk: about nine processes, and
# at most one CLI_TIMEOUT of wall time. An earlier revision made twelve serial
# redis-cli calls plus a grep/tr/awk chain per node — some fifty processes.
# Natively that is instant; under QEMU with the CPU saturated a single PASSING
# probe measured 15s, so a hard cap on the probe starved a healthy cluster of
# a pass. Making the probe cheap is the fix, not a bigger cap.
#
# Leaves the verdict in $last_verdict for the caller to log.
cluster_formed() {
  probe_dir=$(mktemp -d) || return 1
  query_all "$probe_dir" 'CLUSTER INFO' 'CLUSTER NODES'
  # shellcheck disable=SC2086 # $PORTS is a deliberately split list
  last_verdict=$(cd "$probe_dir" && judge_views $PORTS)
  rm -rf "$probe_dir"
  [ "$last_verdict" = 'ok' ]
}

wait_for_cluster() {
  deadline=$(($(now) + CLUSTER_READY_TIMEOUT))
  until cluster_formed; do
    if [ "$(now)" -ge "$deadline" ]; then
      log "not settled after ${CLUSTER_READY_TIMEOUT}s; last probe said: ${last_verdict:-nothing}"
      return 1
    fi
    sleep 0.5
  done
}

# Between attempts, wipe whatever the failed attempt left behind. Without this a
# retry dies immediately on "Node 127.0.0.1:3000x is not empty". One pipelined
# call per node, all in parallel, so a hung node costs one CLI_TIMEOUT in total.
# FLUSHALL errors on a replica (READONLY); that does not stop the RESET after it
# in the same pipeline, and RESET HARD flushes a replica anyway.
reset_nodes() {
  reset_dir=$(mktemp -d) || return 0
  query_all "$reset_dir" 'FLUSHALL' 'CLUSTER RESET HARD'
  rm -rf "$reset_dir"
}

create_cluster() {
  nodes=''
  for port in $PORTS; do
    nodes="$nodes 127.0.0.1:$port"
  done

  attempt=1
  while :; do
    log "forming cluster (attempt $attempt/$CREATE_ATTEMPTS)"
    created=0
    # shellcheck disable=SC2086 # $nodes is a deliberately split argument list
    timeout "$CREATE_TIMEOUT" redis-cli --cluster create $nodes --cluster-replicas 1 --cluster-yes && created=1
    # A create killed mid "Waiting for the cluster to join...." leaves its dots
    # with no trailing newline, which glued the next [cluster-init] line onto
    # them and hid it from anyone grepping the log — on exactly the failure
    # path the log exists for. Always start our own output on a fresh line.
    echo
    if [ "$created" = 1 ]; then
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
# cannot see through. One probe costs at most one CLI_TIMEOUT (5s), so even a
# hung node yields a clean failure well inside the healthcheck's own timeout.
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
