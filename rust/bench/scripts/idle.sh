#!/usr/bin/env bash
# Idle-connection memory test: bench/scripts/idle.sh N
#
# Starts idle_server (2 loops pinned to $SRV_CPUS), opens N idle connections with
# ceil(N/250000) idle_client processes (disjoint source address ranges on 127.0.0.0/8),
# holds 60 s sampling the server RSS every 10 s, pings a random sample on each client,
# then stops everything and appends a summary to bench/results/idle.txt.
# For N > 200000 it re-executes itself inside `unshare -Urn` (private netns) because the
# root namespace's nf_conntrack table caps at nf_conntrack_max (262144).
#
# Env: SRV_CPUS (default 28,29; ignored count for LOOPS), CLI_CPUS (default 30,31), RATE (total connects/s,
# 40000), LOOPS (server loops, default 2), HOLD (seconds, 60), SAMPLE (per client, 500), PORT (19900), MAX_RSS_MB (8192).
set -euo pipefail

N="${1:?usage: idle.sh N}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target/idle}"
BIN="$TARGET/release"
OUT="$ROOT/bench/results/idle.txt"
SRV_CPUS="${SRV_CPUS:-28,29}"
CLI_CPUS="${CLI_CPUS:-30,31}"
RATE="${RATE:-40000}"
HOLD="${HOLD:-60}"
SAMPLE="${SAMPLE:-500}"
PORT="${PORT:-19900}"
LOOPS="${LOOPS:-2}"
MAX_RSS_MB="${MAX_RSS_MB:-8192}"
mkdir -p "$ROOT/bench/results"

if [ "$N" -gt 200000 ] && [ -z "${IDLE_IN_NETNS:-}" ]; then
  if unshare -Urn true 2>/dev/null; then
    exec unshare -Urn env IDLE_IN_NETNS=1 bash -c 'ip link set lo up && exec "$0" "$@"' "$0" "$@"
  fi
  echo "unshare -Urn unavailable: running in the root netns (conntrack cap applies)" >&2
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/idle.XXXXXX")"
SRV_PID=""
CLI_PIDS=()
cleanup() {
  for p in "${CLI_PIDS[@]:-}" "$SRV_PID"; do [ -n "$p" ] && kill -KILL "$p" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

rss_kb() { awk '/VmRSS/ {print $2}' "/proc/$SRV_PID/status"; }
note() { echo "$@" | tee -a "$OUT"; }
abort() { note "ABORT: $*"; exit 1; }

PROCS=$(( (N + 249999) / 250000 ))
PER=$(( N / PROCS ))
NSRC=32
[ "$PROCS" -eq 1 ] && NSRC=64

{
  echo "=== idle test N=$N $(date -Is) host=$(uname -r) netns=${IDLE_IN_NETNS:-root} ==="
  echo "limits: ulimit -Hn=$(ulimit -Hn) ulimit -Sn=$(ulimit -Sn) nr_open=$(cat /proc/sys/fs/nr_open)"
  echo "conntrack_max=$(sysctl -n net.netfilter.nf_conntrack_max 2>/dev/null || echo n/a)" \
    "port_range=$(cat /proc/sys/net/ipv4/ip_local_port_range) somaxconn=$(cat /proc/sys/net/core/somaxconn)"
  echo "MemAvailable_kB=$(awk '/MemAvailable/ {print $2}' /proc/meminfo) client_procs=$PROCS conns_per_proc=$PER"
} | tee -a "$OUT"

taskset -c "$SRV_CPUS" "$BIN/idle_server" --addr "127.0.0.1:$PORT" --loops "$LOOPS" --report 10s \
  >"$WORK/server.log" 2>&1 &
SRV_PID=$!
for _ in $(seq 100); do grep -q listening "$WORK/server.log" && break; sleep 0.1; done
grep -q listening "$WORK/server.log" || abort "server did not start: $(cat "$WORK/server.log")"
sleep 1
RSS_BEFORE=$(( $(rss_kb) * 1024 ))
note "rss_before_bytes=$RSS_BEFORE"

T0=$(date +%s)
for i in $(seq 0 $((PROCS - 1))); do
  lo=$(( 2 + i * NSRC )); hi=$(( lo + NSRC - 1 ))
  taskset -c "$CLI_CPUS" "$BIN/idle_client" -c "$PER" --addr "127.0.0.1:$PORT" --threads 2 \
    --src "127.0.0.$lo-127.0.0.$hi" --rate $(( RATE / PROCS )) --hold 100000 \
    --go "$WORK/go" --done "$WORK/done" --sample "$SAMPLE" >"$WORK/client$i.log" 2>"$WORK/client$i.err" &
  CLI_PIDS+=($!)
done

# Wait for establishment: every client prints ESTABLISHED (clients abort on a 60 s stall).
while :; do
  ok=0
  for i in $(seq 0 $((PROCS - 1))); do grep -q ESTABLISHED "$WORK/client$i.log" && ok=$((ok + 1)); done
  [ "$ok" -eq "$PROCS" ] && break
  kill -0 "$SRV_PID" 2>/dev/null || abort "server died"
  [ $(( $(rss_kb) / 1024 )) -gt "$MAX_RSS_MB" ] && abort "server RSS above ${MAX_RSS_MB} MB"
  [ "$(awk '/MemAvailable/ {print $2}' /proc/meminfo)" -lt 3000000 ] && abort "MemAvailable below 3 GB"
  sleep 1
done
T1=$(date +%s)
sleep 3
RSS_EST=$(( $(rss_kb) * 1024 ))
note "established_in_secs=$((T1 - T0)) rss_after_establish_bytes=$RSS_EST"
cat "$WORK"/client*.log | grep ESTABLISHED | tee -a "$OUT"

# Hold, sampling the server RSS every 10 s.
SAMPLES=()
for t in $(seq 10 10 "$HOLD"); do
  sleep 10
  SAMPLES+=($(( $(rss_kb) * 1024 )))
  note "hold t=${t}s rss_bytes=${SAMPLES[-1]}"
done
RSS_HOLD=$(( $(rss_kb) * 1024 ))

# Echo sampling on each client, then the server's own RESULT while still connected.
touch "$WORK/go"
for i in $(seq 0 $((PROCS - 1))); do
  for _ in $(seq 600); do grep -q RESULT "$WORK/client$i.log" && break; sleep 0.5; done
done
cat "$WORK"/client*.log | grep RESULT | tee -a "$OUT"
kill -TERM "$SRV_PID"
for _ in $(seq 100); do grep -q '^RESULT' "$WORK/server.log" && break; sleep 0.1; done
grep -E '^(REPORT|RESULT)' "$WORK/server.log" | tee -a "$OUT"
touch "$WORK/done"
wait 2>/dev/null || true

SRV_RESULT=$(grep '^RESULT' "$WORK/server.log" || true)
CONNS=$(sed -n 's/.*conns=\([0-9]*\).*/\1/p' <<<"$SRV_RESULT")
CONNS="${CONNS:-0}"
DELTA=$(( RSS_HOLD - RSS_BEFORE ))
note "SUMMARY N=$N server_conns=$CONNS rss_before=$RSS_BEFORE rss_established=$RSS_EST rss_end_hold=$RSS_HOLD" \
  "bytes_per_conn_delta=$(( DELTA / (CONNS > 0 ? CONNS : 1) )) bytes_per_conn_total=$(( RSS_HOLD / (CONNS > 0 ? CONNS : 1) ))" \
  "hold_rss_min=$(printf '%s\n' "${SAMPLES[@]}" | sort -n | head -1) hold_rss_max=$(printf '%s\n' "${SAMPLES[@]}" | sort -n | tail -1)"
echo >>"$OUT"
