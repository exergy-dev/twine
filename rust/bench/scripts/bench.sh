#!/usr/bin/env bash
# Runs the standard twine benchmark matrix on this host and writes
# bench/results/<sha>[-dirty]-<UTC timestamp>.txt (and refreshes bench/results/latest.txt), then prints a markdown table.
#
#   bench/scripts/bench.sh              # whole matrix, ~4 minutes
#   DUR=5 bench/scripts/bench.sh        # shorter cells
#
# CPU layout (detected from nproc): the load generator gets the LAST $LG_CPUS cores
# (default 8, or half the machine when it has fewer than 16); servers get cores from
# the front, so the two sets are disjoint. Twine at k loops is pinned to cores 0..k-1;
# the thread-per-connection and tokio baselines and the HTTP servers are confined
# to the first $BASE_CORES cores with taskset (default min(4, cores left for servers)).
#
# Env: DUR (measured seconds per cell, default 8), WARMUP (default 1), LG_CPUS,
# LG_THREADS (loadgen threads; default = number of loadgen CPUs), BASE_CORES, LOOPS ("1 2 4 8"), PORT (base port, default 19200), RESULTS_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/bench/results}"
# "-dirty" when tracked files differ from HEAD.
git diff --quiet HEAD -- . 2>/dev/null || SHA="$SHA-dirty"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$RESULTS_DIR/$SHA-$TS.txt"
mkdir -p "$RESULTS_DIR"

DUR="${DUR:-8}"
WARMUP="${WARMUP:-1}"
PORT="${PORT:-19200}"
N="$(nproc)"
LG="${LG_CPUS:-$(( N >= 16 ? 8 : N / 2 ))}"
[ "$LG" -lt 1 ] && LG=1
SRV=$(( N - LG ))
if [ "$SRV" -lt 1 ]; then SRV=1; echo "warning: only $N cpu(s); server and load generator share cores" >&2; fi
LG_THREADS="${LG_THREADS:-$LG}"
[ "$LG_THREADS" -ne "$LG" ] && echo "warning: LG_THREADS=$LG_THREADS differs from the $LG pinned loadgen cpus" >&2
LG_FIRST=$(( N - LG ))
LG_PIN="$LG_FIRST-$(( N - 1 ))"
[ "$LG" -eq 1 ] && LG_PIN="$LG_FIRST"
BASE="${BASE_CORES:-$(( SRV < 4 ? SRV : 4 ))}"
BASE_SET="0-$(( BASE - 1 ))"
[ "$BASE" -eq 1 ] && BASE_SET="0"
LOOPS_LIST="${LOOPS:-1 2 4 8}"
command -v taskset >/dev/null || { echo "taskset not found (util-linux)" >&2; exit 1; }
ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

echo "building (release)..." >&2
cargo build --release -p twine-bench >&2
BIN="${CARGO_TARGET_DIR:-$ROOT/target}/release"

SERVER_PID=""
cleanup() { [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_port() { # port
  for _ in $(seq 100); do
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "server (pid $SERVER_PID) exited before listening on port $1; see $OUT.server.log" >&2; return 1; }
    port_open "$1" && return 0
    sleep 0.1
  done
  echo "server did not start on port $1" >&2; return 1
}
start() { # port cmd...
  local port="$1"; shift
  if port_open "$port"; then
    echo "port $port is already answering (stale server?); refusing to start" >&2; exit 1
  fi
  "$@" 2>>"$OUT.server.log" &
  SERVER_PID=$!
  wait_port "$port"
}
stop() { kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=""; sleep 0.3; }
kv() { tr ' ' '\n' <<<"$1" | awk -F= -v k="$2" '$1==k{print $2}'; }

TABLE=""
# cell <label> <config> <loadgen args...>  (server already running on $CUR_PORT)
cell() {
  local label="$1" config="$2"; shift 2
  local out res
  out="$("$BIN/loadgen" "$@" --addr "127.0.0.1:$CUR_PORT" -d "${DUR}s" --warmup "${WARMUP}s" \
        --threads "$LG_THREADS" --pin "$LG_PIN" --server-pid "$SERVER_PID")"
  { echo "### $label | $config"; echo "$out"; echo; } >> "$OUT"
  res="$(grep '^RESULT' <<<"$out")" || { echo "no RESULT line from loadgen for $label [$config]" >&2; exit 1; }
  LAST_RPS="$(kv "$res" rps)"
  local sc; sc="$(kv "$res" server_cores)"
  local per; per="$(awk -v r="$LAST_RPS" -v c="$sc" 'BEGIN{ if (c+0>0) printf "%.0f", r/c; else print "n/a" }')"
  # Flags: non-200 responses, errors, open loop not keeping up with its rate,
  # loadgen saturated (client_cores > 0.85 x threads: numbers are a lower bound).
  local flags
  flags="$(awk -v rps="$LAST_RPS" -v rate="$(kv "$res" rate)" -v n200="$(kv "$res" non200)" \
      -v err="$(kv "$res" errors)" -v cc="$(kv "$res" client_cores)" -v th="$(kv "$res" threads)" 'BEGIN{
        f="";
        if (n200+0>0) f=f " NON200";
        if (err+0>0) f=f " ERRORS";
        if (rate+0>0 && rps+0 < 0.98*rate) f=f " BELOW-RATE";
        if (cc+0 > 0.85*th) f=f " LG-SATURATED(lower-bound)";
        sub(/^ /,"",f); print f }')"
  TABLE+="| $label | $config | $LAST_RPS | $per | $(kv "$res" p50_us) | $(kv "$res" p99_us) | $(kv "$res" p999_us) | $sc | $(kv "$res" errors) | $(kv "$res" non200) | $(kv "$res" conns_used) | $flags |"$'\n'
  echo "  $label [$config]: $LAST_RPS req/s, p99 $(kv "$res" p99_us) us${flags:+  *** $flags}" >&2
}

{
  echo "twine bench  sha=$SHA  date=$(date -u +%FT%TZ)"
  echo "host: $(uname -srm), $N cpus, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
  echo "layout: loadgen threads=$LG_THREADS pinned to $LG_PIN; baselines/http servers on $BASE_SET; DUR=${DUR}s WARMUP=${WARMUP}s"
  echo
} > "$OUT"
: > "$OUT.server.log"
ln -sfn "$(basename "$OUT")" "$RESULTS_DIR/latest.txt"

# --- echo_small: 1000 connections, 64 B ping-pong -------------------------------------
CUR_PORT=$PORT
for k in $LOOPS_LIST; do
  [ "$k" -gt "$SRV" ] && { echo "skip twine $k loops (only $SRV server cpus)" >&2; continue; }
  start "$CUR_PORT" "$BIN/echo_twine" --addr "127.0.0.1:$CUR_PORT" --loops "$k" --pin "0-$(( k - 1 ))"
  cell echo_small "twine ${k} loop(s)" echo -c 1000 --size 64
  stop
done
start "$CUR_PORT" taskset -c "$BASE_SET" "$BIN/echo_threads" --addr "127.0.0.1:$CUR_PORT"
cell echo_small "threads (${BASE} cpus)" echo -c 1000 --size 64
stop
start "$CUR_PORT" taskset -c "$BASE_SET" "$BIN/echo_tokio" --addr "127.0.0.1:$CUR_PORT" --workers "$BASE"
cell echo_small "tokio (${BASE} workers)" echo -c 1000 --size 64
stop

# --- http plaintext ---------------------------------------------------------------------
declare -A PEAK
for srv in twine tokio; do
  if [ "$srv" = twine ]; then
    start "$CUR_PORT" "$BIN/http_twine" --addr "127.0.0.1:$CUR_PORT" --loops "$BASE" --pin "$BASE_SET"
  else
    start "$CUR_PORT" taskset -c "$BASE_SET" "$BIN/http_tokio" --addr "127.0.0.1:$CUR_PORT" --workers "$BASE"
  fi
  cell http "$srv ${BASE}c c=256 p=1" http -c 256 --pipeline 1
  PEAK[$srv]="$LAST_RPS"
  cell http "$srv ${BASE}c c=256 p=16" http -c 256 --pipeline 16
  cell http "$srv ${BASE}c c=10000 p=16" http -c 10000 --pipeline 16
  # Open loop at 80% of this server's own closed-loop peak (c=256, p=1).
  RATE="$(awk -v r="${PEAK[$srv]}" 'BEGIN{printf "%d", r*0.8}')"
  [ "$RATE" -gt 0 ] || { echo "closed-loop peak for $srv is '${PEAK[$srv]}'; cannot derive an open-loop rate" >&2; exit 1; }
  cell http_open "$srv ${BASE}c c=256 rate=$RATE (80% of peak)" http -c 256 --pipeline 1 --rate "$RATE"
  stop
done

# --- micro benchmarks -----------------------------------------------------------------------
T="$(taskset -c "$BASE_SET" "$BIN/timers")"
{ echo "### timers"; echo "$T"; echo; } >> "$OUT"
C="$(taskset -c "0-$(( SRV - 1 ))" "$BIN/callfromthread" --threads 16 -d 5s)"
{ echo "### callfromthread"; echo "$C"; echo; } >> "$OUT"

{
  echo "| bench | config | req/s | req/s per server core | p50 us | p99 us | p99.9 us | server cores | errors | non200 | conns used | flags |"
  echo "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
  printf '%s' "$TABLE"
  echo
  echo "latency: per request, except pipeline=16 cells where it is per batch of 16 requests"
  echo "flags: NON200 / ERRORS = bad responses; BELOW-RATE = open-loop rps < 0.98 x rate; LG-SATURATED = client_cores > 0.85 x loadgen threads (numbers are a lower bound)"
  echo
  echo "| timers (1M) | insert ns | cancel ns | insert+cancel ns | fire ns |"
  echo "| --- | ---: | ---: | ---: | ---: |"
  TR="$(grep '^RESULT' <<<"$T" || true)"
  echo "| ns/op | $(kv "$TR" insert_ns) | $(kv "$TR" cancel_ns) | $(kv "$TR" cycle_ns) | $(kv "$TR" fire_ns) |"
  echo
  CR="$(grep '^RESULT' <<<"$C" || true)"
  echo "callfromthread (16 producers): $(kv "$CR" ops_per_s) calls/s, wakeups/call = $(kv "$CR" wakeups_per_call)"
} | tee -a "$OUT"
echo "results written to $OUT" >&2
