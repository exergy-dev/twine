#!/usr/bin/env bash
# Benchmark the Go twine server (tunsink) against the Rust twine server
# (tunsink-rs) on the same pipeline, with the goroutine-per-connection
# baseline as a reference row.
#
# Runs entirely inside an unprivileged user+network namespace so the TUN
# device needs no root. One cell = one server process pinned to a CPU set, a
# fake Kafka broker on its own CPUs, and the load generator on the remaining
# CPUs streaming length-prefixed packets for DURATION seconds. Throughput is
# the load generator's sent frames/s; the server confirms every frame reached
# the TUN (interface RX counter) and Kafka (kafka_ok). Kafka produce blocks
# when the client buffer is full, so a Kafka cell measures the whole pipeline
# (server + Kafka client + broker) with no drops.
#
#   ./bench.sh matrix results.txt      # full matrix, go rust baseline (~25 min)
#   ./bench.sh quick results.txt       # go vs rust only, ~3 min
#   ./bench.sh cell SERVER MODE KAFKA CORES CONNS SIZE   # e.g. cell go direct off 1core 64 512
#   ./bench.sh compare results.txt     # go vs rust sunk/s table (+ failed checks)
#
# frames/s is the load generator's sent rate; sunk/s is the TUN RX delta over
# the loadgen's elapsed time, and check=ok|FAIL verifies |sent - delivered| <= conns*128
# (loadgen counts whole 64-frame batches only; TUN, and Kafka when on), so frames sitting in socket buffers are not credited.
#
# SERVER is go (Go twine, alias twine), rust (Rust twine) or baseline.
# Env: DURATION (8), SKIP_RUST=1 (do not build or run rust cells),
# RUST_BIN (prebuilt tunsink-rs; skips the cargo build), TWINE_GOMAXPROCS.
# Building the Rust server needs cmake on PATH ($HOME/.local/bin is prepended).
#
# CPU layout assumes sibling threads are (0,1),(2,3),...: the server gets one
# thread per physical core. go runs N loops pinned to the N CPUs; the
# baseline runs GOMAXPROCS=N on the same CPUs; both get N sink goroutines
# that float within the set. rust runs N loops pinned (-pin); its sink threads
# and the librdkafka threads float within the taskset set (no GOMAXPROCS). cpu_s is the server's CPU time over the run, so
# frames/cpu_s compares efficiency when a server does not use its budget.
# The go server runs with GOMAXPROCS=N+1 (TWINE_GOMAXPROCS overrides):
# its loops are locked OS threads, so with GOMAXPROCS=N the sink and Kafka
# goroutines starve on a saturated loop CPU; N+1 measured best (see README).
set -euo pipefail
cd "$(dirname "$0")"
BIN=${BIN:-$(mktemp -d)/bin}
SKIP_RUST=${SKIP_RUST:-0}
RUST_CRATE=../../rust/examples/tunsink
DURATION=${DURATION:-8}
LOADGEN_CPUS=${LOADGEN_CPUS:-16-31}
KAFKA_CPUS=${KAFKA_CPUS:-10,12,14}

# compare FILE: per (mode, kafka, cores, conns, size) cell, go vs rust sunk/s
# (frames/s in old files), rust/go ratio, the baseline and the servers whose check failed. Old files with "twine" rows count as go.
compare() {
  awk '
    /^#/ || NF < 7 { next }
    {
      srv = $1; if (srv == "twine") srv = "go"
      mode = $2; kafka = $3; cores = $4; conns = $5; size = $6; fps = ""
      sunk = ""; chk = ""
      for (i = 7; i <= NF; i++) {
        if ($i ~ /^frames\/s=/) { fps = $i; sub(/^frames\/s=/, "", fps) }
        if ($i ~ /^sunk\/s=/) { sunk = $i; sub(/^sunk\/s=/, "", sunk) }
        if ($i ~ /^check=/) chk = $i
      }
      if (sunk != "") fps = sunk   # old files have no sunk/s: fall back to frames/s
      if (fps == "" || fps !~ /^[0-9]+$/) next
      sub(/^kafka=/, "", kafka); sub(/^conns=/, "", conns); sub(/^size=/, "", size)
      key = mode SUBSEP kafka SUBSEP cores SUBSEP conns SUBSEP size
      if (!(key in seen)) { seen[key] = 1; order[++n] = key }
      v[key, srv] = fps
      if (chk == "check=FAIL" && index(bad[key], srv) == 0) bad[key] = bad[key] (bad[key] == "" ? "" : ",") srv
    }
    function f(x) { return x == "" ? "-" : x }
    END {
      printf "%-8s %-5s %-6s %-5s %-5s %12s %12s %8s %12s  %s\n", "mode", "kafka", "cores", "conns", "size", "go", "rust", "rust/go", "baseline", "chk"
      for (i = 1; i <= n; i++) {
        k = order[i]; split(k, p, SUBSEP)
        g = v[k, "go"]; r = v[k, "rust"]; b = v[k, "baseline"]
        ratio = (g != "" && r != "" && g > 0) ? sprintf("%.0f%%", 100 * r / g) : "-"
        printf "%-8s %-5s %-6s %-5s %-5s %12s %12s %8s %12s  %s\n", p[1], p[2], p[3], p[4], p[5], f(g), f(r), ratio, f(b), (bad[k] == "" ? "-" : bad[k])
      }
    }' "$1"
}

if [ "${1:-}" = compare ]; then compare "${2:?usage: $0 compare results.txt}"; exit; fi

if [ -z "${IN_NETNS:-}" ]; then
  mkdir -p "$BIN"
  GOTOOLCHAIN=local go build -o "$BIN/" ./... >&2
  if [ "$SKIP_RUST" != 1 ]; then
    if [ -n "${RUST_BIN:-}" ]; then
      [ -x "$RUST_BIN" ] || { echo "RUST_BIN=$RUST_BIN is not an executable file" >&2; exit 1; }
    else
      PATH=$HOME/.local/bin:$PATH cargo build --release --manifest-path "$RUST_CRATE/Cargo.toml" >&2 \
        || { echo "rust build failed (needs cmake on PATH; SKIP_RUST=1 to skip)" >&2; exit 1; }
      RUST_BIN=$RUST_CRATE/target/release/tunsink-rs
    fi
    [ -x "$RUST_BIN" ] || { echo "rust binary missing: $RUST_BIN (SKIP_RUST=1 to skip)" >&2; exit 1; }
    cp "$RUST_BIN" "$BIN/tunsink-rs"
  fi
  exec env IN_NETNS=1 BIN="$BIN" unshare -rn "$0" "$@"
fi

ip tuntap add dev tun0 mode tun
ip addr add 10.0.0.1/24 dev tun0
ip link set tun0 up
ip link set lo up
# kfake keeps every produced record in memory, so the broker is restarted
# for each Kafka cell to keep its GC cost constant across the matrix.
KAFKA_PID=
start_kafka() {
  [ -n "$KAFKA_PID" ] && { kill "$KAFKA_PID" 2>/dev/null; wait "$KAFKA_PID" 2>/dev/null || true; }
  taskset -c "$KAFKA_CPUS" "$BIN/fakekafka" -port 9092 >/dev/null 2>&1 &
  KAFKA_PID=$!
  sleep 0.7
}

tun_rx() { ip -s link show tun0 | awk 'f{print $2; exit} /RX:/{f=1}'; }

# cell SERVER MODE KAFKA CORES CONNS SIZE
#   SERVER go|rust|baseline (twine = go)  MODE direct|channel   KAFKA on|off
#   CORES 1core (CPU 2) | 2core (2,4) | 4core (2,4,6,8) | 1+1 (2,4) | 2+2 (2,4,6,8)
cell() {
  local server=$1 mode=$2 kafka=$3 cores=$4 conns=$5 size=$6
  [ "$server" = twine ] && server=go
  local brokers="" cpus loops pin sinks gmp
  [ "$kafka" = on ] && { brokers=127.0.0.1:9092; start_kafka; }
  case $cores in
    1core) cpus=2;       loops=1; pin=2;     sinks=1; gmp=1 ;;
    2core) cpus=2,4;     loops=2; pin=2,4;   sinks=2; gmp=2 ;;
    4core) cpus=2,4,6,8; loops=4; pin=2,4,6,8; sinks=4; gmp=4 ;;
    # split budgets: half the CPUs run loops, the other half is left to the
    # sink goroutines and the Kafka client (baseline: GOMAXPROCS = all CPUs)
    1+1)   cpus=2,4;     loops=1; pin=2;     sinks=1; gmp=2 ;;
    2+2)   cpus=2,4,6,8; loops=2; pin=2,4;   sinks=2; gmp=4 ;;
    *) echo "bad cores $cores" >&2; exit 2 ;;
  esac
  local log; log=$(mktemp)
  local rx0; rx0=$(tun_rx)
  if [ "$server" = rust ] && [ ! -x "$BIN/tunsink-rs" ]; then
    echo "rust binary missing: $BIN/tunsink-rs (unset SKIP_RUST or set RUST_BIN)" >&2; exit 1
  fi
  if [ "$server" = go ]; then
    GOMAXPROCS=${TWINE_GOMAXPROCS:-$((gmp+1))} taskset -c "$cpus" "$BIN/tunsink" -addr 127.0.0.1:9000 -brokers "$brokers" -mode "$mode" \
      -loops "$loops" -pin "$pin" -sinks "$sinks" -spin 0 -stats 0 -watchdog 0 >"$log" 2>&1 &
  elif [ "$server" = rust ]; then
    taskset -c "$cpus" "$BIN/tunsink-rs" -addr 127.0.0.1:9000 -brokers "$brokers" -mode "$mode" \
      -loops "$loops" -pin "$pin" -sinks "$sinks" -spin 0 -stats 0 -watchdog 0 >"$log" 2>&1 &
  else
    GOMAXPROCS=$gmp taskset -c "$cpus" "$BIN/baseline" -addr 127.0.0.1:9000 -brokers "$brokers" \
      -mode "$mode" -sinks "$sinks" -stats 1h >"$log" 2>&1 &
  fi
  local pid=$!
  sleep 0.5
  # a crashed server or failed loadgen must not abort the matrix: report "-"
  local lg; lg=$(taskset -c "$LOADGEN_CPUS" "$BIN/loadgen" -addr 127.0.0.1:9000 -conns "$conns" -size "$size" -duration "${DURATION}s" 2>/dev/null) || lg=
  # TUN RX at the moment the load generator stops: frames the server sank
  # during the measurement window. Frames still in kernel socket buffers are
  # sunk during shutdown and count toward the loss check, not the rate.
  local rx_end; rx_end=$(tun_rx)
  sleep 0.5
  local ticks cpu_s
  ticks=$(awk '{print $14+$15}' "/proc/$pid/stat" 2>/dev/null) || ticks=
  [ -n "$ticks" ] || { ticks=0; echo "warning: $server exited early ($cores $mode kafka=$kafka conns=$conns)" >&2; }
  cpu_s=$(awk -v t="$ticks" -v hz="$(getconf CLK_TCK)" 'BEGIN{printf "%.2f", t/hz}')
  kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  local rx1; rx1=$(tun_rx)
  local sent fps mbps tot
  sent=$(sed -n 's/.*sent=\([0-9]*\).*/\1/p' <<<"$lg"); sent=${sent:-0}
  fps=$(sed -n 's/.*frames\/s=\([0-9]*\).*/\1/p' <<<"$lg"); fps=${fps:--}
  mbps=$(sed -n 's/.*MB\/s=\([0-9.]*\).*/\1/p' <<<"$lg"); mbps=${mbps:--}
  tot=$(grep -o "total frames=.*" "$log" || true); grep "first produce error" "$log" >&2 || true
  local fpc; fpc=$(awk -v s="$sent" -v c="$cpu_s" 'BEGIN{if(c>0)printf "%d", s/c; else print "-"}')
  local sunk chk=ok n_tun=$((rx1-rx0)) tfr kok
  sunk=$(awk -v r="$((rx_end-rx0))" -v s="$sent" -v f="$fps" 'BEGIN{if(s>0 && f ~ /^[0-9]+$/ && f>0)printf "%d", r/(s/f); else print "-"}')
  tfr=$(sed -n 's/^total frames=\([0-9]*\).*/\1/p' <<<"$tot"); kok=$(sed -n 's/.*kafka_ok=\([0-9]*\).*/\1/p' <<<"$tot")
  if [ "$sent" -le 0 ] || [ $((sent-n_tun)) -gt $((conns*128)) ] || [ $((n_tun-sent)) -gt $((conns*128)) ]; then chk=FAIL; fi
  if [ "$kafka" = on ] && { [ -z "$tfr" ] || [ "$kok" != "$tfr" ]; }; then chk=FAIL; fi
  if [ "$chk" = FAIL ]; then
    echo "warning: check FAIL $server $cores $mode kafka=$kafka conns=$conns: sent=$sent tun_rx=$n_tun kafka_ok=${kok:--} server_frames=${tfr:--}" >&2
    # keep the server log of a failed cell for inspection
    mkdir -p "${FAIL_LOG_DIR:-/tmp}" && cp "$log" "${FAIL_LOG_DIR:-/tmp}/tunsink-fail-$server-$cores-$mode-$kafka-$conns.log" && echo "  server log: ${FAIL_LOG_DIR:-/tmp}/tunsink-fail-$server-$cores-$mode-$kafka-$conns.log" >&2
  fi
  local tag=""; [ "$server" = go ] && tag=" gomaxprocs=${TWINE_GOMAXPROCS:-$((gmp+1))}"
  printf "%-8s %-7s kafka=%-3s %-5s conns=%-4s size=%-4s frames/s=%-8s MB/s=%-7s cpu_s=%-6s frames/cpu_s=%-8s tun_rx=%-8s sunk/s=%-8s check=%-4s %s%s\n" \
    "$server" "$mode" "$kafka" "$cores" "$conns" "$size" "$fps" "$mbps" "$cpu_s" "$fpc" "$n_tun" "$sunk" "$chk" "$tot" "$tag"
  rm -f "$log"
}

case ${1:-matrix} in
  cell) shift; cell "$@" ;;
  quick)
    # ~3 minutes: go vs rust only, 4 s cells, one core in both modes with Kafka
    # off/on at 64 and 1000 connections, plus 4 cores direct at 1000.
    out=${2:-/dev/stdout}
    DURATION=${QUICK_DURATION:-4}
    {
      echo "# tunsink quick $(date -u +%FT%TZ) duration=${DURATION}s host=$(uname -r) $(lscpu | sed -n 's/Model name: *//p')"
      for kafka in off on; do
        for conns in 64 1000; do
          for mode in direct channel; do
            for server in go rust; do cell $server $mode $kafka 1core $conns 512; done
          done
        done
        for server in go rust; do cell $server direct $kafka 4core 1000 512; done
      done
    } | tee "$out"
    ;;
  matrix)
    out=${2:-/dev/stdout}
    {
      echo "# tunsink benchmark $(date -u +%FT%TZ) duration=${DURATION}s host=$(uname -r) $(lscpu | sed -n 's/Model name: *//p')"
      for cores in 1core 2core 4core 1+1 2+2; do
        for kafka in off on; do
          for conns in 64 1000; do
            for server in go rust baseline; do
              [ "$server" = rust ] && [ "$SKIP_RUST" = 1 ] && continue
              for mode in direct channel; do
                cell $server $mode $kafka $cores $conns 512
              done
            done
          done
        done
      done
      # larger packets at the most common configuration
      for server in go rust baseline; do
        [ "$server" = rust ] && [ "$SKIP_RUST" = 1 ] && continue
        cell $server direct off 1core 64 1400; cell $server direct on 1core 64 1400
      done
    } | tee "$out"
    ;;
  *) echo "usage: $0 matrix [out] | quick [out] | cell SERVER MODE KAFKA CORES CONNS SIZE | compare results.txt (sunk/s, chk) (SERVER: go|rust|baseline)" >&2; exit 2 ;;
esac
