#!/usr/bin/env bash
# Benchmark the twine server (tunsink) on the TCP -> TUN + Kafka pipeline,
# with the goroutine-per-connection baseline as a reference row.
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
#   ./bench.sh matrix results.txt      # full matrix, go and baseline (~17 min, AES off)
#   ./bench.sh quick results.txt       # go only, AES off and on, ~2.5 min
#   ./bench.sh cell SERVER MODE KAFKA AES CORES CONNS SIZE   # e.g. cell go direct off on 1core 64 512
#   ./bench.sh compare results.txt     # go vs baseline sunk/s table (+ failed checks)
#
# AES is on|off: with on, the load generator encrypts every frame with
# AES-256-GCM (12-byte nonce || ciphertext || 16-byte tag, fixed key) and the
# server decrypts it on the loop before the TUN write and the Kafka produce; a
# tag failure counts decrypt_err and fails the check. AES_LIST overrides the
# AES values quick ("off on") and matrix ("off") iterate over.
#
# frames/s is the load generator's sent rate; sunk/s is the TUN RX delta over
# the loadgen's elapsed time, and check=ok|FAIL verifies |sent - delivered| <= conns*128
# (loadgen counts whole 64-frame batches only; TUN, and Kafka when on) and
# decrypt_err=0, so frames sitting in socket buffers are not credited.
#
# SERVER is go (the twine server, alias twine) or baseline.
# Env: DURATION (8), TWINE_GOMAXPROCS, LOADGEN_KEY (wrong-key negative check,
# see cell).
#
# CPU layout assumes sibling threads are (0,1),(2,3),...: the server gets one
# thread per physical core. go runs N loops pinned to the N CPUs; the
# baseline runs GOMAXPROCS=N on the same CPUs; both get N sink goroutines
# that float within the set. cpu_s is the server's CPU time over the run, so
# frames/cpu_s compares efficiency when a server does not use its budget.
# The go server runs with GOMAXPROCS=N+1 (TWINE_GOMAXPROCS overrides):
# its loops are locked OS threads, so with GOMAXPROCS=N the sink and Kafka
# goroutines starve on a saturated loop CPU; N+1 measured best (see README).
set -euo pipefail
cd "$(dirname "$0")"
BIN=${BIN:-$(mktemp -d)/bin}
DURATION=${DURATION:-8}
LOADGEN_CPUS=${LOADGEN_CPUS:-16-31}
KAFKA_CPUS=${KAFKA_CPUS:-10,12,14}

# compare FILE: per (mode, kafka, aes, cores, conns, size) cell, go vs baseline
# sunk/s (frames/s in old files), baseline/go ratio and the servers whose
# check failed. Old files: "twine" rows count as go, missing aes= counts as off.
compare() {
  awk '
    /^#/ || NF < 7 { next }
    {
      srv = $1; if (srv == "twine") srv = "go"
      mode = $2; kafka = ""; aes = "off"; cores = ""; conns = ""; size = ""; fps = ""
      sunk = ""; chk = ""
      for (i = 3; i <= NF; i++) {
        if ($i ~ /^kafka=/) { kafka = $i; sub(/^kafka=/, "", kafka) }
        else if ($i ~ /^aes=/) { aes = $i; sub(/^aes=/, "", aes) }
        else if ($i ~ /^conns=/) { conns = $i; sub(/^conns=/, "", conns) }
        else if ($i ~ /^size=/) { size = $i; sub(/^size=/, "", size) }
        else if ($i ~ /^frames\/s=/) { fps = $i; sub(/^frames\/s=/, "", fps) }
        else if ($i ~ /^sunk\/s=/) { sunk = $i; sub(/^sunk\/s=/, "", sunk) }
        else if ($i ~ /^check=/) chk = $i
        else if (cores == "" && $i !~ /=/) cores = $i
        if ($i ~ /^frames\/s=/) break
      }
      for (; i <= NF; i++) {
        if ($i ~ /^sunk\/s=/) { sunk = $i; sub(/^sunk\/s=/, "", sunk) }
        if ($i ~ /^check=/) chk = $i
      }
      if (sunk != "") fps = sunk   # old files have no sunk/s: fall back to frames/s
      key = mode SUBSEP kafka SUBSEP aes SUBSEP cores SUBSEP conns SUBSEP size
      if (!(key in seen)) { seen[key] = 1; order[++n] = key }
      if (chk == "check=FAIL" && index(bad[key], srv) == 0) bad[key] = bad[key] (bad[key] == "" ? "" : ",") srv
      if (fps == "" || fps !~ /^[0-9]+$/) next   # crashed cell: no rate, but its FAIL is kept
      v[key, srv] = fps
    }
    function f(x) { return x == "" ? "-" : x }
    END {
      printf "%-8s %-5s %-4s %-6s %-5s %-5s %12s %12s %8s  %s\n", "mode", "kafka", "aes", "cores", "conns", "size", "go", "baseline", "base/go", "chk"
      for (i = 1; i <= n; i++) {
        k = order[i]; split(k, p, SUBSEP)
        g = v[k, "go"]; b = v[k, "baseline"]
        ratio = (g != "" && b != "" && g > 0) ? sprintf("%.0f%%", 100 * b / g) : "-"
        printf "%-8s %-5s %-4s %-6s %-5s %-5s %12s %12s %8s  %s\n", p[1], p[2], p[3], p[4], p[5], p[6], f(g), f(b), ratio, (bad[k] == "" ? "-" : bad[k])
      }
    }' "$1"
}

if [ "${1:-}" = compare ]; then compare "${2:?usage: $0 compare results.txt}"; exit; fi

if [ -z "${IN_NETNS:-}" ]; then
  mkdir -p "$BIN"
  GOTOOLCHAIN=local go build -o "$BIN/" ./... >&2
  exec env IN_NETNS=1 BIN="$BIN" unshare -rn "$0" "$@"
fi

ip tuntap add dev tun0 mode tun
ip addr add 10.0.0.1/24 dev tun0
ip link set tun0 up
ip link set lo up
# kfake keeps every produced record in memory, so the broker is restarted
# for each Kafka cell to keep its GC cost constant across the matrix, and the
# last one is killed on exit (a leaked broker holds gigabytes: 54 of them from
# earlier runs once ate 28 GB and skewed every cell).
KAFKA_PID=
SERVER_PID=
stop_kafka() {
  [ -n "$KAFKA_PID" ] && { kill "$KAFKA_PID" 2>/dev/null; wait "$KAFKA_PID" 2>/dev/null || true; }
  KAFKA_PID=
}
# On exit (normal, set -e, exit N or a signal) stop the broker and a server
# still running in an aborted cell (the server exits gracefully, within its
# 5 s shutdown timeout plus the Kafka flush). Bash resets traps in subshells, so the
# `{ ... } | tee` blocks of quick and matrix install it again inside.
cleanup() { stop_kafka; [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null; SERVER_PID=; }
# An untrapped SIGINT is ignored while bash waits on a child and an untrapped
# SIGTERM skips the EXIT trap, so both are turned into an exit (after the
# current foreground command, at most one loadgen run).
set_traps() { trap cleanup EXIT; trap 'exit 130' INT; trap 'exit 143' TERM; }
set_traps
start_kafka() {
  stop_kafka
  taskset -c "$KAFKA_CPUS" "$BIN/fakekafka" -port 9092 >/dev/null 2>&1 &
  KAFKA_PID=$!
  sleep 0.7
}

tun_rx() { ip -s link show tun0 | awk 'f{print $2; exit} /RX:/{f=1}'; }

# cell SERVER MODE KAFKA AES CORES CONNS SIZE
#   SERVER go|baseline (twine = go)  MODE direct|channel   KAFKA on|off   AES on|off
#   CORES 1core (CPU 2) | 2core (2,4) | 4core (2,4,6,8) | 1+1 (2,4) | 2+2 (2,4,6,8)
cell() {
  local server=$1 mode=$2 kafka=$3 aes=$4 cores=$5 conns=$6 size=$7
  [ "$server" = twine ] && server=go
  local brokers="" cpus loops pin sinks gmp aesflag=()
  case $aes in on) aesflag=(-aes) ;; off) ;; *) echo "bad aes $aes (on|off)" >&2; exit 2 ;; esac
  [ "$kafka" = on ] && { brokers=127.0.0.1:9092; start_kafka; }
  # LOADGEN_KEY=HEX gives the load generator a different key: a negative check
  # that the servers really decrypt (expect check=FAIL, decrypt_err=frames).
  local lgaes=("${aesflag[@]}"); [ "$aes" = on ] && [ -n "${LOADGEN_KEY:-}" ] && lgaes+=(-key "$LOADGEN_KEY")
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
  if [ "$server" = go ]; then
    GOMAXPROCS=${TWINE_GOMAXPROCS:-$((gmp+1))} taskset -c "$cpus" "$BIN/tunsink" -addr 127.0.0.1:9000 -brokers "$brokers" -mode "$mode" \
      -loops "$loops" -pin "$pin" -sinks "$sinks" -spin 0 -stats 0 -watchdog 0 "${aesflag[@]}" >"$log" 2>&1 &
  else
    GOMAXPROCS=$gmp taskset -c "$cpus" "$BIN/baseline" -addr 127.0.0.1:9000 -brokers "$brokers" \
      -mode "$mode" -sinks "$sinks" -stats 1h "${aesflag[@]}" >"$log" 2>&1 &
  fi
  local pid=$!
  SERVER_PID=$pid
  sleep 0.5
  # a crashed server or failed loadgen must not abort the matrix: report "-"
  local lg; lg=$(taskset -c "$LOADGEN_CPUS" "$BIN/loadgen" -addr 127.0.0.1:9000 -conns "$conns" -size "$size" -duration "${DURATION}s" "${lgaes[@]}" 2>/dev/null) || lg=
  # TUN RX at the moment the load generator stops: frames the server sank
  # during the measurement window. Frames still in kernel socket buffers are
  # sunk during shutdown and count toward the loss check, not the rate.
  local rx_end; rx_end=$(tun_rx)
  sleep 0.5
  local ticks cpu_s
  ticks=$(awk '{print $14+$15}' "/proc/$pid/stat" 2>/dev/null) || ticks=
  [ -n "$ticks" ] || { ticks=0; echo "warning: $server exited early ($cores $mode kafka=$kafka conns=$conns)" >&2; }
  cpu_s=$(awk -v t="$ticks" -v hz="$(getconf CLK_TCK)" 'BEGIN{printf "%.2f", t/hz}')
  kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; SERVER_PID=
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
  local derr; derr=$(sed -n 's/.*decrypt_err=\([0-9]*\).*/\1/p' <<<"$tot")
  if [ "$sent" -le 0 ] || [ $((sent-n_tun)) -gt $((conns*128)) ] || [ $((n_tun-sent)) -gt $((conns*128)) ]; then chk=FAIL; fi
  if [ "$kafka" = on ] && { [ -z "$tfr" ] || [ "$kok" != "$tfr" ]; }; then chk=FAIL; fi
  # a server that decrypts must report the counter, and it must be zero
  if [ "$aes" = on ] && { [ -z "$derr" ] || [ "$derr" != 0 ]; }; then chk=FAIL; fi
  if [ "$chk" = FAIL ]; then
    echo "warning: check FAIL $server $cores $mode kafka=$kafka aes=$aes conns=$conns: sent=$sent tun_rx=$n_tun kafka_ok=${kok:--} server_frames=${tfr:--} decrypt_err=${derr:--}" >&2
    # keep the server log of a failed cell for inspection
    mkdir -p "${FAIL_LOG_DIR:-/tmp}" && cp "$log" "${FAIL_LOG_DIR:-/tmp}/tunsink-fail-$server-$cores-$mode-$kafka-$aes-$conns.log" && echo "  server log: ${FAIL_LOG_DIR:-/tmp}/tunsink-fail-$server-$cores-$mode-$kafka-$aes-$conns.log" >&2
  fi
  local tag=""; [ "$server" = go ] && tag=" gomaxprocs=${TWINE_GOMAXPROCS:-$((gmp+1))}"
  printf "%-8s %-7s kafka=%-3s aes=%-3s %-5s conns=%-4s size=%-4s frames/s=%-8s MB/s=%-7s cpu_s=%-6s frames/cpu_s=%-8s tun_rx=%-8s sunk/s=%-8s check=%-4s %s%s\n" \
    "$server" "$mode" "$kafka" "$aes" "$cores" "$conns" "$size" "$fps" "$mbps" "$cpu_s" "$fpc" "$n_tun" "$sunk" "$chk" "$tot" "$tag"
  rm -f "$log"
}

case ${1:-matrix} in
  cell) shift; cell "$@" ;;
  quick)
    # ~2.5 minutes: go only, 4 s cells, AES off and on, one core in
    # both modes with Kafka off/on at 64 and 1000 connections, plus 4 cores
    # direct at 1000.
    out=${2:-/dev/stdout}
    DURATION=${QUICK_DURATION:-4}
    {
      set_traps
      echo "# tunsink quick $(date -u +%FT%TZ) duration=${DURATION}s host=$(uname -r) $(lscpu | sed -n 's/Model name: *//p')"
      for aes in ${AES_LIST:-off on}; do
        for kafka in off on; do
          for conns in 64 1000; do
            for mode in direct channel; do
              cell go $mode $kafka $aes 1core $conns 512
            done
          done
          cell go direct $kafka $aes 4core 1000 512
        done
      done
    } | tee "$out"
    ;;
  matrix)
    out=${2:-/dev/stdout}
    {
      set_traps
      echo "# tunsink benchmark $(date -u +%FT%TZ) duration=${DURATION}s host=$(uname -r) $(lscpu | sed -n 's/Model name: *//p')"
      for aes in ${AES_LIST:-off}; do
        for cores in 1core 2core 4core 1+1 2+2; do
          for kafka in off on; do
            for conns in 64 1000; do
              for server in go baseline; do
                for mode in direct channel; do
                  cell $server $mode $kafka $aes $cores $conns 512
                done
              done
            done
          done
        done
        # larger packets at the most common configuration
        for server in go baseline; do
          cell $server direct off $aes 1core 64 1400; cell $server direct on $aes 1core 64 1400
        done
      done
    } | tee "$out"
    ;;
  *) echo "usage: $0 matrix [out] | quick [out] | cell SERVER MODE KAFKA AES CORES CONNS SIZE | compare results.txt (sunk/s, chk) (SERVER: go|baseline; KAFKA, AES: on|off)" >&2; exit 2 ;;
esac
