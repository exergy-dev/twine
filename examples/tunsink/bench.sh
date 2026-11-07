#!/usr/bin/env bash
# Benchmark tunsink against the goroutine-per-connection baseline.
#
# Runs entirely inside an unprivileged user+network namespace so the TUN
# device needs no root. One cell = one server process pinned to a CPU set, a
# fake Kafka broker on its own CPUs, and the load generator on the remaining
# CPUs streaming length-prefixed packets for DURATION seconds. Throughput is
# the load generator's sent frames/s; the server confirms every frame reached
# the TUN (interface RX counter) and Kafka (kafka_ok). Kafka produce blocks
# when the client buffer is full, so a Kafka cell measures the whole pipeline
# (server + franz-go client + broker) with no drops.
#
#   ./bench.sh matrix results.txt      # full matrix (~14 min)
#   ./bench.sh cell twine direct off 1core 64 512
#
# CPU layout assumes sibling threads are (0,1),(2,3),...: the server gets one
# thread per physical core. twine runs N loops pinned to the N CPUs; the
# baseline runs GOMAXPROCS=N on the same CPUs; both get N sink goroutines
# that float within the set. cpu_s is the server's CPU time over the run, so
# frames/cpu_s compares efficiency when a server does not use its budget.
# The twine server runs with GOMAXPROCS=N+1 (TWINE_GOMAXPROCS overrides):
# its loops are locked OS threads, so with GOMAXPROCS=N the sink and Kafka
# goroutines starve on a saturated loop CPU; N+1 measured best (see README).
set -euo pipefail
cd "$(dirname "$0")"
BIN=${BIN:-$(mktemp -d)/bin}
DURATION=${DURATION:-8}
LOADGEN_CPUS=${LOADGEN_CPUS:-16-31}
KAFKA_CPUS=${KAFKA_CPUS:-10,12,14}

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
#   SERVER twine|baseline   MODE direct|channel   KAFKA on|off
#   CORES 1core (CPU 2) | 2core (2,4) | 4core (2,4,6,8) | 1+1 (2,4) | 2+2 (2,4,6,8)
cell() {
  local server=$1 mode=$2 kafka=$3 cores=$4 conns=$5 size=$6
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
  if [ "$server" = twine ]; then
    GOMAXPROCS=${TWINE_GOMAXPROCS:-$((gmp+1))} taskset -c "$cpus" "$BIN/tunsink" -addr 127.0.0.1:9000 -brokers "$brokers" -mode "$mode" \
      -loops "$loops" -pin "$pin" -sinks "$sinks" -stats 0 -watchdog 0 >"$log" 2>&1 &
  else
    GOMAXPROCS=$gmp taskset -c "$cpus" "$BIN/baseline" -addr 127.0.0.1:9000 -brokers "$brokers" \
      -mode "$mode" -sinks "$sinks" -stats 1h >"$log" 2>&1 &
  fi
  local pid=$!
  sleep 0.5
  local lg; lg=$(taskset -c "$LOADGEN_CPUS" "$BIN/loadgen" -addr 127.0.0.1:9000 -conns "$conns" -size "$size" -duration "${DURATION}s")
  sleep 0.5
  local ticks cpu_s
  ticks=$(awk '{print $14+$15}' "/proc/$pid/stat")
  cpu_s=$(awk -v t="$ticks" -v hz="$(getconf CLK_TCK)" 'BEGIN{printf "%.2f", t/hz}')
  kill -TERM "$pid"; wait "$pid" || true
  local rx1; rx1=$(tun_rx)
  local sent fps mbps tot
  sent=$(sed -n 's/.*sent=\([0-9]*\).*/\1/p' <<<"$lg")
  fps=$(sed -n 's/.*frames\/s=\([0-9]*\).*/\1/p' <<<"$lg")
  mbps=$(sed -n 's/.*MB\/s=\([0-9.]*\).*/\1/p' <<<"$lg")
  tot=$(grep -o "total frames=.*" "$log" || true); grep "first produce error" "$log" >&2 || true
  local fpc; fpc=$(awk -v s="$sent" -v c="$cpu_s" 'BEGIN{if(c>0)printf "%d", s/c; else print "-"}')
  local tag=""; [ "$server" = twine ] && tag=" gomaxprocs=${TWINE_GOMAXPROCS:-$((gmp+1))}"
  printf "%-8s %-7s kafka=%-3s %-5s conns=%-4s size=%-4s frames/s=%-8s MB/s=%-7s cpu_s=%-6s frames/cpu_s=%-8s tun_rx=%-8s %s%s\n" \
    "$server" "$mode" "$kafka" "$cores" "$conns" "$size" "$fps" "$mbps" "$cpu_s" "$fpc" "$((rx1-rx0))" "$tot" "$tag"
  rm -f "$log"
}

case ${1:-matrix} in
  cell) shift; cell "$@" ;;
  matrix)
    out=${2:-/dev/stdout}
    {
      echo "# tunsink benchmark $(date -u +%FT%TZ) duration=${DURATION}s host=$(uname -r) $(lscpu | sed -n 's/Model name: *//p')"
      for cores in 1core 2core 4core 1+1 2+2; do
        for kafka in off on; do
          for conns in 64 1000; do
            for server in twine baseline; do
              for mode in direct channel; do
                cell $server $mode $kafka $cores $conns 512
              done
            done
          done
        done
      done
      # larger packets at the most common configuration
      for server in twine baseline; do cell $server direct off 1core 64 1400; cell $server direct on 1core 64 1400; done
    } | tee "$out"
    ;;
  *) echo "usage: $0 matrix [out] | cell SERVER MODE KAFKA CORES CONNS SIZE" >&2; exit 2 ;;
esac
