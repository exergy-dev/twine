#!/usr/bin/env bash
# Runs every cargo-fuzz target in parallel for N seconds (default 600) and
# prints a summary table. Exits non-zero if any target crashed or failed.
#   scripts/fuzz.sh [seconds]
set -u
SECS="${1:-600}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 2
export CARGO_TARGET_DIR="$ROOT/fuzz/target"
TARGETS=(line netstring lengthprefix http_parse)
LOGDIR="$(mktemp -d)"
trap 'rm -rf "$LOGDIR"' EXIT

# Build once up front so the parallel runs do not fight over the cargo lock.
for t in "${TARGETS[@]}"; do
  cargo +nightly fuzz build "$t" >"$LOGDIR/build.$t.log" 2>&1 || {
    echo "build of $t failed:"; cat "$LOGDIR/build.$t.log"; exit 2; }
done

pids=()
for t in "${TARGETS[@]}"; do
  ( cargo +nightly fuzz run "$t" -- -max_total_time="$SECS" -timeout=10 \
      >"$LOGDIR/$t.log" 2>&1; echo $? >"$LOGDIR/$t.rc" ) &
  pids+=($!)
done
wait "${pids[@]}"

fail=0
printf '\n%-14s %14s %8s %s\n' TARGET EXECS CRASHES NOTE
for t in "${TARGETS[@]}"; do
  log="$LOGDIR/$t.log"
  rc="$(cat "$LOGDIR/$t.rc" 2>/dev/null || echo 99)"
  # Last "#N <status> cov:" line carries the cumulative execution count.
  execs="$(grep -E '^#[0-9]+' "$log" | tail -n1 | sed -E 's/^#([0-9]+).*/\1/')"
  [ -z "$execs" ] && execs="$(grep -oE 'stat::number_of_executed_units: [0-9]+' "$log" | tail -n1 | awk '{print $2}')"
  [ -z "$execs" ] && execs=0
  crashes=0
  note=""
  if [ "$rc" -ne 0 ]; then
    crashes=$(ls "fuzz/artifacts/$t" 2>/dev/null | wc -l)
    [ "$crashes" -eq 0 ] && crashes=1
    note="exit $rc; see fuzz/artifacts/$t/ ($(grep -m1 -E 'SUMMARY|panicked' "$log"))"
    fail=1
  fi
  printf '%-14s %14s %8s %s\n' "$t" "$execs" "$crashes" "$note"
done
exit $fail
