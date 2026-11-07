#!/bin/bash
# 1M idle-connection soak: server on CPUs 0,2,..,14 (PinLoops), client on 16-31.
#
#   idle1m/soak.sh [-n 1000000] [-hold 10m] [-netns]   (run from bench/)
#
# -netns re-runs the whole soak inside a private user+network namespace
# (unshare -Urn). On hosts where the root netns has nf_conntrack active with
# nf_conntrack_max=262144 (this WSL2 host), loopback connections are tracked
# and the table fills at 262,144 entries; a fresh netns has no conntrack
# rules, so the limit does not apply, and its somaxconn can be raised.
# Output (client + server lines) goes to stdout; redirect to results/idle1m.txt.
set -u
N=1000000; HOLD=10m; NETNS=0
while [ $# -gt 0 ]; do
  case $1 in
    -n) N=$2; shift 2;;
    -hold) HOLD=$2; shift 2;;
    -netns) NETNS=1; shift;;
    --inner) shift;;
    *) echo "unknown arg $1"; exit 1;;
  esac
done
export GOTOOLCHAIN=local
BIN=${BIN:-$(mktemp -d)}
go build -o "$BIN/server" ./idle1m/server && go build -o "$BIN/client" ./idle1m || exit 1
if [ "$NETNS" = 1 ] && [ -z "${INNER:-}" ]; then
  exec env INNER=1 BIN="$BIN" unshare -Urn "$0" -n "$N" -hold "$HOLD" -netns
fi
if [ -n "${INNER:-}" ]; then
  ip link set lo up
  sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535 || true
  echo "# netns: somaxconn=$(cat /proc/sys/net/core/somaxconn) tcp_max_syn_backlog=$(cat /proc/sys/net/ipv4/tcp_max_syn_backlog) conntrack_max=$(cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null)"
fi
echo "# host: $(uname -r) mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo) somaxconn=$(cat /proc/sys/net/core/somaxconn) nf_conntrack_max=$(cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null)"
taskset -c 0-14:2 "$BIN/server" -interval 10s > "$BIN/server.log" 2>&1 &
SP=$!
sleep 1
( tail -F "$BIN/server.log" 2>/dev/null & echo $! > "$BIN/tail.pid"; wait ) &
taskset -c 16-31 "$BIN/client" -n "$N" -hold "$HOLD" -sample 10s -server-pid $SP -rate 40000
RC=$?
kill -INT $SP; wait $SP 2>/dev/null
kill "$(cat "$BIN/tail.pid")" 2>/dev/null
exit $RC
