# Benchmarks

Current numbers against Twine's performance targets, the 1M-connection soak,
and how to reproduce them. Raw output:
[`bench/results/baseline.txt`](bench/results/baseline.txt) (in-process
harness), [`bench/results/pinned.txt`](bench/results/pinned.txt) (pinned,
server-bound runs) and [`bench/results/idle1m.txt`](bench/results/idle1m.txt)
(soak). The full measurement log written while the project was built (phase
gates, follow-ups A to H, issues found and fixed) is in
[docs/history/benchmarks-phase-gates.md](docs/history/benchmarks-phase-gates.md).

## Host

| | |
| --- | --- |
| CPU | i9-14900KF desktop (8 P-cores + 16 E-cores); WSL2 shows 16 cores × 2 HT = 32 vCPUs |
| Kernel | Linux 6.18 under WSL2 (Hyper-V): syscalls and cross-thread wake-ups are slower than on bare metal |
| Network | loopback only; the load generator is a separate process pinned to other CPUs |
| Go | go1.25.3 |
| Isolation | none: other workloads ran on the host; each block of the raw files records `/proc/loadavg` |

The network rows below were measured on the v0.1.0 tree on 2026-11-06. The
timer row was re-measured on 2026-11-09 (load average about 56 on 32 vCPUs
while it ran). The targets assume a 16-core bare-metal server driven by wrk
from other machines; nothing here is that host, so read each row with its
note.

Method for the network rows: the server and the load generator run as
separate processes on disjoint CPUs (server on even CPUs 0,2,…,14, one thread
per physical core; client on 16–31). The load generator is `bench/twineload`,
itself a twine event-loop client, unless the row says otherwise, and the
server's CPU time is read from `/proc/<pid>/stat` to show that the server is
the saturated side. A loopback `write(2)` wakes the receiving thread inside
the syscall, so the client configuration changes the server's per-message
cost (one 1-loop server: 204K msgs/s against a client with default spinning,
367K against the `spin` client, which keeps its threads awake with
`SpinIterations(5000)`). Every ratio below compares runs made with the same
client.

## Results vs targets

| Metric | Target | Measured | Status |
| --- | --- | --- | --- |
| Echo, 1M idle connections, server RSS | ≤ 4 GB | **0.58 GB** (564 B RSS per connection; see the soak below). Goroutine-per-connection extrapolates to 10–18 GB | Met |
| Memory per idle connection | < 1 KB excluding user state | 677 B heap per connection at 50K (includes the 16 B echo protocol object); 564 B RSS at 1M. Goroutine-per-connection: 17.2 KB heap + 2.1 KB stack | Met |
| allocs/op on the read path, write path, timer fire, HTTP parse+respond | 0 | 0 (`TestReadPathZeroAlloc`, `TestWriteHitPathZeroAlloc`, `TestWriteBufferedPathZeroAlloc`, `TestTimerFireZeroAlloc`, `TestParseRespondZeroAlloc` are CI gates). net/http: 18 allocs per request | Met |
| Timer insert + cancel with 1M live timers | O(1), ≤ 50 ns, 0 allocs | **16.3 ns**, 0 allocs/op (median of 5; Timer handles come from a per-loop slab, one allocation per 256 timers, which `-benchmem` shows as 74 B/op). Firing: 137 ns per timer, 0 allocs. `time.AfterFunc` + `Stop`: 159 ns, 1 alloc | Met |
| `CallFromThread` wake-ups under 16 producers | ≤ 0.1 per call | 0.017 per call (164 ns/op) | Met |
| HTTP/1.1 plaintext, req/s per core | ≥ 1M | **4.03M on one core with pipelining depth 16** (2.96M per core on eight). Without pipelining: 273K per server core (2.14M on 7.84 cores, 8 loops, c=256) | Met with pipelining; not without (loopback syscall bound, see below) |
| HTTP/1.1 plaintext vs net/http, same cores, c=256 | ≥ 5× | 35× with pipelining depth 16 (23.68M vs 672K on 8 cores); 3.4× without (2.17M vs 634K, medians of 3) | Met with pipelining; not without |
| HTTP/1.1 p99 latency at 80% load | ≤ 1 ms | 1.25 ms (open-loop, 4 loops, 944K req/s offered, c=256; p50 14 µs). The host's p99 floor is 0.59 ms even at 10% load (vCPU jitter). net/http at 80% of its own saturation: p99 2.36 ms | Not met on this host |
| Echo, 64 B messages, msgs/s per core | ≥ 2M | 360–417K per core at 1 loop, 275–283K per core at 8 loops (2.2M total); gnet on the same runs is within ±6% | Not met (loopback syscall bound, see below) |
| Echo scaling 1 → 8 loops, 1000 connections | ≥ 0.8 of linear | 0.75–0.76 with the CPUs left as the host schedules them; **0.95** when the 1-loop baseline runs with the other seven server CPUs busy, so both runs get the same per-core speed from this turbo/hybrid host | Met once host capacity is held constant |

Why the per-core echo and unpipelined HTTP targets are missed here: in 64 B
ping-pong every message costs one `read(2)` and one `write(2)` on loopback,
about 1.4 µs each under WSL2, plus the receive path of the peer's socket,
which loopback runs on the sender's CPU. One core tops out near 350–400K
messages per second whatever the framework, and gnet hits the same ceiling.
The 2M/core and 1M req/core figures assume many messages per syscall
(pipelining, several messages in flight per connection) on bare metal; the
pipelined HTTP row shows what happens when that holds.

Kernel memory is not in RSS: an idle TCP socket costs a few KB of kernel
memory (about 3 KB per socket end on this host, see the soak).

### TLS and kTLS

There are no TLS numbers yet. The memory-BIO path adds the AES-GCM work and
the copies through the BIO buffers on top of the plaintext cost; kTLS offload
(Linux, experimental, `tls.KernelOffload(true)`) is exercised by tests but
has not been benchmarked. Treat any claim that TLS runs at plaintext speed as
unmeasured until benchmarks land.

## 1M-connection soak

Raw samples every 10 s: [`bench/results/idle1m.txt`](bench/results/idle1m.txt).
`bench/idle1m/server` (twine echo, 8 loops pinned to 0,2,…,14, 40 ports from
20000, `RLIMIT_NOFILE` raised to the 1,048,576 hard limit, `Backlog(65535)`)
and `bench/idle1m` (client: 64 goroutines, raw blocking sockets with a 3 s
connect timeout, bound to 127.0.0.2–127.0.0.51 round-robin with
`IP_BIND_ADDRESS_NO_PORT`, paced to 40,000 connects/s, aborts if MemAvailable
falls below 4 GB). Driver: `bench/idle1m/soak.sh -netns`.

| | |
| --- | --- |
| Connections held | **1,000,000** (all established, no retries; 2000 sampled connections echoed a byte after the hold: 2000/2000) |
| Time to establish | 25.0 s at the 40,000/s pacing limit (the limit is the client's, not the server's) |
| Hold | 10 minutes |
| Server RSS | 18.1 MB idle, **580 MB at 1M**, peak 582.0 MB, 582.0 MB at the end |
| Stability | RSS 581.4 MB at hold minute 2 and 582.0 MB at the end: drift +0.55 MB (+0.095%); Go heap in use 552 MB; 14 GCs in total |
| Per connection | **564 B RSS** ((peak − idle) / 1M); heap in use 552 B |
| Host memory | MemAvailable 30.3 GB → 23.6 GB: 6.7 GB for both ends of 1M loopback connections (2M sockets, about 3 KB each) including the server's 0.58 GB and the client |
| Server kernel memory | not separable here; if both ends cost the same, about 3 GB, so RSS plus kernel ≈ 3.6 GB. TCP buffer memory in `/proc/net/sockstat` stayed at 794 pages (idle sockets hold no buffers) |
| fd budget | 1,000,000 connections + 320 listener fds (8 reuseport listeners × 40 ports) + poller fds, under the 1,048,576 limit; the client needs the same |

The run uses a private user and network namespace (`unshare -Urn`,
unprivileged). In the host's root namespace loopback connections are tracked
by `nf_conntrack`, and the first attempt stalled at 261,600 connections when
`nf_conntrack_count` reached `nf_conntrack_max` (262,144): not an fd, memory
or twine limit. A host whose traffic is tracked needs a conntrack table of
over 1M entries for 1M connections. Memory does not depend on core count, so
the result carries over to the 16-core target host; the connection rate does
not.

## Running

`bench/` (the comparison harness, with gnet) is a separate module and
Linux-only.

```sh
make bench                    # root micro-benchmarks, then bench/ harness, -count=5; results in bench/results/<sha>.txt
make bench-compare            # benchstat against BASE (default bench/results/main.txt)
go test -run '^$' -bench Timers1M -benchmem -count=5 .        # the timer row
cd bench && go test -run '^$' -bench EchoScaling -count 5 .
cd bench && go test -run '^$' -bench EchoIdle . -idle-conns=100000   # flags: -idle-conns -idle-settle -echo-loops -scaling-spin
cd bench && go run ./echo_twine -loops 8 &   # then: cd bench && go run ./echoclient -conns 1000 -size 64 -duration 10s
cd bench && go run ./http_twine -loops 8 &   # then: cd bench && go run ./httpclient -conns 256 -duration 10s
```

Pinned, server-bound runs and the soak (not part of `make bench`; each takes
minutes):

```sh
cd bench
go run ./scaling -mode echo -servers twine,gnet -loops 1,2,4,8 -conns 1000,4000 \
    -clients gor,tl,spin,tlm,tlms -duration 10s -out results/pinned.txt
go run ./scaling -mode http -servers twine@8,nethttp@8,nethttp@16@0-15 -conns 256,10000 \
    -clients gor,tl,spin -duration 10s -out results/pinned.txt
idle1m/soak.sh -n 1000000 -hold 10m -netns > results/idle1m.txt   # -netns: see the soak section
```

Client configurations for `scaling`: `gor` is the goroutine-per-connection
`bench/echoclient` or `bench/httpclient`; `tl` is `bench/twineload` with 8
loops on CPUs 16,18,…,30 and default spinning; `spin` is `tl` with
`SpinIterations(5000)` (its threads stay awake); `tlm` and `tlms` use as many
client loops as the server has (default spinning, and spin 5000). Runs
recorded before the rename print `client=tls` for `spin` in `pinned.txt`.

`bench/echo_goroutine`, `bench/echo_gnet` and `bench/http_nethttp` take the
same `-addr`/`-loops` flags as the twine servers. For separate-process
numbers, pin the server and client to disjoint CPUs with `taskset`.

## GC tuning

Twine does not set `GOGC` or `GOMEMLIMIT`. For large connection counts we
recommend `GOGC=200`–`400` together with a `GOMEMLIMIT` at roughly 80% of the
container's memory. Per-connection state is pointer-light so GC scan cost
grows slowly with connections; the slabs behind write rings are allocated
once per loop and reused. The soak above ran with the defaults.
