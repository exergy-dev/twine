# Benchmarks

Measured numbers against the design Section 2 performance targets and the
Section 13 phase gates. Raw output: [`bench/results/baseline.txt`](bench/results/baseline.txt).
Reproduce with `make bench` (root micro-benchmarks, then the `bench/` harness).

## Read this first: what this host is and is not

| | Design target host | This run |
| --- | --- | --- |
| CPU | 16-core Linux server, `GOMAXPROCS=16` | i9-14900KF desktop (8 P-cores + 16 E-cores), WSL2 shows 16 cores × 2 HT = 32 CPUs, `GOMAXPROCS=32` |
| Kernel | bare-metal Linux | Linux 6.18 under WSL2 (Hyper-V): syscalls and cross-thread wakeups are slower than bare metal |
| Network | separate client machine(s), wrk | loopback only; client is Go (in-process for `go test`, or a separate pinned process) |
| Isolation | dedicated | shared with other agents running test suites (load average is logged per block in the raw file) |

Consequences:

- **The ping-pong echo and HTTP harnesses are client-bound.** The in-process
  Go client (one goroutine per connection, one message in flight) costs more
  CPU per message than the server. The `cpu-util` metric (process CPU / wall
  time / 32) is reported so that this is visible. Separate-process runs with the
  server and client pinned to disjoint CPUs are reported alongside.
- **1M connections were not measured.** Idle memory was measured at 10K and
  50K connections and extrapolated linearly.
- **No wrk.** HTTP numbers use `bench/httpclient` (wrk-style: keep-alive, no
  pipelining, one request in flight per connection). The design's "p99 at 80%
  load" needs an open-loop rate-limited client, which was not built; saturated
  p99 is shown instead.

## Section 2 targets

Medians of `-count=5` for micro-benchmarks; single runs (`-count=1`) for the
network harness.

| Metric | Target | Measured here | Baseline | Status |
| --- | --- | --- | --- | --- |
| Echo, 1M idle connections, RSS | ≤ 4 GB | **0.69–0.75 GB** extrapolated (677–740 B heap, 694–748 B RSS per conn at 10K/50K) | goroutine-per-conn: 9.8–18.3 KB RSS/conn → **10–18 GB**; gnet: 0.40 GB | On track (extrapolated; kernel socket memory not counted, see below) |
| Memory per idle connection (goal 5) | < 1 KB excl. user state | 677 B heap/conn at 50K (includes the 16 B echo protocol object) | goroutine 17.2 KB heap + 2.1 KB stack | Met |
| HTTP/1.1 plaintext, req/s per core | ≥ 1M | 152K req/s per loop in-process (8 loops, client-bound, c=256); **213K req/s per server core** pinned separate process (c=256) | net/http: 55.6K req/s per server core pinned | Miss (see causes) |
| HTTP/1.1 plaintext, p99 at 80% load | ≤ 1 ms | Not measured at 80% load. Saturated, c=256: p99 0.66 ms (pinned), 1.05 ms (in-process) | net/http saturated c=256: p99 3.0 ms (pinned), 2.2 ms (in-process) | Not measured as specified |
| Echo, 64 B messages, msgs/s per core | ≥ 2M | 234K msgs/s per loop at 8 loops (in-process); 350K per server core at 1 loop (pinned) | gnet: 222K/loop in-process (twine +5%); 369K per core pinned at 1 loop (twine −5%); goroutine-per-conn: 1.17M total | Miss on absolute; gnet within ±5% in every configuration (within-10% met) |
| allocs/op: read path, write path, timer fire | 0 | 0 (`TestReadPathZeroAlloc`, `TestWriteHitPathZeroAlloc`, `TestWriteBufferedPathZeroAlloc`, `TestTimerFireZeroAlloc` pass; `BenchmarkTimers1M/fire` 0 allocs/op); HTTP parse+respond 0 (`TestParseRespondZeroAlloc` passes) | net/http: 18 allocs/request | Met |
| Timer insert + cancel, 1M active timers | O(1), ≤ 50 ns | **34.7 ns**, 1 alloc (80 B) on the current tree; 9.9 ns, 0 allocs on the earlier pooled-Timer core | `time.AfterFunc`+`Stop`: 159 ns, 1 alloc (112 B) | ns met, 4.6× faster than the runtime; see the 0-alloc note under Phase 3 |

Why the per-core throughput targets are missed here: in 64 B ping-pong each
message costs one `read(2)` and one `write(2)` on loopback, about 1.4 µs each
under WSL2, so one core tops out near 350K messages/s whatever the framework.
gnet hits the same ceiling (354–368K per core). The 2M/core and 1M req/core
targets assume batching that this workload does not allow: many messages per
syscall from pipelining or multiple messages in flight per connection, plus a
bare-metal kernel. They need re-measuring on the target host with wrk and a
pipelined echo client before they can be called failures of twine.

Kernel memory: per-socket kernel memory (socket structs and buffers, a few KB
per idle TCP socket) is not in process RSS. The ≤ 4 GB figure is RSS only,
as the design table states.

## Phase gates

| Phase | Gate | Measured | Result |
| --- | --- | --- | --- |
| 1 | `echo_small` runs and prints msgs/s; read path 0 allocs | `BenchmarkEchoSmall/twine` 1.87M msgs/s; read path 0 allocs | Pass |
| 2 | `echo_small` scales ≥ 0.8× linearly from 1 to 8 loops | in-process: 0.99 (2 loops), 0.88–0.94 (4), **0.69–0.70 (8)** (median of 5: 0.69). Pinned separate process, one hyperthread per core: 0.97 (2), 0.99 (4), **0.76 (8)** | **Fail on this host.** gnet control, same harness: 0.66–0.67 in-process and 0.71 pinned at 8 loops. The falloff is the host's (see below), not twine-specific |
| 3 | `timers_1m` ≤ 50 ns insert+cancel, 0 allocs | 34.7 ns, **1 alloc/op** (80 B) | **ns pass, 0-alloc fail** on the current tree (Timers are no longer pooled; 9.9 ns / 0 allocs before that change) |
| 3 | `callfromthread` wakeups/op ≤ 0.1 under 16 producers | **0.017 wakeups/op** (window=2048), 0.018 (flood); 164 ns/op | Pass |
| 5 | `http_plaintext` ≥ 5× net/http req/s at c=256 | in-process **0.94–1.16×** (median of 5: 0.94, 1.20M vs 1.28M req/s; client-bound, so the ratio is noise around 1); pinned separate process, both servers on cpus 0–15: **3.14×** (1.53M vs 486K req/s); per server core 3.8× (213K vs 55.6K) | **Fail on this host** |

Phase 2 detail: at 8 loops the twine loops are 96% busy (`loop-busy`), so they
are saturated. What drops is work per core: messages per server-core-second
fall from 350K at 1 loop to 282K at 8 loops in the pinned run, and gnet shows
the same 368K → 283K drop. On this host 8 loops spill onto hyperthread
siblings or E-cores shared with the client, and the loopback path in the
kernel is shared. The ratio has to be re-checked on a dedicated 16-core server
with an external client before it can be read as a twine result.

Phase 5 detail: the in-process ratio says little because the shared client
saturates first: server plus client use 74% of all 32 CPUs in the net/http
run (net/http is unrestricted) and 49% in the twine run (held to 8 loops). The pinned run is the fairer comparison, and twine is 3.1× ahead on
throughput and 3.8× on CPU efficiency. Both runs are bounded by a Go client
on WSL2 loopback, so this is not a substitute for wrk on the target host.

## Full harness results (`bench/`, `-count=1`)

A later `-count=5` run (block 9 of the raw file) agrees within a few percent
except for in-process HTTP at c=256, where net/http's median (1.28M req/s)
came out ahead of twine's (1.20M). That workload is limited by the shared
client and does not separate the servers; use the pinned table below.

Twine and gnet use 8 loops (`-echo-loops`, default 8) in EchoSmall,
EchoStream, EchoIdle and HTTPPlaintext. Goroutine-per-conn and net/http use all
32 CPUs.

| Benchmark | twine | goroutine / net/http | gnet |
| --- | --- | --- | --- |
| EchoSmall: 1000 conns, 64 B ping-pong, msgs/s | **1.87M** (p50 492 µs, p99 1.31 ms) | 1.17M (p50 819 µs, p99 2.03 ms) | 1.78M (p50 492 µs, p99 1.38 ms) |
| EchoStream: 100 conns × 1 MiB streams, GB/s echoed | 14.8 | 14.9 | 14.7 |
| HTTPPlaintext c=256, req/s | 1.22M (p99 1.05 ms) | 1.05M (p99 2.23 ms, 4613 allocs per 256 requests) | — |
| HTTPPlaintext c=10000, req/s | **988K** (p99 12.6 ms) | 548K (p99 29.4 ms) | — |
| EchoIdle 50K conns: heap / RSS per conn | 677 B / 713 B | 17.2 KB + 2.1 KB stack / 10.5 KB | 413 B / 407 B |

EchoStream is bounded by the client and loopback copying, and all three
servers land within 2% of each other.

### Echo scaling (in-process, 1000 conns, 64 B)

| loops | twine msgs/s | linearity | loop-busy | cpu-util | gnet msgs/s | gnet linearity |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 332K | 1.00 | 1.00 | 0.18 | 336K | 1.00 |
| 2 | 657K | 0.99 | 1.00 | 0.26 | 664K | 0.99 |
| 4 | 1.16M | 0.88 | 1.00 | 0.41 | 1.25M | 0.93 |
| 8 | 1.85M | **0.70** | 0.96 | 0.62 | 1.79M | **0.66** |

### Echo and HTTP scaling, separate pinned processes (`bench/echoclient`, `bench/httpclient`, 5 s)

Server on even CPUs 0,2,…,14 (one hyperthread per core), client on CPUs 16–31:

| loops | twine msgs/s | per server core | linearity | gnet msgs/s | per server core | gnet linearity |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 349K | 350K | 1.00 | 367K | 369K | 1.00 |
| 2 | 679K | 341K | 0.97 | 744K | 374K | 1.01 |
| 4 | 1.38M | 349K | 0.99 | 1.39M | 353K | 0.95 |
| 8 | 2.12M | 282K | **0.76** | 2.08M | 283K | **0.71** |

HTTP, server on CPUs 0–15, client on 16–31:

| | req/s | server cores used | req per server-core-s | p99 |
| --- | --- | --- | --- | --- |
| net/http c=256 | 486K | 8.75 | 55.6K | 3.01 ms |
| twine (8 loops) c=256 | **1.53M** | 7.16 | **213K** | 0.66 ms |
| net/http c=10000 | 846K | 14.75 | 57.4K | 18.9 ms |
| twine (8 loops) c=10000 | 1.03M | 6.71 | 153K | 12.1 ms |

## Root micro-benchmarks (`benchmarks_test.go`, median of 5)

| Benchmark | ns/op | B/op | allocs/op | Notes |
| --- | --- | --- | --- | --- |
| Timers1M/insert+cancel | 34.7 | 80 | 1 | 1M live timers on the wheel; `CallLater` + `Stop` |
| Timers1M/fire | 190 | 0 | 0 | per timer expiring during an advance over 1M timers (cache-miss bound) |
| TimeAfterFunc1M/insert+cancel | 159 | 112 | 1 | runtime timer heap with 1M live timers |
| CallFromThread/window=2048 | 164 | 0 | 0 | 16 producers; 0.017 wakeups/op, 0.98 coalesced/op |
| CallFromThread/flood | 166 | 0 | 0 | unbounded producers (exercises the overflow spill); 0.018 wakeups/op |
| ChannelSend/window=2048 | 205 | 0 | 0 | same 16 producers → `chan func()` (cap 64K) → one consumer |
| ChannelSend/flood | 83 | 0 | 0 | the channel blocks full producers; twine never blocks a producer |
| DeferToThread/await | 975 | 328 | 4 | sequential, `Await` off-loop |
| DeferToThread/then | 48 100 | 472 | 7 | result observed by a `Then` on the loop: one idle-loop wakeup per op (see below) |
| DeferToThread/parallel | 1 860 | 321 | 3 | `Await` from 32 goroutines |
| FutureResolve/onloop | 136 | 152 | 2 | `NewFuture` + `Then` + `Resolve`, inline dispatch |
| FutureResolve/offloop | 267 | 176 | 3 | resolved off-loop, dispatched via `CallFromThread` |

## Core issues found while benchmarking

1. **Lost wakeup in `CallFromThread` (fixed in the tree).** The loop cleared
   `wakePending` only in `drainTasks`. The zero-timeout spin `Wait` could
   consume the eventfd write of a producer whose CAS had set `wakePending`;
   later producers then coalesced (no eventfd write) while the loop blocked in
   `epoll_wait` with tasks queued. Reproduction: 16 goroutines each doing
   `CallFromThread(signal)` then waiting stalled after 48–143 calls on every
   run, and `Engine.Stop` hung because `beginShutdown` was never run. The
   tree now does `wakePending.Store(false)` and re-checks the queue before
   blocking; the reproduction passes 3.2M calls on every run.
2. **The spin poll ignores a consumed wakeup (open).** `Poller.Wait` drains
   the eventfd and returns `n == 0`, and the spin loop only exits on `n > 0`.
   A `CallFromThread` arriving while the loop spins therefore waits out the
   remaining spin budget at about 0.1 µs per iteration. Sequential round trip
   to an idle `Loops(1)` loop: 35.6 µs at `SpinIterations(0)`, 46.4 µs at the
   default 50, 127 µs at 1000, 1.08 ms at 10000. Suggested fix: have `Wait`
   report that the wake fd fired (or check `tasks.Len() > 0` in the spin
   loop) and break out.
3. **`CallLater` allocates (deliberate).** Timers are no longer pooled, so a
   stale handle cannot cancel a later timer. That costs one 80 B allocation
   per `CallLater` (insert+cancel 9.9 → 34.7 ns) and breaks the Phase 3
   "0 allocs" gate. Per-connection idle timeouts (`SetIdleTimeout`) use the
   intrusive wheel node and still allocate nothing.
4. **Idle-loop wake latency on WSL2 (environment).** Waking a loop thread
   blocked in `epoll_wait` takes about 35 µs here even with spinning off,
   Expect a few µs on bare metal. `DeferToThread/then` (48 µs/op) pays this
   wake (about 35 µs) plus the spin-budget delay of issue 2 (about 10 µs at
   the default 50 spins).
5. **Unexplained stack growth in EchoIdle/twine.** Twine starts no goroutine
   per connection, yet `stack-B/conn` is consistently +90 to +105 B/conn at
   both 10K and 50K connections (gnet and raw-baseline show about 0). It is
   included in the RSS figures above and does not change any verdict, but its
   source (thread g0 stacks or stack growth on the loop goroutines are
   suspects) was not identified.

## Running

```sh
make bench                                   # everything, -count=5, results in bench/results/<sha>.txt
cd bench && go test -run '^$' -bench EchoScaling -count 5 .
cd bench && go test -run '^$' -bench EchoIdle . -idle-conns=100000   # flags: -idle-conns -idle-settle -echo-loops -scaling-spin
cd bench && go run ./echo_twine -loops 8 &   # then: cd bench && go run ./echoclient -conns 1000 -size 64 -duration 10s
cd bench && go run ./http_twine -loops 8 &   # then: cd bench && go run ./httpclient -conns 256 -duration 10s
```

`bench/echo_goroutine`, `bench/echo_gnet` and `bench/http_nethttp` take the same
`-addr`/`-loops` flags. For the separate-process numbers, pin the server and
client to disjoint CPUs with `taskset`.
