# Benchmarks

Measured numbers against the design Section 2 performance targets and the
Section 13 phase gates. Raw output: [`bench/results/baseline.txt`](bench/results/baseline.txt)
(in-process harness), [`bench/results/pinned.txt`](bench/results/pinned.txt) (pinned,
server-bound runs: Phase 2 and Phase 5) and [`bench/results/idle1m.txt`](bench/results/idle1m.txt)
(1M-connection soak). Reproduce with `make bench` (root micro-benchmarks, then the
`bench/` harness); the pinned runs and the soak are separate programs, see "Running".

## Read this first: what this host is and is not

| | Design target host | This run |
| --- | --- | --- |
| CPU | 16-core Linux server, `GOMAXPROCS=16` | i9-14900KF desktop (8 P-cores + 16 E-cores), WSL2 shows 16 cores × 2 HT = 32 CPUs, `GOMAXPROCS=32` |
| Kernel | bare-metal Linux | Linux 6.18 under WSL2 (Hyper-V): syscalls and cross-thread wakeups are slower than bare metal |
| Network | separate client machine(s), wrk | loopback only; client is Go (in-process for `go test`, or a separate pinned process) |
| Isolation | dedicated | shared with other agents running test suites (load average is logged per block in the raw file) |

Consequences:

- **The in-process `go test` harness is client-bound.** The in-process Go
  client (one goroutine per connection, one message in flight) costs more
  CPU per message than the server, so those numbers (the "Full harness results"
  and "Echo scaling (in-process)" sections) are kept only as a secondary note.
  The gate verdicts below use the **pinned, server-bound methodology**: the
  server and the load generator are separate processes on disjoint CPUs, the
  load generator is itself a twine event-loop client (`bench/twineload`), and
  the server process's CPU time is read from `/proc/<pid>/stat` for every run
  to show that the server is the saturated side.
- **1M connections were measured** (soak below), but inside a private network
  namespace: the host's root namespace tracks loopback connections in
  `nf_conntrack` (`nf_conntrack_max` = 262144, not changeable without root),
  which capped the first attempt at 261,600 connections.
- **A load generator influences the server's per-message cost.** A server
  `write(2)` on loopback wakes the peer's thread inside the syscall; if the
  client thread is asleep (an event-loop client with spare capacity) that
  wake-up is charged to the server. The same 1-loop twine server does 152K
  msgs/s against a sleepy 8-loop client with spinning off, 204K with the
  default spin, 367K with `SpinIterations(5000)`, and 412K against a client
  with one busy loop. Every scaling number therefore comes with the client
  configuration that produced it, and the 1-loop baseline is taken with the
  same client.
- **No wrk.** HTTP numbers use `bench/httpclient` (wrk-style: keep-alive, no
  pipelining, one request in flight per connection). The design's "p99 at 80%
  load" needs an open-loop rate-limited client, which was not built; saturated
  p99 is shown instead.

## Section 2 targets

Medians of `-count=5` for micro-benchmarks; single runs (`-count=1`) for the
network harness.

| Metric | Target | Measured here | Baseline | Status |
| --- | --- | --- | --- | --- |
| Echo, 1M idle connections, RSS | ≤ 4 GB | **Measured: 0.58 GB** server RSS at 1,000,000 held connections (582 MB peak, 564 B RSS per connection, +0.55 MB over the last 8 minutes of a 10-minute hold). Host MemAvailable fell 6.7 GB for the 2M loopback sockets (both ends), i.e. about 3 GB of kernel socket memory on the server side if the ends cost the same, so server RSS + its kernel share ≈ 3.6 GB | goroutine-per-conn: 9.8–18.3 KB RSS/conn → **10–18 GB** (extrapolated from 10K/50K); gnet: 0.40 GB (extrapolated) | **Met** (RSS as the table defines it; the kernel share is an estimate, see the soak section) |
| Memory per idle connection (goal 5) | < 1 KB excl. user state | 677 B heap/conn at 50K (includes the 16 B echo protocol object) | goroutine 17.2 KB heap + 2.1 KB stack | Met |
| HTTP/1.1 plaintext, req/s per core | ≥ 1M | **273K req/s per server core** (2.14M req/s on 7.84 cores, 8 pinned loops, c=256, pinned server-bound run); 152K per loop in the old client-bound in-process run | net/http, same 8 cores: 93K per server core (647K req/s on 6.94 cores); GOMAXPROCS=16 on cpus 0–15: 67K per core (708K on 10.5 cores) | Miss (see causes) |
| HTTP/1.1 plaintext, p99 at 80% load | ≤ 1 ms | Not measured at 80% load. Saturated, c=256, pinned: p50 115 µs, p99 0.28–0.30 ms (twineload client, sampled 1 in 16) | net/http saturated c=256, same cores: p50 172 µs, p99 3.0 ms | Not measured as specified (saturated p99 is below 1 ms) |
| Echo, 64 B messages, msgs/s per core | ≥ 2M | 360–417K msgs/s per core at 1 pinned loop (client-dependent, see caveat above), 275–283K per core at 8 pinned loops (2.2M total); old client-bound in-process: 234K per loop at 8 loops | gnet, same harness: 361–393K per core at 1 loop, 266–279K at 8 (2.1–2.2M total); goroutine-per-conn: 1.17M total (in-process) | Miss on absolute; twine within about ±6% of gnet in the matched-client pinned configurations |
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
| 2 | `echo_small` scales ≥ 0.8× linearly from 1 to 8 loops | Pinned, server-bound (server on cpus 0,2,…,14, client on 16–31, server CPU 7.8–7.9 cores at 8 loops): **0.74 (tls) to 0.76 (goroutine client) at 1000 conns**, 0.67–0.68 with a client whose 1-loop baseline is best (2.22M vs 8 × 417K); at 4000 conns 0.75 (goroutine client), **0.7998 (tls)**; 2 loops 0.92–1.03, 4 loops 0.86–0.99 | **Not met on this host.** The closest case is 0.7998 at 4000 connections with `twineload` at `SpinIterations(5000)` (283K → 1.811M msgs/s), 0.0002 under the line; no saturated configuration at 1000 connections passes (best 0.76). **Repeated medians (5 alternating runs per cell): 0.75–0.76 at 1000 conns (not met), 0.81–0.82 at 4000 (met, from a weak 1-loop baseline), 0.60–0.68 at 8000 (not met)**; gnet 0.74 / 0.70–0.77 / 0.58–0.64. gnet control, same runs: 0.70–0.74 (1000), 0.71–0.77 (4000). The 8-loop total saturates near 2.2M msgs/s for twine and gnet alike |
| 3 | `timers_1m` ≤ 50 ns insert+cancel, 0 allocs | 34.7 ns, **1 alloc/op** (80 B) | **ns pass, 0-alloc fail** on the current tree (Timers are no longer pooled; 9.9 ns / 0 allocs before that change) |
| 3 | `callfromthread` wakeups/op ≤ 0.1 under 16 producers | **0.017 wakeups/op** (window=2048), 0.018 (flood); 164 ns/op | Pass |
| 5 | `http_plaintext` ≥ 5× net/http req/s at c=256 | Pinned, server-bound, c=256: twine (8 loops, cpus 0,2,…,14) **2.14M req/s** vs net/http GOMAXPROCS=8 on the same cpus **647K** = **3.30×** (four runs: 3.30, 3.31, 3.37, 3.36); vs net/http GOMAXPROCS=16 on cpus 0–15 (708K) = **3.02×**. With the goroutine-per-connection client: 1.46M vs 586K (2.50×) and vs 507K (2.89×). Best single ratio 3.72× (2.106M vs 566K, one noisy net/http run). c=10000: 1.29M vs 605K = 2.1× (vs GOMAXPROCS=16: 952K, 1.35×) | **Not met on this host** (best 3.7×, typical 3.0–3.3×). The twine server is the saturated side (7.84 of 8 cores), and twine HTTP (2.14M) is already at the host's echo ceiling (2.2M msgs/s at 8 loops), so more server efficiency cannot buy much; **Repeated (3 alternating runs, equal cores): 2.70× at N=1, 2.71× N=2, 3.51× N=4, 3.41× N=8, 2.12× at c=10000 (one run): not met at any N**; the old unpinned in-process (0.94–1.16×) and pinned Go-client (3.14×) numbers are the secondary note below |

## Pinned, server-bound results (Phase 2 and Phase 5 gates)

Raw output: [`bench/results/pinned.txt`](bench/results/pinned.txt). Runner:
`bench/scaling` (`cd bench && go run ./scaling -mode echo|http ...`).

Method:

- The server is its own process under `taskset` on the first N **even** CPUs
  (0,2,…,14: one thread per physical core as the VM presents the topology;
  odd siblings left idle) and twine loops are also pinned with
  `twine.PinLoops` to the same list (loop i on cpus[i]). For net/http,
  `GOMAXPROCS` is N under the same `taskset` (and GOMAXPROCS=16 on cpus 0–15 as
  the second baseline). gnet is run under `taskset` only (it has no pinning).
- The load generator is another process under `taskset -c 16-31`.
  `bench/twineload` is an event-loop client on twine itself (`Loop.ConnectTCP`,
  one request in flight per connection, 8 loops pinned to 16,18,…,30, latency
  sampled on 1 round trip in 16). `gor` is the existing goroutine-per-connection
  `bench/echoclient` / `bench/httpclient`.
- Each run: connect all connections, 2 s warm-up, then a 10 s window. The
  server's CPU time (utime+stime from `/proc/<pid>/stat`) is read at the
  window's start and end by the client; "server cores" is that delta over wall
  time. A server that shows N.0 cores at N loops is saturated, i.e. the run is
  server-bound. The client's own cores are shown too.
- Client configurations: `gor` goroutine client (16 CPUs available); `tl`
  twineload, 8 loops, default spin; `tls` twineload, 8 loops, `SpinIterations(5000)`
  (the client's threads stay awake); `tlm` twineload with as many loops as the
  server (pinned on 16,18,…); `tlms` = `tlm` with spin 5000.
- Linearity = (msgs/s at N loops) / (N × msgs/s at 1 loop) **with the same client
  configuration**. † the 1-loop run of that column was not saturating the
  server (0.91–0.96 cores), so its baseline is client-limited and the linearity
  is flattered. `tl` columns are not valid for linearity: its 1-loop baseline is
  depressed by the wake-up cost described in the caveats (204K vs 417K for the
  same server), which is why its 4- and 8-loop "linearity" exceeds 1.

### Phase 2: echo scaling, 64 B, one message in flight per connection (10 s)

twine, 1000 connections: msgs/s (linearity vs the 1-loop run of the same client; server cores used in brackets)

| client | 1 loop | 2 loops | 4 loops | 8 loops |
| --- | --- | --- | --- | --- |
| gor | 359K [1.00] | 743K (1.03) [2.00] | 1,403K (0.98) [3.99] | 2,188K (0.76) [7.84] |
| tl | 204K [1.00] | 447K (1.09) [2.00] | 1,188K (1.45) [3.99] | 2,254K (1.38) [7.90] |
| tls | 371K [1.00] | 711K (0.96) [2.00] | 1,350K (0.91) [3.99] | 2,212K (0.74) [7.94] |
| tlm | 414K [1.00] | 760K (0.92) [2.00] | 1,447K (0.87) [3.99] | 2,242K (0.68) [7.90] |
| tlms | 417K [1.00] | 764K (0.92) [2.00] | 1,450K (0.87) [3.99] | 2,223K (0.67) [7.94] |

twine, 4000 connections: msgs/s (linearity vs the 1-loop run of the same client; server cores used in brackets)

| client | 1 loop | 2 loops | 4 loops | 8 loops |
| --- | --- | --- | --- | --- |
| gor | 243K [1.00] | 505K (1.04) [2.00] | 964K (0.99) [3.99] | 1,447K (0.75) [7.27] |
| tl | 134K [1.00] | 336K (1.26) [2.00] | 867K (1.62) [3.99] | 1,881K (1.76) [7.90] |
| tls | 283K [1.00] | 546K (0.96) [2.00] | 1,057K (0.93) [3.99] | 1,811K (0.80) [7.93] |
| tlm | 276K [0.96] † | 552K (1.00) [1.98] | 1,017K (0.92) [3.66] | 1,881K (0.85) [7.90] |
| tlms | 295K [0.93] † | 566K (0.96) [2.00] | 1,011K (0.86) [3.66] | 1,851K (0.79) [7.93] |

gnet, 1000 connections: msgs/s (linearity vs the 1-loop run of the same client; server cores used in brackets)

| client | 1 loop | 2 loops | 4 loops | 8 loops |
| --- | --- | --- | --- | --- |
| gor | 362K [1.00] | 658K (0.91) [1.79] | 1,307K (0.90) [3.65] | 2,129K (0.74) [7.60] |
| tl | 109K [1.00] | 269K (1.23) [1.88] | 1,064K (2.44) [3.76] | 2,235K (2.56) [7.79] |
| tls | 373K [1.00] | 645K (0.86) [1.88] | 1,218K (0.82) [3.78] | 2,184K (0.73) [7.71] |
| tlm | 391K [1.00] | 709K (0.91) [1.90] | 1,378K (0.88) [3.81] | 2,229K (0.71) [7.77] |
| tlms | 393K [1.00] | 713K (0.91) [1.89] | 1,381K (0.88) [3.80] | 2,193K (0.70) [7.78] |

gnet, 4000 connections: msgs/s (linearity vs the 1-loop run of the same client; server cores used in brackets)

| client | 1 loop | 2 loops | 4 loops | 8 loops |
| --- | --- | --- | --- | --- |
| gor | 253K [1.00] | 455K (0.90) [1.75] | 927K (0.92) [3.62] | 1,469K (0.73) [7.18] |
| tl | 110K [1.00] | 217K (0.99) [1.84] | 762K (1.73) [3.74] | 1,884K (2.14) [7.66] |
| tls | 327K [1.00] | 471K (0.72) [1.85] | 975K (0.75) [3.76] | 1,854K (0.71) [7.76] |
| tlm | 305K [0.92] † | 473K (0.77) [1.87] | 1,016K (0.83) [3.77] | 1,886K (0.77) [7.69] |
| tlms | 302K [0.91] † | 523K (0.87) [1.84] | 1,027K (0.85) [3.74] | 1,850K (0.77) [7.71] |

Reading the echo tables:

- Every cell above is server-bound except 4000-connection `gor` at 8 loops (7.27
  of 8 server cores) and the † baselines. At 8 loops the twine server uses
  7.84–7.94 cores in every 1000-connection run, whichever client drives it, and
  the client never uses more than its 8 pinned loops (`tl`..`tlms`) or 12.7 of 16
  CPUs (`gor`). Giving the client more CPUs does not raise the HTTP rate either
  (diagnostic runs at the end of the raw file).
- The aggregate 8-loop rate is the same for every client and for gnet:
  2.19–2.26M msgs/s at 1000 connections (twine), 2.13–2.24M (gnet); 1.81–1.88M
  at 4000. It does not depend on which CPUs host the loops either: 8 loops on
  cpus 0–7 (four cores with both siblings), on 0,2,…,14 and on 0–15 gave 2.235M,
  2.223M, 2.234M. Per-core cost rises smoothly with loop count (`tls` client,
  1000 conns: 3 loops 349K/core, 5: 322K, 6: 312K, 7: 296K, 8: 277K) while each
  loop stays 100% busy. That points to a shared resource below twine (the
  loopback path in the guest kernel, or the hypervisor) rather than to twine's
  loops, but no experiment here separates the two.
- Verdict: **the Phase 2 gate is not met on this host.** The only configuration
  at or near 0.8 is 4000 connections with the `tls` client (0.7998, a miss by
  0.0002). At 1000 connections, the Section 12 setting, saturated runs give
  0.74 (`tls`) and 0.76 (`gor`), and 0.67–0.68 against the best 1-loop baseline
  (417K). Taking a fixed 1-loop baseline of 417K (the best any client achieved),
  the 8-loop ceiling of 2.26M gives at most 0.68. The earlier unpinned
  in-process result (0.69–0.70) and the earlier pinned Go-client result (0.76)
  agree with these.

### Phase 2 repeated: 5 alternating 1-loop / 8-loop runs per cell (medians)

Raw: section "FOLLOW-UP A" of `results/pinned.txt`. Server on cpus 0,2,…,14, client on 16–31,
10 s per run, 1-loop and 8-loop runs alternating, 5 repetitions. Linearity = median 8-loop rate /
(8 × median 1-loop rate). The per-run column gives the range of r8_i / (8 × r1_i) over the repetitions.
`tls` = 8-loop twineload with `SpinIterations(5000)`; `gor` = goroutine client. Verdict per cell:
met if the median linearity is ≥ 0.80.

| conns | client | twine 1 loop | twine 8 loops (server cores) | **twine linearity** (per run) | gnet linearity (per run) | verdict (twine) |
| --- | --- | --- | --- | --- | --- | --- |
| 1000 | tls | 371K (370–374K) | 2.229M (2.227–2.235M), 7.94 | **0.752** (0.74–0.76) | 0.739 (0.73–0.74) | not met |
| 1000 | gor | 362K (360–363K) | 2.203M (2.190–2.216M), 7.84 | **0.761** (0.76–0.77) | 0.736 (0.73–0.74) | not met |
| 4000 | tls | 286K (281–289K) | 1.875M (1.856–1.882M), 7.93 | **0.821** (0.81–0.83) | 0.695 (0.69–0.71) | met |
| 4000 | gor | 245K (243–247K) | 1.595M (1.536–1.650M), 7.53 | **0.813** (0.78–0.84) | 0.766 (0.76–0.80) | met (marginal; 2 of 5 runs below 0.80) |
| 8000 | tls | 264K (263–265K) | 1.428M (1.420–1.439M), 7.93 | **0.676** (0.67–0.68) | 0.640 (0.63–0.65) | not met |
| 8000 | gor | 237K (236–238K) | 1.133M (1.129–1.141M), 6.71 | **0.599** (0.59–0.60) | 0.575 (0.57–0.58) | not met |

Notes: gnet 1-loop at 8000 connections with `tls` completed only 3 of 5 runs (the client's
connect burst timed out against the single gnet loop twice: "twine: connect timeout", 51–52 of 8000
connections), so that gnet cell uses n=3; every other cell has n=5. The 1000-connection medians match the
single runs above (0.74/0.76). The 4000-connection pass is real in the sense that all five twine
runs are ≥ 0.81 with `tls`, but it comes from a weak 1-loop baseline, not a strong 8-loop run: twine at
1 loop does 286K here against gnet's 335K, while the 8-loop totals are equal (1.875M vs 1.863M), which is why
gnet's linearity in the same cell is 0.695. The gate as written in Section 13 does not fix the connection count; at
the Section 12 setting (1000) it is not met, it is met at 4000 and not met at 8000.

### Phase 5: HTTP plaintext, `GET / HTTP/1.1`, keep-alive, one request in flight (10 s)

Server-bound at c=256 for twine (7.84–7.93 of 8 cores) and for net/http
(6.4–6.9 of 8 cores: it is partly latency-bound, with its goroutines waiting on
wake-ups, and with 16 cores it uses 8.9–10.6). Rows are one run each except the
repeats noted.

| server | conns | client | req/s | req/s per server core | server cores used | p50 | p99 | client cores | ratio vs twine |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| twine 8 loops, cpus 0,2,…,14 | 256 | gor | 1.464M | 211K | 6.95 | 139 µs | 655 µs | 9.4 | - |
| net/http GOMAXPROCS=8, same cpus | 256 | gor | 586K | 91K | 6.41 | 262 µs | 2.23 ms | 5.4 | twine 2.50× |
| net/http GOMAXPROCS=16, cpus 0–15 | 256 | gor | 507K | 57K | 8.89 | 262 µs | 2.75 ms | 4.9 | twine 2.89× |
| twine 8 loops | 256 | tl (4 runs) | **2.136–2.151M** | 273K | 7.84 | 115 µs | 0.28–0.31 ms | 7.95 | - |
| net/http GOMAXPROCS=8 | 256 | tl (4 runs) | 637–648K | 93K | 6.94 | 172 µs | 3.0 ms | 5.0 | **twine 3.30–3.37×** |
| net/http GOMAXPROCS=16, cpus 0–15 | 256 | tl (4 runs) | 706–708K | 67K | 10.5 | 90 µs | 3.8–3.9 ms | 4.5 | **twine 3.02–3.03×** |
| twine | 256 | tls | 2.106M | 266K | 7.93 | 119 µs | 254 µs | 8.0 | - |
| net/http GOMAXPROCS=8 | 256 | tls | 566K | 85K | 6.69 | 74 µs | 5.2 ms | 8.0 | twine 3.72× |
| net/http GOMAXPROCS=16 | 256 | tls | 613K | 61K | 10.0 | 63 µs | 6.0 ms | 8.0 | twine 3.43× |
| twine | 10000 | gor | 1.083M | 155K | 7.00 | 9.4 ms | 11.5 ms | 10.6 | - |
| net/http GOMAXPROCS=8 | 10000 | gor | 615K | 78K | 7.89 | 15.7 ms | 23.1 ms | 7.0 | twine 1.76× |
| net/http GOMAXPROCS=16 | 10000 | gor | 884K | 57K | 15.55 | 10.5 ms | 17.8 ms | 9.3 | twine 1.23× |
| twine | 10000 | tl | 1.287M | 163K | 7.92 | 7.6 ms | 14.2 ms | 8.0 | - |
| net/http GOMAXPROCS=8 | 10000 | tl | 605K | 77K | 7.84 | 15.7 ms | 25.2 ms | 6.0 | twine 2.13× |
| net/http GOMAXPROCS=16 | 10000 | tl | 952K | 61K | 15.57 | 10.5 ms | 15.7 ms | 7.5 | twine 1.35× |

(`tls` rows at c=10000 are within 3% of the `tl` rows; see the raw file.) Latency is
the closed-loop saturated latency, not "p99 at 80% load".

Is twine's 2.14M req/s client-limited? Raising the client from 8 to 12 and 16 loops
left it at 2.08M and 2.05M with the server still at 7.85–7.95 cores, so no.

Verdict: **the Phase 5 gate (≥ 5× net/http at c=256) is not met on this host.** The
ratio is 3.0–3.4× in the repeated runs (3.7× in one run in which net/http was
slow) against net/http on the same 8 cores or on 16 cores. twine HTTP at 2.14M
req/s is within 5% of what the same 8 pinned cores deliver as a bare 64 B echo
(2.2M msgs/s), so HTTP parsing and response building are not what limits it: the
per-message syscall cost (`epoll_wait` + `read` + `write` on loopback under
WSL2) is. 5× would need 3.2M req/s from 8 cores (400K per core, about the 1-loop
echo ceiling) or a net/http that is 1.5× slower than it was measured here.

### Phase 5 repeated: N pinned loops vs GOMAXPROCS=N, c=256 (medians of 3, alternating)

Raw: section "FOLLOW-UP B" of `results/pinned.txt`. twine with N loops pinned to the first N of
cpus 0,2,…,14; net/http with GOMAXPROCS=N under `taskset` on the same N cpus; twineload client (`tl`, 8 loops on
16,18,…,30); 10 s per run; twine and net/http alternating, 3 repetitions. Server cores = utime+stime from /proc.

| N | twine req/s (min–max) | net/http req/s (min–max) | **ratio of medians** | server cores twine / net/http | p50 / p99 twine | p50 / p99 net/http | verdict (≥ 5×) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 210K (206–210K) | 78K (78–78K) | **2.70×** | 1.00 / 1.00 | 1245 / 1507 µs | 3277 / 6554 µs | not met |
| 2 | 415K (411–419K) | 153K (152–154K) | **2.71×** | 2.00 / 1.85 | 623 / 819 µs | 1573 / 5243 µs | not met |
| 4 | 1.180M (1.178–1.185M) | 337K (331–339K) | **3.51×** | 4.00 / 3.74 | 213 / 328 µs | 590 / 3408 µs | not met |
| 8 | 2.165M (2.158–2.170M) | 634K (572–644K) | **3.41×** | 7.87 / 6.83 | 115 / 279 µs | 172 / 3146 µs | not met |
| 8, c=10000 (one run) | 1.292M | 608K | **2.12×** | 7.91 / 7.87 | 7.6 / 14.7 ms | 15.7 / 25.2 ms | not met |

Twine is at its core limit at every N (cores used = N); net/http is at 85–100% of N cores. The ratio is
2.7× when one core serves both and 3.4–3.5× from 4 cores up, never near 5×. At N=1 twine does 210K req/s on one core (the 1-loop echo does 370–417K).

### Where the time goes at 8 loops (evidence for the loopback-path explanation)

Raw: section "FOLLOW-UP C". Twine echo, 8 loops on cpus 0,2,…,14, `tls` client on 16–31, 1000 connections, deltas over an 8.1 s slice
of the measured window (2.23M msgs/s during the slice):

| | per second |
| --- | --- |
| NET_RX softirqs on the 8 server cpus | 2,231,925 (one per message) |
| NET_RX softirqs on the 16 client cpus | 2,231,996 (one per message) |
| NET_RX softirqs on the idle sibling cpus (1,3,…,15) | 18 |
| server threads, time on cpu (`/proc/<pid>/task/*/schedstat`) | 7.93 cores (each loop thread 98–99% running) |
| server threads, run-queue wait | 0.167 cores in total (4–12 ms per loop-second) |
| server scheduler timeslices | 18,376/s in total (about 2,300 per loop-second, against ~280K messages per loop-second) |

Reading: on loopback the receive half of every packet runs as a NET_RX softirq on the CPU of the sender, so each server `write(2)`
also pays for the TCP receive path of the client's socket (and each client write pays the same for the server's socket): the server CPUs
take one NET_RX per message in addition to their own `read`, `write` and `epoll_wait`. The loop threads are not waiting for a CPU (2%
run-queue wait) and almost never sleep (about 1 context switch per 120 messages), so the loss of per-core throughput from 1 to 8 loops (2.4 → 3.6 µs per
message) is not scheduling contention on the server cpus. It is consistent with that kernel path getting slower when 8 cores and 8 client cores run it at once
(shared socket/lock/cache state, or the hypervisor), but these counters do not isolate which; no `perf` was available.

### Secondary note: the old client-bound numbers

Kept for continuity, not used for any verdict. In-process (`go test`, client in
the server process): echo linearity 0.99 / 0.88 / 0.70 at 2 / 4 / 8 loops
(median-of-5: 0.69), HTTP twine/net-http 0.94–1.16×. Earlier separate-process
runs with the Go goroutine client, both servers on cpus 0–15: echo linearity 0.97
/ 0.99 / 0.76 and HTTP 3.14× (1.53M vs 486K). The pinned runs above agree on echo
(0.76 at 8 loops with `gor`) and are in the same range on HTTP (2.50–2.89× with
`gor` against 3.14×: net/http measured 507–586K here against 486K then). The in-process figures say little because the client takes most of the CPU.

#### Same spinning client for both servers at one core (follow-up D)

With the `tls` client (8 twine loops, spin 5000) driving both servers on cpu 0,
twine serves 374K req/s (3 runs: 373–375K) and net/http with `GOMAXPROCS=1`
161K (160–162K): **2.32×**. Both servers run at 1.00 cores. Twine's one-loop
HTTP rate equals its one-loop echo ceiling (371K msgs/s), so the HTTP parser and
response path add nothing measurable; the ratio is set by the kernel loopback
path, which also serves net/http. Raw lines: `bench/results/pinned.txt`,
section "FOLLOW-UP D".

## 1M-connection soak (Phase 6, Section 2 memory target)

Raw samples every 10 s: [`bench/results/idle1m.txt`](bench/results/idle1m.txt).
`bench/idle1m/server` (twine echo, 8 loops pinned to 0,2,…,14, 40 ports from
20000, `RLIMIT_NOFILE` raised to the 1,048,576 hard limit, `Backlog(65535)`)
and `bench/idle1m` (client: 64 goroutines, raw blocking sockets (3 s connect timeout) bound
to 127.0.0.2–127.0.0.51 round-robin with `IP_BIND_ADDRESS_NO_PORT`, paced to
40,000 connects/s, retries on ECONNREFUSED/ETIMEDOUT/EAGAIN/EADDRNOTAVAIL, aborts
if MemAvailable < 4 GB). Driver: `bench/idle1m/soak.sh -netns`.

| | |
| --- | --- |
| Connections held | **1,000,000** (all established, no retries; 2000 sampled connections echoed a byte after the hold: 2000/2000) |
| Time to establish | **25.0 s** at the 40,000/s pacing limit (the server kept up; the limit is mine, not the server's) |
| Hold | 10 minutes |
| Server RSS | 18.1 MB idle, **580 MB at 1M**, **peak 582.0 MB**, 582.0 MB at the end |
| Stability | RSS at hold minute 2: 581.4 MB; at the end: 582.0 MB; drift **+0.55 MB (+0.095%)**; Go heap in use 552 MB, 14 GCs in total |
| Per connection | **564 B RSS** (peak − idle baseline)/1M; heap in use 552 B/conn |
| Host memory | MemAvailable 30.3 GB → 23.6 GB (minimum), so 6.7 GB for both ends of 1M loopback connections (2M sockets, about 3 KB per socket) including the server's 0.58 GB and the client; the 4 GB abort threshold was never close |
| Server kernel memory | not separable here; if the two ends cost the same it is about 3 GB, so RSS plus kernel ≈ 3.6 GB, under the 4 GB target even when counted. TCP buffer memory in `/proc/net/sockstat` stayed at 794 pages (idle sockets hold no buffers) |
| fd budget | server: 1,000,000 conns + 320 listener fds (8 reuseport listeners × 40 ports) + epoll/eventfd, below 1,048,576; the client needs the same 1,000,000+ |

Verdict: **met**: 1M idle connections on one twine process, RSS 0.58 GB against the ≤ 4 GB
target, flat over 10 minutes. (The earlier 0.69–0.75 GB estimate extrapolated from
10K/50K was 20–30% high: per-connection RSS falls from ~700 B at 50K to 564 B at 1M.)

What blocked the first attempt: in the host's root network namespace the soak
stalled at **261,600 connections**: `nf_conntrack_count` reached `nf_conntrack_max`
(262,144) and progress stopped: client `connect()` calls timed out (1,143 retries in 60 s,
but only ~1,500 new connections), consistent with the kernel dropping packets of untracked flows. It is not an
fd, memory or twine limit (server RSS was 164 MB, 28.5 GB free), and `nf_conntrack_max`
cannot be raised without root. The reported run therefore executes server and client
inside a private user+network namespace (`unshare -Urn`, unprivileged), which has
no conntrack rules and where `somaxconn` could be raised to 65535. This means
the kernel TCP path is the same but without conntrack's per-connection state;
a host whose loopback or NIC traffic is tracked by conntrack needs a larger
table (about one entry per connection, so over 1M) for 1M.

This host is 32 vCPUs and the server used 8 of them; the design target is a 16-core
host. Memory is independent of core count, so the memory verdict transfers; the
connection-establishment rate does not.

## Full harness results (`bench/`, `-count=1`; in-process, client-bound: secondary)

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

### Echo scaling (in-process, 1000 conns, 64 B; client-bound, secondary)

| loops | twine msgs/s | linearity | loop-busy | cpu-util | gnet msgs/s | gnet linearity |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 332K | 1.00 | 1.00 | 0.18 | 336K | 1.00 |
| 2 | 657K | 0.99 | 1.00 | 0.26 | 664K | 0.99 |
| 4 | 1.16M | 0.88 | 1.00 | 0.41 | 1.25M | 0.93 |
| 8 | 1.85M | **0.70** | 0.96 | 0.62 | 1.79M | **0.66** |

### Echo and HTTP scaling, separate pinned processes with the Go goroutine client (earlier run, 5 s; secondary)

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

6. **Server per-message cost depends on whether the peer thread is asleep
   (environment, relevant to `SpinIterations`).** A loopback `write(2)` wakes the
   receiver inside the syscall. One pinned twine loop serving 1000 connections
   does 152K msgs/s against an 8-loop twine client with `SpinIterations(0)`,
   204K with the default spin, 367K with `SpinIterations(5000)`, and 412K
   against a single busy client loop. The server loops show 100% CPU in every
   case. So 1-loop baselines taken against different clients differ by 2.7×, and
   a scaling ratio needs both ends measured with the same client. Not a core bug.
7. **No core bug found while building the pinned runs.** `PinLoops` works as
   documented (loops stay on their CPUs; 8 loops on cpus 0–7, 0,2,…,14 and 0–15 give the
   same total). The 8-loop aggregate ceiling (about 2.2M 64 B msgs/s, 2.14M HTTP
   req/s) is shared with gnet and is independent of CPU placement and of the
   client; per-core cost rises from 2.4 µs/msg at 1 loop to 3.6 µs at 8.

## Running

```sh
make bench                                   # everything, -count=5, results in bench/results/<sha>.txt
cd bench && go test -run '^$' -bench EchoScaling -count 5 .
cd bench && go test -run '^$' -bench EchoIdle . -idle-conns=100000   # flags: -idle-conns -idle-settle -echo-loops -scaling-spin
cd bench && go run ./echo_twine -loops 8 &   # then: cd bench && go run ./echoclient -conns 1000 -size 64 -duration 10s
cd bench && go run ./http_twine -loops 8 &   # then: cd bench && go run ./httpclient -conns 256 -duration 10s
```

Pinned, server-bound runs and the soak (not part of `make bench`; each takes minutes):

```sh
cd bench
go run ./scaling -mode echo -servers twine,gnet -loops 1,2,4,8 -conns 1000,4000 \
    -clients gor,tl,tls,tlm,tlms -duration 10s -out results/pinned.txt
go run ./scaling -mode http -servers twine@8,nethttp@8,nethttp@16@0-15 -conns 256,10000 \
    -clients gor,tl,tls -duration 10s -out results/pinned.txt
idle1m/soak.sh -n 1000000 -hold 10m -netns > results/idle1m.txt   # -netns: see the soak section
# by hand: taskset -c 0,2 ./echo_twine -loops 2 -pin 0,2 &  taskset -c 16-31 ./twineload -proto echo \
#   -conns 1000 -loops 8 -pin 16,18,20,22,24,26,28,30 -server-pid $!
```

`bench/echo_goroutine`, `bench/echo_gnet` and `bench/http_nethttp` take the same
`-addr`/`-loops` flags. For the separate-process numbers, pin the server and
client to disjoint CPUs with `taskset`.
