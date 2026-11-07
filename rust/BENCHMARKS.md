# Benchmarks (Rust port)

Measured 2026-11-07 on the development host: 32-vCPU WSL2 VM (Linux 6.18, Hyper-V),
32 GB RAM, Rust 1.95 release profile. Loopback only; every number is bounded by the
VM's loopback path (one NET_RX softirq per message on the server CPU plus a
cross-CPU wakeup of the load generator), exactly as the Go port's
[../BENCHMARKS.md](../BENCHMARKS.md) documents. Raw output: `bench/results/ddf76dc-20261107T193907Z.txt` (final matrix, after the
load-generator fixes and the sparse slot table; `latest.txt` points at it), `ddf76dc.txt`
(first run), `idle.txt` (idle soaks).

Methodology (`bench/scripts/bench.sh`): servers pinned to cores 0..k-1 (twine) or
confined to cores 0-3 with `taskset` (baselines, HTTP), the Rust load generator
(`bench/src/bin/loadgen.rs`, epoll based, 4 threads) pinned to cores 24-31; 1 s
warm-up, 8 s measured per cell; latency is per request, closed loop unless `rate`
is given (open loop, wrk2-style, recorded from the intended send time).

## Section 2 targets

| Metric | Target | Measured | Verdict |
| --- | --- | --- | --- |
| Echo, 1M idle connections, RSS | ≤ 4 GB (and < 1 KB/conn) | 499 MB at 1M connections on 8 loops (470 B/conn delta, 499 B/conn total), 980 MB on 2 loops before the sparse slot table; flat over the hold; every sampled connection still echoed | **met** |
| HTTP/1.1 plaintext, req/s per core | ≥ 1M | 2.77M (matrix, 4 loops, c=256, pipeline 16); 4.12M in the head-to-head run; 233K unpipelined | **met** with pipelining (TechEmpower plaintext setting), not without |
| HTTP/1.1 p99 at 80% load | ≤ 1 ms | 102 µs at 748K req/s open loop, 4 loops, all 256 connections round-robin (tokio: 295 µs at 636K) | **met** |
| Echo, 64 B, msgs/s per core | ≥ 2M | 172K at 1 loop, 309K per core at 8 loops (load generator saturated there: lower bound) | **not met**: loopback-bound; Go twine measures 173K / 165K on the same cells |
| allocs/op read, write, timer fire | 0 | 0 (alloc gates in `twine/tests/alloc_gates.rs`, `twine-http/tests/alloc.rs`, `twine-proto/tests/alloc_gates.rs`) | **met** |
| Timer insert + cancel, 1M active | ≤ 50 ns | 14.9 ns (insert 32.3, cancel 9.6, fire 39.6) | **met** |

## Matrix (`bench/scripts/bench.sh`, DUR=8, final code)

| bench | config | req/s | req/s per server core | p50 µs | p99 µs | p99.9 µs | server cores | flags |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| echo_small | twine 1 loop | 172,289 | 172,289 | 6,029 | 6,816 | 15,204 | 1.00 | |
| echo_small | twine 2 loops | 343,843 | 171,922 | 3,015 | 3,408 | 4,456 | 2.00 | |
| echo_small | twine 4 loops | 992,840 | 247,591 | 1,016 | 1,638 | 2,621 | 4.01 | |
| echo_small | twine 8 loops | 2,445,909 | 309,217 | 393 | 918 | 1,180 | 7.91 | loadgen saturated (lower bound) |
| echo_small | thread-per-connection (4 cpus) | 643,292 | 160,823 | 1,049 | 4,063 | 8,126 | 4.00 | |
| echo_small | tokio (4 workers) | 798,437 | 199,609 | 885 | 4,719 | 6,554 | 4.00 | |
| http | twine 4 loops c=256 p=1 | 934,986 | 233,164 | 279 | 459 | 983 | 4.01 | |
| http | twine 4 loops c=256 p=16 | 11,110,384 | 2,770,669 | 377 | 590 | 1,180 | 4.01 | |
| http | twine 4 loops c=10000 p=16 | 7,861,273 | 1,960,417 | 20,972 | 24,117 | 26,214 | 4.01 | |
| http_open | twine 4 loops c=256 rate=748K (80% of peak) | 747,989 | 201,072 | 19 | 102 | 262 | 3.72 | |
| http | tokio 4c c=256 p=1 | 794,900 | 199,223 | 328 | 688 | 950 | 3.99 | |
| http | tokio 4c c=256 p=16 | 12,436,639 | 3,116,952 | 328 | 688 | 950 | 3.99 | |
| http | tokio 4c c=10000 p=16 | 10,247,625 | 2,574,780 | 1,114 | 75,497 | 79,692 | 3.98 | |
| http_open | tokio 4c c=256 rate=636K (80% of peak) | 635,917 | 207,139 | 102 | 295 | 377 | 3.07 | |

Pipelined cells report latency per batch of 16. All cells: 0 errors, 0 non-200 responses,
every configured connection active (`conns_used`). The open-loop load generator busy-polls,
so its "saturated" flag is informational there.

Timers (1M active): insert 32.3 ns, cancel 9.6 ns, insert+cancel 14.9 ns, fire 39.6 ns.
`call_from_thread`, 16 producer threads: 4.98M calls/s, 0.12 wakeups per call
(0.016 when producers are confined to 4 CPUs).

Echo scaling 1→8 loops is superlinear (13.9×) because the per-message cost at one
loop is dominated by the kernel: with 1000 connections on one loop the loop thread
spends 94% of its time in `read`/`sendto` (measured from `/proc/<tid>/stat`: 0.17 s
user vs 2.81 s system per 3 s) and only two syscalls are issued per message. The
Section 12 "≥ 0.8× linear" gate is therefore met but not informative on this host.

## Head to head: Rust twine vs Go twine, same load generator, same cores

`/tmp/claude-1000/compare.sh` ran the Go `bench/echo_twine` / `bench/http_twine`
(GOTOOLCHAIN=local, `-pin` to the same cores) and the Rust servers under the identical
Rust load generator, 8 s per cell:

| cell | Go twine req/s | Rust twine req/s | Rust / Go | Go p99 µs | Rust p99 µs |
| --- | ---: | ---: | ---: | ---: | ---: |
| echo c=1000, 1 loop | 180,267 | 180,911 | 1.00 | 6,554 | 6,816 |
| echo c=1000, 4 loops | 1,381,539 | 1,549,085 | 1.12 | 1,311 | 1,507 |
| echo c=1000, 8 loops | 1,308,171 | 1,430,375 | 1.09 | 1,573 | 1,376 |
| http c=256 p=1, 4 loops | 1,277,290 | 1,433,785 | 1.12 | 426 | 442 |
| http c=256 p=16, 4 loops | 13,960,610 | 16,499,709 | 1.18 | 557 | 623 |
| http c=10000 p=16, 4 loops | 8,918,902 | 9,622,003 | 1.08 | 20,972 | 18,874 |

(Final-code run; an earlier run gave 1.03–1.32×. The echo cells use 4 load-generator
threads and are higher than the matrix cells; the ratios are what matters.)

## Versus the tokio baseline

The Go design's HTTP gate was "≥ 5× `net/http`", a baseline that tops out around
160–190K req/s per core. The Rust bench uses a hand-written tokio HTTP/1.1 server as
the external baseline instead, which is far stronger: twine is 1.18× tokio
unpipelined at c=256 (935K vs 795K), 0.89× pipelined at c=256, 0.77× at c=10000
with pipelining, and has a 2.9× lower p99 in the open-loop 80% cell (102 µs vs
295 µs). The pipelined c=10000 gap is a fairness effect: twine serves 2500
connections per loop round-robin, so each connection's 16 responses wait one full
iteration (p50 21 ms, p99 26 ms), while tokio's work stealing gives a 1 ms p50 at
the cost of an 80 ms p99. Both servers are co-limited by the 4-thread load generator
at these rates (`client_cores` ≈ 3.9).

## Idle connections (`bench/scripts/idle.sh`, `bench/results/idle.txt`)

| run | loops | established | server RSS | per connection | hold | echo sample |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| 100K, root netns | 2 | 100,000 (0 failed) | 129.6 MB | 1,197 B | flat over 60 s | 500/500 |
| 1M, `unshare -Urn` | 2 | 1,000,000 (0 failed, 25 s) | 980.6 MB | 970 B | flat over 60 s | 2000/2000 |
| 100K | 8 | 100,000 | 501 MB → **79 MB** | 4,692 B → **503 B** | flat | 500/500 |
| 1M, `unshare -Urn` | 8 | 1,000,000 (0 failed, 25 s) | **499 MB** | **470 B** | flat over 10 s | 2000/2000 |

The arrows mark the sparse slot table (design Section 14): each loop's fd-indexed table
used to hold a full `ConnSlot` per fd, so memory scaled with loops × highest fd. Now an
empty fd costs 16 bytes and live connections are boxed slots recycled through a per-loop
free list. Kernel socket memory is not included in RSS. The host's root network namespace
caps conntrack at 262,144 entries, hence the unprivileged namespace for the 1M runs.

## Notes

- `callfromthread` showed that the cross-thread queue spills to an unbounded
  overflow vector by design (design Section 7.5, same as Go). With 16 producers
  confined to 4 CPUs the loop cannot keep up and the backlog grows without limit;
  the bench throttles producers past 1M queued tasks. Production callers should
  bound their own submission rate.
- The io_uring readiness backend (`--features uring`) passes the full suite; its
  idle `wait(0)` costs 4.8 ns against epoll's 72 ns (a CQ peek vs a syscall). It
  was not benchmarked end to end.
