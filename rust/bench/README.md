# bench (Rust)

Benchmark servers, external baselines and a load generator for the Rust port
(design Section 12). Methodology follows `../../BENCHMARKS.md`: server and load
generator are separate processes pinned to disjoint CPUs, the load generator is
an epoll event-loop client (not goroutine/thread per connection), and the
server's CPU time is read from `/proc/<pid>/stat` for every cell to show which
side is saturated.

```sh
bench/scripts/bench.sh            # whole matrix (~4 min), writes bench/results/<git-sha>.txt
DUR=5 LOOPS="1 2 4" bench/scripts/bench.sh
cargo build --release -p twine-bench     # binaries land in target/release/
```

## Binaries

| Binary | What |
| --- | --- |
| `echo_twine` | twine echo server. `--addr --loops --pin a,b,c --spin N --idle D` |
| `echo_threads` | std thread-per-connection blocking echo (baseline). `--addr` |
| `echo_tokio` | tokio multi-thread echo (baseline, the only use of tokio). `--addr --workers N` |
| `http_twine` | twine-http hello world. `--addr --loops --pin --spin` |
| `http_tokio` | hand-written HTTP/1.1 hello on tokio: keep-alive, pipelining, `\r\n\r\n` framing, `Content-Length: 13`, `Date`. `--addr --workers N` |
| `loadgen` | `loadgen echo\|http`; see below |
| `timers` | 1M `call_later` insert, `cancel_timer`, insert+cancel at 1M active, and fire; ns/op. `--n` |
| `callfromthread` | 16 producer threads hammer `call_from_thread` for 5 s; calls/s and `wakeups / calls` from `MetricsSnapshot`. `--threads -d` |

### loadgen

```sh
loadgen echo --addr 127.0.0.1:9000 -c 1000 -d 10s --size 64 --threads 4 --pin 24-27
loadgen http --addr 127.0.0.1:8080 -c 256 -d 10s --pipeline 16 --threads 8 --pin 24-31
loadgen http --addr 127.0.0.1:8080 -c 256 -d 10s --rate 800000 --server-pid $PID   # open loop
```

| Flag | Meaning |
| --- | --- |
| `-c`, `-d` | connections; measured window (after `--warmup`, default 1s) |
| `--size` | echo message bytes (default 64) |
| `--pipeline` | http: requests sent back to back per batch; the next batch goes out when all responses are in (TechEmpower plaintext style) |
| `--rate` | requests/s offered; selects the open-loop mode |
| `--threads`, `--pin` | event-loop threads (default: number of pinned CPUs, else 4); thread i runs on `cpus[i % len]` |
| `--server-pid` | report that process's CPU use over the window |

**Closed loop** (default): one batch in flight per connection, so the result is the
saturation throughput of whatever side is slower. **Open loop** (`--rate`):
batches are issued on a fixed schedule regardless of responses (wrk2 style) and
latency is measured from the *intended* send time, so a stalled server is charged
for the queueing it causes (no coordinated omission). When every connection is busy
the batch stays scheduled and goes out as soon as a response frees a connection
(`overdue_ticks` counts those events). Open-loop threads busy-poll.

Latency is per batch (one echo message or one pipelined group of `--pipeline`
requests), in a log-linear histogram with 16 buckets per power of two (reported
value is the bucket upper bound, at most ~6% high). Every message is timed, with
one clock read per `recv`; nothing allocates per message (HTTP partial responses
reuse a per-connection carry buffer).

Output ends with one machine-readable line:

```
RESULT mode=http conns=256 threads=4 pipeline=1 rate=0 rps=1411876 per_conn=5515.1 p50_us=172 p90_us=279 p99_us=442 p999_us=623 max_us=1630 errors=0 bytes_rx=554357830 server_cores=3.97 client_cores=3.78
```

`errors` counts connection failures, resets and protocol errors; `client_cores`
near the thread count means the load generator itself is saturated and the
result is client-bound.

## Methodology notes

- `bench.sh` detects `nproc`; the load generator takes the last 8 cores (half the
  machine when it has fewer than 16) and servers take cores from the front. Twine at
  k loops is pinned to cores `0..k-1`; the baselines and the HTTP servers get the first
  `min(4, free cores)` cores via `taskset`. Override with `LG_CPUS`, `BASE_CORES`,
  `LOOPS`, `DUR`.
- Loopback numbers are bounded by syscall cost (one `read` and one `write` per
  64 B message), not by the framework; pipelined HTTP shows the framework's own
  cost. Always compare `server_cores` and `client_cores` before reading a number
  as a server limit.
- A loopback `write(2)` wakes the peer inside the syscall, so the client's
  configuration affects the server's per-message cost. Compare only runs with the
  same load generator settings.
- The open-loop cell offers 80% of the same server's closed-loop peak at c=256,
  pipeline 1.
- Raise `ulimit -n` for c=10000 (the script does it up to the hard limit). WSL2
  numbers carry ~0.6 ms of vCPU jitter at the p99.
- tokio is allowed in this crate only, as the external baseline. `unsafe` is
  confined to `loadgen` (libc sockets, epoll, affinity).

## Idle-connection memory (`idle_server`, `idle_client`, `scripts/idle.sh`)

Measures server RSS per idle connection (design target: 1M idle connections, < 1 KB each
excluding user state, <= 4 GB RSS).

    export CARGO_TARGET_DIR=$PWD/target/idle
    cargo build --release -p twine-bench --bin idle_server --bin idle_client
    bench/scripts/idle.sh 100000      # root netns
    bench/scripts/idle.sh 1000000     # re-executes inside `unshare -Urn` (conntrack cap 262144 in the root netns)

`idle.sh N` pins a 2-loop `idle_server` to `$SRV_CPUS`, opens N connections with
`ceil(N/250000)` `idle_client` processes (disjoint source ranges in 127.0.0.0/8, so the
~28K ephemeral ports per source address are not exhausted), holds 60 s while sampling server RSS
every 10 s, pings a random sample of connections on each client (16-byte echo), and appends a
summary to `bench/results/idle.txt`. Env: `SRV_CPUS`, `CLI_CPUS`, `RATE`, `LOOPS`, `HOLD`,
`SAMPLE`, `PORT`, `MAX_RSS_MB`. The fd limit is raised to the hard limit by both binaries.
