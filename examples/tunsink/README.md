# tunsink: TCP → TUN + Kafka, Go vs Rust

A twine server that accepts TCP clients and writes every length-prefixed
frame they send into a TUN device and onto a Kafka topic. This directory
holds the Go server and a benchmark harness that compares it with the Rust
twine server (same design, same flags, same output) on the identical pipeline,
with a goroutine-per-connection baseline as a reference row.

The Go side is a separate module (it depends on
[franz-go](https://github.com/twmb/franz-go)); build it from this directory.

```
.
├── main.go              Go twine server (the example itself)
├── internal/tunpkt      TUN open helper and an IPv4/UDP packet builder
├── cmd/baseline         same server written with net.Conn + one goroutine per connection
├── cmd/loadgen          N connections streaming frames as fast as the server accepts
├── cmd/fakekafka        in-process kfake broker so the benchmark needs no Kafka install
├── bench.sh             the benchmark matrix (runs in an unprivileged netns)
├── results/             raw benchmark output (quick-go-vs-rust.txt from the host described below)
└── ../../rust/examples/tunsink  Rust twine server (tunsink-rs), same flags and output
```

## Wire format and data flow

Clients send frames of a 4-byte big-endian length followed by one IP packet
(`proto.LengthPrefixed` defaults, 65535-byte maximum). Each frame goes to
the TUN (as a packet received on that interface) and to Kafka (as one record
with the packet as its value).

```
TCP clients ──► twine loop(s) ──► proto.LengthPrefixed ──► FrameReceived
                                                             │
                     -mode direct:  write(tunfd, frame) on the loop (no copy)
                     -mode channel: copy ──► sink chan ──► sink goroutines ──► write(tunfd)
                                                             │
                                             copy ──► sink chan ──► sink goroutines ──► kgo.Produce
```

Everything that touches a twine socket runs on a loop thread inside Protocol
callbacks. Anything that can block runs on a goroutine: the Kafka client
always, the TUN write in `channel` mode. The two cross the line through a
channel send that never blocks the loop and `CallFromThread` on the way back.

Why two modes: a non-blocking TUN fd never blocks on write (the kernel drops
the packet if the device queue is full), so the write can be made on the
loop while the frame is still in the shared read buffer. That saves the
copy and the channel hop for the TUN leg. `channel` mode is the general
shape for a sink that can block.

### Backpressure

The sink channel holds 4096 frames. When it is full the connection stops
reading (`PauseReading`), one drainer goroutine pushes that connection's
backlog, then hands control back to the loop with `CallFromThread`, which
either resumes reading or drains again if the trailing chunk added more
frames. All pause/resume state changes happen on the loop thread. While a
connection is paused its socket buffer fills and the sender's TCP window
closes, so nothing is dropped. The baseline gets the same effect from a
blocking channel send in the connection's goroutine.

Kafka records are produced with `Produce`, which blocks the sink goroutine
when the client buffer (1M records) is full, so Kafka backpressure also
reaches the senders. `-kafka-drop` switches to `TryProduce`, which drops
instead. Records are produced with a background context and flushed before
exit so shutdown does not fail what is still buffered.

### The Rust server

`tunsink-rs` implements the same design with three structural differences.
Kafka goes through a librdkafka `ThreadedProducer`, and a sink that gets
`QueueFull` retries instead of blocking in franz-go's `Produce`. The drain
hand-off runs on the engine's worker pool instead of a goroutine per drain.
Its sink threads and the librdkafka threads are plain OS threads. Everything
else is the same: `direct` and `channel` modes, the 4096-frame sink channel,
one copy per frame, and the pause/drain/resume sequence on the loop thread.

## Running it

```sh
sudo ip tuntap add dev tun0 mode tun user $USER
sudo ip addr add 10.0.0.1/24 dev tun0 && sudo ip link set tun0 up

# Go server
go run . -tun tun0 -addr :9000 -brokers localhost:9092 -topic tun-packets -mode direct -loops 2

# Rust server (needs cmake on PATH for librdkafka)
cargo build --release --manifest-path ../../rust/examples/tunsink/Cargo.toml
../../rust/examples/tunsink/target/release/tunsink-rs -tun tun0 -addr :9000 -brokers localhost:9092 -topic tun-packets -mode direct -loops 2

go run ./cmd/loadgen -addr 127.0.0.1:9000 -conns 64 -size 512 -duration 10s
```

Both servers take the same flags (`-addr -brokers -mode -loops -pin -sinks
-stats -watchdog -topic -tun -kafka-drop -spin`) and print the same
`total frames=... bytes=... tun_err=... kafka_ok=... kafka_err=... pauses=...`
line on exit. `-spin N` (default -1, the library default) sets the loop's busy-poll iterations before it blocks. Without root, run inside an unprivileged user+network namespace
(`unshare -rn`), which is what `bench.sh` does. `-brokers ""` disables Kafka.

## Benchmark

`./bench.sh matrix results/matrix.txt` (or `./bench.sh quick results/quick.txt` for the ~3-minute Go-vs-Rust subset) builds everything, enters `unshare -rn`,
creates `tun0`, and runs each cell: one server process pinned to a CPU set, a
fresh `fakekafka` on its own CPUs (kfake keeps every record in memory, so it
is restarted per cell), and `loadgen` on the remaining CPUs streaming
512-byte packets for 8 s (`DURATION` overrides). Throughput is the load
generator's sent frames/s; every frame is confirmed to have reached the TUN
(interface RX counter) and Kafka (`kafka_ok` equals frames). `cpu_s` is the
server's CPU time over the run. Each cell prints `sunk/s`, the TUN RX delta divided by the load generator's
elapsed time, and `check=ok|FAIL`, which requires sent and TUN RX to differ by
at most `conns*128` (the loadgen counts only whole 64-frame batches, so each
connection's last partial batch can land on either side), and with Kafka on `kafka_ok` to equal the server's frame count. The
loadgen's `frames/s` is only the sent rate, and frames parked in kernel socket
buffers count as sent but never reached the sinks, so `compare` uses `sunk/s`.
Both servers run with `-spin 0` because their loops auto-select busy-poll
iterations from different CPU counts (GOMAXPROCS for Go, taskset size for
Rust), which would make them spin differently at 1core. A server that crashes in a cell prints
`frames/s=-` and the matrix continues.

Three server names: `go` (the Go twine server; `twine` is accepted as an
alias), `rust` (the Rust twine server) and `baseline` (goroutine per
connection). A single cell is `./bench.sh cell SERVER MODE KAFKA CORES CONNS SIZE`.

- **Rust build.** `bench.sh` builds `tunsink-rs` with cargo (into the crate's
  own `target/release`) before entering the namespace, which needs `cmake` on
  PATH (the script prepends `$HOME/.local/bin`). `RUST_BIN=/path/to/tunsink-rs`
  uses a prebuilt binary instead of building. `SKIP_RUST=1` skips the build
  and every `rust` cell.
- **Compare.** `./bench.sh compare results/quick-go-vs-rust.txt` prints, per
  (mode, kafka, cores, conns, size) cell, Go and Rust `sunk/s` (`frames/s` for older files), Rust/Go as a
  percentage, the baseline's figure, and a `chk` column naming servers whose `check` failed. Missing rows show `-`; old `twine`
  rows count as `go`.

CPU budgets (one thread per physical core, siblings left idle):

| config | CPUs | go | rust | baseline |
| --- | --- | --- | --- | --- |
| 1core / 2core / 4core | N | N loops pinned, N sink goroutines, GOMAXPROCS=N+1 | N loops pinned (`-pin`); sink threads and librdkafka threads float in the set | GOMAXPROCS=N |
| 1+1 / 2+2 | 2N | N loops pinned to half, sinks and Kafka client on the rest | N loops pinned to half; sink and librdkafka threads float in the set | GOMAXPROCS=2N |

The Go server runs with GOMAXPROCS=N+1 because its loops are locked OS
threads: with GOMAXPROCS=N the sink and Kafka goroutines starve behind a
saturated loop. The Rust server has no GOMAXPROCS equivalent.

## Results

Quick comparison (`./bench.sh quick results/quick-go-vs-rust.txt`, 2 min 14 s):
Go twine vs Rust twine only, 4 s cells, one run each, 512-byte packets. Host:
i9-14900KF, WSL2 kernel 6.18, Go 1.25.3 + franz-go 1.22.2, Rust 1.95 +
librdkafka 2.10.0 (rdkafka 0.38), kfake broker on 3 dedicated CPUs. All 20
cells pass `check` (every frame reached the TUN and, with Kafka on, Kafka). Raw
rows in [results/quick-go-vs-rust.txt](results/quick-go-vs-rust.txt). Numbers
are `sunk/s`, million frames per second written to the TUN during the load
generator's run:

| cell | Go | Rust | Rust / Go |
| --- | --- | --- | --- |
| direct, TUN only, 1 core, 64 conns | 1.91 | 2.29 | 1.20× |
| channel, TUN only, 1 core, 64 conns | 1.35 | 1.95 | 1.45× |
| direct, TUN only, 1 core, 1000 conns | 1.62 | 2.19 | 1.35× |
| channel, TUN only, 1 core, 1000 conns | 1.19 | 1.71 | 1.43× |
| direct, TUN only, 4 cores, 1000 conns | 5.44 | 6.06 | 1.11× |
| direct, TUN + Kafka, 1 core, 64 conns | 0.90 | 0.93 | 1.03× |
| channel, TUN + Kafka, 1 core, 64 conns | 0.85 | 0.94 | 1.10× |
| direct, TUN + Kafka, 1 core, 1000 conns | 0.90 | 0.86 | 0.96× |
| channel, TUN + Kafka, 1 core, 1000 conns | 0.79 | 0.87 | 1.11× |
| direct, TUN + Kafka, 4 cores, 1000 conns | 1.91 | 1.17 | 0.61× |

### Reading the numbers

- **TUN only: Rust is ahead everywhere, 11–45%.** In `direct` mode both
  servers are bound by one TUN `write(2)` per frame; the Rust loop spends less
  per frame around that syscall (no GC, no goroutine scheduling, a 20% gap at
  one core that narrows to 11% at four cores where the TUN write dominates).
  In `channel` mode the gap widens to ~45% because the hand-off is where the
  runtimes differ most: the Rust server hands frames to its sink thread through
  a crossbeam bounded channel and drains paused connections with one blocking
  drainer thread, the Go server through a Go channel with a goroutine per
  drain; the drainer design was the decisive change (its first version, a
  spinning worker pool, was 25% *behind* Go at 1000 connections).
- **TUN + Kafka at one core: parity, within ±10%.** The pipeline is bound by
  the Kafka client (batching, snappy, broker round trip) sharing the CPU with
  the loop and the sink. franz-go and librdkafka cost about the same here.
- **TUN + Kafka at four cores: Rust at 61%, reproducible (three runs:
  1.33, 1.26, 1.17 M/s vs 1.90, 1.90, 1.91 M/s).** This is a Kafka client
  difference, not a twine one: librdkafka runs one `rdk:main` thread per
  client instance that does all batching and compression, so with four loops
  and four sinks feeding one producer it serialises on that thread (the Rust
  server also burns more CPU, 15.5 vs 13.4 cpu-s, polling `QueueFull`).
  franz-go compresses in the producing goroutines. One librdkafka producer per
  sink thread would parallelise it; left as a follow-up so both servers keep a
  single client.
- **`sunk/s`, not the load generator's rate, is the metric.** At 1000
  connections the generator outruns a one-core server and parks up to ~2.5M
  frames in socket buffers; those are sunk during shutdown and would inflate a
  sent-based rate by 30–40%. `check` confirms nothing was lost.

Two bugs were found and fixed while building this comparison:

- **twine (Go and Rust): a hangup lost across the per-event read cap.** With
  edge-triggered polling a FIN is reported once. If the event carrying it also
  carried more data than `max_reads_per_event` allows, the re-armed read next
  iteration stopped at the final short read and never saw EOF, so the
  connection stayed open until the shutdown timeout (5 s per cell). Both
  libraries now remember the hangup on the connection
  (`TestHangupSurvivesReadCap`, `hangup_survives_read_cap`).
- **Go tunsink: `panic: send on closed channel`** at shutdown when a drain
  goroutine was still pushing its backlog as `close(sink)` ran. Shutdown now
  waits for live drains.

The full matrix (`./bench.sh matrix`, go/rust/baseline, five CPU budgets,
~25 min) has not been rerun since the baseline-only results were replaced; run
it for the complete picture.
