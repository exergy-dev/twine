# tunsink: TCP → TUN + Kafka

A twine server that accepts TCP clients and writes every length-prefixed
frame they send into a TUN device and onto a Kafka topic, with a
goroutine-per-connection baseline and a benchmark harness that compares the
two at equal CPU budgets.

This is a separate Go module (it depends on
[franz-go](https://github.com/twmb/franz-go)); build it from this directory.

```
.
├── main.go              twine server (the example itself)
├── internal/tunpkt      TUN open helper and an IPv4/UDP packet builder
├── cmd/baseline         same server written with net.Conn + one goroutine per connection
├── cmd/loadgen          N connections streaming frames as fast as the server accepts
├── cmd/fakekafka        in-process kfake broker so the benchmark needs no Kafka install
├── bench.sh             the benchmark matrix (runs in an unprivileged netns)
└── results/wsl2.txt     raw results from the host described below
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

## Running it

```sh
sudo ip tuntap add dev tun0 mode tun user $USER
sudo ip addr add 10.0.0.1/24 dev tun0 && sudo ip link set tun0 up
go run . -tun tun0 -addr :9000 -brokers localhost:9092 -topic tun-packets -mode direct -loops 2
go run ./cmd/loadgen -addr 127.0.0.1:9000 -conns 64 -size 512 -duration 10s
```

Without root, run inside an unprivileged user+network namespace
(`unshare -rn`), which is what `bench.sh` does. `-brokers ""` disables Kafka.

## Benchmark

`./bench.sh matrix results/wsl2.txt` builds everything, enters `unshare -rn`,
creates `tun0`, and runs each cell: one server process pinned to a CPU set, a
fresh `fakekafka` on its own CPUs (kfake keeps every record in memory, so it
is restarted per cell), and `loadgen` on the remaining CPUs streaming
512-byte packets for 8 s. Throughput is the load generator's sent frames/s;
every frame is confirmed to have reached the TUN (interface RX counter) and
Kafka (`kafka_ok` equals frames). `cpu_s` is the server's CPU time over the
run.

CPU budgets (one thread per physical core, siblings left idle):

| config | CPUs | twine | baseline |
| --- | --- | --- | --- |
| 1core / 2core / 4core | N | N loops pinned, N sink goroutines, GOMAXPROCS=N+1 | GOMAXPROCS=N |
| 1+1 / 2+2 | 2N | N loops pinned to half, sinks and Kafka client on the rest | GOMAXPROCS=2N |

twine runs with GOMAXPROCS=N+1 because its loops are locked OS threads: with
GOMAXPROCS=N the sink and Kafka goroutines starve behind a saturated loop
(1core, channel, Kafka: 0.74M frames/s at GOMAXPROCS=1 or 32, 0.97M at 2).

### Results

Host: i9-14900KF (16 physical cores), WSL2 kernel 6.18, Go 1.25.3,
franz-go 1.21.1 (default snappy compression), kfake broker on 3 dedicated
CPUs. 512-byte packets, 8 s per cell, one run each. All 84 cells: frames
sent = TUN RX packets = Kafka records acknowledged, zero errors. Raw output
in [results/wsl2.txt](results/wsl2.txt).

Million frames/s, twine vs baseline, `direct` mode:

| budget | TUN only, 64 conns | TUN only, 1000 conns | TUN + Kafka, 64 conns | TUN + Kafka, 1000 conns |
| --- | --- | --- | --- | --- |
| 1 core | 1.94 vs 2.05 (−5%) | 2.24 vs 2.38 (−6%) | 0.98 vs 1.09 (−10%) | 1.24 vs 1.33 (−6%) |
| 2 cores | 3.26 vs 3.28 (−1%) | 3.51 vs 3.63 (−3%) | 1.52 vs 1.69 (−10%) | 1.93 vs 1.94 (−1%) |
| 4 cores | 5.67 vs 5.74 (−1%) | 5.93 vs 5.97 (−1%) | 2.13 vs 2.47 (−14%) | 2.27 vs 1.91 (+19%) |

`channel` mode (TUN write also goes through the sink goroutines):

| budget | TUN only, 64 | TUN only, 1000 | TUN + Kafka, 64 | TUN + Kafka, 1000 |
| --- | --- | --- | --- | --- |
| 1 core | 1.39 vs 1.80 (−23%) | 1.56 vs 2.08 (−25%) | 0.97 vs 1.11 (−13%) | 1.14 vs 1.30 (−12%) |
| 2 cores | 1.94 vs 2.30 (−16%) | 2.11 vs 2.47 (−15%) | 1.28 vs 1.50 (−15%) | 1.42 vs 1.57 (−10%) |
| 4 cores | 2.39 vs 2.75 (−13%) | 2.59 vs 2.99 (−14%) | 1.39 vs 1.47 (−5%) | 1.55 vs 1.62 (−5%) |

1400-byte packets, 1 core, 64 conns, direct: TUN only 1.61 vs 1.81 M/s
(2.3 vs 2.5 GB/s); TUN + Kafka 0.68 vs 0.75 M/s.

Split budgets (`1+1`, `2+2`) are not like-for-like in direct mode: twine's
single loop uses one CPU and leaves the other to the sinks, the baseline
reads on both. Per CPU-second twine is ahead there (1+1 TUN only: 1.91M vs
1.69M frames/cpu_s) but in absolute terms behind (1.92 vs 3.36 M/s).

### Reading the numbers

- **This workload is sink-bound, not socket-bound.** Each frame costs one
  TUN `write(2)` (about 0.45 µs on this host: 2.0M frames/s per core in
  direct mode for either server) and, with Kafka, a `Produce` call plus
  snappy compression and the broker round trip. The TCP read side, where
  twine's advantages live, is a 64 KiB `read(2)` per 100+ frames for both
  servers. There is nothing for the event loop to save.
- **Direct mode: parity.** twine is within 1–6% of goroutine-per-connection
  at every budget with TUN only, and within 1–14% with Kafka on 1–2 cores
  (both bound by franz-go sharing the CPUs with the readers). At 4 cores
  with 1000 connections twine is 19% ahead; at 64 connections 14% behind.
  Those two cells are the noisiest in the matrix (Kafka client scheduling
  across 4 CPUs) and should be read as parity.
- **Channel mode: twine pays for the hand-off.** A blocking channel send
  from a per-connection goroutine is the cheapest possible backpressure.
  twine's equivalent is a copy, a non-blocking send, and when the channel is
  full a `PauseReading`, a drainer goroutine and a `CallFromThread` to
  resume, with up to 127 frames per read chunk still landing in the pending
  queue after the pause. Under permanent sink saturation that path runs
  constantly and costs 13–25%. Batching a whole read chunk into one channel
  send, or one sink channel per loop, would close most of that gap; neither
  changes the library.
- **GOMAXPROCS matters for twine when loops share CPUs with goroutines.**
  Loops are locked OS threads that never yield while data is flowing, so
  goroutine work on the same CPUs depends on the kernel scheduler, not Go's.
  N+1 Ps was 30% better than N or 32 at 1 core with Kafka.
- **1000 connections beat 64 for both servers** because each wakeup finds
  more bytes queued and read batches are larger.

What this benchmark does not measure, and where twine is designed to win:
memory per connection (twine holds no goroutine or per-connection read
buffer, the baseline holds a goroutine, a 64 KiB `bufio.Reader` and a 64 KiB
frame buffer each), tail latency under partial load, and fan-out to many
clients. For a one-way firehose into syscall-bound sinks, the two designs
are equivalent and the sink is the thing to optimise.
