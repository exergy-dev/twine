# tunsink-rs

Rust port of `examples/tunsink` (Go): accepts TCP clients, reads 4-byte
big-endian length-prefixed IP packets (max 65535) and writes each one to a TUN
device and a Kafka topic. Built for head-to-head benchmarking with the Go
server (`examples/tunsink/bench.sh`, which runs `target/release/tunsink-rs`);
flags, modes and the stderr `stats ...` / `total frames=...` lines match.

This crate is deliberately not a member of the `rust/` workspace (empty
`[workspace]` table) so `cargo build --workspace` there never compiles
librdkafka.

## Build

```
PATH=$HOME/.local/bin:$PATH cargo build --release     # -> target/release/tunsink-rs
PATH=$HOME/.local/bin:$PATH cargo test --release
```

librdkafka is compiled from source (about a minute the first time) and needs
`cmake` on `PATH` (on this machine it lives only in `$HOME/.local/bin`).
`rdkafka-sys` is pinned to 4.9.0: 4.10 (librdkafka 2.12.1) does not build
without curl.

## Flags

Go `flag` syntax: `-x v`, `-x=v`, `--x v`; booleans are bare or `-x=true|false`
(the `strconv.ParseBool` spellings). The set is the Go server's.

| flag | default | meaning |
|---|---|---|
| `-tun` | `tun0` | TUN interface (empty: discard) |
| `-addr` | `:9000` | TCP listen address |
| `-brokers` | empty | comma-separated Kafka brokers (empty: Kafka off) |
| `-topic` | `tun-packets` | Kafka topic |
| `-kafka-drop` | off | drop (count `kafka_err`) when the producer queue is full instead of blocking |
| `-aes` | off | frame body is `nonce(12) \|\| AES-256-GCM ciphertext \|\| tag(16)`; decrypt on the loop (ring), count `decrypt_err` and skip on failure |
| `-key` | `000102…1f` | AES-256 key, 64 hex characters |
| `-loops` | `1` | event loops |
| `-pin` | empty | comma-separated CPUs for the loops |
| `-mode` | `channel` | `channel` or `direct` (below) |
| `-sinks` | `1` | sink threads draining the channel |
| `-watchdog` | `10ms` | slow-callback watchdog threshold (`0`: off) |
| `-stats` | `1s` | stats print interval (`0`: off) |
| `-spin` | `-1` | loop busy-poll iterations before blocking in epoll (`-1`: library default) |

Leniencies (none matter to the harness): `-stats`/`-watchdog` also take a bare
number of seconds (Go rejects `-stats 1`), `-sinks 0` is raised to 1 (Go would
start no sink and stall), `-pin` skips empty list items.

SIGTERM/SIGINT stop the engine (graceful close of every connection), then the
drainer, the sinks, flush Kafka (15 s budget) and print one
`total frames=... bytes=... tun_err=... kafka_ok=... kafka_err=... pauses=...`
line, always the last line.

## Design, and what differs from Go

Identical to the Go server:

- Modes. `channel`: each frame is copied once (`to_vec`) on the loop and sent
  to the sink threads, which write the TUN and produce to Kafka; nothing but
  framing runs on the loop. `direct`: the loop writes the TUN from the shared
  read buffer (no copy, non-blocking fd, the kernel drops on a full queue);
  only Kafka goes through the sinks, and with Kafka off nothing is copied.
- One bounded sink channel of 4096 frames shared by all loops and sinks.
- Backpressure: a full channel never drops a frame. The connection is marked
  `paused`, reading is paused, and the frame plus any frames still being parsed
  from the current read chunk go to the connection's `pending` list (mutex
  shared with the drainer). The drainer pushes `pending` with a blocking send,
  then hands control back to the loop, which resumes reading only if `pending`
  is still empty and otherwise queues another drain. TCP flow control carries
  the stall to the sender. Per connection, frames reach the sinks in order.

Different:

- Drainer. Go starts a goroutine per pause, each blocked in a channel send
  (they queue FIFO on the channel). Rust runs one dedicated drainer thread
  serving `DrainReq`s FIFO with a plain blocking `Sender::send`, and resumes
  through `LoopHandle::call_from_thread`. If the loop has already stopped
  (`Err(LoopStopped)`) the drainer flushes whatever is left in `pending`
  itself. `main` joins the drainer after the engine and before closing the
  sinks, so frames read during the shutdown linger (graceful close un-pauses
  reading to find the peer's FIN) are delivered, not dropped.
- Kafka client: librdkafka `ThreadedProducer` (acks=1, no idempotence, linger
  5 ms, snappy requested, 1M-message / 2 GB local queue) instead of franz-go.
  franz-go's blocking `Produce` is emulated by retrying `QueueFull` every
  100 us; `-kafka-drop` (franz-go `TryProduce`) counts `kafka_err` and drops.
  The fake broker does not support snappy, so librdkafka logs one
  `COMPRESSION ... not compressing batch` line and sends uncompressed.
- Log timestamps are `HH:MM:SS` (UTC), not Go's `YYYY/MM/DD HH:MM:SS`.

## Tests

`cargo test --release` runs the real engine over loopback TCP with the same
`Client` and `drainer`, a 2-slot sink channel and no TUN/Kafka:

- `slow_consumer_gets_every_frame_once_in_order_and_conn_resumes`: 20k frames
  to a consumer sleeping 20 us per frame: every frame exactly once and in
  order, `pauses > 0`, the connection ends unpaused
  (`Transport::is_reading_paused() == false`) and frames sent afterwards still
  arrive.
- `fast_consumer_races_the_loop_without_loss_or_reordering`: same with 500k
  frames and an unthrottled consumer, so the drainer finishes while the loop
  is still appending; this is the test that fails if the resume callback
  resumes without re-checking `pending`.
- `backlog_read_during_shutdown_linger_reaches_a_late_consumer`: the consumer
  is stalled, the client writes 240 KB and closes, the engine is stopped and
  joined (the linger reads the rest into `pending`), then the consumer is
  released and the drainer joined: all frames arrive in order.
- AES (`-aes`): `aes_frames_reach_the_sink_as_plaintext` (5,000 sealed frames
  arrive decrypted and in order), `aes_bad_frames_count_decrypt_err_and_keep_the_connection`
  (wrong-key and short frames count `decrypt_err`, nothing reaches the sink,
  the connection stays open and a good frame after them arrives), and
  `aes_direct_mode_writes_plaintext_to_the_tun` (the scratch-buffer path with a
  Unix datagram socket standing in for the TUN fd); `aes.rs` has unit tests for
  round trip, wrong key, short body and key parsing. `src/bin/aesbench.rs`
  measures the standalone per-frame decrypt cost (69–70 ns for 512 bytes on
  the i9-14900KF; Go's `crypto/aes` GCM: 86–88 ns).

## Verified (2026-11-07; AES cells 2026-11-08)

AES-256-GCM (`-aes`, ring 0.17.14): in the 2026-11-08 `bench.sh quick` run every
AES cell passed `check` with `decrypt_err=0`, and the wrong-key negative cell
(`LOADGEN_KEY=ff…`) counted every one of its 20.8M frames as a decrypt error
with `tun_rx=0`. Decryption costs 17% at one core in `direct` mode without
Kafka (2.31 → 1.91 M frames/s), the same relative cost as the Go server
(1.91 → 1.57), and 3–13% elsewhere; results table in
`examples/tunsink/README.md`.


`unshare -rn`, server under `taskset -c 2` with `-loops 1 -pin 2 -sinks 1
-stats 1s -watchdog 0 -spin 0`, loadgen under `taskset -c 16-31` with
`-duration 5s`, fakekafka on CPUs 10,12,14; TUN RX read 0.5 s after the
loadgen exits, then SIGTERM, wait, read again. One run per cell.

| mode | kafka | conns | size | sent | loadgen frames/s | TUN RX before / after SIGTERM | total frames | kafka_ok | pauses | exit after SIGTERM |
|---|---|---|---|---|---|---|---|---|---|---|
| channel | off | 64 | 512 | 7,328,384 | 1,465,740 | 7,330,379 / 7,330,379 | 7,330,379 | 0 | 34,793 | 0.03 s |
| channel | off | 1000 | 512 | 8,388,032 | 1,703,234 | 6,119,438 / 8,422,875 | 8,422,875 | 0 | 32,324 | 5.04 s |
| direct | off | 1000 | 512 | 11,042,880 | 2,236,239 | 9,112,758 / 11,079,526 | 11,079,526 | 0 | 0 | 4.63 s |
| direct | on | 64 | 512 | 3,969,600 | 793,981 | 3,858,101 / 3,971,661 | 3,971,661 | 3,971,661 | 15,969 | 0.23 s |
| channel | on | 1000 | 512 | 5,876,032 | 1,192,876 | 3,234,774 / 5,915,077 | 5,915,077 | 5,915,077 | 22,785 | 5.15 s |
| direct | on | 1000 | 512 | 5,823,424 | 1,178,777 | 3,434,634 / 5,866,905 | 5,866,905 | 5,866,905 | 23,979 | 4.92 s |
| direct | on | 64 | 1400 | 2,196,736 | 439,383 | 2,186,914 / 2,198,590 | 2,198,590 | 2,198,590 | 12,038 | 5.15 s |

Every cell: `total frames` == final TUN RX delta (== `kafka_ok` with Kafka
on), |sent - TUN RX| <= conns*128 (largest: 43,481 at 1000 conns, 2,061 at
64), `tun_err=0`, `kafka_err=0`, and the total line is the last line. Server
`stats frames/s` while loaded: channel/off/64 about 1.31-1.35 M; channel/off/1000
about 1.13-1.19 M; direct/off/1000 about 1.66-1.79 M; Kafka on about 0.70-0.74 M
(64 conns) and 0.59-0.72 M (1000 conns); 1400-byte frames about 0.35-0.40 M.
At 1000 conns the loadgen outruns one CPU, so 2-2.7 M frames are still in
kernel socket buffers at SIGTERM; the graceful close reads and delivers them
(the before/after gap).

Exit time: the ~5 s exits are twine's `shutdown_timeout` (5 s), not this crate,
and the Go server shows the same exit times (same cells: 5.02, 5.04, 4.88, 0.27, 5.08, 5.08,
5.09 s). The 1400-byte cell repeated: 0.37, 5.09, 5.08, 5.09 s. Trigger
confirmed (the read cap, below); mechanism from reading the code: when a
read event carries HANGUP and the 8-read cap (`max_reads_per_event`) is hit,
`handle_read` re-queues the connection (`rust/twine/src/conn.rs`
`queue_ready`, end of `handle_read`) and `handle_ready`
(`rust/twine/src/event_loop.rs`) calls `handle_read(id, false)`: the hangup
is lost, the last short read returns without reading the 0, and the edge never
fires again, so connections with more than 512 KB unread when their FIN
arrived sit until the deadline aborts them (by then nothing is left unread, so
no frames are lost). With the cap lifted (`max_reads_per_event(1 << 20)`, a
throwaway build) the 1400-byte cell exits in 0.10-0.49 s and channel/off/1000
in 2.1 s, which is the time it takes to read and write the 2.3 M-frame backlog.
