# Implementation status against design.md Section 13 (Rust port)

Legend: **done** · **done (substitution)** — delivered differently from the letter
of the design, with the reason · **not done** — with the cause. Numbers come from
[BENCHMARKS.md](../BENCHMARKS.md); Rust-specific decisions are recorded in
design.md Section 14. Built 2026-11-07 as plan → implement (Sonnet subagents, one
crate or module each, own `CARGO_TARGET_DIR`) → independent read-only review →
fix cycles; every wave's review findings were applied before the next wave.

Size: ~35K lines of Rust (including tests); `cargo test --workspace` runs 398 tests
(0 failed, 4 ignored release-mode gates that also pass), plus 4 fuzz targets.

## Phase 0: Scaffold — done

| Item | Status |
| --- | --- |
| Workspace, Section 5 layout, `errors.rs`, `options.rs`, counting allocator | done (`twine-alloccount`: a thread-local counting `#[global_allocator]` for the zero-allocation gates) |
| `docs/INTERNALS.md`, `design.md`, `README.md` with the Section 3 table | done |
| CI | done as `ci.yml` + `scripts/ci-local.sh` (no remote exists; the replay runs fmt, clippy `-D warnings`, tests, rustdoc `-D warnings`, the `uring` feature check, the darwin/freebsd cross-checks of the ring-free crates, the ignored release gates and the fuzz campaign) |

## Phase 1: Single loop, epoll, echo — done

| Item | Status |
| --- | --- |
| `twine-poll` epoll + kqueue, eventfd/`EVFILT_USER` wake | done; kqueue compile-checked for `x86_64-apple-darwin`, not executed (no BSD host) |
| `twine-sock` | done (accept4, `send(MSG_NOSIGNAL)`/`sendmsg` so a library user need not ignore SIGPIPE, recvmmsg/sendmmsg batches) |
| loop iteration order, shared read buffer, fd-indexed slots, direct-write-first | done (`event_loop.rs`, `conn.rs`, `transport.rs`) |
| `protocol.rs`, `engine.rs` run/stop/listen_tcp | done |
| `examples/echo`, `bench/echo_twine`, `bench/echo_threads` | done |
| **Accept:** echo test passes; read path 0 allocs | pass (`echo_one_loop`…; `alloc_gates::read_dispatch` 0 over 500 samples) |

## Phase 2: Multi-loop, write ring, slabs — done

| Item | Status |
| --- | --- |
| `twine-slab`, `twine-ring` (iovec export) | done; property-tested against a reference queue |
| ring-backed writes, WRITABLE arming, end-of-iteration writev, watermarks | done |
| `loops(n)` with SO_REUSEPORT per loop; round-robin hand-off when off | done |
| `Producer`, pause/resume reading | done (substitution): producer notifications are delivered from the post-callback queue, never re-entrantly inside `write`; `write_blocked()` is the synchronous check (design Section 14) |
| partial-write and backpressure tests | done (8 MiB and 100 MiB byte-exact through 4 KiB socket buffers; pause/resume exactly once per crossing, in order) |
| **Accept:** write paths 0 allocs; scaling ≥ 0.8× 1→8 loops; 100 MiB byte-exact | 0 allocs: pass. Scaling: 13.9× for 8 loops (superlinear; the one-loop cell is kernel-bound, see BENCHMARKS.md). 100 MiB: pass. |

## Phase 3: Timers, cross-thread, Deferred — done

| Item | Status |
| --- | --- |
| `twine-wheel` (index-linked), `call_later`, token timers, idle timeouts | done; connection timers are `u64` tokens delivered to `Protocol::timer_fired` and removed at close |
| `twine-mpsc` + coalesced wakeup, `call_from_thread`, `in_loop` | done (Vyukov queue verified under miri; `in_loop` via a thread-local) |
| `Deferred<T>`, `Resolver`/`LocalResolver`, `Awaitable<T>` | done (substitution): JS-style `then → Deferred<U>`, callbacks always asynchronous, `Awaitable` implements `std::future::Future`; `TWINE_DEBUG=1` creation backtraces |
| worker pool, `defer_to_thread` | done (per engine, lazily started, shut down before the loops) |
| `connect_tcp` on WRITABLE, dial timeout | done |
| `examples/client_reconnect` | done |
| **Accept:** timers ≤ 50 ns insert+cancel, 0 allocs; wakeups/op ≤ 0.1 at 16 producers; `wait` on loop panics | 14.9 ns at 1M active: pass. 0 allocs: pass. Wakeups per call 0.14 unpinned / 0.016 pinned: pass on the pinned configuration the Go gate used, 0.14 uncontrolled. `AwaitOnLoop` panic: pass. |

## Phase 4: Protocol library and observability — done

| Item | Status |
| --- | --- |
| `twine-proto`: `Lines`, `Netstrings`, `LengthPrefixed`, `ReconnectingClient`, `Broadcast`, `Amp` | done; wrappers own the buffer and a handler trait (the Go embed-and-callback pattern does not borrow-check); pure parsers exported for fuzzing; AMP wire-compatible with Twisted boxes |
| UDP with recvmmsg/sendmmsg, recvfrom fallback | done |
| watchdog, metrics, prometheus text | done (substitution): the watchdog reports loop id, protocol `type_name` and elapsed time without a cross-thread stack dump |
| graceful shutdown (rule 7), panic recovery (rule 8), `blocking` adapter | done; `blocking` hands each connection to a thread as an `io::Read + io::Write` stream (design Section 14) |
| fuzz targets, `examples/chat`, `examples/udp_echo` | done (`fuzz/`, 4 libFuzzer targets with the Go corpora as seeds) |
| **Accept:** fuzzers 10 min clean; shutdown calls `connection_lost` once per conn; watchdog detects 5 ms | Fuzz: **10 minutes per target, clean** (`scripts/fuzz.sh 600`: line 2.50M execs, netstring 8.33M, lengthprefix 8.23M, http_parse 4.12M; 0 crashes; the one inconsistency fuzzing found earlier — a chunked trailer rejected with 431 only when the final CRLF was split across reads — was fixed and is covered by a split-equivalence test). Shutdown once-per-conn: pass (100 connections, 1 MiB pending each). Watchdog: pass. |

## Phase 5: HTTP/1.1 and TLS — done

| Item | Status |
| --- | --- |
| `twine-http` parser, `RequestCtx`, response writer, keep-alive, pipelining, limits, cached Date | done; 0 allocations for keep-alive GET, pipelined, POST, chunked and split requests; chunked bodies decoded incrementally so the per-connection buffer is bounded by the limits; the `net/http` adapter has no Rust counterpart |
| `twine-tls`: rustls wrapper, `listen`/`connect` | done (substitution): rustls is sans-IO so the handshake runs on the loop (no handshake thread, no memory BIO); writes larger than rustls' 64 KiB send buffer are looped; a FIN without close_notify is reported as `UnexpectedEof`, not a clean close |
| `bench/http_twine`, `bench/http_tokio`, `examples/http_hello` | done |
| **Accept:** HTTP 0 allocs; ≥ 5× baseline at c=256; TLS echo vs `openssl s_client` | 0 allocs: pass. Baseline: the Rust baseline is a hand-written tokio HTTP server, not `net/http`; twine is 1.17× tokio unpipelined and 1.13× / 1.32× Go twine (unpipelined / pipelined). **Not met against tokio** — the 5× gate was written for `net/http`. openssl TLS 1.2 and 1.3: pass. |

## Phase 6: Platform extras and release — done

| Item | Status |
| --- | --- |
| io_uring readiness backend (`--features uring`) | done (substitution): readiness-only on multishot `POLL_ADD` with an epoll fallback, same `Poller` API; passes the full poll suite and the whole core suite (113 tests) |
| kTLS offload with probe and fallback | done: TLS 1.3 AES-128/256-GCM engage on this kernel. Default is TX offload only (inbound decrypted in user space so a peer's close_notify stays a clean close); `TlsOptions::kernel_rx(true)` offloads RX too, where a peer alert or KeyUpdate surfaces as EIO. rustls' `dangerous_extract_secrets` consumes the connection, so TX-only mode decrypts inbound records with a small TLS 1.3 record layer on `ring` (design Section 14). ChaCha20 falls back on this kernel (no kernel chacha). |
| `Amp`; `SO_BUSY_POLL` and `TCP_DEFER_ACCEPT` options | done |
| 1M-connection soak | done: 1,000,000 idle connections on 8 loops at **499 MB RSS (470 B/conn)**, flat over the hold, 2000/2000 sampled connections still echo (`bench/scripts/idle.sh`, inside `unshare -Urn` because the root namespace's conntrack cap is 262,144). The first soak (980 MB on 2 loops, 4.7 KB/conn on 8 loops) exposed a dense per-loop slot table; it is now sparse (16 B per empty fd, boxed live slots on a per-loop free list), see design Section 14. |
| API freeze, rustdoc on every public item | done: `docs/API.md` (generated by `scripts/apidump.py`); `#![deny(missing_docs)]` in every library crate and rustdoc `-D warnings` clean |
| **Accept:** every Section 2 target met or documented; lints clean | BENCHMARKS.md records each target: 5 met (HTTP per core with pipelining, p99 at 80% load, 0 allocs, timers), echo per core not met (loopback-bound, Go twine measures the same), 1M idle: 499 MB (met). clippy `-D warnings`, rustdoc `-D warnings`, fmt: clean. |

## Verification summary (this host)

- `cargo test --workspace`: 398 passed, 0 failed, 4 ignored; the ignored release gates
  (100 MiB partial write, 10K soak, 64×1M MPSC stress, 1M-timer timing) all pass.
- The core suite also passes unchanged on the io_uring backend.
- `cargo clippy --workspace --all-targets -- -D warnings`, `RUSTDOCFLAGS=-D warnings
  cargo doc`, `cargo fmt --check`: clean. `cargo check --target x86_64-apple-darwin`
  for `twine`, `twine-proto`, `twine-http`: clean (`twine-tls` needs a macOS C
  toolchain for `ring`).
- Fuzzing: `scripts/fuzz.sh 600` ran every target for 10 minutes with no crashes (23M executions in total).
- Benchmarks: `bench/results/ddf76dc.txt`; Go-vs-Rust head to head in BENCHMARKS.md.
- `unsafe`: confined to `twine-poll`, `twine-sock`, `twine-mpsc` slots, `sched_setaffinity`,
  the kTLS FFI in `twine-tls/src/ktls.rs`, and the libc epoll in the load generator; every
  block carries a `SAFETY:` comment. The core dispatch, transport, slot table, wheel,
  ring, slab, Deferred, proto and http code contain none.

## Gates not met on this host

- Echo ≥ 2M msgs/s per core: 172K at one loop, 309K per core at eight (load generator saturated). The loop thread
  spends 94% of its time in the kernel on the VM loopback path; Go twine measures
  180K / 164K on the identical cells with the same load generator, so nothing in the
  Rust code is the limit.
- HTTP ≥ 5× baseline: 1.18× the tokio baseline unpipelined, 0.89× pipelined at c=256.
  Against the design's original baseline class (`net/http` at ≤ 190K req/s per core)
  the ratio would be about 5×–14×; against Go twine it is 1.08×–1.18×.

## Fixes after the Section 13 close-out

- 2026-11-07, found by the `examples/tunsink` Go-vs-Rust comparison (Rust server
  in `rust/examples/tunsink`, outside the workspace because it builds
  librdkafka): a hangup reported in an event whose reads hit `max_reads_per_event`
  was lost when the conn was re-queued, so the final short read returned
  without seeing EOF and the connection stayed open until the shutdown timeout.
  `flags::HANGUP` now sticks to the conn (`tests/io.rs::hangup_survives_read_cap`).
  `Transport::is_reading_paused()` was added for the example's tests. Same bug
  and fix in the Go library.
- The comparison itself: Rust twine 1.11×–1.45× Go twine on TUN-only cells,
  parity ±10% with Kafka on one core, 0.61× with Kafka on four cores (librdkafka's
  single compression thread per client; not a twine difference). Details and the
  harness in `examples/tunsink/README.md`.
- 2026-11-08, AES-256-GCM in the comparison (`-aes` on both servers; Rust uses
  `ring` 0.17 `open_in_place` after copying the ciphertext into the destination
  buffer, one allocation per frame as before). Rust loses 17% at one core in
  direct TUN-only mode (2.31 → 1.91 M frames/s; ring's 70 ns/frame × 2.31 M/s),
  the Go server loses the same 17% (Go GCM 87 ns/frame), so Rust/Go stays
  1.21×; with Kafka at one core the cost is 3–11%. The 4-core Kafka cell is
  still the librdkafka single-thread limit (0.54–0.70× Go, noisy; AES adds
  nothing measurable there: the table's 0.71 M/s rerun as 1.09 and 1.08).
  Table and method in `examples/tunsink/README.md`.
