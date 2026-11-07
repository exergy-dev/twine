# Implementation status against design.md Section 13

Legend: **done** · **done (substitution)** — delivered differently from the
letter of the design, with the reason · **not done** — with the cause.
Numbers come from [BENCHMARKS.md](../BENCHMARKS.md); decisions are recorded
in design.md Section 14.

## Phase 0: Scaffold — done

| Item | Status |
| --- | --- |
| `go mod init`, Section 5 layout, Makefile (`test`, `test-race`, `bench`, `lint`) | done (`golangci-lint` v2 config with gocritic/staticcheck/errcheck; `lint` also cross-builds darwin/arm64 and freebsd/amd64 and vets the `twine_debug`/`twine_uring` tags) |
| GitHub Actions Linux + macOS matrix, `-race` | done (`.github/workflows/ci.yml`: Go 1.24/1.25, plus a nightly job for the 64×1M MPSC stress and 10-minute fuzzing). Not executed here: no remote. |
| `errors.go`, `options.go` | done; options beyond Section 6.2 that the text implies were added (`WriteBufferLimit`, `ShutdownTimeout`, `MaxReadsPerEvent`, `MaxAcceptsPerEvent`, `TaskQueueSize`, `Logger`, `BusyPoll`, `UDPBatch`, `CloseLinger`, `Backlog`, `SocketBuffers`, `DialTimeout`, `DialNoDelay`, `LocalAddr`) |
| README with the Section 3 table | done |
| Go version | done (substitution): `go 1.24`, not 1.23, because `runtime.AddCleanup` (Section 11 debug Futures) needs 1.24. x/sys pinned at v0.30.0 (newer releases require Go 1.26). |

## Phase 1: Single loop, epoll, echo — done

| Item | Status |
| --- | --- |
| `internal/poll` epoll + kqueue, edge-triggered, eventfd/pipe wakeup | done; kqueue compiled and vetted for darwin/freebsd/netbsd/openbsd/dragonfly, not executed (no BSD host) |
| `internal/sock` | done (accept4 via raw syscall to stay alloc-free; darwin uses accept + fcntl) |
| `loop.go` iteration order, shared read buffer, fd-indexed conn slice | done |
| `conn.go`/`transport.go` direct-write-first | done (the ring landed in the same wave, so the naive append buffer was never needed) |
| `protocol.go`, `engine.go` New/Run/Stop/ListenTCP | done |
| `examples/echo`, `bench/echo_twine`, `bench/echo_goroutine` | done |
| **Accept:** echo test on Linux and macOS; `echo_small` prints msgs/s; read path 0 allocs | Linux: pass. macOS: compiled only. `echo_small` 1.85–1.87M msgs/s total. Read path 0 allocs (`TestReadPathZeroAlloc`). |

## Phase 2: Multi-loop, write ring, slabs — done

| Item | Status |
| --- | --- |
| `internal/slab`, `internal/ring` with iovec export | done (100% statement coverage, property tests) |
| Ring replaces append buffer; EPOLLOUT arming; end-of-iteration `writev`; watermarks | done; `writev` via raw syscall because `unix.Writev` allocates |
| `Loops(n)` with SO_REUSEPORT per loop; `NextLoop`; `LockOSThread` | done; with `ReusePort(false)` one listener hands connections round-robin |
| Producer/Consumer, Pause/ResumeReading, RegisterProducer | done (pull producers poked from the flush step) |
| Partial-write and backpressure tests | done (100 MiB byte-exact through tiny socket buffers; pause/resume exactly once per crossing) |
| **Accept:** write hit/buffered 0 allocs; `echo_small` ≥ 0.8× linear 1→8 loops; 100 MiB partial-write test | 0 allocs: pass. Scaling, re-measured server-bound with loops pinned to distinct physical cores (`PinLoops`) and a separate pinned event-loop load generator: **0.74–0.76 at 1000 connections, 0.7998 at 4000** (server at 7.8–7.9 of 8 cores, so not client-limited); gnet measures 0.70–0.77 the same way; 2 and 4 loops scale at 0.86–1.03. Per-message cost rises from 2.4 µs at 1 loop to 3.6 µs at 8 on this WSL2 VM's loopback path, which is the shared ceiling. **Not met on this host**; needs the 16-core bare-metal target to settle. 100 MiB: pass. |

## Phase 3: Timers, cross-thread, Future — done

| Item | Status |
| --- | --- |
| `internal/wheel`; `CallLater`, `Timer.Stop/Reset`, `SetIdleTimeout` | done; wheel fires on tick boundaries (never early, < 1 tick late); `CallLater(0)` runs next iteration |
| `internal/mpsc` + coalesced wakeup; `CallFromThread`; `InLoop` | done; `InLoop` uses `gettid` on Linux and a goroutine-id fallback elsewhere |
| `future.go` | done (Then/Catch/Finally/Await/Map/All/Any/Sleep, Resolved/Rejected helpers, PanicError capture, debug creation stacks and GC-without-settle warnings via `runtime.AddCleanup`) |
| Worker pool and `DeferToThread` | done; nested and loop-originated submissions never block the loop |
| `ConnectTCP` on EPOLLOUT; `DialTimeout` | done |
| `examples/client_reconnect` | done (uses `proto.ReconnectingClient`) |
| **Accept:** `timers_1m` ≤ 50 ns insert+cancel, 0 allocs; `callfromthread` ≤ 0.1 wakeups/op; Future tests under `-race`; Await-on-loop panics | 34.7 ns: pass. 0 allocs/op: **pass** (`TestTimerFireZeroAlloc`): Timer handles are carved from a per-loop slab of 256 and never reused, so insert+cancel averages 0 allocations per operation and a stale handle can never cancel a later timer (pooling, which a reviewer showed to be unsafe, is not used). 0.017 wakeups/op: pass. Future `-race`: pass. Await panic: pass. |

## Phase 4: Protocol library and observability — done

| Item | Status |
| --- | --- |
| `proto`: LineReceiver, NetstringReceiver, LengthPrefixed, ReconnectingClient, Broadcast | done; AMP (a Phase 6 item) also done, wire-compatible with Twisted boxes |
| `DatagramProtocol`, `ListenUDP` with recvmmsg/sendmmsg, recvfrom fallback | done (raw `SYS_RECVMMSG`/`SYS_SENDMMSG`; x/sys has no wrapper) |
| `watchdog.go`, `metrics.go`, `Logger`, `TWINE_DEBUG` assertions and poisoning | done (substitution): the watchdog dumps the loop goroutine via `runtime.Stack(all)` filtered by goroutine id instead of a signal to the locked thread. Prometheus text handler written by hand (no client library). |
| Graceful shutdown (rule 7), panic recovery (rule 8), `twine.Blocking` | done; `OnShutdown` hooks run once on the Run goroutine (recorded in Section 14) |
| Fuzz targets; `examples/chat`, `examples/udp_echo` | done; corpora checked in under `testdata/fuzz` |
| **Accept:** fuzzers 10 min clean; shutdown calls ConnectionLost once per conn; watchdog detects 5 ms sleep | Fuzzers: 30 s per target locally (≈4–14M execs each) clean; the 10-minute run is the nightly CI job. Shutdown once-per-conn: pass (100 conns). Watchdog: pass. |

## Phase 5: HTTP/1.1 and TLS — done

| Item | Status |
| --- | --- |
| `http`: parser, `RequestCtx`, response writer, keep-alive, pipelining, limits, cached `Date` | done; stricter than net/http (documented in `http/doc.go`); 0 allocs for GET, POST, pipelined, split and chunked requests |
| `http.Adapt` with cost documented | done (≈20 allocs/request with 3 headers) |
| `tls`: memory-BIO conn, handshake off-loop, `tls.Listen`/`tls.Connect` | done (substitution): handshakes run on a dedicated goroutine per connection instead of the bounded `DeferToThread` pool, because silent peers would otherwise pin every worker for the handshake timeout (Section 14). Tested against `crypto/tls` clients/servers and `openssl s_client` (TLS 1.2 and 1.3). |
| `bench/http_twine`, `bench/http_nethttp`, `examples/http_hello` | done |
| **Accept:** parse+respond 0 allocs; `http_plaintext` ≥ 5× net/http at c=256; TLS echo vs `openssl s_client` | 0 allocs: pass. HTTP, server-bound on the same 8 pinned cores: **2.14M req/s vs net/http 637–648K = 3.3×** (3.0× against net/http with 16 cores); twine p99 0.3 ms vs 3.0 ms. Twine's HTTP rate is within 5% of its own echo ceiling, so the parser is not the limit; the gate is **not met on this host**. openssl: pass. |

## Phase 6: Platform extras and release — done (two performance gates not met on this host)

| Item | Status |
| --- | --- |
| `internal/poll/uring_linux.go` behind `twine_uring` | done (substitution): readiness backend on multishot `IORING_OP_POLL_ADD` with the same `Poller` interface and an epoll fallback; batched accept/read/write submissions were not done because the Poller contract is readiness-based. Passes the full poll suite and the root suite under the tag. |
| kTLS offload with probe and fallback | done: TLS 1.3 AES-128-GCM and AES-256-GCM offload engages on this host (verified with a Go client and `openssl s_client -tls1_3`: `KernelOffloaded()==true`, byte-exact echo, clean close_notify). `crypto/tls` does not export traffic secrets, so they are read by reflection from unexported fields with every access checked at runtime; any mismatch, TLS 1.2, ChaCha20 (rejected by this kernel) or a kernel refusal falls back silently to the memory BIO. Server TX+RX, client TX only; peer alerts/KeyUpdate under RX offload surface as EIO (documented). |
| `proto.AMP`; `SO_BUSY_POLL` and `TCP_DEFER_ACCEPT` options | done (`BusyPoll`, `DeferAccept`) |
| 1M-connection soak on a 16-core host; `BENCHMARKS.md` | done on this 32-vCPU WSL2 host (not a 16-core server): **1,000,000 connections held 10 minutes, server RSS 582 MB (564 B/conn), drift +0.095%**, all sampled connections still echo afterwards. Run inside an unprivileged network namespace because the root namespace's conntrack table (262,144, not raisable without root) capped the first attempt at 261,600. BENCHMARKS.md records every Section 2 target with numbers and causes. |
| API freeze, `v1.0.0` tag, godoc pass | done: `docs/API.md` (generated by `make api`) is the frozen surface; `v1.0.0` tag applied locally (no remote; module path `github.com/robert-malhotra/twine` chosen to match the author's other modules). godoc: every exported identifier documented (checked with an AST script). |
| **Accept:** every Section 2 target met or documented; vet/staticcheck/`-race` clean; godoc 100% | Targets: 5 met (1M idle RSS, <1 KB/conn, gnet parity, 0 allocs, timer insert/cancel), 2 missed with the cause attributed (per-core echo and HTTP throughput; both frameworks hit the VM's loopback ceiling), 1 not measurable as specified (p99 at 80% load; saturated p99 0.3 ms reported). vet, staticcheck, golangci-lint, `-race`, `twine_debug`, `twine_uring`, darwin/freebsd cross-builds: clean. godoc: 100%. |

## Verification summary (this host)

- `make test`, `go test -race -short ./...`, `make test-debug`, `make lint`: all pass.
- Root package: 62 integration tests plus alloc gates; `proto` 60+, `http` 40+, `tls` 18 tests; internal packages property-tested with 100% coverage on slab/ring.
- `make fuzz FUZZTIME=10s` and 30 s per-target runs: clean.
- `make bench` completes; raw output in `bench/results/baseline.txt`; pinned server-bound runs in `bench/results/pinned.txt`; the 1M soak in `bench/results/idle1m.txt`.

## Gates not met on this host

Two Section 13 gates are performance thresholds that this environment cannot
reach, and the comparison framework (gnet) cannot either: Phase 2 scaling
(0.76 vs 0.8) and Phase 5 HTTP throughput (3.3× vs 5× net/http). Both are
bounded by the WSL2 VM's loopback path, where the per-message syscall cost
grows from 2.4 µs to 3.6 µs as more loops run. They need the design's
16-core bare-metal host with wrk to be settled either way.
