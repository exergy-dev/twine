# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Until
v1.0.0, minor releases may break the API; every break is listed here.

## [Unreleased]

### Changed (breaking)

- Removed the engine options `LockOSThread`, `MaxAcceptsPerEvent`,
  `TaskQueueSize` and `UDPBatch`. Loops always lock their OS thread; the
  others had no effect worth tuning.
- `BusyPoll` is now a `ListenOption` (it sets a socket option per listener)
  instead of an engine `Option`.
- `Engine.PrometheusHandler` moved to the new `metrics` package, so binaries
  that do not export metrics no longer link `net/http`.
- `LoopMetrics` is unexported; read metrics through `Engine.Metrics` and
  `Loop.Metrics` (`MetricsSnapshot`).
- `Transport` embeds `Consumer` instead of repeating its methods.
- A `Factory` that returns a nil `Protocol` now refuses the connection, and
  the `Dial`/`ConnectTCP` Future is rejected with `ErrNilProtocol`. Before,
  the connection stayed open and ignored all data. The same applies under
  `tls.Server` and `tls.Client`.
- proto: `LineReceiver.SendLine`, `NetstringReceiver.SendString` and
  `LengthPrefixed.SendFrame` drop the transport argument and always use the
  stored transport.
- proto: `Option` is renamed `ReconnectOption`; the `OnStateChange`
  callback is `func(connected bool, err error)` and is called once more
  when the client gives up, with an error wrapping the new `ErrGaveUp`.
- proto: `LengthPrefixed.ConnectionMade` panics on a `PrefixLen` other
  than 0, 1, 2 or 4 instead of silently using 4.
- tls: `KTLSSupported` is removed (it only reported `GOOS == "linux"`).
  `Transport.KernelOffload()` reports whether TX and RX are offloaded
  (`KernelOffloaded` reported TX only).
- tls: `Server` panics when the config has no certificate source
  (`Certificates`, `GetCertificate` or `GetConfigForClient`), and `Listen`
  returns an error, instead of failing every handshake silently.
- tls: `HandshakeTimeout(d)` with `d <= 0` now means the default (10 s);
  it no longer disables the timeout. Handshake failures reach the
  `OnHandshakeError` hook and the `Connect` Future as a `*HandshakeError`
  carrying the peer address, and are logged at Warn when no hook is set.
- http: `IdleTimeout` zero now means 90 s (was disabled); a negative value
  disables it. The new `ReadTimeout` (default 60 s) bounds an incomplete
  request head or body. Responses larger than the new `MaxResponseBytes`
  (default 16 MiB) become a 500 instead of a truncated body.
- http: a status below 200 is sent as 500; response headers with an
  invalid name are dropped; HEAD responses with an empty body carry no
  Content-Length; canned error responses carry Date and Server.
- http: `RequestCtx.SetUserValue` and `UserValue` are removed; use
  `Transport.SetContext` for per-connection state.
- `Any` with no inputs rejects with `ErrNoFutures` (it rejected with
  `ErrLoopStopped`). A second `Engine.Run`, including one after a completed
  run, returns `ErrRunTwice` (a run after a completed run returned nil).
- Negative `Watchdog`, `ShutdownTimeout`, `CloseLinger` and
  `SpinIterations` values are clamped or mean the default, and
  `DatagramsSent` counts only datagrams the kernel accepted.

### Deprecated

- tls: `Transport.KernelOffloaded` (TX only); use `KernelOffload`.

### Added

- `ErrNoFutures`, `ErrRunTwice` and `ErrNilProtocol` sentinels.
- `Engine.Logger`, `Loop.ReportPanic`, and `poll` backend logging when a
  `twine_uring` build falls back to epoll.
- `NewPanicError`, which wraps a recovered panic value with the current
  stack the way the core does (the `tls` package uses it instead of
  hand-built `PanicError` values).
- proto: `ErrLineTooLong` and `ErrFrameTooLarge` are also available under
  `proto`; `FuzzAMP` and `FuzzLineReceiverRaw` fuzz targets; benchmarks.
- tls: `HandshakeError`, `Transport.KernelOffload`, allocation gates and
  record benchmarks, and a layout test that fails when a Go release
  changes the `crypto/tls` fields kTLS reads.
- http: `ReadTimeout`, `MaxResponseBytes`, `DefaultIdleTimeout`,
  `DefaultReadTimeout`, `DefaultMaxResponseBytes`; a 408 response.
- `twine_debug` assertions inside `internal/`: slab double-Put, re-entrant
  wheel Advance and ring invariants.
- `metrics` package with the Prometheus text-format handler.
- Runnable examples on pkg.go.dev for the root, `proto`, `http` and `tls`
  packages; package documentation for the root package.
- `CONTRIBUTING.md`, `SECURITY.md` and this changelog.

### Fixed

- `twine.Blocking` under `tls.Server`/`tls.Listen` panicked on every
  connection and the panic was swallowed; the client only saw EOF.
- `proto.AMP.Call` and `CallNoAnswer` read the caller's `Args` map on the
  loop after returning (a data race); the arguments are now encoded on the
  caller's goroutine.
- `Engine.Dial` and `Loop.ConnectTCP` with a host name resolved it on the
  loop thread, stalling every connection on that loop; names are now
  resolved on the worker pool.
- `tls.Client` used with `Loop.ConnectTCP` resolved the Future with the
  raw socket before the handshake; the documentation now says so and
  points at `tls.Connect`.
- Zero or negative size and count options (`ReadBufferSize(0)`,
  `MaxReadsPerEvent(0)`, ...) broke the engine instead of meaning the
  default.
- A peer could make `LengthPrefixed` or `NetstringReceiver` pre-allocate
  up to 1 MiB per connection with a 5-byte header.
- The default watchdog sampled every millisecond and burnt about 7% of a
  core on an idle engine; it now backs off to 50 ms while idle.
- `ReconnectingClient.GaveUp` stayed false after a clean `Engine.Stop`.
- A `proto.Broadcast` zero value panicked on `Add`.
- `DatagramReceived`'s `from` address is reused between calls; this is now
  documented, and debug builds clear it after the callback.
- Under kTLS TX offload a peer `KeyUpdate` was silently ignored instead of
  aborting the connection as documented.
- `listener.Close` before `Engine.Run` raced with the loop start.
- `Loop.Metrics().LoopID` was always 0.
- `internal/poll` did not compile on 32-bit NetBSD and OpenBSD.
- macOS: `CallFromThread` allocated, `Engine.Dial` skipped half the loops
  when `ReusePort` was off, and three tests assumed Linux limits.

### Changed

- `Loop.CallFromThread` no longer checks whether it is called on the loop
  (a thread-id read per call); every call goes through the task queue, and
  no wakeup is written while the loop is inside a callback. From a callback,
  `CallLater(0, fn)` is the cheaper way to defer work.
- kTLS offload covers TLS 1.3 AES-GCM only; ChaCha20-Poly1305 connections
  stay on the memory BIO.
- `Engine.ListenUDP` returns an error naming the option when given a
  TCP-only `ListenOption` (`Backlog`, `DeferAccept`, `BusyPoll`,
  `TCPNoDelay`) instead of silently ignoring it.
- `docs/API.md` now lists interface methods, struct fields, constants and
  variables, and CI fails when it is stale.
- The design document and build log moved to `docs/history/`;
  `BENCHMARKS.md` keeps only current results.
- CI tests Go 1.24, oldstable and stable on Linux and macOS, runs the
  io_uring backend and the kTLS tests, lints darwin and the nested modules,
  cross-builds every BSD, and runs govulncheck nightly.

## [0.1.0] - 2026-11-09

First public release. The API is a snapshot and may change before v1.0.0.

### Added

- Core reactor: `Engine` with one event loop per core (epoll on Linux,
  kqueue on macOS and the BSDs), `Protocol` / `Transport` / `Factory`,
  `ListenTCP` with one `SO_REUSEPORT` listener per loop on Linux,
  `ListenUDP` with `recvmmsg`/`sendmmsg` batching on Linux, outbound
  `Loop.ConnectTCP` and `Engine.Dial`.
- Timers on a hierarchical timing wheel (`Loop.CallLater`, `Timer`),
  `CallFromThread` over a lock-free MPSC queue with coalesced wake-ups,
  `DeferToThread` on a bounded worker pool, and a typed `Future[T]` with
  `Then`, `Catch`, `Finally`, `Map`, `All`, `Any` and `Await`.
- Push and pull producers with write high/low water marks, idle timeouts,
  half-close with `CloseLinger`, graceful `Engine.Stop` with
  `ShutdownTimeout`, `OnShutdown` hooks.
- `Blocking` adapter that runs a Protocol on its own goroutine, for code
  that blocks.
- Per-loop metrics, a slow-callback watchdog, panic recovery into
  `PanicError`, a Prometheus handler, and the `twine_debug` build tag (also
  `TWINE_DEBUG=1`) for loop-affinity assertions, read-buffer poisoning and
  Future creation stacks.
- `proto`: `LineReceiver`, `NetstringReceiver`, `LengthPrefixed`, `AMP`,
  `ReconnectingClient` and `Broadcast`, with fuzz targets for the framers.
- `http`: an HTTP/1.1 server with no per-request allocation in the steady
  state (keep-alive and pipelined requests, Content-Length bodies), and
  `Adapt` to run a `net/http` handler on it (a migration bridge that
  allocates per request).
- `tls`: memory-BIO TLS over `crypto/tls` (`Server`, `Client`, `Listen`,
  `Connect`) with handshakes off the loop, and opt-in kTLS data-path offload
  for TLS 1.3 AES-GCM on Linux (`KernelOffload(true)`, experimental).
- `twine_uring` build tag: io_uring readiness backend on Linux with an epoll
  fallback (experimental).
- `bench` module with the comparison harness (goroutine-per-connection,
  gnet, net/http), the pinned server-bound runner and the 1M-connection
  soak; examples `echo`, `chat`, `http_hello`, `udp_echo` and
  `client_reconnect`.

### Platforms

Linux (epoll) is the primary platform and runs in CI; macOS (kqueue) runs in
CI from the first fix-up after this tag. FreeBSD, NetBSD, OpenBSD and
DragonFly only compile. Windows is not supported.

### Known issues

Found by the post-release audit; fixes are tracked for the next release.

- `twine.Blocking` under `tls.Server` or `tls.Listen` panics inside the TLS
  layer and the connection is dropped without a log line.
- `Dial` and `ConnectTCP` with a host name resolve DNS on the loop thread,
  stalling every connection on that loop; pass IP literals.
- `ConnectTCP(addr, tls.Client(...))` resolves its Future with the raw
  socket before the handshake; use `tls.Connect` to get the TLS transport.
- The HTTP server has no default idle or read timeout, and a response body
  larger than `WriteBufferLimit` aborts the connection mid-body.
- `AMP.Call` reads the caller's `Args` map after returning (data race if
  the caller reuses it).
- Zero-valued options such as `ReadBufferSize(0)` break the engine instead
  of meaning "default".
- `ReconnectingClient.GaveUp` stays false after a clean `Engine.Stop`.
- 32-bit NetBSD and OpenBSD do not compile.

[Unreleased]: https://github.com/exergy-dev/twine/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/exergy-dev/twine/releases/tag/v0.1.0
