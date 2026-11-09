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
- A `Factory` that returns a nil `Protocol` now aborts the connection, and
  the `Dial`/`ConnectTCP` Future is rejected with a sentinel error. Before,
  the connection stayed open and ignored all data.
- proto: `LineReceiver.SendLine`, `NetstringReceiver.SendString` and
  `LengthPrefixed.SendFrame` drop the transport argument and always use the
  stored transport.
- proto: `Option` is renamed `ReconnectOption`, and the `OnStateChange`
  callback receives the error that caused the state change.
- tls: `KTLSSupported` is removed (it only reported `GOOS == "linux"`).
  `Transport.KernelOffload()` reports whether TX and RX are offloaded
  (`KernelOffloaded` reported TX only).
- tls: `Server` panics when the config has no certificate source
  (`Certificates`, `GetCertificate` or `GetConfigForClient`), and `Listen`
  returns an error, instead of failing every handshake silently.
- http: `RequestCtx.SetUserValue` and `UserValue` are removed; use
  `Transport.SetContext` for per-connection state.

### Added

- `ErrNoFutures` (from `Any` with no inputs) and `ErrRunTwice` (from a
  second `Engine.Run`).
- `metrics` package with the Prometheus text-format handler.
- Runnable examples on pkg.go.dev for the root, `proto`, `http` and `tls`
  packages; package documentation for the root package.
- `CONTRIBUTING.md`, `SECURITY.md` and this changelog.

### Changed

- kTLS offload covers TLS 1.3 AES-GCM only; ChaCha20-Poly1305 connections
  stay on the memory BIO.
- `docs/API.md` now lists interface methods, struct fields, constants and
  variables, and CI fails when it is stale.
- The design document and build log moved to `docs/history/`;
  `BENCHMARKS.md` keeps only current results.
- CI tests Go 1.24, oldstable and stable on Linux and macOS, runs the
  io_uring backend and the kTLS tests, lints darwin and the nested modules,
  cross-builds every BSD, and runs govulncheck nightly.
- `examples/tunsink`: `github.com/klauspost/compress` v1.20.2
  (GO-2026-6865).

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
  soak; examples `echo`, `chat`, `http_hello`, `udp_echo`,
  `client_reconnect` and `tunsink`.

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
