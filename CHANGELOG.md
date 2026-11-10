# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Until
v1.0.0, minor releases may break the API; every break is listed here.

## [Unreleased]

v0.2 renames and removes API from v0.1 following an ergonomics review. The
list below maps every v0.1 name to its replacement, one per line; the
sections after it give the details and the non-breaking changes.

### Migrating from v0.1

- `import twhttp "github.com/exergy-dev/twine/http"` -> `import "github.com/exergy-dev/twine/twhttp"` (the package is named `twhttp`; the alias goes)
- `import twtls "github.com/exergy-dev/twine/tls"` -> `import "github.com/exergy-dev/twine/twtls"` (package `twtls`)
- `twine.Factory` interface (`BuildProtocol(peer) Protocol`) -> `type Factory func(peer net.Addr) Protocol`
- `twine.FactoryFunc(f)` -> `f` (any `func(net.Addr) twine.Protocol` is a Factory)
- `f.BuildProtocol(peer)` -> `f(peer)`
- `Transport.GetPeer()` -> `Transport.RemoteAddr()`
- `Transport.GetHost()` -> `Transport.LocalAddr()`
- `DatagramTransport.GetHost()` -> `DatagramTransport.LocalAddr()`
- `Transport.LoseConnection()` -> `Transport.Close()` (still graceful: flushes, half-closes, waits up to `CloseLinger`; `Abort` is unchanged)
- `Transport.SetContext(v)` / `Transport.Context()` -> removed; keep per-connection state in your Protocol struct
- `twine.ErrLoopStopped` -> removed; check `twine.ErrEngineStopped`
- `twine.ErrLineTooLong` -> `proto.ErrLineTooLong`
- `twine.ErrFrameTooLarge` -> `proto.ErrFrameTooLarge`
- `twine.Watchdog(0)` (off) -> `twine.Watchdog(-1)`; `Watchdog(0)` now means the default 1 ms
- `twine.ShutdownTimeout(0)` (no wait) -> `twine.ShutdownTimeout(-1)`; 0 now means the default 5 s
- `twine.CloseLinger(0)` (no linger) -> `twine.CloseLinger(-1)`; 0 now means the default 10 s
- `twine.DialTimeout(0)` (no timeout) -> `twine.DialTimeout(-1)`; 0 now means the default 30 s
- `twine.LockOSThread`, `MaxAcceptsPerEvent`, `TaskQueueSize`, `UDPBatch` -> removed
- `twine.BusyPoll(d)` as an engine `Option` -> `twine.BusyPoll(d)` as a `ListenOption`
- `Engine.PrometheusHandler()` -> `metrics.PrometheusHandler(eng)`
- `twine.LoopMetrics` -> `twine.MetricsSnapshot` (from `Engine.Metrics` / `Loop.Metrics`)
- `proto.NewBroadcast()` -> `&proto.Broadcast{}` (the zero value is ready to use)
- `proto.NewReconnectingClient(eng.NextLoop(), addr, f, opts...)` -> `proto.NewReconnectingClient(eng, addr, f, opts...)`
- `proto.Option` -> `proto.ReconnectOption`; `OnStateChange(func(bool))` -> `OnStateChange(func(connected bool, err error))`
- `proto.AMP.Handle func(cmd string, args Args) *twine.Future[Args]` -> `func(cmd string, args Args, reply func(Args, error))`
- AMP handler `return twine.Resolved(nil, res)` -> `reply(res, nil)`; `return twine.Rejected(nil, err)` -> `reply(nil, err)`; `return nil` (unhandled) -> `reply(nil, proto.ErrUnhandledCommand)`
- `lr.SendLine(t, b)`, `ns.SendString(t, b)`, `lp.SendFrame(t, b)` -> `lr.SendLine(b)`, `ns.SendString(b)`, `lp.SendFrame(b)` (the stored transport)
- `tls.Listen(eng, addr, cfg, f, tls.WithListenOptions(lo...), opts...)` -> `eng.ListenTCP(addr, twtls.Server(cfg, f, opts...), lo...)`
- `tls.WithListenOptions` -> removed (listener options go to `ListenTCP`)
- `tls.Connect(eng.NextLoop(), addr, cfg, f, opts...)` -> `twtls.Dial(eng, addr, cfg, f, opts...)` (`Connect(loop, ...)` stays for a chosen loop)
- `tls.Transport.KernelOffloaded()` -> `tx, rx := t.KernelOffload()`
- `tls.KTLSSupported()` -> removed (`KTLSAvailable` probes the kernel)
- `tls.HandshakeTimeout(0)` (off) -> no longer possible; `d <= 0` means the default 10 s
- `http.RequestCtx.SetUserValue` / `UserValue` -> removed; keep state in your own Protocol struct
- `http.Server{IdleTimeout: 0}` (off) -> `IdleTimeout: -1`; 0 now means the default 90 s

#### Changes the compiler will not catch

- `Watchdog(0)`, `CloseLinger(0)`, `ShutdownTimeout(0)` and `DialTimeout(0)`
  still compile but now mean the default, not "off". Pass a negative value
  to disable.
- `proto.ErrLineTooLong` and `proto.ErrFrameTooLarge` are new values with
  new text ("proto: ..." instead of "twine: ..."). Code that matches the
  error string, or compares against the removed root sentinels, breaks.
- Log messages and panic text are prefixed `twine/twhttp:` and
  `twine/twtls:` (were `twine/http:` and `twine/tls:`). Update log filters
  and alerts.
- An AMP handler that returns without calling `reply` leaves the caller
  pending until the connection is lost; returning nil used to send
  UNHANDLED. Call `reply(nil, proto.ErrUnhandledCommand)` instead. Only the
  first `reply` counts, so a panic after replying is logged but not sent.
- `twtls.Listen` returned an error for a configuration with no certificate
  source; `twtls.Server` panics. Check the configuration first if it comes
  from user input.
- `proto.NewReconnectingClient` takes an Engine and picks a loop itself; it
  can no longer be pinned to a chosen loop, and a nil Engine panics.

### Changed (breaking)

- Packages renamed: `http` is `twhttp` and `tls` is `twtls` (import paths
  `github.com/exergy-dev/twine/twhttp` and `.../twtls`). They no longer
  shadow `net/http` and `crypto/tls`, so they are imported without an
  alias.
- `Factory` is a function type, `func(peer net.Addr) Protocol`;
  `FactoryFunc` is removed. The peer argument and the rule that a nil
  Protocol refuses the connection are unchanged.
- `Transport.GetPeer` and `GetHost` are `RemoteAddr` and `LocalAddr`
  (the `net.Conn` names); `DatagramTransport.GetHost` is `LocalAddr`.
- `Transport.LoseConnection` is `Close`. Unlike `net.Conn.Close` it
  flushes pending writes first, then half-closes and waits up to
  `CloseLinger`; `Abort` remains the immediate close.
- `Transport.SetContext` and `Context` are removed: per-connection state
  belongs in the Protocol struct.
- `ErrLoopStopped` is removed (the core never returned it; `ErrEngineStopped`
  is the one to check). `ErrLineTooLong` and `ErrFrameTooLarge` are defined
  by `proto`, which produces them; the root aliases are gone.
- proto: `AMP.Handle` answers through a `reply func(Args, error)` argument
  instead of returning a Future. `reply` may be called inside the handler
  or later from any goroutine; a second call is ignored and a reply after
  the connection is lost writes nothing. `reply(nil, ErrUnhandledCommand)`
  sends UNHANDLED, a `*RemoteError` chooses the code, any other error is
  UNKNOWN. Serving a request allocates 9 times instead of 13.
- proto: `NewBroadcast` is removed; the zero `Broadcast` is an empty group.
- proto: `NewReconnectingClient` takes the `*Engine` and picks its loop once
  with `NextLoop`.
- twtls: `Listen` and `WithListenOptions` are removed; serve TLS with
  `eng.ListenTCP(addr, twtls.Server(cfg, f, tlsOpts...), listenOpts...)`.
  The new `Dial(eng, ...)` is `Connect` on `eng.NextLoop()`. The deprecated
  `Transport.KernelOffloaded` is removed; use `KernelOffload`.
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
  `twtls.Server` and `twtls.Client`.
- proto: `LineReceiver.SendLine`, `NetstringReceiver.SendString` and
  `LengthPrefixed.SendFrame` drop the transport argument and always use the
  stored transport.
- proto: `Option` is renamed `ReconnectOption`; the `OnStateChange`
  callback is `func(connected bool, err error)` and is called once more
  when the client gives up, with an error wrapping the new `ErrGaveUp`.
- proto: `LengthPrefixed.ConnectionMade` panics on a `PrefixLen` other
  than 0, 1, 2 or 4 instead of silently using 4.
- twtls: `KTLSSupported` is removed (it only reported `GOOS == "linux"`).
  `Transport.KernelOffload()` reports whether TX and RX are offloaded.
- twtls: `Server` panics when the config has no certificate source
  (`Certificates`, `GetCertificate` or `GetConfigForClient`) instead of
  failing every handshake silently.
- twtls: `HandshakeTimeout(d)` with `d <= 0` now means the default (10 s);
  it no longer disables the timeout. Handshake failures reach the
  `OnHandshakeError` hook and the `Connect` Future as a `*HandshakeError`
  carrying the peer address, and are logged at Warn when no hook is set.
- twhttp: `IdleTimeout` zero now means 90 s (was disabled); a negative value
  disables it. The new `ReadTimeout` (default 60 s) bounds an incomplete
  request head or body. Responses larger than the new `MaxResponseBytes`
  (default 16 MiB) become a 500 instead of a truncated body.
- twhttp: a status below 200 is sent as 500; response headers with an
  invalid name are dropped; HEAD responses with an empty body carry no
  Content-Length; canned error responses carry Date and Server.
- twhttp: `RequestCtx.SetUserValue` and `UserValue` are removed; keep
  per-connection state in your own Protocol struct.
- `Any` with no inputs rejects with `ErrNoFutures` (it rejected with the
  now removed `ErrLoopStopped`). A second `Engine.Run`, including one after a completed
  run, returns `ErrRunTwice` (a run after a completed run returned nil).
- One rule for duration options: 0 means the default and a negative value
  disables the bound. `Watchdog(0)` is now the default 1 ms threshold
  (`Watchdog(-1)` disables the watchdog), `ShutdownTimeout(0)` the default
  5 s (negative: abort unflushed connections at once), `CloseLinger(0)` the
  default 10 s (negative: close as soon as writes are flushed) and
  `DialTimeout(0)` the default 30 s (negative: no timeout). Negative
  `SpinIterations` values mean the default, and `DatagramsSent` counts only
  datagrams the kernel accepted.

### Added

- `ErrNoFutures`, `ErrRunTwice` and `ErrNilProtocol` sentinels.
- `Engine.Logger`, `Loop.ReportPanic`, and `poll` backend logging when a
  `twine_uring` build falls back to epoll.
- `NewPanicError`, which wraps a recovered panic value with the current
  stack the way the core does (the `twtls` package uses it instead of
  hand-built `PanicError` values).
- proto: `ErrLineTooLong` and `ErrFrameTooLarge` (moved from the root
  package); `FuzzAMP` and `FuzzLineReceiverRaw` fuzz targets; benchmarks.
- twtls: `Dial`, `HandshakeError`, `Transport.KernelOffload`, allocation
  gates and record benchmarks, and a layout test that fails when a Go
  release changes the `crypto/tls` fields kTLS reads.
- twhttp: `ReadTimeout`, `MaxResponseBytes`, `DefaultIdleTimeout`,
  `DefaultReadTimeout`, `DefaultMaxResponseBytes`; a 408 response.
- `twine_debug` assertions inside `internal/`: slab double-Put, re-entrant
  wheel Advance and ring invariants.
- `metrics` package with the Prometheus text-format handler.
- Runnable examples on pkg.go.dev for the root, `proto`, `twhttp` and
  `twtls` packages; package documentation for the root package.
- `CONTRIBUTING.md`, `SECURITY.md` and this changelog.

### Fixed

- `twine.Blocking` under `twtls.Server` panicked on every connection and
  the panic was swallowed; the client only saw EOF.
- `proto.AMP.Call` and `CallNoAnswer` read the caller's `Args` map on the
  loop after returning (a data race); the arguments are now encoded on the
  caller's goroutine.
- `Engine.Dial` and `Loop.ConnectTCP` with a host name resolved it on the
  loop thread, stalling every connection on that loop; names are now
  resolved on the worker pool.
- `twtls.Client` used with `Loop.ConnectTCP` resolved the Future with the
  raw socket before the handshake; the documentation now says so and
  points at `twtls.Connect` and `Dial`.
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
  no wakeup is written while the loop is inside a callback. CallFromThread
  is always correct, from any goroutine including the loop. `CallLater(0)`
  is only for code on the loop that wants a `Timer` handle or the
  next-iteration guarantee.
- kTLS offload covers TLS 1.3 AES-GCM only; ChaCha20-Poly1305 connections
  stay on the memory BIO.
- `Engine.ListenUDP` returns an error naming the option when given a
  TCP-only `ListenOption` (`Backlog`, `DeferAccept`, `BusyPoll`,
  `TCPNoDelay`) instead of silently ignoring it.
- `docs/API.md` now lists interface methods, struct fields, constants and
  variables, and CI fails when it is stale.
- The README teaches the API directly: after the echo server it shows a
  client, a timer, blocking work, an HTTP server and a TLS listener, each a
  compiled Example, then the ownership rules and the package table. The
  Twisted-to-Twine table moved to `docs/TWISTED.md`, GC tuning to
  `BENCHMARKS.md`, and the make-target list lives in `CONTRIBUTING.md`.
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
