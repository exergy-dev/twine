# Twine: High-Performance Go Reactor Framework — Design Doc

*As of 2026-11-06*

---

## 1. Overview

Twine is a Go event-loop networking framework that reproduces Twisted's programming model (Protocol, Transport, Factory, Deferred, producers/consumers, reactor timers) on a shared-nothing, one-loop-per-core reactor built directly on epoll/kqueue. It targets workloads where goroutine-per-connection loses: hundreds of thousands to millions of mostly idle connections, very small messages at very high rates, and tight tail-latency budgets.

Go's runtime already has a reactor (the netpoller) and cheap concurrency, so Twine does not fight it everywhere. The loop owns the hot path (accept, read, parse, write, timers); anything that blocks is offloaded to goroutines via `DeferToThread`, exactly like Twisted's `deferToThread`.

**How to use this document with Claude Code.** Implement the phases in Section 13 in order. Each phase lists deliverables and acceptance criteria that must pass before moving on. The public API in Section 6 is the contract; the hot-path rules in Section 7 and the ownership rules in Section 8 are non-negotiable invariants that every PR must preserve. Package layout is fixed in Section 5. When a design question is not answered here, prefer the choice that keeps the steady-state path allocation-free and lock-free.

- Language: Go 1.23+ (generics, `iter` package, `unique`)
- Module path: `github.com/<org>/twine`
- Platforms: Linux (primary, epoll + optional io_uring), macOS/FreeBSD (kqueue). Windows is out of scope.
- Dependencies: standard library plus `golang.org/x/sys/unix`. No third-party runtime dependencies in the core.

---

## 2. Goals and non-goals

**Goals**

1. Twisted's programming model in idiomatic Go: Protocol/Transport/Factory lifecycle, `CallLater`, `CallFromThread`, a typed `Future` replacing `Deferred`, push/pull producers for backpressure.
2. Zero heap allocations per read and per write on the steady-state path, verified by benchmarks with `-benchmem`.
3. Lock-free hot path: a loop never takes a mutex to accept, read, parse, write, or fire a timer.
4. Linear scaling across cores via SO_REUSEPORT and shared-nothing loops.
5. Memory per idle connection under 1 KB (excluding user protocol state).
6. A protocol library with the batteries Twisted users expect: line/netstring/length-prefixed framing, a zero-alloc HTTP/1.1 parser, UDP with batched syscalls, reconnecting clients.
7. Observability built in: per-loop metrics, slow-callback watchdog, optional Future creation stacks.

**Performance targets** (release gate for v1.0, measured on a 16-core Linux host, `GOMAXPROCS=16`)

| Metric | Target | Baseline to beat |
| --- | --- | --- |
| Echo server, 1M idle connections, RSS | ≤ 4 GB | goroutine-per-conn ≈ 8–16 GB |
| HTTP/1.1 plaintext, requests/s per core | ≥ 1M | `net/http` ≈ 100–200K |
| HTTP/1.1 plaintext, p99 latency at 80% load | ≤ 1 ms | `net/http` |
| Echo, 64-byte messages, msgs/s per core | ≥ 2M | gnet within 10% |
| allocs/op on read path, write path, timer fire | 0 | — |
| Timer insert + cancel, 1M active timers | O(1), ≤ 50 ns each | `time.AfterFunc` |

**Non-goals**

- Replacing `net` for ordinary services. If a service has 10K connections and calls a database per request, use goroutines.
- Windows support.
- HTTP/2, HTTP/3, WebSocket in v1 (WebSocket is a planned v1.1 protocol).
- Twisted's `twisted.web` resource tree, `twisted.application`, or plugin system. Twine is the networking core only.
- Async/await syntax emulation. Blocking code uses goroutines and `Future.Await`.

---

## 3. Twisted → Twine concept map

Every Twisted concept has a Twine equivalent. Names are kept close so Twisted users can transfer intuition.

| Twisted | Twine | Notes |
| --- | --- | --- |
| `reactor` (global) | `twine.Engine` + N `*twine.Loop` | No global. An Engine owns loops; a Transport knows its Loop. |
| `reactor.run()` | `engine.Run(ctx)` | Blocks until ctx is cancelled or `engine.Stop()`. |
| `reactor.callLater(d, f)` | `loop.CallLater(d, f) *Timer` | Timing wheel, O(1) insert/cancel. |
| `reactor.callFromThread(f)` | `loop.CallFromThread(f)` | Lock-free MPSC queue + coalesced eventfd wakeup. |
| `reactor.listenTCP(port, factory)` | `engine.ListenTCP(addr, factory, opts...)` | One listener per loop via SO_REUSEPORT. |
| `reactor.connectTCP(host, port, factory)` | `loop.ConnectTCP(addr, factory) *Future[Transport]` | Non-blocking connect, completes on EPOLLOUT. |
| `reactor.listenUDP(port, proto)` | `engine.ListenUDP(addr, proto)` | `recvmmsg`/`sendmmsg` batching. |
| `Protocol` | `twine.Protocol` interface | Same three methods: ConnectionMade, DataReceived, ConnectionLost. |
| `Factory` / `ServerFactory` | `twine.Factory` interface | `BuildProtocol(addr) Protocol`. |
| `ReconnectingClientFactory` | `twine.ReconnectingClient` | Exponential backoff with jitter. |
| `ITransport` | `twine.Transport` interface | Write, Writev, LoseConnection, Abort, GetPeer, GetHost. |
| `IPushProducer` / `IConsumer` | `twine.Producer` / `twine.Consumer` | Same pause/resume/stop contract. |
| `Deferred` | `twine.Future[T]` | Typed, generic. Callbacks run on the owning loop. |
| `Deferred.addCallback/addErrback` | `Future.Then / Future.Catch` | Chainable, returns a new Future. |
| `DeferredList` | `twine.All[T](...)` / `twine.Any[T](...)` | — |
| `threads.deferToThread(f)` | `twine.DeferToThread(loop, f) *Future[T]` | Runs `f` on a bounded worker pool. |
| `inlineCallbacks` / `async def` | goroutine + `Future.Await(ctx)` | Blocking style, only off-loop. |
| `LineReceiver` | `proto.LineReceiver` | Embeddable struct with a `LineReceived` callback. |
| `NetstringReceiver` | `proto.NetstringReceiver` | — |
| `Int32StringReceiver` | `proto.LengthPrefixed` | Configurable 1/2/4-byte prefix and endianness. |
| `AMP` | `proto.AMP` | Request/response multiplexing over length-prefixed frames. |
| `DatagramProtocol` | `twine.DatagramProtocol` | `DatagramReceived(b []byte, addr)`. |
| `reactor.spawnProcess` | out of scope | Use `os/exec` from a goroutine. |
| `twisted.internet.ssl` | `tls` package (Section 10) | — |
| `reactor.addSystemEventTrigger` | `engine.OnShutdown(f)` | — |

---

## 4. Architecture

The Engine starts one Loop per core, each pinned to an OS thread and owning its own file descriptors, buffers and timers. Nothing on the hot path is shared between loops, so no locks are taken to accept, read, parse, write or fire a timer.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Kernel: SO_REUSEPORT listener per loop, accepts load-balanced across    │
└───────────────┬──────────────────────┬──────────────────────┬────────────┘
                │ accept               │ accept               │ accept
┌───────────────▼──────────────────────▼──────────────────────▼────────────┐
│  Engine (one loop per core, shared-nothing)                              │
│  ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐ │
│  │ Loop 0 (OS thread)   │ │ Loop 1 (OS thread)   │ │ Loop N-1 (OS thread) │ │
│  │ edge-trig epoll/kq   │ │ edge-trig epoll/kq   │ │ edge-trig epoll/kq   │ │
│  │ hierarchical wheel   │ │ hierarchical wheel   │ │ hierarchical wheel   │ │
│  │ MPSC queue + eventfd │ │ MPSC queue + eventfd │ │ MPSC queue + eventfd │ │
│  │ conns: slice by fd   │ │ conns: slice by fd   │ │ conns: slice by fd   │ │
│  │ shared 64 KB rd buf  │ │ shared 64 KB rd buf  │ │ shared 64 KB rd buf  │ │
│  └──────────────────────┘ └──────────────────────┘ └──────────────────────┘ │
└──────────────────────────────┬───────────▲───────────────────────────────┘
          DeferToThread(fn) → Future │           │ CallFromThread(cb) resolves it
                        ┌─────────▼───────────┴─────────┐
                        │ Bounded worker pool (goroutines)│
                        │ DB, disk, CPU work, TLS handshake│
                        └─────────────────────────────────┘
```

Accepts enter from the kernel through a per-loop SO_REUSEPORT listener. Work that blocks leaves the loop through `DeferToThread` and comes back through `CallFromThread`, which is the only cross-thread entry point into a loop.

**Components**

- **Engine**: owns loops, listeners, the worker pool, and lifecycle (`Run`, `Stop`, `OnShutdown`). Picks the loop for an outbound connection round-robin.
- **Loop**: the reactor. One goroutine with `runtime.LockOSThread()`. Its iteration is: drain the task queue, fire expired timers, poll (with a short spin before blocking), dispatch I/O events, flush pending writes, repeat.
- **Poller**: platform abstraction over epoll (Linux) and kqueue (BSD/macOS), edge-triggered. An io_uring backend is an optional Linux build tag in Phase 6.
- **conn**: internal per-connection state: fd, protocol, outbound ring buffer, flags, idle-timer handle. Stored in a slice indexed by fd and recycled on close.
- **Timing wheel**: per-loop hierarchical wheel (4 levels, 256 slots each, 1 ms tick) for `CallLater` and idle timeouts.
- **Task queue**: per-loop lock-free MPSC ring for `CallFromThread`. Producers write a coalescing flag and, only if it was clear, write the eventfd.
- **Worker pool**: bounded goroutine pool (default `4 × GOMAXPROCS`, configurable) backing `DeferToThread`. Overflow blocks the caller goroutine, never the loop; a loop that calls `DeferToThread` enqueues without blocking.

**Loop iteration, in order**

1. Drain the MPSC task queue (bounded to 1024 tasks per iteration to prevent starvation).
2. Advance the timing wheel to `now` and fire expired timers.
3. Spin-poll with timeout 0 up to `SpinIterations` times (default 50); then block in `epoll_wait` with timeout = time to next timer (or −1).
4. For each ready fd: EPOLLIN → read until EAGAIN into the loop's shared buffer, calling `DataReceived` per chunk; EPOLLOUT → flush the conn's ring; EPOLLHUP/ERR → `ConnectionLost`.
5. Flush every conn marked dirty this iteration with `writev`.
6. Recycle closed conns.

---

## 5. Package layout

One Go module, flat where possible. Internal packages hide the poller and buffer machinery so the public surface stays small.

```
twine/
  engine.go          Engine: Run, Stop, ListenTCP, ListenUDP, OnShutdown, options
  loop.go            Loop: iteration, CallLater, CallFromThread, ConnectTCP
  protocol.go        Protocol, Factory, Transport, Producer, Consumer interfaces
  conn.go            internal conn struct, read/write dispatch, close/recycle
  transport.go       Transport implementation over conn
  future.go          Future[T], Then, Catch, Await, All, Any, DeferToThread
  timer.go           Timer handle, Stop, Reset
  options.go         functional options: Loops, ReusePort, SpinIterations, WorkerPool, ...
  errors.go          ErrClosed, ErrLoopStopped, ErrWriteBufferFull, ...
  metrics.go         per-loop counters, Snapshot(), expvar/Prometheus adapters
  watchdog.go        slow-callback detector

  internal/poll/     poller abstraction
    poll.go            Poller interface, Event, flags
    epoll_linux.go     edge-triggered epoll
    kqueue_bsd.go      kqueue (darwin, freebsd, netbsd, openbsd)
    uring_linux.go     io_uring backend, build tag `twine_uring` (Phase 6)
    wake_linux.go      eventfd wakeup
    wake_bsd.go        pipe-pair / EVFILT_USER wakeup
  internal/ring/     byte ring buffer with slab-backed growth, writev iovec export
  internal/slab/     size-classed []byte slab allocator (4K, 16K, 64K, 256K), per-loop
  internal/mpsc/     lock-free bounded MPSC queue of func()
  internal/wheel/    hierarchical timing wheel
  internal/sock/     raw socket helpers: nonblock, nodelay, reuseport, defer_accept, bufsizes

  proto/             protocol library (Section 9)
    line.go            LineReceiver
    netstring.go       NetstringReceiver
    lengthprefix.go    LengthPrefixed
    amp.go             AMP-style request/response
    reconnect.go       ReconnectingClient
  http/              zero-alloc HTTP/1.1 server (Section 9)
  tls/               TLS transport wrappers (Section 10)

  bench/             benchmarks and comparison harness (Section 12)
    echo_twine/  echo_goroutine/  echo_gnet/  http_twine/  http_nethttp/
  examples/
    echo/  chat/  http_hello/  udp_echo/  client_reconnect/
```

**Dependency rules**

- `twine` depends on `internal/*` only. `proto`, `http`, `tls` depend on `twine`, never on `internal/*`.
- `internal/*` packages depend on nothing but the standard library and `golang.org/x/sys/unix`.
- `bench/echo_gnet` is the only place a third-party module appears, and it lives in a separate `go.mod` under `bench/` so the root module stays dependency-free.

---

## 6. Core public API

This is the contract. Signatures may gain options but must not change shape between phases.

### 6.1 Protocol, Factory, Transport

```go
package twine

// Protocol is implemented by user code. All methods are called on the owning Loop.
type Protocol interface {
    // ConnectionMade is called once, after the Transport is ready.
    ConnectionMade(t Transport)
    // DataReceived is called with a slice of the loop's shared read buffer.
    // b is valid ONLY for the duration of the call. Copy what you keep.
    DataReceived(b []byte)
    // ConnectionLost is called exactly once. err is nil on clean close.
    ConnectionLost(err error)
}

// Factory builds a Protocol per accepted or connected transport.
type Factory interface {
    BuildProtocol(peer net.Addr) Protocol
}

// FactoryFunc adapts a function to Factory.
type FactoryFunc func(peer net.Addr) Protocol

// Transport is the loop-affine handle to a connection.
// Every method must be called from the owning Loop (see Section 8) except Loop().
type Transport interface {
    Write(b []byte)             // copies to the outbound ring only if an immediate write would block
    Writev(bs ...[]byte)
    LoseConnection()            // half-close after flushing pending writes
    Abort()                     // immediate close, discard pending writes
    GetPeer() net.Addr
    GetHost() net.Addr
    Loop() *Loop
    SetIdleTimeout(d time.Duration) // 0 disables; implemented on the timing wheel
    SetContext(v any)           // per-connection user slot
    Context() any
    // Backpressure (Twisted IPushProducer/IConsumer):
    PauseReading()              // drop EPOLLIN interest; DataReceived stops
    ResumeReading()
    RegisterProducer(p Producer, streaming bool)
    UnregisterProducer()
    PendingWrite() int          // bytes buffered in the outbound ring
}

// Producer is told to pause/resume when the transport's ring crosses its high/low watermarks.
type Producer interface {
    PauseProducing()
    ResumeProducing()
    StopProducing()
}

// Consumer is implemented by Transport; exposed so protocols can chain producers.
type Consumer interface {
    RegisterProducer(p Producer, streaming bool)
    UnregisterProducer()
    Write(b []byte)
}
```

### 6.2 Engine and Loop

```go
type Engine struct { /* unexported */ }

func New(opts ...Option) *Engine
func (e *Engine) Run(ctx context.Context) error     // blocks; returns on ctx done or Stop
func (e *Engine) Stop()                              // graceful: stop accepting, flush, close
func (e *Engine) OnShutdown(fn func())
func (e *Engine) Loops() []*Loop
func (e *Engine) ListenTCP(addr string, f Factory, opts ...ListenOption) (Listener, error)
func (e *Engine) ListenUDP(addr string, p DatagramProtocol, opts ...ListenOption) (Listener, error)
func (e *Engine) NextLoop() *Loop                   // round-robin; for outbound connects

type Loop struct { /* unexported */ }

func (l *Loop) ID() int
func (l *Loop) CallLater(d time.Duration, fn func()) *Timer
func (l *Loop) CallFromThread(fn func())             // safe from any goroutine; never blocks
func (l *Loop) InLoop() bool                         // true when called on this loop's thread
func (l *Loop) ConnectTCP(addr string, f Factory, opts ...DialOption) *Future[Transport]
func (l *Loop) Now() time.Time                       // cached per iteration; cheap
func (l *Loop) Metrics() LoopMetrics

type Timer struct { /* unexported */ }
func (t *Timer) Stop() bool                          // loop-affine
func (t *Timer) Reset(d time.Duration)               // loop-affine

// Options
func Loops(n int) Option                 // default runtime.GOMAXPROCS(0)
func ReusePort(on bool) Option           // default true on Linux
func SpinIterations(n int) Option        // default 50; 0 = block immediately
func WorkerPool(size int) Option         // default 4*GOMAXPROCS
func ReadBufferSize(n int) Option        // per-loop shared buffer, default 64 KiB
func WriteHighWater(n int) Option        // default 256 KiB; above this, producers are paused
func WriteLowWater(n int) Option         // default 64 KiB
func LockOSThread(on bool) Option        // default true
func Watchdog(threshold time.Duration) Option // default 1ms; 0 disables
func TCPNoDelay(on bool) ListenOption
func DeferAccept(d time.Duration) ListenOption // Linux TCP_DEFER_ACCEPT
```

### 6.3 Future (the Deferred replacement)

```go
// Future[T] is a single-assignment result whose callbacks run on its Loop.
// Then/Catch may be called from any goroutine; the callback itself always runs on the Loop.
type Future[T any] struct { /* unexported */ }

func NewFuture[T any](l *Loop) (*Future[T], Resolver[T])

type Resolver[T any] interface {
    Resolve(v T)                // idempotent; second call is a no-op
    Reject(err error)
}

func (f *Future[T]) Then(fn func(T)) *Future[T]            // runs on loop; returns f for chaining
func (f *Future[T]) Catch(fn func(error)) *Future[T]
func (f *Future[T]) Finally(fn func()) *Future[T]
func (f *Future[T]) Done() bool
func (f *Future[T]) Loop() *Loop
// Await blocks the calling goroutine. Panics if called on the Future's own Loop.
func (f *Future[T]) Await(ctx context.Context) (T, error)

// Map transforms a Future's value into a new Future on the same loop.
func Map[T, U any](f *Future[T], fn func(T) (U, error)) *Future[U]

// All resolves when every input resolves, rejects on the first rejection.
func All[T any](l *Loop, fs ...*Future[T]) *Future[[]T]
// Any resolves with the first value, rejects only if all reject.
func Any[T any](l *Loop, fs ...*Future[T]) *Future[T]

// DeferToThread runs fn on the worker pool and resolves on l.
func DeferToThread[T any](l *Loop, fn func() (T, error)) *Future[T]

// Sleep is CallLater as a Future.
func Sleep(l *Loop, d time.Duration) *Future[struct{}]
```

**Future semantics**

- Resolution from off-loop goroutines is routed through `CallFromThread`; callbacks therefore never run concurrently with loop code.
- A callback that panics rejects the Future with a `PanicError{Value, Stack}`; the loop survives.
- A rejected Future with no `Catch` logs once at WARN with the creation stack when `TWINE_DEBUG_FUTURES=1` (Section 11).
- `Await` from a loop goroutine panics with `ErrAwaitOnLoop`. This is the deadlock Twisted users hit most; fail loud.

### 6.4 Minimal echo server (reference example)

```go
type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t; t.SetIdleTimeout(30 * time.Second) }
func (e *echo) DataReceived(b []byte)             { e.t.Write(b) } // Write copies only if it must buffer
func (e *echo) ConnectionLost(err error)          {}

func main() {
    eng := twine.New(twine.Loops(0)) // 0 = GOMAXPROCS
    eng.ListenTCP(":9000", twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} }))
    log.Fatal(eng.Run(context.Background()))
}
```

---

## 7. Hot-path design

These are the techniques that deliver the targets in Section 2. Each is an invariant, not a suggestion.

### 7.1 Read path

- One read buffer per **loop** (default 64 KiB), not per connection. `DataReceived` receives a sub-slice; protocols that need to retain bytes copy them into their own state. This is the single largest memory win at 1M connections.
- Edge-triggered: on EPOLLIN, loop `read(2)` until EAGAIN or the buffer is full, calling `DataReceived` after each read. Cap at `MaxReadsPerEvent` (default 8) per fd per iteration to prevent one chatty peer from starving others; re-arm by pushing the fd to a "ready" list processed next iteration.
- `PauseReading` removes EPOLLIN from the fd's interest set with `EPOLL_CTL_MOD`; `ResumeReading` adds it back and, because edge-triggered edges may have been missed, immediately attempts one read.

### 7.2 Write path

1. `Write(b)` first checks the ring is empty. If so, call `write(2)` directly. If everything was written, return. Zero copies.
2. Partial write or EAGAIN: copy the remainder into the per-conn ring (`internal/ring`), mark the conn dirty, and arm EPOLLOUT.
3. If the ring is non-empty, append only; do not attempt a syscall. The flush step at the end of the iteration issues one `writev` per dirty conn.
4. When the ring drains, disarm EPOLLOUT. Fire `ResumeProducing` on the registered producer when pending drops below `WriteLowWater`; `PauseProducing` when it exceeds `WriteHighWater`.
5. Ring storage comes from the loop's slab allocator in size classes (4K/16K/64K/256K). A ring grows by chaining slabs, never by `append` reallocation. Slabs are returned on close.

### 7.3 Allocation rules

- **Zero heap allocations** per read, per write, per timer fire, per `CallFromThread` on the steady-state path. Enforced by `testing.AllocsPerRun` tests in CI, which fail on any non-zero result.
- `conn` structs live in `[]*conn` indexed by fd. Freed conns go on a free list; a new accept reuses one. No map lookups on the hot path.
- `conn` keeps pointer-bearing fields to a minimum so GC scanning cost per connection stays low. Buffers are `[]byte` slabs; the GC scans their headers, not their contents.
- No closures allocated per event. Callbacks that need captured state use method values on pre-allocated structs or the `Context()` slot.
- `sync.Pool` is permitted only in off-loop code (worker pool, TLS handshakes).
- `interface{}` conversions of non-pointer types allocate; `Transport.SetContext(v any)` is documented as allocating unless `v` is a pointer.

### 7.4 Timers

- Hierarchical timing wheel per loop: 4 levels × 256 slots, 1 ms base tick. Insert and cancel are O(1); the wheel cascades every 256 ticks.
- `Timer` is a small struct embedded in an intrusive doubly-linked list per slot; `Stop` unlinks in place. Timers are pooled per loop.
- Idle timeouts use the same wheel. With 1M idle connections and a 30 s timeout the wheel holds 1M entries at ~48 bytes each.
- Timers fire on the loop with the loop's cached `Now()`; a timer never fires early and may fire up to one tick late.

### 7.5 Cross-thread calls

- `CallFromThread` pushes to a bounded lock-free MPSC ring (default 64K entries). On overflow it falls back to a mutex-guarded overflow slice so it never drops or blocks.
- Wakeup is coalesced: the producer does an atomic CAS on a `wakePending` flag and writes the eventfd only if it won the CAS. The loop clears the flag before draining. Net effect: N concurrent calls cost at most one syscall.
- A loop calling its own `CallFromThread` appends to a local slice instead, skipping the atomics.

### 7.6 Syscalls and socket options

- All socket I/O goes through `golang.org/x/sys/unix` directly: `Accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)`, `Read`, `Write`, `Writev`, `EpollWait`. The `net` package is used only to parse addresses.
- Listeners: `SO_REUSEADDR`, `SO_REUSEPORT`, `TCP_NODELAY` on accepted sockets, `TCP_DEFER_ACCEPT` when configured, `TCP_QUICKACK` left at default.
- `accept4` loops until EAGAIN, capped at `MaxAcceptsPerEvent` (default 64).
- `EpollWait` is called with a stack-allocated `[256]unix.EpollEvent`.
- Blocking in `EpollWait` releases the P: the loop goroutine is in a syscall, so the Go runtime can use that P for the worker pool. Spin-polling (`SpinIterations`) trades this for latency and is off by default in `Loops()` auto-config when `Loops > GOMAXPROCS/2`.

### 7.7 GC and memory

- Document recommended `GOGC` (200–400) and `GOMEMLIMIT` settings in the README; the framework does not set them.
- Large slabs are allocated with `make([]byte, n)` once per loop and reused; they do not churn.
- A `MetricsSnapshot` exposes heap-in-use, slab utilization, and ring high-water marks so operators can tune.

---

## 8. Concurrency and ownership rules

A connection belongs to exactly one loop for its entire life. Everything else follows from that.

1. **Loop affinity.** `Protocol` methods, `Transport` methods (except `Loop()`), `Timer.Stop/Reset`, and Future callbacks run only on the owning loop's thread. Calling them from elsewhere is a bug. In debug builds (`TWINE_DEBUG=1`) every such call checks `InLoop()` and panics with the caller's stack if it fails.
2. **The one door in.** `CallFromThread` is the only safe way for another goroutine to touch a loop's state. `Future.Resolve/Reject` and `DeferToThread` completion use it internally.
3. **The one door out.** `DeferToThread` is the only sanctioned way to run blocking work. Protocol code that blocks stalls every connection on that loop; the watchdog (Section 11) reports it.
4. **No cross-loop connection access.** To send to a connection on another loop, obtain its `Loop()` and `CallFromThread` a closure that calls `Write`. A `Broadcast` helper in `proto` wraps this pattern.
5. **Buffer lifetime.** The slice passed to `DataReceived` is dead when the method returns. Retaining it is a bug; debug builds poison the buffer after each call to surface it.
6. **Write ownership.** `Write(b)` may retain a copy of `b` but never `b` itself; the caller may reuse `b` immediately after `Write` returns.
7. **Shutdown order.** `Engine.Stop` closes listeners, then on each loop: fires `OnShutdown` hooks, calls `LoseConnection` on every conn, flushes rings for up to `ShutdownTimeout` (default 5 s), then closes fds and exits the loop goroutine. `Run` returns after all loops exit.
8. **Panics.** A panic in `Protocol` code is recovered by the loop, the connection is aborted with `ConnectionLost(PanicError)`, and the loop continues. A panic in loop-internal code is fatal.
9. **Goroutine-per-connection escape hatch.** `twine.Blocking(f Factory) Factory` wraps a factory so each connection's protocol runs on its own goroutine fed by a channel; `DataReceived` copies into the channel. This is the migration path for code that is not ready for callback style, at goroutine-per-conn cost.

---

## 9. Protocol library

Every protocol here is an embeddable struct that implements `twine.Protocol` and exposes a higher-level callback. Framing parsers operate directly on the loop buffer and copy only incomplete trailing bytes.

| Package.Type | Callback | Behaviour |
| --- | --- | --- |
| `proto.LineReceiver` | `LineReceived(line []byte)` | Delimiter configurable (default `\r\n`), `MaxLineLength` (default 16 KiB) aborts with `ErrLineTooLong`. `SetRawMode()` switches to passthrough `RawDataReceived`. `line` is valid only during the call. |
| `proto.NetstringReceiver` | `StringReceived(s []byte)` | `len:data,` framing. `MaxLength` default 1 MiB. |
| `proto.LengthPrefixed` | `FrameReceived(f []byte)` | 1/2/4-byte prefix, big or little endian, optional length-includes-prefix. Replaces Twisted's Int8/16/32StringReceiver. `SendFrame(b)` writes prefix+body with one `Writev`. |
| `proto.AMP` | `Handle(cmd string, args Args) *Future[Args]` | Request/response multiplexing with 32-bit ask IDs over `LengthPrefixed`. `Call(cmd, args) *Future[Args]`. |
| `proto.ReconnectingClient` | wraps a `Factory` | Exponential backoff (initial 1 s, factor 1.6, max 60 s, full jitter). `ResetDelay()` on success. `StopTrying()`. |
| `proto.Broadcast` | — | Loop-aware fan-out: group transports by loop and deliver one `CallFromThread` per loop carrying a shared read-only buffer. |
| `twine.DatagramProtocol` | `DatagramReceived(b []byte, from net.Addr)` | `ListenUDP` reads with `recvmmsg` (batch 64) and buffers outbound datagrams for `sendmmsg` at iteration end. |
| `http.Server` | `Handler(ctx *http.RequestCtx)` | HTTP/1.1 with keep-alive and pipelining. Zero allocations per request: headers parsed into a fixed-capacity slice of byte-slice pairs referencing the loop buffer; the body, if any, is copied. Response writer appends to the ring. No `net/http` types; a `net/http.Handler` adapter is provided at a documented cost. |

**HTTP parser rules** (modelled on fasthttp)

- Request line and headers are parsed with a hand-written state machine, no regex, no `strings` package allocations.
- Header lookup is case-insensitive via a precomputed lowercase comparison; no map.
- `Content-Length` bodies are delivered once complete; chunked bodies are decoded into a per-conn slab.
- Limits: `MaxHeaderBytes` 8 KiB, `MaxBodyBytes` 4 MiB, `MaxPipelinedRequests` 16; exceeding any aborts with 431/413.
- `Date` header is cached per loop and regenerated once per second via `CallLater`.

**Out of scope for v1** (planned v1.1): WebSocket over `http.Server`, HTTP client, TLS-terminated HTTP.

---

## 10. TLS strategy

`crypto/tls` is designed around a blocking `net.Conn`, which does not fit a non-blocking loop. Twine ships two modes and leaves a third as a documented extension point.

| Mode | How | Cost | When |
| --- | --- | --- | --- |
| **Memory-BIO (default)** | `tls.Server(bio, cfg)` where `bio` is an in-memory `net.Conn` whose `Read` returns `ErrWouldBlock` when the inbound queue is empty. The loop feeds ciphertext in via `DataReceived`, drives `Handshake()`/`Read()`/`Write()` until they would block, and ships `bio`'s outbound bytes to the real transport. Record decryption happens on the loop. | One extra copy per direction; `crypto/tls` allocates during handshake (off-loop budget: handshakes run via `DeferToThread`) | Default for all TLS connections |
| **kTLS offload** | After the memory-BIO handshake, export the session keys and call `setsockopt(SOL_TLS)` so the kernel encrypts/decrypts. The loop then does plain `read`/`write`. Requires Linux 5.x with `tls` module; AES-GCM/ChaCha20 only. | Handshake cost only; steady state is as fast as plaintext | Opt-in via `tls.KernelOffload(true)` when available; fall back to memory-BIO otherwise |
| **Goroutine adapter** | `twine.Blocking` factory plus a real `net.Conn` wrapper, so `crypto/tls` runs unmodified on a goroutine | Goroutine-per-conn memory and scheduling | Migration path; not a performance target |

**Design notes**

- The handshake is the allocation-heavy part. Running it via `DeferToThread` keeps the loop allocation-free while it waits; the handshake Future resolves on the loop, after which steady-state reads happen in place.
- `crypto/tls` `Read` requires the full record before returning plaintext; the memory-BIO buffers up to 16 KiB + overhead per conn during a partial record. This is the one place a per-conn buffer is unavoidable.
- ALPN, SNI-based cert selection and session resumption come for free from `crypto/tls.Config`.
- Client TLS (`tls.Connect`) mirrors the server path.
- Phase 5 implements memory-BIO; kTLS is Phase 6 alongside io_uring, gated by a runtime capability probe.

---

## 11. Errors, observability and debugging

Twisted's worst production failures are silent: a slow callback, a swallowed errback, a Deferred that never fires. Twine makes each one loud.

**Error values** (all in `errors.go`, all comparable with `errors.Is`)

| Error | Raised when |
| --- | --- |
| `ErrClosed` | `Write` on a closed transport (no-op, but recorded in metrics) |
| `ErrLoopStopped` | `CallFromThread` or `CallLater` after the loop has exited |
| `ErrWriteBufferFull` | pending writes exceed `WriteBufferLimit` (default 16 MiB); the conn is aborted |
| `ErrAwaitOnLoop` | `Future.Await` called from a loop goroutine (panic) |
| `ErrIdleTimeout` | passed to `ConnectionLost` when the idle timer fires |
| `PanicError{Value, Stack}` | protocol or callback panicked |
| `ErrLineTooLong`, `ErrFrameTooLarge` | framing limits exceeded |

**Watchdog.** Each loop records a monotonic timestamp before dispatching a callback. A separate goroutine samples every loop's "current callback started at" value; if one exceeds `Watchdog(threshold)` it logs the loop ID, the protocol type, and a goroutine dump of that loop thread (`runtime.Stack` on the locked thread via a signal on Linux). Default threshold 1 ms; the log line is rate-limited to once per second per loop.

**Metrics** (`LoopMetrics`, atomics updated on the loop, read from anywhere)

- `Connections`, `Accepts`, `Closes`, `ReadBytes`, `WriteBytes`, `Writev`, `WriteDirectHit` (writes that bypassed the ring)
- `TimersFired`, `TasksRun`, `Wakeups`, `WakeupsCoalesced`
- `IterationNanos` histogram (p50/p99/max over the last 10 s), `PollBlockNanos`
- `SlabInUse`, `RingHighWater`, `BacklogDrops`
- Exposed via `engine.Metrics()` and an optional `metrics.PrometheusHandler()`

**Debug builds** (`TWINE_DEBUG=1` at runtime or `-tags twine_debug` at build)

- Loop-affinity assertions on every Transport/Timer method.
- Read-buffer poisoning (`0xDE`) after each `DataReceived` to catch retained slices.
- `Future` captures its creation stack; unhandled rejections and Futures garbage-collected without resolving (via `runtime.AddCleanup`) are logged with that stack.
- A `--trace` flag on examples writes a `runtime/trace` file.

**Logging.** Twine uses `log/slog` through an `Option` (`Logger(*slog.Logger)`). It never logs on the hot path; all logging is rate-limited or event-driven (accept errors, watchdog, shutdown).

---

## 12. Testing and benchmarking

CI runs on Linux and macOS. Every phase adds tests; no phase is complete with a failing or skipped test.

**Unit tests**

- `internal/ring`, `internal/slab`, `internal/mpsc`, `internal/wheel`: property-based tests with `testing/quick` plus explicit edge cases (wraparound, cascade boundaries, full queue).
- `internal/mpsc`: a stress test with 64 producers × 1M pushes under `-race`.
- `future.go`: resolution ordering, panic capture, `Await` on loop panics, `All`/`Any` with mixed outcomes.
- Protocol parsers: fuzz targets (`go test -fuzz`) for LineReceiver, Netstring, LengthPrefixed and the HTTP parser, run for 10 minutes in nightly CI with a corpus checked in.

**Integration tests** (real sockets on loopback)

- Echo round-trip across 1, 2 and `GOMAXPROCS` loops.
- Backpressure: a slow reader triggers `PauseProducing` and `ResumeProducing` on the writer in the right order, exactly once each per crossing.
- Partial writes: inject EAGAIN with a tiny `SO_SNDBUF` and verify byte-exact delivery of 100 MiB.
- Half-close semantics of `LoseConnection` vs `Abort`.
- Idle timeout fires within one tick of the deadline.
- `CallFromThread` from 1000 goroutines while the loop is blocked in poll; count wakeups ≤ calls.
- Graceful shutdown flushes pending writes and calls `ConnectionLost` exactly once per conn.
- Protocol panic aborts only that connection.
- 100K-connection soak on loopback (CI uses 10K; the full number runs nightly): memory stable over 10 minutes.

**Allocation gates** (fail CI on regression)

```go
func TestReadPathZeroAlloc(t *testing.T) {
    allocs := testing.AllocsPerRun(1000, func() { /* one DataReceived dispatch */ })
    if allocs != 0 { t.Fatalf("read path allocates: %v", allocs) }
}
```

Same gate for: write hit path, write buffered path, timer fire, `CallFromThread` push and drain, HTTP request parse + response.

**Benchmarks** (`bench/`, run with `-benchmem -count=5`, compared with `benchstat`)

| Benchmark | Harness | Compared against |
| --- | --- | --- |
| `echo_idle_1m` | 1M idle conns, measure RSS after 60 s | `echo_goroutine` |
| `echo_small` | 1000 conns, 64 B ping-pong, msgs/s/core | `echo_goroutine`, `echo_gnet` |
| `echo_stream` | 100 conns, 1 MiB streams, GB/s | `echo_goroutine` |
| `http_plaintext` | `wrk -t16 -c256` and `-c10000`, req/s + p99 | `http_nethttp`, fasthttp |
| `timers_1m` | insert/cancel/fire 1M timers, ns/op | `time.AfterFunc` |
| `callfromthread` | 16 producers, ops/s, wakeups/op | channel send + select |

A `make bench` target runs the suite and writes `bench/results/<git-sha>.txt`; `make bench-compare` diffs against `main`. Results that regress more than 5% on any headline metric block merge.

**Race and sanitizers.** The full test suite runs under `-race` in CI. Loop-internal code is lock-free by design, so `-race` exercises the `CallFromThread` boundary, Future resolution, and metrics atomics.

---

## 13. Implementation phases

Work the phases in order. Each phase is a mergeable milestone with acceptance criteria; do not start the next until the current one's criteria pass in CI. Each phase should land as a small series of PRs, not one large one.

### Phase 0: Scaffold

- [ ] `go mod init`, directory layout from Section 5, `Makefile` with `test`, `test-race`, `bench`, `lint` (`golangci-lint` with `gocritic`, `staticcheck`, `errcheck`)
- [ ] GitHub Actions: Linux + macOS matrix, Go 1.23, `-race`
- [ ] `errors.go`, `options.go` with the option types from Section 6.2 (no behaviour yet)
- [ ] `README.md` with the Twisted → Twine table from Section 3

**Accept:** `make test` and `make lint` pass on an empty module.

### Phase 1: Single loop, epoll, echo

- [ ] `internal/poll`: `Poller` interface; epoll (edge-triggered) and kqueue implementations; eventfd/pipe wakeup
- [ ] `internal/sock`: nonblocking accept4/socket helpers, `TCP_NODELAY`, `SO_REUSEPORT`
- [ ] `loop.go`: single loop, iteration order from Section 4, shared read buffer, fd-indexed conn slice
- [ ] `conn.go` + `transport.go`: `Write` with direct-write-first and a naive `[]byte` append buffer (ring comes in Phase 2)
- [ ] `protocol.go` interfaces; `engine.go` with `New`, `Run`, `Stop`, `ListenTCP` for `Loops(1)` only
- [ ] `examples/echo`, `bench/echo_twine`, `bench/echo_goroutine`

**Accept:** echo integration test passes on Linux and macOS; `echo_small` benchmark runs and prints msgs/s; read path has 0 allocs/op.

### Phase 2: Multi-loop, write ring, slabs

- [ ] `internal/slab`, `internal/ring` with `writev` iovec export
- [ ] Replace the Phase 1 append buffer with the ring; implement EPOLLOUT arming, end-of-iteration `writev` flush, `WriteHighWater`/`WriteLowWater`
- [ ] `Loops(n)` with one SO_REUSEPORT listener per loop; `NextLoop` round-robin; `LockOSThread`
- [ ] `Producer`/`Consumer`, `PauseReading`/`ResumeReading`, `RegisterProducer`
- [ ] Partial-write and backpressure integration tests from Section 12

**Accept:** write hit path and buffered path 0 allocs/op; `echo_small` scales ≥ 0.8× linearly from 1 to 8 loops; 100 MiB partial-write test passes byte-exact.

### Phase 3: Timers, cross-thread, Future

- [ ] `internal/wheel` hierarchical timing wheel; `CallLater`, `Timer.Stop/Reset`, `SetIdleTimeout`
- [ ] `internal/mpsc` + coalesced wakeup; `CallFromThread`; `InLoop`
- [ ] `future.go`: `Future[T]`, `Resolver`, `Then/Catch/Finally`, `Await`, `Map`, `All`, `Any`, `Sleep`
- [ ] Worker pool and `DeferToThread`
- [ ] `ConnectTCP` with non-blocking connect completing on EPOLLOUT; `DialOption` timeout
- [ ] `examples/client_reconnect` (plain reconnect loop; `ReconnectingClient` lands in Phase 4)

**Accept:** `timers_1m` ≤ 50 ns insert+cancel, 0 allocs; `callfromthread` wakeups/op ≤ 0.1 under 16 producers; Future tests pass under `-race`; `Await` on loop panics with `ErrAwaitOnLoop`.

### Phase 4: Protocol library and observability

- [ ] `proto`: `LineReceiver`, `NetstringReceiver`, `LengthPrefixed`, `ReconnectingClient`, `Broadcast`
- [ ] `DatagramProtocol`, `ListenUDP` with `recvmmsg`/`sendmmsg` (Linux) and plain recvfrom fallback (BSD)
- [ ] `watchdog.go`, `metrics.go`, `Logger` option, `TWINE_DEBUG` affinity assertions and buffer poisoning
- [ ] Graceful shutdown per Section 8 rule 7; panic recovery per rule 8; `twine.Blocking` adapter
- [ ] Fuzz targets for all framing parsers; `examples/chat`, `examples/udp_echo`

**Accept:** fuzzers run 10 minutes clean; shutdown test calls `ConnectionLost` exactly once per conn; watchdog detects an injected 5 ms sleep.

### Phase 5: HTTP/1.1 and TLS (memory-BIO)

- [ ] `http`: parser state machine, `RequestCtx`, response writer, keep-alive, pipelining, limits, cached `Date`
- [ ] `http.Adapt(net/http.Handler)` with its cost documented
- [ ] `tls`: memory-BIO `net.Conn`, handshake via `DeferToThread`, `tls.Listen`/`tls.Connect` wrappers
- [ ] `bench/http_twine`, `bench/http_nethttp`, `examples/http_hello`

**Accept:** HTTP parse+respond 0 allocs/op; `http_plaintext` ≥ 5× `net/http` req/s at `-c256`; TLS echo passes against `openssl s_client` (HTTP/2 not in scope).

### Phase 6: Platform extras and release

- [ ] `internal/poll/uring_linux.go` behind `twine_uring` build tag: batched accept/read/write submissions, same `Poller` interface
- [ ] kTLS offload with capability probe and fallback
- [ ] `proto.AMP`; `SO_BUSY_POLL` and `TCP_DEFER_ACCEPT` options
- [ ] 1M-connection soak on a 16-core host; publish numbers against the Section 2 targets in `BENCHMARKS.md`
- [ ] API freeze, `v1.0.0` tag, godoc pass on every exported symbol

**Accept:** every Section 2 target met or the miss documented with a cause; `go vet`, `staticcheck`, `-race` clean; godoc coverage 100% of exported identifiers.

---

## 14. Open questions and risks

Decisions deferred to implementation time, and the risks most likely to cost the targets.

**Open questions** (decide in the PR that first needs them; record the answer here)

- [x] Should `DataReceived` be allowed to call `Write` re-entrantly during a flush? **Decided: yes.** `conn.flush` sets `flagFlushing`; `Write` during a flush (e.g. from a producer callback) takes the ring append path only, never a direct syscall.
- [x] Build on gnet's or netpoll's poller instead of writing `internal/poll`? **Decided: own poller** (`internal/poll`, epoll + kqueue, edge-triggered; io_uring readiness backend behind `twine_uring`). gnet is compared in `bench/echo_gnet` (see BENCHMARKS.md).
- [x] Should `Loops(0)` default to `GOMAXPROCS` or `GOMAXPROCS/2` when the worker pool is in heavy use? **Decided: `GOMAXPROCS`**, with spin-polling auto-disabled when `Loops > GOMAXPROCS/2` (Section 7.6) so blocked loops release their Ps to the worker pool. Revisit with production data.
- [ ] Expose `io_uring` as a default on kernels ≥ 6.x once stable, or keep it opt-in forever? **Status: opt-in** (`-tags twine_uring`); the backend is readiness-only (`IORING_OP_POLL_ADD` multishot), so it brings no completion-model batching yet. Default stays epoll.
- [x] `Future.Then` returning the same Future (Twisted chaining) vs. a new one (JS promise semantics). **Decided: same Future; `Map` for transformation.** A panicking `Then` callback rejects the Future for the remaining `Catch` callbacks.

**Decisions recorded during implementation**

- `Transport` handles are generation-checked: `conn` structs are pooled, so a handle kept after `ConnectionLost` becomes inert (counted in `WriteAfterClose`) instead of aliasing the next connection that reuses the struct.
- `LoseConnection` = flush, `SHUT_WR`, then close when the peer closes or after `CloseLinger` (default 10 s; 0 reproduces Twisted's close-on-flush). `Abort` = immediate close, pending writes discarded, `ConnectionLost(nil)`.
- `CallLater(0)` runs on the next iteration via a per-loop due list, never re-entrantly inside the wheel advance (a self-re-arming zero-delay timer would otherwise spin inside `Advance`).
- `OnShutdown` hooks run once on the `Run` goroutine after listeners close and before connections are closed (Section 8 rule 7 says "on each loop"; a single run is what the Twisted trigger does and is what users expect).
- The watchdog dumps the loop goroutine via `runtime.Stack(all)` filtered by goroutine id instead of a signal to the locked thread (portable, no signal handler).
- `CallFromThread` after loop exit drops the function and counts `BacklogDrops`; there is no return value to carry `ErrLoopStopped`, and `Loop.Stopped()` exposes the state.
- kTLS offload is not implementable on `crypto/tls` (Go 1.25): record-layer traffic secrets are unexported and `ExportKeyingMaterial` derives different keys. `tls.KernelOffload(true)` probes the kernel and falls back to memory-BIO.

**Risks**

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Goroutine-per-conn turns out competitive for the user's real workload | The framework is unnecessary | Phase 1 benchmark gate: if `echo_small` is < 1.5× goroutine-per-conn, stop and reassess scope |
| `crypto/tls` memory-BIO adds a per-conn buffer that erodes the 1 KB/conn target under TLS | TLS deployments lose the memory win | Document separate targets for TLS; pursue kTLS in Phase 6 |
| Edge-triggered epoll bugs (missed edges after `ResumeReading`, EPOLLOUT races) | Hangs under load | The "attempt one read on resume" rule; dedicated integration tests that toggle pause/resume 1M times |
| `runtime.LockOSThread` loops starve the Go scheduler when `Loops == GOMAXPROCS` and the worker pool is busy | Latency spikes in `DeferToThread` work | Default `Loops` to `GOMAXPROCS`, document `GOMAXPROCS = loops + workers` tuning, expose `IterationNanos` |
| Slow-callback stalls (the Twisted failure mode) | One bad handler freezes thousands of conns | Watchdog on by default; `Blocking` adapter as an easy out |
| kqueue semantics differ from epoll (level-triggered by default, `EV_CLEAR` for edge) | macOS tests pass but Linux behaves differently, or vice versa | CI matrix on both; the `Poller` interface specifies edge-triggered semantics and each backend is tested against the same suite |
| Scope creep into `twisted.web`-style features | v1 never ships | Non-goals in Section 2 are enforced at review |

