# Twine (Rust): High-Performance Reactor Framework — Design Doc

*Adapted from the Go design (`../design.md`, 2026-11-06) for a Rust implementation, 2026-11-07.*

---

## 1. Overview

Twine is an event-loop networking framework that reproduces Twisted's programming model
(Protocol, Transport, Factory, Deferred, producers/consumers, reactor timers) on a
shared-nothing, one-loop-per-core reactor built directly on epoll/kqueue. It targets workloads
where thread-per-connection and work-stealing async runtimes lose: hundreds of thousands to
millions of mostly idle connections, very small messages at very high rates, and tight
tail-latency budgets.

The loop owns the hot path (accept, read, parse, write, timers); anything that blocks is
offloaded to a bounded thread pool via `defer_to_thread`, exactly like Twisted's
`deferToThread`. Results come back to the loop through `call_from_thread`, the one door in.

**How to use this document with Claude Code.** Implement the phases in Section 13 in order.
Each phase lists deliverables and acceptance criteria. The public API in Section 6 is the
contract; the hot-path rules in Section 7 and the ownership rules in Section 8 are
non-negotiable invariants. Crate layout is fixed in Section 5. When a design question is not
answered here, prefer the choice that keeps the steady-state path allocation-free, lock-free
and free of `unsafe`.

- Language: Rust 2021, stable toolchain (MSRV = the installed stable, 1.85+ features used).
- Workspace: `rust/` in the twine repository; crates `twine`, `twine-proto`, `twine-http`,
  `twine-tls` plus examples and benchmarks.
- Platforms: Linux (primary, epoll; optional io_uring readiness backend), macOS/FreeBSD
  (kqueue, compile-checked). Windows is out of scope.
- Dependencies of the core: `libc` and the `log` facade. Nothing else. `twine-tls` uses
  `rustls`. Benchmark baselines may use `tokio` (confined to the bench crate).

---

## 2. Goals and non-goals

**Goals**

1. Twisted's programming model in idiomatic Rust: Protocol/Transport/Factory lifecycle,
   `call_later`, `call_from_thread`, a typed `Deferred<T>`, push/pull producers for backpressure.
2. Zero heap allocations per read and per write on the steady-state path, verified by tests
   with a counting global allocator.
3. Lock-free hot path: a loop never takes a mutex to accept, read, parse, write, or fire a timer.
4. Linear scaling across cores via SO_REUSEPORT and shared-nothing loops.
5. Memory per idle connection under 1 KB (excluding user protocol state).
6. A protocol library with the batteries Twisted users expect: line/netstring/length-prefixed
   framing, a zero-alloc HTTP/1.1 parser, UDP with batched syscalls, reconnecting clients, AMP.
7. Observability built in: per-loop metrics, slow-callback watchdog, optional `Deferred`
   creation backtraces.
8. Loop affinity and buffer lifetime enforced by the type system, not by debug assertions.

**Performance targets** (same as the Go design; measured on the development host and
recorded with causes for any miss in `BENCHMARKS.md`)

| Metric | Target | Baseline to beat |
| --- | --- | --- |
| Echo server, 1M idle connections, RSS | ≤ 4 GB | thread-per-conn (8 MiB stacks) |
| HTTP/1.1 plaintext, requests/s per core | ≥ 1M | tokio hand-written HTTP hello |
| HTTP/1.1 plaintext, p99 latency at 80% load | ≤ 1 ms | tokio |
| Echo, 64-byte messages, msgs/s per core | ≥ 2M | Go twine within 10% |
| allocs/op on read path, write path, timer fire | 0 | — |
| Timer insert + cancel, 1M active timers | O(1), ≤ 50 ns each | `BTreeMap` timers |

**Non-goals**

- Replacing `std::net`/tokio for ordinary services.
- Windows support.
- HTTP/2, HTTP/3, WebSocket in v1.
- `twisted.web` resource trees, application framework, plugin system.
- An async executor on the loop. Blocking code uses `defer_to_thread`; off-loop async code
  awaits an `Awaitable<T>` (which implements `std::future::Future`).

---

## 3. Twisted → Twine (Rust) concept map

| Twisted | Twine (Rust) | Notes |
| --- | --- | --- |
| `reactor` (global) | `Engine` + N `Loop`s | No global. An Engine owns loops; a Transport knows its Loop. |
| `reactor.run()` | `engine.run()` | Blocks until `stop()`. |
| `reactor.callLater(d, f)` | `loop.call_later(d, f) -> TimerId`; `transport.call_later(d, token)` | Timing wheel, O(1) insert/cancel. |
| `reactor.callFromThread(f)` | `loop_handle.call_from_thread(f)` | Lock-free MPSC queue + coalesced eventfd wakeup. |
| `reactor.listenTCP(port, factory)` | `engine.listen_tcp(addr, factory, opts)` | One listener per loop via SO_REUSEPORT. |
| `reactor.connectTCP(host, port, factory)` | `loop.connect_tcp(addr, builder, opts) -> Deferred<ConnId>` | Non-blocking connect, completes on EPOLLOUT. |
| `reactor.listenUDP(port, proto)` | `engine.listen_udp(addr, builder, opts)` | `recvmmsg`/`sendmmsg` batching. |
| `Protocol` | `trait Protocol` | `connection_made`, `data_received`, `connection_lost`; the transport is a parameter. |
| `Factory` / `ServerFactory` | `trait Factory` | `build_protocol(&self, loop, peer) -> Box<dyn Protocol>`. |
| `ReconnectingClientFactory` | `twine_proto::ReconnectingClient` | Exponential backoff with jitter. |
| `ITransport` | `Transport<'_>` (in callbacks), `TransportHandle` (stored / cross-thread) | write, writev, lose_connection, abort, peer, host. |
| `IPushProducer` / `IConsumer` | `Producer` enum / `PushProducer` trait; `Transport` is the consumer | Same pause/resume/stop contract. |
| `Deferred` | `Deferred<T>` | Typed. Callbacks run on the owning loop. |
| `Deferred.addCallback/addErrback` | `Deferred::then / catch` | `then` returns a new `Deferred<U>` (JS semantics; see Section 14). |
| `DeferredList` | `twine::all` / `twine::any` | — |
| `threads.deferToThread(f)` | `loop.defer_to_thread(f) -> Deferred<T>` | Runs `f` on a bounded worker pool. |
| `inlineCallbacks` / `async def` | `Awaitable<T>` (`wait()` or `.await` off-loop) | Blocking style, only off-loop. |
| `LineReceiver` | `twine_proto::Lines<H>` | Wrapper owning the line buffer; `H: LineHandler`. |
| `NetstringReceiver` | `twine_proto::Netstrings<H>` | — |
| `Int32StringReceiver` | `twine_proto::LengthPrefixed<H>` | Configurable 1/2/4-byte prefix and endianness. |
| `AMP` | `twine_proto::Amp` | Request/response multiplexing over length-prefixed frames. |
| `DatagramProtocol` | `trait DatagramProtocol` | `datagram_received(t, data, from)`. |
| `reactor.spawnProcess` | out of scope | Use `std::process` from a worker thread. |
| `twisted.internet.ssl` | `twine-tls` (Section 10) | rustls, sans-IO on the loop; optional kTLS. |
| `reactor.addSystemEventTrigger` | `engine.on_shutdown(f)` | — |

---

## 4. Architecture

Identical to the Go design: the Engine starts one Loop per core, each on its own OS thread,
owning its own file descriptors, buffers and timers. Nothing on the hot path is shared between
loops. Accepts enter through a per-loop SO_REUSEPORT listener. Blocking work leaves the loop
through `defer_to_thread` and comes back through `call_from_thread`, the only cross-thread
entry point into a loop.

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
│  │ index-linked wheel   │ │ index-linked wheel   │ │ index-linked wheel   │ │
│  │ MPSC queue + eventfd │ │ MPSC queue + eventfd │ │ MPSC queue + eventfd │ │
│  │ conns: Vec by fd     │ │ conns: Vec by fd     │ │ conns: Vec by fd     │ │
│  │ shared 64 KB rd buf  │ │ shared 64 KB rd buf  │ │ shared 64 KB rd buf  │ │
│  └──────────────────────┘ └──────────────────────┘ └──────────────────────┘ │
└──────────────────────────────┬───────────▲───────────────────────────────┘
      defer_to_thread(f) → Deferred │           │ call_from_thread / Resolver settles it
                        ┌─────────▼───────────┴─────────┐
                        │ Bounded worker pool (threads)  │
                        │ DB, disk, CPU work             │
                        └─────────────────────────────────┘
```

**Components**

- **Engine**: owns loops, listeners, the worker pool and lifecycle (`run`, `stop`,
  `on_shutdown`). Picks the loop for an outbound connection round-robin.
- **Loop**: the reactor. One OS thread. Iteration: drain the task queue, fire expired timers,
  poll (short spin, then block), dispatch I/O, drain the post-callback queue, flush pending
  writes, recycle closed connections, repeat.
- **Poller**: `twine-poll`, edge-triggered epoll (Linux) / kqueue (BSD). io_uring readiness
  backend is an optional feature (Phase 6).
- **ConnSlot**: per-connection state indexed by fd: the protocol box, outbound ring, flags,
  idle-timer id, generation. Slots are recycled on close.
- **Timing wheel**: `twine-wheel`, per-loop hierarchical wheel (4 × 256, 1 ms tick) with
  index-linked nodes; used by `call_later`, connection token timers and idle timeouts.
- **Task queue**: `twine-mpsc`, per-loop bounded lock-free MPSC ring for `call_from_thread`,
  with a mutex-guarded overflow vector and a coalesced eventfd wake.
- **Post-callback queue**: per-loop `VecDeque` of pending user-code invocations (producer
  notifications, same-loop handle ops, settled Deferreds, `call_soon`), drained after each
  top-level callback so user code never runs re-entrantly inside user code (Section 8 rule 2).
- **Worker pool**: bounded thread pool (default `4 × cpus`) backing `defer_to_thread`.
  Overflow blocks the calling thread, never a loop.

**Loop iteration, in order**

1. Drain the MPSC task queue (bounded to 1024 tasks per iteration), then the post-callback queue.
2. Advance the timing wheel to `now`; pop and fire expired timers (each followed by a
   post-callback drain).
3. Spin-poll with timeout 0 up to `spin_iterations` times (default 50); then block in
   `epoll_wait` with timeout = time to next timer (or forever).
4. For each ready fd: READABLE → read until EAGAIN into the loop's shared buffer, calling
   `data_received` per chunk; WRITABLE → mark dirty; HANGUP/ERROR → fold into the read path
   / `connection_lost`.
5. Flush every conn marked dirty this iteration with one `writev`.
6. Recycle closed conns (the only place a slot's generation changes).

---

## 5. Crate layout

```
rust/
  Cargo.toml                      workspace
  design.md  README.md  BENCHMARKS.md
  docs/INTERNALS.md  docs/STATUS.md  docs/API.md
  scripts/ci-local.sh  ci.yml

  twine/                          core crate `twine` (deps: libc, log)
    src/lib.rs
    src/errors.rs                 Error, PanicError, Result
    src/options.rs                Options, ListenOptions, DialOptions (builders)
    src/protocol.rs               Protocol, Factory, Producer, PushProducer, WriteFilter, DatagramProtocol
    src/event_loop.rs             Loop, LoopHandle, ConnId, LoopLocal, iteration, task queues
    src/conn.rs                   ConnSlot/ConnIo: flags, read/write dispatch, close/recycle
    src/transport.rs              Transport<'_>, TransportHandle, Payload, ConnOp
    src/timer.rs                  TimerKind, call_later plumbing, idle timers
    src/deferred.rs               Deferred<T>, Resolver, LocalResolver, Awaitable, all/any/sleep
    src/pool.rs                   worker pool, defer_to_thread
    src/engine.rs                 Engine, EngineHandle, Listener, listen_tcp, shutdown
    src/connect.rs                connect_tcp (non-blocking connect)
    src/udp.rs                    listen_udp, DatagramTransport, batched recv/send
    src/blocking.rs               thread-per-connection escape hatch
    src/metrics.rs                LoopMetrics (atomics), MetricsSnapshot, histogram
    src/prometheus.rs             text exposition
    src/watchdog.rs               slow-callback detector thread
    tests/                        loopback integration tests and allocation gates
    internal/poll                 crate twine-poll   (epoll, kqueue, eventfd wake; feature `uring`)
    internal/sock                 crate twine-sock   (nonblocking sockets, sockopts, recvmmsg/sendmmsg)
    internal/slab                 crate twine-slab   (size-classed slabs 4K/16K/64K/256K)
    internal/ring                 crate twine-ring   (slab-chained byte FIFO, iovec export)
    internal/mpsc                 crate twine-mpsc   (Vyukov bounded MPSC)
    internal/wheel                crate twine-wheel  (index-linked hierarchical wheel)
    internal/alloccount           crate twine-alloccount (counting allocator for tests)

  twine-proto/                    Lines, Netstrings, LengthPrefixed, Amp, ReconnectingClient, Broadcast
  twine-http/                     zero-alloc HTTP/1.1 server (parser, RequestCtx, Server)
  twine-tls/                      rustls wrapper protocol, listen/connect helpers, kTLS offload
  examples/                       bins: echo, chat, http_hello, udp_echo, client_reconnect
  bench/                          bins: echo_twine, echo_threads, echo_tokio, http_twine, http_tokio,
                                  loadgen (echo + HTTP, closed and open loop), timers, callfromthread
  fuzz/                           cargo-fuzz targets: line, netstring, lengthprefix, http_parse
```

**Dependency rules**

- `twine` depends on the `twine-*` internal crates, `libc` and `log` only. The internal
  crates depend on `std` and `libc` only (`twine-poll` on `io-uring` behind `uring`).
- `twine-proto`, `twine-http`, `twine-tls` depend on `twine`, never on the internal crates.
- `tokio` appears only in `bench/`. `rustls` (+ `rcgen` for test certificates) only in `twine-tls`.

---

## 6. Core public API

This is the contract. Signatures may gain options but must not change shape between phases.

### 6.1 Protocol, Factory, Transport

```rust
use std::any::Any;
use std::net::SocketAddr;
use std::time::Duration;

/// Implemented by user code. All methods run on the owning loop's thread; the
/// transport is passed in, never stored (store a `TransportHandle` instead).
pub trait Protocol: AsAny + 'static {
    /// Called once, after the transport is ready.
    fn connection_made(&mut self, _t: &mut Transport<'_>) {}
    /// Called with a sub-slice of the loop's shared read buffer. `data` is valid
    /// only for the duration of the call (enforced by the borrow checker); it is
    /// mutable so parsers and TLS can transform it in place.
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]);
    /// Called exactly once. `reason` is None on a clean close. The transport is
    /// already closed: writes are counted no-ops; `id()`, `peer()`, `loop_mut()` work.
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<Error>) {}
    /// Producer hooks, used when this protocol is registered as `Producer::This`
    /// (or as `Producer::Conn(id)` of another connection).
    fn pause_producing(&mut self, _t: &mut Transport<'_>) {}
    fn resume_producing(&mut self, _t: &mut Transport<'_>) {}
    fn stop_producing(&mut self, _t: &mut Transport<'_>) {}
    /// A connection-scoped timer armed with `Transport::call_later(d, token)` fired.
    fn timer_fired(&mut self, _t: &mut Transport<'_>, _token: u64) {}
    /// Runs `f` against the innermost protocol with the view it normally sees.
    /// Wrapper protocols (TLS) override this to forward with a filtered view.
    fn visit(&mut self, t: &mut Transport<'_>, f: &mut dyn FnMut(&mut dyn Any, &mut Transport<'_>)) {
        f(self.as_any_mut(), t)
    }
    /// Name reported by the watchdog and metrics. Default: `std::any::type_name::<Self>()`.
    fn type_name(&self) -> &'static str { std::any::type_name::<Self>() }
}

/// Blanket-implemented downcast support for `Protocol`.
pub trait AsAny { fn as_any(&self) -> &dyn Any; fn as_any_mut(&mut self) -> &mut dyn Any; }
impl<T: Any> AsAny for T { /* ... */ }

/// Builds a Protocol per accepted or connected transport. Called on the owning
/// loop, so it may hand out per-loop state via `lp.local(key)`.
pub trait Factory: Send + Sync + 'static {
    fn build_protocol(&self, lp: &mut Loop, peer: SocketAddr) -> Box<dyn Protocol>;
}
impl<F> Factory for F where F: Fn(&mut Loop, SocketAddr) -> Box<dyn Protocol> + Send + Sync + 'static {}
impl Factory for std::sync::Arc<dyn Factory> {}
/// Pins a closure's signature so `factory(|_, _| Box::new(Echo))` infers.
pub fn factory<F>(f: F) -> F where F: Fn(&mut Loop, SocketAddr) -> Box<dyn Protocol> + Send + Sync + 'static;

/// Identifies a connection on one loop. Generation-checked: stale ids are inert.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ConnId { /* fd: u32, gen: u32, loop_id: u16 */ }
impl ConnId { pub fn loop_id(&self) -> usize; }

/// The loop-affine view of a connection, built on the stack for the duration of
/// one callback. `!Send`; cannot outlive the callback. Also obtainable for any
/// other connection on the same loop via `Loop::conn(id)`.
pub struct Transport<'a> { /* lp: &'a mut Loop, id: ConnId, filter: Option<&'a mut dyn WriteFilter>, foreign: bool */ }

impl<'a> Transport<'a> {
    /// Copies to the outbound ring only if an immediate write(2) would block.
    /// The caller may reuse `b` as soon as `write` returns.
    pub fn write(&mut self, b: &[u8]);
    /// Writes several slices with one writev(2) when possible.
    pub fn writev(&mut self, bufs: &[&[u8]]);
    /// Half-close after flushing pending writes (SHUT_WR, then close when the
    /// peer closes or after `Options::close_linger`).
    pub fn lose_connection(&mut self);
    /// Immediate close; pending writes discarded; `connection_lost(None)`.
    pub fn abort(&mut self);
    pub fn peer(&self) -> SocketAddr;
    pub fn host(&self) -> SocketAddr;
    pub fn id(&self) -> ConnId;
    /// A cheap `Send + Sync` handle for storing or cross-thread use.
    pub fn handle(&self) -> TransportHandle;
    pub fn loop_mut(&mut self) -> &mut Loop;
    pub fn loop_handle(&self) -> &LoopHandle;
    pub fn now(&self) -> std::time::Instant;
    /// Arms an idle timer on the timing wheel; None disables. Any read or write
    /// resets it. Expiry aborts with `Error::IdleTimeout`.
    pub fn set_idle_timeout(&mut self, d: Option<Duration>);
    /// Connection-scoped timer: `Protocol::timer_fired(t, token)` after `d`.
    /// No closure, no allocation; inert once the connection closes. ZERO = next iteration.
    pub fn call_later(&mut self, d: Duration, token: u64) -> TimerId;
    pub fn cancel_timer(&mut self, t: TimerId) -> bool;

    // Backpressure (Twisted IPushProducer/IConsumer):
    /// Drops read interest; `data_received` stops being called.
    pub fn pause_reading(&mut self);
    /// Restores read interest and performs one immediate read so an edge missed
    /// while paused is not lost.
    pub fn resume_reading(&mut self);
    /// Attaches a producer. A streaming (push) producer is paused above
    /// `write_high_water` and resumed below `write_low_water`; a non-streaming
    /// (pull) producer gets `resume_producing` whenever the ring drains, and once
    /// immediately. Panics if a producer is already registered.
    pub fn register_producer(&mut self, p: Producer, streaming: bool);
    pub fn unregister_producer(&mut self);
    /// Bytes buffered in the outbound ring.
    pub fn pending_write(&self) -> usize;
    /// `pending_write() > write_high_water`: the synchronous check (producer
    /// notifications themselves are delivered after the current callback).
    pub fn write_blocked(&self) -> bool;
    pub fn is_open(&self) -> bool;
    pub fn is_closing(&self) -> bool;
    pub fn set_nodelay(&mut self, on: bool) -> Result<()>;
    /// The socket fd while open, for setsockopt-style use only (kTLS).
    pub fn fd(&self) -> Option<std::os::fd::RawFd>;

    // Wrapper support (TLS):
    /// Marks this connection as filtered: writes from other connections' views and
    /// from handles are routed through the protocol's `visit` so the wrapper's
    /// `WriteFilter` sees them.
    pub fn set_filtered(&mut self, on: bool);
    pub fn reborrow(&mut self) -> Transport<'_>;
    pub fn with_filter<'b>(&'b mut self, f: &'b mut dyn WriteFilter) -> Transport<'b>;
}

/// Who gets pause/resume/stop notifications for a connection's outbound ring.
pub enum Producer {
    /// This connection's own protocol (`Protocol::{pause,resume,stop}_producing`).
    This,
    /// Another connection's protocol on the same loop, called with *its* transport.
    Conn(ConnId),
    /// Built in: pause/resume reading of that connection (proxy pattern), no user code.
    PauseReading(ConnId),
    /// A standalone producer (file sender, ...).
    Boxed(Box<dyn PushProducer>),
}
pub trait PushProducer: 'static {
    fn pause_producing(&mut self, consumer: &mut Transport<'_>);
    fn resume_producing(&mut self, consumer: &mut Transport<'_>);
    fn stop_producing(&mut self, consumer: &mut Transport<'_>);
}

/// Implemented by wrapper protocols (TLS) to transform outbound bytes.
pub trait WriteFilter: 'static {
    fn write(&mut self, raw: &mut Transport<'_>, plain: &[u8]);
    fn writev(&mut self, raw: &mut Transport<'_>, bufs: &[&[u8]]) { for b in bufs { self.write(raw, b) } }
    fn lose_connection(&mut self, raw: &mut Transport<'_>) { raw.lose_connection() }
    fn pending_write(&self, raw: &Transport<'_>) -> usize { raw.pending_write() }
}

/// Stored / cross-thread handle. `Clone + Send + Sync`. Every operation is queued
/// to the owning loop (same-loop: the post-callback queue; other threads: the MPSC
/// queue) and enters through `Protocol::visit`. Generation-checked: inert after close.
#[derive(Clone)]
pub struct TransportHandle { /* lp: LoopHandle, id: ConnId */ }
impl TransportHandle {
    pub fn write(&self, data: impl Into<Payload>);
    pub fn lose_connection(&self);
    pub fn abort(&self);
    pub fn call(&self, f: impl FnOnce(&mut Transport<'_>) + Send + 'static);
    pub fn call_proto<P: Protocol>(&self, f: impl FnOnce(&mut P, &mut Transport<'_>) + Send + 'static);
    pub fn id(&self) -> ConnId;
    pub fn loop_handle(&self) -> &LoopHandle;
}
pub enum Payload { Owned(Vec<u8>), Shared(std::sync::Arc<[u8]>), Static(&'static [u8]) }
```

### 6.2 Engine and Loop

```rust
pub struct Engine { /* unexported */ }
impl Engine {
    pub fn new(opts: Options) -> Result<Engine>;
    pub fn builder() -> Options;                       // Options::build(self) -> Result<Engine>
    /// Starts the loops and blocks until `stop()`. Returns the error that
    /// prevented a loop from starting, otherwise Ok after a clean shutdown.
    pub fn run(&self) -> Result<()>;
    pub fn handle(&self) -> EngineHandle;
    // Convenience forwards of EngineHandle methods:
    pub fn listen_tcp(&self, addr: impl std::net::ToSocketAddrs, f: impl Factory, opts: ListenOptions) -> Result<Listener>;
    pub fn listen_udp(&self, addr: impl std::net::ToSocketAddrs,
        build: impl FnOnce(&mut Loop) -> Box<dyn DatagramProtocol> + Send + 'static, opts: ListenOptions) -> Result<Listener>;
    pub fn loop_handles(&self) -> Vec<LoopHandle>;
    pub fn next_loop(&self) -> LoopHandle;
    pub fn loop_local<T: 'static>(&self, init: impl Fn() -> T + Send + Sync + 'static) -> LoopLocal<T>;
    pub fn on_shutdown(&self, f: impl FnOnce() + Send + 'static);
    pub fn stop(&self);
    pub fn metrics(&self) -> Vec<MetricsSnapshot>;
}
/// `Clone + Send + Sync`; everything on `Engine` except `run`.
#[derive(Clone)] pub struct EngineHandle { /* Arc */ }
impl EngineHandle { /* same methods as above minus run/handle, plus */ pub fn is_stopped(&self) -> bool; pub fn wait(&self); }

/// A bound TCP or UDP socket. `close()` stops accepting; existing connections
/// are unaffected. Safe from any thread.
pub struct Listener { /* Arc */ }
impl Listener { pub fn addr(&self) -> SocketAddr; pub fn close(&self); }

/// The reactor. Lives on its thread; `!Send`. User code receives `&mut Loop` in
/// timer, task and Deferred callbacks and via `Transport::loop_mut()`.
pub struct Loop { /* unexported */ }
impl Loop {
    pub fn id(&self) -> usize;
    pub fn handle(&self) -> &LoopHandle;
    pub fn engine(&self) -> &EngineHandle;
    /// Cached at the start of the iteration; cheap; what timers fire against.
    pub fn now(&self) -> std::time::Instant;
    /// A view of another connection on this loop (None if closed/stale/foreign loop).
    pub fn conn(&mut self, id: ConnId) -> Option<Transport<'_>>;
    /// Runs `f` against the protocol of `id` (downcast to P) with its transport.
    /// Err(Busy) if that protocol is currently inside its own callback; Err(Closed) if gone.
    pub fn with_protocol<P: Protocol, R>(&mut self, id: ConnId, f: impl FnOnce(&mut P, &mut Transport<'_>) -> R) -> Result<R>;
    /// Runs `f` on this loop after `d`. ZERO runs on the next iteration (never re-entrantly).
    pub fn call_later(&mut self, d: Duration, f: impl FnOnce(&mut Loop) + 'static) -> TimerId;
    pub fn cancel_timer(&mut self, t: TimerId) -> bool;
    pub fn reset_timer(&mut self, t: TimerId, d: Duration) -> bool;
    /// Runs `f` after the current callback returns (post-callback queue).
    pub fn call_soon(&mut self, f: impl FnOnce(&mut Loop) + 'static);
    /// Per-loop user state (lazily created from the key's initializer).
    pub fn local<T: 'static>(&mut self, key: LoopLocal<T>) -> std::rc::Rc<T>;
    /// Non-blocking connect; resolves with the new connection's id after
    /// `connection_made`, rejects with ConnectTimeout / the socket error.
    pub fn connect_tcp(&mut self, addr: SocketAddr,
        build: impl FnOnce(&mut Loop, SocketAddr) -> Box<dyn Protocol> + 'static, opts: DialOptions) -> Deferred<ConnId>;
    pub fn deferred<T: Send + 'static>(&mut self) -> (Deferred<T>, Resolver<T>);
    pub fn deferred_local<T: 'static>(&mut self) -> (Deferred<T>, LocalResolver<T>);
    pub fn defer_to_thread<T: Send + 'static>(&mut self, f: impl FnOnce() -> Result<T> + Send + 'static) -> Deferred<T>;
    pub fn sleep(&mut self, d: Duration) -> Deferred<()>;
    pub fn metrics(&self) -> MetricsSnapshot;
    pub fn is_stopping(&self) -> bool;
}

/// `Clone + Send + Sync`. The one door in.
#[derive(Clone)] pub struct LoopHandle { /* Arc<Remote> */ }
impl LoopHandle {
    pub fn id(&self) -> usize;
    /// True when called on this loop's thread (thread-local check).
    pub fn in_loop(&self) -> bool;
    /// Schedules `f` on the loop. Never blocks; overflow spills to a mutex-guarded
    /// vector. Wakeups are coalesced. Err(LoopStopped) after the loop has exited.
    pub fn call_from_thread(&self, f: impl FnOnce(&mut Loop) + Send + 'static) -> Result<()>;
    /// Runs `f` on the loop and returns its result to the caller.
    pub fn call<T: Send + 'static>(&self, f: impl FnOnce(&mut Loop) -> T + Send + 'static) -> Awaitable<T>;
    /// Runs `f` on the loop and forwards the Deferred it returns.
    pub fn submit<T: Send + 'static>(&self, f: impl FnOnce(&mut Loop) -> Deferred<T> + Send + 'static) -> Awaitable<T>;
    pub fn connect_tcp(&self, addr: SocketAddr, f: impl Factory, opts: DialOptions) -> Awaitable<TransportHandle>;
    pub fn metrics(&self) -> MetricsSnapshot;
    pub fn is_stopped(&self) -> bool;
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)] pub struct TimerId { /* idx, gen */ }
#[derive(Clone, Copy)] pub struct LoopLocal<T: 'static> { /* idx, PhantomData<fn() -> T> */ }   // Send + Sync key
```

Options are builders (`options.rs`): `Options::default().loops(n).reuse_port(b).spin_iterations(n)
.worker_pool(n).read_buffer_size(n).write_high_water(n).write_low_water(n).write_buffer_limit(n)
.watchdog(Option<Duration>).max_reads_per_event(n).max_accepts_per_event(n).task_queue_size(n)
.shutdown_timeout(d).pin_loops(cpus).close_linger(d).busy_poll(Option<d>).udp_batch(n)`;
`ListenOptions::default().no_delay(b).defer_accept(Option<d>).backlog(n).socket_buffers(r, s)`;
`DialOptions::default().timeout(Option<d>).no_delay(b).local_addr(Option<addr>)`. Defaults as in
the Go design (`loops` = available CPUs, spin 50 but auto 0 when `loops > cpus/2`, etc.).

### 6.3 Deferred (the Deferred replacement)

```rust
/// A single-assignment result whose callbacks run on its loop. `!Send`, tied to
/// the loop that created it. One consumer: `then` and friends take `self`.
#[must_use] pub struct Deferred<T: 'static> { /* Rc<Cell> */ }
impl<T: 'static> Deferred<T> {
    pub fn resolved(lp: &mut Loop, v: T) -> Deferred<T>;
    pub fn rejected(lp: &mut Loop, e: Error) -> Deferred<T>;
    /// Transform the value on the loop; a panic rejects the result with Error::Panic.
    pub fn then<U: 'static>(self, f: impl FnOnce(&mut Loop, T) -> U + 'static) -> Deferred<U>;
    pub fn try_then<U: 'static>(self, f: impl FnOnce(&mut Loop, T) -> Result<U> + 'static) -> Deferred<U>;
    pub fn and_then<U: 'static>(self, f: impl FnOnce(&mut Loop, T) -> Deferred<U> + 'static) -> Deferred<U>;
    /// Handle (and possibly recover from) a rejection.
    pub fn catch(self, f: impl FnOnce(&mut Loop, Error) -> Result<T> + 'static) -> Deferred<T>;
    /// Terminal: observe the outcome.
    pub fn finally(self, f: impl FnOnce(&mut Loop, Result<T>) + 'static);
    /// Terminal: deliver the outcome to a connection's protocol (via `with_protocol`);
    /// dropped silently if the connection is gone.
    pub fn on_conn<P: Protocol>(self, id: ConnId, f: impl FnOnce(&mut P, &mut Transport<'_>, Result<T>) + 'static);
    /// Hand the outcome to another thread.
    pub fn awaitable(self) -> Awaitable<T> where T: Send;
    pub fn is_done(&self) -> bool;
}
/// Settles a Deferred from any thread. `Clone + Send + Sync`. Idempotent.
/// Dropping the last Resolver without settling rejects with Error::Abandoned.
pub struct Resolver<T: Send + 'static> {}
impl<T: Send + 'static> Resolver<T> { pub fn resolve(&self, v: T); pub fn reject(&self, e: Error); }
/// Same, loop-thread only (no Send requirement on T).
pub struct LocalResolver<T: 'static> {}
impl<T: 'static> LocalResolver<T> { pub fn resolve(self, v: T); pub fn reject(self, e: Error); }

/// Off-loop side of a Deferred. `Send`. Implements `std::future::Future<Output = Result<T>>`.
pub struct Awaitable<T> {}
impl<T> Awaitable<T> {
    /// Blocks the calling thread. Panics with Error::AwaitOnLoop on any loop thread.
    pub fn wait(self) -> Result<T>;
    pub fn wait_timeout(self, d: Duration) -> std::result::Result<Result<T>, Awaitable<T>>;
    pub fn try_take(&self) -> Option<Result<T>>;
}

/// Resolves with every value in input order when every input resolves; rejects on the first rejection.
pub fn all<T: 'static>(lp: &mut Loop, ds: Vec<Deferred<T>>) -> Deferred<Vec<T>>;
/// Resolves with the first value; rejects only if every input rejects (with the last error).
pub fn any<T: 'static>(lp: &mut Loop, ds: Vec<Deferred<T>>) -> Deferred<T>;
```

**Deferred semantics**

- Callbacks never run inside `resolve()`/`then()`: they are queued on the loop's post-callback
  queue and run after the current top-level callback (JavaScript promise ordering). This is
  what makes `on_conn` / `with_protocol` safe at callback time.
- Resolution from other threads goes through the loop's task queue (`Resolver` posts a
  Settle task); callbacks therefore never run concurrently with loop code.
- A callback that panics rejects the next Deferred in the chain with `Error::Panic`; the loop
  survives.
- A rejected Deferred dropped with no handler logs once at WARN with the creation backtrace
  when `TWINE_DEBUG=1`. A Resolver dropped without settling rejects with `Error::Abandoned`.
- `Awaitable::wait` on a loop thread panics with `Error::AwaitOnLoop`. Fail loud.

### 6.4 Minimal echo server (reference example)

```rust
struct Echo;
impl Protocol for Echo {
    fn connection_made(&mut self, t: &mut Transport<'_>) { t.set_idle_timeout(Some(Duration::from_secs(30))); }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) { t.write(data); } // copies only if it must buffer
}
fn main() -> twine::Result<()> {
    let eng = Engine::builder().loops(0).build()?;                  // 0 = available CPUs
    eng.listen_tcp("0.0.0.0:9000", twine::factory(|_, _| Box::new(Echo)), ListenOptions::default())?;
    eng.run()
}
```

### 6.5 UDP

```rust
pub trait DatagramProtocol: 'static {
    fn start_protocol(&mut self, _t: &mut DatagramTransport<'_>) {}
    /// `data` is valid only for the call.
    fn datagram_received(&mut self, t: &mut DatagramTransport<'_>, data: &mut [u8], from: SocketAddr);
    fn stop_protocol(&mut self) {}
}
pub struct DatagramTransport<'a> {}
impl<'a> DatagramTransport<'a> {
    /// Buffered and flushed with sendmmsg at the end of the iteration (Linux); sendto elsewhere.
    pub fn write_to(&mut self, b: &[u8], to: SocketAddr);
    pub fn host(&self) -> SocketAddr;
    pub fn loop_mut(&mut self) -> &mut Loop;
    pub fn close(&mut self);
}
```

### 6.6 Blocking escape hatch

```rust
/// Each connection's handler runs on its own thread with a blocking stream, at
/// thread-per-connection cost. The loop side copies chunks into a bounded channel
/// (64 chunks) and pauses reading when it is full.
pub fn blocking<F>(f: F) -> impl Factory where F: Fn(BlockingConn) + Send + Sync + 'static;
pub struct BlockingConn {}   // io::Read + io::Write + peer()/host()/handle()/lose_connection()/abort()
```

---

## 7. Hot-path design

### 7.1 Read path

- One read buffer per **loop** (default 64 KiB), stored beside the `Loop` in the loop thread's
  struct so `&mut [u8]` and `&mut Loop` coexist. `data_received` gets a sub-slice; protocols
  that need to retain bytes copy them (keeping the slice is a compile error).
- Edge-triggered: on READABLE, `read(2)` until EAGAIN or a short read, calling `data_received`
  after each read, then draining the post-callback queue. Cap at `max_reads_per_event`
  (default 8) per fd per iteration; re-arm by pushing the id to a ready list for next iteration.
- `pause_reading` removes READABLE with `EPOLL_CTL_MOD`; `resume_reading` adds it back and
  queues one immediate read because edge-triggered edges may have been missed.
- The protocol box is moved out of its slot (`Option::take`) for the duration of its callback
  and put back after. A 16-byte move, no allocation, no `unsafe`.

### 7.2 Write path

1. `write(b)` first checks the ring is empty (and no flush is in progress). If so, `write(2)`
   directly. If everything was written, return. Zero copies.
2. Partial write or EAGAIN: copy the remainder into the per-conn ring, mark the conn dirty, arm
   WRITABLE.
3. If the ring is non-empty, append only; the flush step at the end of the iteration issues one
   `writev` per dirty conn.
4. When the ring drains, disarm WRITABLE. Queue `resume_producing` when pending drops below
   `write_low_water`; `pause_producing` when it exceeds `write_high_water` (delivered after the
   current callback; `write_blocked()` is the synchronous check).
5. Ring storage comes from the loop's slab allocator (4K/16K/64K/256K). A ring grows by chaining
   slabs, never by reallocation. Slabs are returned on close.

### 7.3 Allocation and `unsafe` rules

- **Zero heap allocations** per read, per write, per timer fire, per `call_from_thread` drain
  on the steady-state path. Enforced by tests with the `twine-alloccount` counting allocator
  which fail on any non-zero result.
- Connections live in a `Vec<ConnSlot>` indexed by fd; closed slots are reset in place (gen+1)
  and reused by the next accept on that fd. No map lookups on the hot path.
- **Indices, never pointers**: dirty/ready/closing lists hold `u32`/`ConnId`; the wheel links
  nodes by index. Arenas may move when they grow.
- No closures allocated per event. Connection timers use `u64` tokens; `Box<dyn FnOnce>` of a
  capture-free closure does not allocate; capturing closures do (documented).
- `Payload::Shared(Arc<[u8]>)` is the cross-loop broadcast currency: one copy, many loops.
- `unsafe` allow-list: libc FFI in `twine-poll`/`twine-sock`; sockaddr casts in `twine-sock`;
  MPSC slot storage; iovec construction in `twine-ring` and the core flush; `sched_setaffinity`;
  kTLS `setsockopt`; the io_uring backend. Every block carries `// SAFETY:`. The core dispatch,
  `Transport`, conn table, wheel, Deferred and slab/ring logic contain none.

### 7.4 Timers

- Hierarchical timing wheel per loop (`twine-wheel`): 4 levels × 256 slots, 1 ms tick. Insert
  and cancel O(1); cascade every 256 ticks.
- Nodes are index-linked in an arena with a free list and generation; `TimerId { idx, gen }`.
- Idle timeouts use a built-in node per connection (`TimerKind::Idle`), re-linked on every read
  or write; the same node is the half-close linger deadline.
- `advance(now)` moves expired timers to a due list; the loop pops and fires them one at a
  time, so a callback may add/remove/reset timers. A timer never fires early and at most one
  tick late. `call_later(ZERO)` runs on the next iteration.

### 7.5 Cross-thread calls

- `call_from_thread` pushes a boxed task to a bounded lock-free MPSC ring (default 64K). On
  overflow it falls back to a mutex-guarded vector so it never drops or blocks.
- Wakeup is coalesced: the producer CASes a `wake_pending` flag and writes the eventfd only if
  it won. The loop clears the flag before draining.
- A `TransportHandle` op from the loop's own thread goes to the thread-local post-callback
  queue: no atomics, no syscall. Hot same-loop access uses `lp.conn(id)` directly.

### 7.6 Syscalls and socket options

- All socket I/O goes through `twine-sock` (`libc`): `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)`,
  `read`, `write`, `writev`, `epoll_wait`. `std::net` is used only to parse addresses.
- Listeners: `SO_REUSEADDR`, `SO_REUSEPORT`, `TCP_NODELAY` on accepted sockets,
  `TCP_DEFER_ACCEPT` when configured.
- `accept4` loops until EAGAIN, capped at `max_accepts_per_event` (default 64).
- `epoll_wait` fills a fixed `[Event; 256]` in the Poller; no allocation.
- Spin-polling (`spin_iterations`) is auto-disabled when `loops > cpus/2`.

### 7.7 Memory

- Slabs are allocated once per loop and reused. `MetricsSnapshot` exposes slab utilization and
  ring high-water marks.
- No GC: the per-connection footprint is the slot (≈ 200 B), the protocol box, and a slab only
  while writes are pending.

---

## 8. Concurrency and ownership rules

A connection belongs to exactly one loop for its entire life. Everything else follows.

1. **Loop affinity is enforced by the compiler.** `Loop` and `Transport<'_>` are `!Send` and
   `Transport` cannot outlive its callback. Only `LoopHandle`, `TransportHandle`, `Resolver`
   and `Awaitable` cross threads.
2. **User code never runs inside user code.** Producer notifications, Deferred callbacks,
   handle ops and `call_soon` go to the post-callback queue, drained right after the current
   top-level callback. Core API methods never call user code synchronously, so a panic can
   only start at an API boundary and core state is consistent when `catch_unwind` catches it.
3. **The one door in.** `LoopHandle::call_from_thread` is the only way another thread touches
   a loop. `Resolver` and `TransportHandle` use it internally.
4. **The one door out.** `defer_to_thread` is the only sanctioned way to run blocking work.
   Protocol code that blocks stalls every connection on that loop; the watchdog reports it.
5. **Cross-connection access.** Same loop: `lp.conn(id)` or `lp.with_protocol(id, ..)`
   (synchronous). Other loops: `TransportHandle` / `call_from_thread`. `twine_proto::Broadcast`
   wraps the fan-out pattern (one `Arc<[u8]>`, one task per loop).
6. **Buffer lifetime.** The slice passed to `data_received` is dead when the method returns
   (compile-time). `write(b)` may retain a copy of `b` but never `b` itself.
7. **Shutdown order.** `stop` closes listeners, runs `on_shutdown` hooks once on the `run`
   thread, then on each loop: `lose_connection` on every conn, flush rings for up to
   `shutdown_timeout` (default 5 s), then close fds and exit the thread. `run` returns after
   all loops exit.
8. **Panics.** A panic in protocol/producer/timer/Deferred/task code is caught with
   `catch_unwind`; for a connection callback the connection is aborted with
   `connection_lost(Some(Error::Panic))` and the loop continues. A panic in loop-internal code
   is fatal. Requires `panic = "unwind"` (every profile).
9. **Slot recycling** happens only in the end-of-iteration recycle step, which needs
   `&mut Loop` — so while any `Transport<'_>` exists, the slot it names cannot be reused.
10. **Close order:** `EPOLL_CTL_DEL` → `stop_producing` → `connection_lost` (protocol moved out)
    → drop the protocol (guarded) → `close(fd)` → bump generation and reset the slot. (Go closed
    the fd first; here the slot is fd-indexed and holds the protocol, so the fd must stay
    reserved until the protocol is gone.)
11. **Thread-per-connection escape hatch.** `twine::blocking(f)` runs each connection's handler
    on its own thread with a blocking `BlockingConn` stream (Section 6.6).

---

## 9. Protocol library (`twine-proto`, `twine-http`)

Framing protocols are wrappers that own the reassembly buffer and a user handler (the Go
"embeddable struct with a callback field" pattern does not survive the borrow checker).
Parsers operate directly on the loop buffer and copy only incomplete trailing bytes.

| Type | Handler trait / callback | Behaviour |
| --- | --- | --- |
| `Lines<H: LineHandler>` | `line_received(&mut self, t, line: &[u8])` | Delimiter configurable (default `\r\n`), `max_line_length` (default 16 KiB) aborts with `Error::LineTooLong`. `set_raw_mode()` → `raw_data_received`. `send_line(t, line)`. |
| `Netstrings<H: NetstringHandler>` | `string_received(t, s)` | `len:data,` framing, `max_length` default 1 MiB. |
| `LengthPrefixed<H: FrameHandler>` | `frame_received(t, f)` | 1/2/4-byte prefix, big or little endian, optional length-includes-prefix. `send_frame(t, b)` uses one `writev`. |
| `Amp` | `AmpHandler::handle(cmd, args, t) -> Deferred<Args>` | Request/response with 32-bit ask ids over `LengthPrefixed` boxes, wire-compatible with Twisted. `call(cmd, args) -> Deferred<Args>`. |
| `ReconnectingClient` | wraps a `Factory` | Exponential backoff (initial 1 s, factor 1.6, max 60 s, full jitter). `reset_delay()`, `stop_trying()`, `OnStateChange`. |
| `Broadcast` | — | Loop-aware fan-out: members grouped by loop; `send(Arc<[u8]>)` delivers one task per loop. |
| `DatagramProtocol` | `datagram_received(t, data, from)` | `recvmmsg` batch 64, `sendmmsg` at iteration end. |
| `twine_http::Server<H: Handler>` | `handle(&self, ctx: &mut RequestCtx<'_>)` | HTTP/1.1 keep-alive and pipelining. Zero allocations per request: headers parsed into a fixed-capacity array of offset pairs into the loop buffer (or the per-conn reassembly slab); response appended to a per-conn `Vec<u8>` reused across requests then written once. |

**HTTP parser rules** (modelled on fasthttp / the Go port)

- Hand-written state machine, no regex, no `String` allocations.
- Header lookup is case-insensitive via `eq_ignore_ascii_case`; no map.
- `Content-Length` bodies delivered once complete; chunked bodies decoded into a per-conn buffer.
- Limits: `max_header_bytes` 8 KiB, `max_body_bytes` 4 MiB, `max_pipelined_requests` 16;
  exceeding any aborts with 431/413.
- `Date` header cached per loop (`LoopLocal`) and regenerated once per second via `call_later`.

**Out of scope for v1**: WebSocket, HTTP client, TLS-terminated HTTP (composes, but untested).

---

## 10. TLS strategy (`twine-tls`)

rustls is sans-IO, which fits a non-blocking loop directly: no memory-BIO shim is needed.

| Mode | How | Cost | When |
| --- | --- | --- | --- |
| **rustls on the loop (default)** | `TlsProtocol<Inner>` wraps the user protocol: `data_received` feeds ciphertext via `read_tls`, `process_new_packets`, drains plaintext to the inner protocol through a filtered `Transport`; the inner protocol's writes pass through a `WriteFilter` that encrypts (`writer().write` + `write_tls`) into the raw transport. The handshake runs on the loop (CPU only, ~100 µs; no worker thread). | One extra copy per direction; rustls allocates per record in the buffered API | Default |
| **kTLS offload** | After the handshake, `dangerous_extract_secrets()` → `setsockopt(SOL_TLS, TLS_TX/TLS_RX)`; the loop then does plain `read`/`write` and clears the filter. Linux 5.x+ with the `tls` module; AES-GCM / ChaCha20-Poly1305. | Handshake cost only | Opt-in `KernelOffload(true)` with capability probe and silent fallback |
| **Thread adapter** | `twine::blocking` + `rustls::StreamOwned` | Thread-per-conn | Migration path |

- `twine_tls::listen(engine, addr, Arc<ServerConfig>, inner_factory, opts)` and
  `twine_tls::connect(loop, addr, Arc<ClientConfig>, server_name, build, opts)`.
- ALPN, SNI cert selection and session resumption come from rustls config.
- Phase 5 implements the rustls wrapper; kTLS is Phase 6 with a runtime probe.

---

## 11. Errors, observability and debugging

**Error values** (`errors.rs`, one enum)

| Variant | Raised when |
| --- | --- |
| `Closed` | `write` on a closed transport (no-op, counted in metrics) |
| `LoopStopped` | `call_from_thread` after the loop has exited |
| `WriteBufferFull` | pending writes exceed `write_buffer_limit` (default 16 MiB); the conn is aborted |
| `AwaitOnLoop` | `Awaitable::wait` on a loop thread (panic) |
| `IdleTimeout` | idle timer fired |
| `ConnectTimeout`, `ConnectionReset`, `Shutdown`, `EngineStopped` | as named |
| `Abandoned` | a Resolver was dropped without settling |
| `Busy` | `with_protocol` on a protocol inside its own callback |
| `Panic(PanicError)` | protocol or callback panicked |
| `LineTooLong`, `FrameTooLarge` | framing limits exceeded |
| `Io(io::Error)`, `Addr(String)` | everything else |

**Watchdog.** Each loop records a monotonic timestamp before dispatching a callback. A separate
thread samples every loop; if one exceeds `watchdog` (default 1 ms) it logs the loop id, the
protocol's `type_name`, and the elapsed time, rate-limited to once per second per loop
(substitution: no cross-thread stack dump).

**Metrics** (`LoopMetrics`, atomics updated on the loop, read from anywhere): connections,
accepts, closes, reads, read_bytes, write_bytes, writev, write_direct_hit, write_after_close,
timers_fired, tasks_run, wakeups, wakeups_coalesced, iterations, poll_block_nanos, panics,
backlog_drops, accept_errors, datagrams, datagrams_sent, slow_callbacks; iteration duration
p50/p99/max over the last 10 s; slab_in_use, slab_free, ring_high_water. Exposed via
`engine.metrics()` and `twine::prometheus::render(&snapshots)`.

**Debug mode** (`TWINE_DEBUG=1`): `Deferred` creation backtraces, unhandled-rejection and
abandoned-resolver warnings with that backtrace, panic backtraces in `PanicError`. Loop affinity
and buffer lifetime need no debug checks (compile-time).

**Logging.** Twine uses the `log` facade. It never logs on the hot path; all logging is
rate-limited or event-driven (accept errors, watchdog, shutdown).

---

## 12. Testing and benchmarking

**Unit tests**: each internal crate ships property tests (in-crate xorshift PRNG) plus explicit
edge cases (wraparound, cascade boundaries, full queue) and alloc gates; `twine-mpsc` has a
64-producer × 1M stress (release, `--ignored`). `deferred.rs`: resolution ordering, panic
capture, `wait` on loop panics, `all`/`any` with mixed outcomes. Parsers: `cargo-fuzz` targets
for Lines, Netstrings, LengthPrefixed and the HTTP parser, run for 10 minutes with seed corpora.

**Integration tests** (real sockets on loopback, `twine/tests/`): echo round-trip across 1, 2 and
N loops; backpressure (slow reader triggers pause/resume exactly once per crossing, in order);
partial writes (tiny SO_SNDBUF, 100 MiB byte-exact); `lose_connection` vs `abort` half-close;
idle timeout within one tick; `call_from_thread` from 1000 threads while the loop is blocked,
wakeups ≤ calls; graceful shutdown flushes and calls `connection_lost` exactly once per conn;
protocol panic aborts only that connection; 10K-connection soak (`--ignored`).

**Allocation gates** (fail on regression): read dispatch, write hit path, write buffered path,
timer fire, `call_from_thread` push and drain, HTTP parse + respond.

**Benchmarks** (`bench/`, release): `echo_small` (1000 conns, 64 B ping-pong, msgs/s/core) vs
`echo_threads`, `echo_tokio` and the Go twine numbers in `../BENCHMARKS.md`; scaling 1→8 loops;
`http_plaintext` (c=256 and c=10000, with and without pipelining, p99 at 80% open-loop load) vs
`http_tokio`; `timers_1m`; `callfromthread` wakeups/op; idle RSS at 100K/1M connections.
`scripts/bench.sh` writes `bench/results/<git-sha>.txt`.

**Lints**: `cargo clippy --workspace --all-targets -- -D warnings`, `#![deny(missing_docs)]`
in every library crate, `cargo doc` warning-free, `cargo check --target x86_64-apple-darwin`
for the kqueue path, `cargo check --features uring`.

---

## 13. Implementation phases

### Phase 0: Scaffold — done (docs/STATUS.md)
- [x] Workspace, crate layout from Section 5, `errors.rs`, `options.rs`, counting allocator,
      `docs/INTERNALS.md`, this document, `README.md` with the Section 3 table
**Accept:** `cargo build --workspace` and `cargo clippy` pass on the skeleton.

### Phase 1: Single loop, epoll, echo — done (status and substitutions in docs/STATUS.md)
- [x] `twine-poll` (epoll + kqueue, eventfd), `twine-sock`
- [x] `event_loop.rs`, `conn.rs`, `transport.rs`: iteration order, shared read buffer, fd-indexed
      slots, direct-write-first, `protocol.rs`, `engine.rs` with `run`/`stop`/`listen_tcp`
- [x] `examples/echo`, `bench/echo_twine`, `bench/echo_threads`
**Accept:** echo integration test passes; read path 0 allocs/op.

### Phase 2: Multi-loop, write ring, slabs — done (status and substitutions in docs/STATUS.md)
- [x] `twine-slab`, `twine-ring`; ring-backed writes, WRITABLE arming, end-of-iteration `writev`,
      watermarks; `loops(n)` with one SO_REUSEPORT listener per loop; `Producer`,
      pause/resume reading; partial-write and backpressure tests
**Accept:** write hit and buffered paths 0 allocs; `echo_small` scales ≥ 0.8× linearly 1→8 loops
(or the miss is documented with its cause); 100 MiB partial-write byte-exact.

### Phase 3: Timers, cross-thread, Deferred — done (status and substitutions in docs/STATUS.md)
- [x] `twine-wheel`; `call_later`, token timers, idle timeouts; `twine-mpsc` + coalesced wakeup;
      `call_from_thread`; `in_loop`; `deferred.rs`; worker pool; `connect_tcp`; `examples/client_reconnect`
**Accept:** `timers_1m` ≤ 50 ns insert+cancel, 0 allocs; `callfromthread` wakeups/op ≤ 0.1
under 16 producers; Deferred tests pass; `wait` on loop panics with `AwaitOnLoop`.

### Phase 4: Protocol library and observability — done (status and substitutions in docs/STATUS.md)
- [x] `twine-proto`: Lines, Netstrings, LengthPrefixed, ReconnectingClient, Broadcast, Amp
- [x] UDP with recvmmsg/sendmmsg; watchdog, metrics, prometheus; graceful shutdown; panic
      recovery; `blocking` adapter; fuzz targets; `examples/chat`, `examples/udp_echo`
**Accept:** fuzzers run 10 minutes clean; shutdown test calls `connection_lost` exactly once per
conn; watchdog detects an injected 5 ms sleep.

### Phase 5: HTTP/1.1 and TLS — done (status and substitutions in docs/STATUS.md)
- [x] `twine-http`: parser, `RequestCtx`, response writer, keep-alive, pipelining, limits, cached Date
- [x] `twine-tls`: rustls wrapper, `listen`/`connect`; `bench/http_twine`, `bench/http_tokio`, `examples/http_hello`
**Accept:** HTTP parse+respond 0 allocs/op; `http_plaintext` ≥ 5× tokio baseline at c=256 (or
documented); TLS echo passes against `openssl s_client`.

### Phase 6: Platform extras and release — done (status and substitutions in docs/STATUS.md)
- [x] io_uring readiness backend behind `uring`; kTLS offload with probe and fallback;
      `SO_BUSY_POLL`/`TCP_DEFER_ACCEPT`; 100K/1M idle soak; `BENCHMARKS.md`; `docs/STATUS.md`;
      `docs/API.md`; rustdoc on every public item
**Accept:** every Section 2 target met or the miss documented with a cause; clippy/doc clean.

---

## 14. Decisions recorded for the Rust adaptation

- **Callbacks receive `&mut Transport<'_>`; nothing stores a borrow.** Go's stored
  `Transport` interface would need interior mutability and raw pointers into a growable slot
  table, and Go's synchronous `PauseProducing` call from inside `Write` would alias the
  protocol's `&mut self`. The view type costs one parameter and buys compile-time affinity.
- **Protocol moved out of its slot during its callback.** Lets the view borrow the whole loop
  (so other same-loop connections are reachable) with no `unsafe`; `with_protocol` on a
  protocol that is out returns `Error::Busy`.
- **Post-callback queue (rule 2).** Producer notifications and Deferred callbacks are delivered
  after the current callback instead of synchronously as in Go/Twisted. `write_blocked()`
  provides the synchronous check pipelining code needs.
- **`Deferred::then` returns a new `Deferred<U>`** (JS semantics) rather than Go's "same Future,
  `Map` to transform": `T` is moved into the callback, so a Deferred has one consumer.
  `Resolver` (Send) and `LocalResolver` split the settle side by thread.
- **Token timers for connections** (`timer_fired(t, token)`) replace closure timers that would
  capture a transport; zero allocation and inert after close.
- **Indices, never pointers** for conns and timers (arenas move on growth).
- **Close order** changed (rule 10) because the fd-indexed slot holds the protocol.
- **`SetContext`/`Context` dropped**: the protocol struct is the per-connection state.
- **`blocking` is an `io::Read + io::Write` stream** rather than a Protocol proxy: more useful
  for non-callback code and the only shape that type-checks.
- **TLS on the loop via rustls**, no handshake thread: rustls is sans-IO and the handshake is
  CPU-only. kTLS uses `dangerous_extract_secrets` instead of Go's reflection.
- **Watchdog logs without a stack dump** (no portable cross-thread backtrace); it reports loop,
  protocol `type_name` and elapsed time.
- **io_uring remains readiness-only** behind a feature, as in Go.
- **`in_loop` uses a thread-local**, not `gettid`.
- **Panic recovery requires `panic = "unwind"`**; the workspace sets no profile to abort and
  `Engine::new` logs a warning if `cfg!(panic = "abort")`.

**Decisions recorded during implementation** (2026-11-07)

- `Poller::{add, modify, delete}` take `&mut self`: the loop owns its poller exclusively,
  which lets the io_uring backend keep its ring state without a mutex.
- Listener fds are claimed with an atomic swap by whoever closes them (`ListenerInner::take_fd`),
  so a `Listener::close` racing a UDP socket's own close can never `close(2)` a reused fd
  number (found as an "IO Safety violation" abort in a parallel test run).
- `ConnId` carries the loop id and every slot lookup checks it; a stale id from another loop
  cannot alias a recycled fd.
- Connection token timers are tracked per slot and removed at close, so the wheel does not
  grow with churn; `MetricsSnapshot::timers_active` exposes the gauge.
- The post-callback queue is drained at most 1024 items per pass (`more_tasks` re-polls with
  timeout 0), so a self-requeueing `call_soon` cannot starve I/O.
- `drain_pending` and producer hooks run with `foreign = true` views on FILTERED connections,
  so boxed producers and cross-connection writes pass through a TLS wrapper's `WriteFilter`.
- The worker pool lives in the engine (`OnceLock`), created on first `defer_to_thread` and
  closed before the loops stop; `Awaitable::wait` detects loop threads through the same
  thread-local `in_loop` uses.
- `twine-sock::write`/`writev` use `send`/`sendmsg` with `MSG_NOSIGNAL` (falling back to
  `write`/`writev` on `ENOTSOCK`): a library cannot assume the binary ignores SIGPIPE.
- TLS: rustls caps its send buffer at 64 KiB, so the write filter loops partial writes and
  flushes between them; plaintext written before the handshake completes is buffered in order.
  A FIN without close_notify reaches the inner protocol as `UnexpectedEof`. kTLS: because
  `dangerous_extract_secrets` consumes the rustls connection, TX-only offload (clients, and
  servers that already hold part of the peer's next record) decrypts inbound records with a
  minimal TLS 1.3 record layer on `ring`; a kernel-RX peer alert or KeyUpdate surfaces as EIO.
- HTTP: chunked bodies are decoded incrementally and the consumed raw prefix dropped, so the
  per-connection buffer is bounded by `max_header_bytes + max_body_bytes` plus one chunk line;
  the `Date` header is regenerated lazily when the loop's cached clock crosses a second
  boundary instead of by a timer.
- **Sparse slot table.** The 1M-connection soak showed memory scaling with loops × highest fd
  (4.7 KB/conn at 8 loops): fds are process-global, so every loop's dense fd-indexed table of
  full `ConnSlot`s grew to the largest fd. The table is now `Vec<SlotEntry { gen, Option<Box<
  ConnSlot>> }>` (16 bytes per empty fd; the generation stays with the fd so stale ids remain
  inert across box reuse) with a per-loop free list capped at the loop's peak live count;
  `Ring::new()` no longer allocates. Result: 499 MB for 1M idle connections on 8 loops.
- **AMP arguments are bytes** (`Args = BTreeMap<String, Vec<u8>>`): Twisted's `amp.String` is
  binary; a box with an invalid key gets a `MALFORMED` error reply instead of killing the
  connection, and `max_pairs` (4096) bounds the per-box decoded size.
- **kTLS defaults to TX-only** (`TlsOptions::kernel_rx(false)`): with kernel RX a peer's
  close_notify or KeyUpdate is an EIO read error, so the clean-close path is kept in user
  space unless the application opts in.
- Fuzzing found one parser inconsistency (a chunked trailer rejected with 431 only when the
  final CRLF was split across reads); incremental and one-shot decoding are now tested to
  agree at every split point.
