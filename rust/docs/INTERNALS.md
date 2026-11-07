# Internal crate contracts (Rust)

This file is the contract between the `twine` core (`twine/src/*`) and the
internal crates under `twine/internal/*`. The core is written against these
signatures; a change here is a change to the core. The Go originals live in
`../internal/*` and `../docs/INTERNALS.md`; mirror their behaviour and tests,
adapted to Rust ownership.

Rules that apply to every internal crate:

- Dependencies: `std` and `libc` only (`twine-ring` may depend on `twine-slab`;
  `twine-poll` may depend on `io-uring` behind the `uring` feature).
- **Zero heap allocations on the steady-state path** (push/pop, add/remove,
  write/consume, wait, wake). Every crate ships a test using
  `twine_alloccount::allocs` / `allocs_per_run` that fails on a non-zero result
  for its hot operations. Declare the `#[global_allocator]` exactly once per
  test binary: for unit tests, one `#[cfg(test)] #[global_allocator] static ..`
  at the top of `lib.rs`; an integration test file is its own binary.
- Timing gates (wheel ≤ 50 ns, mpsc 64×1M stress) are `#[ignore]`d tests run
  with `cargo test -p <crate> --release -- --ignored`; debug-mode timings are
  meaningless and are not gated.
- Not thread-safe unless stated: each loop owns its own instance. Only
  `mpsc::Producer::push` and `poll::Waker::wake` may be called from other threads.
- `#![deny(missing_docs)]`, `#![deny(unsafe_op_in_unsafe_fn)]`, `cargo clippy
  -- -D warnings` clean. Every `unsafe` block has a `// SAFETY:` comment.
  Allowed `unsafe`: libc FFI in `poll` and `sock`, sockaddr casts in `sock`,
  the slot storage in `mpsc`, iovec construction in `ring`. Nothing else.
- Errors are `std::io::Error` (`ErrorKind::WouldBlock` for EAGAIN). The core
  maps them to `twine::Error`.
- Property tests use a small in-crate xorshift PRNG (no `rand` dependency) with
  a fixed seed, plus explicit edge-case tests (wraparound, cascade boundaries,
  full queue). Tests must pass under `cargo test` and compile on macOS/BSD
  (`cfg(target_os)` gates on Linux-only syscalls).
- Platforms: Linux (primary) and `target_os = "macos" | "freebsd" | "netbsd" |
  "openbsd" | "dragonfly"` for kqueue/BSD paths. Windows is out of scope.

---

## twine-poll

Platform abstraction over epoll (Linux) and kqueue (BSD/macOS), edge-triggered
on both (EPOLLET; EV_CLEAR), plus a cross-thread wakeup.

```rust
/// Event flags. Several may be set at once.
pub const READABLE: u32 = 1 << 0; // EPOLLIN | EPOLLPRI  / EVFILT_READ
pub const WRITABLE: u32 = 1 << 1; // EPOLLOUT            / EVFILT_WRITE
pub const HANGUP:   u32 = 1 << 2; // EPOLLHUP | EPOLLRDHUP / EV_EOF
pub const ERROR:    u32 = 1 << 3; // EPOLLERR            / EV_ERROR

/// One ready notification. `wait` fills a caller-provided slice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Event { pub fd: i32, pub flags: u32 }

/// Maximum number of events `wait` translates per call (the kernel event array
/// is a fixed `[libc::epoll_event; MAX_EVENTS]` inside the Poller, so wait does
/// not allocate).
pub const MAX_EVENTS: usize = 256;

/// One epoll/kqueue instance plus its wake fd. Owned by one loop thread.
pub struct Poller { /* epfd/kq, wake fd(s), kernel event array */ }

impl Poller {
    /// Creates the poll instance and the wake fd (eventfd on Linux, EVFILT_USER
    /// on kqueue, non-blocking pipe pair where EVFILT_USER is unavailable).
    pub fn new() -> io::Result<Poller>;
    /// Registers fd edge-triggered with the given interest (READABLE and/or
    /// WRITABLE). HANGUP and ERROR are always reported and need not be requested.
    pub fn add(&mut self, fd: i32, interest: u32) -> io::Result<()>;
    /// Replaces fd's interest set.
    pub fn modify(&mut self, fd: i32, interest: u32) -> io::Result<()>;
    /// Removes fd. The caller closes the fd itself, after `delete`.
    pub fn delete(&mut self, fd: i32) -> io::Result<()>;
    /// Blocks up to `timeout` (`None` = forever, `Some(ZERO)` = poll) and fills
    /// `events` (at most `min(events.len(), MAX_EVENTS)`). The wake fd is
    /// drained internally and NEVER reported. EINTR is retried internally and
    /// never returned. Does not allocate.
    pub fn wait(&mut self, events: &mut [Event], timeout: Option<Duration>) -> io::Result<usize>;
    /// Returns a handle other threads use to wake this poller.
    pub fn waker(&self) -> Waker;
}
impl Drop for Poller { /* closes poll fd and wake fds */ }

/// Cross-thread wake handle. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct Waker { /* Arc<OwnedFd> or dup'd fd; kqueue: kq fd + ident */ }
impl Waker {
    /// Makes a blocked or future `wait` return promptly. One syscall
    /// (write(eventfd) / kevent NOTE_TRIGGER / write(pipe)). Coalescing is the
    /// caller's job. A would-block on the pipe fallback is not an error.
    pub fn wake(&self) -> io::Result<()>;
}
```

Notes:
- epoll: register `EPOLLIN|EPOLLOUT|EPOLLRDHUP|EPOLLET` as requested; map
  `EPOLLIN|EPOLLPRI` → READABLE, `EPOLLOUT` → WRITABLE, `EPOLLHUP|EPOLLRDHUP` →
  HANGUP, `EPOLLERR` → ERROR. The eventfd is registered `EPOLLIN|EPOLLET` with a
  `u64` marker so it can be recognised and drained (read 8 bytes until EAGAIN).
- Timeout: round *up* to whole ms for epoll so 1 ns does not become 0 (busy
  spin); clamp to `i32::MAX`. kqueue: `timespec`.
- kqueue (`cfg(any(target_os = "macos", target_os = "freebsd", target_os =
  "netbsd", target_os = "openbsd", target_os = "dragonfly"))`): one
  `EV_ADD|EV_CLEAR` kevent per wanted filter; `modify` is stateless: it issues
  `EV_ADD|EV_CLEAR` for each wanted filter and `EV_DELETE` for each unwanted one
  in a single `kevent` call, ignoring ENOENT. `EV_EOF` → HANGUP
  in addition to READABLE/WRITABLE; `EV_ERROR` → ERROR. Wake: `EVFILT_USER`
  with `NOTE_TRIGGER` (macOS/FreeBSD), pipe pair elsewhere. This code is
  compile-checked with `cargo check --target x86_64-apple-darwin`, not executed.
- Layout: `src/lib.rs` (flags, Event, re-exports), `src/epoll.rs`
  (`cfg(target_os = "linux")`), `src/kqueue.rs` (BSD cfg). The io_uring
  backend (`uring` feature, readiness via multishot `IORING_OP_POLL_ADD`, same
  API) is Wave 4 work: do not add it or any `feature = "uring"` cfg now.
- Tests (Linux): socketpair readiness; edge-triggered semantics (a second
  `wait(Some(0))` returns 0 events until more data arrives); `modify` toggling
  READABLE; `delete`; `wake` from another thread unblocks `wait(None)` within
  10 ms and reports 0 events; 1000 wakes coalesce without error;
  `wait(Some(0))` allocates 0; `wake` allocates 0; HANGUP reported when the
  peer closes; ERROR/HANGUP on a reset socket.

## twine-sock

Raw socket helpers. All sockets are created `SOCK_NONBLOCK|SOCK_CLOEXEC`
(`accept4` on Linux/FreeBSD; `accept` + `fcntl` on macOS). File descriptors
are plain `i32` (`RawFd`); the core owns their lifetimes.

```rust
/// Creates, binds and listens a non-blocking TCP socket. SO_REUSEADDR is
/// always set; SO_REUSEPORT when `reuse_port`. Returns the fd and the bound
/// address (so port 0 is resolved). Binds exactly the given family and leaves
/// IPV6_V6ONLY at the kernel default; the core resolves "host:port" strings.
pub fn listen_tcp(addr: SocketAddr, reuse_port: bool, backlog: i32) -> io::Result<(i32, SocketAddr)>;
/// Binds a non-blocking UDP socket, same address rules.
pub fn listen_udp(addr: SocketAddr, reuse_port: bool) -> io::Result<(i32, SocketAddr)>;
/// accept4(SOCK_NONBLOCK|SOCK_CLOEXEC). Returns WouldBlock when the backlog is
/// empty; EINTR and ECONNABORTED are retried internally. Does not allocate
/// (SocketAddr is built from a stack sockaddr_storage).
pub fn accept(lfd: i32) -> io::Result<(i32, SocketAddr)>;
/// Creates a non-blocking socket, optionally binds `local`, and starts
/// connecting. `in_progress == true` for EINPROGRESS (the common case): the
/// caller waits for WRITABLE and calls `connect_error`.
pub fn connect(addr: SocketAddr, local: Option<SocketAddr>) -> io::Result<(i32, bool)>;
/// Reads SO_ERROR after a non-blocking connect completes. Ok(()) = connected.
pub fn connect_error(fd: i32) -> io::Result<()>;

pub fn set_nodelay(fd: i32, on: bool) -> io::Result<()>;
pub fn set_defer_accept(fd: i32, d: Duration) -> io::Result<()>; // Linux; Ok(()) elsewhere
pub fn set_busy_poll(fd: i32, d: Duration) -> io::Result<()>;    // Linux; Ok(()) elsewhere
pub fn set_buffers(fd: i32, rcv: usize, snd: usize) -> io::Result<()>; // 0 = leave default
pub fn set_keepalive(fd: i32, on: bool) -> io::Result<()>;
pub fn set_nonblock(fd: i32) -> io::Result<()>;
pub fn set_reuse_port(fd: i32, on: bool) -> io::Result<()>;
pub fn local_addr(fd: i32) -> io::Result<SocketAddr>;   // getsockname
pub fn peer_addr(fd: i32) -> io::Result<SocketAddr>;    // getpeername
pub fn so_error(fd: i32) -> io::Result<Option<i32>>;    // pending SO_ERROR errno

/// Thin syscall wrappers used on the hot path. None allocates. EINTR is
/// retried; EAGAIN surfaces as WouldBlock.
pub fn read(fd: i32, buf: &mut [u8]) -> io::Result<usize>;
pub fn write(fd: i32, buf: &[u8]) -> io::Result<usize>;
pub fn writev(fd: i32, iov: &[libc::iovec]) -> io::Result<usize>;
pub fn shutdown_write(fd: i32) -> io::Result<()>;       // SHUT_WR
pub fn close(fd: i32);                                  // ignores errors
pub fn send_to(fd: i32, buf: &[u8], to: SocketAddr) -> io::Result<usize>;
pub fn recv_from(fd: i32, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
/// Fills a sockaddr_storage from a SocketAddr; returns the length. No alloc.
pub fn to_sockaddr(addr: SocketAddr, out: &mut libc::sockaddr_storage) -> libc::socklen_t;
/// Converts a sockaddr_storage (len bytes valid) back; None for other families.
pub fn from_sockaddr(sa: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<SocketAddr>;

/// Batched UDP. Each message has a payload buffer of `buf_size` bytes and
/// address storage. Implemented with recvmmsg/sendmmsg on Linux (raw
/// `libc::syscall(SYS_recvmmsg, ..)` if the libc wrapper is unavailable);
/// elsewhere loops over recv_from/send_to with identical observable behaviour.
pub struct Batch { /* Vec<libc::mmsghdr>, Vec<libc::iovec>, Vec<sockaddr_storage>, Vec<Box<[u8]>>-or-one-slab */ }
impl Batch {
    pub fn new(n: usize, buf_size: usize) -> Batch;
    pub fn len(&self) -> usize;                       // n
    /// Payload buffer of message i (full capacity) for filling before send.
    pub fn buf_mut(&mut self, i: usize) -> &mut [u8];
    /// Sets the payload length of message i (bytes to send).
    pub fn set_len(&mut self, i: usize, n: usize);
    /// Sets the destination of message i. No alloc.
    pub fn set_addr(&mut self, i: usize, addr: SocketAddr);
    /// Received message i: payload (len = bytes received) and source address.
    pub fn msg(&self, i: usize) -> (&[u8], Option<SocketAddr>);
    pub fn msg_mut(&mut self, i: usize) -> (&mut [u8], Option<SocketAddr>);
    /// Receives up to n messages. WouldBlock when nothing is readable. Resets
    /// every message's length to capacity before the call.
    pub fn recv_mmsg(&mut self, fd: i32) -> io::Result<usize>;
    /// Sends the first n messages; returns how many were sent.
    pub fn send_mmsg(&mut self, fd: i32, n: usize) -> io::Result<usize>;
}
```

Tests: listen on 127.0.0.1:0, connect with `std::net::TcpStream`, `accept`
returns a fd and the peer address; `connect` to the listener completes after
WRITABLE (use twine-poll? no — use `libc::poll` in the test) and
`connect_error` is Ok; connect to a closed port yields ECONNREFUSED from
`connect_error`; UDP batch round-trip of 10 datagrams with
`recv_mmsg`/`send_mmsg`; `accept` returns WouldBlock on an idle listener;
`accept`, `read`, `write`, `writev` allocate 0; IPv6 loopback round trip.

## twine-slab

Size-classed byte slab allocator, per loop (not thread-safe).

```rust
/// Slab sizes, ascending.
pub const CLASSES: [usize; 4] = [4 << 10, 16 << 10, 64 << 10, 256 << 10];
/// One slab. Deref to `[u8]`; `capacity()` is the class size.
pub type Slab = Box<[u8]>;

pub struct Allocator { /* Vec<Slab> free list per class (preallocated capacity 64), counters */ }
impl Allocator {
    pub fn new() -> Allocator;
    /// Returns a slab with len == smallest class >= n. n larger than the largest
    /// class returns a fresh Box rounded up to a multiple of 256 KiB that is
    /// not pooled (`put` drops it). `get` from a non-empty free list must not allocate.
    pub fn get(&mut self, n: usize) -> Slab;
    /// Returns b (identified by len) to its class free list; non-class sizes are dropped.
    /// Never allocates once the free list Vec has capacity (preallocate 64 per class).
    pub fn put(&mut self, b: Slab);
    pub fn stats(&self) -> Stats;
    /// Releases free slabs above `keep` bytes per class.
    pub fn trim(&mut self, keep: usize);
}
/// Returns the class size for n, or None if larger than all classes.
pub fn class_for(n: usize) -> Option<usize>;

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats { pub in_use: usize, pub free: usize, pub allocated: usize, pub gets: u64, pub puts: u64, pub misses: u64 }
```

## twine-ring

Outbound byte FIFO for one connection, backed by slab segments. It never grows
by reallocating a contiguous buffer. The allocator is passed in explicitly
(the ring cannot hold a reference to the loop's allocator).

```rust
pub struct Ring { /* VecDeque<Segment> (capacity kept across reset), len, high_water */ }
struct Segment { buf: Slab, head: usize, tail: usize }  // data in [head, tail); the
// single-segment case may wrap (circular) so one 4 KiB slab serves a connection
// that interleaves write/consume without touching the allocator again.

impl Ring {
    pub fn new() -> Ring;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    /// Appends b, chaining slabs from `a` as needed. Copies b.
    pub fn write(&mut self, a: &mut Allocator, b: &[u8]);
    /// Appends each slice in order (one write per slice, no extra copy).
    pub fn writev(&mut self, a: &mut Allocator, bufs: &[&[u8]]);
    /// Fills dst with the readable regions in FIFO order; returns how many were
    /// filled (stops when dst is full). Never allocates. A wrapped segment
    /// contributes two iovecs. The iovecs are valid until the next &mut call.
    pub fn iovecs(&self, dst: &mut [libc::iovec]) -> usize;
    /// First contiguous readable region (empty slice when empty).
    pub fn peek(&self) -> &[u8];
    /// Drops n bytes from the front (n <= len). Segments that become empty are
    /// returned to `a`, except the last one, which is kept (reset to empty) so
    /// the next write needs no allocation.
    pub fn consume(&mut self, a: &mut Allocator, n: usize);
    /// Drops all data and returns every segment to `a` (the ring then holds no slab).
    pub fn reset(&mut self, a: &mut Allocator);
    /// Maximum len observed since new/reset_high_water.
    pub fn high_water(&self) -> usize;
    pub fn reset_high_water(&mut self);
}
```

Tests: property test writing random chunks (1 B – 300 KiB) and consuming
random amounts against a reference `VecDeque<u8>`, cross-checking `iovecs`
by copying the iovec contents out (unsafe slice from `iov_base`/`iov_len`);
wraparound inside one segment; `consume` exactly to a segment boundary; `write`
after `reset`; steady-state write+consume within one slab allocates 0;
`iovecs` allocates 0; high-water tracking; 100% statement coverage was reached
in Go — aim for the same.

## twine-mpsc

Bounded lock-free multi-producer single-consumer queue (Vyukov bounded queue:
per-slot sequence numbers, producers CAS the tail, the single consumer reads
the head without CAS). Head and tail are padded to separate cache lines
(`#[repr(align(64))]`).

```rust
/// Creates a queue with capacity rounded up to a power of two (minimum 2).
pub fn channel<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>);

/// Producer side: `Clone + Send + Sync`.
pub struct Producer<T> { /* Arc<Inner<T>> */ }
impl<T: Send> Producer<T> {
    /// Enqueues v. Returns Err(v) when the queue is full. Lock-free, never allocates.
    pub fn push(&self, v: T) -> Result<(), T>;
    /// Approximate (tail - head). Safe from any thread.
    pub fn len(&self) -> usize;
    pub fn capacity(&self) -> usize;
}
/// Consumer side: `Send`, not `Sync`, not `Clone` (single consumer by construction).
pub struct Consumer<T> { /* Arc<Inner<T>> */ }
impl<T: Send> Consumer<T> {
    /// Dequeues the oldest item. Returns None when empty. Drops nothing;
    /// the slot is marked free so it does not retain the value.
    pub fn pop(&self) -> Option<T>;
    pub fn len(&self) -> usize;
    pub fn capacity(&self) -> usize;
}
impl<T> Drop for Inner<T> { /* drops any items still in the queue */ }
```

Tests: FIFO per producer; 64 producers × 1M pushes total (cfg attribute or env
`TWINE_LONG_TESTS=1` → full; default 64 × 10K) with the consumer draining
concurrently and verifying every item arrives exactly once; full queue returns
Err; empty returns None; push/pop allocate 0; items still queued are dropped
exactly once when both ends drop (count with an Arc counter); `Producer` is
`Send + Sync` and `Consumer` is `Send` (static assertions). Optional: run
under `cargo +nightly miri test` for the small cases.

## twine-wheel

Hierarchical timing wheel, 4 levels × 256 slots, configurable base tick
(default 1 ms). Insert and cancel are O(1) with no allocation after warm-up.
Nodes live in an index-linked arena (`Vec<Node<K>>` with a free list); there
are no pointers, so the arena may move when it grows. Each node carries a
generation so a stale `TimerId` never cancels a later timer.

```rust
/// Handle to a scheduled timer. `Copy`. Stale handles are harmless.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerId { idx: u32, gen: u32 }

pub struct Wheel<K> {
    /* levels: [[u32; 256]; 4] slot heads (or 1024 sentinel nodes for circular lists),
       nodes: Vec<Node<K>>, free: Vec<u32>, due: Vec<(TimerId, K)> (reused),
       current tick, tick_ns, len */
}
impl<K> Wheel<K> {
    /// Creates a wheel whose tick 0 is at monotonic nanosecond time `now_ns`.
    pub fn new(tick: Duration, now_ns: u64) -> Wheel<K>;
    /// Schedules `kind` to fire when the wheel is advanced to or past `at_ns`
    /// (absolute monotonic ns). A time in the past fires on the next advance. O(1).
    pub fn add(&mut self, at_ns: u64, kind: K) -> TimerId;
    /// Unschedules the timer and returns its payload; None if it was not
    /// scheduled (fired, removed, or stale). O(1).
    pub fn remove(&mut self, id: TimerId) -> Option<K>;
    /// Reschedules a live timer to `at_ns`, keeping its id and payload. Returns
    /// false if the id is not live. O(1).
    pub fn reset(&mut self, id: TimerId, at_ns: u64) -> bool;
    /// Advances the wheel to `now_ns`, cascading levels and moving every expired
    /// timer to the due list in expiry order (FIFO insertion order within a
    /// tick). Returns how many expired. Does not call user code.
    pub fn advance(&mut self, now_ns: u64) -> usize;
    /// Pops the next due timer (after `advance`). The caller fires it; it may
    /// add/remove/reset timers between pops.
    pub fn pop_due(&mut self) -> Option<(TimerId, K)>;
    /// A monotonic ns time at or before the earliest scheduled timer's expiry
    /// (the loop uses it as a poll timeout bound); None when nothing is
    /// scheduled. Scan level 0 from the current slot (≤ 256 steps); if level 0
    /// is empty, return the time of the next level-0 wrap (next cascade).
    pub fn next(&self) -> Option<u64>;
    /// Reports whether id is live (scheduled, not yet popped).
    pub fn contains(&self, id: TimerId) -> bool;
    /// Absolute expiry of a live timer.
    pub fn expires(&self, id: TimerId) -> Option<u64>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn now_ns(&self) -> u64;
    pub fn tick(&self) -> Duration;
}
```

Semantics: a timer never fires early and fires at most one tick late: it
becomes due at the first `advance(now)` whose tick-aligned `now` is `>= at`
(so `add(5.5 ms)` is not due at `advance(5.7 ms)` with a 1 ms tick, but is at
6 ms). Order within a tick is FIFO among timers placed directly in that
level-0 slot; a timer cascading in from a higher level may fire after timers
added to the slot later (as in the Go original). `next()` returns the
earliest expiry *tick* (ceil(at / tick) * tick), which may be up to one tick
after `at`. Timers beyond the horizon (256^4 ticks ≈ 49
days at 1 ms) are clamped to the top level and re-inserted on cascade until
they fit. Tests: never-early / ≤ 1-tick-late property over random durations up
to 2^20 ticks with random advance step sizes; cascade boundaries
(255→256, 65535→65536, 16777215→16777216 ticks); remove between pops of the
same slot; add during the due phase with `at <= now` (fires on the *next*
advance, never re-entrantly); 1M timers add+remove benchmark (release mode,
`#[ignore]`d or a bin) ≤ 50 ns/op with 0 allocs after warm-up; `advance` +
`pop_due` allocate 0; stale id after remove returns None; generation reuse
(remove, add into the same slot, old id is dead).

---

## How the core uses them (for context, not to implement)

- One `poll::Poller`, one `slab::Allocator`, one `mpsc::Consumer<Task>` (its
  `Producer` lives in the `LoopHandle`), one `wheel::Wheel<TimerKind>`, one
  64 KiB read buffer per loop.
- `ConnIo` embeds a `ring::Ring` and an idle `TimerId`; `Loop::call_later`
  wraps `wheel.add` with `TimerKind::Fn(Box<dyn FnOnce(&mut Loop)>)`.
- Loop iteration: drain mpsc (≤ 1024) → `wheel.advance(now)` + pop_due/fire →
  spin `wait(Some(0))` up to `spin_iterations` then `wait(next timer)` →
  dispatch events → `writev` each dirty conn via `ring.iovecs` → recycle closed conns.
