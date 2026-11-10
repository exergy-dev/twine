# Internal packages

The `twine` core (loop.go, conn.go, ...) is written against the `internal/*`
packages described here. This file records their API, semantics and test
obligations; a change to a signature or a guarantee below is a change to the
core and belongs in the same commit as the core edit.

Rules that apply to every package:

- Standard library + `golang.org/x/sys/unix` only. The one internal import
  is `internal/debugtag` (see below), which slab, ring and wheel use for
  debug-build checks.
- **Zero heap allocations on the steady-state path** (push/pop, add/remove,
  write/consume, wait). Every package ships a `testing.AllocsPerRun` test that
  fails on a non-zero result for its hot operations.
- Not goroutine-safe unless stated (each loop owns its own instance). Only
  `mpsc.Queue.Push`, `mpsc.Queue.Len` and `poll.Poller.Wake` may be called
  from other goroutines.
- `testing/quick` property tests plus explicit edge-case tests (wraparound,
  cascade boundaries, full queue).
- Exported identifiers exist because the core calls them. Helpers that only
  tests need (`ring.Ring.Peek`, `wheel.Wheel.Now`, `wheel.Timer.Expires`,
  `mpsc.Queue.Cap`, `slab.ClassFor`, `sock.Message.AddrOf`) live in each
  package's `export_test.go`.
- Doc comment on every exported identifier and a package doc comment.
  `go vet`, `staticcheck` and `golangci-lint` clean, with and without
  `-tags twine_uring` and `-tags twine_debug`.

Module path: `github.com/exergy-dev/twine`. Go 1.24.

---

## internal/debugtag

```go
package debugtag

// On is true when built with -tags twine_debug (on.go), false otherwise
// (off.go).
const On = false
```

Debug-only invariant checks are written `if debugtag.On { ... }`, so a
release build compiles them out entirely (no call, no defer, no field
access remains; release benchmarks are unaffected). With `-tags
twine_debug`:

- `slab.Allocator.Put` panics on a double Put (the slab is already on its
  free list, so two owners would share one buffer) and `Get` panics with a
  message when `n > slab.MaxClass`.
- `wheel.Wheel.Advance` panics when called from inside a timer's `Fn`/`FnT`
  (the guard is cleared by a defer, so a recovered callback panic does not
  leave it set).
- `ring.Ring.Write` and `Consume` verify the segment chain after every call.

The root package has its own `debugBuildTag` for its checks. Tagged tests
(`TestPutTwicePanics`, `TestAdvanceReentryPanics`, `TestRingInvariantsDebug`,
...) run with `go test -tags twine_debug ./internal/...`.

## internal/poll

Platform abstraction over epoll (Linux), io_uring multishot poll (Linux,
`-tags twine_uring`) and kqueue (darwin, dragonfly, freebsd, netbsd,
openbsd). Edge-triggered on every backend (EPOLLET; EV_CLEAR; multishot
POLL_ADD).

```go
package poll

// Event flags. Several may be set at once.
const (
    Readable uint32 = 1 << iota // EPOLLIN|EPOLLPRI / EVFILT_READ
    Writable                    // EPOLLOUT / EVFILT_WRITE
    Hangup                      // EPOLLHUP|EPOLLRDHUP / EV_EOF
    Error                       // EPOLLERR / EV_ERROR, or EV_EOF with fflags != 0
)

// MaxEvents caps the events one Wait returns; the loop sizes its event
// array with it.
const MaxEvents = 256

// Event is one ready notification. Wait fills a caller-provided slice.
type Event struct {
    FD    int32
    Flags uint32
}

// Poller is one epoll/io_uring/kqueue instance plus its wakeup channel.
type Poller struct { /* unexported */ }

// New creates the poll instance and the wake channel: an eventfd on Linux,
// EVFILT_USER on darwin/dragonfly/freebsd, a non-blocking pipe on
// netbsd/openbsd (golang.org/x/sys has no EVFILT_USER there). With
// twine_uring, New falls back to epoll when io_uring cannot be set up.
func New() (*Poller, error)

// Add registers fd edge-triggered with the given interest (Readable and/or
// Writable). Hangup and Error are always reported and need not be requested.
func (p *Poller) Add(fd int, flags uint32) error
// Mod replaces fd's interest set; readiness for a newly requested direction
// is reported by the next Wait.
func (p *Poller) Mod(fd int, flags uint32) error
// Del removes fd. Callers close the fd themselves, after Del.
func (p *Poller) Del(fd int) error

// Wait blocks up to timeout (-1 = forever, 0 = poll) and fills at most
// min(len(evs), MaxEvents) entries; an empty evs is EINVAL. The wake channel
// is consumed internally and never reported. Wait does not allocate.
func (p *Poller) Wait(evs []Event, timeout time.Duration) (int, error)

// Wake makes a blocked or future Wait return promptly. Safe from any
// goroutine; a single syscall (write(eventfd) / kevent NOTE_TRIGGER /
// write(pipe)), retried on EINTR. A full pipe or saturated eventfd is not an
// error (a wakeup is already pending). Coalescing is the caller's job.
func (p *Poller) Wake() error

// Close releases the poll fd and wake channel. Only Close may be called
// after Close (it returns nil); any other use of a closed Poller is invalid.
func (p *Poller) Close() error

// Backend names the kernel interface in use: "epoll", "kqueue",
// "io_uring", or "epoll (io_uring unavailable)" when a twine_uring build
// fell back, so the loop can log the fallback once.
func (p *Poller) Backend() string
```

Semantics:

- **EINTR.** Wait never returns EINTR. A Wait interrupted by a signal
  returns `(0, nil)` early (io_uring with a negative timeout waits again
  instead), and on io_uring a timed Wait woken by a
  completion that carries no event (a stale-generation or internal CQE) also
  returns `(0, nil)` early. The loop already treats a zero-event return as a
  spurious wakeup and recomputes its timeout, so there is no remaining-time
  bookkeeping (and no `time.Now`) inside Wait.
- epoll: interest is `EPOLLRDHUP|EPOLLET` plus `EPOLLIN`/`EPOLLOUT` as
  requested; `EPOLLIN|EPOLLPRI` → Readable, `EPOLLOUT` → Writable,
  `EPOLLHUP|EPOLLRDHUP` → Hangup, `EPOLLERR` → Error. Timeouts are rounded
  up to a whole millisecond (1 ns never becomes a busy 0), -1 passes through.
  Add of a registered fd is EEXIST, Mod/Del of an unregistered fd ENOENT.
- kqueue: one `EV_ADD|EV_CLEAR` kevent per filter (READ, WRITE). Mod and Del
  `EV_DELETE` the filters no longer wanted and ignore ENOENT. Where
  `EV_RECEIPT` exists (darwin, dragonfly, freebsd, openbsd:
  `kqueue_receipt_bsd.go`) all changes go in one kevent call with a receipt
  each, leaving pending events untouched; golang.org/x/sys has no
  `EV_RECEIPT` for NetBSD (`kqueue_receipt_netbsd.go`), so there each change
  is its own kevent call. READ and WRITE kevents for one fd are merged when
  adjacent, but the same fd may appear twice in a result. Full nanosecond
  timeouts. EV_EOF is delivered only for registered filters, so an fd with
  no interest gets no Hangup.
- io_uring (`-tags twine_uring`): readiness only, one multishot
  `IORING_OP_POLL_ADD` per fd. Add/Mod queue SQEs that the next Wait submits
  (one `io_uring_enter` per loop iteration); Wait(0) with nothing queued is a
  memory-only CQ check. `user_data` carries a per-fd generation so stale CQEs
  are dropped; Del submits at once (a live poll holds the file open) and
  retries `-EALREADY` removes. A terminated multishot (CQ overflow,
  `-ECANCELED`) is re-armed and reports current readiness; `-EBADR`
  (dropped CQEs) re-arms every registration (`resync`). Only the triggering
  direction is reported per wakeup; Add's registration errors surface as
  `Error|Hangup` from the next Wait. The EXT_ARG timeout struct is pinned
  with `runtime.Pinner` for the ring's lifetime. Under sustained CQ overflow
  a ready fd whose data is left unread is reported on every Wait (each
  re-arm reports it again); the loop always reads to EAGAIN, so this costs
  spurious events only. The fallback is the same `epoller` the default
  build uses.

Files and build tags:

| File | Build tag | Contents |
| --- | --- | --- |
| `poll.go` | all | flags, `MaxEvents`, `Event`, package doc |
| `epoller_linux.go` | `linux` | unexported `epoller` (epoll fd + kernel event buffer): add/mod/del/wait/close |
| `epoll_linux.go` | `linux && !twine_uring` | `Poller` embedding `epoller` |
| `uring_linux.go` | `linux && twine_uring` | io_uring `Poller`, `*epoller` fallback |
| `wake_linux.go` | `linux` | eventfd waker |
| `kqueue_bsd.go` | `darwin \|\| dragonfly \|\| freebsd \|\| netbsd \|\| openbsd` | kqueue `Poller` |
| `kqueue_receipt_bsd.go` | `darwin \|\| dragonfly \|\| freebsd \|\| openbsd` | `evReceipt = EV_RECEIPT` |
| `kqueue_receipt_netbsd.go` | netbsd (file suffix) | `evReceipt = 0` |
| `wake_bsd.go` | `darwin \|\| dragonfly \|\| freebsd` | EVFILT_USER waker |
| `wake_pipe_bsd.go` | `netbsd \|\| openbsd` | non-blocking pipe waker |

The module builds and vets for linux/{amd64,386,arm,arm64},
darwin/{amd64,arm64}, freebsd/{amd64,386}, netbsd/{amd64,386,arm},
openbsd/{amd64,386} and dragonfly/amd64 (kevent's `Ident` is 32-bit on the
32-bit BSDs, so code converts it with `int(kev.Ident)`).

Tests (one shared suite in `poll_test.go`, Linux extras in
`poll_linux_test.go`, io_uring internals in `uring_linux_test.go`):
socketpair readiness, edge-triggered (a second Wait(0) reports nothing until
new data), Mod toggling Readable, Del, Hangup, RDHUP without read interest,
Wake from another goroutine within 10 ms reporting 0 events, 1000+
concurrent Wakes coalescing, more ready fds than `len(evs)`, Wait(0)/Wake/
Mod/Del+Add at 0 allocs, `Backend()`. Under `twine_uring`:
`TestEpollFallbackSuite` re-runs the whole suite on the fallback
(`TWINE_POLL_TEST_FORCE_EPOLL=1`), plus stale-generation drops, re-arm after
cancel, `-EALREADY` remove retries, SQ overflow, and `TestURingCQOverflow`
(8-entry rings, 64 ready fds, then `resync`). `BenchmarkWaitWake` runs in
every Linux build (`epoll/{wait0,wake+wait,pingpong-locked,mod+wait}`; the
uring build compares `uring/*` with `epoll/*`).

## internal/sock

Raw socket helpers. All sockets are created non-blocking and close-on-exec.

```go
package sock

// Listen creates, binds and listens a non-blocking TCP socket. network is
// "tcp", "tcp4" or "tcp6"; addr is a host:port as accepted by
// net.ResolveTCPAddr. SO_REUSEADDR is always set; SO_REUSEPORT when
// reusePort. Returns the fd and the bound address (port 0 resolved).
func Listen(network, addr string, reusePort bool, backlog int) (fd int, laddr *net.TCPAddr, err error)

// ListenUDP binds a non-blocking UDP socket, same address rules.
func ListenUDP(network, addr string, reusePort bool) (fd int, laddr *net.UDPAddr, err error)

// Accept returns unix.EAGAIN when the backlog is empty; EINTR and
// ECONNABORTED are retried internally. The EAGAIN path does not allocate.
func Accept(lfd int) (fd int, sa unix.Sockaddr, err error)

// ConnectAddr creates a non-blocking socket and starts connecting to the
// already resolved raddr (the root package resolves names off the loop);
// inProgress is true for EINPROGRESS (the common case): wait for Writable,
// then call ConnectError. la may be nil (no bind); a nil or unspecified
// raddr.IP dials the loopback address.
func ConnectAddr(network string, raddr, la *net.TCPAddr) (fd int, inProgress bool, err error)
// ConnectError reads SO_ERROR after a non-blocking connect; nil = connected.
func ConnectError(fd int) error

func SetNoDelay(fd int, on bool) error
func SetDeferAccept(fd int, d time.Duration) error // Linux; nil no-op elsewhere
func SetBusyPoll(fd int, d time.Duration) error    // Linux; nil no-op elsewhere
func SetBuffers(fd, rcv, snd int) error            // <= 0 leaves a side unchanged

// SockaddrToTCP converts for accept/connect (allocates once per connection;
// the read/write path never calls it). nil for unsupported families.
func SockaddrToTCP(sa unix.Sockaddr) *net.TCPAddr
// LocalTCP returns getsockname as a *net.TCPAddr, or nil.
func LocalTCP(fd int) *net.TCPAddr

// UDPAddrToSockaddr fills caller-provided storage for sendto/sendmmsg (no
// allocation when the storage is reused).
func UDPAddrToSockaddr(a *net.UDPAddr, v4 *unix.SockaddrInet4, v6 *unix.SockaddrInet6) unix.Sockaddr

// Batched UDP.
type Message struct {
    Buf     []byte                // receive capacity / bytes to send
    N       int                   // bytes received or sent by the last call
    Addr    unix.RawSockaddrInet6 // storage; interpret by Family
    AddrLen uint32                // 0 = no address (connected socket)
}
type Batch struct { /* preallocated mmsghdr/iovec arrays (Linux) */ }
func NewBatch(n, bufSize int) *Batch
func (b *Batch) Msgs() []Message                     // len n, owned by the Batch
func (b *Batch) RecvMmsg(fd int) (int, error)        // EAGAIN when nothing is queued
func (b *Batch) SendMmsg(fd int, n int) (int, error) // sends the first n messages
// SetAddr fills the raw sockaddr from a *net.UDPAddr without allocating.
func (m *Message) SetAddr(a *net.UDPAddr)
// FillAddr fills dst in place from the received raw sockaddr, reusing
// dst.IP's backing array (no allocation once it has 16 bytes of capacity).
// The IPv6 scope id is dropped.
func (m *Message) FillAddr(dst *net.UDPAddr)
```

Platforms: `sys_accept4.go` (linux, dragonfly, freebsd) creates sockets with
`SOCK_NONBLOCK|SOCK_CLOEXEC` and accepts with a raw `accept4` syscall
(`unix.Accept4` would move its sockaddr buffer to the heap on every
EAGAIN). `sys_fcntl.go` (darwin, netbsd, openbsd) creates and accepts under
`syscall.ForkLock` and sets `O_NONBLOCK`/`FD_CLOEXEC` with fcntl, since
golang.org/x/sys has no `Accept4` there. Batches use one
`recvmmsg`/`sendmmsg` call on Linux (`mmsg_linux.go`) and loop over
`recvfrom`/`sendto` with the same observable behaviour on the BSDs
(`mmsg_bsd.go`, `batch_loop.go`; each received datagram then allocates one
`unix.Sockaddr`). Errors are returned as the raw `unix.Errno`: boxing an
errno below 256 does not allocate.

Tests: listen on 127.0.0.1:0 and accept a net.Dial client with the peer
address; ConnectAddr completes after Writable with a nil ConnectError; a closed
port yields ECONNREFUSED; dual-stack and IPv6 listeners; UDP batch round
trip of 10 datagrams through both RecvMmsg/SendMmsg and the portable loop;
Accept EAGAIN and the mmsg paths at 0 allocs. Benchmarks:
`BenchmarkAcceptEAGAIN`, `BenchmarkSendMmsg`, `BenchmarkRecvMmsg` (8 × 64 B
datagrams per op over loopback).

## internal/slab

Size-classed `[]byte` allocator, one per loop (not goroutine-safe).

```go
package slab

// MaxClass is the largest class and the largest n Get accepts.
const MaxClass = 256 << 10
// Classes returns the slab sizes, ascending: 4 KiB, 16 KiB, 64 KiB, MaxClass.
// It returns a copy; the allocator's table is unexported.
func Classes() [4]int

type Allocator struct { /* free list per class, counters */ }
// New preallocates 64 free-list slots per class.
func New() *Allocator
// Get returns a slice with len == cap == the smallest class >= n. n must be
// <= MaxClass (larger n panics; ring never asks for more). Get from a
// non-empty free list does not allocate. Reused slabs are not cleared.
func (a *Allocator) Get(n int) []byte
// Put returns b (identified by cap) to its class free list; a non-class cap
// is dropped. Each slab from Get must be Put at most once. Put does not
// allocate until a class holds more than 64 free slabs.
func (a *Allocator) Put(b []byte)

type Stats struct {
    InUse     int    // bytes currently handed out
    Free      int    // bytes sitting in free lists
    Allocated int    // high water mark of InUse+Free
    Gets, Puts, Misses uint64 // Misses = Gets that had to make()
}
func (a *Allocator) Stats() Stats
```

Free slabs are never released back to the GC while the loop runs.

## internal/ring

Outbound byte queue for one connection. Storage is a chain of slab segments
and never grows by `append` reallocation. Each segment is a circular buffer;
only the head segment can hold wrapped data, and only when it is also the
tail, so a connection whose backlog stays within 4 KiB uses one 4 KiB slab.
Growth takes the smallest class that fits the rest of the current Write
(capped at `slab.MaxClass`) but never less than one class above the current
tail (4K → 16K → 64K → 256K).

```go
package ring

// Ring is a byte FIFO backed by slab segments. Zero value is unusable; call
// Init. Must not be copied after Init (it points at its own inline segment);
// a leading noCopy field makes go vet's copylocks check report copies.
type Ring struct { /* head/tail segments, length, inline segment, spare structs */ }

// Init binds r to an allocator; may be called again on a pooled Ring
// (segments still held are returned first).
func (r *Ring) Init(a *slab.Allocator)
// Len returns readable bytes.
func (r *Ring) Len() int
// Write appends b (copies). An empty b never takes a segment.
func (r *Ring) Write(b []byte)
// Writev appends each slice in order.
func (r *Ring) Writev(bs [][]byte)
// Iovecs fills dst with the readable regions in FIFO order and returns the
// filled prefix (a wrapped segment contributes two). Never allocates.
func (r *Ring) Iovecs(dst []unix.Iovec) []unix.Iovec
// Consume drops n bytes from the front (panics unless 0 <= n <= Len).
// Emptied segments go back to the allocator, including the last one when
// Len reaches 0.
func (r *Ring) Consume(n int)
// Reset drops all data and returns every segment.
func (r *Ring) Reset()
```

An empty Ring holds no slab: none before the first non-empty Write, and the
Consume that drains it returns the last segment, so an idle connection pins
no buffer and the core needs no Reset after a drain. A write/drain cycle
costs a free-list Get/Put and allocates nothing. Segment structs are pooled
inside the Ring (one inline, the rest on a spare list), so Write allocates
nothing once warm. The property test writes random chunks and consumes
random amounts against a reference `[]byte` queue, cross-checking Iovecs
(copied out with `unsafe.Slice`), `Ring.verify` and allocator accounting.
`unix.Iovec.Base` is `*byte`: set it with `iov.Base = &seg.buf[off]` and
`iov.SetLen(n)`.

## internal/mpsc

Bounded lock-free multi-producer single-consumer queue of `func()`.

```go
package mpsc

type Queue struct { /* Vyukov bounded queue: sequence per cell, padded head/tail */ }
// New rounds capacity up to a power of two (minimum 2).
func New(capacity int) *Queue
// Push enqueues fn; false when full. Lock-free, safe from any goroutine,
// never allocates (the func value is stored in the cell).
func (q *Queue) Push(fn func()) bool
// Pop dequeues the oldest item (single consumer only); false when empty.
// Clears the cell's func reference.
func (q *Queue) Pop() (func(), bool)
// Len is approximate (tail - head, clamped to the capacity) and safe from
// any goroutine.
func (q *Queue) Len() int
```

Algorithm: Dmitry Vyukov's bounded MPMC queue (each cell has an atomic
`seq`; producers CAS `tail`; the single consumer advances `head` without
CAS). head and tail sit on separate cache lines (`[56]byte` padding).
Tests: FIFO per producer; `TestStress` runs 64 producers against a 1024-cell
queue with a concurrent consumer that checks per-producer order and
exactly-once delivery: 1M items in total by default, 640K with `-short`, 64
× 1M with `TWINE_STRESS=1` (the nightly job). The consumer records the first
inconsistency and keeps draining, so a lost or duplicated item fails the
test instead of hanging it. Full queue returns false; empty returns false;
Push/Pop 0 allocs; benchmark with 16 producers.

## internal/wheel

Hierarchical timing wheel, 4 levels × 256 slots, configurable tick (the core
uses 1 ms). Insert and cancel O(1), no allocation. Timers are intrusive list
nodes owned by the caller.

```go
package wheel

// Timer is an intrusive node; the zero value is a valid unscheduled timer.
type Timer struct {
    // Fn is called by Advance when the timer expires.
    Fn func()
    // FnT, used when Fn is nil, is called with the expired timer. Storing a
    // package-level function here costs no allocation (a method value in Fn
    // would); callers embed Timer as the first field of their own struct
    // and convert the *Timer back. Both nil: firing is a no-op.
    FnT func(*Timer)
    // unexported: next, prev *Timer; expires int64 (ticks); loc int32
}
// Scheduled reports whether t is in a wheel (false inside its own callback
// unless the callback re-added it).
func (t *Timer) Scheduled() bool

type Wheel struct { /* slot lists [4][256], occupancy bitmaps [4][4]uint64, due/firing lists */ }
// New creates a wheel whose tick 0 is at monotonic ns now (tick <= 0 → 1 ms).
func New(tick time.Duration, now int64) *Wheel
// Add schedules t for absolute monotonic ns at; an at not after the last
// Advance fires on the next Advance (or within the current Advance when
// called from a callback). Re-adding a scheduled timer moves it. O(1).
func (w *Wheel) Add(t *Timer, at int64)
// Remove unschedules t; false if it was not scheduled. O(1).
func (w *Wheel) Remove(t *Timer) bool
// Advance moves the wheel to now, cascading levels and firing every expired
// timer: due timers first, then tick by tick, FIFO within a slot.
// Callbacks may Add/Remove any timer. Returns the number fired. Must not be
// called from a callback.
func (w *Wheel) Advance(now int64) int
// Next returns a tick-boundary time at or before the earliest timer can
// fire (an earlier cascade boundary counts), false when none is scheduled.
// O(1): it scans the occupancy bitmaps of all four levels (4 × 4 words).
func (w *Wheel) Next() (int64, bool)
// Len returns the number of scheduled timers.
func (w *Wheel) Len() int
```

Semantics: a timer never fires early and fires at most one tick late (exact
when `at` and `now` are tick multiples). Timers beyond the horizon (256^4
ticks ≈ 49.7 days at 1 ms) park in the top level and are re-placed on
cascade. Tests: never-early/≤1-tick-late property over random durations and
Advance steps; cascade boundaries (255→256, 65535→65536,
16777215→16777216); Remove inside a callback of another timer in the same
slot; Add from a callback (fires in the same Advance iff `at <= now`); FnT
recovering its embedding struct and Fn-over-FnT precedence; Next never
later than the earliest expiry (brute-force scan) and always making
progress; Add/Remove/Advance at 0 allocs with both Fn and FnT.
Benchmarks: 1M-timer Add+Remove and Reset, AdvanceFire.

---

## How the core uses them

- One `poll.Poller`, one `slab.Allocator`, one `mpsc.Queue`, one
  `wheel.Wheel` and one read buffer per Loop; the loop's event array is
  `[poll.MaxEvents]poll.Event`.
- `conn` embeds a `ring.Ring` (outbound backlog) and a `wheel.Timer` (idle
  timeout). A `CallLater` `Timer` embeds a `wheel.Timer` as its first field
  with `FnT` set to a package-level function that converts back. Timer
  handles are carved from a per-loop chunk of 256 (one allocation per 256
  `CallLater` calls) and are never reused, so a stale handle cannot touch a
  later timer.
- Loop iteration: drain the mpsc queue (≤ 1024 tasks) → `wheel.Advance(now)`
  → `Wait(0)` up to SpinIterations times, then `Wait(timeout to the next
  timer)` (a zero-event return is just another iteration) → dispatch events
  → `writev` each dirty conn from `ring.Iovecs` and `Consume` what the
  kernel took → recycle closed conns.
