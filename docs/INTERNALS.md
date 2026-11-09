# Internal package contracts

This file is the contract between the `twine` core (loop.go, conn.go, ...) and
the `internal/*` packages. The core is written against these signatures; a
change here is a change to the core. Design rules that apply to every package:

- Standard library + `golang.org/x/sys/unix` only. No other imports.
- **Zero heap allocations on the steady-state path** (push/pop, add/remove,
  write/consume, wait). Every package ships a `testing.AllocsPerRun` test that
  fails on a non-zero result for its hot operations.
- Not goroutine-safe unless stated (each loop owns its own instance). Only
  `mpsc.Queue.Push` and `poll.Poller.Wake` may be called from other goroutines.
- `testing/quick` property tests plus explicit edge-case tests (wraparound,
  cascade boundaries, full queue) per design Section 12.
- Doc comment on every exported identifier. `go vet` and `staticcheck` clean.
- Package doc comment (`// Package xxx ...`) in a `doc.go` or the main file.

Module path: `github.com/exergy-dev/twine`. Go 1.24.

---

## internal/poll

Platform abstraction over epoll (Linux) and kqueue (darwin, freebsd, netbsd,
openbsd, dragonfly). Edge-triggered on both backends (EPOLLET; EV_CLEAR).

```go
package poll

// Event flags. Several may be set at once.
const (
    Readable uint32 = 1 << iota // EPOLLIN  / EVFILT_READ
    Writable                    // EPOLLOUT / EVFILT_WRITE
    Hangup                      // EPOLLHUP | EPOLLRDHUP / EV_EOF
    Error                       // EPOLLERR / EV_ERROR
)

// Event is one ready notification. Wait fills a caller-provided slice.
type Event struct {
    FD    int32
    Flags uint32
}

// Poller is one epoll/kqueue instance plus its wakeup fd.
type Poller struct { /* unexported */ }

// New creates the poll instance and the wake fd (eventfd on Linux, EVFILT_USER
// on kqueue, falling back to a non-blocking pipe pair where EVFILT_USER is
// unavailable, e.g. NetBSD < 10 / OpenBSD).
func New() (*Poller, error)

// Add registers fd edge-triggered with the given interest (Readable and/or
// Writable). Hangup and Error are always reported and need not be requested.
func (p *Poller) Add(fd int, flags uint32) error
// Mod replaces fd's interest set.
func (p *Poller) Mod(fd int, flags uint32) error
// Del removes fd. Callers close the fd themselves, after Del.
func (p *Poller) Del(fd int) error

// Wait blocks up to timeout (-1 = forever, 0 = poll) and fills evs. The wake
// fd is consumed internally (drained) and NEVER reported in evs. Returns the
// number of events written. EINTR is retried internally and never returned.
// Must not allocate: use a fixed-size stack or struct-embedded array of
// unix.EpollEvent / unix.Kevent_t of len(evs) capped at 256 to translate.
func (p *Poller) Wait(evs []Event, timeout time.Duration) (int, error)

// Wake makes a blocked or future Wait return promptly. Safe from any
// goroutine. Coalescing is the caller's responsibility; Wake itself must be a
// single syscall (write(eventfd) / kevent EVFILT_USER NOTE_TRIGGER / write(pipe)).
// A would-block on the pipe fallback is not an error (the loop is already awake).
func (p *Poller) Wake() error

// Close releases the poll fd and wake fds.
func (p *Poller) Close() error
```

Notes:
- epoll: register `EPOLLIN|EPOLLOUT|EPOLLRDHUP|EPOLLET` as requested; map
  `EPOLLIN|EPOLLPRI` → Readable, `EPOLLOUT` → Writable, `EPOLLHUP|EPOLLRDHUP` →
  Hangup, `EPOLLERR` → Error. The eventfd is registered `EPOLLIN|EPOLLET`.
- kqueue: one `EV_ADD|EV_CLEAR` kevent per filter (READ, WRITE). `Mod` must
  compute the diff and `EV_DELETE` the filter no longer wanted (deleting a
  filter that is not registered returns ENOENT, which Mod ignores). Events
  with `EV_EOF` set map to Hangup in addition to Readable/Writable; `EV_ERROR`
  maps to Error with `Data` holding the errno.
- Timeout conversion: `time.Duration` → ms for epoll (round up so 1ns does not
  become 0 → busy spin; -1 passes through), → `unix.Timespec` for kqueue.
- Build tags: `epoll_linux.go`, `wake_linux.go`; `kqueue_bsd.go` with
  `//go:build darwin || dragonfly || freebsd || netbsd || openbsd`;
  `wake_bsd.go` same tag. `uring_linux.go` is Phase 6 (tag `twine_uring`) and
  out of scope for wave 1 – but structure `epoll_linux.go` with
  `//go:build linux && !twine_uring` so it can be swapped.
- Tests (Linux runnable; same test file must compile on BSD): socketpair
  readiness, edge-triggered semantics (a second Wait with timeout 0 returns 0
  events until more data arrives), Mod toggling Readable, Del, Wake from
  another goroutine unblocks Wait within 10ms and reports 0 events, 1000
  Wakes coalesce without error, Wait(0) allocates 0 bytes, Wake allocates 0.

## internal/sock

Raw socket helpers. All sockets are created `SOCK_NONBLOCK|SOCK_CLOEXEC`.

```go
package sock

// Listen creates, binds and listens a non-blocking TCP socket. network is
// "tcp", "tcp4" or "tcp6"; addr is a host:port as accepted by net.ResolveTCPAddr
// (":9000", "0.0.0.0:9000", "[::1]:9000"). SO_REUSEADDR is always set;
// SO_REUSEPORT when reusePort. Returns the fd and the bound address (so
// port 0 is resolved).
func Listen(network, addr string, reusePort bool, backlog int) (fd int, laddr *net.TCPAddr, err error)

// ListenUDP binds a non-blocking UDP socket, same address rules.
func ListenUDP(network, addr string, reusePort bool) (fd int, laddr *net.UDPAddr, err error)

// Accept calls accept4(SOCK_NONBLOCK|SOCK_CLOEXEC) (accept + fcntl on darwin,
// which lacks accept4). Returns unix.EAGAIN when the backlog is empty; EINTR
// and ECONNABORTED are retried internally.
func Accept(lfd int) (fd int, sa unix.Sockaddr, err error)

// Connect creates a non-blocking socket and starts connecting. It returns
// inProgress=true for EINPROGRESS (the common case); the caller waits for
// Writable and calls ConnectError. local may be "" (no bind).
func Connect(network, addr, local string) (fd int, raddr *net.TCPAddr, inProgress bool, err error)

// ConnectError reads SO_ERROR after a non-blocking connect completes. nil
// means connected.
func ConnectError(fd int) error

func SetNoDelay(fd int, on bool) error
func SetDeferAccept(fd int, d time.Duration) error   // Linux; no-op nil elsewhere
func SetBusyPoll(fd int, d time.Duration) error      // Linux; no-op nil elsewhere
func SetBuffers(fd, rcv, snd int) error              // 0 = leave default
func SetKeepAlive(fd int, on bool) error
func SetNonblock(fd int) error

// SockaddrToTCP / SockaddrToUDP convert a unix.Sockaddr to a *net.TCPAddr /
// *net.UDPAddr. They allocate (one per accept is acceptable; the read/write
// path never calls them). Return nil for unsupported families.
func SockaddrToTCP(sa unix.Sockaddr) *net.TCPAddr
func SockaddrToUDP(sa unix.Sockaddr) *net.UDPAddr

// LocalTCP / LocalUDP return getsockname as a net addr.
func LocalTCP(fd int) *net.TCPAddr
func LocalUDP(fd int) *net.UDPAddr

// UDPAddrToSockaddr converts for sendto/sendmmsg. Does not allocate when
// dst is non-nil and reused (fill *unix.SockaddrInet4 / *unix.SockaddrInet6
// passed by the caller). Returns the filled Sockaddr.
func UDPAddrToSockaddr(a *net.UDPAddr, v4 *unix.SockaddrInet4, v6 *unix.SockaddrInet6) unix.Sockaddr

// Batched UDP (Linux). Each Message has Buf for payload (len = capacity to
// read into / bytes to send), and Addr storage. RecvMmsg/SendMmsg fill N
// (bytes received or sent) per message and return the count of messages
// processed. Implemented with unix.Syscall6(unix.SYS_RECVMMSG, ...) and a
// package-level reusable []unix.Mmsghdr / []unix.Iovec / raw sockaddr storage
// sized by the batch at construction: hence a Batch type.
type Message struct {
    Buf  []byte
    N    int
    Addr unix.RawSockaddrInet6 // storage; interpret by Family
    AddrLen uint32
}
type Batch struct { /* preallocated mmsghdr/iovec arrays */ }
func NewBatch(n, bufSize int) *Batch
func (b *Batch) Msgs() []Message                 // len n; caller sets Buf lengths / Addr before SendMmsg
func (b *Batch) RecvMmsg(fd int) (int, error)    // returns EAGAIN when nothing to read
func (b *Batch) SendMmsg(fd int, n int) (int, error) // sends the first n messages
// On non-Linux, RecvMmsg/SendMmsg loop over unix.Recvfrom/unix.Sendto with
// the same observable behaviour (fallback per design Section 13 Phase 4).
// AddrOf converts a received message's raw sockaddr to *net.UDPAddr (allocates).
func (m *Message) AddrOf() *net.UDPAddr
// SetAddr fills the raw sockaddr from a *net.UDPAddr without allocating.
func (m *Message) SetAddr(a *net.UDPAddr)
```

Tests: listen on 127.0.0.1:0, connect with net.Dial, Accept returns a fd and
the peer address; Connect to the listener completes after Writable and
ConnectError is nil; Connect to a closed port yields ECONNREFUSED from
ConnectError; UDP batch round-trip of 10 datagrams with RecvMmsg/SendMmsg;
Accept returns EAGAIN on an idle listener.

## internal/slab

Size-classed `[]byte` allocator, per loop (not goroutine-safe).

```go
package slab

// Classes are the slab sizes, ascending.
var Classes = [...]int{4 << 10, 16 << 10, 64 << 10, 256 << 10}

type Allocator struct { /* free lists per class, counters */ }
func New() *Allocator
// Get returns a slice with len == cap == the smallest class >= n. n larger
// than the largest class returns a fresh make([]byte, n) rounded up to a
// multiple of 256 KiB that is not pooled (Put frees it to the GC). Get from
// a non-empty free list must not allocate.
func (a *Allocator) Get(n int) []byte
// Put returns b (identified by cap) to its class free list. Slices whose cap
// is not a class are dropped. Put never allocates once a free list has been
// grown (free lists are slices of []byte that grow by append; preallocate 64
// slots per class in New).
func (a *Allocator) Put(b []byte)
// ClassFor returns the class size for n or -1 if larger than all classes.
func ClassFor(n int) int

type Stats struct {
    InUse     int   // bytes currently handed out
    Free      int   // bytes sitting in free lists
    Allocated int   // bytes ever made (high water mark of InUse+Free)
    Gets, Puts, Misses uint64 // Misses = Gets that had to make()
}
func (a *Allocator) Stats() Stats
// Trim releases free slabs above keep bytes per class back to the GC.
func (a *Allocator) Trim(keep int)
```

## internal/ring

Outbound byte queue for one connection. Storage is a chain of slab segments;
it never grows by `append` reallocation. Conceptually a ring: the single-
segment case wraps around inside the segment so a connection that stays
within one 4 KiB slab never touches the allocator after the first write.

```go
package ring

// Ring is a byte FIFO backed by slab segments. Zero value is unusable; call
// Init. Not goroutine-safe.
type Ring struct { /* head/tail segment pointers, len, highWater, alloc */ }

// Init binds r to an allocator. Init may be called on a Ring embedded in a
// pooled conn struct repeatedly (after Reset).
func (r *Ring) Init(a *slab.Allocator)
// Len returns readable bytes.
func (r *Ring) Len() int
// Write appends b. Grows by chaining slabs from the allocator; copies b.
func (r *Ring) Write(b []byte)
// Writev appends each slice in order (one Write per slice, no extra copy).
func (r *Ring) Writev(bs [][]byte)
// Iovecs fills dst with the readable regions in FIFO order and returns the
// filled prefix. It stops when dst is full. Never allocates. A segment may
// contribute two iovecs when its data wraps.
func (r *Ring) Iovecs(dst []unix.Iovec) []unix.Iovec
// Peek returns the first contiguous readable region (nil when empty).
func (r *Ring) Peek() []byte
// Consume drops n bytes from the front (n <= Len). Segments that become empty
// are returned to the allocator, except the last one, which is kept (reset to
// empty) so the next Write needs no allocation.
func (r *Ring) Consume(n int)
// Reset drops all data and returns every segment to the allocator.
func (r *Ring) Reset()
// HighWater returns the maximum Len observed since Init/ResetHighWater.
func (r *Ring) HighWater() int
func (r *Ring) ResetHighWater()
```

Segment structs are pooled inside the Ring/allocator (a free list of
`*segment`), so Write on a non-empty free list allocates nothing. The
property test writes random chunks and consumes random amounts, comparing to
a reference `[]byte` queue, with iovec export cross-checked by copying the
iovecs out (use `unsafe.Slice((*byte)(unsafe.Pointer(iov.Base)), iov.Len)`).

Design note on iovec: `unix.Iovec.Base` is `*byte`; set via
`iov.Base = &seg.buf[off]` and `iov.SetLen(n)`.

## internal/mpsc

Bounded lock-free multi-producer single-consumer queue of `func()`.

```go
package mpsc

type Queue struct { /* Vyukov bounded queue: sequence per slot, head/tail atomics, padded */ }
// New rounds capacity up to a power of two (minimum 2).
func New(capacity int) *Queue
// Push enqueues fn. Returns false when the queue is full. Lock-free and safe
// from any goroutine. Never allocates (the func value is stored in the slot).
func (q *Queue) Push(fn func()) bool
// Pop dequeues the oldest item. Single consumer only. Returns false when
// empty. Clears the slot's func reference so it does not retain garbage.
func (q *Queue) Pop() (func(), bool)
// Len is approximate (tail - head) and safe from any goroutine.
func (q *Queue) Len() int
// Cap returns the rounded capacity.
func (q *Queue) Cap() int
```

Algorithm: Dmitry Vyukov's bounded MPMC queue (each cell has an atomic
`seq`; producers CAS `tail`; the consumer reads `head` without CAS since it is
single). Pad head and tail onto separate cache lines (`[56]byte` padding).
Tests: FIFO per producer; 64 producers × 1M pushes total (guarded by
`testing.Short()` → 64 × 10K) under `-race` with the consumer draining
concurrently and verifying every item arrives exactly once; full queue
returns false; empty returns false; Push/Pop 0 allocs; benchmark with 16
producers.

## internal/wheel

Hierarchical timing wheel, 4 levels × 256 slots, configurable base tick
(default 1 ms). Insert and cancel O(1), no allocation. Timers are intrusive
list nodes owned by the caller (the core pools them).

```go
package wheel

// Timer is an intrusive node. Callers embed or allocate it; the wheel never
// allocates one. Fn is invoked by Advance when the timer expires. Zero value
// is a valid unscheduled timer.
type Timer struct {
    Fn   func()
    // unexported: prev, next *Timer; expires int64 (ticks); level/slot or a
    // scheduled flag
}
// Scheduled reports whether t is currently in a wheel.
func (t *Timer) Scheduled() bool
// Expires returns the absolute expiry in ticks (meaningful when Scheduled).
func (t *Timer) Expires() int64

type Wheel struct { /* levels [4][256]list; current tick; count; tick duration */ }
// New creates a wheel whose tick 0 is at the given monotonic nanosecond time.
func New(tick time.Duration, now int64) *Wheel
// Add schedules t to fire when the wheel is advanced to or past `at` (absolute
// monotonic nanoseconds). An `at` in the past fires on the next Advance. If t
// is already scheduled it is removed first (Reset semantics). O(1).
func (w *Wheel) Add(t *Timer, at int64)
// Remove unschedules t. Returns false if it was not scheduled. O(1).
func (w *Wheel) Remove(t *Timer) bool
// Advance moves the wheel to `now` (monotonic ns), cascading levels and
// calling Fn on every expired timer in expiry order within a tick (FIFO
// insertion order inside a slot). Timers added or removed from inside Fn are
// handled correctly (iterate by unlinking the slot list first). Returns the
// number fired.
func (w *Wheel) Advance(now int64) int
// Next returns a monotonic ns time at or before the earliest scheduled
// timer's expiry (the loop uses it as a poll timeout upper bound), and false
// when no timer is scheduled. Implementation: scan level 0 from the current
// slot (≤256 steps); if level 0 is empty return the time of the next level-0
// wrap, which is when the next cascade happens.
func (w *Wheel) Next() (int64, bool)
// Len returns the number of scheduled timers.
func (w *Wheel) Len() int
// Now returns the wheel's current tick as monotonic ns.
func (w *Wheel) Now() int64
```

Semantics: a timer never fires early and fires at most one tick late (after
`Advance(now)` with `now >= at`). Timers beyond the wheel's horizon
(256^4 ticks ≈ 49 days at 1 ms) are clamped to the top level and re-inserted
on cascade until they fit. Tests: never-early/≤1-tick-late property over
random durations up to 2^20 ticks with random Advance step sizes; cascade
boundaries (255→256, 65535→65536, 16777215→16777216 ticks); Remove inside Fn
of another timer in the same slot; Add from within Fn of a timer that expires
in the same Advance (must fire in that Advance if `at <= now`, else later);
1M timers Add+Remove benchmark ≤ 50 ns/op with 0 allocs; Advance firing 0
allocs.

---

## How the core uses them (for context, not to implement)

- One `poll.Poller`, one `slab.Allocator`, one `mpsc.Queue`, one
  `wheel.Wheel`, one 64 KiB read buffer per Loop.
- `conn` embeds a `ring.Ring` and a `wheel.Timer` (idle timeout); `Timer`
  (CallLater) wraps a pooled `wheel.Timer`.
- Loop iteration: drain mpsc (≤1024) → `wheel.Advance(now)` → spin
  `Wait(0)` up to SpinIterations then `Wait(next timer)` → dispatch events →
  `writev` each dirty conn via `ring.Iovecs` → recycle closed conns.
