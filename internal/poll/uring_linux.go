//go:build linux && twine_uring

// io_uring readiness backend (build tag twine_uring).
//
// This backend uses io_uring for readiness notification only. Every fd gets
// one multishot IORING_OP_POLL_ADD (IORING_POLL_ADD_MULTI, edge-triggered,
// the io_uring default); Wait reaps the resulting CQEs and translates their
// poll masks into the same Event flags as the epoll backend. Batched
// accept/read/write submissions (the io_uring completion model) were NOT
// implemented: the Poller contract is readiness-based and the root package
// performs its own non-blocking syscalls, so a completion backend would need
// a different Poller API (whether io_uring should ever become the default is
// an open question in docs/history/design.md). What io_uring buys here is
// syscall batching: Add and Mod only write SQEs into the shared ring and are
// submitted together by the next Wait (one io_uring_enter per loop
// iteration instead of one epoll_ctl per change), and Wait(0) with nothing
// to submit is a memory-only check of the completion ring.
//
// Design notes:
//
//   - user_data is gen<<32 | fd. A per-fd generation is bumped on Mod and Del
//     so CQEs posted for an earlier registration (already queued in the CQ
//     when the caller changed its mind, or belonging to a closed fd whose
//     number was reused) are dropped instead of being reported. Add of a
//     registered fd fails with EEXIST and Mod/Del of an unregistered fd with
//     ENOENT, like epoll; these checks come from the fd table.
//   - Mod is POLL_REMOVE(old) + POLL_ADD(new), not POLL_UPDATE: the fresh arm
//     re-reports current readiness exactly like EPOLL_CTL_MOD, and a REMOVE
//     that finds nothing (multishot already terminated) is harmless.
//   - A poll request holds a reference to the file: until it is removed the
//     socket stays open (no FIN/RST to the peer) even after the caller's
//     close(2). Del therefore submits immediately, and retries POLL_REMOVE
//     when it fails with -EALREADY, which the kernel returns (leaving the
//     multishot poll armed!) whenever the poll has a completion in flight,
//     e.g. an arm that was immediately ready, or a concurrent wakeup. Del's
//     enter uses GETEVENTS so this thread's pending poll task_work runs and
//     the retry succeeds; a remaining -EALREADY (task_work owned by another
//     thread) is retried by the next Wait.
//   - If an fd's POLL_ADD is still unsubmitted in the SQ, Mod rewrites that
//     SQE in place and Del turns it into a NOP, so an ADD and its REMOVE are
//     never submitted in one batch (the arm-then-remove case above) and
//     Add→Mod→Del within one loop iteration costs no kernel work at all.
//   - When the kernel terminates a multishot poll (CQE without
//     IORING_CQE_F_MORE, e.g. CQ overflow or -ECANCELED because the
//     submitting thread exited) the poll is re-armed with the same
//     generation; the new arm reports current readiness so no edge is lost.
//   - The wake eventfd (shared waker type) is registered the same way and is
//     drained, never reported.
//   - The ring is created without COOP_TASKRUN, DEFER_TASKRUN and
//     SINGLE_ISSUER: poll completions run as task_work on the submitting OS
//     thread, and goroutines migrate between threads, so the default
//     TWA_SIGNAL notification is required for completions to be posted while
//     another thread waits.
//
// Semantic differences from epoll:
//
//   - Add's registration errors (EBADF for a closed fd, ...) are asynchronous:
//     Add returns nil and the next Wait reports Error|Hangup for that fd.
//   - A multishot CQE carries the wakeup's poll key, so only the direction
//     that triggered the wakeup is reported: with Readable|Writable interest,
//     new data on an already-writable socket yields Readable, whereas epoll
//     ET re-polls and reports Readable|Writable. A Writable edge still
//     arrives whenever send-buffer space frees up (the write-space wakeup
//     carries POLLOUT); wakeups without a key (state changes such as
//     hangup) report the full current mask, as epoll does.
//   - Positive timeouts have nanosecond resolution (no millisecond round-up).
//   - Wait(0) does not enter the kernel when nothing is queued, so it reports
//     readiness that has already been posted to the CQ; a wakeup whose
//     task_work is still pending on another thread shows up on a later Wait.
//
// New falls back to the epoll backend (the epoller the default build uses)
// when io_uring is unusable (any setup or mmap failure, e.g.
// ENOSYS/EPERM/EINVAL, the kernel lacks IORING_FEAT_NODROP or
// IORING_FEAT_EXT_ARG, or multishot poll (Linux 5.13) is rejected), so a
// -tags twine_uring binary still runs everywhere.

package poll

import (
	"encoding/binary"
	"math"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// io_uring ABI constants (include/uapi/linux/io_uring.h). x/sys v0.30.0 only
// provides the syscall numbers.
const (
	ioringSetupCQSize = 1 << 3 // IORING_SETUP_CQSIZE
	ioringSetupClamp  = 1 << 4 // IORING_SETUP_CLAMP

	ioringFeatSingleMmap = 1 << 0 // IORING_FEAT_SINGLE_MMAP
	ioringFeatNoDrop     = 1 << 1 // IORING_FEAT_NODROP
	ioringFeatExtArg     = 1 << 8 // IORING_FEAT_EXT_ARG

	ioringOffSQRing = 0          // IORING_OFF_SQ_RING
	ioringOffCQRing = 0x8000000  // IORING_OFF_CQ_RING
	ioringOffSQEs   = 0x10000000 // IORING_OFF_SQES

	ioringEnterGetEvents = 1 << 0 // IORING_ENTER_GETEVENTS
	ioringEnterExtArg    = 1 << 3 // IORING_ENTER_EXT_ARG

	ioringSQCQOverflow = 1 << 1 // IORING_SQ_CQ_OVERFLOW (sq_flags)

	ioringOpNop        = 0 // IORING_OP_NOP
	ioringOpPollAdd    = 6 // IORING_OP_POLL_ADD
	ioringOpPollRemove = 7 // IORING_OP_POLL_REMOVE

	ioringPollAddMulti = 1 << 0 // IORING_POLL_ADD_MULTI (sqe->len)

	ioringCQEFMore = 1 << 1 // IORING_CQE_F_MORE
)

// Ring sizes. Add/Mod only write SQEs, so the SQ is flushed early if a loop
// iteration queues more than sqEntries changes. The CQ is larger so that a
// burst of readiness on many fds rarely overflows (overflow is still
// correct, via IORING_FEAT_NODROP and re-arming, just slower). They are
// variables only so that tests can force an overflow with tiny rings.
var (
	sqEntries uint32 = 512
	cqEntries uint32 = 4096
)

// initialFDs is the initial size of the fd table; it doubles on demand (the
// only allocation after New, and only in Add).
const initialFDs = 1024

// user_data layout: bits 32..62 hold the generation and bits 0..31 the fd.
// Bit 63 marks internal requests: a POLL_REMOVE carries udInternal|target so
// that an -EALREADY result can be retried, and udIgnore marks a cancelled
// SQE (NOP) or a CQE already handled by Del.
const (
	udInternal = 1 << 63
	udIgnore   = udInternal | 0xFFFFFFFF // fd 0xFFFFFFFF is never valid
	genMask    = 1<<31 - 1
	// delRetries bounds the synchronous POLL_REMOVE retries in Del; any
	// further -EALREADY is retried by the next Wait.
	delRetries = 3
)

// ioUringParams is struct io_uring_params.
type ioUringParams struct {
	sqEntries uint32
	cqEntries uint32
	flags     uint32
	_         [2]uint32 // sq_thread_cpu, sq_thread_idle
	features  uint32
	_         [4]uint32 // wq_fd, resv[3]
	sqOff     sqringOffsets
	cqOff     cqringOffsets
}

// sqringOffsets is struct io_sqring_offsets.
type sqringOffsets struct {
	head     uint32
	tail     uint32
	ringMask uint32
	_        uint32 // ring_entries
	flags    uint32
	_        uint32 // dropped
	array    uint32
	_        uint32 // resv1
	_        uint64 // user_addr
}

// cqringOffsets is struct io_cqring_offsets.
type cqringOffsets struct {
	head     uint32
	tail     uint32
	ringMask uint32
	_        uint32 // ring_entries
	_        uint32 // overflow
	cqes     uint32
	_        uint32 // flags
	_        uint32 // resv1
	_        uint64 // user_addr
}

// ioUringSqe is struct io_uring_sqe (64 bytes, no SQE128).
type ioUringSqe struct {
	opcode   uint8
	_        uint8  // flags
	_        uint16 // ioprio
	fd       int32
	_        uint64 // off / addr2
	addr     uint64
	len      uint32
	opFlags  uint32 // poll32_events for POLL_ADD
	userData uint64
	_        [24]byte // buf_index, personality, splice_fd_in, addr3, __pad2
}

// ioUringCqe is struct io_uring_cqe (16 bytes, no CQE32).
type ioUringCqe struct {
	userData uint64
	res      int32
	flags    uint32
}

// getEventsArg is struct io_uring_getevents_arg (IORING_ENTER_EXT_ARG).
type getEventsArg struct {
	sigmask   uint64
	sigmaskSz uint32
	_         uint32 // min_wait_usec (6.12+), pad before
	ts        uint64 // *__kernel_timespec
}

// kernelTimespec is struct __kernel_timespec (64-bit fields on every arch,
// unlike unix.Timespec).
type kernelTimespec struct {
	sec  int64
	nsec int64
}

// Compile-time ABI size checks.
var (
	_ [unsafe.Sizeof(ioUringParams{}) - 120]struct{}
	_ [120 - unsafe.Sizeof(ioUringParams{})]struct{}
	_ [unsafe.Sizeof(ioUringSqe{}) - 64]struct{}
	_ [64 - unsafe.Sizeof(ioUringSqe{})]struct{}
	_ [unsafe.Sizeof(ioUringCqe{}) - 16]struct{}
	_ [16 - unsafe.Sizeof(ioUringCqe{})]struct{}
	_ [unsafe.Sizeof(getEventsArg{}) - 24]struct{}
	_ [24 - unsafe.Sizeof(getEventsArg{})]struct{}
)

// bigEndian reports whether poll32_events must be halfword-swapped (the
// kernel applies swahw32 on big-endian hosts).
var bigEndian = binary.NativeEndian.Uint16([]byte{0, 1}) == 1

// forceEpoll makes New skip io_uring and use the epoll fallback. It is set
// only by tests (TestMain) to run the shared suite against the fallback.
var forceEpoll bool

// Poller is one io_uring instance (or, where io_uring is unusable, one epoll
// instance) plus its eventfd wakeup channel.
//
// A Poller is owned by one goroutine; only Wake may be called concurrently.
type Poller struct {
	w      waker
	closed bool
	ring   *uring   // nil when running on the epoll fallback
	ep     *epoller // non-nil iff ring == nil
	// uringErr records why io_uring was not used (nil when ring != nil or
	// when forceEpoll is set). Diagnostics only.
	uringErr error
}

// New creates the io_uring instance and the eventfd used by Wake. If
// io_uring is unavailable or lacks multishot poll support, it transparently
// falls back to epoll.
func New() (*Poller, error) {
	return open(!forceEpoll)
}

// open creates a Poller, trying io_uring first when useURing is set.
func open(useURing bool) (*Poller, error) {
	p := &Poller{}
	if err := p.w.open(); err != nil {
		return nil, err
	}
	if useURing {
		r, err := newURing(p.w.fd)
		if err == nil {
			p.ring = r
			return p, nil
		}
		p.uringErr = err
	}
	ep := &epoller{}
	if err := ep.open(p.w.fd); err != nil {
		_ = p.w.close()
		return nil, err
	}
	p.ep = ep
	return p, nil
}

// Add registers fd edge-triggered with the given interest (Readable and/or
// Writable). Hangup and Error are always reported and need not be requested.
//
// On io_uring the registration is queued and submitted by the next Wait; a
// kernel-side failure (such as a closed fd) is then reported by Wait as
// Error|Hangup for fd instead of being returned here.
func (p *Poller) Add(fd int, flags uint32) error {
	if p.ring == nil {
		return p.ep.add(fd, flags)
	}
	return p.ring.add(fd, flags)
}

// Mod replaces fd's interest set. If fd is already ready for a requested
// direction, the next Wait reports it.
func (p *Poller) Mod(fd int, flags uint32) error {
	if p.ring == nil {
		return p.ep.mod(fd, flags)
	}
	return p.ring.mod(fd, flags)
}

// Del removes fd. Callers close the fd themselves, after Del.
func (p *Poller) Del(fd int) error {
	if p.ring == nil {
		return p.ep.del(fd)
	}
	return p.ring.del(fd)
}

// Wait blocks up to timeout (negative = forever, 0 = poll) and fills evs,
// returning the number of events written. At most min(len(evs), MaxEvents)
// events are returned per call and, on io_uring, each fd appears at most
// once; an empty evs is rejected with EINVAL.
//
// The wake eventfd is drained internally and never reported, so a Wait woken
// only by Wake returns (0, nil). EINTR is never returned: a Wait interrupted
// by a signal, or woken by completions that carry no event (a stale or
// internal CQE), may return (0, nil) before timeout. Wait does not allocate.
func (p *Poller) Wait(evs []Event, timeout time.Duration) (int, error) {
	if p.ring == nil {
		return p.ep.wait(evs, timeout, &p.w)
	}
	if len(evs) == 0 {
		return 0, unix.EINVAL
	}
	return p.ring.wait(evs[:min(len(evs), MaxEvents)], timeout, &p.w)
}

// Wake makes a blocked or future Wait return promptly. It is safe to call
// from any goroutine and costs a single write(2) on the eventfd; coalescing
// repeated wakeups is the caller's responsibility. Wake must not be called
// after Close (the fd number may have been reused). It does not allocate.
func (p *Poller) Wake() error {
	return p.w.wake()
}

// Close releases the io_uring (or epoll) instance and the eventfd. Calling
// Close more than once returns nil.
func (p *Poller) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	var err error
	if p.ring != nil {
		err = p.ring.close()
	} else {
		err = p.ep.close()
	}
	if err2 := p.w.close(); err == nil {
		err = err2
	}
	return err
}

// Backend names the kernel interface in use: "io_uring", or "epoll
// (io_uring unavailable)" when New fell back because io_uring could not be
// set up (the reason is not exposed; the fallback is fully functional).
func (p *Poller) Backend() string {
	switch {
	case p.ring != nil:
		return "io_uring"
	case p.uringErr != nil:
		return "epoll (io_uring unavailable)"
	}
	return "epoll" // fallback forced by a test
}

// ---------------------------------------------------------------------------
// io_uring state
// ---------------------------------------------------------------------------

// fd table entry states.
const (
	fdFree  = 0
	fdPoll  = 1 // registered by the caller
	fdWaker = 2 // the internal wake eventfd
)

// fdState is the per-fd registration record.
type fdState struct {
	gen    uint32 // generation encoded in user_data; bumped on Mod/Del
	events uint32 // poll mask of the current registration
	stamp  uint32 // Wait sequence number when idx was assigned
	idx    int32  // index in the caller's evs during Wait call stamp
	addSeq uint32 // SQ position of the last POLL_ADD queued for this fd
	state  uint8
}

// uring is the io_uring instance with its mapped rings.
type uring struct {
	fd int

	sqRing, cqRing, sqesMem []byte // mappings (cqRing may alias sqRing)

	sqHead  *uint32 // kernel-written
	sqTail  *uint32 // user-written
	sqFlags *uint32 // kernel-written
	sqMask  uint32
	sqLen   uint32
	sqes    []ioUringSqe
	sqLocal uint32 // next SQ tail (published to *sqTail by enter)

	cqHead  *uint32 // user-written
	cqTail  *uint32 // kernel-written
	cqMask  uint32
	cqes    []ioUringCqe
	cqLocal uint32 // CQ head as last published

	fds []fdState
	seq uint32 // Wait sequence number for per-fd coalescing

	arg getEventsArg // arg.ts holds &ts, set once by newURing
	ts  kernelTimespec
	pin runtime.Pinner // pins ts while arg refers to it
}

// sysSetup wraps io_uring_setup(2).
func sysSetup(entries uint32, params *ioUringParams) (int, error) {
	fd, _, e := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(params)), 0)
	if e != 0 {
		return -1, e
	}
	return int(fd), nil
}

// newURing sets up the ring and arms a multishot poll on the wake eventfd.
// Any error means io_uring is unusable here and the caller falls back.
func newURing(wakeFD int) (r *uring, err error) {
	params := ioUringParams{flags: ioringSetupCQSize | ioringSetupClamp, cqEntries: cqEntries}
	fd, err := sysSetup(sqEntries, &params)
	if err != nil {
		return nil, err
	}
	r = &uring{fd: fd}
	// The kernel reads the timeout through the uintptr in r.arg, which the
	// GC cannot see: pin r.ts for the ring's lifetime (close unpins it).
	r.pin.Pin(&r.ts)
	r.arg = getEventsArg{sigmaskSz: 8, ts: uint64(uintptr(unsafe.Pointer(&r.ts)))}
	defer func() {
		if err != nil {
			_ = r.close()
			r = nil
		}
	}()
	const need = ioringFeatNoDrop | ioringFeatExtArg
	if params.features&need != need {
		return nil, unix.ENOTSUP
	}

	sqSize := int(params.sqOff.array) + int(params.sqEntries)*4
	cqSize := int(params.cqOff.cqes) + int(params.cqEntries)*int(unsafe.Sizeof(ioUringCqe{}))
	single := params.features&ioringFeatSingleMmap != 0
	if single && cqSize > sqSize {
		sqSize = cqSize
	}
	const prot = unix.PROT_READ | unix.PROT_WRITE
	const flags = unix.MAP_SHARED | unix.MAP_POPULATE
	if r.sqRing, err = unix.Mmap(fd, ioringOffSQRing, sqSize, prot, flags); err != nil {
		return nil, err
	}
	if single {
		r.cqRing = r.sqRing
	} else if r.cqRing, err = unix.Mmap(fd, ioringOffCQRing, cqSize, prot, flags); err != nil {
		return nil, err
	}
	sqeSize := int(params.sqEntries) * int(unsafe.Sizeof(ioUringSqe{}))
	if r.sqesMem, err = unix.Mmap(fd, ioringOffSQEs, sqeSize, prot, flags); err != nil {
		return nil, err
	}

	so, co := &params.sqOff, &params.cqOff
	r.sqHead = (*uint32)(unsafe.Pointer(&r.sqRing[so.head]))
	r.sqTail = (*uint32)(unsafe.Pointer(&r.sqRing[so.tail]))
	r.sqFlags = (*uint32)(unsafe.Pointer(&r.sqRing[so.flags]))
	r.sqMask = *(*uint32)(unsafe.Pointer(&r.sqRing[so.ringMask]))
	r.sqLen = params.sqEntries
	r.sqes = unsafe.Slice((*ioUringSqe)(unsafe.Pointer(&r.sqesMem[0])), params.sqEntries)
	// The SQ index array is the identity mapping, written once.
	arr := unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqRing[so.array])), params.sqEntries)
	for i := range arr {
		arr[i] = uint32(i)
	}
	r.sqLocal = atomic.LoadUint32(r.sqTail)

	r.cqHead = (*uint32)(unsafe.Pointer(&r.cqRing[co.head]))
	r.cqTail = (*uint32)(unsafe.Pointer(&r.cqRing[co.tail]))
	r.cqMask = *(*uint32)(unsafe.Pointer(&r.cqRing[co.ringMask]))
	r.cqes = unsafe.Slice((*ioUringCqe)(unsafe.Pointer(&r.cqRing[co.cqes])), params.cqEntries)
	r.cqLocal = atomic.LoadUint32(r.cqHead)

	n := initialFDs
	for n <= wakeFD {
		n *= 2
	}
	r.fds = make([]fdState, n)

	// Arm the wake eventfd and probe multishot poll support: kernels before
	// 5.13 reject IORING_POLL_ADD_MULTI with an immediate -EINVAL CQE.
	st := &r.fds[wakeFD]
	st.state = fdWaker
	st.events = unix.POLLIN
	if err = r.queuePollAdd(wakeFD, st); err != nil {
		return nil, err
	}
	if e := r.enter(0, ioringEnterGetEvents, nil, 0); e != 0 {
		return nil, e
	}
	if r.sqLocal != atomic.LoadUint32(r.sqHead) {
		return nil, unix.EAGAIN
	}
	want := pollUD(wakeFD, st.gen)
	for h, t := r.cqLocal, atomic.LoadUint32(r.cqTail); h != t; h++ {
		cqe := &r.cqes[h&r.cqMask]
		if cqe.userData == want && cqe.res < 0 {
			return nil, unix.Errno(-cqe.res)
		}
	}
	return r, nil
}

// close unmaps the rings and closes the ring fd. The kernel cancels all
// outstanding polls (and drops their file references) on teardown.
func (r *uring) close() error {
	if r.sqesMem != nil {
		_ = unix.Munmap(r.sqesMem)
	}
	if r.cqRing != nil && unsafe.SliceData(r.cqRing) != unsafe.SliceData(r.sqRing) {
		_ = unix.Munmap(r.cqRing)
	}
	if r.sqRing != nil {
		_ = unix.Munmap(r.sqRing)
	}
	r.sqesMem, r.cqRing, r.sqRing, r.sqes, r.cqes = nil, nil, nil, nil, nil
	r.pin.Unpin()
	return unix.Close(r.fd)
}

// pollUD encodes the user_data of fd's poll request.
func pollUD(fd int, gen uint32) uint64 {
	return uint64(gen&genMask)<<32 | uint64(uint32(fd))
}

// pollInterest translates Readable/Writable into poll(2) bits. POLLRDHUP is
// always requested so a half-close is reported even without read interest
// (as with the epoll backend); POLLERR and POLLHUP are implicit.
func pollInterest(flags uint32) uint32 {
	ev := uint32(unix.POLLRDHUP)
	if flags&Readable != 0 {
		ev |= unix.POLLIN
	}
	if flags&Writable != 0 {
		ev |= unix.POLLOUT
	}
	return ev
}

// pollFlags translates a poll(2) result mask into Event flags.
func pollFlags(m uint32) uint32 {
	var f uint32
	if m&(unix.POLLIN|unix.POLLPRI) != 0 {
		f |= Readable
	}
	if m&unix.POLLOUT != 0 {
		f |= Writable
	}
	if m&(unix.POLLHUP|unix.POLLRDHUP) != 0 {
		f |= Hangup
	}
	if m&(unix.POLLERR|unix.POLLNVAL) != 0 {
		f |= Error
	}
	return f
}

// enter calls io_uring_enter(2), first publishing the SQ tail (release) so
// every queued SQE is submitted. The return value is deliberately ignored:
// with SQEs to submit it is the submit count even when the wait timed out.
func (r *uring) enter(minComplete, flags uint32, arg unsafe.Pointer, argSz uintptr) unix.Errno {
	atomic.StoreUint32(r.sqTail, r.sqLocal)
	toSubmit := r.sqLocal - atomic.LoadUint32(r.sqHead)
	_, _, e := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(r.fd), uintptr(toSubmit),
		uintptr(minComplete), uintptr(flags), uintptr(arg), argSz)
	return e
}

// submit flushes queued SQEs without waiting. EAGAIN/EBUSY (transient
// resource shortage) leave the SQEs queued for the next Wait.
func (r *uring) submit() error {
	for r.sqLocal != atomic.LoadUint32(r.sqHead) {
		switch e := r.enter(0, 0, nil, 0); e {
		case 0, unix.EINTR:
		case unix.EAGAIN, unix.EBUSY:
			return nil
		default:
			return e
		}
	}
	return nil
}

// next returns a zeroed SQE slot, flushing the SQ first if it is full. The
// caller fills it and increments sqLocal.
func (r *uring) next() (*ioUringSqe, error) {
	if r.sqLocal-atomic.LoadUint32(r.sqHead) >= r.sqLen {
		if err := r.submit(); err != nil {
			return nil, err
		}
		if r.sqLocal-atomic.LoadUint32(r.sqHead) >= r.sqLen {
			return nil, unix.EBUSY
		}
	}
	sqe := &r.sqes[r.sqLocal&r.sqMask]
	*sqe = ioUringSqe{}
	return sqe, nil
}

// queuePollAdd queues a multishot POLL_ADD for fd's current registration.
func (r *uring) queuePollAdd(fd int, st *fdState) error {
	sqe, err := r.next()
	if err != nil {
		return err
	}
	ev := pollEvents32(st.events)
	sqe.opcode = ioringOpPollAdd
	sqe.fd = int32(fd)
	sqe.len = ioringPollAddMulti
	sqe.opFlags = ev
	sqe.userData = pollUD(fd, st.gen)
	st.addSeq = r.sqLocal
	r.sqLocal++
	return nil
}

// pollEvents32 encodes a poll mask for sqe->poll32_events.
func pollEvents32(ev uint32) uint32 {
	if bigEndian {
		return ev<<16 | ev>>16
	}
	return ev
}

// queuedAdd returns fd's POLL_ADD SQE for its current registration if it
// has not been submitted yet (it lies in [sqHead, sqLocal) and still carries
// the current user_data), or nil.
func (r *uring) queuedAdd(fd int, st *fdState) *ioUringSqe {
	head := atomic.LoadUint32(r.sqHead)
	if st.addSeq-head >= r.sqLocal-head {
		return nil
	}
	sqe := &r.sqes[st.addSeq&r.sqMask]
	if sqe.opcode != ioringOpPollAdd || sqe.userData != pollUD(fd, st.gen) {
		return nil
	}
	return sqe
}

// queuePollRemove queues a POLL_REMOVE of the request with user_data target.
// Its own completion carries udInternal|target (retried on -EALREADY, else
// ignored); the target's -ECANCELED completion has a stale generation.
func (r *uring) queuePollRemove(target uint64) error {
	sqe, err := r.next()
	if err != nil {
		return err
	}
	sqe.opcode = ioringOpPollRemove
	sqe.fd = -1
	sqe.addr = target
	sqe.userData = udInternal | target
	r.sqLocal++
	return nil
}

// lookup returns fd's table entry if fd is registered by the caller.
func (r *uring) lookup(fd int) *fdState {
	if fd < 0 || fd >= len(r.fds) || r.fds[fd].state != fdPoll {
		return nil
	}
	return &r.fds[fd]
}

func (r *uring) add(fd int, flags uint32) error {
	if fd < 0 || fd > math.MaxInt32 {
		return unix.EBADF
	}
	if fd >= len(r.fds) {
		n := 2 * len(r.fds)
		for n <= fd {
			n *= 2
		}
		fds := make([]fdState, n)
		copy(fds, r.fds)
		r.fds = fds
	}
	st := &r.fds[fd]
	if st.state != fdFree {
		return unix.EEXIST
	}
	st.gen = (st.gen + 1) & genMask
	st.events = pollInterest(flags)
	if err := r.queuePollAdd(fd, st); err != nil {
		return err
	}
	st.state = fdPoll
	return nil
}

func (r *uring) mod(fd int, flags uint32) error {
	st := r.lookup(fd)
	if st == nil {
		return unix.ENOENT
	}
	if sqe := r.queuedAdd(fd, st); sqe != nil {
		// Not submitted yet: rewrite the queued arm in place.
		st.gen = (st.gen + 1) & genMask
		st.events = pollInterest(flags)
		sqe.opFlags = pollEvents32(st.events)
		sqe.userData = pollUD(fd, st.gen)
		return nil
	}
	if err := r.queuePollRemove(pollUD(fd, st.gen)); err != nil {
		return err
	}
	st.gen = (st.gen + 1) & genMask
	st.events = pollInterest(flags)
	return r.queuePollAdd(fd, st)
}

func (r *uring) del(fd int) error {
	st := r.lookup(fd)
	if st == nil {
		return unix.ENOENT
	}
	if sqe := r.queuedAdd(fd, st); sqe != nil {
		// Never submitted: cancel the queued arm.
		*sqe = ioUringSqe{opcode: ioringOpNop, fd: -1, userData: udIgnore}
	} else if err := r.queuePollRemove(pollUD(fd, st.gen)); err != nil {
		return err
	}
	st.gen = (st.gen + 1) & genMask
	st.state = fdFree
	// Submit now (including REMOVEs queued by earlier Mods) so the kernel
	// drops its file references before the caller closes fd.
	return r.submitRemoves()
}

// submitRemoves submits the SQ with GETEVENTS (running this thread's
// pending poll task_work) and re-queues any POLL_REMOVE that this enter
// completed with -EALREADY, up to delRetries times. A remaining -EALREADY
// CQE is left in the CQ for drain to retry.
func (r *uring) submitRemoves() error {
	for try := 0; ; try++ {
		t0 := atomic.LoadUint32(r.cqTail)
		for r.sqLocal != atomic.LoadUint32(r.sqHead) {
			e := r.enter(0, ioringEnterGetEvents, nil, 0)
			if e == unix.EAGAIN || e == unix.EBUSY {
				return nil // still queued; the next Wait submits it
			}
			if e != 0 && e != unix.EINTR {
				return e
			}
		}
		if try == delRetries {
			return nil
		}
		retried := false
		for h, t1 := t0, atomic.LoadUint32(r.cqTail); h != t1; h++ {
			cqe := &r.cqes[h&r.cqMask]
			if cqe.userData&udInternal == 0 || cqe.userData == udIgnore ||
				cqe.res != -int32(unix.EALREADY) {
				continue
			}
			if r.queuePollRemove(cqe.userData&^udInternal) != nil {
				return nil // SQ full and unflushable: drain retries it
			}
			cqe.userData = udIgnore // handled; the CQ slot is ours until head moves
			retried = true
		}
		if !retried {
			return nil
		}
	}
}

// resync re-arms every registration after the kernel reported dropped CQEs
// (EBADR), since an edge may have been lost.
func (r *uring) resync() {
	for fd := range r.fds {
		st := &r.fds[fd]
		if st.state == fdFree {
			continue
		}
		if r.queuePollRemove(pollUD(fd, st.gen)) != nil {
			return
		}
		st.gen = (st.gen + 1) & genMask
		if r.queuePollAdd(fd, st) != nil {
			return
		}
	}
}

// wait is Poller.Wait for io_uring; evs is non-empty and at most MaxEvents
// long. With a timeout it makes one pass (enter, then reap), so completions
// that carry no event end it early with (0, nil); without one it waits again
// until something is reported.
func (r *uring) wait(evs []Event, timeout time.Duration, w *waker) (int, error) {
	for {
		pending := r.sqLocal != atomic.LoadUint32(r.sqHead)
		ready := atomic.LoadUint32(r.cqTail) != r.cqLocal
		overflow := atomic.LoadUint32(r.sqFlags)&ioringSQCQOverflow != 0
		if pending || overflow || (!ready && timeout != 0) {
			var e unix.Errno
			switch {
			case ready || timeout == 0:
				e = r.enter(0, ioringEnterGetEvents, nil, 0)
			case timeout < 0:
				e = r.enter(1, ioringEnterGetEvents, nil, 0)
			default:
				// r.arg points at r.ts, which newURing pinned.
				r.ts = kernelTimespec{sec: int64(timeout / time.Second), nsec: int64(timeout % time.Second)}
				e = r.enter(1, ioringEnterGetEvents|ioringEnterExtArg,
					unsafe.Pointer(&r.arg), unsafe.Sizeof(r.arg))
			}
			switch e {
			case 0, unix.EINTR, unix.EAGAIN, unix.EBUSY, unix.ETIME:
			case unix.EBADR:
				r.resync()
			default:
				return 0, e
			}
		}
		n, woke, err := r.drain(evs, w)
		if n > 0 || woke || err != nil || timeout >= 0 {
			return n, err
		}
	}
}

// drain reaps CQEs into evs, coalescing by fd, and reports whether the wake
// eventfd fired (it is then drained). It stops before the first CQE that
// would need a slot beyond len(evs), leaving it for the next Wait.
func (r *uring) drain(evs []Event, w *waker) (n int, woke bool, err error) {
	r.seq++
	if r.seq == 0 {
		for i := range r.fds {
			r.fds[i].stamp = 0
		}
		r.seq = 1
	}
	head := r.cqLocal
	tail := atomic.LoadUint32(r.cqTail) // acquire: CQE contents are visible
	for ; head != tail; head++ {
		cqe := &r.cqes[head&r.cqMask]
		ud, res, more := cqe.userData, cqe.res, cqe.flags&ioringCQEFMore != 0
		if ud&udInternal != 0 {
			if ud != udIgnore && res == -int32(unix.EALREADY) {
				// The remove raced with an in-flight completion and the
				// poll is still armed: try again.
				if e := r.queuePollRemove(ud &^ udInternal); e != nil {
					break // SQ unflushable; keep the CQE and retry later
				}
			}
			continue
		}
		fd := int(uint32(ud))
		if fd >= len(r.fds) {
			continue
		}
		st := &r.fds[fd]
		if st.state == fdFree || st.gen != uint32(ud>>32) {
			continue // stale: registration was changed or removed
		}
		// A terminated multishot is re-armed unless the registration
		// itself failed (res < 0 other than ECANCELED).
		rearm := !more && (res >= 0 || res == -int32(unix.ECANCELED))
		if st.state == fdWaker {
			woke = true
			if rearm {
				if e := r.queuePollAdd(fd, st); e != nil {
					err = e
				}
			} else if !more {
				err = unix.Errno(-res)
			}
			continue
		}
		var f uint32
		switch {
		case res >= 0:
			f = pollFlags(uint32(res))
		case res != -int32(unix.ECANCELED):
			f = Error | Hangup // registration failed, e.g. EBADF
		}
		if f != 0 && st.stamp != r.seq && n == len(evs) {
			break // evs full; keep this CQE for the next Wait
		}
		if rearm && r.queuePollAdd(fd, st) != nil {
			f |= Error | Hangup // cannot re-arm: surface it rather than hang
		}
		if f == 0 {
			continue
		}
		if st.stamp == r.seq {
			evs[st.idx].Flags |= f
			continue
		}
		st.stamp = r.seq
		st.idx = int32(n)
		evs[n] = Event{FD: int32(fd), Flags: f}
		n++
	}
	r.cqLocal = head
	atomic.StoreUint32(r.cqHead, head) // release: slots may be reused
	if woke {
		w.drain()
	}
	return n, woke, err
}
