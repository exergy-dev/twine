// Package poll is a thin, allocation-free abstraction over the kernel
// readiness APIs used by twine's event loops: epoll on Linux and kqueue on
// darwin, dragonfly, freebsd, netbsd and openbsd.
//
// Both backends are edge-triggered (EPOLLET on Linux, EV_CLEAR on kqueue): a
// registered fd is reported once per readiness transition, so the caller must
// read or write until EAGAIN before expecting another event for that fd.
//
// A Poller is owned by a single goroutine (the loop). Only Wake may be called
// from other goroutines.
//
// Backend differences the caller must tolerate:
//
//   - Hangup may be reported with or without Readable. On Linux a peer
//     half-close is EPOLLIN|EPOLLRDHUP (Readable|Hangup); a full hangup while
//     reading is paused is reported as Hangup alone. On kqueue, EV_EOF is only
//     delivered for a registered filter, so an fd with no interest (flags 0)
//     receives no Hangup notification at all.
//   - On kqueue the READ and WRITE filters of one fd are distinct kevents.
//     Wait merges them when the kernel returns them adjacently, but the same
//     FD may still appear twice in one Wait result.
//   - A failed non-blocking connect is reported as Writable|Hangup|Error on
//     epoll and as Writable|Hangup|Error (EV_EOF with a non-zero socket error
//     in fflags) on kqueue.
//   - On io_uring (build tag twine_uring) a wakeup reports only the
//     direction(s) that triggered it, e.g. Readable without Writable on an
//     already-writable socket; do not rely on Writable being re-reported.
//     Add on a closed fd succeeds and the next Wait reports Error|Hangup.
//   - Mod re-evaluates readiness on both backends: if the fd is already ready
//     for a newly requested direction, the next Wait reports it.
//   - Re-registration differs: epoll Add of an already registered fd fails
//     with EEXIST and Mod of an unregistered fd fails with ENOENT, while
//     kqueue silently modifies/adds in both cases. Callers should Add once,
//     Mod afterwards, and Del before close.
//   - The epoll timeout is rounded up to a whole millisecond; kqueue uses the
//     full nanosecond timeout.
package poll

// Event flags. Several may be set at once in Event.Flags.
const (
	// Readable reports that the fd has data to read (EPOLLIN|EPOLLPRI /
	// EVFILT_READ). As an interest flag it requests read readiness.
	Readable uint32 = 1 << iota
	// Writable reports that the fd can accept writes (EPOLLOUT /
	// EVFILT_WRITE). As an interest flag it requests write readiness.
	Writable
	// Hangup reports that the peer closed the connection or this side was
	// shut down (EPOLLHUP|EPOLLRDHUP / EV_EOF). It is always reported and
	// need not be requested.
	Hangup
	// Error reports a pending socket error (EPOLLERR / EV_ERROR, or EV_EOF
	// with a non-zero error on kqueue). It is always reported and need not be
	// requested.
	Error
)

// MaxEvents caps the number of events one Wait call returns: Wait fills at
// most min(len(evs), MaxEvents) entries, so a caller's event array needs no
// more than MaxEvents. The kernel event buffer of that size is embedded in
// the Poller so Wait never allocates.
const MaxEvents = 256

// Event is one ready notification. Wait fills a caller-provided slice.
type Event struct {
	// FD is the file descriptor the event refers to.
	FD int32
	// Flags is a combination of Readable, Writable, Hangup and Error.
	Flags uint32
}
