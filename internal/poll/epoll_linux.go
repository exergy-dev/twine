//go:build linux && !twine_uring

package poll

import (
	"math"
	"time"

	"golang.org/x/sys/unix"
)

// Poller is one epoll instance plus its eventfd wakeup channel.
//
// A Poller is owned by one goroutine; only Wake may be called concurrently.
type Poller struct {
	epfd   int
	w      waker
	closed bool
	// kevs is the kernel event buffer, embedded so Wait never allocates and
	// never re-zeroes a large stack array.
	kevs [maxEvents]unix.EpollEvent
}

// New creates the epoll instance and the eventfd used by Wake.
func New() (*Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	p := &Poller{epfd: epfd}
	if err := p.w.open(); err != nil {
		unix.Close(epfd)
		return nil, err
	}
	ev := unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLET, Fd: int32(p.w.fd)}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, p.w.fd, &ev); err != nil {
		p.w.close()
		unix.Close(epfd)
		return nil, err
	}
	return p, nil
}

// interest translates Readable/Writable into epoll event bits. EPOLLRDHUP and
// EPOLLET are always set so that a half-close is reported even when the fd
// has no read interest; EPOLLHUP and EPOLLERR are implicit in epoll.
func interest(flags uint32) uint32 {
	ev := uint32(unix.EPOLLRDHUP | unix.EPOLLET)
	if flags&Readable != 0 {
		ev |= unix.EPOLLIN
	}
	if flags&Writable != 0 {
		ev |= unix.EPOLLOUT
	}
	return ev
}

// Add registers fd edge-triggered with the given interest (Readable and/or
// Writable). Hangup and Error are always reported and need not be requested.
func (p *Poller) Add(fd int, flags uint32) error {
	ev := unix.EpollEvent{Events: interest(flags), Fd: int32(fd)}
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_ADD, fd, &ev)
}

// Mod replaces fd's interest set. If fd is already ready for a newly
// requested direction, the next Wait reports it.
func (p *Poller) Mod(fd int, flags uint32) error {
	ev := unix.EpollEvent{Events: interest(flags), Fd: int32(fd)}
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_MOD, fd, &ev)
}

// Del removes fd. Callers close the fd themselves, after Del.
func (p *Poller) Del(fd int) error {
	var ev unix.EpollEvent // non-nil for kernels < 2.6.9
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_DEL, fd, &ev)
}

// Wait blocks up to timeout (negative = forever, 0 = poll) and fills evs,
// returning the number of events written. At most min(len(evs), 256) events
// are returned per call; an empty evs is rejected with EINVAL.
//
// The wake eventfd is drained internally and never reported, so a Wait woken
// only by Wake returns (0, nil). EINTR is retried internally (with the
// remaining timeout) and never returned. Positive timeouts are rounded up to
// a whole millisecond. Wait does not allocate.
func (p *Poller) Wait(evs []Event, timeout time.Duration) (int, error) {
	max := len(evs)
	if max == 0 {
		return 0, errNoEvents
	}
	if max > maxEvents {
		max = maxEvents
	}
	msec := durationToMs(timeout)
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	var n int
	for {
		var err error
		n, err = unix.EpollWait(p.epfd, p.kevs[:max], msec)
		if err == nil {
			break
		}
		if err != unix.EINTR {
			return 0, err
		}
		if timeout > 0 {
			rem := time.Until(deadline)
			if rem <= 0 {
				return 0, nil
			}
			msec = durationToMs(rem)
		}
	}
	out := 0
	wfd := int32(p.w.fd)
	for i := 0; i < n; i++ {
		kev := &p.kevs[i]
		if kev.Fd == wfd {
			p.w.drain()
			continue
		}
		var f uint32
		if kev.Events&(unix.EPOLLIN|unix.EPOLLPRI) != 0 {
			f |= Readable
		}
		if kev.Events&unix.EPOLLOUT != 0 {
			f |= Writable
		}
		if kev.Events&(unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
			f |= Hangup
		}
		if kev.Events&unix.EPOLLERR != 0 {
			f |= Error
		}
		evs[out] = Event{FD: kev.Fd, Flags: f}
		out++
	}
	return out, nil
}

// Wake makes a blocked or future Wait return promptly. It is safe to call
// from any goroutine and costs a single write(2) on the eventfd; coalescing
// repeated wakeups is the caller's responsibility. Wake must not be called
// after Close (the fd number may have been reused). It does not allocate.
func (p *Poller) Wake() error {
	return p.w.wake()
}

// Close releases the epoll fd and the eventfd. Calling Close more than once
// returns nil.
func (p *Poller) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err := p.w.close()
	if err2 := unix.Close(p.epfd); err == nil {
		err = err2
	}
	return err
}

// durationToMs converts a Wait timeout to the millisecond argument of
// epoll_wait. Negative durations mean "block forever" (-1). Positive
// durations are rounded up so that a sub-millisecond timeout never becomes a
// zero (non-blocking) poll, and are capped at math.MaxInt32.
func durationToMs(d time.Duration) int {
	if d < 0 {
		return -1
	}
	ms := d / time.Millisecond
	if d%time.Millisecond != 0 {
		ms++
	}
	if ms > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(ms)
}
