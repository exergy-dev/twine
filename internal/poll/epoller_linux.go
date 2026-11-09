//go:build linux

package poll

import (
	"math"
	"time"

	"golang.org/x/sys/unix"
)

// epoller is one epoll instance with an embedded kernel event buffer. It is
// the whole backend of the default Linux Poller and the fallback of the
// io_uring Poller.
type epoller struct {
	fd int
	// kevs is the kernel event buffer, embedded so wait never allocates and
	// never re-zeroes a large stack array.
	kevs [MaxEvents]unix.EpollEvent
}

// open creates the epoll instance and registers the wake eventfd
// edge-triggered for reading.
func (e *epoller) open(wakeFD int) error {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return err
	}
	ev := unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLET, Fd: int32(wakeFD)}
	if err := unix.EpollCtl(fd, unix.EPOLL_CTL_ADD, wakeFD, &ev); err != nil {
		unix.Close(fd)
		return err
	}
	e.fd = fd
	return nil
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

func (e *epoller) add(fd int, flags uint32) error {
	ev := unix.EpollEvent{Events: interest(flags), Fd: int32(fd)}
	return unix.EpollCtl(e.fd, unix.EPOLL_CTL_ADD, fd, &ev)
}

func (e *epoller) mod(fd int, flags uint32) error {
	ev := unix.EpollEvent{Events: interest(flags), Fd: int32(fd)}
	return unix.EpollCtl(e.fd, unix.EPOLL_CTL_MOD, fd, &ev)
}

func (e *epoller) del(fd int) error {
	var ev unix.EpollEvent // non-nil for kernels < 2.6.9
	return unix.EpollCtl(e.fd, unix.EPOLL_CTL_DEL, fd, &ev)
}

func (e *epoller) close() error {
	return unix.Close(e.fd)
}

// wait is Poller.Wait for epoll. Events on the wake eventfd w drain it and
// are not reported.
func (e *epoller) wait(evs []Event, timeout time.Duration, w *waker) (int, error) {
	if len(evs) == 0 {
		return 0, unix.EINVAL
	}
	evs = evs[:min(len(evs), MaxEvents)]
	msec := durationToMs(timeout)
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	var n int
	for {
		var err error
		n, err = unix.EpollWait(e.fd, e.kevs[:len(evs)], msec)
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
	wfd := int32(w.fd)
	for i := 0; i < n; i++ {
		kev := &e.kevs[i]
		if kev.Fd == wfd {
			w.drain()
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
