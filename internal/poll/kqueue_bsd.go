//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package poll

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Poller is one kqueue instance plus its wakeup channel (EVFILT_USER, or a
// non-blocking pipe where EVFILT_USER is unavailable).
//
// A Poller is owned by one goroutine; only Wake may be called concurrently.
type Poller struct {
	kq     int
	w      waker
	closed bool
	ts     unix.Timespec
	chg    [2]unix.Kevent_t
	rcpt   [2]unix.Kevent_t
	// kevs is the kernel event buffer, embedded so Wait never allocates and
	// never re-zeroes a large stack array.
	kevs [maxEvents]unix.Kevent_t
}

// New creates the kqueue instance and the wakeup channel used by Wake.
func New() (*Poller, error) {
	syscall.ForkLock.RLock()
	kq, err := unix.Kqueue()
	if err == nil {
		unix.CloseOnExec(kq)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, err
	}
	p := &Poller{kq: kq}
	if err := p.w.open(kq); err != nil {
		unix.Close(kq)
		return nil, err
	}
	return p, nil
}

// apply submits changes to the kqueue. ENOENT (deleting a filter that is not
// registered) is ignored. With EV_RECEIPT every change yields a receipt in
// p.rcpt and pending events are left untouched; without it each change is
// submitted on its own so that one failure cannot hide another.
func (p *Poller) apply(changes []unix.Kevent_t) error {
	if len(changes) == 0 {
		return nil
	}
	if evReceipt != 0 {
		for i := range changes {
			changes[i].Flags |= evReceipt
		}
		zero := &p.ts
		*zero = unix.Timespec{}
		n, err := unix.Kevent(p.kq, changes, p.rcpt[:len(changes)], zero)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			r := &p.rcpt[i]
			if r.Flags&unix.EV_ERROR != 0 && r.Data != 0 {
				if errno := unix.Errno(r.Data); errno != unix.ENOENT {
					return errno
				}
			}
		}
		return nil
	}
	for i := range changes {
		_, err := unix.Kevent(p.kq, changes[i:i+1], nil, nil)
		if err != nil && err != unix.ENOENT {
			return err
		}
	}
	return nil
}

// changes builds the kevent change list for fd: for each of the READ and
// WRITE filters, EV_ADD|EV_CLEAR when wanted, otherwise EV_DELETE if del is
// set (Mod/Del) or nothing (Add).
func (p *Poller) changes(fd int, flags uint32, del bool) []unix.Kevent_t {
	n := 0
	if flags&Readable != 0 {
		unix.SetKevent(&p.chg[n], fd, unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
		n++
	} else if del {
		unix.SetKevent(&p.chg[n], fd, unix.EVFILT_READ, unix.EV_DELETE)
		n++
	}
	if flags&Writable != 0 {
		unix.SetKevent(&p.chg[n], fd, unix.EVFILT_WRITE, unix.EV_ADD|unix.EV_CLEAR)
		n++
	} else if del {
		unix.SetKevent(&p.chg[n], fd, unix.EVFILT_WRITE, unix.EV_DELETE)
		n++
	}
	for i := 0; i < n; i++ {
		p.chg[i].Fflags = 0
		p.chg[i].Data = 0
	}
	return p.chg[:n]
}

// Add registers fd edge-triggered (EV_CLEAR) with the given interest
// (Readable and/or Writable). Hangup and Error are reported on the registered
// filters; an fd added with no interest receives no notifications on kqueue.
func (p *Poller) Add(fd int, flags uint32) error {
	return p.apply(p.changes(fd, flags, false))
}

// Mod replaces fd's interest set: wanted filters are (re-)added, the others
// are deleted (ENOENT ignored). Re-adding a filter re-evaluates readiness, so
// the next Wait reports a direction that is already ready.
func (p *Poller) Mod(fd int, flags uint32) error {
	return p.apply(p.changes(fd, flags, true))
}

// Del removes fd. Callers close the fd themselves, after Del.
func (p *Poller) Del(fd int) error {
	return p.apply(p.changes(fd, 0, true))
}

// Wait blocks up to timeout (negative = forever, 0 = poll) and fills evs,
// returning the number of events written. At most min(len(evs), 256) events
// are returned per call; an empty evs is rejected with EINVAL.
//
// The wakeup channel is consumed internally and never reported, so a Wait
// woken only by Wake returns (0, nil). EINTR is retried internally (with the
// remaining timeout) and never returned. READ and WRITE kevents for the same
// fd are merged into one Event when adjacent. Wait does not allocate.
func (p *Poller) Wait(evs []Event, timeout time.Duration) (int, error) {
	max := len(evs)
	if max == 0 {
		return 0, errNoEvents
	}
	if max > maxEvents {
		max = maxEvents
	}
	var ts *unix.Timespec
	var deadline time.Time
	if timeout >= 0 {
		p.ts = unix.NsecToTimespec(int64(timeout))
		ts = &p.ts
		if timeout > 0 {
			deadline = time.Now().Add(timeout)
		}
	}
	var n int
	for {
		var err error
		n, err = unix.Kevent(p.kq, nil, p.kevs[:max], ts)
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
			p.ts = unix.NsecToTimespec(int64(rem))
		}
	}
	out := 0
	for i := 0; i < n; i++ {
		kev := &p.kevs[i]
		if p.w.consume(kev) {
			continue
		}
		var f uint32
		switch kev.Filter {
		case unix.EVFILT_READ:
			f = Readable
		case unix.EVFILT_WRITE:
			f = Writable
		}
		if kev.Flags&unix.EV_EOF != 0 {
			f |= Hangup
			if kev.Fflags != 0 {
				f |= Error // socket error (e.g. ECONNREFUSED) in fflags
			}
		}
		if kev.Flags&unix.EV_ERROR != 0 {
			f |= Error
		}
		fd := int32(kev.Ident)
		if out > 0 && evs[out-1].FD == fd {
			evs[out-1].Flags |= f
			continue
		}
		evs[out] = Event{FD: fd, Flags: f}
		out++
	}
	return out, nil
}

// Wake makes a blocked or future Wait return promptly. It is safe to call
// from any goroutine and costs a single kevent(2) (EVFILT_USER NOTE_TRIGGER)
// or write(2) (pipe fallback; a full pipe is not an error). Coalescing
// repeated wakeups is the caller's responsibility. Wake must not be called
// after Close. It does not allocate.
func (p *Poller) Wake() error {
	return p.w.wake()
}

// Close releases the kqueue fd and the wakeup channel. Calling Close more
// than once returns nil.
func (p *Poller) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err := p.w.close()
	if err2 := unix.Close(p.kq); err == nil {
		err = err2
	}
	return err
}
