//go:build linux && !twine_uring

package poll

import "time"

// Poller is one epoll instance plus its eventfd wakeup channel.
//
// A Poller is owned by one goroutine; only Wake may be called concurrently.
type Poller struct {
	epoller
	w      waker
	closed bool
}

// New creates the epoll instance and the eventfd used by Wake.
func New() (*Poller, error) {
	p := &Poller{}
	if err := p.w.open(); err != nil {
		return nil, err
	}
	if err := p.epoller.open(p.w.fd); err != nil {
		_ = p.w.close()
		return nil, err
	}
	return p, nil
}

// Add registers fd edge-triggered with the given interest (Readable and/or
// Writable). Hangup and Error are always reported and need not be requested.
func (p *Poller) Add(fd int, flags uint32) error { return p.add(fd, flags) }

// Mod replaces fd's interest set. If fd is already ready for a newly
// requested direction, the next Wait reports it.
func (p *Poller) Mod(fd int, flags uint32) error { return p.mod(fd, flags) }

// Del removes fd. Callers close the fd themselves, after Del.
func (p *Poller) Del(fd int) error { return p.del(fd) }

// Wait blocks up to timeout (negative = forever, 0 = poll) and fills evs,
// returning the number of events written. At most min(len(evs), MaxEvents)
// events are returned per call; an empty evs is rejected with EINVAL.
//
// The wake eventfd is drained internally and never reported, so a Wait woken
// only by Wake returns (0, nil). EINTR is retried internally (with the
// remaining timeout) and never returned. Positive timeouts are rounded up to
// a whole millisecond. Wait does not allocate.
func (p *Poller) Wait(evs []Event, timeout time.Duration) (int, error) {
	return p.wait(evs, timeout, &p.w)
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
	if err2 := p.epoller.close(); err == nil {
		err = err2
	}
	return err
}
