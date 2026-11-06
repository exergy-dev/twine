//go:build darwin || dragonfly || freebsd

package poll

import "golang.org/x/sys/unix"

// wakeIdent is the EVFILT_USER identifier used for wakeups. EVFILT_USER
// identifiers live in their own namespace, so it cannot clash with an fd.
const wakeIdent = 0

// waker is the kqueue wakeup channel: an EVFILT_USER event registered with
// EV_CLEAR, so retrieving it resets it and no drain is needed.
//
// All fields are immutable after open, so wake may be called concurrently.
type waker struct {
	kq   int
	trig [1]unix.Kevent_t // prebuilt NOTE_TRIGGER change
}

// open registers the EVFILT_USER event on kq.
func (w *waker) open(kq int) error {
	var add [1]unix.Kevent_t
	unix.SetKevent(&add[0], wakeIdent, unix.EVFILT_USER, unix.EV_ADD|unix.EV_CLEAR)
	if _, err := unix.Kevent(kq, add[:], nil, nil); err != nil {
		return err
	}
	w.kq = kq
	unix.SetKevent(&w.trig[0], wakeIdent, unix.EVFILT_USER, 0)
	w.trig[0].Fflags = unix.NOTE_TRIGGER
	return nil
}

// wake triggers the user event: a single kevent(2).
func (w *waker) wake() error {
	for {
		_, err := unix.Kevent(w.kq, w.trig[:], nil, nil)
		if err != unix.EINTR {
			return err
		}
	}
}

// consume reports whether kev is the wakeup event. EV_CLEAR already reset it.
func (w *waker) consume(kev *unix.Kevent_t) bool {
	return kev.Filter == unix.EVFILT_USER && kev.Ident == wakeIdent
}

// close is a no-op: the user event dies with the kqueue.
func (w *waker) close() error { return nil }
