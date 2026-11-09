//go:build netbsd || openbsd

package poll

import "golang.org/x/sys/unix"

// waker is the kqueue wakeup channel for systems without EVFILT_USER in
// golang.org/x/sys (NetBSD, OpenBSD): a non-blocking pipe whose read end is
// registered EVFILT_READ|EV_CLEAR and drained by Wait.
//
// The fd fields and one are immutable after open, so wake may be called
// concurrently with consume and with other wake calls.
type waker struct {
	r, w int
	one  [1]byte
	buf  [64]byte // drain target; only touched by the owning goroutine
}

// open creates the pipe and registers its read end on kq.
func (w *waker) open(kq int) error {
	var fds [2]int
	if err := unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		return err
	}
	var add [1]unix.Kevent_t
	unix.SetKevent(&add[0], fds[0], unix.EVFILT_READ, unix.EV_ADD|unix.EV_CLEAR)
	if _, err := unix.Kevent(kq, add[:], nil, nil); err != nil {
		unix.Close(fds[0])
		unix.Close(fds[1])
		return err
	}
	w.r, w.w = fds[0], fds[1]
	w.one[0] = 1
	return nil
}

// wake writes one byte to the pipe: a single write(2). A full pipe (EAGAIN)
// means a wakeup is already pending, so it is not an error.
func (w *waker) wake() error {
	for {
		_, err := unix.Write(w.w, w.one[:])
		switch err {
		case unix.EINTR:
			continue
		case unix.EAGAIN:
			return nil
		}
		return err
	}
}

// consume reports whether kev is the wakeup pipe and, if so, drains it.
func (w *waker) consume(kev *unix.Kevent_t) bool {
	if kev.Filter != unix.EVFILT_READ || int(kev.Ident) != w.r {
		return false
	}
	for {
		n, err := unix.Read(w.r, w.buf[:])
		if err == unix.EINTR {
			continue
		}
		if err != nil || n < len(w.buf) {
			return true
		}
	}
}

// close closes both ends of the pipe.
func (w *waker) close() error {
	err := unix.Close(w.r)
	if err2 := unix.Close(w.w); err == nil {
		err = err2
	}
	return err
}
