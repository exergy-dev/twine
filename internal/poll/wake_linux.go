//go:build linux

package poll

import (
	"encoding/binary"

	"golang.org/x/sys/unix"
)

// waker is the Linux wakeup channel: a non-blocking eventfd. It is kept
// independent of the epoll backend so an io_uring backend can reuse it.
//
// All fields are immutable after open, so wake may be called concurrently
// with drain and with other wake calls.
type waker struct {
	fd  int
	one [8]byte // the value 1 as a host-order uint64
	buf [8]byte // drain target; only touched by the owning goroutine
}

// open creates the eventfd.
func (w *waker) open() error {
	fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		return err
	}
	w.fd = fd
	binary.NativeEndian.PutUint64(w.one[:], 1)
	return nil
}

// wake adds 1 to the eventfd counter: a single write(2). EAGAIN means the
// counter is saturated, so the reader is already guaranteed to wake.
func (w *waker) wake() error {
	_, err := unix.Write(w.fd, w.one[:])
	if err == unix.EAGAIN {
		return nil
	}
	return err
}

// drain resets the eventfd counter to zero. EAGAIN (already drained) is
// ignored.
func (w *waker) drain() {
	for {
		_, err := unix.Read(w.fd, w.buf[:])
		if err != unix.EINTR {
			return
		}
	}
}

// close closes the eventfd.
func (w *waker) close() error {
	return unix.Close(w.fd)
}
