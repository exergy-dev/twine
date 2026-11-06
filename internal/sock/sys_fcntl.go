//go:build darwin || netbsd || openbsd

package sock

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// newSocket creates a socket and marks it non-blocking and close-on-exec.
// darwin has no SOCK_NONBLOCK/SOCK_CLOEXEC, so the fd is created under
// syscall.ForkLock (as the net package does) to keep it from leaking into a
// concurrently forked child.
func newSocket(fam, typ int) (int, error) {
	syscall.ForkLock.RLock()
	fd, err := unix.Socket(fam, typ, 0)
	if err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// accept is accept(2) followed by CloseOnExec and SetNonblock: golang.org/x/sys
// has no Accept4 for darwin, netbsd or openbsd.
func accept(lfd int) (int, unix.Sockaddr, error) {
	syscall.ForkLock.RLock()
	fd, sa, err := unix.Accept(lfd)
	if err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, nil, err
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return -1, nil, err
	}
	return fd, sa, nil
}
