//go:build !darwin && !openbsd

package twine

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// rawWritev calls writev(2) with a prebuilt iovec array; unix.Writev takes
// [][]byte and allocates the iovecs, so it is not used on the hot path. It
// returns the bytes written, or -1 and the errno (EINTR is retried).
func rawWritev(fd int, iov []unix.Iovec) (int, error) {
	if len(iov) == 0 {
		return 0, nil
	}
	for {
		n, _, e := unix.Syscall(unix.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
		if e == unix.EINTR {
			continue
		}
		if e != 0 {
			return -1, e
		}
		return int(n), nil
	}
}
