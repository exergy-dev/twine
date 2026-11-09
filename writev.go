package twine

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// writevSegments writes a prebuilt iovec array with one write(2) per
// segment. It is rawWritev where raw system calls are unsupported (see
// writev_libc.go) and is compiled everywhere so the Linux tests cover it.
// It stops at the first short write and returns the bytes written, or -1
// and the errno when nothing was written (EINTR is retried), matching the
// writev(2) contract the callers expect.
func writevSegments(fd int, iov []unix.Iovec) (int, error) {
	total := 0
	for i := range iov {
		if iov[i].Len == 0 {
			continue
		}
		seg := unsafe.Slice(iov[i].Base, int(iov[i].Len))
		n, err := unix.Write(fd, seg)
		for err == unix.EINTR {
			n, err = unix.Write(fd, seg)
		}
		if n > 0 {
			total += n
		}
		if err != nil {
			if total > 0 {
				return total, nil
			}
			return -1, err
		}
		if n < len(seg) {
			break
		}
	}
	return total, nil
}
