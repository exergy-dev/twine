//go:build darwin || openbsd

package twine

import "golang.org/x/sys/unix"

// rawWritev writes a prebuilt iovec array. Apple supports system calls only
// through libSystem, OpenBSD only through libc, and the pinned x/sys has no
// writev wrapper for either, so the segments go out with one write(2) each.
func rawWritev(fd int, iov []unix.Iovec) (int, error) { return writevSegments(fd, iov) }
