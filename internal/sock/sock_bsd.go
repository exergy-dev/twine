//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package sock

import (
	"time"

	"golang.org/x/sys/unix"
)

// SetDeferAccept is a Linux-only option (TCP_DEFER_ACCEPT); it is a no-op
// returning nil on this platform.
func SetDeferAccept(fd int, d time.Duration) error { return nil }

// SetBusyPoll is a Linux-only option (SO_BUSY_POLL); it is a no-op returning
// nil on this platform.
func SetBusyPoll(fd int, d time.Duration) error { return nil }

// BSD raw sockaddrs carry their own length.
func setLen4(r *unix.RawSockaddrInet4) { r.Len = unix.SizeofSockaddrInet4 }
func setLen6(r *unix.RawSockaddrInet6) { r.Len = unix.SizeofSockaddrInet6 }
