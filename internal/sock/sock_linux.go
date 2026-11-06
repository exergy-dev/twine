package sock

import (
	"time"

	"golang.org/x/sys/unix"
)

// SetDeferAccept sets TCP_DEFER_ACCEPT on a listening socket: accept only
// completes once data has arrived or d has elapsed. d is rounded up to whole
// seconds; d <= 0 disables it.
func SetDeferAccept(fd int, d time.Duration) error {
	secs := 0
	if d > 0 {
		secs = int((d + time.Second - 1) / time.Second)
	}
	return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_DEFER_ACCEPT, secs)
}

// SetBusyPoll sets SO_BUSY_POLL to d (in microseconds, rounded up; d <= 0
// disables it). Raising it above net.core.busy_read needs CAP_NET_ADMIN and
// fails with EPERM otherwise.
func SetBusyPoll(fd int, d time.Duration) error {
	usec := 0
	if d > 0 {
		usec = int((d + time.Microsecond - 1) / time.Microsecond)
	}
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_BUSY_POLL, usec)
}

// Linux raw sockaddrs have no length byte.
func setLen4(*unix.RawSockaddrInet4) {}
func setLen6(*unix.RawSockaddrInet6) {}
