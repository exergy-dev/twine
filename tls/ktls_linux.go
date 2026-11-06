//go:build linux

package tls

import (
	"errors"

	"golang.org/x/sys/unix"
)

// probeKTLS reports whether the kernel accepts the "tls" ULP. The ULP can
// only be attached to an established socket, so the probe connects a
// throwaway loopback pair. If that cannot be set up it falls back to an
// unconnected socket, where ENOTCONN means the ULP exists (tls_init rejects
// non-established sockets) and ENOENT means it does not.
func probeKTLS() bool {
	if ok, err := probeConnected(); err == nil {
		return ok
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return false
	}
	defer unix.Close(fd)
	err = unix.SetsockoptString(fd, unix.SOL_TCP, unix.TCP_ULP, "tls")
	return err == nil || errors.Is(err, unix.ENOTCONN)
}

// probeConnected attaches the ULP to a connected loopback socket. A non-nil
// error means the probe could not be set up (not that kTLS is missing).
func probeConnected() (bool, error) {
	lfd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return false, err
	}
	defer unix.Close(lfd)
	if err := unix.Bind(lfd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		return false, err
	}
	if err := unix.Listen(lfd, 1); err != nil {
		return false, err
	}
	sa, err := unix.Getsockname(lfd)
	if err != nil {
		return false, err
	}
	cfd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return false, err
	}
	defer unix.Close(cfd)
	// Blocking connect to a listening loopback socket completes from the
	// backlog without an accept.
	if err := unix.Connect(cfd, sa); err != nil {
		return false, err
	}
	return unix.SetsockoptString(cfd, unix.SOL_TCP, unix.TCP_ULP, "tls") == nil, nil
}
