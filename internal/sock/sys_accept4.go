//go:build linux || dragonfly || freebsd

package sock

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// newSocket creates a non-blocking, close-on-exec socket atomically.
func newSocket(fam, typ int) (int, error) {
	return unix.Socket(fam, typ|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
}

// accept is one accept4(SOCK_NONBLOCK|SOCK_CLOEXEC) call. It issues the
// syscall directly because unix.Accept4 moves its sockaddr buffer to the
// heap, which would allocate on every EAGAIN.
func accept(lfd int) (int, unix.Sockaddr, error) {
	var rsa unix.RawSockaddrAny
	l := uint32(unix.SizeofSockaddrAny)
	r, _, e := unix.Syscall6(unix.SYS_ACCEPT4, uintptr(lfd),
		uintptr(unsafe.Pointer(&rsa)), uintptr(unsafe.Pointer(&l)),
		unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0, 0)
	if e != 0 {
		return -1, nil, errnoErr(e)
	}
	fd := int(r)
	sa := rawToSockaddr(&rsa)
	if sa == nil {
		unix.Close(fd)
		return -1, nil, unix.EAFNOSUPPORT
	}
	return fd, sa, nil
}

// Preallocated errors so the error path never boxes an Errno.
var (
	errEAGAIN error = unix.EAGAIN
	errEINVAL error = unix.EINVAL
	errENOENT error = unix.ENOENT
)

// errnoErr converts a raw errno to an error without allocating for the
// common cases.
func errnoErr(e unix.Errno) error {
	switch e {
	case 0:
		return nil
	case unix.EAGAIN:
		return errEAGAIN
	case unix.EINVAL:
		return errEINVAL
	case unix.ENOENT:
		return errENOENT
	}
	return e
}

// rawToSockaddr converts an IPv4/IPv6 raw sockaddr to a unix.Sockaddr (it
// allocates). Returns nil for other families.
func rawToSockaddr(rsa *unix.RawSockaddrAny) unix.Sockaddr {
	switch rsa.Addr.Family {
	case unix.AF_INET:
		r := (*unix.RawSockaddrInet4)(unsafe.Pointer(rsa))
		return &unix.SockaddrInet4{Port: getPort(&r.Port), Addr: r.Addr}
	case unix.AF_INET6:
		r := (*unix.RawSockaddrInet6)(unsafe.Pointer(rsa))
		return &unix.SockaddrInet6{Port: getPort(&r.Port), ZoneId: r.Scope_id, Addr: r.Addr}
	}
	return nil
}
