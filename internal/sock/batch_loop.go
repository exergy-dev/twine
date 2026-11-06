package sock

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// recvLoop is the portable RecvMmsg: one recvfrom(2) per message until
// EAGAIN. It allocates one unix.Sockaddr per datagram received (the
// golang.org/x/sys Recvfrom contract); the EAGAIN path does not allocate.
func (b *Batch) recvLoop(fd int) (int, error) {
	for i := range b.msgs {
		m := &b.msgs[i]
		var (
			n    int
			from unix.Sockaddr
			err  error
		)
		for {
			n, from, err = unix.Recvfrom(fd, m.Buf, unix.MSG_DONTWAIT)
			if err != unix.EINTR {
				break
			}
		}
		if err != nil {
			if i == 0 {
				return 0, err
			}
			return i, nil
		}
		m.N = n
		storeSockaddr(m, from)
	}
	return len(b.msgs), nil
}

// sendLoop is the portable SendMmsg: one sendto(2) per message. It stops at
// the first error; the error is returned only if no message was sent. An
// IPv4 destination rejected by an AF_INET6 socket (BSD dual-stack) is retried
// once in IPv4-mapped form. It does not allocate.
func (b *Batch) sendLoop(fd int, n int) (int, error) {
	if n > len(b.msgs) {
		n = len(b.msgs)
	}
	for i := 0; i < n; i++ {
		m := &b.msgs[i]
		to := b.sockaddr(m, false)
		var err error
		for {
			err = unix.Sendto(fd, m.Buf, unix.MSG_DONTWAIT, to)
			if err == unix.EINTR {
				continue
			}
			if (err == unix.EAFNOSUPPORT || err == unix.EINVAL) && to == unix.Sockaddr(&b.sa4) {
				to = b.sockaddr(m, true)
				continue
			}
			break
		}
		if err != nil {
			if i == 0 {
				return 0, err
			}
			return i, nil
		}
		m.N = len(m.Buf)
	}
	return n, nil
}

// sockaddr converts m's raw address into the batch's reusable Sockaddr
// storage. mapped forces the IPv4-mapped AF_INET6 form for an IPv4 address.
// Returns nil when m has no address.
func (b *Batch) sockaddr(m *Message, mapped bool) unix.Sockaddr {
	if m.AddrLen == 0 {
		return nil
	}
	switch m.Addr.Family {
	case unix.AF_INET:
		r := (*unix.RawSockaddrInet4)(unsafe.Pointer(&m.Addr))
		if mapped {
			b.sa6.Port = getPort(&r.Port)
			b.sa6.ZoneId = 0
			b.sa6.Addr = [16]byte{10: 0xff, 11: 0xff}
			copy(b.sa6.Addr[12:], r.Addr[:])
			return &b.sa6
		}
		b.sa4.Port = getPort(&r.Port)
		b.sa4.Addr = r.Addr
		return &b.sa4
	case unix.AF_INET6:
		b.sa6.Port = getPort(&m.Addr.Port)
		b.sa6.ZoneId = m.Addr.Scope_id
		b.sa6.Addr = m.Addr.Addr
		return &b.sa6
	}
	return nil
}

// storeSockaddr writes sa into m's raw address storage.
func storeSockaddr(m *Message, sa unix.Sockaddr) {
	m.Addr = unix.RawSockaddrInet6{}
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		r := (*unix.RawSockaddrInet4)(unsafe.Pointer(&m.Addr))
		r.Family = unix.AF_INET
		setLen4(r)
		putPort(&r.Port, sa.Port)
		r.Addr = sa.Addr
		m.AddrLen = unix.SizeofSockaddrInet4
	case *unix.SockaddrInet6:
		r := &m.Addr
		r.Family = unix.AF_INET6
		setLen6(r)
		putPort(&r.Port, sa.Port)
		r.Addr = sa.Addr
		r.Scope_id = sa.ZoneId
		m.AddrLen = unix.SizeofSockaddrInet6
	default:
		m.AddrLen = 0
	}
}
