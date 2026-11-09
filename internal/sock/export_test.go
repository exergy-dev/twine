package sock

import (
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

// AddrOf converts a received message's raw sockaddr to a *net.UDPAddr,
// keeping the IPv6 zone that FillAddr drops. It allocates. Returns nil when
// AddrLen is 0 or the family is unsupported. Test helper.
func (m *Message) AddrOf() *net.UDPAddr {
	if m.AddrLen == 0 {
		return nil
	}
	switch m.Addr.Family {
	case unix.AF_INET:
		r := (*unix.RawSockaddrInet4)(unsafe.Pointer(&m.Addr))
		return &net.UDPAddr{IP: ip4(r.Addr), Port: getPort(&r.Port)}
	case unix.AF_INET6:
		r := &m.Addr
		return &net.UDPAddr{IP: ip6(r.Addr), Port: getPort(&r.Port), Zone: zoneName(r.Scope_id)}
	}
	return nil
}
