// Package sock holds the raw socket helpers used by twine's event loops:
// non-blocking listen/accept/connect, socket options, address conversion and
// batched UDP I/O.
//
// Every socket this package creates or accepts is non-blocking and
// close-on-exec (SOCK_NONBLOCK|SOCK_CLOEXEC where the platform supports it,
// fcntl otherwise). The net package is used only to parse and represent
// addresses; all I/O goes through golang.org/x/sys/unix.
//
// Address family selection for the "tcp"/"udp" networks: a wildcard host
// (":9000", "[::]:9000") listens on a dual-stack AF_INET6 socket
// (IPV6_V6ONLY=0), falling back to AF_INET when IPv6 is unavailable; an IPv4
// literal (including "0.0.0.0") uses AF_INET; any other IPv6 literal uses
// AF_INET6. "tcp4"/"udp4" always use AF_INET and "tcp6"/"udp6" always use
// AF_INET6 with IPV6_V6ONLY=1.
package sock

import (
	"net"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Listen creates, binds and listens a non-blocking TCP socket. network is
// "tcp", "tcp4" or "tcp6"; addr is a host:port as accepted by
// net.ResolveTCPAddr (":9000", "0.0.0.0:9000", "[::1]:9000"). SO_REUSEADDR
// is always set; SO_REUSEPORT when reusePort. backlog <= 0 means
// unix.SOMAXCONN. Returns the fd and the bound address (so port 0 is
// resolved).
func Listen(network, addr string, reusePort bool, backlog int) (fd int, laddr *net.TCPAddr, err error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return -1, nil, net.UnknownNetworkError(network)
	}
	ta, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return -1, nil, err
	}
	if backlog <= 0 {
		backlog = unix.SOMAXCONN
	}
	opts := func(fd int) error {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
			return err
		}
		if reusePort {
			return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
		return nil
	}
	fd, err = bindAny(network, unix.SOCK_STREAM, ta.IP, ta.Port, ta.Zone, opts)
	if err != nil {
		return -1, nil, err
	}
	if err := unix.Listen(fd, backlog); err != nil {
		unix.Close(fd)
		return -1, nil, err
	}
	laddr = LocalTCP(fd)
	if laddr == nil {
		unix.Close(fd)
		return -1, nil, unix.EINVAL
	}
	return fd, laddr, nil
}

// ListenUDP binds a non-blocking UDP socket, with the same network and
// address rules as Listen. SO_REUSEPORT is set when reusePort; SO_REUSEADDR
// is not set (on UDP it would let unrelated sockets share the port).
func ListenUDP(network, addr string, reusePort bool) (fd int, laddr *net.UDPAddr, err error) {
	switch network {
	case "udp", "udp4", "udp6":
	default:
		return -1, nil, net.UnknownNetworkError(network)
	}
	ua, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return -1, nil, err
	}
	opts := func(fd int) error {
		if reusePort {
			return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}
		return nil
	}
	fd, err = bindAny(network, unix.SOCK_DGRAM, ua.IP, ua.Port, ua.Zone, opts)
	if err != nil {
		return -1, nil, err
	}
	laddr = localUDP(fd)
	if laddr == nil {
		unix.Close(fd)
		return -1, nil, unix.EINVAL
	}
	return fd, laddr, nil
}

// Accept accepts one connection from the non-blocking listener lfd. The new
// fd is non-blocking and close-on-exec (accept4 where available, accept +
// fcntl on darwin, netbsd and openbsd). It returns unix.EAGAIN when the
// backlog is empty; EINTR and ECONNABORTED are retried internally. The
// EAGAIN path does not allocate.
func Accept(lfd int) (fd int, sa unix.Sockaddr, err error) {
	for {
		fd, sa, err = accept(lfd)
		if err == nil {
			return fd, sa, nil
		}
		if err != unix.EINTR && err != unix.ECONNABORTED {
			return -1, nil, err
		}
	}
}

// ConnectAddr creates a non-blocking TCP socket and starts connecting to
// raddr, which is already resolved: it never does a name lookup (the root
// package resolves host names off the loop thread). It returns
// inProgress=true for EINPROGRESS (the common case); the caller waits for
// Writable (or Hangup/Error) and then calls ConnectError. la may be nil (no
// bind). A nil or unspecified raddr.IP dials the loopback address. On error
// the socket is closed and fd is -1.
func ConnectAddr(network string, raddr, la *net.TCPAddr) (fd int, inProgress bool, err error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return -1, false, net.UnknownNetworkError(network)
	}
	fam := dialFamily(network, raddr.IP, la)
	ip := raddr.IP
	if ip == nil || ip.IsUnspecified() {
		ip = net.IPv4(127, 0, 0, 1)
		if fam == unix.AF_INET6 {
			ip = net.IPv6loopback
		}
	}
	rsa, err := toSockaddr(fam, ip, raddr.Port, raddr.Zone)
	if err != nil {
		return -1, false, err
	}
	fd, err = newSocket(fam, unix.SOCK_STREAM)
	if err != nil {
		return -1, false, err
	}
	if fam == unix.AF_INET6 && network == "tcp6" {
		_ = unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, 1)
	}
	if la != nil {
		lsa, err := toSockaddr(fam, la.IP, la.Port, la.Zone)
		if err == nil {
			err = unix.Bind(fd, lsa)
		}
		if err != nil {
			unix.Close(fd)
			return -1, false, err
		}
	}
	switch err = unix.Connect(fd, rsa); err {
	case nil:
		return fd, false, nil
	case unix.EINPROGRESS, unix.EALREADY, unix.EINTR:
		// EINTR on a non-blocking connect: the handshake continues
		// asynchronously, exactly as for EINPROGRESS.
		return fd, true, nil
	default:
		unix.Close(fd)
		return -1, false, err
	}
}

// ConnectError reads SO_ERROR after a non-blocking connect completes (the fd
// reported Writable, Hangup or Error). nil means connected. Calling it before
// the connect completes also returns nil, so only call it after an event.
func ConnectError(fd int) error {
	v, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ERROR)
	if err != nil {
		return err
	}
	if v != 0 {
		return unix.Errno(v)
	}
	return nil
}

// SetNoDelay sets TCP_NODELAY (disables Nagle's algorithm when on).
func SetNoDelay(fd int, on bool) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, boolInt(on))
}

// SetBuffers sets SO_RCVBUF to rcv and SO_SNDBUF to snd. A value <= 0 leaves
// that buffer at its default.
func SetBuffers(fd, rcv, snd int) error {
	if rcv > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, rcv); err != nil {
			return err
		}
	}
	if snd > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, snd); err != nil {
			return err
		}
	}
	return nil
}

// SockaddrToTCP converts a unix.Sockaddr to a *net.TCPAddr. It allocates
// (one per accept is acceptable; the read/write path never calls it).
// Returns nil for unsupported families.
func SockaddrToTCP(sa unix.Sockaddr) *net.TCPAddr {
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.TCPAddr{IP: ip4(sa.Addr), Port: sa.Port}
	case *unix.SockaddrInet6:
		return &net.TCPAddr{IP: ip6(sa.Addr), Port: sa.Port, Zone: zoneName(sa.ZoneId)}
	}
	return nil
}

// sockaddrToUDP converts a unix.Sockaddr to a *net.UDPAddr. It allocates.
// Returns nil for unsupported families.
func sockaddrToUDP(sa unix.Sockaddr) *net.UDPAddr {
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.UDPAddr{IP: ip4(sa.Addr), Port: sa.Port}
	case *unix.SockaddrInet6:
		return &net.UDPAddr{IP: ip6(sa.Addr), Port: sa.Port, Zone: zoneName(sa.ZoneId)}
	}
	return nil
}

// LocalTCP returns getsockname(fd) as a *net.TCPAddr, or nil on error.
func LocalTCP(fd int) *net.TCPAddr {
	sa, err := unix.Getsockname(fd)
	if err != nil {
		return nil
	}
	return SockaddrToTCP(sa)
}

// localUDP returns getsockname(fd) as a *net.UDPAddr, or nil on error.
func localUDP(fd int) *net.UDPAddr {
	sa, err := unix.Getsockname(fd)
	if err != nil {
		return nil
	}
	return sockaddrToUDP(sa)
}

// UDPAddrToSockaddr converts a for sendto/sendmmsg, filling caller-provided
// storage so that it does not allocate when that storage is reused. An IPv4
// address fills v4 (or, when v4 is nil and v6 is not, v6 with the
// IPv4-mapped form, as needed for a dual-stack AF_INET6 socket on BSD); any
// other address fills v6. A nil storage pointer that is needed is allocated.
// A non-numeric IPv6 zone is resolved with net.InterfaceByName, which
// allocates. Returns the filled Sockaddr, or nil when a is nil.
func UDPAddrToSockaddr(a *net.UDPAddr, v4 *unix.SockaddrInet4, v6 *unix.SockaddrInet6) unix.Sockaddr {
	if a == nil {
		return nil
	}
	if ip := a.IP.To4(); ip != nil && (v4 != nil || v6 == nil) {
		if v4 == nil {
			v4 = new(unix.SockaddrInet4)
		}
		v4.Port = a.Port
		copy(v4.Addr[:], ip)
		return v4
	}
	if v6 == nil {
		v6 = new(unix.SockaddrInet6)
	}
	v6.Port = a.Port
	v6.Addr = [16]byte{}
	copy(v6.Addr[:], a.IP.To16())
	v6.ZoneId = zoneIndex(a.Zone)
	return v6
}

// Message is one datagram slot of a Batch. Buf holds the payload: its length
// is the capacity to read into for RecvMmsg and the bytes to send for
// SendMmsg. Addr is raw sockaddr storage (sockaddr_in or sockaddr_in6,
// interpreted by its Family field) of AddrLen bytes; AddrLen 0 means "no
// address" (send on a connected socket).
type Message struct {
	// Buf is the payload buffer (receive capacity / bytes to send).
	Buf []byte
	// N is the number of bytes received or sent by the last batch call.
	N int
	// Addr is the peer address storage; interpret by Family.
	Addr unix.RawSockaddrInet6
	// AddrLen is the valid length of Addr in bytes (0 = no address).
	AddrLen uint32
}

// NewBatch returns a Batch of n messages (at least 1), each with a Buf of
// bufSize bytes carved from one backing allocation. All batch memory is
// allocated here, so RecvMmsg and SendMmsg do not allocate.
func NewBatch(n, bufSize int) *Batch {
	if n < 1 {
		n = 1
	}
	if bufSize < 0 {
		bufSize = 0
	}
	mem := make([]byte, n*bufSize)
	b := &Batch{msgs: make([]Message, n)}
	for i := range b.msgs {
		lo, hi := i*bufSize, (i+1)*bufSize
		b.msgs[i].Buf = mem[lo:hi:hi]
	}
	b.init()
	return b
}

// Msgs returns the batch's messages (len n). The caller sets Buf lengths and
// Addr before SendMmsg, and reads Buf[:N] and Addr after RecvMmsg. The slice
// is owned by the Batch and reused by every call.
func (b *Batch) Msgs() []Message { return b.msgs }

// SetAddr fills the raw sockaddr from a without allocating (except for a
// non-numeric IPv6 zone, see UDPAddrToSockaddr). An IPv4 (or IPv4-mapped)
// address is stored as sockaddr_in, anything else as sockaddr_in6. A nil a
// clears the address (AddrLen 0).
func (m *Message) SetAddr(a *net.UDPAddr) {
	m.Addr = unix.RawSockaddrInet6{}
	if a == nil {
		m.AddrLen = 0
		return
	}
	if ip := a.IP.To4(); ip != nil {
		r := (*unix.RawSockaddrInet4)(unsafe.Pointer(&m.Addr))
		r.Family = unix.AF_INET
		setLen4(r)
		putPort(&r.Port, a.Port)
		copy(r.Addr[:], ip)
		m.AddrLen = unix.SizeofSockaddrInet4
		return
	}
	r := &m.Addr
	r.Family = unix.AF_INET6
	setLen6(r)
	putPort(&r.Port, a.Port)
	copy(r.Addr[:], a.IP.To16())
	r.Scope_id = zoneIndex(a.Zone)
	m.AddrLen = unix.SizeofSockaddrInet6
}

// FillAddr fills dst in place from the received raw sockaddr: dst.IP is
// resliced to 4 or 16 bytes (reusing its backing array when its capacity is
// at least 16, so the call does not allocate), Port is set and Zone is set to
// "". The IPv6 scope id is dropped. For AddrLen 0 or an unsupported family
// dst is cleared (nil IP, port 0).
func (m *Message) FillAddr(dst *net.UDPAddr) {
	dst.Zone = ""
	if m.AddrLen != 0 {
		switch m.Addr.Family {
		case unix.AF_INET:
			r := (*unix.RawSockaddrInet4)(unsafe.Pointer(&m.Addr))
			dst.IP = growIP(dst.IP, net.IPv4len)
			copy(dst.IP, r.Addr[:])
			dst.Port = getPort(&r.Port)
			return
		case unix.AF_INET6:
			r := &m.Addr
			dst.IP = growIP(dst.IP, net.IPv6len)
			copy(dst.IP, r.Addr[:])
			dst.Port = getPort(&r.Port)
			return
		}
	}
	dst.IP = nil
	dst.Port = 0
}

// growIP reslices ip to n bytes, allocating only when its capacity is short.
// It always allocates at least 16 bytes so later IPv6 fills can reuse it.
func growIP(ip net.IP, n int) net.IP {
	if cap(ip) >= n {
		return ip[:n]
	}
	return make(net.IP, n, net.IPv6len)
}

// --- unexported helpers ---

// bindAny creates a socket of type typ for network/ip, applies opts and
// binds it. For a dual-stack wildcard it falls back to AF_INET when IPv6 is
// unavailable.
func bindAny(network string, typ int, ip net.IP, port int, zone string, opts func(fd int) error) (int, error) {
	fam, v6only, dual := listenFamily(network, ip)
	fd, err := bindSocket(fam, typ, ip, port, zone, v6only, opts)
	if err != nil && dual {
		if fd4, err4 := bindSocket(unix.AF_INET, typ, nil, port, "", false, opts); err4 == nil {
			return fd4, nil
		}
	}
	return fd, err
}

// bindSocket creates a non-blocking, close-on-exec socket, sets IPV6_V6ONLY
// for AF_INET6, applies opts and binds. The socket is closed on error.
func bindSocket(fam, typ int, ip net.IP, port int, zone string, v6only bool, opts func(fd int) error) (int, error) {
	sa, err := toSockaddr(fam, ip, port, zone)
	if err != nil {
		return -1, err
	}
	fd, err := newSocket(fam, typ)
	if err != nil {
		return -1, err
	}
	if fam == unix.AF_INET6 {
		err = unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_V6ONLY, boolInt(v6only))
	}
	if err == nil && opts != nil {
		err = opts(fd)
	}
	if err == nil {
		err = unix.Bind(fd, sa)
	}
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// listenFamily picks the address family for a listening socket. dual reports
// a dual-stack wildcard that may fall back to AF_INET.
func listenFamily(network string, ip net.IP) (fam int, v6only, dual bool) {
	switch network[len(network)-1] {
	case '4':
		return unix.AF_INET, false, false
	case '6':
		return unix.AF_INET6, true, false
	}
	if ip == nil || (ip.To4() == nil && ip.IsUnspecified()) {
		return unix.AF_INET6, false, true
	}
	if ip.To4() != nil {
		return unix.AF_INET, false, false
	}
	return unix.AF_INET6, false, false
}

// dialFamily picks the address family for an outgoing connection.
func dialFamily(network string, ip net.IP, local *net.TCPAddr) int {
	switch network[len(network)-1] {
	case '4':
		return unix.AF_INET
	case '6':
		return unix.AF_INET6
	}
	if ip == nil || ip.IsUnspecified() {
		if local != nil && local.IP != nil && local.IP.To4() == nil {
			return unix.AF_INET6
		}
		return unix.AF_INET
	}
	if ip.To4() != nil {
		return unix.AF_INET
	}
	return unix.AF_INET6
}

// toSockaddr builds a unix.Sockaddr of family fam. A nil ip is the
// unspecified address. An IPv4 ip in an AF_INET6 sockaddr uses the
// IPv4-mapped form.
func toSockaddr(fam int, ip net.IP, port int, zone string) (unix.Sockaddr, error) {
	if fam == unix.AF_INET {
		sa := &unix.SockaddrInet4{Port: port}
		if ip != nil {
			ip4 := ip.To4()
			if ip4 == nil {
				return nil, unix.EAFNOSUPPORT
			}
			copy(sa.Addr[:], ip4)
		}
		return sa, nil
	}
	sa := &unix.SockaddrInet6{Port: port, ZoneId: zoneIndex(zone)}
	if ip != nil {
		ip16 := ip.To16()
		if ip16 == nil {
			return nil, unix.EAFNOSUPPORT
		}
		copy(sa.Addr[:], ip16)
	}
	return sa, nil
}

// zoneIndex converts an IPv6 zone (interface name or number) to an index.
func zoneIndex(zone string) uint32 {
	if zone == "" {
		return 0
	}
	if n, err := strconv.ParseUint(zone, 10, 32); err == nil {
		return uint32(n)
	}
	if ifi, err := net.InterfaceByName(zone); err == nil {
		return uint32(ifi.Index)
	}
	return 0
}

// zoneName converts an IPv6 scope id to a zone string. It uses the numeric
// form (accepted everywhere a zone is parsed) to avoid an interface lookup
// per accepted connection.
func zoneName(id uint32) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(uint64(id), 10)
}

func ip4(a [4]byte) net.IP {
	ip := make(net.IP, net.IPv4len)
	copy(ip, a[:])
	return ip
}

func ip6(a [16]byte) net.IP {
	ip := make(net.IP, net.IPv6len)
	copy(ip, a[:])
	return ip
}

// getPort reads a network-byte-order port from raw sockaddr storage.
func getPort(p *uint16) int {
	b := (*[2]byte)(unsafe.Pointer(p))
	return int(b[0])<<8 | int(b[1])
}

// putPort writes a network-byte-order port into raw sockaddr storage.
func putPort(p *uint16, port int) {
	b := (*[2]byte)(unsafe.Pointer(p))
	b[0] = byte(port >> 8)
	b[1] = byte(port)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
