package sock

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// waitFD polls fd for events, failing the test after timeout.
func waitFD(t *testing.T, fd int, events int16, timeout time.Duration) int16 {
	t.Helper()
	pfd := []unix.PollFd{{Fd: int32(fd), Events: events}}
	deadline := time.Now().Add(timeout)
	for {
		ms := int(time.Until(deadline) / time.Millisecond)
		if ms <= 0 {
			t.Fatalf("fd %d not ready for %#x within %v", fd, events, timeout)
		}
		n, err := unix.Poll(pfd, ms)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if n > 0 {
			return pfd[0].Revents
		}
	}
}

func closeFD(t *testing.T, fd int) {
	t.Cleanup(func() { unix.Close(fd) })
}

func listen(t *testing.T, network, addr string) (int, *net.TCPAddr) {
	t.Helper()
	fd, laddr, err := Listen(network, addr, false, 128)
	if err != nil {
		t.Fatalf("Listen(%s, %s): %v", network, addr, err)
	}
	closeFD(t, fd)
	return fd, laddr
}

func checkNonblockCloexec(t *testing.T, fd int) {
	t.Helper()
	fl, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fl&unix.O_NONBLOCK == 0 {
		t.Errorf("fd %d is blocking", fd)
	}
	fdfl, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fdfl&unix.FD_CLOEXEC == 0 {
		t.Errorf("fd %d is not close-on-exec", fd)
	}
}

func TestListenAccept(t *testing.T) {
	lfd, laddr := listen(t, "tcp4", "127.0.0.1:0")
	if laddr.Port == 0 || !laddr.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("laddr = %v, want 127.0.0.1:<port>", laddr)
	}
	checkNonblockCloexec(t, lfd)
	if got := LocalTCP(lfd); got == nil || got.Port != laddr.Port {
		t.Fatalf("LocalTCP = %v, want port %d", got, laddr.Port)
	}

	c, err := net.Dial("tcp4", laddr.String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	waitFD(t, lfd, unix.POLLIN, 5*time.Second)
	fd, sa, err := Accept(lfd)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	closeFD(t, fd)
	checkNonblockCloexec(t, fd)
	peer := SockaddrToTCP(sa)
	want := c.LocalAddr().(*net.TCPAddr)
	if peer == nil || !peer.IP.Equal(want.IP) || peer.Port != want.Port {
		t.Fatalf("peer = %v, want %v", peer, want)
	}
	// Data flows on the accepted fd.
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	waitFD(t, fd, unix.POLLIN, 5*time.Second)
	var buf [16]byte
	n, err := unix.Read(fd, buf[:])
	if err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("Read = %q, %v", buf[:n], err)
	}
	if err := SetNoDelay(fd, true); err != nil {
		t.Errorf("SetNoDelay: %v", err)
	}
	if err := SetKeepAlive(fd, true); err != nil {
		t.Errorf("SetKeepAlive: %v", err)
	}
	if err := SetBuffers(fd, 64<<10, 64<<10); err != nil {
		t.Errorf("SetBuffers: %v", err)
	}
	if err := SetBuffers(fd, 0, 0); err != nil {
		t.Errorf("SetBuffers(0,0): %v", err)
	}
	if err := SetNonblock(fd); err != nil {
		t.Errorf("SetNonblock: %v", err)
	}
}

func TestAcceptEAGAIN(t *testing.T) {
	lfd, _ := listen(t, "tcp4", "127.0.0.1:0")
	fd, sa, err := Accept(lfd)
	if err != unix.EAGAIN {
		t.Fatalf("Accept on idle listener = %d, %v, %v; want EAGAIN", fd, sa, err)
	}
	if n := testing.AllocsPerRun(100, func() { Accept(lfd) }); n != 0 {
		t.Fatalf("Accept EAGAIN path: %v allocs/op, want 0", n)
	}
}

func TestConnect(t *testing.T) {
	lfd, laddr := listen(t, "tcp4", "127.0.0.1:0")
	fd, raddr, inProgress, err := Connect("tcp4", laddr.String(), "")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	closeFD(t, fd)
	checkNonblockCloexec(t, fd)
	if raddr.Port != laddr.Port {
		t.Fatalf("raddr = %v, want %v", raddr, laddr)
	}
	if inProgress {
		waitFD(t, fd, unix.POLLOUT, 5*time.Second)
	}
	if err := ConnectError(fd); err != nil {
		t.Fatalf("ConnectError: %v", err)
	}
	waitFD(t, lfd, unix.POLLIN, 5*time.Second)
	afd, sa, err := Accept(lfd)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	closeFD(t, afd)
	local := LocalTCP(fd)
	if peer := SockaddrToTCP(sa); peer == nil || local == nil || peer.Port != local.Port {
		t.Fatalf("accepted peer %v != connector local %v", peer, local)
	}
}

func TestConnectWithLocal(t *testing.T) {
	_, laddr := listen(t, "tcp4", "127.0.0.1:0")
	fd, _, inProgress, err := Connect("tcp", laddr.String(), "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	closeFD(t, fd)
	if inProgress {
		waitFD(t, fd, unix.POLLOUT, 5*time.Second)
	}
	if err := ConnectError(fd); err != nil {
		t.Fatalf("ConnectError: %v", err)
	}
	if l := LocalTCP(fd); l == nil || !l.IP.Equal(net.IPv4(127, 0, 0, 1)) || l.Port == 0 {
		t.Fatalf("LocalTCP = %v", l)
	}
}

func TestConnectRefused(t *testing.T) {
	// Find a port that is (very likely) closed.
	lfd, laddr, err := Listen("tcp4", "127.0.0.1:0", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	unix.Close(lfd)
	fd, _, inProgress, err := Connect("tcp4", laddr.String(), "")
	if err != nil {
		if err != unix.ECONNREFUSED {
			t.Fatalf("Connect: %v, want ECONNREFUSED", err)
		}
		return
	}
	closeFD(t, fd)
	if inProgress {
		waitFD(t, fd, unix.POLLOUT, 5*time.Second)
	}
	if err := ConnectError(fd); err != unix.ECONNREFUSED {
		t.Fatalf("ConnectError = %v, want ECONNREFUSED", err)
	}
}

func TestListenDualStack(t *testing.T) {
	lfd, laddr := listen(t, "tcp", ":0")
	c, err := net.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", laddr.Port))
	if err != nil {
		t.Fatalf("Dial v4 to dual-stack listener: %v", err)
	}
	defer c.Close()
	waitFD(t, lfd, unix.POLLIN, 5*time.Second)
	fd, sa, err := Accept(lfd)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	closeFD(t, fd)
	peer := SockaddrToTCP(sa)
	if peer == nil || !peer.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("peer = %v, want 127.0.0.1 (possibly v4-mapped)", peer)
	}
}

func TestListenIPv6(t *testing.T) {
	fd, laddr, err := Listen("tcp6", "[::1]:0", false, 0)
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	closeFD(t, fd)
	if !laddr.IP.Equal(net.IPv6loopback) {
		t.Fatalf("laddr = %v", laddr)
	}
	cfd, _, inProgress, err := Connect("tcp6", laddr.String(), "")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	closeFD(t, cfd)
	if inProgress {
		waitFD(t, cfd, unix.POLLOUT, 5*time.Second)
	}
	if err := ConnectError(cfd); err != nil {
		t.Fatalf("ConnectError: %v", err)
	}
}

func TestListenReusePort(t *testing.T) {
	fd1, laddr, err := Listen("tcp4", "127.0.0.1:0", true, 16)
	if err != nil {
		t.Fatal(err)
	}
	closeFD(t, fd1)
	fd2, laddr2, err := Listen("tcp4", laddr.String(), true, 16)
	if err != nil {
		t.Fatalf("second SO_REUSEPORT listener: %v", err)
	}
	closeFD(t, fd2)
	if laddr2.Port != laddr.Port {
		t.Fatalf("ports differ: %d vs %d", laddr.Port, laddr2.Port)
	}
}

func TestListenErrors(t *testing.T) {
	if _, _, err := Listen("udp", ":0", false, 0); err == nil {
		t.Error("Listen(udp) succeeded")
	}
	if _, _, err := ListenUDP("tcp", ":0", false); err == nil {
		t.Error("ListenUDP(tcp) succeeded")
	}
	if _, _, _, err := Connect("udp", "127.0.0.1:1", ""); err == nil {
		t.Error("Connect(udp) succeeded")
	}
	if _, _, err := Listen("tcp4", "bad address", false, 0); err == nil {
		t.Error("Listen(bad address) succeeded")
	}
}

func TestSockaddrConversions(t *testing.T) {
	sa4 := &unix.SockaddrInet4{Port: 1234, Addr: [4]byte{10, 1, 2, 3}}
	if a := SockaddrToTCP(sa4); a.String() != "10.1.2.3:1234" {
		t.Errorf("SockaddrToTCP(v4) = %v", a)
	}
	if a := SockaddrToUDP(sa4); a.String() != "10.1.2.3:1234" {
		t.Errorf("SockaddrToUDP(v4) = %v", a)
	}
	sa6 := &unix.SockaddrInet6{Port: 53, ZoneId: 7}
	copy(sa6.Addr[:], net.ParseIP("fe80::1"))
	if a := SockaddrToUDP(sa6); a.String() != "[fe80::1%7]:53" {
		t.Errorf("SockaddrToUDP(v6) = %v", a)
	}
	if SockaddrToTCP(&unix.SockaddrUnix{Name: "x"}) != nil || SockaddrToUDP(nil) != nil {
		t.Error("unsupported family did not return nil")
	}

	var v4 unix.SockaddrInet4
	var v6 unix.SockaddrInet6
	ua := &net.UDPAddr{IP: net.ParseIP("192.0.2.9"), Port: 9}
	if sa := UDPAddrToSockaddr(ua, &v4, &v6); sa != &v4 || v4.Port != 9 || v4.Addr != [4]byte{192, 0, 2, 9} {
		t.Errorf("UDPAddrToSockaddr(v4) = %#v", sa)
	}
	// v4 into v6-only storage: IPv4-mapped.
	if sa := UDPAddrToSockaddr(ua, nil, &v6); sa != &v6 || !net.IP(v6.Addr[:]).Equal(ua.IP) || v6.Port != 9 {
		t.Errorf("UDPAddrToSockaddr(v4, mapped) = %#v", sa)
	}
	ua6 := &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443, Zone: "3"}
	if sa := UDPAddrToSockaddr(ua6, &v4, &v6); sa != &v6 || v6.ZoneId != 3 || v6.Port != 443 {
		t.Errorf("UDPAddrToSockaddr(v6) = %#v", sa)
	}
	if UDPAddrToSockaddr(nil, &v4, &v6) != nil {
		t.Error("UDPAddrToSockaddr(nil) != nil")
	}
	if sa := UDPAddrToSockaddr(ua, nil, nil); sa == nil {
		t.Error("UDPAddrToSockaddr with nil storage returned nil")
	}
	if n := testing.AllocsPerRun(100, func() { UDPAddrToSockaddr(ua, &v4, &v6) }); n != 0 {
		t.Errorf("UDPAddrToSockaddr: %v allocs/op, want 0", n)
	}
}

func TestMessageAddr(t *testing.T) {
	var m Message
	for _, s := range []string{"127.0.0.1:5353", "[::1]:80", "[fe80::2%5]:8080", "0.0.0.0:0"} {
		ua, err := net.ResolveUDPAddr("udp", s)
		if err != nil {
			t.Fatal(err)
		}
		m.SetAddr(ua)
		got := m.AddrOf()
		if got == nil || !got.IP.Equal(ua.IP) || got.Port != ua.Port || got.Zone != ua.Zone {
			t.Errorf("SetAddr/AddrOf(%s) = %v", s, got)
		}
	}
	m.SetAddr(nil)
	if m.AddrLen != 0 || m.AddrOf() != nil {
		t.Error("SetAddr(nil) did not clear the address")
	}
	ua := &net.UDPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 99}
	if n := testing.AllocsPerRun(100, func() { m.SetAddr(ua) }); n != 0 {
		t.Errorf("SetAddr: %v allocs/op, want 0", n)
	}
}

func TestNewBatch(t *testing.T) {
	b := NewBatch(4, 100)
	msgs := b.Msgs()
	if len(msgs) != 4 {
		t.Fatalf("len(Msgs) = %d", len(msgs))
	}
	for i := range msgs {
		if len(msgs[i].Buf) != 100 || cap(msgs[i].Buf) != 100 {
			t.Fatalf("msg %d buf len/cap = %d/%d", i, len(msgs[i].Buf), cap(msgs[i].Buf))
		}
	}
	// Buffers must not overlap.
	msgs[0].Buf = append(msgs[0].Buf[:100], 'x')
	if msgs[1].Buf[0] != 0 {
		t.Fatal("append to msg 0 clobbered msg 1")
	}
	if b := NewBatch(0, -1); len(b.Msgs()) != 1 {
		t.Fatal("NewBatch(0) should clamp to 1 message")
	}
}

func listenUDP(t *testing.T) (int, *net.UDPAddr) {
	t.Helper()
	fd, laddr, err := ListenUDP("udp4", "127.0.0.1:0", false)
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	closeFD(t, fd)
	checkNonblockCloexec(t, fd)
	return fd, laddr
}

// roundTrip sends 10 datagrams from a to b with send, receives them with
// recv, then echoes them back to the received addresses.
func roundTrip(t *testing.T, send func(*Batch, int, int) (int, error), recv func(*Batch, int) (int, error)) {
	const count = 10
	afd, aaddr := listenUDP(t)
	bfd, baddr := listenUDP(t)

	sb := NewBatch(count, 64)
	msgs := sb.Msgs()
	for i := range msgs {
		msgs[i].Buf = append(msgs[i].Buf[:0], fmt.Sprintf("datagram-%02d", i)...)
		msgs[i].SetAddr(baddr)
	}
	sent := 0
	for sent < count {
		// Shift unsent messages to the front is unnecessary: send all at once
		// and retry on a short write by resending the tail.
		n, err := send(sb, afd, count-sent)
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		for i := 0; i < n; i++ {
			if msgs[i].N != len(msgs[i].Buf) {
				t.Fatalf("msg %d: N = %d, want %d", i, msgs[i].N, len(msgs[i].Buf))
			}
		}
		copy(msgs, msgs[n:])
		sent += n
	}

	rb := NewBatch(count, 64)
	var got [][]byte
	for len(got) < count {
		waitFD(t, bfd, unix.POLLIN, 5*time.Second)
		n, err := recv(rb, bfd)
		if err == unix.EAGAIN {
			continue
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		for _, m := range rb.Msgs()[:n] {
			got = append(got, append([]byte(nil), m.Buf[:m.N]...))
			from := m.AddrOf()
			if from == nil || !from.IP.Equal(aaddr.IP) || from.Port != aaddr.Port {
				t.Fatalf("from = %v, want %v", from, aaddr)
			}
		}
	}
	for i, g := range got {
		if want := fmt.Sprintf("datagram-%02d", i); string(g) != want {
			t.Fatalf("datagram %d = %q, want %q", i, g, want)
		}
	}
	if n, err := recv(rb, bfd); err != unix.EAGAIN {
		t.Fatalf("recv on empty socket = %d, %v; want EAGAIN", n, err)
	}

	// Echo one received datagram back using its raw address.
	rb.Msgs()[0].Buf = append(rb.Msgs()[0].Buf[:0], "echo"...)
	if n, err := send(rb, bfd, 1); n != 1 || err != nil {
		t.Fatalf("echo send = %d, %v", n, err)
	}
	waitFD(t, afd, unix.POLLIN, 5*time.Second)
	eb := NewBatch(2, 64)
	n, err := recv(eb, afd)
	if n != 1 || err != nil || !bytes.Equal(eb.Msgs()[0].Buf[:eb.Msgs()[0].N], []byte("echo")) {
		t.Fatalf("echo recv = %d, %v", n, err)
	}
}

func TestUDPBatchRoundTrip(t *testing.T) {
	roundTrip(t,
		func(b *Batch, fd, n int) (int, error) { return b.SendMmsg(fd, n) },
		func(b *Batch, fd int) (int, error) { return b.RecvMmsg(fd) })
}

func TestUDPBatchLoopRoundTrip(t *testing.T) {
	// The portable recvfrom/sendto loop, exercised on every platform.
	roundTrip(t,
		func(b *Batch, fd, n int) (int, error) { return b.sendLoop(fd, n) },
		func(b *Batch, fd int) (int, error) { return b.recvLoop(fd) })
}

func TestUDPConnectedSend(t *testing.T) {
	afd, _ := listenUDP(t)
	bfd, baddr := listenUDP(t)
	if err := unix.Connect(afd, UDPAddrToSockaddr(baddr, nil, nil)); err != nil {
		t.Fatal(err)
	}
	b := NewBatch(1, 16)
	m := &b.Msgs()[0]
	m.Buf = append(m.Buf[:0], "hi"...)
	m.SetAddr(nil)
	if n, err := b.SendMmsg(afd, 1); n != 1 || err != nil {
		t.Fatalf("SendMmsg on connected socket = %d, %v", n, err)
	}
	waitFD(t, bfd, unix.POLLIN, 5*time.Second)
	r := NewBatch(1, 16)
	if n, err := r.RecvMmsg(bfd); n != 1 || err != nil || string(r.Msgs()[0].Buf[:r.Msgs()[0].N]) != "hi" {
		t.Fatalf("RecvMmsg = %d, %v", n, err)
	}
	if n, err := b.SendMmsg(afd, 0); n != 0 || err != nil {
		t.Fatalf("SendMmsg(0) = %d, %v", n, err)
	}
}

func TestUDPDualStack(t *testing.T) {
	// A "udp" wildcard socket receives IPv4 datagrams and can reply to them.
	fd, laddr, err := ListenUDP("udp", ":0", false)
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	closeFD(t, fd)
	c, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: laddr.Port})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("q")); err != nil {
		t.Fatal(err)
	}
	waitFD(t, fd, unix.POLLIN, 5*time.Second)
	b := NewBatch(1, 16)
	if n, err := b.RecvMmsg(fd); n != 1 || err != nil {
		t.Fatalf("RecvMmsg = %d, %v", n, err)
	}
	from := b.Msgs()[0].AddrOf()
	if !from.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("from = %v", from)
	}
	// Reply via SetAddr with a plain IPv4 address (stored as sockaddr_in).
	m := &b.Msgs()[0]
	m.SetAddr(c.LocalAddr().(*net.UDPAddr))
	m.Buf = append(m.Buf[:0], "a"...)
	if n, err := b.SendMmsg(fd, 1); n != 1 || err != nil {
		t.Fatalf("SendMmsg sockaddr_in on dual-stack socket = %d, %v", n, err)
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [4]byte
	if n, err := c.Read(buf[:]); err != nil || string(buf[:n]) != "a" {
		t.Fatalf("reply = %q, %v", buf[:n], err)
	}
}

func TestFillAddr(t *testing.T) {
	var m Message
	dst := &net.UDPAddr{IP: make(net.IP, 0, 16), Zone: "stale"}
	for _, s := range []string{"127.0.0.1:5353", "[2001:db8::7]:80", "10.0.0.1:1"} {
		ua, err := net.ResolveUDPAddr("udp", s)
		if err != nil {
			t.Fatal(err)
		}
		m.SetAddr(ua)
		m.FillAddr(dst)
		if !dst.IP.Equal(ua.IP) || dst.Port != ua.Port || dst.Zone != "" {
			t.Errorf("FillAddr(%s) = %v", s, dst)
		}
	}
	if len(dst.IP) != 4 {
		t.Errorf("IPv4 FillAddr left len(IP) = %d, want 4", len(dst.IP))
	}
	// A nil IP is grown once, with room for IPv6.
	var fresh net.UDPAddr
	m.FillAddr(&fresh)
	if cap(fresh.IP) < 16 {
		t.Errorf("grown IP cap = %d, want >= 16", cap(fresh.IP))
	}
	m.SetAddr(nil)
	m.FillAddr(dst)
	if dst.IP != nil || dst.Port != 0 {
		t.Errorf("FillAddr with no address = %v", dst)
	}

	v4 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 7}
	v6 := &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 8}
	scoped := &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 9, Zone: "200"}
	var m4, m6, ms Message
	m4.SetAddr(v4)
	m6.SetAddr(v6)
	ms.SetAddr(scoped)
	ms.FillAddr(dst)
	if !dst.IP.Equal(scoped.IP) || dst.Zone != "" {
		t.Errorf("FillAddr(scoped) = %v, want zone dropped", dst)
	}
	dst.IP = make(net.IP, 0, 16)
	if n := testing.AllocsPerRun(100, func() {
		m4.FillAddr(dst)
		m6.FillAddr(dst)
		ms.FillAddr(dst)
	}); n != 0 {
		t.Errorf("FillAddr: %v allocs/op, want 0", n)
	}
}
