package twine

import (
	"net"

	"github.com/exergy-dev/twine/internal/poll"
	"github.com/exergy-dev/twine/internal/sock"
	"golang.org/x/sys/unix"
)

// udpSendBuf is the per-message buffer of the outbound batch; larger
// datagrams bypass the batch and are sent with one sendto.
const udpSendBuf = 8 << 10

// udpState is the per-socket UDP state.
type udpState struct {
	lst   *listener
	proto DatagramProtocol
	in    *sock.Batch
	out   *sock.Batch
	outN  int
	addrs []*net.UDPAddr // scratch for DatagramReceived (reused; not retained)
}

var _ DatagramTransport = (*conn)(nil)

func (e *Engine) newUDPListener(addr string, p DatagramProtocol, lc listenConfig) (*listener, error) {
	fd, laddr, err := sock.ListenUDP("udp", addr, false)
	if err != nil {
		return nil, err
	}
	if lc.rcvBuf > 0 || lc.sndBuf > 0 {
		_ = sock.SetBuffers(fd, lc.rcvBuf, lc.sndBuf)
	}
	lst := &listener{eng: e, addr: laddr, dgram: p, lc: lc, udp: true}
	lp := e.NextLoop()
	c := &conn{loop: lp, fd: int32(fd), kind: kindUDP}
	c.idle.Fn = c.onIdle
	c.udp = &udpState{lst: lst, proto: p}
	c.host = laddr
	lst.fds = []int{fd}
	lst.conns = []*conn{c}
	lp.CallFromThread(func() { lp.registerUDP(c) })
	return lst, nil
}

func (l *Loop) registerUDP(c *conn) {
	u := c.udp
	if l.stopping || u.lst.closed.Load() {
		_ = unix.Close(int(c.fd))
		return
	}
	batch := l.cfg.udpBatch
	if batch <= 0 {
		batch = 1
	}
	u.in = sock.NewBatch(batch, 64<<10)
	u.out = sock.NewBatch(batch, udpSendBuf)
	u.addrs = make([]*net.UDPAddr, batch)
	for i := range u.addrs {
		u.addrs[i] = &net.UDPAddr{IP: make(net.IP, 0, 16)}
	}
	l.register(c)
	if err := l.poller.Add(int(c.fd), poll.Readable); err != nil {
		l.cfg.logger.Error("twine: registering UDP socket", "loop", l.id, "err", err)
		l.unregister(c)
		_ = unix.Close(int(c.fd))
		return
	}
	l.safeCall(c, func() { u.proto.StartProtocol(c) })
	l.handleUDPRead(c)
}

// handleUDPRead receives a batch with recvmmsg and dispatches each datagram.
func (l *Loop) handleUDPRead(c *conn) {
	u := c.udp
	if u == nil || u.in == nil || c.flags&flagClosed != 0 {
		return
	}
	for round := 0; round < l.cfg.maxReadsPerEv; round++ {
		msgs := u.in.Msgs()
		for i := range msgs {
			msgs[i].Buf = msgs[i].Buf[:cap(msgs[i].Buf)]
		}
		n, err := u.in.RecvMmsg(int(c.fd))
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR {
				return
			}
			l.cfg.logger.Warn("twine: recvmmsg", "loop", l.id, "err", err)
			return
		}
		if n == 0 {
			return
		}
		l.metrics.Datagrams.Add(uint64(n))
		for i := 0; i < n; i++ {
			m := &msgs[i]
			a := u.addrs[i]
			m.FillAddr(a)
			l.callDatagram(c, m.Buf[:m.N], a)
			if c.flags&flagClosed != 0 {
				return
			}
		}
		if n < len(msgs) {
			return
		}
	}
	c.queueReady()
}

func (l *Loop) callDatagram(c *conn, b []byte, from net.Addr) {
	l.cbStart.Store(l.nowNs)
	l.cbProto.Store(c)
	defer l.endCallback()
	defer l.recoverProto(c)
	c.udp.proto.DatagramReceived(b, from)
	if debugEnabled {
		poison(b)
	}
}

// WriteTo queues a datagram for the end-of-iteration sendmmsg flush.
// Datagrams larger than the batch buffer are sent immediately.
func (c *conn) WriteTo(b []byte, to net.Addr) {
	c.loop.assertInLoop()
	u := c.udp
	if u == nil || c.flags&flagClosed != 0 {
		return
	}
	ua, ok := to.(*net.UDPAddr)
	if !ok {
		return
	}
	if len(b) > udpSendBuf || u.out == nil {
		var v4 unix.SockaddrInet4
		var v6 unix.SockaddrInet6
		sa := sock.UDPAddrToSockaddr(ua, &v4, &v6)
		// UDP is best effort: a failed send (EMSGSIZE above the socket's
		// limit, EAGAIN with a full buffer) drops this datagram only and
		// leaves the socket usable. Count only what the kernel took.
		if sa != nil && unix.Sendto(int(c.fd), b, 0, sa) == nil {
			c.loop.metrics.DatagramsSent.Add(1)
		}
		return
	}
	if u.outN == len(u.out.Msgs()) {
		c.loop.flushUDP(c)
		if u.outN == len(u.out.Msgs()) {
			// Socket would block and the batch is full: drop (UDP).
			return
		}
	}
	m := &u.out.Msgs()[u.outN]
	m.Buf = m.Buf[:len(b)]
	copy(m.Buf, b)
	m.N = 0
	m.SetAddr(ua)
	u.outN++
	c.markDirty()
}

// flushUDP sends the pending batch with sendmmsg.
func (l *Loop) flushUDP(c *conn) {
	u := c.udp
	if u == nil || u.outN == 0 {
		if c.flags&flagWantWrite != 0 {
			c.disarmWrite()
		}
		return
	}
	sent, err := u.out.SendMmsg(int(c.fd), u.outN)
	if sent > 0 {
		l.metrics.DatagramsSent.Add(uint64(sent))
	}
	if sent < u.outN {
		if err != nil && err != unix.EAGAIN && err != unix.EINTR {
			// Drop the batch on a hard error.
			u.outN = 0
			l.cfg.logger.Warn("twine: sendmmsg", "loop", l.id, "err", err)
			return
		}
		// Shift the unsent tail to the front and wait for Writable.
		msgs := u.out.Msgs()
		for i := sent; i < u.outN; i++ {
			msgs[i-sent], msgs[i] = msgs[i], msgs[i-sent]
		}
		u.outN -= sent
		c.armWrite()
		return
	}
	u.outN = 0
	if c.flags&flagWantWrite != 0 {
		c.disarmWrite()
	}
}

// GetHost returns the bound address (DatagramTransport).
func (c *conn) GetHost() net.Addr { return c.host }

// Loop returns the owning loop (DatagramTransport).
func (c *conn) Loop() *Loop { return c.loop }

// Close closes the UDP socket (DatagramTransport). Loop-affine.
func (c *conn) Close() {
	c.loop.assertInLoop()
	c.closeUDP()
}

// closeUDP flushes pending datagrams, closes the fd and calls StopProtocol.
func (c *conn) closeUDP() {
	if c.fd < 0 || c.flags&flagClosed != 0 {
		return
	}
	l := c.loop
	l.flushUDP(c)
	c.flags |= flagClosed
	_ = l.poller.Del(int(c.fd))
	l.unregister(c)
	_ = unix.Close(int(c.fd))
	c.fd = -1
	if c.udp != nil && c.udp.proto != nil {
		l.safeCall(c, c.udp.proto.StopProtocol)
	}
}
