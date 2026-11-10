package twine

import (
	"net"
	"net/netip"
	"strconv"

	"github.com/exergy-dev/twine/internal/poll"
	"github.com/exergy-dev/twine/internal/sock"
	"github.com/exergy-dev/twine/internal/wheel"
	"golang.org/x/sys/unix"
)

// dialState holds the in-flight connect bookkeeping.
type dialState struct {
	factory Factory
	res     Resolver[Transport]
	timer   wheel.Timer
	c       *conn
	noDelay bool
}

func (d *dialState) onTimeout() {
	d.c.rejectConnect(ErrConnectTimeout)
}

// ConnectTCP starts a non-blocking connect to addr, a host:port. The Future
// resolves with the Transport after ConnectionMade has been called, or
// rejects with the lookup or connect error, ErrConnectTimeout, ErrNilProtocol
// or ErrEngineStopped. It is safe to call from any goroutine and never
// blocks: an IP literal (or an empty host, which dials the loopback address)
// is connected directly, while a host name is resolved on the engine's
// worker pool first, so a slow DNS lookup never stalls the loop. DialTimeout
// bounds the connect, not the lookup.
func (l *Loop) ConnectTCP(addr string, f Factory, opts ...DialOption) *Future[Transport] {
	fut, res := NewFuture[Transport](l)
	dc := defaultDialConfig()
	for _, o := range opts {
		o(&dc)
	}
	if l.stopped.Load() || l.eng.stopped() {
		res.Reject(ErrEngineStopped)
		return fut
	}
	raddr, rok := literalTCPAddr(addr)
	var laddr *net.TCPAddr
	lok := true
	if dc.local != "" {
		laddr, lok = literalTCPAddr(dc.local)
	}
	if rok && lok {
		if l.InLoop() {
			l.startConnect(raddr, laddr, f, dc, res)
		} else {
			l.CallFromThread(func() { l.startConnect(raddr, laddr, f, dc, res) })
		}
		return fut
	}
	// A host name: look it up off the loop, then connect on it.
	ok := l.eng.pool.submit(func() {
		ra, la := raddr, laddr
		var err error
		if !rok {
			ra, err = resolveTCPAddr("tcp", addr)
		}
		if err == nil && !lok {
			la, err = resolveTCPAddr("tcp", dc.local)
		}
		if err != nil {
			res.Reject(err)
			return
		}
		if l.stopped.Load() || l.eng.stopped() {
			res.Reject(ErrEngineStopped)
			return
		}
		l.CallFromThread(func() { l.startConnect(ra, la, f, dc, res) })
	}, true)
	if !ok {
		res.Reject(ErrEngineStopped)
	}
	return fut
}

// resolveTCPAddr is the name lookup for addresses that are not IP literals.
// It blocks, so it only ever runs on the worker pool; tests replace it.
var resolveTCPAddr = net.ResolveTCPAddr

// literalTCPAddr parses addr without a name lookup. ok is false when the
// host is a name or the port a service name (or addr is malformed, which the
// resolver then reports).
func literalTCPAddr(addr string) (a *net.TCPAddr, ok bool) {
	if ap, err := netip.ParseAddrPort(addr); err == nil {
		return net.TCPAddrFromAddrPort(ap), true
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, false
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, false
	}
	if host == "" {
		return &net.TCPAddr{Port: int(p)}, true
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return nil, false
	}
	return net.TCPAddrFromAddrPort(netip.AddrPortFrom(ip, uint16(p))), true
}

func (l *Loop) startConnect(raddr, laddr *net.TCPAddr, f Factory, dc dialConfig, res Resolver[Transport]) {
	if l.stopping {
		res.Reject(ErrEngineStopped)
		return
	}
	fd, inProgress, err := sock.ConnectAddr("tcp", raddr, laddr)
	if err != nil {
		res.Reject(err)
		return
	}
	c := l.getConn()
	c.fd = int32(fd)
	c.kind = kindConnecting
	c.peer = raddr
	d := &dialState{factory: f, res: res, c: c, noDelay: dc.noDelay}
	d.timer.Fn = d.onTimeout
	c.dial = d
	l.register(c)
	if dc.timeout > 0 {
		l.wheel.Add(&d.timer, l.nowNs+int64(dc.timeout))
	}
	if !inProgress {
		l.completeConnect(c)
		return
	}
	if err := l.poller.Add(fd, poll.Writable); err != nil {
		c.rejectConnect(err)
	}
}

// handleConnectReady is called on Writable/Error/Hangup for a connecting fd.
func (l *Loop) handleConnectReady(c *conn, flags uint32) {
	if c.dial == nil {
		return
	}
	if err := sock.ConnectError(int(c.fd)); err != nil {
		c.rejectConnect(err)
		return
	}
	if flags&(poll.Error|poll.Hangup) != 0 && flags&poll.Writable == 0 {
		c.rejectConnect(unix.ECONNREFUSED)
		return
	}
	l.completeConnect(c)
}

// completeConnect turns a connecting conn into a live TCP conn.
func (l *Loop) completeConnect(c *conn) {
	d := c.dial
	c.dial = nil
	if d.timer.Scheduled() {
		l.wheel.Remove(&d.timer)
	}
	fd := int(c.fd)
	if d.noDelay {
		_ = sock.SetNoDelay(fd, true)
	}
	c.host = sock.LocalTCP(fd)
	c.kind = kindTCP
	l.metrics.connections.Add(1)
	c.h = &transport{c: c, gen: c.gen}
	p := d.factory.BuildProtocol(c.peer)
	if p == nil {
		c.abortNoCallback()
		d.res.Reject(ErrNilProtocol)
		return
	}
	c.setProto(p)
	// An in-progress connect registered the fd for Writable: switch it to
	// the steady-state interest. An immediate connect never registered it.
	err := l.poller.Mod(fd, poll.Readable)
	if err != nil {
		err = l.poller.Add(fd, poll.Readable)
	}
	if err != nil {
		c.lostErr = err
		c.abortNoCallback()
		d.res.Reject(err)
		return
	}
	l.callConnectionMade(c)
	if c.flags&flagClosed != 0 {
		err := c.lostErr
		if err == nil {
			err = ErrClosed // ConnectionMade aborted the connection
		}
		d.res.Reject(err)
		return
	}
	d.res.Resolve(c.h)
}

// rejectConnect fails an in-flight connect and releases the fd.
func (c *conn) rejectConnect(err error) {
	d := c.dial
	if d == nil {
		return
	}
	c.dial = nil
	l := c.loop
	if d.timer.Scheduled() {
		l.wheel.Remove(&d.timer)
	}
	if c.fd >= 0 {
		_ = l.poller.Del(int(c.fd))
		l.unregister(c)
		_ = unix.Close(int(c.fd))
		c.fd = -1
	}
	peer := c.peer
	l.putConn(c)
	if e, ok := err.(unix.Errno); ok {
		err = &net.OpError{Op: "dial", Net: "tcp", Addr: peer, Err: e}
	}
	d.res.Reject(err)
}

// Dial is a convenience: ConnectTCP on the engine's next loop.
func (e *Engine) Dial(addr string, f Factory, opts ...DialOption) *Future[Transport] {
	return e.NextLoop().ConnectTCP(addr, f, opts...)
}
