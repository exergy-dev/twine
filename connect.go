package twine

import (
	"net"

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

// ConnectTCP starts a non-blocking connect to addr. The Future resolves with
// the Transport after ConnectionMade has been called, or rejects with the
// connect error or ErrConnectTimeout. It is safe to call from any goroutine.
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
	if l.InLoop() {
		l.startConnect(addr, f, dc, res)
	} else {
		l.CallFromThread(func() { l.startConnect(addr, f, dc, res) })
	}
	return fut
}

func (l *Loop) startConnect(addr string, f Factory, dc dialConfig, res Resolver[Transport]) {
	if l.stopping {
		res.Reject(ErrEngineStopped)
		return
	}
	fd, raddr, inProgress, err := sock.Connect("tcp", addr, dc.local)
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
		p = nopProtocol{}
	}
	c.setProto(p)
	// The fd may already be registered with Writable interest; set the
	// steady-state interest set.
	var err error
	if c.flags&flagWantWrite != 0 {
		err = l.poller.Mod(fd, poll.Readable)
	} else {
		err = l.poller.Mod(fd, poll.Readable)
		if err != nil {
			err = l.poller.Add(fd, poll.Readable)
		}
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
