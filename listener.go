package twine

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/exergy-dev/twine/internal/poll"
	"github.com/exergy-dev/twine/internal/sock"
	"golang.org/x/sys/unix"
)

// listener is a TCP or UDP Listener. For TCP with ReusePort it owns one fd
// per loop; otherwise a single fd on loop 0. A UDP listener has one conn of
// kind kindUDP and a nil factory; the datagram protocol lives on that conn.
type listener struct {
	eng     *Engine
	addr    net.Addr
	factory Factory
	lc      listenConfig

	mu     sync.Mutex
	conns  []*conn // the conn registered on each loop; c.fd is the socket
	closed atomic.Bool
}

// Addr returns the bound address.
func (l *listener) Addr() net.Addr { return l.addr }

// Close stops accepting. Existing connections are unaffected. Safe from any
// goroutine; completes asynchronously on each loop.
func (l *listener) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	e := l.eng
	e.removeListener(l)
	l.mu.Lock()
	conns := l.conns
	l.mu.Unlock()

	// Before Run (or after a Stop that came before Run) no loop is running
	// and none can start while e.mu is held (Run takes it to start), so the
	// sockets are closed here; the queued register tasks then find fd == -1.
	// Otherwise the close is queued behind the register task on each loop:
	// touching c.fd from this goroutine would race with the loop.
	e.mu.Lock()
	if st := e.state.Load(); st == stateNew || (st == stateStopped && !e.ran.Load()) {
		for _, c := range conns {
			if c.fd >= 0 {
				_ = unix.Close(int(c.fd))
				c.fd = -1
			}
		}
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()
	for _, c := range conns {
		c.loop.CallFromThread(func() {
			if c.kind == kindUDP {
				c.closeUDP()
			} else {
				c.closeListenerFD()
			}
		})
	}
	return nil
}

func (e *Engine) newTCPListener(addr string, f Factory, lc listenConfig) (*listener, error) {
	lst := &listener{eng: e, factory: f, lc: lc}
	perLoop := e.cfg.reusePort && len(e.loops) > 1
	n := 1
	if perLoop {
		n = len(e.loops)
	}
	var fds []int // sockets opened so far, closed if a later one fails
	for i := 0; i < n; i++ {
		fd, laddr, err := sock.Listen("tcp", addr, e.cfg.reusePort, lc.backlog)
		if err != nil {
			for _, fd := range fds {
				_ = unix.Close(fd)
			}
			return nil, err
		}
		if lc.deferAccept > 0 {
			_ = sock.SetDeferAccept(fd, lc.deferAccept)
		}
		if lst.addr == nil {
			lst.addr = laddr
			if n > 1 && laddr.Port != 0 {
				// Bind the remaining sockets to the resolved port.
				addr = laddr.String()
			}
		}
		fds = append(fds, fd)
		lp := e.loops[i]
		c := &conn{loop: lp, fd: int32(fd), kind: kindListener, lst: lst}
		lst.conns = append(lst.conns, c)
		lp.CallFromThread(func() { lp.registerListener(c) })
	}
	return lst, nil
}

// registerListener adds the listening fd to the loop's poller.
func (l *Loop) registerListener(c *conn) {
	if c.fd < 0 {
		return
	}
	if l.stopping || c.lst.closed.Load() {
		_ = unix.Close(int(c.fd))
		c.fd = -1
		return
	}
	l.register(c)
	if err := l.poller.Add(int(c.fd), poll.Readable); err != nil {
		l.cfg.logger.Error("twine: registering listener", "loop", l.id, "err", err)
		l.unregister(c)
		_ = unix.Close(int(c.fd))
		c.fd = -1
		return
	}
	// Drain any backlog that accumulated before registration (edge-triggered
	// epoll would otherwise wait for the next arrival).
	l.handleAccept(c)
}

// closeListenerFD removes and closes a listening socket (loop-affine).
func (c *conn) closeListenerFD() {
	l := c.loop
	if c.fd < 0 {
		return
	}
	_ = l.poller.Del(int(c.fd))
	l.unregister(c)
	_ = unix.Close(int(c.fd))
	c.fd = -1
}

// handleAccept accepts until EAGAIN or maxAcceptsPerEvent.
func (l *Loop) handleAccept(c *conn) {
	lst := c.lst
	lfd := int(c.fd)
	if lfd < 0 {
		return
	}
	for i := 0; i < maxAcceptsPerEvent; i++ {
		fd, sa, err := sock.Accept(lfd)
		if err != nil {
			if err == unix.EAGAIN {
				return
			}
			l.metrics.acceptErrors.Add(1)
			if err == unix.EMFILE || err == unix.ENFILE || err == unix.ENOBUFS || err == unix.ENOMEM {
				l.cfg.logger.Warn("twine: accept failed", "loop", l.id, "err", err)
				return
			}
			continue
		}
		l.metrics.accepts.Add(1)
		l.setupAccepted(fd, lst)
		peer := sock.SockaddrToTCP(sa)
		host := lst.addr
		if l.cfg.reusePort || len(l.eng.loops) == 1 {
			l.attachTCP(fd, peer, host, lst.factory)
			continue
		}
		// Single listener: hand the connection to the next loop.
		target := l.eng.acceptLoop()
		if target == l {
			l.attachTCP(fd, peer, host, lst.factory)
			continue
		}
		target.CallFromThread(func() {
			if target.stopping {
				_ = unix.Close(fd)
				return
			}
			target.attachTCP(fd, peer, host, lst.factory)
		})
	}
	// Cap hit; more may be pending. Re-check next iteration.
	c.queueReady()
}

// setupAccepted applies per-socket options.
func (l *Loop) setupAccepted(fd int, lst *listener) {
	if lst.lc.noDelay {
		_ = sock.SetNoDelay(fd, true)
	}
	if lst.lc.rcvBuf > 0 || lst.lc.sndBuf > 0 {
		_ = sock.SetBuffers(fd, lst.lc.rcvBuf, lst.lc.sndBuf)
	}
	if lst.lc.busyPoll > 0 {
		_ = sock.SetBusyPoll(fd, lst.lc.busyPoll)
	}
}
