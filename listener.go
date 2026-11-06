package twine

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/robert-malhotra/twine/internal/poll"
	"github.com/robert-malhotra/twine/internal/sock"
	"golang.org/x/sys/unix"
)

// listener is a TCP or UDP Listener. For TCP with ReusePort it owns one fd
// per loop; otherwise a single fd on loop 0.
type listener struct {
	eng     *Engine
	addr    net.Addr
	factory Factory
	dgram   DatagramProtocol
	lc      listenConfig
	udp     bool

	mu     sync.Mutex
	fds    []int   // one per loop (or one)
	conns  []*conn // the conn registered on each loop
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
	l.eng.removeListener(l)
	l.mu.Lock()
	conns := l.conns
	l.mu.Unlock()
	for _, c := range conns {
		c := c
		lp := c.loop
		if lp.started.Load() {
			lp.CallFromThread(func() {
				if c.kind == kindUDP {
					c.closeUDP()
				} else {
					c.closeListenerFD()
				}
			})
		} else {
			// Loop never started; close the fd directly. registerListener
			// sees fd == -1 and skips it.
			_ = unix.Close(int(c.fd))
			c.fd = -1
		}
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
	for i := 0; i < n; i++ {
		fd, laddr, err := sock.Listen("tcp", addr, e.cfg.reusePort, lc.backlog)
		if err != nil {
			for _, fd := range lst.fds {
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
		lst.fds = append(lst.fds, fd)
		lp := e.loops[i]
		c := &conn{loop: lp, fd: int32(fd), kind: kindListener, lst: lst}
		c.idle.Fn = c.onIdle
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

// handleAccept accepts until EAGAIN or MaxAcceptsPerEvent.
func (l *Loop) handleAccept(c *conn) {
	lst := c.lst
	lfd := int(c.fd)
	if lfd < 0 {
		return
	}
	for i := 0; i < l.cfg.maxAcceptsPerEv; i++ {
		fd, sa, err := sock.Accept(lfd)
		if err != nil {
			if err == unix.EAGAIN {
				return
			}
			l.metrics.AcceptErrors.Add(1)
			if err == unix.EMFILE || err == unix.ENFILE || err == unix.ENOBUFS || err == unix.ENOMEM {
				l.cfg.logger.Warn("twine: accept failed", "loop", l.id, "err", err)
				return
			}
			continue
		}
		l.metrics.Accepts.Add(1)
		l.setupAccepted(fd, lst)
		peer := sock.SockaddrToTCP(sa)
		host := lst.addr
		if l.cfg.reusePort || len(l.eng.loops) == 1 {
			l.attachTCP(fd, peer, host, lst.factory, lst.lc.noDelay)
			continue
		}
		// Single listener: hand the connection to the next loop.
		target := l.eng.NextLoop()
		if target == l {
			l.attachTCP(fd, peer, host, lst.factory, lst.lc.noDelay)
			continue
		}
		target.CallFromThread(func() {
			if target.stopping {
				_ = unix.Close(fd)
				return
			}
			target.attachTCP(fd, peer, host, lst.factory, lst.lc.noDelay)
		})
	}
	// Cap hit; more may be pending. Re-check next iteration.
	if c.flags&flagInReady == 0 {
		c.flags |= flagInReady
		l.ready = append(l.ready, c)
	}
}

// setupAccepted applies per-socket options.
func (l *Loop) setupAccepted(fd int, lst *listener) {
	if lst.lc.noDelay {
		_ = sock.SetNoDelay(fd, true)
	}
	if lst.lc.rcvBuf > 0 || lst.lc.sndBuf > 0 {
		_ = sock.SetBuffers(fd, lst.lc.rcvBuf, lst.lc.sndBuf)
	}
	if l.cfg.busyPoll > 0 {
		_ = sock.SetBusyPoll(fd, l.cfg.busyPoll)
	}
}
