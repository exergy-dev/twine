package twine

import (
	"net"
	"time"

	"golang.org/x/sys/unix"
)

// *conn implements Transport. Every method is loop-affine except Loop().
var _ Transport = (*conn)(nil)
var _ Consumer = (*conn)(nil)

// Write copies b to the outbound ring only if an immediate write would block.
func (c *conn) Write(b []byte) {
	c.loop.assertInLoop()
	c.write(b)
}

// Writev writes several slices with one syscall when possible.
func (c *conn) Writev(bs ...[]byte) {
	c.loop.assertInLoop()
	c.writev(bs)
}

// LoseConnection flushes pending writes, sends SHUT_WR and closes once the
// peer closes its side.
func (c *conn) LoseConnection() {
	c.loop.assertInLoop()
	c.loseConnection()
}

// Abort closes immediately, discarding pending writes. ConnectionLost is
// delivered with a nil error at the end of the iteration.
func (c *conn) Abort() {
	c.loop.assertInLoop()
	c.abortNoCallback()
}

// GetPeer returns the remote address.
func (c *conn) GetPeer() net.Addr { return c.peer }

// GetHost returns the local address.
func (c *conn) GetHost() net.Addr { return c.host }

// Loop returns the owning loop. Safe from any goroutine.
func (c *conn) Loop() *Loop { return c.loop }

// SetIdleTimeout arms the idle timer; 0 disables.
func (c *conn) SetIdleTimeout(d time.Duration) {
	c.loop.assertInLoop()
	c.idleTimeout = d
	if c.flags&flagClosed != 0 {
		return
	}
	if d <= 0 {
		if c.idle.Scheduled() {
			c.loop.wheel.Remove(&c.idle)
		}
		return
	}
	c.loop.wheel.Add(&c.idle, c.loop.nowNs+int64(d))
}

// SetContext stores a per-connection user value.
func (c *conn) SetContext(v any) { c.ctx = v }

// Context returns the per-connection user value.
func (c *conn) Context() any { return c.ctx }

// PauseReading drops read interest.
func (c *conn) PauseReading() {
	c.loop.assertInLoop()
	if c.flags&(flagClosed|flagReadPaused|flagEOF) != 0 {
		return
	}
	c.flags |= flagReadPaused
	_ = c.loop.poller.Mod(int(c.fd), c.interest())
}

// ResumeReading restores read interest and performs one immediate read so an
// edge missed while paused is not lost.
func (c *conn) ResumeReading() {
	c.loop.assertInLoop()
	if c.flags&flagClosed != 0 || c.flags&flagReadPaused == 0 {
		return
	}
	if c.flags&flagEOF != 0 {
		return
	}
	c.flags &^= flagReadPaused
	_ = c.loop.poller.Mod(int(c.fd), c.interest())
	if c.flags&flagInReady == 0 {
		c.flags |= flagInReady
		c.loop.ready = append(c.loop.ready, c)
	}
}

// RegisterProducer attaches a producer. A streaming (push) producer is paused
// and resumed around the write watermarks; a non-streaming (pull) producer
// has ResumeProducing called whenever the ring drains, and once immediately.
func (c *conn) RegisterProducer(p Producer, streaming bool) {
	c.loop.assertInLoop()
	if c.producer != nil {
		panic("twine: RegisterProducer called with a producer already registered")
	}
	c.producer = p
	c.streaming = streaming
	c.flags &^= flagProducerPaused
	if c.flags&flagClosed != 0 {
		c.loop.safeCall(c, p.StopProducing)
		c.producer = nil
		return
	}
	if streaming {
		if c.ring.Len() > c.loop.cfg.writeHighWater {
			c.flags |= flagProducerPaused
			c.loop.safeCall(c, p.PauseProducing)
		}
	} else if c.ring.Len() == 0 {
		c.loop.safeCall(c, p.ResumeProducing)
	}
}

// UnregisterProducer detaches the producer.
func (c *conn) UnregisterProducer() {
	c.loop.assertInLoop()
	c.producer = nil
	c.flags &^= flagProducerPaused
}

// PendingWrite returns the bytes buffered in the outbound ring.
func (c *conn) PendingWrite() int { return c.ring.Len() }

// SetNoDelay toggles TCP_NODELAY on the socket.
func (c *conn) SetNoDelay(on bool) error {
	v := 0
	if on {
		v = 1
	}
	return unix.SetsockoptInt(int(c.fd), unix.IPPROTO_TCP, unix.TCP_NODELAY, v)
}
