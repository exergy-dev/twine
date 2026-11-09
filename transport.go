package twine

import (
	"net"
	"time"
)

// transport is the Transport handle given to protocols. conn structs are
// pooled and reused across connections, so the handle carries the generation
// of the conn it was issued for; a handle kept after ConnectionLost becomes
// inert instead of acting on whatever connection reuses the struct. One
// 16-byte allocation per accepted/connected socket.
type transport struct {
	c   *conn
	gen uint32
}

var _ Transport = (*transport)(nil)
var _ Consumer = (*transport)(nil)

// live reports whether the handle still refers to its original connection
// and that connection is open.
func (t *transport) live() bool { return t.gen == t.c.gen && t.c.flags&flagClosed == 0 }

// FD returns the socket's file descriptor while the handle is live, and -1
// otherwise. It exists for kernel offload setup (setsockopt, for example
// kernel TLS) and must not be used for I/O: reading or writing the descriptor
// behind the loop's back corrupts its buffering and readiness state. Loop
// goroutine only.
func (t *transport) FD() int {
	if !t.live() {
		return -1
	}
	return int(t.c.fd)
}

// Write copies b to the outbound ring only if an immediate write would block.
func (t *transport) Write(b []byte) {
	t.c.loop.assertInLoop()
	if t.gen != t.c.gen {
		t.c.loop.metrics.WriteAfterClose.Add(1)
		return
	}
	t.c.write(b)
}

// Writev writes several slices with one syscall when possible.
func (t *transport) Writev(bs ...[]byte) {
	t.c.loop.assertInLoop()
	if t.gen != t.c.gen {
		t.c.loop.metrics.WriteAfterClose.Add(1)
		return
	}
	t.c.writev(bs)
}

// LoseConnection flushes pending writes, sends SHUT_WR and closes once the
// peer closes its side or CloseLinger elapses.
func (t *transport) LoseConnection() {
	t.c.loop.assertInLoop()
	if !t.live() {
		return
	}
	t.c.loseConnection()
}

// Abort closes immediately, discarding pending writes. ConnectionLost is
// delivered with a nil error at the end of the iteration.
func (t *transport) Abort() {
	t.c.loop.assertInLoop()
	if !t.live() {
		return
	}
	t.c.abortNoCallback()
}

// GetPeer returns the remote address.
func (t *transport) GetPeer() net.Addr { return t.c.peerFor(t.gen) }

// GetHost returns the local address.
func (t *transport) GetHost() net.Addr { return t.c.hostFor(t.gen) }

// Loop returns the owning loop. Safe from any goroutine.
func (t *transport) Loop() *Loop { return t.c.loop }

// SetIdleTimeout arms the idle timer; 0 disables.
func (t *transport) SetIdleTimeout(d time.Duration) {
	t.c.loop.assertInLoop()
	if !t.live() {
		return
	}
	c := t.c
	c.idleTimeout = d
	if d <= 0 {
		if c.idle.Scheduled() {
			c.loop.wheel.Remove(&c.idle)
		}
		return
	}
	c.loop.wheel.Add(&c.idle, c.loop.nowNs+int64(d))
}

// SetContext stores a per-connection user value.
func (t *transport) SetContext(v any) {
	if t.gen == t.c.gen {
		t.c.ctx = v
	}
}

// Context returns the per-connection user value.
func (t *transport) Context() any {
	if t.gen != t.c.gen {
		return nil
	}
	return t.c.ctx
}

// PauseReading drops read interest.
func (t *transport) PauseReading() {
	t.c.loop.assertInLoop()
	if !t.live() {
		return
	}
	c := t.c
	if c.flags&(flagReadPaused|flagEOF) != 0 {
		return
	}
	c.flags |= flagReadPaused
	_ = c.loop.poller.Mod(int(c.fd), c.interest())
}

// ResumeReading restores read interest and performs one immediate read so an
// edge missed while paused is not lost.
func (t *transport) ResumeReading() {
	t.c.loop.assertInLoop()
	if !t.live() {
		return
	}
	c := t.c
	if c.flags&flagReadPaused == 0 || c.flags&flagEOF != 0 {
		return
	}
	c.flags &^= flagReadPaused
	_ = c.loop.poller.Mod(int(c.fd), c.interest())
	c.queueReady()
}

// RegisterProducer attaches a producer. A streaming (push) producer is paused
// and resumed around the write watermarks; a non-streaming (pull) producer
// has ResumeProducing called whenever the ring drains, and once immediately.
func (t *transport) RegisterProducer(p Producer, streaming bool) {
	t.c.loop.assertInLoop()
	if t.gen != t.c.gen {
		return
	}
	c := t.c
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
func (t *transport) UnregisterProducer() {
	t.c.loop.assertInLoop()
	if t.gen != t.c.gen {
		return
	}
	t.c.producer = nil
	t.c.flags &^= flagProducerPaused
}

// PendingWrite returns the bytes buffered in the outbound ring.
func (t *transport) PendingWrite() int {
	if t.gen != t.c.gen {
		return 0
	}
	return t.c.ring.Len()
}

// peerFor returns the peer address if gen matches (addresses are cleared on
// recycle, so a stale handle sees nil rather than a stranger's address).
func (c *conn) peerFor(gen uint32) net.Addr {
	if gen != c.gen {
		return nil
	}
	return c.peer
}

func (c *conn) hostFor(gen uint32) net.Addr {
	if gen != c.gen {
		return nil
	}
	return c.host
}
