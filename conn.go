package twine

import (
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/twine/internal/poll"
	"github.com/exergy-dev/twine/internal/ring"
	"github.com/exergy-dev/twine/internal/wheel"
	"golang.org/x/sys/unix"
)

// conn kinds.
const (
	kindTCP uint8 = iota
	kindListener
	kindUDP
	kindConnecting
)

// conn flags.
const (
	flagClosed         uint32 = 1 << iota // no more I/O; queued for recycle
	flagClosing                           // LoseConnection requested; flush then shut down
	flagShutWR                            // SHUT_WR already sent
	flagEOF                               // peer closed its side
	flagDirty                             // in the loop's dirty list
	flagWantWrite                         // Writable interest armed
	flagReadPaused                        // Readable interest dropped
	flagInReady                           // in the loop's ready list
	flagFlushing                          // inside flush(); Write must append only
	flagLostCalled                        // ConnectionLost delivered
	flagRecycled                          // finishClose done
	flagProducerPaused                    // PauseProducing has been delivered
	flagLinger                            // idle timer is acting as the half-close linger deadline
	flagPooled                            // allocated by getConn; may be returned to the free list
	flagHangup                            // a poll event carried a hangup: read to EOF, even after a short read
)

// conn is the per-connection state. It implements Transport directly so that
// handing a Transport to a Protocol allocates nothing. Pointer-bearing fields
// are kept few so GC scanning per connection stays cheap.
type conn struct {
	fd    int32
	kind  uint8
	flags uint32
	loop  *Loop
	proto Protocol
	ring  ring.Ring
	idle  wheel.Timer

	idleTimeout time.Duration
	peer, host  net.Addr
	ctx         any
	lostErr     error

	producer  Producer
	streaming bool

	// kind-specific
	lst  *listener
	udp  *udpState
	dial *dialState

	// gen is bumped on recycle so stale transport handles become inert.
	gen uint32
	h   *transport

	// protoName is the protocol's type name for the watchdog, which reads
	// it from another goroutine; it is resolved once per type.
	protoName atomic.Pointer[string]
}

var protoNames sync.Map // reflect.Type -> *string

// setProto records the protocol and its cached type name.
func (c *conn) setProto(p Protocol) {
	c.proto = p
	t := reflect.TypeOf(p)
	if v, ok := protoNames.Load(t); ok {
		c.protoName.Store(v.(*string))
		return
	}
	name := t.String()
	v, _ := protoNames.LoadOrStore(t, &name)
	c.protoName.Store(v.(*string))
}

// reset clears user-visible state before the conn goes on the free list.
func (c *conn) reset() {
	c.gen++
	c.h = nil
	c.fd = -1
	c.kind = kindTCP
	// Keep the struct marked closed and recycled until getConn hands it out
	// again, so any stale internal reference finds a dead conn.
	c.flags = flagClosed | flagRecycled | flagPooled
	c.proto = nil
	c.idleTimeout = 0
	c.peer, c.host = nil, nil
	c.ctx = nil
	c.lostErr = nil
	c.producer = nil
	c.streaming = false
	c.lst = nil
	c.udp = nil
	c.dial = nil
}

// queueReady schedules a re-read of c on the next iteration (read cap hit,
// reading resumed, or a hangup to collect).
func (c *conn) queueReady() {
	if c.flags&flagInReady == 0 {
		c.flags |= flagInReady
		c.loop.ready = append(c.loop.ready, readyEntry{c: c, gen: c.gen})
	}
}

// --- lifecycle -------------------------------------------------------------

// attachTCP initialises an accepted or connected socket and registers it.
func (l *Loop) attachTCP(fd int, peer, host net.Addr, f Factory) *conn {
	c := l.getConn()
	c.fd = int32(fd)
	c.kind = kindTCP
	c.peer, c.host = peer, host
	l.register(c)
	l.metrics.connections.Add(1)
	c.h = &transport{c: c, gen: c.gen}
	p := f.BuildProtocol(peer)
	if p == nil {
		// The factory refused the connection: close it without callbacks.
		c.abortNoCallback()
		return c
	}
	c.setProto(p)
	if err := l.poller.Add(fd, poll.Readable); err != nil {
		c.lostErr = err
		c.abortNoCallback()
		return c
	}
	l.callConnectionMade(c)
	return c
}

func (l *Loop) callConnectionMade(c *conn) {
	l.cbStart.Store(l.nowNs)
	l.cbProto.Store(c)
	defer l.endCallback()
	defer l.recoverProto(c)
	c.proto.ConnectionMade(c.h)
}

// onIdle is the idle-timer callback (wheel.Timer.Fn).
func (c *conn) onIdle() {
	if c.flags&flagClosed != 0 {
		return
	}
	if c.flags&flagLinger == 0 {
		c.lostErr = ErrIdleTimeout
	}
	c.abortNoCallback()
}

// eof handles a zero-length read: the peer closed its side.
func (c *conn) eof() {
	if c.flags&(flagClosed|flagEOF) != 0 {
		return
	}
	c.flags |= flagEOF
	// Stop reading; the socket will not deliver more data.
	if c.flags&flagReadPaused == 0 {
		c.flags |= flagReadPaused
	}
	if c.ring.Len() == 0 {
		// Nothing left to send: close now. ConnectionLost(nil) is a clean
		// close; a pending LoseConnection also ends here.
		c.abortNoCallback()
		return
	}
	// Pending writes remain: keep flushing, then close (peer may still read).
	c.flags |= flagClosing
	c.markDirty()
}

// fail aborts with err delivered to ConnectionLost.
func (c *conn) fail(err error) {
	if c.flags&flagClosed != 0 {
		return
	}
	c.lostErr = err
	c.abortNoCallback()
}

// abortNoCallback closes the connection at the end of this iteration without
// touching lostErr. Pending writes are discarded.
func (c *conn) abortNoCallback() {
	if c.flags&flagClosed != 0 {
		return
	}
	c.flags |= flagClosed
	c.loop.closing = append(c.loop.closing, c)
}

// loseConnection requests a graceful close: flush, then SHUT_WR, then close
// when the peer closes (or immediately if it already has).
func (c *conn) loseConnection() {
	if c.flags&(flagClosed|flagClosing) != 0 {
		return
	}
	c.flags |= flagClosing
	if c.ring.Len() == 0 {
		c.finishLose()
		return
	}
	c.markDirty()
}

// finishLose is called when the ring is empty and flagClosing is set.
func (c *conn) finishLose() {
	if c.flags&flagClosed != 0 {
		return
	}
	if c.flags&flagEOF != 0 {
		// Peer already closed; nothing left to wait for.
		c.abortNoCallback()
		return
	}
	if c.flags&flagShutWR == 0 {
		c.flags |= flagShutWR
		linger := c.loop.cfg.closeLinger
		if linger <= 0 {
			c.abortNoCallback()
			return
		}
		if err := unix.Shutdown(int(c.fd), unix.SHUT_WR); err != nil {
			c.abortNoCallback()
			return
		}
		// Keep reading until the peer closes (a read of 0 completes the
		// close with ConnectionLost(nil)), but not forever: reuse the idle
		// timer as a linger deadline.
		c.flags |= flagLinger
		c.idleTimeout = linger
		c.loop.wheel.Add(&c.idle, c.loop.nowNs+int64(linger))
		if c.flags&flagReadPaused != 0 && c.flags&flagEOF == 0 {
			// Reading was paused by the protocol; we still need to see the
			// peer's FIN, so restore read interest (data is discarded by
			// the protocol having paused, but EOF closes the conn).
			c.flags &^= flagReadPaused
			_ = c.loop.poller.Mod(int(c.fd), c.interest())
			c.queueReady()
		}
	}
}

// --- write path ------------------------------------------------------------

// markDirty queues c for the end-of-iteration flush and arms write interest.
func (c *conn) markDirty() {
	if c.flags&flagDirty == 0 {
		c.flags |= flagDirty
		c.loop.dirty = append(c.loop.dirty, c)
	}
}

func (c *conn) armWrite() {
	if c.flags&flagWantWrite != 0 {
		return
	}
	c.flags |= flagWantWrite
	_ = c.loop.poller.Mod(int(c.fd), c.interest())
}

func (c *conn) disarmWrite() {
	if c.flags&flagWantWrite == 0 {
		return
	}
	c.flags &^= flagWantWrite
	_ = c.loop.poller.Mod(int(c.fd), c.interest())
}

// interest computes the poller interest set from the flags.
func (c *conn) interest() uint32 {
	var f uint32
	if c.flags&flagReadPaused == 0 {
		f |= poll.Readable
	}
	if c.flags&flagWantWrite != 0 {
		f |= poll.Writable
	}
	return f
}

// write implements Transport.Write: direct syscall when the ring is empty,
// otherwise append.
func (c *conn) write(b []byte) {
	if len(b) == 0 {
		return
	}
	if c.flags&(flagClosed|flagClosing) != 0 {
		c.loop.metrics.writeAfterClose.Add(1)
		return
	}
	if c.ring.Len() == 0 && c.flags&flagFlushing == 0 {
		n, err := unix.Write(int(c.fd), b)
		if n == len(b) {
			c.loop.metrics.writeBytes.Add(uint64(n))
			c.loop.metrics.writeDirectHit.Add(1)
			c.touchIdle()
			if c.producer != nil && !c.streaming {
				c.markDirty() // flush pokes the pull producer for more
			}
			return
		}
		if n > 0 {
			c.loop.metrics.writeBytes.Add(uint64(n))
			b = b[n:]
		} else if err != nil && err != unix.EAGAIN && err != unix.EINTR {
			c.failWrite(err)
			return
		}
	}
	c.ring.Write(b)
	c.afterBuffer()
}

// writev implements Transport.Writev.
func (c *conn) writev(bs [][]byte) {
	if c.flags&(flagClosed|flagClosing) != 0 {
		c.loop.metrics.writeAfterClose.Add(1)
		return
	}
	total := 0
	for _, b := range bs {
		total += len(b)
	}
	if total == 0 {
		return
	}
	if c.ring.Len() == 0 && c.flags&flagFlushing == 0 && len(bs) <= len(c.loop.iovecs) {
		iov := c.loop.iovecs[:0]
		for _, b := range bs {
			if len(b) == 0 {
				continue
			}
			var v unix.Iovec
			v.Base = &b[0]
			v.SetLen(len(b))
			iov = append(iov, v)
		}
		n, err := rawWritev(int(c.fd), iov)
		if n == total {
			c.loop.metrics.writeBytes.Add(uint64(n))
			c.loop.metrics.writeDirectHit.Add(1)
			c.touchIdle()
			if c.producer != nil && !c.streaming {
				c.markDirty()
			}
			return
		}
		if n < 0 {
			if err != unix.EAGAIN && err != unix.EINTR {
				c.failWrite(err)
				return
			}
			n = 0
		}
		c.loop.metrics.writeBytes.Add(uint64(n))
		// Buffer the unwritten tail.
		for _, b := range bs {
			if n >= len(b) {
				n -= len(b)
				continue
			}
			c.ring.Write(b[n:])
			n = 0
		}
		c.afterBuffer()
		return
	}
	c.ring.Writev(bs)
	c.afterBuffer()
}

// afterBuffer runs after bytes were appended to the ring.
func (c *conn) afterBuffer() {
	c.markDirty()
	c.armWrite()
	pending := c.ring.Len()
	if int64(pending) > c.loop.metrics.ringHighWater.Load() {
		c.loop.metrics.ringHighWater.Store(int64(pending))
	}
	if pending > c.loop.cfg.writeLimit {
		c.fail(ErrWriteBufferFull)
		return
	}
	if c.producer != nil && c.streaming && c.flags&flagProducerPaused == 0 && pending > c.loop.cfg.writeHighWater {
		c.flags |= flagProducerPaused
		c.loop.safeCall(c, c.producer.PauseProducing)
	}
}

func (c *conn) failWrite(err error) {
	if err == unix.EPIPE || err == unix.ECONNRESET {
		err = ErrConnectionReset
	}
	c.fail(err)
}

// flush writes as much of the ring as the socket accepts with one writev.
func (c *conn) flush() {
	if c.ring.Len() == 0 {
		if c.flags&flagWantWrite != 0 {
			c.disarmWrite()
		}
		if c.flags&flagClosing != 0 {
			c.finishLose()
			return
		}
		if c.producer != nil && !c.streaming {
			// Pull producer: the socket took everything directly; ask for
			// more from the flush step rather than re-entrantly from Write.
			c.flags |= flagFlushing
			c.loop.safeCall(c, c.producer.ResumeProducing)
			c.flags &^= flagFlushing
			// Twisted contract: one ResumeProducing per drain. A producer
			// that wrote nothing is expected to call Write itself later,
			// which re-marks the conn dirty through the direct-hit path.
			if c.ring.Len() > 0 {
				c.markDirty()
			}
		}
		return
	}
	c.flags |= flagFlushing
	iov := c.ring.Iovecs(c.loop.iovecs[:])
	n, err := rawWritev(int(c.fd), iov)
	c.loop.metrics.writev.Add(1)
	if n > 0 {
		c.loop.metrics.writeBytes.Add(uint64(n))
		c.ring.Consume(n)
		c.touchIdle()
	} else if err != nil && err != unix.EAGAIN && err != unix.EINTR {
		c.flags &^= flagFlushing
		c.failWrite(err)
		return
	}
	if c.ring.Len() == 0 {
		c.ring.Reset() // release the slab so idle conns hold no buffer
		c.disarmWrite()
		if c.flags&flagClosing != 0 {
			c.flags &^= flagFlushing
			c.finishLose()
			return
		}
	} else {
		// Edge-triggered: Writable arrives when the socket drains.
		c.armWrite()
	}
	// Producer resume below the low-water mark; pull producers are poked
	// whenever the ring drains.
	if c.producer != nil {
		if c.streaming {
			if c.flags&flagProducerPaused != 0 && c.ring.Len() <= c.loop.cfg.writeLowWater {
				c.flags &^= flagProducerPaused
				c.loop.safeCall(c, c.producer.ResumeProducing)
			}
		} else if c.ring.Len() == 0 {
			c.loop.safeCall(c, c.producer.ResumeProducing)
		}
	}
	c.flags &^= flagFlushing
}

// touchIdle resets the idle timer after activity.
func (c *conn) touchIdle() {
	if c.idleTimeout > 0 {
		c.loop.wheel.Add(&c.idle, c.loop.nowNs+int64(c.idleTimeout))
	}
}

// sockErr reads SO_ERROR.
func (c *conn) sockErr() error {
	v, err := unix.GetsockoptInt(int(c.fd), unix.SOL_SOCKET, unix.SO_ERROR)
	if err != nil {
		return err
	}
	if v == 0 {
		return nil
	}
	e := unix.Errno(v)
	if e == unix.ECONNRESET || e == unix.EPIPE {
		return ErrConnectionReset
	}
	return e
}
