package tls

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/twine"
)

// wouldBlockError is the type of errWouldBlock. It is a net.Error whose
// Timeout and Temporary methods both return true: crypto/tls checks
// Temporary() (conn.go readRecordOrCCS) to decide whether a read error is
// sticky, so a temporary error leaves the tls.Conn usable for the next call.
type wouldBlockError struct{}

func (wouldBlockError) Error() string   { return "tls: memory BIO would block" }
func (wouldBlockError) Timeout() bool   { return true }
func (wouldBlockError) Temporary() bool { return true }

// errWouldBlock is returned by bio.Read in loop mode when the inbound queue
// is empty.
var errWouldBlock net.Error = wouldBlockError{}

// bio is the memory-BIO net.Conn that a *crypto/tls.Conn runs over.
//
// Handshake mode (the default): the tls.Conn is driven by the handshake
// goroutine.
// Read blocks on cond until the loop feeds ciphertext (feed) or the
// connection is lost (shutdown). Write appends to out and asks the loop, via
// CallFromThread, to ship out to the real transport (flushOut).
//
// Loop mode (after the handshake Future resolves): only the loop goroutine
// touches the bio and the tls.Conn. Read serves queued bytes and then the
// borrowed DataReceived slice, returning errWouldBlock when both are empty.
// Write passes each record straight to the underlying transport, whose
// Write copies only if the socket would block.
//
// Fields marked (mu) are guarded by mu in handshake mode. In loop mode they
// are touched only by the loop, after a happens-before edge established by
// the handshake Future (the handshake goroutine has returned from Handshake
// before the Future resolves, and resolution goes through CallFromThread).
type bio struct {
	loop  *twine.Loop     // immutable
	under twine.Transport // loop-only
	laddr net.Addr        // immutable
	raddr net.Addr        // immutable

	mu          sync.Mutex
	cond        sync.Cond
	in          []byte // (mu) owned inbound ciphertext; unread bytes are in[r:]
	r           int    // (mu) read offset into in
	out         []byte // (mu) outbound ciphertext written during the handshake
	flushQueued bool   // (mu) a flushOut task is pending on the loop
	err         error  // (mu) terminal error once the connection is gone
	noWrite     bool   // (mu) kTLS TX installed: crypto/tls writes are refused
	refused     bool   // (mu) a write was refused because of noWrite

	// loopMode is set once, by the loop, when the handshake has completed.
	// It is atomic so that Read, called on the loop for every record, can
	// test it without taking mu.
	loopMode atomic.Bool

	borrowed []byte // loop mode only: the current DataReceived slice
	flushFn  func() // b.flushOut, allocated once
}

var _ net.Conn = (*bio)(nil)

func newBio(l *twine.Loop, under twine.Transport) *bio {
	b := &bio{loop: l, under: under, laddr: under.GetHost(), raddr: under.GetPeer()}
	b.cond.L = &b.mu
	b.flushFn = b.flushOut
	return b
}

// Read implements net.Conn. In handshake mode it blocks until ciphertext is
// available or the connection is gone. In loop mode it never blocks.
func (b *bio) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.loopMode.Load() {
		return b.readLoop(p)
	}
	// Handshake mode: only the handshake goroutine reads, and it has
	// returned from Handshake before the loop sets loopMode.
	b.mu.Lock()
	for b.r == len(b.in) && b.err == nil {
		b.cond.Wait()
	}
	if b.r == len(b.in) {
		err := b.err
		b.mu.Unlock()
		return 0, err
	}
	n := b.take(p)
	b.mu.Unlock()
	return n, nil
}

// take copies queued bytes into p. Caller holds mu or is the loop in loop
// mode.
func (b *bio) take(p []byte) int {
	n := copy(p, b.in[b.r:])
	b.r += n
	if b.r == len(b.in) {
		b.in = b.in[:0]
		b.r = 0
	}
	return n
}

// readLoop is Read in loop mode: queued bytes first, then the borrowed
// slice, then errWouldBlock.
func (b *bio) readLoop(p []byte) (int, error) {
	if b.r < len(b.in) {
		return b.take(p), nil
	}
	if len(b.borrowed) > 0 {
		n := copy(p, b.borrowed)
		b.borrowed = b.borrowed[n:]
		return n, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	return 0, errWouldBlock
}

// appendIn queues a copy of p. Caller holds mu or is the loop in loop mode.
func (b *bio) appendIn(p []byte) {
	if b.r > 0 && cap(b.in)-len(b.in) < len(p) {
		// Compact before growing.
		n := copy(b.in, b.in[b.r:])
		b.in = b.in[:n]
		b.r = 0
	}
	b.in = append(b.in, p...)
}

// feed queues a copy of ciphertext for the handshake goroutine and wakes it.
// Loop-only, handshake mode.
func (b *bio) feed(p []byte) {
	b.mu.Lock()
	if b.err == nil {
		b.appendIn(p)
		b.cond.Signal()
	}
	b.mu.Unlock()
}

// lend makes p readable without copying. Loop-only, loop mode. The caller
// must call unlend before the DataReceived that supplied p returns.
func (b *bio) lend(p []byte) {
	if len(b.borrowed) > 0 {
		// A previous slice was not fully consumed (should not happen; unlend
		// copies remainders). Keep order by queueing both.
		b.appendIn(b.borrowed)
		b.borrowed = nil
	}
	if b.r < len(b.in) {
		b.appendIn(p)
		return
	}
	b.borrowed = p
}

// unlend copies whatever is left of the borrowed slice into the owned queue
// (reading was paused or the connection closed mid-chunk) and drops the
// reference. Loop-only, loop mode.
func (b *bio) unlend() {
	if len(b.borrowed) > 0 && b.err == nil {
		b.appendIn(b.borrowed)
	}
	b.borrowed = nil
}

// Write implements net.Conn. In handshake mode it queues p and schedules a
// flush on the loop; in loop mode it writes p to the transport directly.
func (b *bio) Write(p []byte) (int, error) {
	b.mu.Lock()
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return 0, err
	}
	if b.noWrite {
		// crypto/tls may swallow this error (its KeyUpdate reply path does),
		// so record the refusal for the Transport to act on.
		b.refused = true
		b.mu.Unlock()
		return 0, errOffloadWrite
	}
	if b.loopMode.Load() {
		b.mu.Unlock()
		b.under.Write(p)
		return len(p), nil
	}
	b.out = append(b.out, p...)
	schedule := !b.flushQueued
	b.flushQueued = true
	b.mu.Unlock()
	if schedule {
		b.loop.CallFromThread(b.flushFn)
	}
	return len(p), nil
}

// flushOut ships ciphertext written during the handshake to the transport.
// Loop-only. It is idempotent and is also called directly before the mode
// switch so that handshake bytes always precede application data.
func (b *bio) flushOut() {
	b.mu.Lock()
	b.flushQueued = false
	data := b.out
	b.out = nil
	b.mu.Unlock()
	if len(data) > 0 {
		b.under.Write(data) // copies if it must buffer
	}
}

// switchToLoopMode is called on the loop once the handshake has completed.
func (b *bio) switchToLoopMode() {
	b.mu.Lock()
	b.loopMode.Store(true)
	b.out = nil
	b.mu.Unlock()
}

// shutdown marks the bio dead with err and wakes a blocked handshake
// goroutine. Queued ciphertext is kept (see below). Safe from any goroutine;
// the first error wins.
func (b *bio) shutdown(err error) {
	b.mu.Lock()
	if b.err == nil {
		b.err = err
	}
	// Queued ciphertext is kept: the handshake goroutine reads it before it
	// sees err, so an alert that arrived just before the close is reported.
	b.cond.Broadcast()
	b.mu.Unlock()
}

// Close implements net.Conn. crypto/tls calls it only from tls.Conn.Close,
// which this package never calls; it is provided for completeness.
func (b *bio) Close() error {
	b.shutdown(net.ErrClosed)
	return nil
}

// LocalAddr implements net.Conn.
func (b *bio) LocalAddr() net.Addr { return b.laddr }

// RemoteAddr implements net.Conn.
func (b *bio) RemoteAddr() net.Addr { return b.raddr }

// SetDeadline implements net.Conn. Deadlines do not apply to a memory BIO;
// crypto/tls sets a write deadline around close_notify, so this must succeed.
func (b *bio) SetDeadline(time.Time) error { return nil }

// SetReadDeadline implements net.Conn as a no-op.
func (b *bio) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline implements net.Conn as a no-op.
func (b *bio) SetWriteDeadline(time.Time) error { return nil }

// eofOr maps a nil connection-lost error to io.EOF for the handshake.
func eofOr(err error) error {
	if err == nil {
		return io.EOF
	}
	return err
}
