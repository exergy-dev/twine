package tls

import (
	ctls "crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/exergy-dev/twine"
)

// plainBufSize is the per-connection plaintext buffer handed to
// tls.Conn.Read: one maximum-size TLS record (2^14 bytes of plaintext).
const plainBufSize = 16 << 10

// Connection states.
const (
	stHandshaking uint8 = iota
	stReady
	stFailed // handshake failed; the inner protocol is never called
	stLost   // the underlying connection is gone
)

// Transport is the twine.Transport handed to the inner protocol of a TLS
// connection. Write encrypts on the loop and ships the ciphertext to the
// underlying transport; DataReceived on the inner protocol carries plaintext.
// Like every twine.Transport it is loop-affine: all methods except Loop must
// be called on the owning loop. Inner protocols reach the TLS-specific
// methods with a type assertion: t.(*tls.Transport).ConnectionState().
type Transport struct {
	f     *factory
	inner twine.Protocol

	under twine.Transport
	loop  *twine.Loop
	bio   *bio
	tc    *ctls.Conn
	plain []byte // per-connection plaintext buffer, allocated after the handshake

	state     uint8
	made      bool // inner.ConnectionMade has been called
	paused    bool // inner asked to pause reading
	draining  bool // re-entrancy guard for drain
	closeSent bool // close_notify sent (LoseConnection)
	peerEOF   bool // close_notify received
	aborted   bool // Abort requested (by inner or an error); deliver nothing more
	hsDone    bool // handshake outcome reported (hook and Future) exactly once
	inTLS     bool // inside a tls.Conn call; Write/LoseConnection defer
	// deferred holds plaintext written re-entrantly during a tls.Conn call;
	// closeDeferred records a LoseConnection made the same way.
	deferred      []byte
	closeDeferred bool
	offloaded     bool // kTLS TX installed: Write goes straight to the socket
	rxOffloaded   bool // kTLS RX installed too: DataReceived is plaintext
	closeTimer    *twine.Timer
	lostErr       error
	hsTimer       *twine.Timer
	drainFn       func()
}

var (
	_ twine.Transport = (*Transport)(nil)
	_ twine.Consumer  = (*Transport)(nil)
)

// wrapper is the twine.Protocol installed on the underlying connection. It
// is the same object as Transport, viewed through a different method set, so
// that Transport's exported API does not include the Protocol methods.
type wrapper Transport

// ConnectionMade starts the handshake on a worker goroutine.
func (w *wrapper) ConnectionMade(under twine.Transport) {
	t := (*Transport)(w)
	t.under = under
	t.loop = under.Loop()
	t.bio = newBio(t.loop, under)
	t.drainFn = t.drainTask
	if t.f.client {
		t.tc = ctls.Client(t.bio, t.f.cfg)
	} else {
		t.tc = ctls.Server(t.bio, t.f.cfg)
	}
	if d := t.f.opts.handshakeTimeout; d > 0 {
		t.hsTimer = t.loop.CallLater(d, t.onHandshakeTimeout)
	}
	// From here until the Future settles, only the worker touches t.tc.
	t.startHandshake().Then(t.onHandshakeDone).Catch(t.onHandshakeError)
}

// handshakeOnPool selects where the blocking handshake runs. Design Section
// 10 puts it on the engine worker pool (twine.DeferToThread). It runs on a
// dedicated goroutine instead because a handshake blocks for whole network
// round trips: on a bounded pool, as many silent peers as there are workers
// (4*GOMAXPROCS by default) would hold every worker for HandshakeTimeout,
// stalling all other handshakes and every other DeferToThread user. Either
// way the Future settles on the loop through CallFromThread, which is the
// happens-before edge the concurrency rule relies on. Setting this to true
// restores the design's DeferToThread path unchanged.
const handshakeOnPool = false

// startHandshake runs t.tc.Handshake off the loop and returns a Future that
// settles on the loop.
func (t *Transport) startHandshake() *twine.Future[struct{}] {
	if handshakeOnPool {
		return twine.DeferToThread(t.loop, t.runHandshake)
	}
	fut, res := twine.NewFuture[struct{}](t.loop)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				res.Reject(&twine.PanicError{Value: r, Stack: debug.Stack()})
			}
		}()
		if _, err := t.runHandshake(); err != nil {
			// Settle the user's Connect Future from here as well (Reject is
			// idempotent and thread-safe): the loop callback that normally
			// does it is dropped if the loop exits mid-handshake.
			if t.f.res != nil {
				t.f.res.Reject(err)
			}
			res.Reject(err)
			return
		}
		res.Resolve(struct{}{})
	}()
	return fut
}

// DataReceived feeds ciphertext to the handshake worker or, after the
// handshake, decrypts it on the loop and delivers plaintext to inner.
func (w *wrapper) DataReceived(b []byte) {
	t := (*Transport)(w)
	switch t.state {
	case stHandshaking:
		t.bio.feed(b) // copies: b dies when we return, the worker reads later
	case stReady:
		if t.peerEOF || t.aborted {
			return
		}
		if t.rxOffloaded {
			t.inner.DataReceived(b) // the kernel already decrypted b
			return
		}
		t.bio.lend(b)
		defer t.bio.unlend() // never retain b, even if inner panics
		t.drain()
	}
}

// ConnectionLost tears down the bio (waking a blocked handshake worker) and
// tells inner, exactly once, if it had been told about the connection.
func (w *wrapper) ConnectionLost(err error) {
	t := (*Transport)(w)
	prev := t.state
	t.state = stLost
	if t.bio == nil {
		// Lost before ConnectionMade (the core failed to register the fd):
		// nothing was started, but a Connect Future must still settle.
		t.handshakeFailed(eofOr(err))
		return
	}
	t.bio.shutdown(eofOr(err))
	if prev == stHandshaking || prev == stFailed {
		t.stopTimer()
		// The worker's callback reports the failure with crypto/tls's own
		// error (e.g. the peer's alert), which is more useful than EOF. The
		// worker also rejects the Connect Future directly, so the Future
		// settles even if the loop exits before that callback runs.
		return
	}
	if t.made {
		if t.lostErr != nil {
			err = t.lostErr
		}
		if t.rxOffloaded && errors.Is(err, syscall.EIO) {
			err = fmt.Errorf("tls: kTLS read got a non-data record (close_notify, alert or KeyUpdate): %w", err)
		}
		if err == nil && !t.peerEOF && !t.closeSent {
			// A TCP FIN without close_notify is a truncation, not a clean
			// close; the inner protocol must be able to tell them apart.
			err = io.ErrUnexpectedEOF
		}
		t.made = false
		t.inner.ConnectionLost(err)
	}
}

// runHandshake runs on a worker goroutine. It is the only code that touches
// t.tc before the handshake Future settles.
func (t *Transport) runHandshake() (struct{}, error) {
	return struct{}{}, t.tc.Handshake()
}

func (t *Transport) stopTimer() {
	if t.hsTimer != nil {
		t.hsTimer.Stop()
		t.hsTimer = nil
	}
}

func (t *Transport) onHandshakeTimeout() {
	t.hsTimer = nil
	if t.state != stHandshaking {
		return
	}
	t.state = stFailed // never let a late onHandshakeDone call inner
	t.bio.shutdown(ErrHandshakeTimeout)
	t.under.Abort()
}

// onHandshakeDone runs on the loop once the worker's Handshake returned nil.
func (t *Transport) onHandshakeDone(struct{}) {
	t.stopTimer()
	if t.state != stHandshaking {
		// Lost (or timed out) after the worker finished but before this
		// callback ran: the inner protocol is never involved.
		t.handshakeFailed(eofOr(t.bio.err))
		return
	}
	// Ship the last handshake flight before anything else, then switch.
	t.bio.flushOut()
	t.bio.switchToLoopMode()
	t.state = stReady
	t.plain = make([]byte, plainBufSize)
	if t.f.opts.kernelOffload {
		t.offloaded = installKTLS(t)
	}
	t.made = true
	func() {
		defer t.recoverInner()
		t.inner.ConnectionMade(t)
	}()
	t.hsDone = true
	if t.f.res != nil {
		if t.lostErr != nil {
			t.f.res.Reject(t.lostErr)
		} else {
			t.f.res.Resolve(t)
		}
	}
	// Application data may already be waiting, both in the bio queue and
	// inside crypto/tls's record buffer.
	if !t.aborted && t.lostErr == nil && !t.rxOffloaded {
		t.drainGuarded()
	}
}

// onHandshakeError runs on the loop when the worker's Handshake failed.
func (t *Transport) onHandshakeError(err error) {
	t.stopTimer()
	if t.state == stHandshaking {
		t.state = stFailed
		t.bio.flushOut() // deliver the alert, if crypto/tls produced one
		t.bio.shutdown(err)
		t.under.Abort()
	}
	if t.bio.err == ErrHandshakeTimeout {
		err = ErrHandshakeTimeout
	}
	t.handshakeFailed(err)
}

func (t *Transport) handshakeFailed(err error) {
	if t.hsDone {
		return
	}
	t.hsDone = true
	if fn := t.f.opts.onHandshakeError; fn != nil {
		fn(err)
	}
	if t.f.res != nil {
		t.f.res.Reject(err)
	}
}

// drain reads plaintext until crypto/tls would block and delivers each chunk
// to inner. Loop-only, loop mode.
func (t *Transport) drain() {
	if t.draining {
		return
	}
	t.draining = true
	defer func() { t.draining = false }()
	for t.state == stReady && !t.paused && !t.peerEOF && !t.aborted && t.lostErr == nil {
		t.inTLS = true
		n, err := t.tc.Read(t.plain) // may write alerts or KeyUpdate replies
		t.leaveTLS(nil)
		if n > 0 {
			t.inner.DataReceived(t.plain[:n])
		}
		if err != nil {
			t.readError(err)
			return
		}
	}
}

// drainGuarded is drain for callers outside an underlying protocol callback,
// where a panic in inner would otherwise escape without closing the
// connection.
func (t *Transport) drainGuarded() {
	defer t.recoverInner()
	t.drain()
	if t.state == stReady {
		t.bio.unlend()
	}
}

// drainTask runs on the loop after ResumeReading.
func (t *Transport) drainTask() {
	if t.rxOffloaded {
		return
	}
	if t.state == stReady && !t.paused && !t.aborted {
		t.drainGuarded()
	}
}

// readError handles the error half of a tls.Conn.Read result.
func (t *Transport) readError(err error) {
	if errors.Is(err, errWouldBlock) {
		return
	}
	if err == io.EOF {
		// close_notify from the peer: answer with ours and close. Once
		// close_notify has gone both ways the TLS session carries nothing
		// more, so when our side is fully flushed the socket is closed
		// outright (as crypto/tls Conn.Close does) instead of waiting for the
		// peer's FIN; ConnectionLost(nil) follows either way.
		t.peerEOF = true
		t.LoseConnection()
		if t.under.PendingWrite() == 0 {
			t.under.Abort()
		}
		return
	}
	// Bad record, bad MAC, alert from the peer, ...: crypto/tls has already
	// written any alert through the bio.
	if t.lostErr == nil {
		t.lostErr = err
	}
	t.aborted = true
	t.under.Abort()
}

// recoverInner turns a panic in inner code run from a Future callback or a
// task (not from an underlying protocol callback, which the core guards)
// into an abort delivering the panic to inner.ConnectionLost. Like the
// core's own recovery, it logs the panic through the engine logger and counts
// it in MetricsSnapshot.Panics.
func (t *Transport) recoverInner() {
	if r := recover(); r != nil {
		pe, ok := r.(*twine.PanicError)
		if !ok {
			pe = &twine.PanicError{Value: r, Stack: debug.Stack()}
		}
		t.loop.ReportPanic(pe)
		if t.lostErr == nil {
			t.lostErr = pe
		}
		t.aborted = true
		t.under.Abort()
	}
}

// ConnectionState returns the negotiated TLS parameters: version, cipher
// suite, ALPN protocol, peer certificates, SNI server name, resumption. It is
// loop-affine and meaningful once inner.ConnectionMade has been called.
func (t *Transport) ConnectionState() ctls.ConnectionState {
	if t.tc == nil || t.state == stHandshaking {
		return ctls.ConnectionState{}
	}
	return t.tc.ConnectionState()
}

// KernelOffloaded reports whether record protection of outgoing data has
// been offloaded to the kernel (kTLS TX). On a server it normally covers
// incoming data too; see the package documentation. The mode is fixed when
// inner.ConnectionMade is called.
func (t *Transport) KernelOffloaded() bool { return t.offloaded }

// Underlying returns the raw (ciphertext) transport. Writing to it directly
// corrupts the TLS stream; it is exposed for socket-level inspection and
// metrics.
func (t *Transport) Underlying() twine.Transport { return t.under }

// Write encrypts b into one or more TLS records and writes them to the
// underlying transport, which copies only if the socket would block. Writes
// after LoseConnection or close are dropped.
func (t *Transport) Write(b []byte) {
	if t.state != stReady || t.closeSent || len(b) == 0 {
		return
	}
	if t.offloaded {
		t.under.Write(b) // the kernel builds the records
		return
	}
	if t.inTLS {
		// Re-entered from inside a crypto/tls call (a Producer's
		// PauseProducing fires synchronously from the transport write that
		// tls.Conn.Write makes while holding its output lock). Queue the
		// plaintext; leaveTLS writes it once that call has returned.
		t.deferred = append(t.deferred, b...)
		return
	}
	t.inTLS = true
	_, err := t.tc.Write(b)
	t.leaveTLS(err)
}

// leaveTLS ends a crypto/tls call: it writes plaintext and close_notify that
// were requested re-entrantly during the call, then reports a write error.
func (t *Transport) leaveTLS(err error) {
	for err == nil && (len(t.deferred) > 0 || t.closeDeferred) {
		if len(t.deferred) > 0 {
			d := t.deferred
			t.deferred = nil
			_, err = t.tc.Write(d)
			continue
		}
		t.closeDeferred = false
		_ = t.tc.CloseWrite()
		t.under.LoseConnection()
	}
	t.inTLS = false
	if err != nil {
		t.deferred, t.closeDeferred = nil, false
		t.writeError(err)
	}
}

// Writev encrypts each slice in turn, as Write.
func (t *Transport) Writev(bs ...[]byte) {
	for _, b := range bs {
		t.Write(b)
	}
}

func (t *Transport) writeError(err error) {
	if t.lostErr == nil {
		t.lostErr = err
	}
	t.aborted = true
	t.under.Abort()
}

// LoseConnection sends a close_notify alert and then loses the underlying
// connection gracefully (flush, then half-close). inner.ConnectionLost
// follows with a nil error once the peer has closed.
func (t *Transport) LoseConnection() {
	if t.state != stReady {
		if t.under != nil {
			t.under.LoseConnection()
		}
		return
	}
	if t.offloaded {
		if !t.closeSent {
			t.closeSent = true
			t.closeOffloaded(time.Millisecond)
		}
		return
	}
	if t.closeDeferred {
		return
	}
	if !t.closeSent {
		t.closeSent = true
		if t.inTLS {
			t.closeDeferred = true // see Write
			return
		}
		t.inTLS = true
		_ = t.tc.CloseWrite() // writes close_notify through the bio
		t.leaveTLS(nil)
	}
	t.under.LoseConnection()
}

// closeOffloaded sends close_notify through the kernel and then loses the
// connection. The alert must follow every byte of application data already
// accepted, but data the core could not write immediately sits in its ring
// and reaches the socket later; an alert sent now would overtake it. So the
// alert waits (polling with a short back-off) until the ring is empty.
func (t *Transport) closeOffloaded(delay time.Duration) {
	t.closeTimer = nil
	if t.state != stReady {
		return
	}
	if t.under.PendingWrite() == 0 {
		fder, _ := t.under.(interface{ FD() int })
		fd := -1
		if fder != nil {
			fd = fder.FD()
		}
		if fd < 0 {
			t.under.Abort()
			return
		}
		err := sendCloseNotify(fd)
		if err == nil {
			t.under.LoseConnection()
			return
		}
		if !isRetry(err) {
			t.under.Abort()
			return
		}
	}
	if delay < 20*time.Millisecond {
		delay *= 2
	}
	t.closeTimer = t.loop.CallLater(delay, func() { t.closeOffloaded(delay) })
}

// Abort closes the underlying connection immediately without close_notify,
// discarding pending writes.
func (t *Transport) Abort() {
	t.aborted = true // stop delivering plaintext already decrypted
	if t.under != nil {
		t.under.Abort()
	}
}

// GetPeer returns the remote address.
func (t *Transport) GetPeer() net.Addr { return t.under.GetPeer() }

// GetHost returns the local address.
func (t *Transport) GetHost() net.Addr { return t.under.GetHost() }

// Loop returns the owning loop. Safe from any goroutine.
func (t *Transport) Loop() *twine.Loop { return t.loop }

// SetIdleTimeout arms the underlying idle timer; 0 disables. Ciphertext
// traffic in either direction resets it.
func (t *Transport) SetIdleTimeout(d time.Duration) { t.under.SetIdleTimeout(d) }

// SetContext stores a per-connection user value on the underlying transport.
func (t *Transport) SetContext(v any) { t.under.SetContext(v) }

// Context returns the per-connection user value.
func (t *Transport) Context() any { return t.under.Context() }

// PauseReading stops plaintext delivery and drops read interest on the
// socket. Ciphertext already received stays buffered until ResumeReading.
func (t *Transport) PauseReading() {
	t.paused = true
	t.under.PauseReading()
}

// ResumeReading restores read interest and, on the next loop iteration,
// delivers any plaintext buffered while paused.
func (t *Transport) ResumeReading() {
	if !t.paused {
		return
	}
	t.paused = false
	t.under.ResumeReading()
	if t.state == stReady {
		t.loop.CallFromThread(t.drainFn)
	}
}

// RegisterProducer registers p with the underlying transport; the
// watermarks apply to buffered ciphertext.
func (t *Transport) RegisterProducer(p twine.Producer, streaming bool) {
	t.under.RegisterProducer(p, streaming)
}

// UnregisterProducer detaches the producer.
func (t *Transport) UnregisterProducer() { t.under.UnregisterProducer() }

// PendingWrite returns the ciphertext bytes buffered in the underlying ring.
func (t *Transport) PendingWrite() int { return t.under.PendingWrite() }
