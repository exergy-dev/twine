package twtls

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"syscall"
	"time"

	"github.com/exergy-dev/twine"
)

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
// methods with a type assertion: t.(*twtls.Transport).ConnectionState(),
// with this package imported as twtls (see the package documentation).
type Transport struct {
	f     *factory
	inner twine.Protocol
	peer  net.Addr // from BuildProtocol; immutable

	under twine.Transport
	loop  *twine.Loop
	bio   *bio
	tc    *tls.Conn
	// scratch is the loop's shared plaintext buffer (set when the handshake
	// completes); plain is this connection's own, allocated only if a drain
	// ever finds scratch in use (TLS nested in TLS). See loopScratch.
	scratch *loopScratch
	plain   []byte

	state     uint8
	made      bool // inner.ConnectionMade has been called
	paused    bool // inner asked to pause reading
	draining  bool // re-entrancy guard for drain
	closeSent bool // close_notify sent (LoseConnection)
	peerEOF   bool // close_notify received
	aborted   bool // Abort requested (by inner or an error); deliver nothing more
	selfAbort bool // Abort called on this Transport: ConnectionLost(nil), not a truncation
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

// ConnectionMade starts the handshake on its own goroutine.
func (w *wrapper) ConnectionMade(under twine.Transport) {
	t := (*Transport)(w)
	t.under = under
	t.loop = under.Loop()
	t.bio = newBio(t.loop, under)
	t.drainFn = t.drainTask
	if t.f.client {
		t.tc = tls.Client(t.bio, t.f.cfg)
	} else {
		t.tc = tls.Server(t.bio, t.f.cfg)
	}
	t.hsTimer = t.loop.CallLater(t.f.opts.handshakeTimeout, t.onHandshakeTimeout) // always > 0
	// From here until the Future settles, only the handshake goroutine
	// touches t.tc.
	t.startHandshake().Then(t.onHandshakeDone).Catch(t.onHandshakeError)
}

// startHandshake runs t.tc.Handshake on a goroutine of its own and returns
// a Future that settles on the loop. A handshake blocks for whole network
// round trips while it waits for the peer, so it does not run on the
// engine's bounded DeferToThread pool: there, as many silent peers as there
// are workers would hold every worker for HandshakeTimeout and stall all
// other handshakes and DeferToThread users. The Future settles through
// CallFromThread, which is the happens-before edge the concurrency rule
// relies on. Until then the goroutine is the only code touching t.tc.
func (t *Transport) startHandshake() *twine.Future[struct{}] {
	fut, res := twine.NewFuture[struct{}](t.loop)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				res.Reject(twine.NewPanicError(r))
			}
		}()
		if err := t.tc.Handshake(); err != nil {
			// Build the HandshakeError here so that the Connect Future and
			// the OnHandshakeError hook receive the same value, and settle
			// the Future from here as well (Reject is idempotent and
			// thread-safe): the loop callback that normally does it is
			// dropped if the loop exits mid-handshake.
			herr := t.handshakeError(err)
			if t.f.res != nil {
				t.f.res.Reject(herr)
			}
			res.Reject(herr)
			return
		}
		res.Resolve(struct{}{})
	}()
	return fut
}

// DataReceived feeds ciphertext to the handshake goroutine or, after the
// handshake, decrypts it on the loop and delivers plaintext to inner.
func (w *wrapper) DataReceived(b []byte) {
	t := (*Transport)(w)
	switch t.state {
	case stHandshaking:
		t.bio.feed(b) // copies: b dies when we return, the handshake reads later
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

// ConnectionLost tears down the bio (waking a blocked handshake goroutine) and
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
		// The handshake goroutine's callback reports the failure with
		// crypto/tls's own error (e.g. the peer's alert), which is more
		// useful than EOF. The goroutine also rejects the Connect Future
		// directly, so it settles even if the loop exits before that
		// callback runs.
		return
	}
	if t.made {
		if t.lostErr != nil {
			err = t.lostErr
		}
		if t.rxOffloaded && errors.Is(err, syscall.EIO) {
			err = fmt.Errorf("tls: kTLS read got a non-data record (close_notify, alert or KeyUpdate): %w", err)
		}
		if err == nil && !t.peerEOF && !t.closeSent && !t.selfAbort {
			// A TCP FIN without close_notify is a truncation, not a clean
			// close; the inner protocol must be able to tell them apart.
			// An Abort by the inner protocol itself stays nil, as the core's
			// Transport documents.
			err = io.ErrUnexpectedEOF
		}
		t.made = false
		t.inner.ConnectionLost(err)
	}
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

// onHandshakeDone runs on the loop once the handshake goroutine's Handshake returned nil.
func (t *Transport) onHandshakeDone(struct{}) {
	t.stopTimer()
	if t.state != stHandshaking {
		// Lost (or timed out) after the handshake finished but before this
		// callback ran: the inner protocol is never involved.
		t.handshakeFailed(eofOr(t.bio.err))
		return
	}
	// Ship the last handshake flight before anything else, then switch.
	t.bio.flushOut()
	t.bio.switchToLoopMode()
	t.state = stReady
	t.scratch = scratchFor(t.loop)
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

// onHandshakeError runs on the loop when the handshake goroutine's
// Handshake failed (err is then a *HandshakeError) or panicked.
func (t *Transport) onHandshakeError(err error) {
	t.stopTimer()
	if t.state == stHandshaking {
		t.state = stFailed
		t.bio.flushOut() // deliver the alert, if crypto/tls produced one
		t.bio.shutdown(err)
		t.under.Abort()
	}
	t.handshakeFailed(err)
}

// handshakeError wraps err with the peer address. When HandshakeTimeout
// expired, the handshake failed on the read error the timeout caused, and
// ErrHandshakeTimeout is reported instead. Safe from the handshake goroutine.
func (t *Transport) handshakeError(err error) *HandshakeError {
	var he *HandshakeError
	if errors.As(err, &he) {
		return he
	}
	if t.bio != nil {
		t.bio.mu.Lock()
		if t.bio.err == ErrHandshakeTimeout {
			err = ErrHandshakeTimeout
		}
		t.bio.mu.Unlock()
	}
	return &HandshakeError{Peer: t.peer, Err: err}
}

// handshakeFailed reports a failed handshake exactly once: to the
// OnHandshakeError hook, or to the engine's log when there is no hook, and
// to the Connect Future.
func (t *Transport) handshakeFailed(err error) {
	if t.hsDone {
		return
	}
	t.hsDone = true
	he := t.handshakeError(err)
	if fn := t.f.opts.onHandshakeError; fn != nil {
		fn(he)
	} else {
		loggerOf(t.loop).Warn("twine/twtls: handshake failed", "peer", t.peer, "err", he.Err)
	}
	if t.f.res != nil {
		t.f.res.Reject(he)
	}
}

// loggerOf returns the engine's logger when the core exposes one (a Logger
// method on the Loop or the Engine), and slog.Default() otherwise.
func loggerOf(l *twine.Loop) *slog.Logger {
	if l != nil {
		if lg := l.Engine().Logger(); lg != nil {
			return lg
		}
	}
	return slog.Default()
}

// drain reads plaintext until crypto/tls would block and delivers each chunk
// to inner. Loop-only, loop mode. It decrypts into the loop's shared buffer
// unless a drain further up the stack is using it (see loopScratch).
func (t *Transport) drain() {
	if t.draining {
		return
	}
	t.draining = true
	sc := t.scratch
	shared := !sc.busy
	var buf []byte
	if shared {
		sc.busy = true
		buf = sc.buf
	} else {
		if t.plain == nil {
			t.plain = make([]byte, plainBufSize)
		}
		buf = t.plain
	}
	defer func() {
		t.draining = false
		if shared {
			sc.busy = false
		}
	}()
	for t.state == stReady && !t.paused && !t.peerEOF && !t.aborted && t.lostErr == nil {
		t.inTLS = true
		n, err := t.tc.Read(buf) // may write alerts or KeyUpdate replies
		t.leaveTLS(nil)
		if n > 0 {
			t.inner.DataReceived(buf[:n])
		}
		if t.offloaded && t.bio.refused && (err == nil || errors.Is(err, errWouldBlock)) {
			// Under kTLS TX crypto/tls tried to write without failing the
			// read: that is its reply to a peer KeyUpdate that requests one.
			// It keeps the write error to itself until its next Write, which
			// never comes (Write goes to the kernel), and carries on reading
			// (Read then usually returns errWouldBlock), so the peer would
			// wait for the KeyUpdate RFC 8446 section 4.6.3 requires. Abort.
			// A real read error (whose alert was refused) is reported as is.
			err = errOffloadWrite
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
		pe := twine.NewPanicError(r)
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
func (t *Transport) ConnectionState() tls.ConnectionState {
	if t.tc == nil || t.state == stHandshaking {
		return tls.ConnectionState{}
	}
	return t.tc.ConnectionState()
}

// KernelOffload reports which directions of record protection run in the
// kernel (kTLS): tx for data this side sends, rx for data it receives. A
// server normally gets both, a client tx only, and rx is never set without
// tx; see the package documentation. With rx, any non-data record from the
// peer, close_notify included, ends the connection with an error wrapping
// syscall.EIO. The mode is fixed when inner.ConnectionMade is called.
func (t *Transport) KernelOffload() (tx, rx bool) { return t.offloaded, t.rxOffloaded }

// KernelOffloaded reports whether kTLS offload of outgoing data (tx) is in
// use.
//
// Deprecated: Use KernelOffload, which also reports the receive direction.
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

// Writev writes the slices in order. On the memory BIO each slice is
// encrypted into its own record(s), as by Write. Under kTLS TX offload they
// reach the socket in one writev and the kernel builds the records.
func (t *Transport) Writev(bs ...[]byte) {
	if t.offloaded && t.state == stReady && !t.closeSent {
		t.under.Writev(bs...)
		return
	}
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
	t.selfAbort = true
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
