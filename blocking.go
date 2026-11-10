package twine

import (
	"net"
	"time"
)

// Blocking wraps a Factory so that each connection's Protocol runs on its own
// goroutine fed by a channel, at goroutine-per-connection cost. DataReceived
// copies each chunk into the channel; the Transport the Protocol sees is a
// thread-safe proxy that routes every call through CallFromThread. Every
// proxy method returns at once except Context and PendingWrite, which have a
// result to return and so wait for a round trip to the loop. This is the
// migration path for code that blocks or is not ready for callback style. It
// works over any Transport, including the twtls package's: wrap the inner
// factory, as in twtls.Server(cfg, twine.Blocking(f)).
//
// Backpressure: the channel holds up to 64 chunks; when it is full the loop
// pauses reading until the goroutine catches up. A panic in the wrapped
// Protocol is logged and counted like a loop-resident one, aborts only its
// connection, and reaches its ConnectionLost as a *PanicError. If f returns
// nil for a connection, so does the wrapped factory.
func Blocking(f Factory) Factory {
	return func(peer net.Addr) Protocol {
		inner := f(peer)
		if inner == nil {
			return nil
		}
		return &blockingProto{inner: inner}
	}
}

type blockingEvent struct {
	data []byte
	lost bool
	err  error
}

type blockingProto struct {
	inner Protocol
	t     Transport // the underlying transport; loop-affine
	loop  *Loop
	proxy *asyncTransport
	ch    chan blockingEvent
	// paused reading because the channel filled
	paused bool
	// inflight counts chunks queued but not yet consumed
	inflight int
}

const blockingDepth = 64

func (b *blockingProto) ConnectionMade(t Transport) {
	b.t = t
	b.loop = t.Loop()
	// The addresses are captured here, on the loop, because the proxy
	// returns them on the protocol's goroutine.
	b.proxy = &asyncTransport{t: t, loop: b.loop, peer: t.RemoteAddr(), host: t.LocalAddr()}
	// +1 so the final ConnectionLost event never blocks the loop when the
	// goroutine is behind by a full window.
	b.ch = make(chan blockingEvent, blockingDepth+1)
	go b.run()
}

func (b *blockingProto) run() {
	// A panic in the wrapped protocol aborts only this connection, as it
	// would for a loop-resident protocol.
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		pe := NewPanicError(r)
		b.loop.ReportPanic(pe)
		b.loop.CallFromThread(b.t.Abort)
		// Drain so the loop's final ConnectionLost send never blocks, and
		// deliver the panic to the inner protocol in place of whatever the
		// abort reported (nil for the core, a truncation error under TLS).
		for ev := range b.ch {
			if ev.lost {
				err := error(pe)
				func() {
					defer func() { _ = recover() }()
					b.inner.ConnectionLost(err)
				}()
				return
			}
			b.loop.CallFromThread(b.consumed)
		}
	}()
	b.inner.ConnectionMade(b.proxy)
	for ev := range b.ch {
		if ev.lost {
			b.inner.ConnectionLost(ev.err)
			return
		}
		b.inner.DataReceived(ev.data)
		b.loop.CallFromThread(b.consumed)
	}
}

// consumed runs on the loop after the goroutine processed one chunk.
func (b *blockingProto) consumed() {
	b.inflight--
	if b.paused && b.inflight < blockingDepth/2 {
		b.paused = false
		b.t.ResumeReading()
	}
}

func (b *blockingProto) DataReceived(data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	b.inflight++
	b.ch <- blockingEvent{data: cp}
	if b.inflight >= blockingDepth && !b.paused {
		b.paused = true
		b.t.PauseReading()
	}
}

func (b *blockingProto) ConnectionLost(err error) {
	if b.ch == nil {
		b.inner.ConnectionLost(err)
		return
	}
	b.ch <- blockingEvent{lost: true, err: err}
	close(b.ch)
}

// asyncTransport is a Transport proxy safe to call from any goroutine. Writes
// copy their argument because they are delivered asynchronously. Every method
// returns at once except Context and PendingWrite, which wait for a round
// trip to the loop (see sync); RemoteAddr, LocalAddr and Loop answer from values
// captured at ConnectionMade.
type asyncTransport struct {
	t          Transport
	loop       *Loop
	peer, host net.Addr
}

func (a *asyncTransport) call(fn func()) { a.loop.CallFromThread(fn) }

func (a *asyncTransport) Write(b []byte) {
	cp := make([]byte, len(b))
	copy(cp, b)
	a.call(func() { a.t.Write(cp) })
}

func (a *asyncTransport) Writev(bs ...[]byte) {
	total := 0
	for _, b := range bs {
		total += len(b)
	}
	cp := make([]byte, 0, total)
	for _, b := range bs {
		cp = append(cp, b...)
	}
	a.call(func() { a.t.Write(cp) })
}

func (a *asyncTransport) LoseConnection()      { a.call(a.t.LoseConnection) }
func (a *asyncTransport) Abort()               { a.call(a.t.Abort) }
func (a *asyncTransport) RemoteAddr() net.Addr { return a.peer }
func (a *asyncTransport) LocalAddr() net.Addr  { return a.host }
func (a *asyncTransport) Loop() *Loop          { return a.loop }
func (a *asyncTransport) SetContext(v any)     { a.call(func() { a.t.SetContext(v) }) }
func (a *asyncTransport) Context() any         { return a.sync(a.t.Context) }
func (a *asyncTransport) PauseReading()        { a.call(a.t.PauseReading) }
func (a *asyncTransport) ResumeReading()       { a.call(a.t.ResumeReading) }
func (a *asyncTransport) UnregisterProducer()  { a.call(a.t.UnregisterProducer) }

// PendingWrite blocks the calling goroutine until the loop answers, as does
// Context above.
func (a *asyncTransport) PendingWrite() int {
	return a.sync(func() any { return a.t.PendingWrite() }).(int)
}

func (a *asyncTransport) SetIdleTimeout(d time.Duration) {
	a.call(func() { a.t.SetIdleTimeout(d) })
}

func (a *asyncTransport) RegisterProducer(p Producer, streaming bool) {
	a.call(func() { a.t.RegisterProducer(p, streaming) })
}

// sync runs fn on the loop and waits for its result. Once the loop has
// exited nothing else touches the transport, so fn runs on the caller.
func (a *asyncTransport) sync(fn func() any) any {
	l := a.loop
	if l.InLoop() {
		return fn()
	}
	ch := make(chan any, 1)
	l.CallFromThread(func() { ch <- fn() })
	select {
	case v := <-ch:
		return v
	case <-l.exitCh:
		return fn()
	}
}
