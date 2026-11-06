package twine

import (
	"net"
	"time"
)

// Blocking wraps a Factory so that each connection's Protocol runs on its own
// goroutine fed by a channel, at goroutine-per-connection cost. DataReceived
// copies each chunk into the channel; the Transport the Protocol sees is a
// thread-safe proxy that routes every call through CallFromThread. This is
// the migration path for code that blocks or is not ready for callback style.
//
// Backpressure: the channel holds up to 64 chunks; when it is full the loop
// pauses reading until the goroutine catches up.
func Blocking(f Factory) Factory {
	return FactoryFunc(func(peer net.Addr) Protocol {
		return &blockingProto{inner: f.BuildProtocol(peer)}
	})
}

type blockingEvent struct {
	data []byte
	lost bool
	err  error
}

type blockingProto struct {
	inner Protocol
	t     *transport
	proxy *asyncTransport
	ch    chan blockingEvent
	// paused reading because the channel filled
	paused bool
	// inflight counts chunks queued but not yet consumed
	inflight int
}

const blockingDepth = 64

func (b *blockingProto) ConnectionMade(t Transport) {
	b.t = t.(*transport)
	b.proxy = &asyncTransport{c: b.t}
	// +1 so the final ConnectionLost event never blocks the loop when the
	// goroutine is behind by a full window.
	b.ch = make(chan blockingEvent, blockingDepth+1)
	go b.run()
}

func (b *blockingProto) run() {
	b.inner.ConnectionMade(b.proxy)
	for ev := range b.ch {
		if ev.lost {
			b.inner.ConnectionLost(ev.err)
			return
		}
		b.inner.DataReceived(ev.data)
		b.t.c.loop.CallFromThread(b.consumed)
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
// copy their argument because they are delivered asynchronously.
type asyncTransport struct {
	c *transport
}

func (a *asyncTransport) call(fn func()) { a.c.c.loop.CallFromThread(fn) }

func (a *asyncTransport) Write(b []byte) {
	cp := make([]byte, len(b))
	copy(cp, b)
	a.call(func() { a.c.Write(cp) })
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
	a.call(func() { a.c.Write(cp) })
}

func (a *asyncTransport) LoseConnection()     { a.call(a.c.LoseConnection) }
func (a *asyncTransport) Abort()              { a.call(a.c.Abort) }
func (a *asyncTransport) GetPeer() net.Addr   { return a.c.GetPeer() }
func (a *asyncTransport) GetHost() net.Addr   { return a.c.GetHost() }
func (a *asyncTransport) Loop() *Loop         { return a.c.Loop() }
func (a *asyncTransport) SetContext(v any)    { a.call(func() { a.c.SetContext(v) }) }
func (a *asyncTransport) Context() any        { return a.sync(func() any { return a.c.Context() }) }
func (a *asyncTransport) PauseReading()       { a.call(a.c.PauseReading) }
func (a *asyncTransport) ResumeReading()      { a.call(a.c.ResumeReading) }
func (a *asyncTransport) UnregisterProducer() { a.call(a.c.UnregisterProducer) }

// PendingWrite blocks the calling goroutine until the loop answers.
func (a *asyncTransport) PendingWrite() int {
	return a.sync(func() any { return a.c.PendingWrite() }).(int)
}

func (a *asyncTransport) SetIdleTimeout(d time.Duration) {
	a.call(func() { a.c.SetIdleTimeout(d) })
}

func (a *asyncTransport) RegisterProducer(p Producer, streaming bool) {
	a.call(func() { a.c.RegisterProducer(p, streaming) })
}

// sync runs fn on the loop and waits for its result.
func (a *asyncTransport) sync(fn func() any) any {
	l := a.c.c.loop
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
