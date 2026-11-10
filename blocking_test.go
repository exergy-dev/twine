package twine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// sleepEcho is a goroutine-style protocol: it blocks in DataReceived and
// records which goroutine each callback ran on.
type sleepEcho struct {
	sleep time.Duration
	t     Transport
	l     *Loop

	mu      sync.Mutex
	gids    []int64 // goroutine of every callback, in order
	onLoop  int     // callbacks that ran on the loop thread
	lostErr error
	lost    chan struct{}
}

func (p *sleepEcho) note() {
	g := goid()
	on := p.l != nil && p.l.InLoop()
	p.mu.Lock()
	p.gids = append(p.gids, g)
	if on {
		p.onLoop++
	}
	p.mu.Unlock()
}

func (p *sleepEcho) ConnectionMade(t Transport) {
	p.t = t
	p.l = t.Loop()
	p.note()
}

func (p *sleepEcho) DataReceived(b []byte) {
	p.note()
	time.Sleep(p.sleep)
	p.t.Write(b)
}

func (p *sleepEcho) ConnectionLost(err error) {
	p.note()
	p.mu.Lock()
	p.lostErr = err
	p.mu.Unlock()
	close(p.lost)
}

func TestBlockingEcho(t *testing.T) {
	const clients, msgs = 10, 20
	e := startEngine(t, Loops(2))
	lst := listen(t, e, Blocking(FactoryFunc(func(net.Addr) Protocol {
		return &sleepEcho{sleep: 5 * time.Millisecond, lost: make(chan struct{})}
	})))
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", lst.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(20 * time.Second))
			for j := 0; j < msgs; j++ {
				msg := []byte(fmt.Sprintf("client %d message %d", i, j))
				if _, err := c.Write(msg); err != nil {
					t.Errorf("client %d: %v", i, err)
					return
				}
				got := make([]byte, len(msg))
				if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, msg) {
					t.Errorf("client %d msg %d: got %q, %v", i, j, got, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestBlockingDoesNotBlockLoop(t *testing.T) {
	const sleep = 200 * time.Millisecond
	e := startEngine(t, Loops(1))
	blk := listen(t, e, Blocking(FactoryFunc(func(net.Addr) Protocol {
		return &sleepEcho{sleep: sleep, lost: make(chan struct{})}
	})))
	plain := listen(t, e, echoFactory())
	bc := dialTCP(t, blk.Addr().String())
	ec := dialTCP(t, plain.Addr().String())
	echoOnce(t, ec, []byte("warm"))

	start := time.Now()
	if _, err := bc.Write([]byte("block")); err != nil {
		t.Fatal(err)
	}
	done := make(chan time.Duration, 1)
	go func() {
		got := make([]byte, 5)
		_ = bc.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(bc, got); err != nil || string(got) != "block" {
			t.Errorf("blocking echo: %q, %v", got, err)
		}
		done <- time.Since(start)
	}()
	// While the blocking protocol sleeps, the loop keeps serving others.
	var worst time.Duration
	pings := 0
	for time.Since(start) < sleep-20*time.Millisecond {
		t0 := time.Now()
		echoOnce(t, ec, []byte("ping"))
		if d := time.Since(t0); d > worst {
			worst = d
		}
		pings++
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("%d pings during the blocking call, worst RTT %v", pings, worst)
	if worst >= 50*time.Millisecond {
		t.Errorf("plain echo on the same loop took %v while a Blocking protocol slept", worst)
	}
	select {
	case d := <-done:
		if d < sleep {
			t.Errorf("blocking echo returned after %v, before its %v sleep", d, sleep)
		}
	case <-time.After(5 * time.Second):
		t.Error("blocking echo never answered")
	}
}

func TestBlockingConnectionLostOnInnerGoroutine(t *testing.T) {
	e := startEngine(t, Loops(1))
	pCh := make(chan *sleepEcho, 1)
	lst := listen(t, e, Blocking(FactoryFunc(func(net.Addr) Protocol {
		p := &sleepEcho{sleep: time.Millisecond, lost: make(chan struct{})}
		pCh <- p
		return p
	})))
	c := dialTCP(t, lst.Addr().String())
	echoOnce(t, c, []byte("one"))
	echoOnce(t, c, []byte("two"))
	c.Close()
	p := <-pCh
	select {
	case <-p.lost:
	case <-time.After(5 * time.Second):
		t.Fatal("inner ConnectionLost never called")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lostErr != nil {
		t.Errorf("inner ConnectionLost(%v), want nil", p.lostErr)
	}
	if len(p.gids) != 4 { // made, data, data, lost
		t.Errorf("inner callbacks = %d, want 4 (made, 2x data, lost)", len(p.gids))
	}
	for i, g := range p.gids {
		if g != p.gids[0] {
			t.Errorf("callback %d ran on goroutine %d, ConnectionMade on %d; want one goroutine", i, g, p.gids[0])
		}
	}
	if p.onLoop != 0 {
		t.Errorf("%d inner callbacks ran on the loop thread", p.onLoop)
	}
	if g := e.Loops()[0].gid.Load(); len(p.gids) > 0 && p.gids[0] == g {
		t.Error("inner protocol ran on the loop goroutine")
	}
}

// Many chunks queued behind a slow inner protocol: ordering and completeness
// hold, and the loop stays responsive.
func TestBlockingBackpressure(t *testing.T) {
	e := startEngine(t, Loops(1))
	var total atomic.Int64
	lst := listen(t, e, Blocking(FactoryFunc(func(net.Addr) Protocol {
		return &hookProto{
			made: nil,
			data: func(p *hookProto, b []byte) {
				time.Sleep(200 * time.Microsecond)
				total.Add(int64(len(b)))
				p.t.Write(b)
			},
		}
	})))
	plain := listen(t, e, echoFactory())
	c := dialTCP(t, lst.Addr().String())
	ec := dialTCP(t, plain.Addr().String())
	const size = 4 << 20
	go func() {
		buf := make([]byte, size)
		fillPattern(buf, 0)
		for off := 0; off < size; off += 1000 {
			end := off + 1000
			if end > size {
				end = size
			}
			if _, err := c.Write(buf[off:end]); err != nil {
				return
			}
		}
	}()
	readDone := make(chan struct{})
	var n, bad int64
	go func() {
		defer close(readDone)
		pc := newPatternChecker()
		_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, _ = io.CopyN(pc, c, size)
		n, bad = pc.off, pc.bad
	}()
	for i := 0; i < 20; i++ {
		t0 := time.Now()
		echoOnce(t, ec, []byte("ping"))
		if d := time.Since(t0); d > 100*time.Millisecond {
			t.Errorf("plain echo took %v while a Blocking protocol was flooded", d)
		}
		time.Sleep(5 * time.Millisecond)
	}
	<-readDone
	if n != size || bad >= 0 {
		t.Errorf("blocking echo returned %d of %d bytes (first bad %d), inner saw %d", n, size, bad, total.Load())
	}
}

// proxyUser drives every method of the Blocking proxy Transport from the
// protocol's own goroutine.
type proxyUser struct {
	t       Transport
	results chan string
	lost    chan error
}

type nopProducer struct{}

func (nopProducer) PauseProducing()  {}
func (nopProducer) ResumeProducing() {}
func (nopProducer) StopProducing()   {}

func (p *proxyUser) ConnectionMade(t Transport) {
	p.t = t
	ok := func(name string, cond bool) {
		if !cond {
			p.results <- name
		}
	}
	ok("RemoteAddr", t.RemoteAddr() != nil)
	ok("LocalAddr", t.LocalAddr() != nil)
	ok("Loop", t.Loop() != nil && !t.Loop().InLoop())
	v := new(int)
	t.SetContext(v)
	ok("Context", t.Context() == any(v))
	t.SetIdleTimeout(time.Minute)
	ok("PendingWrite", t.PendingWrite() == 0)
	t.RegisterProducer(nopProducer{}, true)
	t.UnregisterProducer()
	t.PauseReading()
	t.ResumeReading()
	t.Writev([]byte("he"), nil, []byte("llo"))
	p.results <- "done"
}

func (p *proxyUser) DataReceived(b []byte) {
	switch string(b) {
	case "panic":
		panic("proxy boom")
	case "quit":
		p.t.Write([]byte("bye"))
		p.t.LoseConnection()
	case "abort":
		p.t.Abort()
	default:
		p.t.Write(b)
	}
}

func (p *proxyUser) ConnectionLost(err error) { p.lost <- err }

func TestBlockingProxyMethods(t *testing.T) {
	e := startEngine(t, Loops(1), Logger(nil))
	results := make(chan string, 16)
	lost := make(chan error, 4)
	lst := listen(t, e, Blocking(FactoryFunc(func(net.Addr) Protocol {
		return &proxyUser{results: results, lost: lost}
	})))
	expectLost := func(want func(error) bool) {
		t.Helper()
		select {
		case err := <-lost:
			if !want(err) {
				t.Errorf("ConnectionLost(%v)", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no ConnectionLost")
		}
	}

	c := dialTCP(t, lst.Addr().String())
	for r := range results {
		if r == "done" {
			break
		}
		t.Errorf("proxy %s returned the wrong value", r)
	}
	got := make([]byte, 5)
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "hello" {
		t.Fatalf("Writev through the proxy: %q, %v", got, err)
	}
	echoOnce(t, c, []byte("echo"))
	if _, err := c.Write([]byte("quit")); err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(c); err != nil || string(b) != "bye" {
		t.Errorf("after LoseConnection read %q, %v; want bye then EOF", b, err)
	}
	c.Close()
	expectLost(func(err error) bool { return err == nil })

	// Abort from the goroutine closes the connection.
	c2 := dialTCP(t, lst.Addr().String())
	<-results
	if _, err := c2.Write([]byte("abort")); err != nil {
		t.Fatal(err)
	}
	_ = c2.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadAll(c2); err != nil && !errors.Is(err, unix.ECONNRESET) {
		t.Errorf("read after Abort: %v", err)
	}
	expectLost(func(err error) bool { return err == nil })

	// A panic in the inner protocol aborts the connection, reaches the inner
	// ConnectionLost as a PanicError, and is counted.
	before := e.Loops()[0].Metrics().Panics
	c3 := dialTCP(t, lst.Addr().String())
	<-results
	if _, err := c3.Write([]byte("panic")); err != nil {
		t.Fatal(err)
	}
	_ = c3.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.ReadAll(c3)
	expectLost(func(err error) bool {
		var pe *PanicError
		return errors.As(err, &pe) && pe.Value == "proxy boom"
	})
	if d := e.Loops()[0].Metrics().Panics - before; d != 1 {
		t.Errorf("Panics grew by %d, want 1", d)
	}
}
