package proto

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// freeAddr returns a loopback address with nothing listening on it.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestReconnectingClient(t *testing.T) {
	e := startEngine(t, 2)
	addr := freeAddr(t)

	states := make(chan bool, 64)
	var built atomic.Int32
	clientFactory := twine.FactoryFunc(func(net.Addr) twine.Protocol {
		built.Add(1)
		return &LineReceiver{}
	})
	rc := NewReconnectingClient(e.Loops()[0], addr, clientFactory,
		InitialDelay(10*time.Millisecond), MaxDelay(40*time.Millisecond),
		OnStateChange(func(up bool) { states <- up }))
	rc.Start()
	rc.Start() // idempotent

	// Nothing listens yet: attempts fail and are retried with backoff.
	waitFor(t, "failed retries", func() bool { return rc.Attempts() >= 4 })
	if rc.Connected() {
		t.Fatal("connected with no server")
	}

	// The server comes up; the client connects on a later retry.
	serverConns := make(chan twine.Transport, 8)
	lst, err := e.ListenTCP(addr, twine.FactoryFunc(func(net.Addr) twine.Protocol {
		p := &LineReceiver{}
		p.OnConnectionMade = func(t twine.Transport) { serverConns <- t }
		return p
	}))
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, states, true)
	if !rc.Connected() || built.Load() != 1 {
		t.Fatalf("connected %v built %d", rc.Connected(), built.Load())
	}

	// The server drops the connection; the client reconnects.
	srv := <-serverConns
	srv.Loop().CallFromThread(srv.Abort)
	expectState(t, states, false)
	expectState(t, states, true)
	if built.Load() != 2 {
		t.Fatalf("built %d protocols", built.Load())
	}

	// StopTrying: the next loss is not retried.
	rc.StopTrying()
	onLoop(t, e.Loops()[0], func() {}) // FIFO barrier: StopTrying has run
	srv = <-serverConns
	srv.Loop().CallFromThread(srv.Abort)
	expectState(t, states, false)
	n := rc.Attempts()
	time.Sleep(200 * time.Millisecond)
	if rc.Attempts() != n || rc.Connected() {
		t.Fatalf("still trying after StopTrying: attempts %d -> %d", n, rc.Attempts())
	}

	// Start resumes.
	rc.Start()
	expectState(t, states, true)
	rc.StopTrying()
	lst.Close()
}

func expectState(t *testing.T, ch <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("state %v, want %v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no state change to %v", want)
	}
}

func TestReconnectingClientMaxRetries(t *testing.T) {
	e := startEngine(t, 1)
	rc := NewReconnectingClient(e.Loops()[0], freeAddr(t), twine.FactoryFunc(func(net.Addr) twine.Protocol { return nil }),
		InitialDelay(time.Millisecond), MaxRetries(3))
	rc.Start()
	waitFor(t, "give up", rc.GaveUp)
	if rc.Attempts() != 4 { // the first attempt plus three retries
		t.Fatalf("attempts %d", rc.Attempts())
	}
}

func TestReconnectingClientBackoff(t *testing.T) {
	rc := NewReconnectingClient(nil, "127.0.0.1:1", nil,
		InitialDelay(100*time.Millisecond), Factor(2), MaxDelay(time.Second))
	for n, want := range []time.Duration{100, 100, 200, 400, 800, 1000, 1000} {
		want *= time.Millisecond
		if got := rc.ceiling(max(n, 1)); got != want {
			t.Fatalf("ceiling(%d) = %v, want %v", n, got, want)
		}
	}
	var lo, hi bool
	for k := 0; k < 2000; k++ {
		d := rc.jitter(time.Second)
		if d < 0 || d > time.Second {
			t.Fatalf("jitter %v out of [0, 1s]", d)
		}
		lo = lo || d < 100*time.Millisecond
		hi = hi || d > 900*time.Millisecond
	}
	if !lo || !hi {
		t.Fatal("jitter does not span [0, ceiling]")
	}
	d := NewReconnectingClient(nil, "", nil)
	if d.initial != time.Second || d.max != time.Minute || d.factor != 1.6 {
		t.Fatalf("defaults %v %v %v", d.initial, d.max, d.factor)
	}
}
