package proto

import (
	"errors"
	"net"
	"strconv"
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

// stateEvent is one OnStateChange call.
type stateEvent struct {
	up  bool
	err error
}

func TestReconnectingClient(t *testing.T) {
	e := startEngine(t, 2)
	addr := freeAddr(t)

	states := make(chan stateEvent, 64)
	lines := make(chan string, 64)
	var built atomic.Int32
	clientFactory := func(net.Addr) twine.Protocol {
		n := built.Add(1)
		c := &LineReceiver{}
		c.OnConnectionMade = func(twine.Transport) { c.SendLine([]byte("ping " + strconv.Itoa(int(n)))) }
		c.LineReceived = func(l []byte) { lines <- string(l) }
		return c
	}
	rc := NewReconnectingClient(e.Loops()[0], addr, clientFactory,
		InitialDelay(10*time.Millisecond), MaxDelay(40*time.Millisecond),
		DialOptions(twine.DialTimeout(5*time.Second)),
		OnStateChange(func(up bool, err error) { states <- stateEvent{up, err} }))
	rc.Start()
	rc.Start() // idempotent

	// Nothing listens yet: attempts fail and are retried with backoff.
	waitFor(t, "failed retries", func() bool { return rc.Attempts() >= 4 })
	if rc.Connected() {
		t.Fatal("connected with no server")
	}
	if rc.LastError() == nil {
		t.Fatal("no LastError after failed connects")
	}

	// The server comes up; the client connects on a later retry and a line
	// round-trips through the wrapper protocol.
	serverConns := make(chan twine.Transport, 8)
	lst, err := e.ListenTCP(addr, func(net.Addr) twine.Protocol {
		p := &LineReceiver{}
		p.OnConnectionMade = func(t twine.Transport) { serverConns <- t }
		p.LineReceived = func(l []byte) { p.SendLine(append([]byte("echo: "), l...)) }
		return p
	})
	if err != nil {
		t.Fatal(err)
	}
	expectState(t, states, true)
	if !rc.Connected() || built.Load() != 1 {
		t.Fatalf("connected %v built %d", rc.Connected(), built.Load())
	}
	expectLine(t, lines, "echo: ping 1")
	onLoop(t, e.Loops()[0], func() {
		if rc.Transport() == nil {
			t.Error("Transport is nil while connected")
		}
	})

	// The server drops the connection; the client reconnects.
	srv := <-serverConns
	srv.Loop().CallFromThread(srv.Abort)
	if ev := expectState(t, states, false); errors.Is(ev.err, ErrGaveUp) {
		t.Fatalf("loss reported as give-up: %v", ev.err)
	}
	expectState(t, states, true)
	if built.Load() != 2 {
		t.Fatalf("built %d protocols", built.Load())
	}
	expectLine(t, lines, "echo: ping 2")

	// StopTrying: the next loss is not retried.
	rc.StopTrying()
	onLoop(t, e.Loops()[0], func() {}) // FIFO barrier: StopTrying has run
	srv = <-serverConns
	srv.Loop().CallFromThread(srv.Abort)
	expectState(t, states, false)
	n := rc.Attempts()
	time.Sleep(200 * time.Millisecond)
	if rc.Attempts() != n || rc.Connected() || rc.GaveUp() {
		t.Fatalf("still trying after StopTrying: attempts %d -> %d, gave up %v", n, rc.Attempts(), rc.GaveUp())
	}

	// Start resumes.
	rc.Start()
	expectState(t, states, true)
	rc.StopTrying()
	lst.Close()
}

func expectState(t *testing.T, ch <-chan stateEvent, want bool) stateEvent {
	t.Helper()
	select {
	case ev := <-ch:
		if ev.up != want {
			t.Fatalf("state %v (%v), want %v", ev.up, ev.err, want)
		}
		return ev
	case <-time.After(10 * time.Second):
		t.Fatalf("no state change to %v", want)
	}
	return stateEvent{}
}

func expectLine(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("line %q, want %q", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no line %q", want)
	}
}

func TestReconnectingClientMaxRetries(t *testing.T) {
	e := startEngine(t, 1)
	states := make(chan stateEvent, 8)
	rc := NewReconnectingClient(e.Loops()[0], freeAddr(t), func(net.Addr) twine.Protocol { return nil },
		InitialDelay(time.Millisecond), MaxRetries(3),
		OnStateChange(func(up bool, err error) { states <- stateEvent{up, err} }))
	rc.Start()
	ev := expectState(t, states, false)
	if !errors.Is(ev.err, ErrGaveUp) || !errors.Is(ev.err, rc.LastError()) || rc.LastError() == nil {
		t.Fatalf("give-up error %v (last %v)", ev.err, rc.LastError())
	}
	if !rc.GaveUp() {
		t.Fatal("GaveUp false after the give-up callback")
	}
	if rc.Attempts() != 4 { // the first attempt plus three retries
		t.Fatalf("attempts %d", rc.Attempts())
	}
	select {
	case ev := <-states:
		t.Fatalf("extra state change %v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestReconnectingClientEngineStop covers a clean Engine.Stop, which loses
// connections with a nil error: the client must give up rather than
// schedule a retry on an exiting loop, both while connected and while
// waiting between retries.
func TestReconnectingClientEngineStop(t *testing.T) {
	for _, connected := range []bool{true, false} {
		name := "retrying"
		if connected {
			name = "connected"
		}
		t.Run(name, func(t *testing.T) {
			e := startEngine(t, 2)
			addr := freeAddr(t)
			if connected {
				lst, err := e.ListenTCP(addr, func(net.Addr) twine.Protocol { return &LineReceiver{} })
				if err != nil {
					t.Fatal(err)
				}
				defer lst.Close()
			}
			states := make(chan stateEvent, 64)
			rc := NewReconnectingClient(e.Loops()[1], addr, func(net.Addr) twine.Protocol { return &LineReceiver{} },
				InitialDelay(time.Hour), // a retry would never fire before the test ends
				OnStateChange(func(up bool, err error) { states <- stateEvent{up, err} }))
			rc.Start()
			if connected {
				expectState(t, states, true)
			} else {
				waitFor(t, "first failure", func() bool { return rc.LastError() != nil })
			}
			e.Stop()
			select {
			case <-e.Done():
			case <-time.After(10 * time.Second):
				t.Fatal("engine did not stop")
			}
			if !rc.GaveUp() || rc.Connected() {
				t.Fatalf("after Stop: gave up %v, connected %v", rc.GaveUp(), rc.Connected())
			}
			ev := expectState(t, states, false)
			if connected && !errors.Is(ev.err, ErrGaveUp) {
				// The server side of the same engine may close the connection
				// before the shutdown hook reaches this loop: then the loss and
				// the give-up arrive as two calls.
				ev = expectState(t, states, false)
			}
			if !errors.Is(ev.err, ErrGaveUp) {
				t.Fatalf("final state error %v, want ErrGaveUp", ev.err)
			}
			if !connected && !errors.Is(ev.err, twine.ErrEngineStopped) {
				t.Fatalf("final state error %v, want ErrEngineStopped", ev.err)
			}
			if len(states) != 0 {
				t.Fatalf("%d extra state changes", len(states))
			}
		})
	}
}

func TestReconnectingClientResetDelay(t *testing.T) {
	e := startEngine(t, 1)
	rc := NewReconnectingClient(e.Loops()[0], freeAddr(t), func(net.Addr) twine.Protocol { return nil },
		InitialDelay(time.Millisecond), MaxDelay(time.Millisecond))
	rc.Start()
	waitFor(t, "failed retries", func() bool { return rc.Attempts() >= 3 })
	rc.StopTrying()
	rc.ResetDelay()
	onLoop(t, e.Loops()[0], func() {
		if rc.failures != 0 {
			t.Errorf("failures %d after ResetDelay", rc.failures)
		}
	})
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
