package twine

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// startEngine runs an engine with the given options and stops it at test end.
func startEngine(t testing.TB, opts ...Option) *Engine {
	t.Helper()
	opts = append([]Option{Watchdog(0)}, opts...)
	e := New(opts...)
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	// Wait for loops to start.
	deadline := time.Now().Add(5 * time.Second)
	for _, l := range e.loops {
		for !l.started.Load() {
			if time.Now().After(deadline) {
				t.Fatal("loops did not start")
			}
			time.Sleep(time.Millisecond)
		}
	}
	t.Cleanup(func() {
		e.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("engine did not stop")
		}
	})
	return e
}

type echoProto struct{ t Transport }

func (p *echoProto) ConnectionMade(t Transport) { p.t = t }
func (p *echoProto) DataReceived(b []byte)      { p.t.Write(b) }
func (p *echoProto) ConnectionLost(error)       {}

func echoFactory() Factory {
	return FactoryFunc(func(net.Addr) Protocol { return &echoProto{} })
}

func TestEchoRoundTrip(t *testing.T) {
	for _, loops := range []int{1, 2, runtime.GOMAXPROCS(0)} {
		t.Run("loops="+itoa(loops), func(t *testing.T) {
			e := startEngine(t, Loops(loops))
			lst, err := e.ListenTCP("127.0.0.1:0", echoFactory())
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, err := net.Dial("tcp", lst.Addr().String())
					if err != nil {
						t.Error(err)
						return
					}
					defer c.Close()
					msg := make([]byte, 1000)
					rand.Read(msg)
					for j := 0; j < 100; j++ {
						if _, err := c.Write(msg); err != nil {
							t.Error(err)
							return
						}
						got := make([]byte, len(msg))
						if _, err := io.ReadFull(c, got); err != nil {
							t.Error(err)
							return
						}
						if !bytes.Equal(got, msg) {
							t.Error("echo mismatch")
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestRunTwice(t *testing.T) {
	e := New(Loops(1), Watchdog(0))
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	for !e.loops[0].started.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := e.Run(context.Background()); !errors.Is(err, ErrRunTwice) {
		t.Errorf("Run while running = %v, want ErrRunTwice", err)
	}
	e.Stop()
	if err := <-errCh; err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := e.Run(context.Background()); !errors.Is(err, ErrRunTwice) {
		t.Errorf("Run after Run = %v, want ErrRunTwice", err)
	}

	// Stop before Run: Run returns nil at once.
	e2 := New(Loops(1), Watchdog(0))
	e2.Stop()
	if err := e2.Run(context.Background()); err != nil {
		t.Errorf("Run after Stop = %v, want nil", err)
	}
}

// onLoop runs fn on l and waits for it.
func onLoop(t testing.TB, l *Loop, fn func()) {
	t.Helper()
	done := make(chan struct{})
	l.CallFromThread(func() { defer close(done); fn() })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("loop did not run task")
	}
}
