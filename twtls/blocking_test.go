package twtls_test

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/twtls"
)

// blockingEcho runs on its own goroutine under twine.Blocking: it may block,
// and it calls the proxy Transport from that goroutine.
type blockingEcho struct {
	t    twine.Transport
	lost chan error
}

func (p *blockingEcho) ConnectionMade(t twine.Transport) { p.t = t }
func (p *blockingEcho) DataReceived(b []byte) {
	time.Sleep(time.Millisecond) // blocking is the point of Blocking
	p.t.Write(b)
}
func (p *blockingEcho) ConnectionLost(err error) { p.lost <- err }

// twtls.Server(cfg, twine.Blocking(f)) used to panic on the *twtls.Transport
// assertion inside Blocking, dropping every connection silently.
func TestBlockingUnderTLS(t *testing.T) {
	e := startEngine(t)
	lost := make(chan error, 1)
	f := twtls.Server(serverConfig(t), twine.Blocking(func(net.Addr) twine.Protocol {
		return &blockingEcho{lost: lost}
	}))
	lst, err := e.ListenTCP("127.0.0.1:0", f)
	if err != nil {
		t.Fatal(err)
	}
	c := dial(t, lst.Addr().String(), nil)
	for _, msg := range []string{"hello", "over", "tls"} {
		if _, err := c.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(c, got); err != nil {
			t.Fatalf("read echo of %q: %v", msg, err)
		}
		if string(got) != msg {
			t.Fatalf("echo %q, want %q", got, msg)
		}
	}
	c.Close()
	select {
	case <-lost:
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost after the client closed")
	}
	var panics uint64
	for _, m := range e.Metrics() {
		panics += m.Panics
	}
	if panics != 0 {
		t.Errorf("Panics = %d", panics)
	}
}

// madePanicker panics in ConnectionMade, which the tls Transport calls from
// its handshake callback rather than from a core-guarded protocol callback.
type madePanicker struct{ lost chan error }

func (p *madePanicker) ConnectionMade(twine.Transport) { panic("made boom") }
func (p *madePanicker) DataReceived([]byte)            {}
func (p *madePanicker) ConnectionLost(err error)       { p.lost <- err }

// A panic recovered by the tls Transport is logged and counted like one the
// core recovers.
func TestInnerPanicCounted(t *testing.T) {
	e := startEngine(t, twine.Logger(nil))
	lost := make(chan error, 1)
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol {
		return &madePanicker{lost: lost}
	})
	c := dial(t, addr, nil)
	defer c.Close()
	select {
	case err := <-lost:
		var pe *twine.PanicError
		if !errors.As(err, &pe) {
			t.Fatalf("ConnectionLost(%v), want PanicError", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost")
	}
	var panics uint64
	for _, m := range e.Metrics() {
		panics += m.Panics
	}
	if panics == 0 {
		t.Error("Panics metric is 0 after a panic in ConnectionMade under TLS")
	}
}
