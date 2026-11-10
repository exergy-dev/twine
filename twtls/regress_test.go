package twtls_test

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// recProto records DataReceived calls and optionally aborts on the first.
type recProto struct {
	t          twine.Transport
	lost       chan error
	abortFirst bool
	aborted    bool
	calls      atomic.Int32
	afterAbort atomic.Int32
}

func (p *recProto) ConnectionMade(t twine.Transport) { p.t = t }
func (p *recProto) DataReceived([]byte) {
	p.calls.Add(1)
	if p.aborted {
		p.afterAbort.Add(1)
	}
	if p.abortFirst && !p.aborted {
		p.aborted = true
		p.t.Abort()
	}
}
func (p *recProto) ConnectionLost(err error) { p.lost <- err }

// TestTruncationIsNotCleanClose: a TCP FIN without close_notify must reach
// the inner protocol as io.ErrUnexpectedEOF, never as a clean (nil) close.
func TestTruncationIsNotCleanClose(t *testing.T) {
	e := startEngine(t)
	p := &recProto{lost: make(chan error, 1)}
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol { return p })
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := tls.Client(raw, &tls.Config{InsecureSkipVerify: true})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	raw.Close() // FIN without close_notify
	select {
	case err := <-p.lost:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("ConnectionLost(%v), want io.ErrUnexpectedEOF for a truncated stream", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConnectionLost not delivered")
	}
}

// holdConn buffers writes while hold is set so several TLS records can be
// released to the peer in one TCP segment.
type holdConn struct {
	net.Conn
	mu   sync.Mutex
	hold bool
	buf  []byte
}

func (h *holdConn) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.hold {
		h.buf = append(h.buf, p...)
		return len(p), nil
	}
	return h.Conn.Write(p)
}

// TestNoDataAfterAbort: once the inner protocol calls Abort, no further
// plaintext from the same ciphertext chunk may be delivered.
func TestNoDataAfterAbort(t *testing.T) {
	e := startEngine(t)
	p := &recProto{lost: make(chan error, 1), abortFirst: true}
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol { return p })
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	hc := &holdConn{Conn: raw}
	c := tls.Client(hc, &tls.Config{InsecureSkipVerify: true})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	hc.mu.Lock()
	hc.hold = true
	hc.mu.Unlock()
	for _, s := range []string{"a", "b", "c"} {
		if _, err := c.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	hc.mu.Lock()
	hc.hold = false
	_, err = raw.Write(hc.buf)
	hc.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.lost:
		if n := p.afterAbort.Load(); n > 0 {
			t.Errorf("DataReceived called %d times after Abort", n)
		}
		if p.calls.Load() == 0 {
			t.Error("no DataReceived at all")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConnectionLost not delivered")
	}
}
