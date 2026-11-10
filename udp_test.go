package twine

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// udpEcho echoes each datagram to its sender. "burst" triggers 200 WriteTo
// calls in one callback.
type udpEcho struct {
	starts, stops atomic.Int32
	stopCh        chan struct{}
	stopOnce      sync.Once
	tr            DatagramTransport

	mu    sync.Mutex
	froms []string
	sink  errSink
}

func newUDPEcho() *udpEcho { return &udpEcho{stopCh: make(chan struct{})} }

func (u *udpEcho) StartProtocol(t DatagramTransport) {
	u.tr = t
	u.starts.Add(1)
	if !t.Loop().InLoop() {
		u.sink.Errorf("StartProtocol ran off the loop")
	}
}

func (u *udpEcho) DatagramReceived(b []byte, from net.Addr) {
	u.mu.Lock()
	u.froms = append(u.froms, from.String())
	u.mu.Unlock()
	if string(b) == "burst" {
		for i := 0; i < 200; i++ {
			u.tr.WriteTo([]byte(fmt.Sprintf("burst-%03d", i)), from)
		}
		return
	}
	if string(b) == "oversize" {
		// Larger than any UDP datagram: the send fails with EMSGSIZE.
		u.tr.WriteTo(make([]byte, 70000), from)
	}
	u.tr.WriteTo(b, from)
}

func (u *udpEcho) StopProtocol() {
	u.stops.Add(1)
	u.stopOnce.Do(func() { close(u.stopCh) })
}

func dialUDP(t *testing.T, addr net.Addr) *net.UDPConn {
	t.Helper()
	c, err := net.DialUDP("udp", nil, addr.(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestUDPEcho(t *testing.T) {
	e := startEngine(t, Loops(1))
	u := newUDPEcho()
	lst, err := e.ListenUDP("127.0.0.1:0", u)
	if err != nil {
		t.Fatal(err)
	}
	c := dialUDP(t, lst.Addr())
	buf := make([]byte, 2048)
	for i := 0; i < 1000; i++ {
		msg := []byte(fmt.Sprintf("datagram %d %s", i, bytes.Repeat([]byte{byte(i)}, i%300)))
		if _, err := c.Write(msg); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("datagram %d: %v", i, err)
		}
		if !bytes.Equal(buf[:n], msg) {
			t.Fatalf("datagram %d: echo mismatch (%d bytes, want %d)", i, n, len(msg))
		}
	}
	u.mu.Lock()
	froms := append([]string(nil), u.froms...)
	u.mu.Unlock()
	if len(froms) != 1000 {
		t.Errorf("server saw %d datagrams, want 1000", len(froms))
	}
	want := c.LocalAddr().String()
	for i, f := range froms {
		if f != want {
			t.Errorf("datagram %d from %s, want %s", i, f, want)
			break
		}
	}
	if s := u.starts.Load(); s != 1 {
		t.Errorf("StartProtocol called %d times", s)
	}
	u.sink.report(t)
}

// A datagram larger than the 8 KiB send batch buffer bypasses the batch
// (direct sendto) and arrives whole through the 64 KiB receive buffer.
func TestUDPLargeDatagram(t *testing.T) {
	// The largest datagram a UDP socket may send is capped by its SO_SNDBUF
	// on macOS and the BSDs, whose default (net.inet.udp.maxdgram, 9216 on
	// macOS) is barely above udpSendBuf; sending more fails with EMSGSIZE.
	// Raise SO_SNDBUF on both ends so a near-64 KiB datagram is legal
	// everywhere. Linux does not tie the datagram size to SO_SNDBUF.
	const sndBuf = 256 << 10
	e := startEngine(t, Loops(1))
	u := newUDPEcho()
	lst, err := e.ListenUDP("127.0.0.1:0", u, SocketBuffers(0, sndBuf))
	if err != nil {
		t.Fatal(err)
	}
	c := dialUDP(t, lst.Addr())
	if err := c.SetWriteBuffer(sndBuf); err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 60000)
	fillPattern(msg, 0)
	if _, err := c.Write(msg); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 70000)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("%v (server: %d datagrams received, %d sent)", err,
			e.Loops()[0].Metrics().Datagrams, e.Loops()[0].Metrics().DatagramsSent)
	}
	if !bytes.Equal(buf[:n], msg) {
		t.Errorf("60 KB datagram echo: got %d bytes, mismatch", n)
	}
}

// A datagram the kernel refuses (EMSGSIZE) is dropped on its own: it is not
// counted as sent and the socket keeps working.
func TestUDPOversizeDropped(t *testing.T) {
	e := startEngine(t, Loops(1))
	u := newUDPEcho()
	lst, err := e.ListenUDP("127.0.0.1:0", u)
	if err != nil {
		t.Fatal(err)
	}
	c := dialUDP(t, lst.Addr())
	for _, msg := range []string{"oversize", "after"} {
		if _, err := c.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 128)
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("echo of %q: %v", msg, err)
		}
		if string(buf[:n]) != msg {
			t.Fatalf("echo of %q: got %q", msg, buf[:n])
		}
	}
	// The batch flush counts after sendmmsg returns, so the client can see
	// the last echo before the counter moves. The oversize datagram is
	// (wrongly) counted, if at all, before the first echo is even queued.
	sent := func() uint64 { return e.Loops()[0].Metrics().DatagramsSent }
	waitUntil(5*time.Second, func() bool { return sent() >= 2 })
	if n := sent(); n != 2 {
		t.Errorf("DatagramsSent = %d, want 2 (the oversize datagram was not sent)", n)
	}
	u.sink.report(t)
}

func TestUDPBatchWriteTo(t *testing.T) {
	e := startEngine(t, Loops(1))
	u := newUDPEcho()
	lst, err := e.ListenUDP("127.0.0.1:0", u)
	if err != nil {
		t.Fatal(err)
	}
	c := dialUDP(t, lst.Addr())
	_ = c.SetReadBuffer(4 << 20)
	if _, err := c.Write([]byte("burst")); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	buf := make([]byte, 256)
	for len(seen) < 200 {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("after %d of 200 datagrams: %v", len(seen), err)
		}
		s := string(buf[:n])
		if seen[s] {
			t.Errorf("duplicate datagram %q", s)
		}
		seen[s] = true
	}
	for i := 0; i < 200; i++ {
		if k := fmt.Sprintf("burst-%03d", i); !seen[k] {
			t.Errorf("missing %s", k)
		}
	}
	// The counter is added to after sendmmsg returns, so the client can read
	// the last datagrams before the loop has counted them.
	if !waitUntil(2*time.Second, func() bool { return e.Loops()[0].Metrics().DatagramsSent >= 200 }) {
		t.Errorf("DatagramsSent = %d, want >= 200", e.Loops()[0].Metrics().DatagramsSent)
	}
}

func TestUDPStopProtocol(t *testing.T) {
	t.Run("Close", func(t *testing.T) {
		e := startEngine(t, Loops(1))
		u := newUDPEcho()
		lst, err := e.ListenUDP("127.0.0.1:0", u)
		if err != nil {
			t.Fatal(err)
		}
		if !waitUntil(5*time.Second, func() bool { return u.starts.Load() == 1 }) {
			t.Fatal("StartProtocol not called")
		}
		if err := lst.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-u.stopCh:
		case <-time.After(5 * time.Second):
			t.Fatal("StopProtocol not called after Close")
		}
		_ = lst.Close() // idempotent
		time.Sleep(20 * time.Millisecond)
		if s := u.stops.Load(); s != 1 {
			t.Errorf("StopProtocol called %d times", s)
		}
	})
	t.Run("Engine.Stop", func(t *testing.T) {
		e := New(Loops(1), Watchdog(0))
		u := newUDPEcho()
		if _, err := e.ListenUDP("127.0.0.1:0", u); err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() { errCh <- e.Run(context.Background()) }()
		if !waitUntil(5*time.Second, func() bool { return u.starts.Load() == 1 }) {
			t.Fatal("StartProtocol not called")
		}
		e.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
		if s := u.stops.Load(); s != 1 {
			t.Errorf("StopProtocol called %d times on Engine.Stop, want 1", s)
		}
	})
}

// fromKeeper stores the source address pointer it is given (the bug the
// call-scoped rule is about) next to a value copy.
type fromKeeper struct {
	tr     DatagramTransport
	mu     sync.Mutex
	kept   []net.Addr
	copies []string
	got    chan struct{}
}

func (k *fromKeeper) StartProtocol(t DatagramTransport) { k.tr = t }
func (k *fromKeeper) DatagramReceived(b []byte, from net.Addr) {
	k.mu.Lock()
	k.kept = append(k.kept, from)
	k.copies = append(k.copies, from.(*net.UDPAddr).AddrPort().String())
	k.mu.Unlock()
	k.got <- struct{}{}
	if string(b) == "shut" {
		k.tr.Close() // StopProtocol closes k.got
	}
}
func (k *fromKeeper) StopProtocol() { close(k.got) }

// The source address passed to DatagramReceived is call-scoped: debug builds
// empty it after the call, so a retained pointer is visibly wrong, while a
// copy taken during the call stays right.
func TestUDPFromScrubbedInDebug(t *testing.T) {
	enableDebug(t)
	e := startEngine(t, Loops(1))
	k := &fromKeeper{got: make(chan struct{}, 4)}
	lst, err := e.ListenUDP("127.0.0.1:0", k)
	if err != nil {
		t.Fatal(err)
	}
	c1 := dialUDP(t, lst.Addr())
	c2 := dialUDP(t, lst.Addr())
	for _, c := range []*net.UDPConn{c1, c2} {
		if _, err := c.Write([]byte("hi")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-k.got:
		case <-time.After(5 * time.Second):
			t.Fatal("datagram not received")
		}
	}
	onLoop(t, e.loops[0], func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		for i, a := range k.kept {
			if s := a.String(); s != ":0" && s != "<nil>" {
				t.Errorf("retained from[%d] = %s after the call, want it scrubbed", i, s)
			}
		}
		want := []string{c1.LocalAddr().String(), c2.LocalAddr().String()}
		for i, s := range k.copies {
			if s != want[i] {
				t.Errorf("copy %d = %s, want %s", i, s, want[i])
			}
		}
	})
}

// DatagramTransport.Close closes the socket from the protocol and calls
// StopProtocol once.
func TestDatagramTransportClose(t *testing.T) {
	e := startEngine(t, Loops(1))
	k := &fromKeeper{got: make(chan struct{}, 4)}
	lst, err := e.ListenUDP("127.0.0.1:0", k)
	if err != nil {
		t.Fatal(err)
	}
	c := dialUDP(t, lst.Addr())
	if _, err := c.Write([]byte("shut")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for open := true; open; {
		select {
		case _, open = <-k.got:
		case <-deadline:
			t.Fatal("StopProtocol not called after Close")
		}
	}
	onLoop(t, e.loops[0], func() {
		if n := e.loops[0].nconns; n != 0 {
			t.Errorf("%d fds still registered after Close", n)
		}
	})
	if err := lst.Close(); err != nil {
		t.Errorf("Listener.Close after DatagramTransport.Close: %v", err)
	}
}

// ListenUDP rejects the TCP-only ListenOptions by name instead of ignoring
// them, and still accepts SocketBuffers. The engine need not be running.
func TestListenUDPRejectsTCPOnlyOptions(t *testing.T) {
	e := New(Loops(1))
	t.Cleanup(e.Stop)
	for _, tc := range []struct {
		name string
		opt  ListenOption
	}{
		{"Backlog", Backlog(128)},
		{"DeferAccept", DeferAccept(time.Second)},
		{"BusyPoll", BusyPoll(50 * time.Microsecond)},
		{"TCPNoDelay", TCPNoDelay(false)},
	} {
		lst, err := e.ListenUDP("127.0.0.1:0", newUDPEcho(), SocketBuffers(1<<16, 1<<16), tc.opt)
		if err == nil {
			lst.Close()
			t.Errorf("ListenUDP with %s succeeded; want an error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.name) {
			t.Errorf("ListenUDP with %s: error %q does not name the option", tc.name, err)
		}
	}
	lst, err := e.ListenUDP("127.0.0.1:0", newUDPEcho(), SocketBuffers(1<<16, 1<<16))
	if err != nil {
		t.Fatalf("ListenUDP with SocketBuffers only: %v", err)
	}
	lst.Close()
}
