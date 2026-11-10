package twine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// clientProto records what an outbound connection sees.
type clientProto struct {
	made    atomic.Bool
	madeTr  chan Transport
	mu      sync.Mutex
	data    []byte
	lost    chan error
	onMade  func(Transport)
	onBytes func(Transport, []byte)
	t       Transport
}

func newClientProto() *clientProto {
	return &clientProto{madeTr: make(chan Transport, 1), lost: make(chan error, 1)}
}

func (p *clientProto) ConnectionMade(t Transport) {
	p.t = t
	p.made.Store(true)
	p.madeTr <- t
	if p.onMade != nil {
		p.onMade(t)
	}
}

func (p *clientProto) DataReceived(b []byte) {
	p.mu.Lock()
	p.data = append(p.data, b...)
	p.mu.Unlock()
	if p.onBytes != nil {
		p.onBytes(p.t, b)
	}
}

func (p *clientProto) ConnectionLost(err error) { p.lost <- err }

func (p *clientProto) received() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return string(p.data)
}

func TestConnectTCPToTwineServer(t *testing.T) {
	e := startEngine(t, Loops(2))
	lst := listen(t, e, echoFactory())
	l := e.Loops()[1]
	cp := newClientProto()
	f := l.ConnectTCP(lst.Addr().String(), func(net.Addr) Protocol { return cp })
	type thenRes struct {
		madeFirst, onLoop bool
		tr                Transport
	}
	thenCh := make(chan thenRes, 1)
	f.Then(func(tr Transport) { thenCh <- thenRes{cp.made.Load(), l.InLoop(), tr} })
	tr, err := awaitT(t, f)
	if err != nil {
		t.Fatalf("ConnectTCP: %v", err)
	}
	var r thenRes
	select {
	case r = <-thenCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Then never ran")
	}
	if !r.madeFirst {
		t.Error("Future resolved before ConnectionMade")
	}
	if !r.onLoop {
		t.Error("Then ran off the loop")
	}
	madeTr := <-cp.madeTr
	if tr != madeTr || r.tr != madeTr {
		t.Error("Transport in the Future differs from the one passed to ConnectionMade")
	}
	if tr.Loop() != l {
		t.Errorf("connected transport on loop %d, want %d", tr.Loop().ID(), l.ID())
	}
	var peer, host string
	onLoop(t, l, func() {
		peer, host = tr.RemoteAddr().String(), tr.LocalAddr().String()
		tr.Write([]byte("ping"))
	})
	if peer != lst.Addr().String() {
		t.Errorf("RemoteAddr = %s, want %s", peer, lst.Addr())
	}
	if host == "" || host == peer {
		t.Errorf("LocalAddr = %q", host)
	}
	if !waitUntil(5*time.Second, func() bool { return cp.received() == "ping" }) {
		t.Errorf("client received %q, want echo \"ping\"", cp.received())
	}
	onLoop(t, l, tr.Close)
	if err, ok := recvErr(t, cp.lost, 5*time.Second, "client ConnectionLost"); ok && err != nil {
		t.Errorf("client ConnectionLost(%v), want nil", err)
	}
}

func TestConnectTCPToNetListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	srvErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			srvErr <- err
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 5)
		if _, err := io.ReadFull(c, buf); err != nil {
			srvErr <- err
			return
		}
		if string(buf) != "hello" {
			srvErr <- errors.New("server got " + string(buf))
			return
		}
		_, err = c.Write([]byte("world"))
		srvErr <- err
	}()

	e := startEngine(t, Loops(1))
	cp := newClientProto()
	cp.onMade = func(tr Transport) { tr.Write([]byte("hello")) }
	f := e.Loops()[0].ConnectTCP(ln.Addr().String(), func(net.Addr) Protocol { return cp })
	if _, err := awaitT(t, f); err != nil {
		t.Fatalf("ConnectTCP: %v", err)
	}
	if err, _ := recvErr(t, srvErr, 5*time.Second, "net.Listener side"); err != nil {
		t.Fatalf("server: %v", err)
	}
	// The server closes after writing: the client gets the data, then a
	// clean ConnectionLost.
	if err, ok := recvErr(t, cp.lost, 5*time.Second, "client ConnectionLost"); ok && err != nil {
		t.Errorf("client ConnectionLost(%v), want nil", err)
	}
	if got := cp.received(); got != "world" {
		t.Errorf("client received %q, want \"world\"", got)
	}
}

func TestConnectRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	built := atomic.Bool{}
	f := l.ConnectTCP(addr, func(net.Addr) Protocol { built.Store(true); return newClientProto() })
	caught := make(chan bool, 1)
	f.Catch(func(error) { caught <- l.InLoop() })
	_, err = awaitT(t, f)
	if err == nil {
		t.Fatal("ConnectTCP to a closed port resolved")
	}
	if !errors.Is(err, unix.ECONNREFUSED) {
		t.Errorf("err = %v (%T), want one wrapping ECONNREFUSED", err, err)
	}
	select {
	case on := <-caught:
		if !on {
			t.Error("Catch ran off the loop")
		}
	case <-time.After(5 * time.Second):
		t.Error("Catch never ran")
	}
	if built.Load() {
		t.Error("Factory called for a refused connection")
	}
	if c := l.Metrics().Connections; c != 0 {
		t.Errorf("Connections gauge = %d after a refused connect", c)
	}
}

// blackholeAddr returns an address whose SYNs are never answered: a
// non-routable address if the environment routes it, else a loopback
// listener whose accept queue is full.
func blackholeAddr(t *testing.T) string {
	const nonRoutable = "10.255.255.1:9"
	d := net.Dialer{Timeout: 100 * time.Millisecond}
	c, err := d.Dial("tcp", nonRoutable)
	if c != nil {
		c.Close()
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return nonRoutable
	}
	// Fall back to a listener with backlog 0 whose queue we fill.
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Skipf("no blackhole available: %v", err)
	}
	unix.CloseOnExec(fd)
	t.Cleanup(func() { unix.Close(fd) })
	if err := unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Skipf("no blackhole available: %v", err)
	}
	if err := unix.Listen(fd, 0); err != nil {
		t.Skipf("no blackhole available: %v", err)
	}
	sa, _ := unix.Getsockname(fd)
	port := sa.(*unix.SockaddrInet4).Port
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	// Fill the accept queue (backlog 0 admits one; add a spare).
	for i := 0; i < 3; i++ {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			break
		}
		t.Cleanup(func() { c.Close() })
	}
	c, err = net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if c != nil {
		c.Close()
	}
	if errors.As(err, &ne) && ne.Timeout() {
		return addr
	}
	t.Skipf("no blackhole available (non-routable: immediate result; full backlog: %v)", err)
	return ""
}

func TestConnectTimeout(t *testing.T) {
	addr := blackholeAddr(t)
	e := startEngine(t, Loops(1))
	const timeout = 200 * time.Millisecond
	start := time.Now()
	f := e.Loops()[0].ConnectTCP(addr, func(net.Addr) Protocol { return newClientProto() }, DialTimeout(timeout))
	_, err := awaitT(t, f)
	el := time.Since(start)
	if !errors.Is(err, ErrConnectTimeout) {
		t.Fatalf("ConnectTCP(%s) err = %v after %v, want ErrConnectTimeout", addr, err, el)
	}
	if el < timeout-10*time.Millisecond || el > timeout+time.Second {
		t.Errorf("timed out after %v, DialTimeout %v", el, timeout)
	}
	if c := e.Loops()[0].Metrics().Connections; c != 0 {
		t.Errorf("Connections gauge = %d after a timed-out connect", c)
	}
}

func TestConnectAfterStop(t *testing.T) {
	e := New(Loops(1), Watchdog(-1))
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	if !waitUntil(5*time.Second, func() bool { return e.Loops()[0].started.Load() }) {
		t.Fatal("loop did not start")
	}
	e.Stop()
	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	f := e.Loops()[0].ConnectTCP("127.0.0.1:1", func(net.Addr) Protocol { return newClientProto() })
	if _, err := awaitT(t, f); !errors.Is(err, ErrEngineStopped) {
		t.Errorf("ConnectTCP after Stop: %v, want ErrEngineStopped", err)
	}
	if _, err := awaitT(t, e.Dial("127.0.0.1:1", func(net.Addr) Protocol { return nil })); !errors.Is(err, ErrEngineStopped) {
		t.Errorf("Dial after Stop: %v, want ErrEngineStopped", err)
	}
	if _, err := e.ListenTCP("127.0.0.1:0", echoFactory()); !errors.Is(err, ErrEngineStopped) {
		t.Errorf("ListenTCP after Stop: %v, want ErrEngineStopped", err)
	}
}

// Engine.Dial spreads outbound connections round-robin over the loops. The
// ReusePort(false) case (the default off Linux) also checks that a single
// listener's accept hand-off neither shares nor skews that sequence: the
// accepted side of these same connections must be spread evenly too.
func TestEngineDialRoundRobin(t *testing.T) {
	for _, reuse := range []bool{true, false} {
		t.Run(fmt.Sprintf("ReusePort=%v", reuse), func(t *testing.T) {
			testEngineDialRoundRobin(t, reuse)
		})
	}
}

func testEngineDialRoundRobin(t *testing.T, reusePort bool) {
	const loops = 4
	e := startEngine(t, Loops(loops), ReusePort(reusePort))
	var mu sync.Mutex
	accepted := map[int]int{}
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) {
			mu.Lock()
			accepted[tr.Loop().ID()]++
			mu.Unlock()
		}}
	}))
	var ids []int
	var trs []Transport
	for i := 0; i < 2*loops; i++ {
		f := e.Dial(lst.Addr().String(), func(net.Addr) Protocol { return newClientProto() })
		tr, err := awaitT(t, f)
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		if tr.Loop() != f.Loop() {
			t.Errorf("Dial %d: transport on loop %d, Future on loop %d", i, tr.Loop().ID(), f.Loop().ID())
		}
		ids = append(ids, tr.Loop().ID())
		trs = append(trs, tr)
	}
	t.Logf("Dial loop ids: %v", ids)
	count := map[int]int{}
	for i, id := range ids {
		count[id]++
		if i > 0 && id != (ids[i-1]+1)%loops {
			t.Errorf("Dial %d went to loop %d after loop %d; want round-robin", i, id, ids[i-1])
		}
	}
	for id := 0; id < loops; id++ {
		if count[id] != 2 {
			t.Errorf("loop %d got %d of %d dials", id, count[id], 2*loops)
		}
	}
	total := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, c := range accepted {
			n += c
		}
		return n
	}
	if !waitUntil(5*time.Second, func() bool { return total() == 2*loops }) {
		t.Fatalf("server accepted %d of %d connections", total(), 2*loops)
	}
	mu.Lock()
	t.Logf("accepted per loop: %v", accepted)
	if !reusePort {
		// The kernel picks the loop under SO_REUSEPORT; the single-listener
		// hand-off is round-robin and must be exact.
		for id := 0; id < loops; id++ {
			if accepted[id] != 2 {
				t.Errorf("loop %d accepted %d of %d connections", id, accepted[id], 2*loops)
			}
		}
	}
	mu.Unlock()
	for _, tr := range trs {
		onLoop(t, tr.Loop(), tr.Abort)
	}
}

// A Factory returning nil refuses the connection: the accepted socket is
// closed at once (the client sees EOF), and a Dial Future rejects with
// ErrNilProtocol. Blocking passes the refusal through.
func TestFactoryReturnsNil(t *testing.T) {
	e := startEngine(t, Loops(1))
	nilFactory := func(net.Addr) Protocol { return nil }
	for name, f := range map[string]Factory{"plain": nilFactory, "Blocking": Blocking(nilFactory)} {
		t.Run(name, func(t *testing.T) {
			lst := listen(t, e, f)
			c := dialTCP(t, lst.Addr().String())
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			start := time.Now()
			n, err := c.Read(make([]byte, 1))
			if n != 0 || (err != io.EOF && !errors.Is(err, unix.ECONNRESET)) {
				t.Fatalf("read on a refused connection = %d, %v; want EOF", n, err)
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("refused connection closed after %v", d)
			}

			echo := listen(t, e, echoFactory())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = e.Dial(echo.Addr().String(), f).Await(ctx)
			if !errors.Is(err, ErrNilProtocol) {
				t.Fatalf("Dial with a nil-returning factory = %v, want ErrNilProtocol", err)
			}
		})
	}
	if !waitUntil(2*time.Second, func() bool { return e.Loops()[0].Metrics().Connections == 0 }) {
		t.Errorf("Connections = %d, want 0 (refused connections leaked)", e.Loops()[0].Metrics().Connections)
	}
}

func TestLiteralTCPAddr(t *testing.T) {
	for addr, want := range map[string]string{
		"127.0.0.1:80":       "127.0.0.1:80",
		"[::1]:443":          "[::1]:443",
		"[fe80::1%lo]:9":     "[fe80::1%lo]:9",
		":9000":              ":9000",
		"[::ffff:1.2.3.4]:1": "1.2.3.4:1", // as net.ResolveTCPAddr prints it
		"localhost:80":       "",
		"example.com:443":    "",
		"127.0.0.1:http":     "",
		"127.0.0.1":          "",
		"":                   "",
	} {
		a, ok := literalTCPAddr(addr)
		got := ""
		if ok {
			got = a.String()
		}
		if got != want {
			t.Errorf("literalTCPAddr(%q) = %q, %v; want %q", addr, got, ok, want)
		}
	}
}

// A host name is resolved on the worker pool, never on the loop: while a
// lookup stalls, another connection on the same loop keeps echoing.
func TestDialHostnameResolvesOffLoop(t *testing.T) {
	e := startEngine(t, Loops(1))
	lst := listen(t, e, echoFactory())
	_, port, _ := net.SplitHostPort(lst.Addr().String())

	release := make(chan struct{})
	var lookups atomic.Int32
	orig := resolveTCPAddr
	resolveTCPAddr = func(network, addr string) (*net.TCPAddr, error) {
		lookups.Add(1)
		host, p, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		switch host {
		case "slow.test":
			<-release
			return net.ResolveTCPAddr(network, net.JoinHostPort("127.0.0.1", p))
		case "local.test":
			return net.ResolveTCPAddr(network, net.JoinHostPort("127.0.0.1", p))
		}
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	t.Cleanup(func() { resolveTCPAddr = orig })

	c := dialTCP(t, lst.Addr().String())
	echoOnce(t, c, []byte("warm"))

	p := newClientProto()
	fut := e.Loops()[0].ConnectTCP(net.JoinHostPort("slow.test", port), func(net.Addr) Protocol { return p },
		LocalAddr("local.test:0"))
	if !waitUntil(2*time.Second, func() bool { return lookups.Load() >= 1 }) {
		t.Fatal("resolver never called")
	}
	// The lookup is stalled; the loop must still serve the other connection.
	for i := 0; i < 5; i++ {
		start := time.Now()
		echoOnce(t, c, []byte("during lookup"))
		if d := time.Since(start); d > 50*time.Millisecond {
			t.Errorf("echo took %v while a DNS lookup was stalled", d)
		}
	}
	if fut.Done() {
		t.Fatal("Dial settled before its lookup finished")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr, err := fut.Await(ctx)
	if err != nil {
		t.Fatalf("Dial via the resolver: %v", err)
	}
	if lookups.Load() != 2 {
		t.Errorf("%d lookups, want 2 (remote and LocalAddr)", lookups.Load())
	}
	onLoop(t, tr.Loop(), func() {
		if ta, ok := tr.RemoteAddr().(*net.TCPAddr); !ok || ta.String() != lst.Addr().String() {
			t.Errorf("RemoteAddr = %v, want %v", tr.RemoteAddr(), lst.Addr())
		}
		tr.Abort()
	})

	// A failed lookup rejects the Future with the resolver's error.
	_, err = e.Dial("nowhere.test:1", echoFactory()).Await(ctx)
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) {
		t.Errorf("Dial to an unknown host = %v, want a *net.DNSError", err)
	}
}
