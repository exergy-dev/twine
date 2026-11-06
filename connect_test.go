package twine

import (
	"context"
	"errors"
	"io"
	"net"
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
	f := l.ConnectTCP(lst.Addr().String(), FactoryFunc(func(net.Addr) Protocol { return cp }))
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
		peer, host = tr.GetPeer().String(), tr.GetHost().String()
		tr.Write([]byte("ping"))
	})
	if peer != lst.Addr().String() {
		t.Errorf("GetPeer = %s, want %s", peer, lst.Addr())
	}
	if host == "" || host == peer {
		t.Errorf("GetHost = %q", host)
	}
	if !waitUntil(5*time.Second, func() bool { return cp.received() == "ping" }) {
		t.Errorf("client received %q, want echo \"ping\"", cp.received())
	}
	onLoop(t, l, tr.LoseConnection)
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
	f := e.Loops()[0].ConnectTCP(ln.Addr().String(), FactoryFunc(func(net.Addr) Protocol { return cp }))
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
	f := l.ConnectTCP(addr, FactoryFunc(func(net.Addr) Protocol { built.Store(true); return newClientProto() }))
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
		t.Error("BuildProtocol called for a refused connection")
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
	addr := net.JoinHostPort("127.0.0.1", itoa(port))
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
	f := e.Loops()[0].ConnectTCP(addr, FactoryFunc(func(net.Addr) Protocol { return newClientProto() }), DialTimeout(timeout))
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
	e := New(Loops(1), Watchdog(0))
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
	f := e.Loops()[0].ConnectTCP("127.0.0.1:1", FactoryFunc(func(net.Addr) Protocol { return newClientProto() }))
	if _, err := awaitT(t, f); !errors.Is(err, ErrEngineStopped) {
		t.Errorf("ConnectTCP after Stop: %v, want ErrEngineStopped", err)
	}
	if _, err := awaitT(t, e.Dial("127.0.0.1:1", FactoryFunc(func(net.Addr) Protocol { return nil }))); !errors.Is(err, ErrEngineStopped) {
		t.Errorf("Dial after Stop: %v, want ErrEngineStopped", err)
	}
	if _, err := e.ListenTCP("127.0.0.1:0", echoFactory()); !errors.Is(err, ErrEngineStopped) {
		t.Errorf("ListenTCP after Stop: %v, want ErrEngineStopped", err)
	}
}

func TestEngineDialRoundRobin(t *testing.T) {
	const loops = 4
	e := startEngine(t, Loops(loops))
	lst := listen(t, e, echoFactory())
	var ids []int
	var trs []Transport
	for i := 0; i < 2*loops; i++ {
		f := e.Dial(lst.Addr().String(), FactoryFunc(func(net.Addr) Protocol { return newClientProto() }))
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
	for _, tr := range trs {
		tr := tr
		onLoop(t, tr.Loop(), tr.Abort)
	}
}
