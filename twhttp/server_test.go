package twhttp

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// countingFactory counts connections built by the server.
type countingFactory struct {
	f     twine.Factory
	conns atomic.Int64
	lost  atomic.Int64
}

func (cf *countingFactory) build(peer net.Addr) twine.Protocol {
	cf.conns.Add(1)
	return &lostCounter{Protocol: cf.f(peer), cf: cf}
}

// lostCounter counts ConnectionLost calls.
type lostCounter struct {
	twine.Protocol
	cf *countingFactory
}

func (l *lostCounter) ConnectionLost(err error) {
	l.Protocol.ConnectionLost(err)
	l.cf.lost.Add(1)
}

// startServer runs s on a fresh two-loop engine and returns its address
// and connection counter.
func startServer(t *testing.T, s *Server) (string, *countingFactory) {
	t.Helper()
	e := startEngine(t, twine.Loops(2), twine.Watchdog(-1))
	cf := &countingFactory{f: s.Factory()}
	lst, err := e.ListenTCP("127.0.0.1:0", cf.build)
	if err != nil {
		t.Fatal(err)
	}
	return lst.Addr().String(), cf
}

func newClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 1, DisableCompression: true},
	}
}

func testHandler(ctx *RequestCtx) {
	switch string(ctx.Path()) {
	case "/hello":
		ctx.SetContentType("text/plain")
		ctx.SetHeader("X-Method", string(ctx.Method())) // test only: allocates
		ctx.WriteString("hello world")
	case "/echo":
		ctx.SetHeaderBytes([]byte("X-Query"), ctx.RawQuery())
		ctx.SetHeaderBytes([]byte("X-Hdr"), ctx.Header("x-test-header"))
		ctx.Write(ctx.Body())
	case "/close":
		ctx.SetConnectionClose()
		ctx.WriteString("bye")
	case "/panic":
		panic("handler panic")
	case "/big":
		ctx.Write(bytes.Repeat([]byte("0123456789abcdef"), 1<<14)) // 256 KiB
	default:
		ctx.SetStatus(404)
		ctx.WriteString("not found")
	}
}

func TestServerGetPostKeepAlive(t *testing.T) {
	s := &Server{Name: "twine-test", Handler: testHandler}
	addr, cf := startServer(t, s)
	c := newClient()
	defer c.CloseIdleConnections()

	resp, err := c.Get("http://" + addr + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "hello world" ||
		resp.Header.Get("Content-Type") != "text/plain" || resp.Header.Get("X-Method") != "GET" ||
		resp.Header.Get("Server") != "twine-test" {
		t.Fatalf("GET: %d %q %v", resp.StatusCode, body, resp.Header)
	}
	d, err := http.ParseTime(resp.Header.Get("Date"))
	if err != nil || !strings.HasSuffix(resp.Header.Get("Date"), " GMT") {
		t.Fatalf("Date %q: %v", resp.Header.Get("Date"), err)
	}
	if _, err := time.Parse(time.RFC1123, resp.Header.Get("Date")); err != nil {
		t.Fatalf("Date not RFC1123: %v", err)
	}
	if delta := time.Since(d); delta < -2*time.Second || delta > 2*time.Second {
		t.Fatalf("Date off by %v", delta)
	}

	payload := strings.Repeat("payload-", 1000)
	req, _ := http.NewRequest("POST", "http://"+addr+"/echo?a=1&b=2", strings.NewReader(payload))
	req.Header.Set("X-Test-Header", "value")
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != payload || resp.Header.Get("X-Query") != "a=1&b=2" ||
		resp.Header.Get("X-Hdr") != "value" {
		t.Fatalf("POST: %d len=%d %v", resp.StatusCode, len(body), resp.Header)
	}

	// Chunked request body (unknown length).
	req, _ = http.NewRequest("POST", "http://"+addr+"/echo", io.MultiReader(strings.NewReader("abc"), strings.NewReader("def")))
	req.ContentLength = -1
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "abcdef" {
		t.Fatalf("chunked POST: %q", body)
	}

	resp, err = c.Get("http://" + addr + "/big")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) != 256<<10 || !bytes.Equal(body[:16], []byte("0123456789abcdef")) {
		t.Fatalf("big: %d", len(body))
	}

	resp, err = c.Get("http://" + addr + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if n := cf.conns.Load(); n != 1 {
		t.Fatalf("keep-alive: %d connections used, want 1", n)
	}
}

func TestServer100KeepAlive(t *testing.T) {
	s := &Server{Handler: testHandler}
	addr, cf := startServer(t, s)
	c := newClient()
	defer c.CloseIdleConnections()
	for i := 0; i < 100; i++ {
		resp, err := c.Get("http://" + addr + "/echo?i=" + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.Header.Get("X-Query") != "i="+strconv.Itoa(i) {
			t.Fatalf("request %d: %v", i, resp.Header)
		}
	}
	if n := cf.conns.Load(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

func dial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	return nc, bufio.NewReader(nc)
}

func TestServerPipelining(t *testing.T) {
	s := &Server{Handler: testHandler, MaxPipelinedRequests: 2}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	var reqs bytes.Buffer
	const n = 5
	for i := 0; i < n; i++ {
		fmt.Fprintf(&reqs, "GET /echo?n=%d HTTP/1.1\r\nHost: x\r\n\r\n", i)
	}
	if _, err := nc.Write(reqs.Bytes()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if got := resp.Header.Get("X-Query"); got != "n="+strconv.Itoa(i) {
			t.Fatalf("response %d out of order: %q", i, got)
		}
	}
	// The connection is still usable after the pipelining yield.
	fmt.Fprintf(nc, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("after pipeline: %v %v", resp, err)
	}
}

func TestServerDeepPipeline(t *testing.T) {
	s := &Server{Handler: testHandler}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	const n = 200
	var reqs bytes.Buffer
	for i := 0; i < n; i++ {
		fmt.Fprintf(&reqs, "GET /echo?n=%d HTTP/1.1\r\nHost: x\r\n\r\n", i)
	}
	go nc.Write(reqs.Bytes())
	for i := 0; i < n; i++ {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("X-Query"); got != "n="+strconv.Itoa(i) {
			t.Fatalf("response %d: %q", i, got)
		}
	}
}

func TestServer431(t *testing.T) {
	s := &Server{Handler: testHandler}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	fmt.Fprintf(nc, "GET /hello HTTP/1.1\r\nHost: x\r\nX-Big: %s\r\n\r\n", strings.Repeat("a", 9<<10))
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 431 || !resp.Close {
		t.Fatalf("status %d close %v", resp.StatusCode, resp.Close)
	}
	// The server half-closes: the client sees EOF.
	if rest, err := io.ReadAll(br); err != nil || len(rest) != 0 {
		t.Fatalf("expected EOF, got %q %v", rest, err)
	}
}

func TestServer413(t *testing.T) {
	s := &Server{Handler: testHandler, MaxBodyBytes: 1024}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	fmt.Fprintf(nc, "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 2048\r\n\r\n%s", strings.Repeat("b", 2048))
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 || !resp.Close {
		t.Fatalf("status %d", resp.StatusCode)
	}

	// Chunked body over the limit.
	nc, br = dial(t, addr)
	fmt.Fprintf(nc, "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n800\r\n%s\r\n0\r\n\r\n", strings.Repeat("c", 2048))
	resp, err = http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("chunked status %d", resp.StatusCode)
	}
}

func TestServerConnectionClose(t *testing.T) {
	s := &Server{Handler: testHandler}
	addr, _ := startServer(t, s)

	// Client asks for close.
	nc, br := dial(t, addr)
	fmt.Fprintf(nc, "GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\nGET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	if !resp.Close {
		t.Fatal("response to Connection: close must close")
	}
	if rest, _ := io.ReadAll(br); len(rest) != 0 {
		t.Fatalf("data after close: %q", rest)
	}

	// Handler asks for close.
	nc, br = dial(t, addr)
	fmt.Fprintf(nc, "GET /close HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err = http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	if !resp.Close || string(b) != "bye" {
		t.Fatalf("close=%v body=%q", resp.Close, b)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}

	// HTTP/1.0 without keep-alive closes; with keep-alive stays open.
	nc, br = dial(t, addr)
	fmt.Fprintf(nc, "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	resp, err = http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	if resp.Close || resp.Header.Get("Connection") != "keep-alive" {
		t.Fatalf("1.0 keep-alive: close=%v", resp.Close)
	}
	fmt.Fprintf(nc, "GET /hello HTTP/1.0\r\n\r\n")
	resp, err = http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	if !resp.Close {
		t.Fatal("HTTP/1.0 without keep-alive must close")
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestServerPanicAndBadRequest(t *testing.T) {
	s := &Server{Handler: testHandler, Logger: discardLogger()}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	fmt.Fprintf(nc, "GET /panic HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 500 || !resp.Close {
		t.Fatalf("panic: %d", resp.StatusCode)
	}

	nc, br = dial(t, addr)
	fmt.Fprintf(nc, "BAD\r\n\r\n")
	resp, err = http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 || !resp.Close {
		t.Fatalf("bad request: %d", resp.StatusCode)
	}
}

func TestServerDateRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the Date timer")
	}
	s := &Server{Handler: testHandler}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	get := func() time.Time {
		fmt.Fprintf(nc, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		d, err := http.ParseTime(resp.Header.Get("Date"))
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	first := get()
	time.Sleep(2100 * time.Millisecond)
	second := get()
	if !second.After(first) {
		t.Fatalf("Date not refreshed: %v then %v", first, second)
	}
	if delta := time.Since(second); delta < -time.Second || delta > 2*time.Second {
		t.Fatalf("Date stale by %v", delta)
	}
}

func TestServerListen(t *testing.T) {
	e := startEngine(t, twine.Loops(1), twine.Watchdog(-1))
	s := &Server{Handler: testHandler}
	lst, err := s.Listen(e, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := newClient()
	defer c.CloseIdleConnections()
	resp, err := c.Get("http://" + lst.Addr().String() + "/hello")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// TestServerBackpressure pipelines many large responses to a client that
// does not read at first: the connection must pause as a producer instead
// of buffering everything (which would hit WriteBufferLimit), then deliver
// every response once the client reads.
func TestServerBackpressure(t *testing.T) {
	s := &Server{Handler: testHandler}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	const n = 100 // 100 x 256 KiB = 25 MiB, above the 16 MiB write limit
	var reqs bytes.Buffer
	for i := 0; i < n; i++ {
		reqs.WriteString("GET /big HTTP/1.1\r\nHost: x\r\n\r\n")
	}
	if _, err := nc.Write(reqs.Bytes()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < n; i++ {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		m, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if m != 256<<10 {
			t.Fatalf("response %d: %d bytes", i, m)
		}
	}
}

// TestServerCloseWhilePaused closes a connection from a request served
// after a pipelining yield (reading paused): the server must still observe
// the client's EOF and finish the close promptly.
func TestServerCloseWhilePaused(t *testing.T) {
	s := &Server{Handler: testHandler, MaxPipelinedRequests: 1}
	addr, cf := startServer(t, s)
	nc, br := dial(t, addr)
	fmt.Fprintf(nc, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\nGET /close HTTP/1.1\r\nHost: x\r\n\r\n")
	for i := 0; i < 2; i++ {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	nc.Close()
	deadline := time.Now().Add(2 * time.Second)
	for cf.lost.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server did not finish closing within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDateCacheCleanup serves one Server through Factory (not Listen) on
// several engines in turn: each engine's per-loop Date caches must be
// dropped when it stops, so a long-lived Server does not keep stopped
// loops reachable.
func TestDateCacheCleanup(t *testing.T) {
	s := &Server{Handler: testHandler}
	for i := 0; i < 3; i++ {
		e := twine.New(twine.Loops(2), twine.Watchdog(-1))
		lst, err := e.ListenTCP("127.0.0.1:0", s.Factory())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { errc <- e.Run(ctx) }()
		c := newClient()
		for j := 0; j < 4; j++ { // spread over both loops
			resp, err := c.Get("http://" + lst.Addr().String() + "/hello")
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			c.CloseIdleConnections()
		}
		s.mu.Lock()
		live := len(s.dates)
		s.mu.Unlock()
		if live == 0 {
			t.Fatal("no Date cache while serving")
		}
		cancel()
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		dates, engines := len(s.dates), len(s.engines)
		s.mu.Unlock()
		if dates != 0 || engines != 0 {
			t.Fatalf("engine %d stopped: %d Date caches and %d engines still referenced", i, dates, engines)
		}
	}
}

// TestServerMaxResponseBytes lowers the engine's WriteBufferLimit and the
// server's MaxResponseBytes below it: a body over the limit must become a
// complete 500 instead of a connection aborted mid-body.
func TestServerMaxResponseBytes(t *testing.T) {
	e := startEngine(t, twine.Loops(1), twine.Watchdog(-1), twine.WriteBufferLimit(1<<20))
	s := &Server{MaxResponseBytes: 512 << 10, Logger: discardLogger(), Handler: func(ctx *RequestCtx) {
		n, _ := strconv.Atoi(string(ctx.RawQuery()))
		ctx.Write(bytes.Repeat([]byte{'z'}, n))
	}}
	lst, err := s.Listen(e, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nc, br := dial(t, lst.Addr().String())
	// Neither request is read until both are answered, so the response
	// must fit in the write buffer.
	fmt.Fprintf(nc, "GET /?%d HTTP/1.1\r\nHost: x\r\n\r\nHEAD /?%d HTTP/1.1\r\nHost: x\r\n\r\nGET /?%d HTTP/1.1\r\nHost: x\r\n\r\n",
		400<<10, 600<<10, 8<<20)
	time.Sleep(100 * time.Millisecond)
	for _, want := range []struct {
		method string
		status int
		n      int64
	}{{"GET", 200, 400 << 10}, {"HEAD", 200, 0}, {"GET", 500, int64(len("Internal Server Error\n"))}} {
		resp, err := http.ReadResponse(br, &http.Request{Method: want.method})
		if err != nil {
			t.Fatalf("%+v: %v", want, err)
		}
		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil || resp.StatusCode != want.status || n != want.n {
			t.Fatalf("%+v: status %d, %d body bytes, %v", want, resp.StatusCode, n, err)
		}
		if want.status == 500 && !resp.Close {
			t.Fatal("500 for an oversized body must close")
		}
	}
	expectClosed(t, br, 5*time.Second)
}

// TestMaxResponseBytesDefault checks the default limit through the fake
// transport (no write buffer involved).
func TestMaxResponseBytesDefault(t *testing.T) {
	s := &Server{Logger: discardLogger(), Handler: func(ctx *RequestCtx) {
		ctx.Write(make([]byte, DefaultMaxResponseBytes+1))
	}}
	out, closed := serveOnce(t, testLoop(t), s, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.HasPrefix(out, "HTTP/1.1 500 ") || !closed || len(out) > 1024 {
		t.Fatalf("closed=%v %d bytes: %q", closed, len(out), firstLine(out))
	}
}

// TestIdleTimeoutSetting checks the value handed to the transport's idle
// timer: zero means the default, negative disables it.
func TestIdleTimeoutSetting(t *testing.T) {
	l := testLoop(t)
	for _, tc := range []struct{ set, want time.Duration }{
		{0, DefaultIdleTimeout},
		{-1, 0},
		{5 * time.Second, 5 * time.Second},
	} {
		h := newHarness(t, l, &Server{IdleTimeout: tc.set})
		var got time.Duration
		onLoop(t, l, func() { got = h.ft.idle })
		h.lose()
		if got != tc.want {
			t.Errorf("IdleTimeout %v: transport idle timeout %v, want %v", tc.set, got, tc.want)
		}
	}
}

// expectClosed reads from br until EOF and fails if that takes longer
// than within.
func expectClosed(t *testing.T, br *bufio.Reader, within time.Duration) []byte {
	t.Helper()
	start := time.Now()
	rest, err := io.ReadAll(br)
	if err != nil {
		t.Fatalf("read: %v (got %q)", err, rest)
	}
	if d := time.Since(start); d > within {
		t.Fatalf("closed after %v, want within %v", d, within)
	}
	return rest
}

func TestServerIdleTimeout(t *testing.T) {
	s := &Server{Handler: testHandler, IdleTimeout: 200 * time.Millisecond}
	addr, _ := startServer(t, s)
	_, br := dial(t, addr)
	if rest := expectClosed(t, br, 5*time.Second); len(rest) != 0 {
		t.Fatalf("idle close sent %q", rest)
	}
}

// TestServerReadTimeoutTrickle trickles a head one byte at a time, which
// keeps the idle timer from firing: ReadTimeout must answer 408 and close.
func TestServerReadTimeoutTrickle(t *testing.T) {
	// The idle timeout alone would close the connection after 400 ms; the
	// trickle (every 25 ms) keeps resetting it, so only ReadTimeout fires.
	const timeout = 800 * time.Millisecond
	s := &Server{Handler: testHandler, ReadTimeout: timeout, IdleTimeout: 400 * time.Millisecond}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	done := make(chan struct{})
	defer close(done)
	go func() {
		if _, err := io.WriteString(nc, "GET /hello HTTP/1.1\r\nHost: x\r\nX-Slow: "); err != nil {
			return
		}
		tick := time.NewTicker(25 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if _, err := nc.Write([]byte{'a'}); err != nil {
					return
				}
			}
		}
	}()
	start := time.Now()
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusRequestTimeout || !resp.Close {
		t.Fatalf("status %d close %v", resp.StatusCode, resp.Close)
	}
	if d := time.Since(start); d < timeout {
		t.Fatalf("408 after %v, before ReadTimeout %v", d, timeout)
	}
	expectClosed(t, br, 5*time.Second)
}

// TestServerReadTimeoutRestarts checks that a request completed in time
// clears the clock, that the stale timer firing later does nothing, and
// that the next incomplete request gets a fresh ReadTimeout.
func TestServerReadTimeoutRestarts(t *testing.T) {
	const timeout = 600 * time.Millisecond
	s := &Server{Handler: testHandler, ReadTimeout: timeout}
	addr, _ := startServer(t, s)
	nc, br := dial(t, addr)
	get := func() {
		t.Helper()
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	}
	io.WriteString(nc, "GET /hello HTTP/1.1\r\nHo")
	time.Sleep(timeout / 4)
	io.WriteString(nc, "st: x\r\n\r\n")
	get()
	time.Sleep(timeout) // the timer armed by the first request fires
	io.WriteString(nc, "GET /hello HTTP/1.1\r\nHost: x\r\n\r\n")
	get()
	io.WriteString(nc, "GET /hello HTTP/1.1\r\n")
	start := time.Now()
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if d := time.Since(start); resp.StatusCode != http.StatusRequestTimeout || d > 5*time.Second {
		t.Fatalf("status %d after %v", resp.StatusCode, d)
	}
}
