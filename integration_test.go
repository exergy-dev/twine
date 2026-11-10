package twine

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	mrand "math/rand/v2"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Shared helpers for the integration test files.
// ---------------------------------------------------------------------------

var errBoom = errors.New("boom")

// errSink collects failures observed on a loop goroutine so that they are
// reported from the test goroutine (t.Fatal on a loop would Goexit the loop,
// and t.Error after the test returned panics).
type errSink struct {
	mu   sync.Mutex
	msgs []string
}

func (s *errSink) Errorf(format string, args ...any) {
	s.mu.Lock()
	s.msgs = append(s.msgs, fmt.Sprintf(format, args...))
	s.mu.Unlock()
}

func (s *errSink) report(t testing.TB) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.msgs {
		t.Error(m)
	}
	s.msgs = nil
}

// hookProto is a Protocol whose callbacks are closures; it keeps the
// Transport in p.t (loop-affine).
type hookProto struct {
	t    Transport
	made func(p *hookProto, t Transport)
	data func(p *hookProto, b []byte)
	lost func(p *hookProto, err error)
}

func (p *hookProto) ConnectionMade(t Transport) {
	p.t = t
	if p.made != nil {
		p.made(p, t)
	}
}

func (p *hookProto) DataReceived(b []byte) {
	if p.data != nil {
		p.data(p, b)
	}
}

func (p *hookProto) ConnectionLost(err error) {
	if p.lost != nil {
		p.lost(p, err)
	}
}

func hookFactory(build func() *hookProto) Factory {
	return func(net.Addr) Protocol { return build() }
}

// listen binds a TCP listener on loopback or fails the test.
func listen(t testing.TB, e *Engine, f Factory, opts ...ListenOption) Listener {
	t.Helper()
	lst, err := e.ListenTCP("127.0.0.1:0", f, opts...)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	return lst
}

// dialTCP connects to addr; the connection is closed at test cleanup.
func dialTCP(t testing.TB, addr string) *net.TCPConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close() })
	return c.(*net.TCPConn)
}

// waitUntil polls cond until it holds or timeout passes.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// recvErr waits for one value on ch.
func recvErr(t testing.TB, ch <-chan error, timeout time.Duration, what string) (error, bool) {
	t.Helper()
	select {
	case err := <-ch:
		return err, true
	case <-time.After(timeout):
		t.Errorf("timed out after %v waiting for %s", timeout, what)
		return nil, false
	}
}

// sumMetrics adds every loop's counters.
func sumMetrics(e *Engine) MetricsSnapshot {
	var s MetricsSnapshot
	for _, l := range e.Loops() {
		m := l.Metrics()
		s.Connections += m.Connections
		s.Accepts += m.Accepts
		s.Closes += m.Closes
		s.WriteAfterClose += m.WriteAfterClose
		s.Writev += m.Writev
		s.Wakeups += m.Wakeups
		s.WakeupsCoalesced += m.WakeupsCoalesced
		s.TasksRun += m.TasksRun
		s.Panics += m.Panics
		s.BacklogDrops += m.BacklogDrops
		s.SlowCallbacks += m.SlowCallbacks
	}
	return s
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// fillPattern writes the deterministic test stream starting at absolute
// offset off into b.
func fillPattern(b []byte, off int64) {
	i := 0
	// Unaligned head.
	for ; i < len(b) && (off+int64(i))&7 != 0; i++ {
		o := uint64(off + int64(i))
		b[i] = byte(splitmix64(o>>3) >> (8 * (o & 7)))
	}
	for ; i+8 <= len(b); i += 8 {
		o := uint64(off + int64(i))
		binary.LittleEndian.PutUint64(b[i:], splitmix64(o>>3))
	}
	for ; i < len(b); i++ {
		o := uint64(off + int64(i))
		b[i] = byte(splitmix64(o>>3) >> (8 * (o & 7)))
	}
}

// patternChecker verifies a stream against fillPattern.
type patternChecker struct {
	off     int64
	scratch []byte
	bad     int64 // first bad offset, -1 if none
}

func newPatternChecker() *patternChecker { return &patternChecker{bad: -1} }

func (c *patternChecker) Write(b []byte) (int, error) {
	if cap(c.scratch) < len(b) {
		c.scratch = make([]byte, len(b))
	}
	exp := c.scratch[:len(b)]
	fillPattern(exp, c.off)
	if c.bad < 0 && !bytes.Equal(exp, b) {
		for i := range b {
			if b[i] != exp[i] {
				c.bad = c.off + int64(i)
				break
			}
		}
	}
	c.off += int64(len(b))
	return len(b), nil
}

// readAllPattern reads conn to EOF, verifying the pattern; it returns the
// byte count and the first mismatching offset (-1 if none).
func readAllPattern(c net.Conn) (int64, int64, error) {
	pc := newPatternChecker()
	_, err := io.Copy(pc, c)
	return pc.off, pc.bad, err
}

// ---------------------------------------------------------------------------
// Echo round trip.
// ---------------------------------------------------------------------------

func TestIntegrationEcho(t *testing.T) {
	const clients = 64
	msgs := 1000
	if testing.Short() {
		msgs = 100
	}
	for _, loops := range []int{1, 2, runtime.GOMAXPROCS(0)} {
		t.Run("loops="+strconv.Itoa(loops), func(t *testing.T) {
			e := startEngine(t, Loops(loops))
			lst := listen(t, e, echoFactory())
			addr := lst.Addr().String()
			var wg sync.WaitGroup
			for i := 0; i < clients; i++ {
				wg.Add(1)
				go func(seed uint64) {
					defer wg.Done()
					c, err := net.DialTimeout("tcp", addr, 5*time.Second)
					if err != nil {
						t.Error(err)
						return
					}
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(60 * time.Second))
					rng := mrand.New(mrand.NewPCG(seed, 99))
					buf := make([]byte, 512)
					got := make([]byte, 512)
					for j := 0; j < msgs; j++ {
						n := 1 + rng.IntN(len(buf))
						for k := 0; k < n; k++ {
							buf[k] = byte(rng.Uint32())
						}
						if _, err := c.Write(buf[:n]); err != nil {
							t.Errorf("client %d msg %d write: %v", seed, j, err)
							return
						}
						if _, err := io.ReadFull(c, got[:n]); err != nil {
							t.Errorf("client %d msg %d read: %v", seed, j, err)
							return
						}
						if !bytes.Equal(got[:n], buf[:n]) {
							t.Errorf("client %d msg %d: echo mismatch", seed, j)
							return
						}
					}
				}(uint64(i))
			}
			wg.Wait()
		})
	}
}

// ---------------------------------------------------------------------------
// Backpressure: streaming producer around the write watermarks.
// ---------------------------------------------------------------------------

type streamProducer struct {
	t         Transport
	chunk     []byte
	crossings int
	sink      *errSink

	paused  bool // loop-only
	resumes int  // loop-only

	written  atomic.Int64
	stops    atomic.Int32
	pausedCh chan struct{}
	once     sync.Once

	mu     sync.Mutex
	events []string
}

func (p *streamProducer) record(ev string) {
	p.mu.Lock()
	p.events = append(p.events, ev)
	p.mu.Unlock()
}

func (p *streamProducer) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

func (p *streamProducer) produce() {
	limit := p.written.Load() + 32<<20
	for !p.paused {
		off := p.written.Load()
		if off >= limit {
			p.sink.Errorf("producer wrote 32 MiB without PauseProducing")
			p.t.Abort()
			return
		}
		fillPattern(p.chunk, off)
		p.written.Add(int64(len(p.chunk)))
		p.t.Write(p.chunk) // may call PauseProducing synchronously
	}
}

func (p *streamProducer) PauseProducing() {
	if p.paused {
		p.sink.Errorf("PauseProducing while already paused")
	}
	p.paused = true
	p.record("pause")
	p.once.Do(func() { close(p.pausedCh) })
}

func (p *streamProducer) ResumeProducing() {
	if !p.paused {
		p.sink.Errorf("ResumeProducing while not paused")
	}
	// A streaming producer is resumed only once the ring has drained to the
	// low-water mark (default 64 KiB), whatever the kernel buffers do.
	if n := p.t.PendingWrite(); n > 64<<10 {
		p.sink.Errorf("ResumeProducing with %d bytes still pending, above the 64 KiB low-water mark", n)
	}
	p.paused = false
	p.record("resume")
	p.resumes++
	if p.resumes < p.crossings {
		p.produce()
		return
	}
	p.t.Close()
}

func (p *streamProducer) StopProducing() { p.stops.Add(1) }

// bpSockBuf is the SO_SNDBUF / SO_RCVBUF pinned by TestBackpressureProducer.
const bpSockBuf = 64 << 10

// dialTCPRcvBuf dials addr with SO_RCVBUF set to rcvBuf before the connect,
// so the advertised window is fixed and receive-buffer autotuning is off.
func dialTCPRcvBuf(t testing.TB, addr string, rcvBuf int) *net.TCPConn {
	t.Helper()
	d := net.Dialer{Timeout: 5 * time.Second, Control: func(_, _ string, rc syscall.RawConn) error {
		var serr error
		err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, rcvBuf)
		})
		if err != nil {
			return err
		}
		return serr
	}}
	c, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close() })
	return c.(*net.TCPConn)
}

func TestBackpressureProducer(t *testing.T) {
	for _, crossings := range []int{1, 2} {
		t.Run("crossings="+strconv.Itoa(crossings), func(t *testing.T) {
			e := startEngine(t, Loops(1))
			sink := &errSink{}
			prodCh := make(chan *streamProducer, 1)
			lostCh := make(chan error, 1)
			lst := listen(t, e, hookFactory(func() *hookProto {
				return &hookProto{
					made: func(_ *hookProto, tr Transport) {
						p := &streamProducer{t: tr, chunk: make([]byte, 1<<20), crossings: crossings,
							sink: sink, pausedCh: make(chan struct{})}
						prodCh <- p
						tr.RegisterProducer(p, true)
						p.produce()
					},
					lost: func(_ *hookProto, err error) { lostCh <- err },
				}
			}), SocketBuffers(0, bpSockBuf))
			// Pin the client's receive buffer before connecting. With fixed
			// buffers at both ends the kernel can hold at most a few hundred
			// KiB of the stream, far less than the 1 MiB chunk that crosses
			// the high-water mark, so the ring cannot drain to the low-water
			// mark while the client is not reading. With autotuned buffers it
			// can: macOS grows the send and receive buffers as the connection
			// runs, and the first CI run saw [pause resume pause] before any
			// read.
			c := dialTCPRcvBuf(t, lst.Addr().String(), bpSockBuf)
			var p *streamProducer
			select {
			case p = <-prodCh:
			case <-time.After(5 * time.Second):
				t.Fatal("server never accepted")
			}
			select {
			case <-p.pausedCh:
			case <-time.After(10 * time.Second):
				t.Fatalf("PauseProducing never called; events=%v", p.snapshot())
			}
			// The client does not read for a while: nothing may resume.
			time.Sleep(100 * time.Millisecond)
			ev := p.snapshot()
			t.Logf("events before client reads: %v", ev)
			if len(ev) != 1 || ev[0] != "pause" {
				t.Errorf("events before client reads = %v, want [pause]", ev)
			}
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, bad, err := readAllPattern(c)
			if err != nil {
				t.Errorf("client read: %v", err)
			}
			if bad >= 0 {
				t.Errorf("stream corrupt at offset %d", bad)
			}
			if w := p.written.Load(); n != w {
				t.Errorf("client read %d bytes, producer wrote %d", n, w)
			}
			c.Close()
			if err, ok := recvErr(t, lostCh, 5*time.Second, "ConnectionLost"); ok && err != nil {
				t.Errorf("ConnectionLost(%v), want nil", err)
			}
			var want []string
			for i := 0; i < crossings; i++ {
				want = append(want, "pause", "resume")
			}
			if ev := p.snapshot(); fmt.Sprint(ev) != fmt.Sprint(want) {
				t.Errorf("producer events = %v, want %v", ev, want)
			}
			if s := p.stops.Load(); s != 1 {
				t.Errorf("StopProducing called %d times after close, want 1", s)
			}
			sink.report(t)
		})
	}
}

// pullProducer writes one chunk per ResumeProducing (Twisted FileSender
// style) and relies on the transport to ask for more.
type pullProducer struct {
	t       Transport
	chunk   []byte
	total   int64
	written atomic.Int64
	done    bool
}

func (p *pullProducer) ResumeProducing() {
	if p.done {
		return
	}
	off := p.written.Load()
	n := int64(len(p.chunk))
	if p.total-off < n {
		n = p.total - off
	}
	fillPattern(p.chunk[:n], off)
	p.written.Add(n)
	p.t.Write(p.chunk[:n])
	if p.written.Load() == p.total {
		p.done = true
		p.t.UnregisterProducer()
		p.t.Close()
	}
}
func (p *pullProducer) PauseProducing() {}
func (p *pullProducer) StopProducing()  {}

// A non-streaming producer must be asked for more data whenever the
// transport can take it, including when its previous chunk was written
// directly to the socket without touching the ring.
func TestPullProducer(t *testing.T) {
	const total = 8 << 20
	e := startEngine(t, Loops(1))
	prodCh := make(chan *pullProducer, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) {
			p := &pullProducer{t: tr, chunk: make([]byte, 16<<10), total: total}
			prodCh <- p
			tr.RegisterProducer(p, false)
		}}
	}))
	c := dialTCP(t, lst.Addr().String())
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	n, bad, err := readAllPattern(c)
	var p *pullProducer
	select {
	case p = <-prodCh:
	default:
	}
	written := int64(-1)
	if p != nil {
		written = p.written.Load()
	}
	if err != nil || n != total || bad >= 0 {
		t.Errorf("pull producer: client read %d of %d bytes (producer wrote %d), bad=%d, err=%v",
			n, total, written, bad, err)
	}
}

// ---------------------------------------------------------------------------
// Partial writes with tiny socket buffers.
// ---------------------------------------------------------------------------

type tinyProducer struct {
	t        Transport
	buf      []byte
	total    int64
	written  int64 // loop-only
	paused   bool
	finished bool
	crc      uint32
	crcOut   chan uint32
}

func (p *tinyProducer) produce() {
	for !p.paused && p.written < p.total {
		n := int64(len(p.buf))
		if p.total-p.written < n {
			n = p.total - p.written
		}
		b := p.buf[:n]
		fillPattern(b, p.written)
		p.crc = crc32.Update(p.crc, crc32.IEEETable, b)
		p.written += n
		p.t.Write(b)
	}
	if p.written == p.total && !p.finished {
		p.finished = true
		p.crcOut <- p.crc
		p.t.Close()
	}
}
func (p *tinyProducer) PauseProducing()  { p.paused = true }
func (p *tinyProducer) ResumeProducing() { p.paused = false; p.produce() }
func (p *tinyProducer) StopProducing()   {}

func TestPartialWritesTinyBuffers(t *testing.T) {
	total := int64(100 << 20)
	if testing.Short() {
		total = 10 << 20
	}
	e := startEngine(t, Loops(1))
	crcOut := make(chan uint32, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) {
			p := &tinyProducer{t: tr, buf: make([]byte, 64<<10), total: total, crcOut: crcOut}
			tr.RegisterProducer(p, true)
			p.produce()
		}}
	}), SocketBuffers(0, 4096))

	// Expected stream checksum, computed independently of the server.
	expCRC := uint32(0)
	{
		buf := make([]byte, 1<<20)
		for off := int64(0); off < total; off += int64(len(buf)) {
			n := int64(len(buf))
			if total-off < n {
				n = total - off
			}
			fillPattern(buf[:n], off)
			expCRC = crc32.Update(expCRC, crc32.IEEETable, buf[:n])
		}
	}

	d := net.Dialer{Timeout: 5 * time.Second, Control: func(_, _ string, rc syscall.RawConn) error {
		var serr error
		err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		})
		if err != nil {
			return err
		}
		return serr
	}}
	c, err := d.Dial("tcp", lst.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(100 * time.Second))
	h := crc32.NewIEEE()
	start := time.Now()
	n, err := io.Copy(h, c)
	t.Logf("received %d bytes in %v", n, time.Since(start))
	if err != nil {
		t.Errorf("client read: %v", err)
	}
	if n != total {
		t.Errorf("received %d bytes, want %d", n, total)
	}
	if got := h.Sum32(); got != expCRC {
		t.Errorf("client crc %08x, want %08x", got, expCRC)
	}
	// The point of the tiny buffers: the socket must have pushed back, so
	// data went through the ring and the writev flush path.
	if m := sumMetrics(e); m.Writev == 0 {
		t.Error("no ring flushes (Writev == 0): the tiny buffers never caused a partial write / EAGAIN")
	} else {
		t.Logf("%d ring flushes", m.Writev)
	}
	select {
	case s := <-crcOut:
		if s != expCRC {
			t.Errorf("server crc %08x, want %08x", s, expCRC)
		}
	case <-time.After(time.Second):
		t.Error("server never finished producing")
	}
}

// ---------------------------------------------------------------------------
// Transport.Close (half close) vs Abort, CloseLinger.
// ---------------------------------------------------------------------------

func TestCloseHalfClose(t *testing.T) {
	e := startEngine(t, Loops(1))
	var mu sync.Mutex
	var got []byte
	lostCh := make(chan error, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) {
				tr.Write([]byte("hello"))
				tr.Close()
			},
			data: func(_ *hookProto, b []byte) {
				mu.Lock()
				got = append(got, b...)
				mu.Unlock()
			},
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}))
	c := dialTCP(t, lst.Addr().String())
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, err := io.ReadAll(c) // returns at EOF (server SHUT_WR)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(b) != "hello" {
		t.Errorf("client read %q before EOF, want %q", b, "hello")
	}
	// Half-closed: the client can still send and the server receives it.
	if _, err := c.Write([]byte("world")); err != nil {
		t.Fatalf("client write after EOF: %v", err)
	}
	if !waitUntil(5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return string(got) == "world" }) {
		mu.Lock()
		t.Errorf("server received %q after half-close, want %q", got, "world")
		mu.Unlock()
	}
	select {
	case err := <-lostCh:
		t.Fatalf("ConnectionLost(%v) before the client closed", err)
	default:
	}
	c.Close()
	if err, ok := recvErr(t, lostCh, 5*time.Second, "ConnectionLost after client close"); ok && err != nil {
		t.Errorf("ConnectionLost(%v), want nil", err)
	}
}

func TestAbortDiscardsPending(t *testing.T) {
	const big = 8 << 20
	e := startEngine(t, Loops(1))
	sink := &errSink{}
	lostCh := make(chan error, 1)
	pendingCh := make(chan int, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) {
				buf := make([]byte, big)
				fillPattern(buf, 0)
				tr.Write(buf)
				pendingCh <- tr.PendingWrite()
				tr.Abort()
				tr.Write([]byte("after abort")) // no-op
			},
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}), SocketBuffers(0, 16<<10))
	c := dialTCP(t, lst.Addr().String())
	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, bad, err := readAllPattern(c)
	elapsed := time.Since(start)
	if err != nil && !errors.Is(err, syscall.ECONNRESET) {
		t.Errorf("client read: %v (want EOF or reset)", err)
	}
	if bad >= 0 {
		t.Errorf("stream corrupt at offset %d", bad)
	}
	if n >= big {
		t.Errorf("client received all %d bytes; Abort should discard pending writes", n)
	}
	if elapsed > 2*time.Second {
		t.Errorf("client saw close after %v, want immediate", elapsed)
	}
	if p := <-pendingCh; p == 0 {
		t.Logf("note: nothing was pending at Abort time")
	}
	t.Logf("client received %d of %d bytes after Abort", n, big)
	if err, ok := recvErr(t, lostCh, 5*time.Second, "ConnectionLost"); ok && err != nil {
		t.Errorf("ConnectionLost(%v) after Abort, want nil", err)
	}
	if w := sumMetrics(e).WriteAfterClose; w < 1 {
		t.Errorf("WriteAfterClose = %d after Write following Abort, want >= 1", w)
	}
	sink.report(t)
}

func TestCloseLingerForcesClose(t *testing.T) {
	const linger = 200 * time.Millisecond
	e := startEngine(t, Loops(1), CloseLinger(linger))
	type res struct {
		err     error
		elapsed time.Duration
	}
	lostCh := make(chan res, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		var start time.Time
		return &hookProto{
			made: func(_ *hookProto, tr Transport) {
				start = time.Now()
				tr.Close()
			},
			lost: func(_ *hookProto, err error) { lostCh <- res{err, time.Since(start)} },
		}
	}))
	c := dialTCP(t, lst.Addr().String())
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if b, err := io.ReadAll(c); err != nil || len(b) != 0 {
		t.Errorf("client read %q, %v; want EOF", b, err)
	}
	// The client never closes its side.
	select {
	case r := <-lostCh:
		t.Logf("ConnectionLost(%v) after %v", r.err, r.elapsed)
		if r.elapsed < linger-20*time.Millisecond {
			t.Errorf("forced close after %v, before the %v linger", r.elapsed, linger)
		}
		if r.elapsed > linger+time.Second {
			t.Errorf("forced close after %v, linger is %v", r.elapsed, linger)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConnectionLost not called within 3s although CloseLinger is 200ms and the peer never closes")
	}
	// The socket is fully closed now: the client's read side sees EOF or a
	// reset and writes eventually fail.
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	var werr error
	for i := 0; i < 50 && werr == nil; i++ {
		_, werr = c.Write([]byte("x"))
		time.Sleep(10 * time.Millisecond)
	}
	if werr == nil {
		t.Error("client writes still succeed after the server's linger close")
	}
}

// ---------------------------------------------------------------------------
// Idle timeout.
// ---------------------------------------------------------------------------

func TestIdleTimeout(t *testing.T) {
	const idle = 50 * time.Millisecond
	type res struct {
		err     error
		elapsed time.Duration
		sinceRx time.Duration
	}
	t.Run("idle", func(t *testing.T) {
		e := startEngine(t, Loops(1))
		lostCh := make(chan res, 1)
		lst := listen(t, e, hookFactory(func() *hookProto {
			var start time.Time
			return &hookProto{
				made: func(_ *hookProto, tr Transport) { start = time.Now(); tr.SetIdleTimeout(idle) },
				lost: func(_ *hookProto, err error) { lostCh <- res{err: err, elapsed: time.Since(start)} },
			}
		}))
		dialTCP(t, lst.Addr().String())
		select {
		case r := <-lostCh:
			t.Logf("idle timeout fired after %v", r.elapsed)
			if !errors.Is(r.err, ErrIdleTimeout) {
				t.Errorf("ConnectionLost(%v), want ErrIdleTimeout", r.err)
			}
			if r.elapsed < idle-10*time.Millisecond {
				t.Errorf("idle timeout fired early: %v < %v", r.elapsed, idle)
			}
			if r.elapsed > idle+100*time.Millisecond {
				t.Errorf("idle timeout fired late: %v (deadline %v + tick + 100ms scheduling slack)", r.elapsed, idle)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("idle timeout never fired")
		}
	})
	t.Run("activity resets", func(t *testing.T) {
		// A longer timeout here: the client's sleeps run on a shared CI
		// runner, and a 25 ms sleep against a 50 ms timeout has been seen
		// to overshoot on macOS.
		const idle = 300 * time.Millisecond
		e := startEngine(t, Loops(1))
		lostCh := make(chan res, 1)
		lst := listen(t, e, hookFactory(func() *hookProto {
			var start, lastRx time.Time
			return &hookProto{
				made: func(_ *hookProto, tr Transport) { start = time.Now(); lastRx = start; tr.SetIdleTimeout(idle) },
				data: func(p *hookProto, b []byte) { lastRx = time.Now(); p.t.Write(b) },
				lost: func(_ *hookProto, err error) {
					lostCh <- res{err: err, elapsed: time.Since(start), sinceRx: time.Since(lastRx)}
				},
			}
		}))
		c := dialTCP(t, lst.Addr().String())
		buf := make([]byte, 1)
		for i := 0; i < 6; i++ {
			time.Sleep(idle / 4)
			if _, err := c.Write([]byte{'x'}); err != nil {
				t.Fatalf("write %d: %v (connection timed out despite activity)", i, err)
			}
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.ReadFull(c, buf); err != nil {
				t.Fatalf("read %d: %v (connection timed out despite activity)", i, err)
			}
		}
		select {
		case r := <-lostCh:
			if !errors.Is(r.err, ErrIdleTimeout) {
				t.Errorf("ConnectionLost(%v), want ErrIdleTimeout", r.err)
			}
			if r.elapsed < 6*idle/4 {
				t.Errorf("connection lost after %v despite activity every %v", r.elapsed, idle/4)
			}
			if r.sinceRx < idle-10*time.Millisecond || r.sinceRx > idle+100*time.Millisecond {
				t.Errorf("timeout fired %v after the last read, want ~%v", r.sinceRx, idle)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("idle timeout never fired after activity stopped")
		}
	})
}

// ---------------------------------------------------------------------------
// CallFromThread wakeup coalescing.
// ---------------------------------------------------------------------------

func TestCallFromThreadWhileBlockedInPoll(t *testing.T) {
	const calls = 1000
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	// Let the loop settle into a blocking poll (no timers, no conns).
	time.Sleep(50 * time.Millisecond)
	before := l.Metrics()
	var ran, offLoop atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			l.CallFromThread(func() {
				if !l.InLoop() {
					offLoop.Add(1)
				}
				ran.Add(1)
			})
		}()
	}
	close(start)
	wg.Wait()
	if !waitUntil(10*time.Second, func() bool { return ran.Load() == calls }) {
		t.Fatalf("only %d of %d CallFromThread functions ran", ran.Load(), calls)
	}
	if n := offLoop.Load(); n != 0 {
		t.Errorf("%d functions ran off the loop", n)
	}
	after := l.Metrics()
	wakeups := after.Wakeups - before.Wakeups
	coalesced := after.WakeupsCoalesced - before.WakeupsCoalesced
	t.Logf("%d calls -> %d wakeups, %d coalesced", calls, wakeups, coalesced)
	if wakeups > calls {
		t.Errorf("wakeups = %d > calls = %d", wakeups, calls)
	}
	if wakeups+coalesced != calls {
		t.Errorf("wakeups+coalesced = %d, want %d", wakeups+coalesced, calls)
	}
}

// A producer whose task is consumed by the same drain that its wakeup
// interrupted must not leave the loop asleep with the wakeup flag set:
// otherwise every later CallFromThread coalesces onto a wakeup that was
// already consumed and the loop never runs them.
func TestCallFromThreadNoLostWakeup(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	for round := 0; round < 5; round++ {
		// T1 runs inside drainTasks and makes another goroutine push T2
		// (setting wakePending and writing the eventfd) before returning,
		// so T2 is popped by the same drain.
		l.CallFromThread(func() {
			pushed := make(chan struct{})
			go func() {
				l.CallFromThread(func() {})
				close(pushed)
			}()
			<-pushed
		})
		time.Sleep(50 * time.Millisecond) // the loop goes back to sleep
		ran := make(chan struct{})
		l.CallFromThread(func() { close(ran) })
		select {
		case <-ran:
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: CallFromThread never ran; loop asleep with wakePending=%v, queued=%d",
				round, l.wakePending.Load(), l.tasks.Len())
		}
	}
}

// ---------------------------------------------------------------------------
// Protocol panic isolation.
// ---------------------------------------------------------------------------

func TestProtocolPanicAbortsOnlyThatConnection(t *testing.T) {
	e := startEngine(t, Loops(1))
	lostCh := make(chan error, 8)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{
			data: func(p *hookProto, b []byte) {
				if bytes.Contains(b, []byte("PANIC")) {
					panic(errBoom)
				}
				p.t.Write(b)
			},
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}))
	addr := lst.Addr().String()
	clients := make([]*net.TCPConn, 4)
	for i := range clients {
		clients[i] = dialTCP(t, addr)
		// Make sure each is accepted and echoing before the panic.
		echoOnce(t, clients[i], []byte("warmup"))
	}
	if _, err := clients[0].Write([]byte("PANIC")); err != nil {
		t.Fatal(err)
	}
	_ = clients[0].SetReadDeadline(time.Now().Add(5 * time.Second))
	if b, err := io.ReadAll(clients[0]); len(b) != 0 || (err != nil && !errors.Is(err, syscall.ECONNRESET)) {
		t.Errorf("panicking conn: client read %q, %v; want EOF or reset", b, err)
	}
	err, ok := recvErr(t, lostCh, 5*time.Second, "ConnectionLost of the panicking conn")
	if ok {
		var pe *PanicError
		if !errors.As(err, &pe) {
			t.Errorf("ConnectionLost(%v), want *PanicError", err)
		} else {
			if pe.Value != errBoom {
				t.Errorf("PanicError.Value = %v, want errBoom", pe.Value)
			}
			if len(pe.Stack) == 0 {
				t.Error("PanicError.Stack is empty")
			}
		}
		if !errors.Is(err, errBoom) {
			t.Errorf("errors.Is(%v, errBoom) = false; Unwrap should expose the panic value", err)
		}
	}
	// The other connections on the same loop keep working.
	for i := 1; i < len(clients); i++ {
		for j := 0; j < 100; j++ {
			echoOnce(t, clients[i], []byte(fmt.Sprintf("client %d msg %d", i, j)))
		}
	}
	select {
	case err := <-lostCh:
		t.Errorf("another connection was lost: %v", err)
	default:
	}
	if p := sumMetrics(e).Panics; p < 1 {
		t.Errorf("Panics metric = %d, want >= 1", p)
	}
}

func echoOnce(t testing.TB, c net.Conn, msg []byte) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(msg); err != nil {
		t.Fatalf("echo write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("echo read: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo got %q, want %q", got, msg)
	}
}

// ---------------------------------------------------------------------------
// PauseReading / ResumeReading.
// ---------------------------------------------------------------------------

func TestPauseResumeReading(t *testing.T) {
	t.Run("paused stops receiving", func(t *testing.T) {
		e := startEngine(t, Loops(1))
		var rx atomic.Int64
		trCh := make(chan Transport, 1)
		lst := listen(t, e, hookFactory(func() *hookProto {
			return &hookProto{
				made: func(_ *hookProto, tr Transport) { trCh <- tr },
				data: func(_ *hookProto, b []byte) { rx.Add(int64(len(b))) },
			}
		}))
		c := dialTCP(t, lst.Addr().String())
		tr := <-trCh
		l := tr.Loop()
		onLoop(t, l, tr.PauseReading)
		if _, err := c.Write(make([]byte, 1000)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		if n := rx.Load(); n != 0 {
			t.Errorf("paused transport received %d bytes", n)
		}
		onLoop(t, l, tr.ResumeReading)
		if !waitUntil(2*time.Second, func() bool { return rx.Load() == 1000 }) {
			t.Errorf("after ResumeReading received %d of 1000 bytes", rx.Load())
		}
		// Data arriving after resume is delivered normally.
		if _, err := c.Write(make([]byte, 500)); err != nil {
			t.Fatal(err)
		}
		if !waitUntil(2*time.Second, func() bool { return rx.Load() == 1500 }) {
			t.Errorf("received %d of 1500 bytes", rx.Load())
		}
	})

	t.Run("toggle under load", func(t *testing.T) {
		const toggles = 10000
		// A small read buffer keeps the bytes per toggle (and the run
		// time under -race) low.
		e := startEngine(t, Loops(1), ReadBufferSize(4096))
		var rx, done atomic.Int64
		var badOff atomic.Int64
		badOff.Store(-1)
		lst := listen(t, e, hookFactory(func() *hookProto {
			pc := newPatternChecker()
			n := 0
			return &hookProto{data: func(p *hookProto, b []byte) {
				pc.Write(b)
				if pc.bad >= 0 {
					badOff.CompareAndSwap(-1, pc.bad)
				}
				rx.Add(int64(len(b)))
				if n >= toggles {
					return
				}
				n++
				done.Store(int64(n))
				tr := p.t
				tr.PauseReading()
				// Alternate how the resume is scheduled.
				if n%2 == 0 {
					tr.Loop().CallLater(0, tr.ResumeReading)
				} else {
					tr.Loop().CallFromThread(tr.ResumeReading)
				}
			}}
		}))
		c := dialTCP(t, lst.Addr().String())
		buf := make([]byte, 16<<10)
		var sent int64
		deadline := time.Now().Add(60 * time.Second)
		_ = c.SetWriteDeadline(deadline)
		for done.Load() < toggles || sent < 4<<20 {
			if time.Now().After(deadline) {
				t.Fatalf("only %d toggles after 60s (sent %d, received %d)", done.Load(), sent, rx.Load())
			}
			fillPattern(buf, sent)
			if _, err := c.Write(buf); err != nil {
				t.Fatalf("client write: %v (toggles=%d sent=%d received=%d)", err, done.Load(), sent, rx.Load())
			}
			sent += int64(len(buf))
		}
		if !waitUntil(10*time.Second, func() bool { return rx.Load() == sent }) {
			t.Errorf("server received %d of %d bytes after %d toggles (stalled?)", rx.Load(), sent, done.Load())
		}
		if b := badOff.Load(); b >= 0 {
			t.Errorf("stream corrupt at offset %d", b)
		}
		t.Logf("%d toggles, %d bytes", done.Load(), sent)
	})
}

// ---------------------------------------------------------------------------
// Write after close and stale Transport handles.
// ---------------------------------------------------------------------------

func TestStaleTransportHandle(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.Loops()[0]
	madeCh := make(chan Transport, 4)
	lostCh := make(chan error, 4)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) { madeCh <- tr },
			data: func(p *hookProto, b []byte) { p.t.Write(b) },
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}))
	addr := lst.Addr().String()

	a := dialTCP(t, addr)
	stale := <-madeCh
	a.Close()
	if _, ok := recvErr(t, lostCh, 5*time.Second, "ConnectionLost of the first conn"); !ok {
		t.FailNow()
	}
	before := sumMetrics(e).WriteAfterClose

	// The next accepted connection reuses the pooled conn (LIFO free list).
	b := dialTCP(t, addr)
	var fresh Transport
	select {
	case fresh = <-madeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("second conn not accepted")
	}
	echoOnce(t, b, []byte("ping"))
	// The point of the test: the new connection reuses the pooled conn
	// struct the stale handle refers to.
	st, ok1 := stale.(*transport)
	fr, ok2 := fresh.(*transport)
	if !ok1 || !ok2 {
		t.Fatalf("unexpected Transport types %T, %T", stale, fresh)
	}
	if st.c != fr.c {
		t.Fatal("the second connection did not reuse the pooled conn; the stale-handle check would be vacuous")
	}

	onLoop(t, l, func() {
		stale.Write([]byte("STALE"))
		stale.Writev([]byte("STA"), []byte("LE"))
		stale.SetIdleTimeout(time.Millisecond)
		stale.PauseReading()
		stale.Close()
		stale.Abort()
	})
	after := sumMetrics(e).WriteAfterClose
	if after-before < 1 {
		t.Errorf("WriteAfterClose went %d -> %d after Write on a stale handle, want an increment", before, after)
	}

	// The new connection is unaffected: no extra bytes, still open, still
	// echoing, still reading, no idle timeout.
	time.Sleep(50 * time.Millisecond)
	_ = b.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	extra := make([]byte, 64)
	if n, err := b.Read(extra); n > 0 || !isTimeout(err) {
		t.Errorf("new connection received %q (err %v) from a stale handle", extra[:n], err)
	}
	_ = b.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := b.Write([]byte("pong")); err != nil {
		t.Fatalf("write on new conn: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(b, got); err != nil || string(got) != "pong" {
		t.Errorf("new conn echo = %q, %v; want \"pong\" (stale handle affected it)", got, err)
	}
	select {
	case err := <-lostCh:
		t.Errorf("new connection lost (%v) after calls on a stale handle", err)
	default:
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestWriteBufferLimit(t *testing.T) {
	e := startEngine(t, Loops(1), WriteBufferLimit(1<<20))
	lostCh := make(chan error, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{
			made: func(_ *hookProto, tr Transport) { tr.Write(make([]byte, 4<<20)) },
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}), SocketBuffers(0, 16<<10))
	dialTCP(t, lst.Addr().String()) // never reads
	if err, ok := recvErr(t, lostCh, 5*time.Second, "ConnectionLost"); ok && !errors.Is(err, ErrWriteBufferFull) {
		t.Errorf("ConnectionLost(%v), want ErrWriteBufferFull", err)
	}
}

// ---------------------------------------------------------------------------
// Transport accessors.
// ---------------------------------------------------------------------------

func TestTransportAddrs(t *testing.T) {
	e := startEngine(t, Loops(1))
	type info struct {
		peer, host string
	}
	infoCh := make(chan info, 2)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) {
			var in info
			in.peer, in.host = tr.RemoteAddr().String(), tr.LocalAddr().String()
			infoCh <- in
			tr.Close()
		}}
	}))
	for i := 0; i < 2; i++ { // the second conn reuses the pooled conn
		c := dialTCP(t, lst.Addr().String())
		var in info
		select {
		case in = <-infoCh:
		case <-time.After(5 * time.Second):
			t.Fatal("not accepted")
		}
		if in.peer != c.LocalAddr().String() {
			t.Errorf("conn %d: RemoteAddr = %s, want %s", i, in.peer, c.LocalAddr())
		}
		if in.host != c.RemoteAddr().String() {
			t.Errorf("conn %d: LocalAddr = %s, want %s", i, in.host, c.RemoteAddr())
		}
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = io.ReadAll(c)
		c.Close()
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Writev and large writes.
// ---------------------------------------------------------------------------

func TestWritevByteExact(t *testing.T) {
	rng := mrand.New(mrand.NewPCG(7, 7))
	randBytes := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	// Batch 1: small slices with empties (direct writev).
	b1 := [][]byte{randBytes(10), nil, randBytes(300), {}, randBytes(1)}
	// Batch 2: more slices than the loop's iovec array.
	var b2 [][]byte
	for i := 0; i < 100; i++ {
		b2 = append(b2, randBytes(1+rng.IntN(2000)))
	}
	// Batch 3: large slices that cannot all fit in the socket buffer.
	b3 := [][]byte{randBytes(3 << 20), randBytes(5), randBytes(2 << 20)}
	// Batch 4: appended behind pending data.
	b4 := [][]byte{randBytes(7), randBytes(70000)}
	var want []byte
	for _, bs := range [][][]byte{b1, b2, b3, b4} {
		for _, b := range bs {
			want = append(want, b...)
		}
	}

	e := startEngine(t, Loops(1))
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) {
			tr.Writev(b1...)
			tr.Writev(b2...)
			tr.Writev(b3...)
			tr.Writev(b4...)
			tr.Close()
		}}
	}))
	c := dialTCP(t, lst.Addr().String())
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Writev delivered %d bytes, want %d (byte-exact mismatch)", len(got), len(want))
	}
}

func TestLargeSingleWrite(t *testing.T) {
	const size = 8 << 20
	want := make([]byte, size)
	if _, err := crand.Read(want); err != nil {
		t.Fatal(err)
	}
	e := startEngine(t, Loops(1))
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{made: func(_ *hookProto, tr Transport) {
			buf := append([]byte(nil), want...)
			tr.Write(buf)
			// The caller may reuse b as soon as Write returns.
			for i := range buf {
				buf[i] = 0
			}
			tr.Close()
		}}
	}))
	c := dialTCP(t, lst.Addr().String())
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("8 MiB write: got %d bytes, want %d (byte-exact mismatch)", len(got), len(want))
	}
}

// ---------------------------------------------------------------------------
// Connection distribution without SO_REUSEPORT.
// ---------------------------------------------------------------------------

func TestReusePortOffDistributes(t *testing.T) {
	const loops, conns = 4, 200
	e := startEngine(t, Loops(loops), ReusePort(false))
	lst := listen(t, e, echoFactory())
	clients := make([]net.Conn, 0, conns)
	for i := 0; i < conns; i++ {
		clients = append(clients, dialTCP(t, lst.Addr().String()))
	}
	if !waitUntil(5*time.Second, func() bool { return sumMetrics(e).Connections == conns }) {
		t.Fatalf("server has %d connections, want %d", sumMetrics(e).Connections, conns)
	}
	var dist []uint64
	for _, l := range e.Loops() {
		m := l.Metrics()
		dist = append(dist, m.Connections)
		if m.Connections == 0 {
			t.Errorf("loop %d has no connections", l.ID())
		}
	}
	t.Logf("distribution over loops: %v", dist)
	// Every connection still works.
	for i, c := range clients {
		if i%20 == 0 {
			echoOnce(t, c, []byte("hi"))
		}
	}
}

// ---------------------------------------------------------------------------
// Idle-connection soak.
// ---------------------------------------------------------------------------

func TestSoakIdleConnections(t *testing.T) {
	n := 10000
	if testing.Short() {
		n = 2000
	}
	// Each connection costs two fds in this process. macOS caps the
	// per-process limit at kern.maxfilesperproc (typically 10240), which
	// does not fit 10000 connections; scale down rather than fail on EMFILE.
	n = fdBudgetConns(t, n, 1000)
	e := startEngine(t, Loops(4))
	lst := listen(t, e, echoFactory())
	addr := lst.Addr().String()

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	clients := make([]net.Conn, n)
	var next atomic.Int64
	var dialErr atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				var c net.Conn
				var err error
				for try := 0; try < 20; try++ {
					c, err = net.DialTimeout("tcp", addr, 5*time.Second)
					if err == nil {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err != nil {
					dialErr.Store(err)
					return
				}
				clients[i] = c
			}
		}()
	}
	wg.Wait()
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.Close()
			}
		}
	}()
	if err := dialErr.Load(); err != nil {
		open := 0
		for _, c := range clients {
			if c != nil {
				open++
			}
		}
		t.Fatalf("dial: %v (%d of %d clients connected)", err, open, n)
	}
	if !waitUntil(30*time.Second, func() bool { return sumMetrics(e).Connections == uint64(n) }) {
		t.Fatalf("server has %d connections, want %d", sumMetrics(e).Connections, n)
	}
	time.Sleep(2 * time.Second) // hold
	for i := 0; i < n; i += n / 10 {
		echoOnce(t, clients[i], []byte("alive"))
	}
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	perConn := (int64(m1.HeapInuse) - int64(m0.HeapInuse)) / int64(n)
	t.Logf("%d idle connections: HeapInuse %d -> %d (%d B/conn incl. client side)", n, m0.HeapInuse, m1.HeapInuse, perConn)
	if perConn > 4<<10 {
		t.Errorf("heap per connection = %d B, want < 4 KiB", perConn)
	}

	for i, c := range clients {
		c.Close()
		clients[i] = nil
	}
	if !waitUntil(30*time.Second, func() bool { return sumMetrics(e).Connections == 0 }) {
		t.Errorf("Connections gauge = %d after closing every client, want 0", sumMetrics(e).Connections)
	}
	if m := sumMetrics(e); m.Closes != m.Accepts || m.Accepts != uint64(n) {
		t.Errorf("Accepts = %d, Closes = %d, want both %d", m.Accepts, m.Closes, n)
	}
}

// A hangup is reported once by edge-triggered polling. When the event that
// carries it also carries more data than the per-event read cap allows, the
// re-armed read next iteration must keep reading to EOF instead of stopping at
// the final short read, which would leave the conn open until shutdown.
func TestHangupSurvivesReadCap(t *testing.T) {
	e := startEngine(t, Loops(1), ReadBufferSize(4096), MaxReadsPerEvent(2), Watchdog(-1))
	var rx atomic.Int64
	var stalled atomic.Bool
	lostCh := make(chan error, 1)
	lst := listen(t, e, hookFactory(func() *hookProto {
		return &hookProto{
			data: func(_ *hookProto, b []byte) {
				// Stall the loop once so the rest of the data and the FIN
				// are queued in the socket before the next event.
				if stalled.CompareAndSwap(false, true) {
					time.Sleep(300 * time.Millisecond)
				}
				rx.Add(int64(len(b)))
			},
			lost: func(_ *hookProto, err error) { lostCh <- err },
		}
	}))
	c := dialTCP(t, lst.Addr().String())
	const total = 100*1024 + 100 // ends on a short read
	if _, err := c.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the loop is now inside the stalled callback
	if _, err := c.Write(make([]byte, total-4096)); err != nil {
		t.Fatal(err)
	}
	c.Close()
	select {
	case <-lostCh:
	case <-time.After(3 * time.Second):
		t.Fatalf("ConnectionLost not delivered; received %d of %d bytes", rx.Load(), total)
	}
	if n := rx.Load(); n != total {
		t.Errorf("received %d of %d bytes", n, total)
	}
}
