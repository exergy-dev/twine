package twtls_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/twtls"
)

// --- helpers -----------------------------------------------------------------

var (
	certOnce sync.Once
	testCert tls.Certificate
	testPool *x509.CertPool
)

// selfSigned returns a self-signed ECDSA P-256 certificate for localhost and
// 127.0.0.1, and a pool that trusts it.
func selfSigned(t testing.TB) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	certOnce.Do(func() {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			panic(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: "twine test"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(24 * time.Hour),
			KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
			IsCA:                  true,
			DNSNames:              []string{"localhost"},
			IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			panic(err)
		}
		leaf, err := x509.ParseCertificate(der)
		if err != nil {
			panic(err)
		}
		testCert = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
		testPool = x509.NewCertPool()
		testPool.AddCert(leaf)
	})
	return testCert, testPool
}

func serverConfig(t testing.TB) *tls.Config {
	cert, _ := selfSigned(t)
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

func startEngine(t testing.TB, opts ...twine.Option) *twine.Engine {
	t.Helper()
	opts = append([]twine.Option{twine.Loops(2), twine.Watchdog(-1)}, opts...)
	e := twine.New(opts...)
	errCh := make(chan error, 1)
	go func() { errCh <- e.Run(context.Background()) }()
	t.Cleanup(func() {
		e.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("engine did not stop")
		}
	})
	return e
}

// echo is a plaintext echo protocol.
type echo struct{ t twine.Transport }

func (p *echo) ConnectionMade(t twine.Transport) { p.t = t }
func (p *echo) DataReceived(b []byte)            { p.t.Write(b) }
func (p *echo) ConnectionLost(error)             {}

func echoFactory() twine.Factory {
	return func(net.Addr) twine.Protocol { return &echo{} }
}

func listen(t testing.TB, e *twine.Engine, cfg *tls.Config, f twine.Factory, opts ...twtls.Option) string {
	t.Helper()
	lst, err := e.ListenTCP("127.0.0.1:0", twtls.Server(cfg, f, opts...))
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	return lst.Addr().String()
}

func dial(t testing.TB, addr string, cfg *tls.Config) *tls.Conn {
	t.Helper()
	if cfg == nil {
		cfg = &tls.Config{InsecureSkipVerify: true}
	}
	d := &net.Dialer{Timeout: 5 * time.Second}
	c, err := tls.DialWithDialer(d, "tcp", addr, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	return c
}

// --- server tests ------------------------------------------------------------

func TestEchoServerMessages(t *testing.T) {
	e := startEngine(t)
	addr := listen(t, e, serverConfig(t), echoFactory())
	c := dial(t, addr, nil)
	defer c.Close()
	buf := make([]byte, 64)
	for i := 0; i < 1000; i++ {
		msg := []byte(fmt.Sprintf("message %04d", i))
		if _, err := c.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(c, buf[:len(msg)]); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(buf[:len(msg)], msg) {
			t.Fatalf("echo %d: got %q want %q", i, buf[:len(msg)], msg)
		}
	}
}

func TestEchoServerStream1MiB(t *testing.T) {
	e := startEngine(t)
	addr := listen(t, e, serverConfig(t), echoFactory())
	c := dial(t, addr, nil)
	defer c.Close()
	data := make([]byte, 1<<20)
	_, _ = rand.Read(data)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Write(data)
		errCh <- err
	}()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("1 MiB stream corrupted")
	}
}

// stateProto reports the server-side ConnectionState and echoes.
type stateProto struct {
	echo
	ch chan tls.ConnectionState
}

func (p *stateProto) ConnectionMade(t twine.Transport) {
	p.t = t
	p.ch <- t.(*twtls.Transport).ConnectionState()
}

func TestVersionsAndALPN(t *testing.T) {
	for _, tc := range []struct {
		name string
		ver  uint16
	}{{"TLS12", tls.VersionTLS12}, {"TLS13", tls.VersionTLS13}} {
		t.Run(tc.name, func(t *testing.T) {
			e := startEngine(t)
			ch := make(chan tls.ConnectionState, 1)
			cfg := serverConfig(t)
			cfg.NextProtos = []string{"h2", "twine/1"}
			addr := listen(t, e, cfg, func(net.Addr) twine.Protocol {
				return &stateProto{ch: ch}
			})
			c := dial(t, addr, &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tc.ver,
				MaxVersion:         tc.ver,
				NextProtos:         []string{"twine/1"},
				ServerName:         "sni.example",
			})
			defer c.Close()
			var st tls.ConnectionState
			select {
			case st = <-ch:
			case <-time.After(10 * time.Second):
				t.Fatal("no ConnectionMade")
			}
			if !st.HandshakeComplete || st.Version != tc.ver {
				t.Fatalf("server state: complete=%v version=%x want %x", st.HandshakeComplete, st.Version, tc.ver)
			}
			if st.NegotiatedProtocol != "twine/1" {
				t.Fatalf("server ALPN = %q", st.NegotiatedProtocol)
			}
			if st.ServerName != "sni.example" {
				t.Fatalf("server SNI = %q", st.ServerName)
			}
			if cs := c.ConnectionState(); cs.Version != tc.ver || cs.NegotiatedProtocol != "twine/1" {
				t.Fatalf("client state: version=%x alpn=%q", cs.Version, cs.NegotiatedProtocol)
			}
			// And data flows.
			if _, err := c.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 4)
			if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping" {
				t.Fatalf("echo: %q %v", buf, err)
			}
		})
	}
}

// byeProto writes a message, then loses the connection.
type byeProto struct {
	lost chan error
	big  []byte
}

func (p *byeProto) ConnectionMade(t twine.Transport) {
	t.Write([]byte("bye"))
	if p.big != nil {
		t.Write(p.big)
	}
	t.Close()
	t.Write([]byte("dropped")) // after Close: dropped
}
func (p *byeProto) DataReceived([]byte)      {}
func (p *byeProto) ConnectionLost(err error) { p.lost <- err }

func TestLoseConnectionSendsCloseNotify(t *testing.T) {
	e := startEngine(t)
	lost := make(chan error, 1)
	big := bytes.Repeat([]byte("x"), 300<<10) // forces ring buffering
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol {
		return &byeProto{lost: lost, big: big}
	})
	c := dial(t, addr, nil)
	got, err := io.ReadAll(c) // io.ReadAll returns nil on io.EOF
	if err != nil {
		t.Fatalf("ReadAll: %v (want clean close_notify EOF)", err)
	}
	want := append([]byte("bye"), big...)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	// A further Read must also be a clean io.EOF, not io.ErrUnexpectedEOF.
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("Read after close_notify: %v", err)
	}
	c.Close()
	select {
	case err := <-lost:
		if err != nil {
			t.Fatalf("ConnectionLost(%v), want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost")
	}
}

// lostProto records lifecycle calls.
type lostProto struct {
	echo
	made atomic.Int32
	lost chan error
}

func (p *lostProto) ConnectionMade(t twine.Transport) { p.made.Add(1); p.t = t }
func (p *lostProto) ConnectionLost(err error)         { p.lost <- err }

func TestPeerCloseNotify(t *testing.T) {
	e := startEngine(t)
	lost := make(chan error, 1)
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol {
		return &lostProto{lost: lost}
	})
	c := dial(t, addr, nil)
	if _, err := c.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil { // client close_notify
		t.Fatal(err)
	}
	// The server answers with its own close_notify.
	if _, err := c.Read(buf); err != io.EOF {
		t.Fatalf("client Read after CloseWrite: %v, want io.EOF", err)
	}
	c.Close()
	select {
	case err := <-lost:
		if err != nil {
			t.Fatalf("ConnectionLost(%v), want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost")
	}
}

func TestServerHandshakeFailure(t *testing.T) {
	e := startEngine(t)
	hsErr := make(chan error, 1)
	p := &lostProto{lost: make(chan error, 1)}
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol { return p },
		twtls.OnHandshakeError(func(err error) { hsErr <- err }))
	// A verifying client that does not trust the self-signed cert. With TLS
	// 1.2 the server is still reading when the client's alert arrives, so
	// the alert itself reaches the server (TLS 1.3 servers write session
	// tickets into the closed socket and usually see a reset instead).
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr,
		&tls.Config{ServerName: "localhost", MaxVersion: tls.VersionTLS12})
	if err == nil {
		c.Close()
		t.Fatal("client handshake unexpectedly succeeded")
	}
	select {
	case err := <-hsErr:
		if err == nil || !strings.Contains(err.Error(), "bad certificate") {
			t.Fatalf("server handshake error %v, want the client's bad_certificate alert", err)
		}
		var he *twtls.HandshakeError
		if !errors.As(err, &he) || he.Peer == nil {
			t.Fatalf("server handshake error %v (%T), want a *HandshakeError with the peer", err, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnHandshakeError not called")
	}
	time.Sleep(50 * time.Millisecond)
	if p.made.Load() != 0 {
		t.Fatal("inner ConnectionMade called after failed handshake")
	}
	select {
	case err := <-p.lost:
		t.Fatalf("inner ConnectionLost(%v) called after failed handshake", err)
	default:
	}
}

func TestHandshakeTimeout(t *testing.T) {
	e := startEngine(t)
	hsErr := make(chan error, 1)
	addr := listen(t, e, serverConfig(t), echoFactory(),
		twtls.HandshakeTimeout(100*time.Millisecond),
		twtls.OnHandshakeError(func(err error) { hsErr <- err }))
	c, err := net.Dial("tcp", addr) // never sends a ClientHello
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case err := <-hsErr:
		if !errors.Is(err, twtls.ErrHandshakeTimeout) {
			t.Fatalf("handshake error %v, want ErrHandshakeTimeout", err)
		}
		var he *twtls.HandshakeError
		if !errors.As(err, &he) || he.Err != twtls.ErrHandshakeTimeout {
			t.Fatalf("handshake error %v (%T), want *HandshakeError{Err: ErrHandshakeTimeout}", err, err)
		}
		if he.Peer == nil || he.Peer.String() != c.LocalAddr().String() {
			t.Fatalf("HandshakeError.Peer = %v, want %v", he.Peer, c.LocalAddr())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handshake did not time out")
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still open after handshake timeout")
	}
}

// logCapture is a slog.Handler that keeps every record.
type logCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.recs = append(h.recs, r.Clone())
	h.mu.Unlock()
	return nil
}
func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }

// find returns the first record with msg and its attributes as strings.
func (h *logCapture) find(msg string) (slog.Record, map[string]string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r.Message == msg {
			attrs := map[string]string{}
			r.Attrs(func(a slog.Attr) bool {
				attrs[a.Key] = a.Value.String()
				return true
			})
			return r, attrs, true
		}
	}
	return slog.Record{}, nil, false
}

// TestHandshakeFailureLoggedWithoutHook: with no OnHandshakeError hook, a
// failed handshake is logged once at Warn with the peer address. The
// handler is installed both as the engine's logger and as slog's default,
// whichever the package can reach.
func TestHandshakeFailureLoggedWithoutHook(t *testing.T) {
	h := &logCapture{}
	lg := slog.New(h)
	prev := slog.Default()
	slog.SetDefault(lg)
	t.Cleanup(func() { slog.SetDefault(prev) })
	e := startEngine(t, twine.Logger(lg))
	addr := listen(t, e, serverConfig(t), echoFactory())
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("GET / HTTP/1.0\r\n\r\n")); err != nil { // not a ClientHello
		t.Fatal(err)
	}
	const msg = "twine/twtls: handshake failed"
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, attrs, ok := h.find(msg)
		if ok {
			if r.Level != slog.LevelWarn {
				t.Fatalf("logged at %v, want Warn", r.Level)
			}
			if attrs["peer"] != c.LocalAddr().String() {
				t.Fatalf("logged peer %q, want %s (attrs %v)", attrs["peer"], c.LocalAddr(), attrs)
			}
			if attrs["err"] == "" {
				t.Fatalf("no err attribute: %v", attrs)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handshake failure not logged")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	h.mu.Lock()
	n := 0
	for _, r := range h.recs {
		if r.Message == msg {
			n++
		}
	}
	h.mu.Unlock()
	if n != 1 {
		t.Fatalf("logged %d times, want once", n)
	}
}

func TestConcurrentClients(t *testing.T) {
	e := startEngine(t)
	addr := listen(t, e, serverConfig(t), echoFactory())
	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr,
				&tls.Config{InsecureSkipVerify: true})
			if err != nil {
				errs <- fmt.Errorf("client %d dial: %w", i, err)
				return
			}
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(30 * time.Second))
			buf := make([]byte, 64)
			for j := 0; j < 10; j++ {
				msg := []byte(fmt.Sprintf("client %03d msg %02d", i, j))
				if _, err := c.Write(msg); err != nil {
					errs <- err
					return
				}
				if _, err := io.ReadFull(c, buf[:len(msg)]); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(buf[:len(msg)], msg) {
					errs <- fmt.Errorf("client %d: got %q", i, buf[:len(msg)])
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// pauser pauses reading on every chunk and resumes shortly after, echoing.
type pauser struct {
	t       twine.Transport
	resumes atomic.Int32
}

func (p *pauser) ConnectionMade(t twine.Transport) { p.t = t }
func (p *pauser) DataReceived(b []byte) {
	p.t.Write(b)
	p.t.PauseReading()
	p.t.Loop().CallLater(time.Millisecond, func() {
		p.resumes.Add(1)
		p.t.ResumeReading()
	})
}
func (p *pauser) ConnectionLost(error) {}

func TestPauseResumeReading(t *testing.T) {
	e := startEngine(t)
	p := &pauser{}
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol { return p })
	c := dial(t, addr, nil)
	defer c.Close()
	data := make([]byte, 512<<10)
	_, _ = rand.Read(data)
	go func() { _, _ = c.Write(data) }()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data corrupted across pause/resume")
	}
	if p.resumes.Load() == 0 {
		t.Fatal("never paused")
	}
}

// panicker panics in DataReceived.
type panicker struct{ lost chan error }

func (p *panicker) ConnectionMade(twine.Transport) {}
func (p *panicker) DataReceived([]byte)            { panic("boom") }
func (p *panicker) ConnectionLost(err error)       { p.lost <- err }

func TestInnerPanic(t *testing.T) {
	e := startEngine(t, twine.Logger(nil))
	lost := make(chan error, 1)
	addr := listen(t, e, serverConfig(t), func(net.Addr) twine.Protocol {
		return &panicker{lost: lost}
	})
	c := dial(t, addr, nil)
	defer c.Close()
	_, _ = c.Write([]byte("x"))
	select {
	case err := <-lost:
		var pe *twine.PanicError
		if !errors.As(err, &pe) {
			t.Fatalf("ConnectionLost(%v), want PanicError", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ConnectionLost")
	}
}

// --- client tests ------------------------------------------------------------

// goEchoServer runs a crypto/tls echo server and returns its address.
func goEchoServer(t testing.TB, cfg *tls.Config) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

// clientProto sends lines and collects the echo.
type clientProto struct {
	t       twine.Transport
	want    int
	got     bytes.Buffer
	done    chan []byte
	state   chan tls.ConnectionState
	made    atomic.Int32
	payload []byte
}

func (p *clientProto) ConnectionMade(t twine.Transport) {
	p.made.Add(1)
	p.t = t
	if p.state != nil {
		p.state <- t.(*twtls.Transport).ConnectionState()
	}
	t.Write(p.payload)
}
func (p *clientProto) DataReceived(b []byte) {
	p.got.Write(b)
	if p.got.Len() >= p.want {
		select {
		case p.done <- bytes.Clone(p.got.Bytes()):
		default:
		}
		p.t.Close()
	}
}
func (p *clientProto) ConnectionLost(error) {}

func TestClientConnect(t *testing.T) {
	cert, pool := selfSigned(t)
	for _, tc := range []struct {
		name string
		ver  uint16
	}{{"TLS12", tls.VersionTLS12}, {"TLS13", tls.VersionTLS13}} {
		t.Run(tc.name, func(t *testing.T) {
			addr := goEchoServer(t, &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{"twine/1"},
				MaxVersion:   tc.ver,
			})
			e := startEngine(t)
			payload := bytes.Repeat([]byte("0123456789abcdef"), 20000) // 320 KB
			p := &clientProto{want: len(payload), done: make(chan []byte, 1),
				state: make(chan tls.ConnectionState, 1), payload: payload}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			// ServerName defaults from addr's host (127.0.0.1, an IP SAN).
			tr, err := twtls.Connect(e.NextLoop(), addr,
				&tls.Config{RootCAs: pool, NextProtos: []string{"twine/1"}},
				func(net.Addr) twine.Protocol { return p }).Await(ctx)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			if _, ok := tr.(*twtls.Transport); !ok {
				t.Fatalf("Connect resolved with %T", tr)
			}
			st := <-p.state
			if st.Version != tc.ver || st.NegotiatedProtocol != "twine/1" {
				t.Fatalf("client state: version=%x alpn=%q", st.Version, st.NegotiatedProtocol)
			}
			select {
			case got := <-p.done:
				if !bytes.Equal(got, payload) {
					t.Fatal("echo mismatch")
				}
			case <-ctx.Done():
				t.Fatal("timeout waiting for echo")
			}
		})
	}
}

func TestClientHandshakeFailure(t *testing.T) {
	cert, pool := selfSigned(t)
	addr := goEchoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	e := startEngine(t)
	hsErr := make(chan error, 1)
	p := &clientProto{done: make(chan []byte, 1), payload: []byte("x")}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := twtls.Connect(e.NextLoop(), addr,
		&tls.Config{RootCAs: pool, ServerName: "wrong.example"},
		func(net.Addr) twine.Protocol { return p },
		twtls.OnHandshakeError(func(err error) { hsErr <- err })).Await(ctx)
	var verr *tls.CertificateVerificationError
	if !errors.As(err, &verr) {
		t.Fatalf("Connect error %v (%T), want CertificateVerificationError", err, err)
	}
	var he *twtls.HandshakeError
	if !errors.As(err, &he) || he.Peer == nil || he.Peer.String() != addr {
		t.Fatalf("Connect error %v (%T), want a *HandshakeError with Peer %s", err, err, addr)
	}
	select {
	case herr := <-hsErr:
		if herr != err {
			t.Fatalf("OnHandshakeError(%v) != Connect error %v", herr, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnHandshakeError not called")
	}
	if p.made.Load() != 0 {
		t.Fatal("inner ConnectionMade called after failed handshake")
	}
}

// TestClientWithConnectTCP: Client used with Loop.ConnectTCP directly. The
// dial Future carries the raw socket transport (it resolves before the
// handshake); the TLS Transport reaches the inner protocol's ConnectionMade.
func TestClientWithConnectTCP(t *testing.T) {
	cert, pool := selfSigned(t)
	addr := goEchoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	e := startEngine(t)
	payload := []byte("over ConnectTCP")
	p := &clientProto{want: len(payload), done: make(chan []byte, 1),
		state: make(chan tls.ConnectionState, 1), payload: payload}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	raw, err := e.NextLoop().ConnectTCP(addr, twtls.Client(&tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
		func(net.Addr) twine.Protocol { return p })).Await(ctx)
	if err != nil {
		t.Fatalf("ConnectTCP: %v", err)
	}
	if _, ok := raw.(*twtls.Transport); ok {
		t.Fatal("ConnectTCP resolved with the TLS Transport; the Client doc says it is the raw socket")
	}
	select {
	case st := <-p.state: // clientProto asserts *twtls.Transport
		if !st.HandshakeComplete {
			t.Fatal("inner ConnectionMade before the handshake completed")
		}
	case <-ctx.Done():
		t.Fatal("no inner ConnectionMade")
	}
	select {
	case got := <-p.done:
		if !bytes.Equal(got, payload) {
			t.Fatalf("echo %q", got)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for echo")
	}
}

// TestServerConfigWithoutCertificate: Server panics on a Config that cannot
// serve a handshake, the one crypto/tls.Listen rejects.
func TestServerConfigWithoutCertificate(t *testing.T) {
	e := startEngine(t)
	for _, cfg := range []*tls.Config{nil, {}, {NextProtos: []string{"h2"}}} {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("Server(%v) did not panic", cfg)
				}
				if err, ok := r.(error); !ok || !strings.Contains(err.Error(), "neither Certificates, GetCertificate, nor GetConfigForClient") {
					t.Fatalf("Server(%v) panicked with %v", cfg, r)
				}
			}()
			twtls.Server(cfg, echoFactory())
		}()
	}
	cert, _ := selfSigned(t)
	for _, cfg := range []*tls.Config{
		{Certificates: []tls.Certificate{cert}},
		{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &cert, nil }},
		{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return serverConfig(t), nil }},
	} {
		addr := listen(t, e, cfg, echoFactory())
		c := dial(t, addr, nil)
		if _, err := c.Write([]byte("ok")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 2)
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ok" {
			t.Fatalf("echo %q %v", buf, err)
		}
		c.Close()
	}
}

func TestClientDialError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	e := startEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = twtls.Dial(e, addr, &tls.Config{InsecureSkipVerify: true}, echoFactory()).Await(ctx)
	if err == nil {
		t.Fatal("Connect to closed port succeeded")
	}
}

// TestTwineToTwine runs both ends in one engine; each handshake, client and
// server, runs on a goroutine of its own.
func TestTwineToTwine(t *testing.T) {
	e := startEngine(t)
	_, pool := selfSigned(t)
	addr := listen(t, e, serverConfig(t), echoFactory())
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("twine client %d", i))
			p := &clientProto{want: len(payload), done: make(chan []byte, 1), payload: payload}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, err := twtls.Connect(e.NextLoop(), addr, &tls.Config{RootCAs: pool, ServerName: "localhost"},
				func(net.Addr) twine.Protocol { return p }).Await(ctx)
			if err != nil {
				t.Errorf("Connect %d: %v", i, err)
				return
			}
			select {
			case got := <-p.done:
				if !bytes.Equal(got, payload) {
					t.Errorf("client %d: got %q", i, got)
				}
			case <-ctx.Done():
				t.Errorf("client %d: timeout", i)
			}
		}(i)
	}
	wg.Wait()
}

// --- interop -----------------------------------------------------------------

func TestOpenSSLClient(t *testing.T) {
	bin, err := exec.LookPath("openssl")
	if err != nil {
		t.Skip("openssl not on PATH")
	}
	e := startEngine(t)
	addr := listen(t, e, serverConfig(t), echoFactory())
	for _, ver := range []string{"-tls1_2", "-tls1_3"} {
		t.Run(ver, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, "s_client", "-connect", addr, "-quiet", ver)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = stdin.Close()
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}()
			const line = "hello from openssl s_client"
			if _, err := io.WriteString(stdin, line+"\n"); err != nil {
				t.Fatal(err)
			}
			got := make(chan string, 1)
			go func() {
				s, _ := bufio.NewReader(stdout).ReadString('\n')
				got <- s
			}()
			select {
			case s := <-got:
				if strings.TrimRight(s, "\r\n") != line {
					t.Fatalf("openssl echo %q; stderr:\n%s", s, stderr.String())
				}
			case <-ctx.Done():
				t.Fatalf("timeout; stderr:\n%s", stderr.String())
			}
		})
	}
}

// TestSilentPeersDoNotStallHandshakes: peers that connect and never send a
// ClientHello must not block other handshakes, even with a one-worker pool.
func TestSilentPeersDoNotStallHandshakes(t *testing.T) {
	e := startEngine(t, twine.WorkerPool(1))
	addr := listen(t, e, serverConfig(t), echoFactory())
	for i := 0; i < 4; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	}
	time.Sleep(20 * time.Millisecond)
	d := &net.Dialer{Timeout: 3 * time.Second, Deadline: time.Now().Add(3 * time.Second)}
	c, err := tls.DialWithDialer(d, "tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("handshake stalled behind silent peers: %v", err)
	}
	c.Close()
}

// reentrantProducer writes from PauseProducing, which the core calls
// synchronously from inside the ciphertext write that tls.Conn.Write makes.
type reentrantProducer struct {
	t      twine.Transport
	pauses atomic.Int32
}

func (p *reentrantProducer) PauseProducing() {
	p.pauses.Add(1)
	p.t.Write([]byte("P"))
}
func (p *reentrantProducer) ResumeProducing() {}
func (p *reentrantProducer) StopProducing()   {}

type producerProto struct {
	prod  *reentrantProducer
	total int
}

func (p *producerProto) ConnectionMade(t twine.Transport) {
	p.prod.t = t
	t.RegisterProducer(p.prod, true)
	chunk := bytes.Repeat([]byte("d"), 64<<10)
	for i := 0; i < p.total/len(chunk); i++ {
		t.Write(chunk)
	}
	t.Close()
}
func (p *producerProto) DataReceived([]byte)  {}
func (p *producerProto) ConnectionLost(error) {}

func TestReentrantWriteFromProducer(t *testing.T) {
	e := startEngine(t, twine.WriteHighWater(64<<10), twine.WriteLowWater(16<<10))
	prod := &reentrantProducer{}
	const total = 2 << 20
	f := twtls.Server(serverConfig(t), func(net.Addr) twine.Protocol {
		return &producerProto{prod: prod, total: total}
	})
	lst, err := e.ListenTCP("127.0.0.1:0", f, twine.SocketBuffers(0, 16<<10))
	if err != nil {
		t.Fatal(err)
	}
	addr := lst.Addr().String()
	c := dial(t, addr, nil)
	defer c.Close()
	time.Sleep(50 * time.Millisecond) // let the ring fill past the high-water mark
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	n := int(prod.pauses.Load())
	if n == 0 {
		t.Skip("producer was never paused; re-entrancy not exercised")
	}
	if len(got) != total+n || bytes.Count(got, []byte("P")) != n {
		t.Fatalf("got %d bytes with %d P, want %d with %d", len(got), bytes.Count(got, []byte("P")), total+n, n)
	}
}

// TestTLSInsideTLS: a TLS server nested inside another, on a one-loop
// engine. The inner connection's drain runs inside the outer connection's
// DataReceived, while the loop's shared plaintext buffer still holds the
// outer plaintext the inner connection is reading from; the inner drain
// must decrypt into a buffer of its own (see loopScratch).
func TestTLSInsideTLS(t *testing.T) {
	e := startEngine(t, twine.Loops(1))
	cfg := serverConfig(t)
	addr := listen(t, e, cfg, twtls.Server(cfg, echoFactory()))
	outer := dial(t, addr, nil)
	defer outer.Close()
	c := tls.Client(outer, &tls.Config{InsecureSkipVerify: true})
	if err := c.Handshake(); err != nil {
		t.Fatalf("inner handshake: %v", err)
	}
	buf := make([]byte, 64)
	for i := 0; i < 200; i++ {
		msg := []byte(fmt.Sprintf("nested message %04d", i))
		if _, err := c.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(c, buf[:len(msg)]); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(buf[:len(msg)], msg) {
			t.Fatalf("echo %d: got %q want %q", i, buf[:len(msg)], msg)
		}
	}
	data := make([]byte, 1<<20)
	_, _ = rand.Read(data)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Write(data)
		errCh <- err
	}()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("stream read: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("stream write: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("nested 1 MiB stream corrupted")
	}
}
