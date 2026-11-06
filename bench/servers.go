// Package bench holds the echo servers and the load generator shared by the
// benchmark harness (harness_test.go) and the standalone server and client
// binaries in the subdirectories (design Section 12).
//
// Every server implements the same byte-for-byte echo so that the twine,
// goroutine-per-connection and gnet numbers are comparable.
package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
	"github.com/robert-malhotra/twine"
	"golang.org/x/sys/unix"
)

// Server is a running echo server.
type Server interface {
	// Addr is the bound address in host:port form.
	Addr() string
	// Conns reports the number of currently open server-side connections.
	Conns() int
	// Close stops the server and closes every connection. It blocks until
	// the server's goroutines have exited.
	Close()
}

// GoroutineBufSize is the per-connection read buffer of the
// goroutine-per-connection server. 16 KiB is a middle ground between the
// 4 KiB a memory-conscious service would pick and io.Copy's 32 KiB.
const GoroutineBufSize = 16 << 10

// ---------------------------------------------------------------------------
// twine

// TwineServer is the twine echo server.
type TwineServer struct {
	// Engine is exposed so benchmarks can read per-loop metrics.
	Engine *twine.Engine
	ln     twine.Listener
	runErr chan error
}

// twineEcho echoes every chunk back. It registers itself as a streaming
// producer so that a client that stops reading pauses the read side instead
// of growing the write ring without bound (Twisted's IPushProducer pattern).
type twineEcho struct{ t twine.Transport }

func (e *twineEcho) ConnectionMade(t twine.Transport) { e.t = t; t.RegisterProducer(e, true) }
func (e *twineEcho) DataReceived(b []byte)            { e.t.Write(b) }
func (e *twineEcho) ConnectionLost(error)             {}
func (e *twineEcho) PauseProducing()                  { e.t.PauseReading() }
func (e *twineEcho) ResumeProducing()                 { e.t.ResumeReading() }
func (e *twineEcho) StopProducing()                   {}

// StartTwine starts a twine echo server on addr with the given number of
// loops (0 means GOMAXPROCS). Extra engine options are appended after the
// harness defaults (watchdog off, short shutdown timeout).
func StartTwine(addr string, loops int, opts ...twine.Option) (*TwineServer, error) {
	return startTwine(addr, loops, twine.FactoryFunc(func(net.Addr) twine.Protocol { return &twineEcho{} }), opts...)
}

// startTwine runs a twine engine serving f on addr.
func startTwine(addr string, loops int, f twine.Factory, opts ...twine.Option) (*TwineServer, error) {
	o := append([]twine.Option{
		twine.Loops(loops),
		twine.Watchdog(0),
		twine.ShutdownTimeout(200 * time.Millisecond),
	}, opts...)
	eng := twine.New(o...)
	ln, err := eng.ListenTCP(addr, f)
	if err != nil {
		return nil, err
	}
	s := &TwineServer{Engine: eng, ln: ln, runErr: make(chan error, 1)}
	go func() { s.runErr <- eng.Run(context.Background()) }()
	// Wait until every loop has registered its listener and is accepting;
	// otherwise a burst of dials can overflow the backlog. ListenTCP queued
	// the registration with CallFromThread and tasks run in FIFO order, so a
	// barrier task completing proves it ran.
	for _, l := range eng.Loops() {
		done := make(chan struct{})
		l.CallFromThread(func() { close(done) })
		select {
		case <-done:
		case err := <-s.runErr:
			return nil, fmt.Errorf("twine engine exited during start: %v", err)
		case <-time.After(10 * time.Second):
			eng.Stop()
			return nil, errors.New("twine loops did not start")
		}
	}
	return s, nil
}

// Addr implements Server.
func (s *TwineServer) Addr() string { return s.ln.Addr().String() }

// Conns implements Server.
func (s *TwineServer) Conns() int {
	n := 0
	for _, l := range s.Engine.Loops() {
		n += int(l.Metrics().Connections)
	}
	return n
}

// Close implements Server. It gives up after 15 s and reports the stall on
// stderr rather than hanging the benchmark run.
func (s *TwineServer) Close() {
	s.Engine.Stop()
	select {
	case <-s.Engine.Done():
	case <-time.After(15 * time.Second):
		fmt.Fprintln(os.Stderr, "bench: twine engine did not stop within 15s (shutdown task not run: lost loop wakeup?)")
	}
}

// ---------------------------------------------------------------------------
// goroutine per connection

// GoroutineServer is the net.Listen + goroutine-per-connection echo server.
type GoroutineServer struct {
	ln    net.Listener
	mu    sync.Mutex
	conns map[net.Conn]struct{}
	n     atomic.Int64
	wg    sync.WaitGroup
}

// StartGoroutine starts a goroutine-per-connection echo server on addr.
func StartGoroutine(addr string) (*GoroutineServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &GoroutineServer{ln: ln, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

func (s *GoroutineServer) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return
		}
		s.mu.Lock()
		if s.conns == nil { // closed
			s.mu.Unlock()
			c.Close()
			return
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.n.Add(1)
		s.wg.Add(1)
		go s.serve(c)
	}
}

func (s *GoroutineServer) serve(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		c.Close()
		s.mu.Lock()
		if s.conns != nil {
			delete(s.conns, c)
		}
		s.mu.Unlock()
		s.n.Add(-1)
	}()
	buf := make([]byte, GoroutineBufSize)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if _, werr := c.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// Addr implements Server.
func (s *GoroutineServer) Addr() string { return s.ln.Addr().String() }

// Conns implements Server.
func (s *GoroutineServer) Conns() int { return int(s.n.Load()) }

// Close implements Server.
func (s *GoroutineServer) Close() {
	s.ln.Close()
	s.mu.Lock()
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()
	for c := range conns {
		c.Close()
	}
	s.wg.Wait()
}

// ---------------------------------------------------------------------------
// gnet

// GnetServer is the gnet v2 echo server.
type GnetServer struct {
	gnet.BuiltinEventEngine
	addr   string
	eng    gnet.Engine
	booted chan struct{}
	n      atomic.Int64
	runErr chan error
}

// OnBoot implements gnet.EventHandler.
func (s *GnetServer) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	close(s.booted)
	return gnet.None
}

// OnOpen implements gnet.EventHandler.
func (s *GnetServer) OnOpen(gnet.Conn) ([]byte, gnet.Action) {
	s.n.Add(1)
	return nil, gnet.None
}

// OnClose implements gnet.EventHandler.
func (s *GnetServer) OnClose(gnet.Conn, error) gnet.Action {
	s.n.Add(-1)
	return gnet.None
}

// OnTraffic implements gnet.EventHandler: the canonical gnet echo.
func (s *GnetServer) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)
	_, _ = c.Write(buf)
	return gnet.None
}

// StartGnet starts a gnet echo server with loops event loops (0 means
// GOMAXPROCS) and SO_REUSEPORT listeners. A port of 0 in addr is resolved to
// a free port first, because with SO_REUSEPORT every gnet loop binds its own
// socket and they must agree on the port.
func StartGnet(addr string, loops int) (*GnetServer, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if port == "0" || port == "" {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		addr = ln.Addr().String()
		ln.Close()
	} else if host == "" {
		addr = "0.0.0.0:" + port
	}
	s := &GnetServer{addr: addr, booted: make(chan struct{}), runErr: make(chan error, 1)}
	opts := []gnet.Option{
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithLogger(quietLogger{}),
	}
	if loops > 0 {
		opts = append(opts, gnet.WithNumEventLoop(loops))
	}
	go func() { s.runErr <- gnet.Run(s, "tcp://"+addr, opts...) }()
	select {
	case <-s.booted:
	case err := <-s.runErr:
		return nil, fmt.Errorf("gnet: %v", err)
	case <-time.After(10 * time.Second):
		return nil, errors.New("gnet did not boot")
	}
	// OnBoot can fire before every loop is polling its listener; probe.
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_, err = c.Write([]byte{1})
			if err == nil {
				var b [1]byte
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				_, err = io.ReadFull(c, b[:])
			}
			c.Close()
			if err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			s.Close()
			return nil, fmt.Errorf("gnet not accepting: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	return s, nil
}

// quietLogger drops gnet's informational logging and keeps errors.
type quietLogger struct{}

func (quietLogger) Debugf(string, ...any) {}
func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Warnf(string, ...any)  {}
func (quietLogger) Errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gnet: "+format+"\n", args...)
}
func (quietLogger) Fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gnet: "+format+"\n", args...)
	os.Exit(1)
}

var _ logging.Logger = quietLogger{}

// Addr implements Server.
func (s *GnetServer) Addr() string { return s.addr }

// Conns implements Server.
func (s *GnetServer) Conns() int { return int(s.n.Load()) }

// Close implements Server.
func (s *GnetServer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.eng.Stop(ctx)
	select {
	case <-s.runErr:
	case <-ctx.Done():
	}
}

// ---------------------------------------------------------------------------
// raw accept (memory baseline)

// RawServer accepts connections with raw syscalls and only stores their file
// descriptors. It never reads. Its per-connection memory is the floor that
// the idle-connection benchmark subtracts: anything above it is the server's
// own cost.
type RawServer struct {
	fd   int
	addr string
	mu   sync.Mutex
	fds  []int
	done chan struct{}
}

// StartRaw starts a RawServer on an IPv4 addr.
func StartRaw(addr string) (*RawServer, error) {
	ta, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	sa := &unix.SockaddrInet4{Port: ta.Port}
	copy(sa.Addr[:], ta.IP.To4())
	if err := unix.Bind(fd, sa); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if err := unix.Listen(fd, 4096); err != nil {
		unix.Close(fd)
		return nil, err
	}
	lsa, err := unix.Getsockname(fd)
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	s := &RawServer{fd: fd, done: make(chan struct{})}
	in4 := lsa.(*unix.SockaddrInet4)
	s.addr = net.JoinHostPort(net.IP(in4.Addr[:]).String(), fmt.Sprint(in4.Port))
	go s.acceptLoop()
	return s, nil
}

func (s *RawServer) acceptLoop() {
	defer close(s.done)
	for {
		nfd, _, err := unix.Accept4(s.fd, unix.SOCK_CLOEXEC)
		if err == unix.EINTR || err == unix.ECONNABORTED {
			continue
		}
		if err != nil {
			return
		}
		s.mu.Lock()
		s.fds = append(s.fds, nfd)
		s.mu.Unlock()
	}
}

// Addr implements Server.
func (s *RawServer) Addr() string { return s.addr }

// Conns implements Server.
func (s *RawServer) Conns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.fds)
}

// Close implements Server.
func (s *RawServer) Close() {
	_ = unix.Shutdown(s.fd, unix.SHUT_RDWR) // wakes the blocked accept
	<-s.done
	unix.Close(s.fd)
	s.mu.Lock()
	for _, fd := range s.fds {
		unix.Close(fd)
	}
	s.fds = nil
	s.mu.Unlock()
}
