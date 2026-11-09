package twine

import (
	"context"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A zero (or negative) size or count option means "default"; before, a zero
// read buffer made every connection see EOF and a zero read cap spun forever.
func TestZeroOptions(t *testing.T) {
	cases := map[string][]Option{
		"ReadBufferSize(0)":   {ReadBufferSize(0)},
		"ReadBufferSize(-1)":  {ReadBufferSize(-1)},
		"MaxReadsPerEvent(0)": {MaxReadsPerEvent(0)},
		"WriteHighWater(0)":   {WriteHighWater(0)},
		"WriteLowWater(0)":    {WriteLowWater(0)},
		"WriteBufferLimit(0)": {WriteBufferLimit(0)},
		"WorkerPool(0)":       {WorkerPool(0)},
		"Loops(0)":            {Loops(0)},
		"SpinIterations(-5)":  {SpinIterations(-5)},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			e := startEngine(t, opts...)
			lst := listen(t, e, echoFactory(), Backlog(0))
			c := dialTCP(t, lst.Addr().String())
			echoOnce(t, c, []byte("zero means default"))
		})
	}
}

func TestOptionDefaults(t *testing.T) {
	e := New(ReadBufferSize(0), MaxReadsPerEvent(-3), WriteBufferLimit(0), WriteHighWater(0),
		WriteLowWater(0), Watchdog(-1), ShutdownTimeout(-1), CloseLinger(-1))
	defer e.Stop()
	c := e.cfg
	if c.readBufferSize != defaultReadBufferSize || len(e.loops[0].rdbuf) != defaultReadBufferSize {
		t.Errorf("readBufferSize = %d", c.readBufferSize)
	}
	if c.maxReadsPerEv != defaultMaxReadsPerEv {
		t.Errorf("maxReadsPerEv = %d", c.maxReadsPerEv)
	}
	if c.writeLimit != defaultWriteLimit || c.writeHighWater != defaultWriteHighWater || c.writeLowWater != defaultWriteLowWater {
		t.Errorf("write marks = %d/%d/%d", c.writeLowWater, c.writeHighWater, c.writeLimit)
	}
	if c.watchdog != 0 || c.shutdownTimeout != 0 || c.closeLinger != 0 {
		t.Errorf("negative durations not clamped: %v %v %v", c.watchdog, c.shutdownTimeout, c.closeLinger)
	}
}

func TestEngineLogger(t *testing.T) {
	e := New()
	defer e.Stop()
	if e.Logger() != slog.Default() {
		t.Error("default Engine.Logger is not slog.Default()")
	}
	l := slog.New(slog.DiscardHandler)
	e2 := New(Logger(l))
	defer e2.Stop()
	if e2.Logger() != l {
		t.Error("Engine.Logger is not the configured logger")
	}
	e3 := New(Logger(nil))
	defer e3.Stop()
	if e3.Logger() == nil || e3.Logger().Enabled(context.Background(), slog.LevelError) {
		t.Error("Logger(nil) does not yield a discarding logger")
	}
}

func TestSpinIterationsAuto(t *testing.T) {
	procs := runtime.GOMAXPROCS(0)
	e := New(Loops(1))
	defer e.Stop()
	want := defaultSpinIterations
	if 1 > procs/2 {
		want = 0
	}
	if got := e.cfg.spinIterations; got != want {
		t.Errorf("auto spin with 1 loop of %d procs = %d, want %d", procs, got, want)
	}
	e2 := New(Loops(procs))
	defer e2.Stop()
	if got := e2.cfg.spinIterations; got != 0 {
		t.Errorf("auto spin with Loops == GOMAXPROCS = %d, want 0", got)
	}
	e3 := New(Loops(procs), SpinIterations(7))
	defer e3.Stop()
	if got := e3.cfg.spinIterations; got != 7 {
		t.Errorf("explicit SpinIterations(7) = %d", got)
	}
}

// DeferAccept sets TCP_DEFER_ACCEPT on Linux, and a client that sends first
// is still served.
func TestDeferAccept(t *testing.T) {
	e := startEngine(t, Loops(1))
	lst := listen(t, e, echoFactory(), DeferAccept(2*time.Second), BusyPoll(time.Microsecond))
	if runtime.GOOS == "linux" {
		fd := lst.(*listener).fds[0]
		v, err := unix.GetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_DEFER_ACCEPT)
		if err != nil {
			t.Fatal(err)
		}
		if v <= 0 {
			t.Errorf("TCP_DEFER_ACCEPT = %d, want > 0", v)
		}
	}
	c := dialTCP(t, lst.Addr().String())
	echoOnce(t, c, []byte("deferred"))
}

// LocalAddr binds the outbound socket before connecting.
func TestLocalAddr(t *testing.T) {
	e := startEngine(t, Loops(1))
	lst := listen(t, e, echoFactory())

	// Find a free local port.
	pl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := pl.Addr().(*net.TCPAddr).Port
	pl.Close()

	p := newClientProto()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr, err := e.Dial(lst.Addr().String(), FactoryFunc(func(net.Addr) Protocol { return p }),
		LocalAddr(net.JoinHostPort("127.0.0.1", itoa(port)))).Await(ctx)
	if err != nil {
		t.Fatalf("Dial with LocalAddr: %v", err)
	}
	var host net.Addr
	onLoop(t, tr.Loop(), func() { host = tr.GetHost(); tr.Abort() })
	ta, ok := host.(*net.TCPAddr)
	if !ok || ta.Port != port || !ta.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("GetHost = %v, want 127.0.0.1:%d", host, port)
	}
}
