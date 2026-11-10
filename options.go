package twine

import (
	"log/slog"
	"runtime"
	"time"
)

// Option configures an Engine. Options that take a size or a count treat 0
// (and any negative value) as "use the default"; options that take a
// duration say what 0 means.
type Option func(*config)

// ListenOption configures a listener created by ListenTCP or ListenUDP.
// ListenUDP honours only SocketBuffers; the others apply to TCP listeners and
// are ignored for UDP.
type ListenOption func(*listenConfig)

// DialOption configures an outbound connection created by Loop.ConnectTCP
// or Engine.Dial.
type DialOption func(*dialConfig)

// Internal sizes that used to be options. None changes behaviour, only
// batching: the CallFromThread ring spills into an unbounded slice when
// full, and accept and UDP batches end early when the socket runs dry.
const (
	// maxAcceptsPerEvent caps accept(2) calls per listener per iteration.
	maxAcceptsPerEvent = 64
	// taskQueueSize is the capacity of the per-loop lock-free task ring.
	taskQueueSize = 64 << 10
	// udpBatch is the recvmmsg/sendmmsg batch size.
	udpBatch = 64
)

type config struct {
	loops           int
	reusePort       bool
	spinIterations  int // -1: automatic (see SpinIterations)
	workerPool      int
	readBufferSize  int
	writeHighWater  int
	writeLowWater   int
	writeLimit      int
	watchdog        time.Duration
	logger          *slog.Logger
	maxReadsPerEv   int
	shutdownTimeout time.Duration
	closeLinger     time.Duration
	pinCPUs         []int
}

// Defaults for the size and count options.
const (
	defaultSpinIterations = 50
	defaultReadBufferSize = 64 << 10
	defaultWriteHighWater = 256 << 10
	defaultWriteLowWater  = 64 << 10
	defaultWriteLimit     = 16 << 20
	defaultMaxReadsPerEv  = 8
	defaultBacklog        = 4096
)

func defaultConfig() config {
	return config{
		reusePort:       runtime.GOOS == "linux",
		spinIterations:  -1,
		watchdog:        time.Millisecond,
		logger:          slog.Default(),
		shutdownTimeout: 5 * time.Second,
		closeLinger:     10 * time.Second,
	}
}

// orDefault replaces a zero or negative size/count with its default.
func orDefault(v *int, def int) {
	if *v <= 0 {
		*v = def
	}
}

// finalize applies the defaults for zero values and the auto-config rules
// that depend on other fields.
func (c *config) finalize() {
	procs := runtime.GOMAXPROCS(0)
	orDefault(&c.loops, procs)
	orDefault(&c.workerPool, 4*procs)
	orDefault(&c.readBufferSize, defaultReadBufferSize)
	orDefault(&c.writeHighWater, defaultWriteHighWater)
	orDefault(&c.writeLowWater, defaultWriteLowWater)
	orDefault(&c.writeLimit, defaultWriteLimit)
	orDefault(&c.maxReadsPerEv, defaultMaxReadsPerEv)
	if c.spinIterations < 0 {
		// Spin-polling is off by default when loops take more than half of
		// the Ps, because spinning loops would starve the worker pool.
		c.spinIterations = defaultSpinIterations
		if c.loops > procs/2 {
			c.spinIterations = 0
		}
	}
	if c.writeLowWater > c.writeHighWater {
		c.writeLowWater = c.writeHighWater / 4
	}
	if c.watchdog < 0 {
		c.watchdog = 0
	}
	if c.shutdownTimeout < 0 {
		c.shutdownTimeout = 0
	}
	if c.closeLinger < 0 {
		c.closeLinger = 0
	}
	if c.logger == nil {
		c.logger = slog.New(slog.DiscardHandler)
	}
}

// Loops sets the number of event loops. 0 means runtime.GOMAXPROCS(0).
func Loops(n int) Option { return func(c *config) { c.loops = n } }

// ReusePort toggles SO_REUSEPORT TCP listeners (one per loop). Default true
// on Linux, false elsewhere. When off, a single listener is shared and
// accepted connections are handed to loops round-robin. ListenUDP always
// binds one socket without SO_REUSEPORT.
func ReusePort(on bool) Option { return func(c *config) { c.reusePort = on } }

// SpinIterations sets how many zero-timeout polls a loop performs before it
// blocks; 0 blocks immediately. By default (or with a negative n) it is 50,
// or 0 when Loops > GOMAXPROCS/2, because spinning loops would then starve
// the worker pool.
func SpinIterations(n int) Option { return func(c *config) { c.spinIterations = n } }

// WorkerPool sets the size of the goroutine pool backing DeferToThread.
// 0 means the default, 4*GOMAXPROCS.
func WorkerPool(size int) Option { return func(c *config) { c.workerPool = size } }

// ReadBufferSize sets the size of the per-loop read buffer shared by every
// connection on the loop. 0 means the default, 64 KiB.
func ReadBufferSize(n int) Option { return func(c *config) { c.readBufferSize = n } }

// WriteHighWater sets the pending-write level above which a registered
// streaming producer is paused. 0 means the default, 256 KiB.
func WriteHighWater(n int) Option { return func(c *config) { c.writeHighWater = n } }

// WriteLowWater sets the pending-write level at or below which a paused
// streaming producer is resumed. 0 means the default, 64 KiB; a value above
// the high-water mark is lowered to a quarter of it.
func WriteLowWater(n int) Option { return func(c *config) { c.writeLowWater = n } }

// WriteBufferLimit sets the pending-write level above which a connection is
// aborted with ErrWriteBufferFull. 0 means the default, 16 MiB.
func WriteBufferLimit(n int) Option { return func(c *config) { c.writeLimit = n } }

// Watchdog sets the slow-callback threshold: a callback still running after
// threshold is logged with the loop goroutine's stack and counted in
// MetricsSnapshot.SlowCallbacks. Default 1 ms; 0 disables the watchdog.
// While loops are busy it samples every threshold/2 (at least 1 ms); while
// every loop is idle it backs off to one sample every 50 ms, so the first
// slow callback after an idle period is reported only if it runs longer than
// threshold plus the current sampling interval.
func Watchdog(threshold time.Duration) Option { return func(c *config) { c.watchdog = threshold } }

// Logger sets the slog logger. Twine never logs on the hot path. nil
// discards every record. Default slog.Default().
func Logger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// MaxReadsPerEvent caps read(2) calls per connection per loop iteration, so
// one busy connection cannot starve the others. 0 means the default, 8.
func MaxReadsPerEvent(n int) Option { return func(c *config) { c.maxReadsPerEv = n } }

// ShutdownTimeout bounds how long Stop waits for pending writes to flush
// before aborting the remaining connections with ErrShutdown. Default 5s; 0
// aborts them without waiting.
func ShutdownTimeout(d time.Duration) Option { return func(c *config) { c.shutdownTimeout = d } }

// PinLoops pins loop i to cpus[i % len(cpus)] with sched_setaffinity (Linux;
// ignored elsewhere). Pinning loops to distinct physical cores keeps them off
// each other's hyperthreads and away from the worker pool. Every loop is
// always locked to its own OS thread, pinned or not. Default: no pinning.
func PinLoops(cpus ...int) Option {
	return func(c *config) { c.pinCPUs = append([]int(nil), cpus...) }
}

// CloseLinger bounds how long LoseConnection waits, after sending SHUT_WR,
// for the peer to close its side before the connection is closed anyway.
// Default 10s; 0 closes as soon as pending writes are flushed (Twisted's
// behaviour).
func CloseLinger(d time.Duration) Option { return func(c *config) { c.closeLinger = d } }

type listenConfig struct {
	noDelay     bool
	deferAccept time.Duration
	busyPoll    time.Duration
	backlog     int
	rcvBuf      int
	sndBuf      int
}

func defaultListenConfig() listenConfig {
	return listenConfig{noDelay: true, backlog: defaultBacklog}
}

// TCPNoDelay sets TCP_NODELAY on accepted sockets. Default true. TCP only.
func TCPNoDelay(on bool) ListenOption { return func(c *listenConfig) { c.noDelay = on } }

// DeferAccept sets TCP_DEFER_ACCEPT on the listening socket, so a connection
// is accepted only once the client has sent data (Linux only; ignored
// elsewhere). 0, the default, disables it. TCP only.
func DeferAccept(d time.Duration) ListenOption { return func(c *listenConfig) { c.deferAccept = d } }

// BusyPoll sets SO_BUSY_POLL on accepted sockets (Linux only; ignored
// elsewhere). 0, the default, disables it. TCP only.
func BusyPoll(d time.Duration) ListenOption { return func(c *listenConfig) { c.busyPoll = d } }

// Backlog sets the listen(2) backlog. 0 means the default, 4096. TCP only.
func Backlog(n int) ListenOption { return func(c *listenConfig) { c.backlog = n } }

// SocketBuffers sets SO_RCVBUF / SO_SNDBUF on accepted TCP sockets, or on the
// UDP socket. 0 leaves the kernel default.
func SocketBuffers(rcv, snd int) ListenOption {
	return func(c *listenConfig) { c.rcvBuf = rcv; c.sndBuf = snd }
}

type dialConfig struct {
	timeout time.Duration
	noDelay bool
	local   string
}

func defaultDialConfig() dialConfig {
	return dialConfig{timeout: 30 * time.Second, noDelay: true}
}

// DialTimeout bounds the TCP connect of a ConnectTCP attempt (not the name
// lookup that may precede it). Default 30s; 0 means no timeout.
func DialTimeout(d time.Duration) DialOption { return func(c *dialConfig) { c.timeout = d } }

// DialNoDelay sets TCP_NODELAY on the connected socket. Default true.
func DialNoDelay(on bool) DialOption { return func(c *dialConfig) { c.noDelay = on } }

// LocalAddr binds the outbound socket to a local host:port before
// connecting (port 0 picks an ephemeral port).
func LocalAddr(addr string) DialOption { return func(c *dialConfig) { c.local = addr } }
