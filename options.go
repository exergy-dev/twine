package twine

import (
	"log/slog"
	"runtime"
	"time"
)

// Option configures an Engine.
type Option func(*config)

// ListenOption configures a listener created by ListenTCP or ListenUDP.
type ListenOption func(*listenConfig)

// DialOption configures an outbound connection created by Loop.ConnectTCP.
type DialOption func(*dialConfig)

type config struct {
	loops           int
	reusePort       bool
	spinIterations  int
	spinSet         bool
	workerPool      int
	readBufferSize  int
	writeHighWater  int
	writeLowWater   int
	writeLimit      int
	lockOSThread    bool
	watchdog        time.Duration
	logger          *slog.Logger
	maxReadsPerEv   int
	maxAcceptsPerEv int
	taskQueueSize   int
	shutdownTimeout time.Duration
	busyPoll        time.Duration
	udpBatch        int
	closeLinger     time.Duration
}

func defaultConfig() config {
	n := runtime.GOMAXPROCS(0)
	return config{
		loops:           n,
		reusePort:       runtime.GOOS == "linux",
		spinIterations:  50,
		workerPool:      4 * n,
		readBufferSize:  64 << 10,
		writeHighWater:  256 << 10,
		writeLowWater:   64 << 10,
		writeLimit:      16 << 20,
		lockOSThread:    true,
		watchdog:        time.Millisecond,
		logger:          slog.Default(),
		maxReadsPerEv:   8,
		maxAcceptsPerEv: 64,
		taskQueueSize:   64 << 10,
		shutdownTimeout: 5 * time.Second,
		udpBatch:        64,
		closeLinger:     10 * time.Second,
	}
}

// finalize applies the auto-config rules that depend on other fields.
func (c *config) finalize() {
	if c.loops <= 0 {
		c.loops = runtime.GOMAXPROCS(0)
	}
	// Section 7.6: spin-polling is off by default when loops take more than
	// half of the Ps, because the loops would starve the worker pool.
	if !c.spinSet && c.loops > runtime.GOMAXPROCS(0)/2 {
		c.spinIterations = 0
	}
	if c.workerPool <= 0 {
		c.workerPool = 4 * runtime.GOMAXPROCS(0)
	}
	if c.writeLowWater > c.writeHighWater {
		c.writeLowWater = c.writeHighWater / 4
	}
	if c.logger == nil {
		c.logger = slog.New(discardHandler{})
	}
}

// Loops sets the number of event loops. 0 means runtime.GOMAXPROCS(0).
func Loops(n int) Option { return func(c *config) { c.loops = n } }

// ReusePort toggles SO_REUSEPORT listeners (one per loop). Default true on
// Linux. When off, a single listener is shared and accepted connections are
// handed to loops round-robin.
func ReusePort(on bool) Option { return func(c *config) { c.reusePort = on } }

// SpinIterations sets how many zero-timeout polls a loop performs before it
// blocks. Default 50; 0 blocks immediately. The default is forced to 0 when
// Loops > GOMAXPROCS/2 unless SpinIterations is set explicitly.
func SpinIterations(n int) Option {
	return func(c *config) { c.spinIterations = n; c.spinSet = true }
}

// WorkerPool sets the size of the goroutine pool backing DeferToThread.
// Default 4*GOMAXPROCS.
func WorkerPool(size int) Option { return func(c *config) { c.workerPool = size } }

// ReadBufferSize sets the per-loop shared read buffer. Default 64 KiB.
func ReadBufferSize(n int) Option { return func(c *config) { c.readBufferSize = n } }

// WriteHighWater sets the pending-write level above which a registered
// producer is paused. Default 256 KiB.
func WriteHighWater(n int) Option { return func(c *config) { c.writeHighWater = n } }

// WriteLowWater sets the pending-write level below which a paused producer is
// resumed. Default 64 KiB.
func WriteLowWater(n int) Option { return func(c *config) { c.writeLowWater = n } }

// WriteBufferLimit sets the pending-write level at which a connection is
// aborted with ErrWriteBufferFull. Default 16 MiB.
func WriteBufferLimit(n int) Option { return func(c *config) { c.writeLimit = n } }

// LockOSThread pins each loop goroutine to its own OS thread. Default true.
func LockOSThread(on bool) Option { return func(c *config) { c.lockOSThread = on } }

// Watchdog sets the slow-callback threshold. Default 1ms; 0 disables.
func Watchdog(threshold time.Duration) Option { return func(c *config) { c.watchdog = threshold } }

// Logger sets the slog logger. Twine never logs on the hot path.
func Logger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// MaxReadsPerEvent caps read(2) calls per fd per iteration. Default 8.
func MaxReadsPerEvent(n int) Option { return func(c *config) { c.maxReadsPerEv = n } }

// MaxAcceptsPerEvent caps accept4(2) calls per listener per iteration.
// Default 64.
func MaxAcceptsPerEvent(n int) Option { return func(c *config) { c.maxAcceptsPerEv = n } }

// TaskQueueSize sets the capacity of the per-loop lock-free CallFromThread
// ring. Default 64K. Overflow spills to a mutex-guarded slice.
func TaskQueueSize(n int) Option { return func(c *config) { c.taskQueueSize = n } }

// ShutdownTimeout bounds how long Stop waits for pending writes to flush.
// Default 5s.
func ShutdownTimeout(d time.Duration) Option { return func(c *config) { c.shutdownTimeout = d } }

// CloseLinger bounds how long LoseConnection waits, after sending SHUT_WR,
// for the peer to close its side before the connection is closed anyway.
// Default 10s; 0 closes as soon as pending writes are flushed (Twisted's
// behaviour).
func CloseLinger(d time.Duration) Option { return func(c *config) { c.closeLinger = d } }

// BusyPoll sets SO_BUSY_POLL on accepted sockets (Linux). 0 disables.
func BusyPoll(d time.Duration) Option { return func(c *config) { c.busyPoll = d } }

// UDPBatch sets the recvmmsg/sendmmsg batch size. Default 64.
func UDPBatch(n int) Option { return func(c *config) { c.udpBatch = n } }

type listenConfig struct {
	noDelay     bool
	deferAccept time.Duration
	backlog     int
	rcvBuf      int
	sndBuf      int
}

func defaultListenConfig() listenConfig {
	return listenConfig{noDelay: true, backlog: 4096}
}

// TCPNoDelay sets TCP_NODELAY on accepted sockets. Default true.
func TCPNoDelay(on bool) ListenOption { return func(c *listenConfig) { c.noDelay = on } }

// DeferAccept sets TCP_DEFER_ACCEPT (Linux only; ignored elsewhere).
func DeferAccept(d time.Duration) ListenOption { return func(c *listenConfig) { c.deferAccept = d } }

// Backlog sets the listen(2) backlog. Default 4096.
func Backlog(n int) ListenOption { return func(c *listenConfig) { c.backlog = n } }

// SocketBuffers sets SO_RCVBUF / SO_SNDBUF on accepted sockets. 0 leaves the
// kernel default.
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

// DialTimeout bounds a ConnectTCP attempt. Default 30s; 0 means no timeout.
func DialTimeout(d time.Duration) DialOption { return func(c *dialConfig) { c.timeout = d } }

// DialNoDelay sets TCP_NODELAY on the connected socket. Default true.
func DialNoDelay(on bool) DialOption { return func(c *dialConfig) { c.noDelay = on } }

// LocalAddr binds the outbound socket to a local address before connecting.
func LocalAddr(addr string) DialOption { return func(c *dialConfig) { c.local = addr } }
