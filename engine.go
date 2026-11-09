package twine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Engine owns the loops, listeners, worker pool and lifecycle.
type Engine struct {
	cfg   config
	loops []*Loop
	pool  *workerPool

	mu            sync.Mutex
	listeners     []*listener
	shutdownHooks []func()
	state         atomic.Int32
	ran           atomic.Bool   // Run got past its start (ErrRunTwice from then on)
	next          atomic.Uint32 // NextLoop: outbound connections, UDP sockets
	acceptNext    atomic.Uint32 // acceptLoop: single-listener accept hand-off
	stopCh        chan struct{}
	doneCh        chan struct{}
}

const (
	stateNew int32 = iota
	stateRunning
	stateStopping
	stateStopped
)

// New creates an Engine. Loops are created now (so ListenTCP and
// CallFromThread work before Run) but their pollers open in Run.
func New(opts ...Option) *Engine {
	e := &Engine{stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	e.cfg = defaultConfig()
	for _, o := range opts {
		o(&e.cfg)
	}
	e.cfg.finalize()
	e.loops = make([]*Loop, e.cfg.loops)
	for i := range e.loops {
		e.loops[i] = newLoop(e, i)
	}
	e.pool = newWorkerPool(e.cfg.workerPool)
	return e
}

// Loops returns the engine's loops.
func (e *Engine) Loops() []*Loop { return e.loops }

// NextLoop picks a loop round-robin, for outbound connections.
func (e *Engine) NextLoop() *Loop {
	n := e.next.Add(1) - 1
	return e.loops[int(n%uint32(len(e.loops)))]
}

// acceptLoop picks the loop for a connection accepted by a single
// (non-SO_REUSEPORT) listener, round-robin. It has its own counter so that
// accepts and NextLoop do not interleave on one sequence: with a shared
// counter, a process that dials itself advanced it twice per connection and
// sent every Dial to the even loops and every accept to the odd ones.
func (e *Engine) acceptLoop() *Loop {
	n := e.acceptNext.Add(1) - 1
	return e.loops[int(n%uint32(len(e.loops)))]
}

// Metrics returns a snapshot for every loop.
func (e *Engine) Metrics() []MetricsSnapshot {
	out := make([]MetricsSnapshot, len(e.loops))
	for i, l := range e.loops {
		out[i] = l.Metrics()
	}
	return out
}

// OnShutdown registers fn to run once when Stop begins, after listeners are
// closed and before connections are closed. Hooks run on the goroutine that
// called Run, in registration order.
func (e *Engine) OnShutdown(fn func()) {
	e.mu.Lock()
	e.shutdownHooks = append(e.shutdownHooks, fn)
	e.mu.Unlock()
}

// Run starts the loops and blocks until ctx is done or Stop is called. It
// returns nil after a clean shutdown (or at once if Stop was called before
// Run), the error that prevented a loop from starting, or ErrRunTwice if the
// Engine has already run.
func (e *Engine) Run(ctx context.Context) error {
	if !e.state.CompareAndSwap(stateNew, stateRunning) {
		if e.state.Load() == stateStopped && !e.ran.Load() {
			return nil // Stop was called before Run
		}
		return ErrRunTwice
	}
	e.ran.Store(true)
	ready := make(chan error, len(e.loops))
	for _, l := range e.loops {
		go l.run(ready)
	}
	var startErr error
	for range e.loops {
		if err := <-ready; err != nil && startErr == nil {
			startErr = err
		}
	}
	if startErr != nil {
		e.state.Store(stateStopped)
		e.shutdownLoops()
		close(e.doneCh)
		return fmt.Errorf("twine: starting loops: %w", startErr)
	}
	if e.cfg.watchdog > 0 {
		go e.watchdog()
	}

	select {
	case <-ctx.Done():
		e.state.CompareAndSwap(stateRunning, stateStopping)
	case <-e.stopCh:
	}
	e.shutdown()
	return nil
}

// Stop begins graceful shutdown: stop accepting, flush, close. It returns
// immediately; Run returns when every loop has exited. Safe from any
// goroutine, including loop callbacks.
func (e *Engine) Stop() {
	if e.state.CompareAndSwap(stateNew, stateStopped) {
		// Never ran: release listener sockets created before Run.
		e.mu.Lock()
		ls := e.listeners
		e.listeners = nil
		e.mu.Unlock()
		for _, l := range ls {
			_ = l.Close()
		}
		e.pool.close()
		close(e.doneCh)
		return
	}
	if e.state.CompareAndSwap(stateRunning, stateStopping) {
		close(e.stopCh)
	}
}

// Done is closed when Run has returned.
func (e *Engine) Done() <-chan struct{} { return e.doneCh }

// shutdown closes listeners, runs the OnShutdown hooks, then asks every loop
// to LoseConnection its connections, flush for up to ShutdownTimeout and exit.
func (e *Engine) shutdown() {
	e.mu.Lock()
	ls := e.listeners
	e.listeners = nil
	hooks := e.shutdownHooks
	e.mu.Unlock()
	for _, l := range ls {
		_ = l.Close()
	}
	for _, h := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					e.cfg.logger.Error("twine: panic in OnShutdown hook", "panic", r)
				}
			}()
			h()
		}()
	}
	e.shutdownLoops()
	e.pool.close()
	e.state.Store(stateStopped)
	close(e.doneCh)
}

func (e *Engine) shutdownLoops() {
	for _, l := range e.loops {
		if !l.started.Load() {
			continue
		}
		l.CallFromThread(l.beginShutdown)
	}
	for _, l := range e.loops {
		if !l.started.Load() {
			continue
		}
		<-l.exitCh
	}
}

func (e *Engine) stopped() bool { return e.state.Load() >= stateStopping }

// ListenTCP binds addr and starts accepting on every loop (one SO_REUSEPORT
// listener per loop) or, when ReusePort is off, on loop 0 with accepted
// connections handed to loops round-robin. It may be called before Run.
func (e *Engine) ListenTCP(addr string, f Factory, opts ...ListenOption) (Listener, error) {
	if e.stopped() {
		return nil, ErrEngineStopped
	}
	lc := defaultListenConfig()
	for _, o := range opts {
		o(&lc)
	}
	if lc.backlog <= 0 {
		lc.backlog = defaultBacklog
	}
	lst, err := e.newTCPListener(addr, f, lc)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.listeners = append(e.listeners, lst)
	e.mu.Unlock()
	return lst, nil
}

// ListenUDP binds a UDP socket on one loop and delivers datagrams to p.
func (e *Engine) ListenUDP(addr string, p DatagramProtocol, opts ...ListenOption) (Listener, error) {
	if e.stopped() {
		return nil, ErrEngineStopped
	}
	lc := defaultListenConfig()
	for _, o := range opts {
		o(&lc)
	}
	lst, err := e.newUDPListener(addr, p, lc)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.listeners = append(e.listeners, lst)
	e.mu.Unlock()
	return lst, nil
}

// removeListener forgets a closed listener.
func (e *Engine) removeListener(l *listener) {
	e.mu.Lock()
	for i, x := range e.listeners {
		if x == l {
			e.listeners = append(e.listeners[:i], e.listeners[i+1:]...)
			break
		}
	}
	e.mu.Unlock()
}
