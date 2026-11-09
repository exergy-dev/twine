package twine

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/twine/internal/mpsc"
	"github.com/exergy-dev/twine/internal/poll"
	"github.com/exergy-dev/twine/internal/slab"
	"github.com/exergy-dev/twine/internal/wheel"
	"golang.org/x/sys/unix"
)

// maxEvents is the size of the stack-resident event array handed to Wait.
const maxEvents = 256

// maxTasksPerIteration bounds task-queue draining so I/O is never starved.
const maxTasksPerIteration = 1024

// wheelTick is the timing wheel resolution.
const wheelTick = time.Millisecond

// readyEntry is a conn queued for a re-read next iteration together with
// the generation it had when queued, so a conn closed and recycled in between
// is skipped instead of being read as a different connection.
type readyEntry struct {
	c   *conn
	gen uint32
}

// Loop is the reactor: one goroutine, pinned to an OS thread, owning its own
// poller, connections, buffers and timers. Nothing on the hot path is shared
// with other loops.
type Loop struct {
	id  int
	eng *Engine
	cfg *config

	// Cross-thread entry (the one door in).
	tasks          *mpsc.Queue
	wakePending    atomic.Bool
	overflowMu     sync.Mutex
	overflow       []func()
	overflowActive atomic.Bool
	local          []func() // tasks queued by the loop itself
	moreTasks      bool     // drain hit the per-iteration cap

	// Identity.
	tid      atomic.Int64 // kernel thread id of the locked loop thread
	gid      atomic.Int64 // goroutine id (InLoop fallback, watchdog stack filter)
	started  atomic.Bool
	stopped  atomic.Bool
	wakeBusy atomic.Int32 // CallFromThread callers inside Wake

	// Reactor state (loop-affine from here on).
	poller *poll.Poller
	alloc  *slab.Allocator
	wheel  *wheel.Wheel
	epoch  time.Time
	now    time.Time
	nowNs  int64

	rdbuf  []byte
	events [maxEvents]poll.Event
	iovecs [64]unix.Iovec

	conns     []*conn // indexed by fd
	freeConns []*conn
	nconns    int
	dirty     []*conn      // need a flush this iteration
	closing   []*conn      // recycle at end of iteration
	ready     []readyEntry // read cap hit; re-read next iteration
	readyNext []readyEntry

	due     []*Timer // CallLater(0) timers; fire next iteration
	dueNext []*Timer
	// timerSlab hands out Timer handles from a chunk so CallLater performs
	// one allocation per timerSlabSize timers instead of one each. Handles
	// are never reused, so a stale handle can never affect a later timer.
	timerSlab []Timer

	// Shutdown.
	stopping     bool
	stopDeadline int64
	exitCh       chan struct{}

	// Observability.
	metrics LoopMetrics
	cbStart atomic.Int64 // monotonic ns when the current callback began; 0 when idle
	cbProto atomic.Pointer[conn]
}

func newLoop(e *Engine, id int) *Loop {
	l := &Loop{
		id:     id,
		eng:    e,
		cfg:    &e.cfg,
		tasks:  mpsc.New(e.cfg.taskQueueSize),
		exitCh: make(chan struct{}),
		epoch:  time.Now(),
	}
	l.now = l.epoch
	l.alloc = slab.New()
	l.wheel = wheel.New(wheelTick, 0)
	l.rdbuf = make([]byte, e.cfg.readBufferSize)
	l.conns = make([]*conn, 1024)
	return l
}

// ID returns the loop's index within its Engine.
func (l *Loop) ID() int { return l.id }

// Engine returns the owning Engine.
func (l *Loop) Engine() *Engine { return l.eng }

// Now returns the time cached at the start of the current iteration. It is
// cheap and is what timers fire against.
func (l *Loop) Now() time.Time { return l.now }

// Stopped reports whether the loop has exited. CallFromThread after this
// point drops the function (ErrLoopStopped semantics) and counts it in
// LoopMetrics.BacklogDrops.
func (l *Loop) Stopped() bool { return l.stopped.Load() }

// Metrics returns a snapshot of the loop's counters. Safe from any goroutine.
func (l *Loop) Metrics() MetricsSnapshot { return l.metrics.snapshot() }

// InLoop reports whether the caller is running on this loop's thread.
func (l *Loop) InLoop() bool {
	if !l.started.Load() {
		return false
	}
	if haveThreadID {
		return threadID() == l.tid.Load()
	}
	return goid() == l.gid.Load()
}

// CallFromThread schedules fn to run on the loop. It is safe from any
// goroutine and never blocks: overflow beyond TaskQueueSize spills into a
// mutex-guarded slice. Wakeups are coalesced so N concurrent calls cost at
// most one eventfd write. A call made from the loop itself appends to a local
// slice and skips the atomics. After the loop has exited fn is dropped.
func (l *Loop) CallFromThread(fn func()) {
	if l.stopped.Load() {
		l.metrics.BacklogDrops.Add(1)
		return
	}
	if l.InLoop() {
		l.local = append(l.local, fn)
		return
	}
	// While the overflow slice is non-empty every producer appends there so
	// a single producer's tasks keep FIFO order across the spill.
	if l.overflowActive.Load() || !l.tasks.Push(fn) {
		l.overflowMu.Lock()
		l.overflow = append(l.overflow, fn)
		l.overflowActive.Store(true)
		l.overflowMu.Unlock()
	}
	if l.wakePending.CompareAndSwap(false, true) {
		l.wakeBusy.Add(1)
		if l.started.Load() && !l.stopped.Load() {
			l.metrics.Wakeups.Add(1)
			_ = l.poller.Wake()
		}
		l.wakeBusy.Add(-1)
	} else {
		l.metrics.WakeupsCoalesced.Add(1)
	}
}

// CallLater runs fn on the loop after d. It returns a Timer that may be
// stopped or reset from the loop. The Timer handle is single-use: once it has
// fired or Stop has returned true it is recycled and must not be used again.
func (l *Loop) CallLater(d time.Duration, fn func()) *Timer {
	l.assertInLoop()
	t := l.newTimer()
	t.fn = fn
	if d <= 0 {
		// Run on the next iteration rather than inside the current wheel
		// advance, so a callback that re-arms itself with 0 cannot starve
		// I/O (and so timers never fire re-entrantly from CallLater).
		t.due = true
		l.due = append(l.due, t)
		return t
	}
	l.wheel.Add(&t.w, l.monoNow()+int64(d))
	return t
}

// timerSlabSize is the number of Timer handles carved from one allocation.
const timerSlabSize = 256

// newTimer returns a fresh, never-before-used Timer handle.
func (l *Loop) newTimer() *Timer {
	if len(l.timerSlab) == 0 {
		l.timerSlab = make([]Timer, timerSlabSize)
	}
	t := &l.timerSlab[0]
	l.timerSlab = l.timerSlab[1:]
	t.loop = l
	t.w.FnT = fireNode
	return t
}

// takeDue snapshots the zero-delay timers queued so far; fireDue runs them
// later in the same iteration. Splitting the two steps guarantees that a
// CallLater(0) made during this iteration runs on the next one.
func (l *Loop) takeDue() {
	l.dueNext, l.due = l.due, l.dueNext[:0]
}

// fireDue runs the zero-delay timers snapshotted by takeDue.
func (l *Loop) fireDue() {
	for i, t := range l.dueNext {
		l.dueNext[i] = nil
		if !t.due {
			continue // stopped, or moved onto the wheel by Reset
		}
		t.due = false
		l.metrics.TimersFired.Add(1)
		t.fire()
	}
	l.dueNext = l.dueNext[:0]
}

// monoNow returns the cached monotonic nanoseconds since the loop epoch.
// Loop-affine callers use it so that timers are relative to the iteration's
// clock, matching Now().
func (l *Loop) monoNow() int64 { return l.nowNs }

// refreshNow updates the cached clock.
func (l *Loop) refreshNow() {
	l.now = time.Now()
	l.nowNs = int64(l.now.Sub(l.epoch))
}

// run is the loop goroutine body.
func (l *Loop) run(ready chan<- error) {
	// The loop always owns its OS thread: InLoop compares kernel thread ids,
	// and a loop that blocks in poll must not hand its thread to other
	// goroutines.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if n := len(l.cfg.pinCPUs); n > 0 {
		if err := pinToCPU(l.cfg.pinCPUs[l.id%n]); err != nil {
			l.cfg.logger.Warn("twine: pinning loop", "loop", l.id, "cpu", l.cfg.pinCPUs[l.id%n], "err", err)
		}
	}
	p, err := poll.New()
	if err != nil {
		ready <- err
		return
	}
	l.poller = p
	l.tid.Store(threadID())
	l.gid.Store(goid())

	l.refreshNow()
	l.started.Store(true)
	ready <- nil

	// A task may have been queued (and a wakeup requested) before the poller
	// existed; make sure the first Wait does not block on it.
	l.wakePending.Store(true)
	_ = l.poller.Wake()

	l.iterate()

	l.stopped.Store(true)
	l.drainTasksAfterStop()
	// A CallFromThread that observed stopped == false may still be inside
	// Wake; never close the poller under it.
	for l.wakeBusy.Load() != 0 {
		runtime.Gosched()
	}
	// The OS thread returns to the scheduler once we unlock it; forget its
	// id so InLoop never says yes to a stranger.
	l.tid.Store(-1)
	l.gid.Store(-1)
	_ = l.poller.Close()
	close(l.exitCh)
}

// iterate runs the loop until shutdown completes.
func (l *Loop) iterate() {
	spin := l.cfg.spinIterations
	for {
		iterStart := l.nowNs
		l.takeDue()

		// 1. Drain the task queue (bounded).
		l.drainTasks()
		if l.stopping && l.shutdownComplete() {
			return
		}

		// 2. Fire expired timers.
		l.refreshNow()
		l.fireDue()
		fired := l.wheel.Advance(l.nowNs)
		if fired > 0 {
			l.metrics.TimersFired.Add(uint64(fired))
		}

		// 3. Poll: spin with timeout 0, then block until the next timer.
		n := 0
		var err error
		for s := 0; ; s++ {
			n, err = l.poller.Wait(l.events[:], 0)
			if n > 0 || err != nil || s >= spin {
				break
			}
			// A zero-timeout Wait consumes a wakeup without reporting it;
			// stop spinning as soon as a task is queued so CallFromThread
			// latency does not grow with the spin budget.
			if l.tasks.Len() > 0 || l.overflowActive.Load() || len(l.due) > 0 {
				break
			}
		}
		if n == 0 && err == nil {
			// The spin may have consumed a wakeup whose task was already
			// drained; clear the coalescing flag so the next producer wakes
			// us, then re-check the queue before blocking.
			l.wakePending.Store(false)
		}
		if n == 0 && err == nil && !l.hasImmediateWork() {
			timeout := l.pollTimeout()
			pollStart := int64(time.Since(l.epoch))
			n, err = l.poller.Wait(l.events[:], timeout)
			l.refreshNow()
			l.metrics.PollBlockNanos.Add(uint64(l.nowNs - pollStart))
		} else {
			l.refreshNow()
		}
		if err != nil {
			l.cfg.logger.Error("twine: poll failed", "loop", l.id, "err", err)
		}

		// 4. Dispatch I/O.
		for i := 0; i < n; i++ {
			ev := &l.events[i]
			c := l.connByFD(int(ev.FD))
			if c == nil {
				continue
			}
			l.dispatch(c, ev.Flags)
		}
		// Connections whose read cap was hit last iteration.
		if len(l.ready) > 0 {
			l.readyNext, l.ready = l.ready, l.readyNext[:0]
			for _, re := range l.readyNext {
				c := re.c
				if c.gen != re.gen {
					continue // closed and recycled since it was queued
				}
				c.flags &^= flagInReady
				l.handleReady(c)
			}
			clear(l.readyNext)
			l.readyNext = l.readyNext[:0]
		}

		// 5. Flush dirty connections with writev.
		l.flushDirty()

		// 6. Recycle closed connections.
		l.recycleClosed()

		l.metrics.Iterations.Add(1)
		l.metrics.iterHist.observe(l.nowNs-iterStart, l.now.Unix())
		st := l.alloc.Stats()
		l.metrics.slabInUse.Store(int64(st.InUse))
		l.metrics.slabFree.Store(int64(st.Free))

		if l.stopping && l.shutdownComplete() {
			return
		}
	}
}

// handleReady re-runs the read-side handler for a conn whose per-event cap
// was hit last iteration or whose reading was just resumed.
func (l *Loop) handleReady(c *conn) {
	switch c.kind {
	case kindListener:
		l.handleAccept(c)
	case kindUDP:
		l.handleUDPRead(c)
	case kindTCP:
		if c.flags&(flagClosed|flagReadPaused) == 0 {
			l.handleRead(c, false)
		}
	}
}

// hasImmediateWork reports whether the loop should not block in poll.
func (l *Loop) hasImmediateWork() bool {
	// A wakeup that arrived during the zero-timeout spin was already
	// consumed by Wait, so the queue itself must be checked before blocking.
	return l.moreTasks || len(l.local) > 0 || len(l.due) > 0 || len(l.ready) > 0 ||
		len(l.dirty) > 0 || len(l.closing) > 0 || l.tasks.Len() > 0 || l.hasOverflow()
}

func (l *Loop) hasOverflow() bool { return l.overflowActive.Load() }

// pollTimeout returns how long the loop may block: until the next timer, or
// forever.
func (l *Loop) pollTimeout() time.Duration {
	next, ok := l.wheel.Next()
	if l.stopping {
		if !ok || l.stopDeadline < next {
			next, ok = l.stopDeadline, true
		}
	}
	if !ok {
		return -1
	}
	d := time.Duration(next - int64(time.Since(l.epoch)))
	if d <= 0 {
		return 0
	}
	// Round up to the tick: the wheel fires on tick boundaries, so waking
	// early would advance nothing and spin.
	return (d + wheelTick - 1) / wheelTick * wheelTick
}

// drainTasks runs queued CallFromThread functions, at most
// maxTasksPerIteration per call.
func (l *Loop) drainTasks() {
	l.moreTasks = false
	l.wakePending.Store(false)
	budget := maxTasksPerIteration
	// Tasks queued by the loop itself during the previous iteration run
	// first. Drain by index (re-entrant appends land past the snapshot) and
	// keep the backing array so steady-state local calls do not allocate.
	n := len(l.local)
	i := 0
	for ; i < n && budget > 0; i++ {
		fn := l.local[i]
		l.local[i] = nil
		budget--
		l.runTask(fn)
	}
	if i == len(l.local) {
		l.local = l.local[:0]
	} else if i > 0 {
		rest := copy(l.local, l.local[i:])
		clear(l.local[rest:])
		l.local = l.local[:rest]
	}
	for budget > 0 {
		fn, ok := l.tasks.Pop()
		if !ok {
			break
		}
		budget--
		l.runTask(fn)
	}
	if budget > 0 && l.overflowActive.Load() {
		// Overflowed tasks are older than anything pushed to the ring after
		// them, and while overflow is active producers keep appending there,
		// so by the time the ring is empty the overflow slice is the FIFO.
		l.overflowMu.Lock()
		ov := l.overflow
		l.overflow = nil
		if len(ov) == 0 {
			l.overflowActive.Store(false)
		}
		l.overflowMu.Unlock()
		for j, fn := range ov {
			if budget == 0 {
				l.overflowMu.Lock()
				l.overflow = append(ov[j:], l.overflow...)
				l.overflowMu.Unlock()
				break
			}
			budget--
			l.runTask(fn)
		}
	}
	if budget == 0 {
		l.moreTasks = true
	}
}

// runTask runs one task with panic recovery and watchdog bookkeeping.
func (l *Loop) runTask(fn func()) {
	l.metrics.TasksRun.Add(1)
	l.cbStart.Store(l.nowNs)
	defer l.cbStart.Store(0)
	defer l.recoverTask()
	fn()
}

func (l *Loop) recoverTask() {
	if r := recover(); r != nil {
		pe := capturePanic(r)
		l.metrics.Panics.Add(1)
		l.cfg.logger.Error("twine: panic in CallFromThread task", "loop", l.id, "panic", pe.Value, "stack", string(pe.Stack))
	}
}

// drainTasksAfterStop runs whatever was queued while the loop was exiting so
// that Future callbacks and shutdown hooks are not lost silently.
func (l *Loop) drainTasksAfterStop() {
	for {
		l.moreTasks = false
		l.drainTasks()
		if !l.moreTasks && len(l.local) == 0 && l.tasks.Len() == 0 {
			l.overflowMu.Lock()
			empty := len(l.overflow) == 0
			l.overflowMu.Unlock()
			if empty {
				return
			}
		}
	}
}

// connByFD returns the conn registered for fd, or nil.
func (l *Loop) connByFD(fd int) *conn {
	if fd < 0 || fd >= len(l.conns) {
		return nil
	}
	return l.conns[fd]
}

// register stores c in the fd-indexed table.
func (l *Loop) register(c *conn) {
	fd := int(c.fd)
	if fd >= len(l.conns) {
		n := len(l.conns) * 2
		for n <= fd {
			n *= 2
		}
		grown := make([]*conn, n)
		copy(grown, l.conns)
		l.conns = grown
	}
	l.conns[fd] = c
	l.nconns++
}

// unregister removes c from the fd table.
func (l *Loop) unregister(c *conn) {
	fd := int(c.fd)
	if fd >= 0 && fd < len(l.conns) && l.conns[fd] == c {
		l.conns[fd] = nil
		l.nconns--
	}
}

// getConn returns a zeroed conn from the free list or a new one.
func (l *Loop) getConn() *conn {
	if n := len(l.freeConns); n > 0 {
		c := l.freeConns[n-1]
		l.freeConns[n-1] = nil
		l.freeConns = l.freeConns[:n-1]
		c.flags = flagPooled
		return c
	}
	c := &conn{loop: l, flags: flagPooled}
	c.idle.Fn = c.onIdle // method value allocated once per conn, amortised by pooling
	c.ring.Init(l.alloc)
	return c
}

// putConn returns c to the free list after clearing user state.
func (l *Loop) putConn(c *conn) {
	c.reset()
	l.freeConns = append(l.freeConns, c)
}

// dispatch routes one poll event to the right handler.
func (l *Loop) dispatch(c *conn, flags uint32) {
	switch c.kind {
	case kindListener:
		if flags&(poll.Readable|poll.Error|poll.Hangup) != 0 {
			l.handleAccept(c)
		}
		return
	case kindUDP:
		if flags&poll.Readable != 0 {
			l.handleUDPRead(c)
		}
		if flags&poll.Writable != 0 {
			c.markDirty()
		}
		return
	case kindConnecting:
		l.handleConnectReady(c, flags)
		return
	}
	if c.flags&flagClosed != 0 {
		return
	}
	if flags&poll.Error != 0 {
		if err := c.sockErr(); err != nil {
			c.fail(err)
			return
		}
	}
	// Hangup is folded into the read path: read(2) returns 0 (half-close) or
	// an error (reset), which closes with the right ConnectionLost error and
	// never discards data still queued in the socket. While reading is paused
	// the hangup is discovered when the protocol resumes.
	if flags&poll.Hangup != 0 {
		// Edge-triggered polling reports the hangup once; remember it so a read
		// re-armed after the per-event cap or after a pause still reads to EOF.
		c.flags |= flagHangup
	}
	if flags&(poll.Readable|poll.Hangup) != 0 && c.flags&flagReadPaused == 0 {
		l.handleRead(c, flags&poll.Hangup != 0)
		if c.flags&flagClosed != 0 {
			return
		}
	}
	if flags&poll.Writable != 0 {
		if c.ring.Len() > 0 {
			c.markDirty()
		} else if c.flags&flagWantWrite != 0 {
			// Spurious or post-flush edge: drop write interest.
			c.disarmWrite()
			if c.flags&flagClosing != 0 {
				c.finishLose()
			}
		}
	}
}

// handleRead reads until EAGAIN, the buffer is short, or the per-event cap,
// calling DataReceived per chunk with a sub-slice of the shared buffer. When
// the event carried a hangup, a short read is not treated as "drained": the
// FIN behind the data must be read in the same pass because edge-triggered
// polling will not report it again.
func (l *Loop) handleRead(c *conn, hangup bool) {
	hangup = hangup || c.flags&flagHangup != 0
	buf := l.rdbuf
	fd := int(c.fd)
	maxReads := l.cfg.maxReadsPerEv
	i := 0
	for ; i < maxReads; i++ {
		n, err := unix.Read(fd, buf)
		if n > 0 {
			l.metrics.ReadBytes.Add(uint64(n))
			l.metrics.Reads.Add(1)
			if c.idleTimeout > 0 {
				l.wheel.Add(&c.idle, l.nowNs+int64(c.idleTimeout))
			}
			l.callDataReceived(c, buf[:n])
			if c.flags&(flagClosed|flagReadPaused) != 0 {
				return
			}
			if n < len(buf) && !hangup {
				return // short read: socket drained
			}
			continue
		}
		if n == 0 && err == nil {
			c.eof()
			return
		}
		switch err {
		case unix.EAGAIN:
			return
		case unix.EINTR:
			i--
			continue
		case unix.ECONNRESET, unix.EPIPE:
			c.fail(ErrConnectionReset)
			return
		default:
			c.fail(err)
			return
		}
	}
	// Cap hit with the socket possibly still readable: re-arm next iteration.
	c.queueReady()
}

// callDataReceived invokes the protocol with panic recovery and watchdog
// bookkeeping. The defer is open-coded and does not allocate.
func (l *Loop) callDataReceived(c *conn, b []byte) {
	l.cbStart.Store(l.nowNs)
	l.cbProto.Store(c)
	defer l.endCallback()
	defer l.recoverProto(c)
	c.proto.DataReceived(b)
	if debugEnabled {
		poison(b)
	}
}

func (l *Loop) endCallback() {
	l.cbStart.Store(0)
}

// recoverProto turns a protocol panic into ConnectionLost(PanicError) for
// that connection only.
func (l *Loop) recoverProto(c *conn) {
	if r := recover(); r != nil {
		pe := capturePanic(r)
		l.metrics.Panics.Add(1)
		l.cfg.logger.Error("twine: panic in protocol", "loop", l.id, "fd", c.fd, "panic", pe.Value, "stack", string(pe.Stack))
		switch c.kind {
		case kindUDP:
			c.closeUDP() // calls StopProtocol; UDP conns are not pooled
		case kindListener:
			// Nothing to tear down; the listener keeps accepting.
		default:
			c.lostErr = pe
			c.abortNoCallback()
		}
	}
}

// capturePanic builds a PanicError with the current stack.
func capturePanic(r any) *PanicError {
	if pe, ok := r.(*PanicError); ok {
		return pe
	}
	buf := make([]byte, 16<<10)
	n := runtime.Stack(buf, false)
	return &PanicError{Value: r, Stack: buf[:n]}
}

// flushDirty issues one writev per dirty conn and updates write interest and
// producer state. It processes the conns that were dirty when it started;
// conns re-marked during the pass (a producer that refilled the ring) wait
// for the next iteration, so one fast producer cannot monopolise the loop.
func (l *Loop) flushDirty() {
	n := len(l.dirty)
	for i := 0; i < n; i++ {
		c := l.dirty[i]
		l.dirty[i] = nil
		c.flags &^= flagDirty
		if c.flags&flagClosed != 0 {
			continue
		}
		if c.kind == kindUDP {
			l.flushUDP(c)
			continue
		}
		c.flush()
	}
	if len(l.dirty) == n {
		l.dirty = l.dirty[:0]
		return
	}
	rest := copy(l.dirty, l.dirty[n:])
	clear(l.dirty[rest:])
	l.dirty = l.dirty[:rest]
}

// recycleClosed finishes closing connections: ConnectionLost, fd close,
// slab release, conn reuse.
func (l *Loop) recycleClosed() {
	for i := 0; i < len(l.closing); i++ {
		c := l.closing[i]
		l.finishClose(c)
	}
	clear(l.closing)
	l.closing = l.closing[:0]
}

// finishClose completes the close of c. The order is: unregister from the
// poller, close the fd, call ConnectionLost exactly once, release storage.
// It is idempotent: reset keeps flagRecycled set until getConn hands the
// struct out again.
func (l *Loop) finishClose(c *conn) {
	if c.flags&flagRecycled != 0 {
		return
	}
	c.flags |= flagRecycled
	if c.idle.Scheduled() {
		l.wheel.Remove(&c.idle)
	}
	fd := int(c.fd)
	if fd >= 0 {
		_ = l.poller.Del(fd)
		l.unregister(c)
		_ = unix.Close(fd)
		c.fd = -1
	}
	l.metrics.Closes.Add(1)
	l.metrics.Connections.Add(^uint64(0))
	// Twisted order: the producer is stopped before connectionLost.
	if c.producer != nil {
		p := c.producer
		c.producer = nil
		l.safeCall(c, p.StopProducing)
	}
	if c.proto != nil && c.flags&flagLostCalled == 0 {
		c.flags |= flagLostCalled
		l.callConnectionLost(c)
	}
	c.ring.Reset()
	if c.flags&flagPooled != 0 {
		l.putConn(c)
	}
}

func (l *Loop) callConnectionLost(c *conn) {
	l.cbStart.Store(l.nowNs)
	l.cbProto.Store(c)
	defer l.endCallback()
	defer l.recoverLost(c)
	c.proto.ConnectionLost(c.lostErr)
}

func (l *Loop) recoverLost(c *conn) {
	if r := recover(); r != nil {
		pe := capturePanic(r)
		l.metrics.Panics.Add(1)
		l.cfg.logger.Error("twine: panic in ConnectionLost", "loop", l.id, "fd", c.fd, "panic", pe.Value)
	}
}

// safeCall runs fn with panic recovery attributed to c.
func (l *Loop) safeCall(c *conn, fn func()) {
	l.cbStart.Store(l.nowNs)
	defer l.endCallback()
	defer l.recoverProto(c)
	fn()
}

// beginShutdown starts graceful shutdown on this loop (called via
// CallFromThread by the Engine).
func (l *Loop) beginShutdown() {
	if l.stopping {
		return
	}
	l.stopping = true
	l.stopDeadline = l.nowNs + int64(l.cfg.shutdownTimeout)
	for _, c := range l.conns {
		if c == nil {
			continue
		}
		switch c.kind {
		case kindListener:
			c.closeListenerFD()
		case kindUDP:
			c.closeUDP()
		case kindConnecting:
			c.rejectConnect(ErrEngineStopped)
		default:
			if c.flags&flagClosed == 0 {
				c.loseConnection()
			}
		}
	}
}

// shutdownComplete reports whether every connection is gone or the deadline
// passed (in which case remaining conns are aborted).
func (l *Loop) shutdownComplete() bool {
	if l.nconns == 0 && len(l.closing) == 0 {
		return true
	}
	if l.nowNs >= l.stopDeadline {
		for _, c := range l.conns {
			if c != nil && c.flags&flagClosed == 0 {
				c.lostErr = ErrShutdown
				c.abortNoCallback()
			}
		}
		l.recycleClosed()
		return true
	}
	return false
}
