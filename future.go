package twine

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Future is a single-assignment result whose callbacks run on its Loop. It
// replaces Twisted's Deferred. Then/Catch/Finally may be called from any
// goroutine; the callbacks themselves always run on the Loop, in registration
// order, never concurrently with loop code.
type Future[T any] struct {
	loop *Loop

	mu       sync.Mutex
	state    atomic.Int32 // 0 pending, 1 resolved, 2 rejected
	val      T
	err      error
	thens    []func(T)
	catches  []func(error)
	finallys []func()
	handled  bool // a Catch was registered
	waiters  chan struct{}

	dbg *futureDebug // creation stack and settle flag in debug mode
}

const (
	futPending int32 = iota
	futResolved
	futRejected
)

// Resolver settles a Future. Resolve and Reject are idempotent: the second
// call is a no-op.
type Resolver[T any] interface {
	Resolve(v T)
	Reject(err error)
}

// NewFuture creates a pending Future on l and its Resolver.
func NewFuture[T any](l *Loop) (*Future[T], Resolver[T]) {
	f := &Future[T]{loop: l}
	if debugFutures {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		// The cleanup argument must not be reachable from f, or f is never
		// collected; dbg points only at the loop, not at the Future.
		f.dbg = &futureDebug{stack: buf[:n], loop: l}
		runtime.AddCleanup(f, futureCleanup, f.dbg)
	}
	return f, (*resolver[T])(f)
}

type futureDebug struct {
	stack   []byte
	settled atomic.Bool
	loop    *Loop
}

func futureCleanup(d *futureDebug) {
	if !d.settled.Load() && d.loop != nil {
		d.loop.cfg.logger.Warn("twine: Future garbage-collected without resolving", "loop", d.loop.id, "created", string(d.stack))
	}
}

func (f *Future[T]) debugStack() string {
	if f.dbg == nil {
		return ""
	}
	return string(f.dbg.stack)
}

type resolver[T any] Future[T]

func (r *resolver[T]) Resolve(v T)      { (*Future[T])(r).settle(v, nil, futResolved) }
func (r *resolver[T]) Reject(err error) { var zero T; (*Future[T])(r).settle(zero, err, futRejected) }

// settle records the outcome and schedules callbacks on the loop. The first
// call wins; later calls are ignored.
func (f *Future[T]) settle(v T, err error, st int32) {
	f.mu.Lock()
	if f.state.Load() != futPending {
		f.mu.Unlock()
		return
	}
	f.val, f.err = v, err
	f.state.Store(st)
	if f.dbg != nil {
		f.dbg.settled.Store(true)
	}
	w := f.waiters
	f.mu.Unlock()
	if w != nil {
		close(w)
	}
	if f.loop == nil || f.loop.InLoop() {
		f.dispatch()
		return
	}
	f.loop.CallFromThread(f.dispatch)
}

// dispatch runs the registered callbacks on the loop and clears them.
func (f *Future[T]) dispatch() {
	f.mu.Lock()
	thens, catches, finallys := f.thens, f.catches, f.finallys
	f.thens, f.catches, f.finallys = nil, nil, nil
	st, val, err, handled := f.state.Load(), f.val, f.err, f.handled
	f.mu.Unlock()

	if st == futResolved {
		for _, fn := range thens {
			if pe := f.guard(func() { fn(val) }); pe != nil {
				// A panicking callback rejects the Future for later observers.
				f.mu.Lock()
				f.state.Store(futRejected)
				f.err = pe
				var zero T
				f.val = zero
				f.mu.Unlock()
				st, err = futRejected, pe
				break
			}
		}
	}
	if st == futRejected {
		for _, fn := range catches {
			_ = f.guard(func() { fn(err) })
		}
		if !handled && len(catches) == 0 && debugFutures && f.loop != nil {
			f.loop.cfg.logger.Warn("twine: unhandled Future rejection", "loop", f.loop.id, "err", err, "created", f.debugStack())
		}
	}
	for _, fn := range finallys {
		_ = f.guard(fn)
	}
}

// guard runs fn with panic recovery and returns the PanicError if any.
func (f *Future[T]) guard(fn func()) (pe *PanicError) {
	defer func() {
		if r := recover(); r != nil {
			pe = capturePanic(r)
			if f.loop != nil {
				f.loop.metrics.Panics.Add(1)
				f.loop.cfg.logger.Error("twine: panic in Future callback", "loop", f.loop.id, "panic", pe.Value)
			}
		}
	}()
	fn()
	return nil
}

// addCallback registers a callback or, when already settled, schedules an
// immediate dispatch on the loop.
func (f *Future[T]) addCallback(then func(T), catch func(error), finally func()) {
	f.mu.Lock()
	if then != nil {
		f.thens = append(f.thens, then)
	}
	if catch != nil {
		f.catches = append(f.catches, catch)
		f.handled = true
	}
	if finally != nil {
		f.finallys = append(f.finallys, finally)
	}
	settled := f.state.Load() != futPending
	f.mu.Unlock()
	if settled {
		if f.loop == nil || f.loop.InLoop() {
			f.dispatch()
		} else {
			f.loop.CallFromThread(f.dispatch)
		}
	}
}

// Then registers fn to run on the loop with the value. It returns f for
// chaining (Twisted semantics; use Map to transform).
func (f *Future[T]) Then(fn func(T)) *Future[T] { f.addCallback(fn, nil, nil); return f }

// Catch registers fn to run on the loop with the error.
func (f *Future[T]) Catch(fn func(error)) *Future[T] { f.addCallback(nil, fn, nil); return f }

// Finally registers fn to run on the loop after resolution or rejection.
func (f *Future[T]) Finally(fn func()) *Future[T] { f.addCallback(nil, nil, fn); return f }

// Done reports whether the Future is settled.
func (f *Future[T]) Done() bool { return f.state.Load() != futPending }

// Loop returns the loop callbacks run on.
func (f *Future[T]) Loop() *Loop { return f.loop }

// Result returns the outcome once Done; the zero value and nil before.
func (f *Future[T]) Result() (T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.val, f.err
}

// Await blocks the calling goroutine until the Future settles or ctx is
// done. It panics with ErrAwaitOnLoop when called on the Future's own loop,
// since that would deadlock the loop.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	if f.loop != nil && f.loop.InLoop() {
		panic(ErrAwaitOnLoop)
	}
	f.mu.Lock()
	if f.state.Load() != futPending {
		v, err := f.val, f.err
		f.mu.Unlock()
		return v, err
	}
	if f.waiters == nil {
		f.waiters = make(chan struct{})
	}
	w := f.waiters
	f.mu.Unlock()
	select {
	case <-w:
		f.mu.Lock()
		v, err := f.val, f.err
		f.mu.Unlock()
		return v, err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Resolved returns an already-resolved Future on l.
func Resolved[T any](l *Loop, v T) *Future[T] {
	f, r := NewFuture[T](l)
	r.Resolve(v)
	return f
}

// Rejected returns an already-rejected Future on l.
func Rejected[T any](l *Loop, err error) *Future[T] {
	f, r := NewFuture[T](l)
	r.Reject(err)
	return f
}

// Map transforms a Future's value into a new Future on the same loop. An
// error from fn, or a rejection of f, rejects the result.
func Map[T, U any](f *Future[T], fn func(T) (U, error)) *Future[U] {
	out, r := NewFuture[U](f.loop)
	f.Then(func(v T) {
		u, err := fn(v)
		if err != nil {
			r.Reject(err)
			return
		}
		r.Resolve(u)
	}).Catch(r.Reject)
	return out
}

// All resolves with every value, in input order, when every input resolves;
// it rejects on the first rejection.
func All[T any](l *Loop, fs ...*Future[T]) *Future[[]T] {
	out, r := NewFuture[[]T](l)
	if len(fs) == 0 {
		r.Resolve([]T{})
		return out
	}
	vals := make([]T, len(fs))
	var remaining atomic.Int32
	remaining.Store(int32(len(fs)))
	for i, f := range fs {
		i := i
		f.Then(func(v T) {
			vals[i] = v
			if remaining.Add(-1) == 0 {
				r.Resolve(vals)
			}
		}).Catch(r.Reject)
	}
	return out
}

// Any resolves with the first value; it rejects only if every input rejects,
// with the last error.
func Any[T any](l *Loop, fs ...*Future[T]) *Future[T] {
	out, r := NewFuture[T](l)
	if len(fs) == 0 {
		r.Reject(ErrLoopStopped)
		return out
	}
	var remaining atomic.Int32
	remaining.Store(int32(len(fs)))
	for _, f := range fs {
		f.Then(r.Resolve).Catch(func(err error) {
			if remaining.Add(-1) == 0 {
				r.Reject(err)
			}
		})
	}
	return out
}

// Sleep is CallLater as a Future. It must be called on l.
func Sleep(l *Loop, d time.Duration) *Future[struct{}] {
	out, r := NewFuture[struct{}](l)
	l.CallLater(d, func() { r.Resolve(struct{}{}) })
	return out
}

// DeferToThread runs fn on the engine's worker pool and resolves the Future
// on l. A loop calling DeferToThread never blocks; an off-loop caller blocks
// only when every worker is busy and the submission queue is full.
func DeferToThread[T any](l *Loop, fn func() (T, error)) *Future[T] {
	out, r := NewFuture[T](l)
	l.eng.pool.submit(func() {
		defer func() {
			if rec := recover(); rec != nil {
				r.Reject(capturePanic(rec))
			}
		}()
		v, err := fn()
		if err != nil {
			r.Reject(err)
			return
		}
		r.Resolve(v)
	})
	return out
}
