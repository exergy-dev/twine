package twine

import (
	"sync"
	"sync/atomic"
)

// workerPool is the bounded goroutine pool behind DeferToThread. Workers are
// started lazily, one whenever a job is submitted and no worker is idle, up
// to size. Submissions beyond the buffered queue block the submitting
// goroutine; a loop never blocks because its submissions spill into an
// unbounded overflow drained by a helper goroutine.
type workerPool struct {
	size  int
	jobs  chan func()
	mu    sync.Mutex
	alive int
	idle  atomic.Int32
	done  chan struct{}
	spill []func()
	spMu  sync.Mutex
	spOn  bool
}

func newWorkerPool(size int) *workerPool {
	if size <= 0 {
		size = 1
	}
	return &workerPool{size: size, jobs: make(chan func(), size*4), done: make(chan struct{})}
}

// submit queues fn. When fromLoop is true it never blocks. It returns false
// once the pool has been closed, in which case fn is dropped.
func (p *workerPool) submit(fn func(), fromLoop bool) bool {
	select {
	case <-p.done:
		return false
	default:
	}
	select {
	case p.jobs <- fn:
		p.ensureWorker()
		return true
	default:
	}
	if fromLoop {
		p.spMu.Lock()
		p.spill = append(p.spill, fn)
		start := !p.spOn
		p.spOn = true
		p.spMu.Unlock()
		if start {
			go p.drainSpill()
		}
		p.ensureWorker()
		return true
	}
	p.ensureWorker()
	select {
	case p.jobs <- fn:
		return true
	case <-p.done:
		return false
	}
}

func (p *workerPool) drainSpill() {
	for {
		p.spMu.Lock()
		if len(p.spill) == 0 {
			p.spOn = false
			p.spMu.Unlock()
			return
		}
		fn := p.spill[0]
		p.spill[0] = nil
		p.spill = p.spill[1:]
		p.spMu.Unlock()
		select {
		case p.jobs <- fn:
			p.ensureWorker()
		case <-p.done:
			return
		}
	}
}

// ensureWorker starts a worker when none is idle and the pool is not full.
// Checking idle after the enqueue means a nested DeferToThread from a busy
// worker always finds (or gets) someone to run it.
func (p *workerPool) ensureWorker() {
	if p.idle.Load() > 0 {
		return
	}
	p.mu.Lock()
	if p.alive < p.size {
		p.alive++
		go p.worker()
	}
	p.mu.Unlock()
}

func (p *workerPool) worker() {
	for {
		p.idle.Add(1)
		select {
		case fn := <-p.jobs:
			p.idle.Add(-1)
			fn()
		case <-p.done:
			p.idle.Add(-1)
			return
		}
	}
}

func (p *workerPool) close() {
	select {
	case <-p.done:
		return
	default:
		close(p.done)
	}
}
