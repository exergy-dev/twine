package twine

import "sync"

// workerPool is the bounded goroutine pool behind DeferToThread. Workers are
// started lazily up to size. Submissions beyond the buffered queue block the
// submitting goroutine; a loop never blocks because its submissions are
// spilled into an unbounded overflow drained by a helper goroutine.
type workerPool struct {
	size  int
	jobs  chan func()
	mu    sync.Mutex
	alive int
	wg    sync.WaitGroup
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

// submit queues fn. It never blocks when called from a loop thread.
func (p *workerPool) submit(fn func()) {
	p.ensureWorker()
	select {
	case p.jobs <- fn:
		return
	case <-p.done:
		return
	default:
	}
	if onLoopThread() {
		p.spMu.Lock()
		p.spill = append(p.spill, fn)
		start := !p.spOn
		p.spOn = true
		p.spMu.Unlock()
		if start {
			go p.drainSpill()
		}
		return
	}
	select {
	case p.jobs <- fn:
	case <-p.done:
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
		case <-p.done:
			return
		}
	}
}

func (p *workerPool) ensureWorker() {
	p.mu.Lock()
	if p.alive < p.size && (p.alive == 0 || len(p.jobs) > 0) {
		p.alive++
		p.wg.Add(1)
		go p.worker()
	}
	p.mu.Unlock()
}

func (p *workerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case fn := <-p.jobs:
			fn()
		case <-p.done:
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

// loopThreads records which OS threads run loops, so submit can tell whether
// it must not block.
var loopThreads sync.Map // tid -> *Loop

func onLoopThread() bool {
	if !haveThreadID {
		return false
	}
	_, ok := loopThreads.Load(threadID())
	return ok
}
