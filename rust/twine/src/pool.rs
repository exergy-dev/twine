//! The worker pool behind `Loop::defer_to_thread` (design Sections 6.3 and 8
//! rule 4; Go reference `worker.go`).
//!
//! Threads start lazily, one whenever a job is queued and no worker is idle, up
//! to the pool size, and are named `twine-worker-N`. The submission queue is
//! bounded (`4 * size`). A submission from a loop thread or from another pool
//! worker never blocks: when the queue is full it spills into an unbounded
//! overflow list drained by a helper thread. Any other thread blocks until
//! there is room.
//!
//! Pool ownership: one pool per engine, held in `EngineInner::pool` and created
//! lazily by the first `defer_to_thread` on that engine. `EngineHandle`'s
//! shutdown closes it before the loops stop: queued jobs are cancelled (their
//! Deferreds reject with `Error::EngineStopped`), running jobs finish on their
//! detached threads. Idle workers also exit after [`IDLE_EXIT`].

use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use crate::engine::EngineHandle;

/// A unit of work. `true` runs it, `false` cancels it (pool closed).
pub(crate) type Job = Box<dyn FnOnce(bool) + Send>;

/// Idle workers exit after this long without work.
const IDLE_EXIT: Duration = Duration::from_secs(30);

thread_local! {
    /// True on pool worker threads (their submissions must not block).
    static IS_WORKER: Cell<bool> = const { Cell::new(false) };
}

struct State {
    queue: VecDeque<Job>,
    spill: VecDeque<Job>,
    workers: usize,
    idle: usize,
    spawned: usize,
    drainer: bool,
    closed: bool,
}

/// The bounded worker pool.
pub(crate) struct Pool {
    size: usize,
    cap: usize,
    st: Mutex<State>,
    not_empty: Condvar,
    not_full: Condvar,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Pool {
    pub(crate) fn new(size: usize) -> Arc<Pool> {
        let size = size.max(1);
        Arc::new(Pool {
            size,
            cap: size * 4,
            st: Mutex::new(State {
                queue: VecDeque::new(),
                spill: VecDeque::new(),
                workers: 0,
                idle: 0,
                spawned: 0,
                drainer: false,
                closed: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        })
    }

    /// Queues `job`. `from_loop` submissions never block. Returns the job back
    /// if the pool is closed.
    pub(crate) fn submit(self: &Arc<Pool>, job: Job, from_loop: bool) -> Result<(), Job> {
        let from_loop = from_loop || IS_WORKER.with(|w| w.get());
        let mut g = lock(&self.st);
        if g.closed {
            return Err(job);
        }
        if g.queue.len() < self.cap && g.spill.is_empty() {
            g.queue.push_back(job);
        } else if from_loop {
            g.spill.push_back(job);
            if !g.drainer {
                g.drainer = true;
                let p = self.clone();
                if std::thread::Builder::new()
                    .name("twine-worker-spill".into())
                    .spawn(move || p.drain_spill())
                    .is_err()
                {
                    g.drainer = false;
                    log::error!("twine: cannot spawn spill thread");
                }
            }
        } else {
            self.ensure_worker(&mut g);
            while g.queue.len() >= self.cap && !g.closed {
                g = self.not_full.wait(g).unwrap_or_else(|e| e.into_inner());
            }
            if g.closed {
                return Err(job);
            }
            g.queue.push_back(job);
        }
        self.ensure_worker(&mut g);
        drop(g);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Starts a worker when more jobs are queued than workers are idle.
    fn ensure_worker(self: &Arc<Pool>, g: &mut State) {
        if g.workers >= self.size || g.queue.len() <= g.idle {
            return;
        }
        g.workers += 1;
        g.spawned += 1;
        let n = g.spawned;
        let p = self.clone();
        let r = std::thread::Builder::new()
            .name(format!("twine-worker-{n}"))
            .spawn(move || p.worker());
        if let Err(e) = r {
            g.workers -= 1;
            log::error!("twine: cannot spawn worker thread: {e}");
        }
    }

    fn worker(self: Arc<Pool>) {
        IS_WORKER.with(|w| w.set(true));
        let mut g = lock(&self.st);
        loop {
            if let Some(job) = g.queue.pop_front() {
                drop(g);
                self.not_full.notify_all();
                job(true);
                g = lock(&self.st);
                continue;
            }
            if g.closed {
                break;
            }
            g.idle += 1;
            let (ng, to) = self
                .not_empty
                .wait_timeout(g, IDLE_EXIT)
                .unwrap_or_else(|e| e.into_inner());
            g = ng;
            g.idle -= 1;
            if to.timed_out() && g.queue.is_empty() {
                break;
            }
        }
        g.workers -= 1;
    }

    /// Moves overflow jobs into the queue as room appears.
    fn drain_spill(self: Arc<Pool>) {
        let mut g = lock(&self.st);
        loop {
            if g.spill.is_empty() {
                g.drainer = false;
                return;
            }
            while g.queue.len() >= self.cap && !g.closed {
                g = self.not_full.wait(g).unwrap_or_else(|e| e.into_inner());
            }
            if g.closed {
                g.drainer = false;
                return; // `shutdown` took the overflow list
            }
            if let Some(j) = g.spill.pop_front() {
                g.queue.push_back(j);
            }
            self.ensure_worker(&mut g);
            self.not_empty.notify_one();
        }
    }

    /// Closes the pool and cancels everything not yet started.
    pub(crate) fn shutdown(&self) {
        let jobs: Vec<Job> = {
            let mut g = lock(&self.st);
            g.closed = true;
            let mut v: Vec<Job> = g.queue.drain(..).collect();
            v.extend(g.spill.drain(..));
            v
        };
        self.not_empty.notify_all();
        self.not_full.notify_all();
        for j in jobs {
            j(false);
        }
    }
}

/// The pool for `engine`, created (sized by `Options::worker_pool`) on first
/// use. A pool requested once shutdown has begun is returned already closed, so
/// submissions are cancelled instead of starting threads.
pub(crate) fn pool_for(engine: &EngineHandle) -> Arc<Pool> {
    let p = engine
        .0
        .pool
        .get_or_init(|| Pool::new(engine.0.opts.worker_pool))
        .clone();
    if engine.is_stopping_or_stopped() {
        p.shutdown();
    }
    p
}
