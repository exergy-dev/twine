//! The reactor: [`Loop`], its thread body, the iteration, the cross-thread door
//! ([`LoopHandle`]) and the post-callback queue (design Sections 4, 7 and 8).

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{
    fence, AtomicBool,
    Ordering::{Acquire, Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use twine_poll::{Event, Poller};
use twine_wheel::Wheel;

use crate::conn::{flags, ConnSlot, Kind, SlotEntry, Table};
use crate::engine::EngineHandle;
use crate::errors::{Error, PanicError, Result};
use crate::metrics::{self, LoopMetrics, MetricsSnapshot};
use crate::options::Options;
use crate::protocol::Protocol;
use crate::timer::TimerKind;
use crate::transport::{ConnOp, Transport};

pub use twine_wheel::TimerId;

/// Maximum number of `call_from_thread` tasks run per iteration.
pub(crate) const MAX_TASKS_PER_ITERATION: usize = 1024;
/// Maximum number of post-callback items run per `drain_pending` call.
pub(crate) const MAX_PENDING_PER_DRAIN: usize = 1024;
/// Timing wheel resolution.
pub(crate) const WHEEL_TICK: Duration = Duration::from_millis(1);
const WHEEL_TICK_NS: u64 = 1_000_000;
const MAX_EVENTS: usize = twine_poll::MAX_EVENTS;

thread_local! {
    /// Address of the `Remote` of the loop running on this thread (0 = none).
    static CURRENT_LOOP: Cell<usize> = const { Cell::new(0) };
    /// The post-callback queue (design Section 8 rule 2).
    static PENDING: RefCell<VecDeque<Pending>> = const { RefCell::new(VecDeque::new()) };
}

/// Identifies a connection on one loop. Generation-checked: stale ids are inert.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ConnId {
    pub(crate) fd: u32,
    pub(crate) gen: u32,
    pub(crate) loop_id: u16,
}

impl ConnId {
    /// The index of the loop that owns this connection.
    pub fn loop_id(&self) -> usize {
        self.loop_id as usize
    }
}

/// A key for per-loop user state, created with `Engine::loop_local`. `Send + Sync + Copy`.
pub struct LoopLocal<T: 'static> {
    pub(crate) idx: usize,
    pub(crate) _p: PhantomData<fn() -> T>,
}

impl<T: 'static> Clone for LoopLocal<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: 'static> Copy for LoopLocal<T> {}

/// A queued unit of work for the loop thread, from another thread.
pub(crate) enum Task {
    Call(Box<dyn FnOnce(&mut Loop) + Send>),
    Op(ConnId, ConnOp),
    Shutdown,
}

/// A queued unit of user code that runs after the current top-level callback.
pub(crate) enum Pending {
    Call(Box<dyn FnOnce(&mut Loop)>),
    Notify(ConnId, Notify),
    Op(ConnId, ConnOp),
}

/// Producer notifications (delivered from the post-callback queue).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Notify {
    Pause,
    Resume,
    Stop,
}

/// True when the calling thread is some loop's thread.
pub(crate) fn on_any_loop_thread() -> bool {
    CURRENT_LOOP.with(|c| c.get()) != 0
}

pub(crate) fn pending_push(p: Pending) {
    PENDING.with(|q| q.borrow_mut().push_back(p));
}

fn pending_pop() -> Option<Pending> {
    PENDING.with(|q| q.borrow_mut().pop_front())
}

fn pending_is_empty() -> bool {
    PENDING.with(|q| q.borrow().is_empty())
}

/// The cross-thread side of a loop.
pub(crate) struct Remote {
    pub(crate) id: usize,
    pub(crate) tx: twine_mpsc::Producer<Task>,
    pub(crate) overflow: Mutex<Vec<Task>>,
    pub(crate) overflow_active: AtomicBool,
    pub(crate) wake_pending: AtomicBool,
    pub(crate) waker: OnceLock<twine_poll::Waker>,
    pub(crate) started: AtomicBool,
    pub(crate) stopped: AtomicBool,
    pub(crate) metrics: Arc<LoopMetrics>,
}

/// `Clone + Send + Sync`. The one door in.
#[derive(Clone)]
pub struct LoopHandle(pub(crate) Arc<Remote>);

impl LoopHandle {
    pub(crate) fn new(
        id: usize,
        tx: twine_mpsc::Producer<Task>,
        metrics: Arc<LoopMetrics>,
    ) -> LoopHandle {
        LoopHandle(Arc::new(Remote {
            id,
            tx,
            overflow: Mutex::new(Vec::new()),
            overflow_active: AtomicBool::new(false),
            wake_pending: AtomicBool::new(false),
            waker: OnceLock::new(),
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            metrics,
        }))
    }

    /// The loop's index within its engine.
    pub fn id(&self) -> usize {
        self.0.id
    }

    /// True when called on this loop's thread (thread-local check).
    pub fn in_loop(&self) -> bool {
        CURRENT_LOOP.with(|c| c.get()) == Arc::as_ptr(&self.0) as usize
    }

    /// Schedules `f` on the loop. Never blocks; overflow spills to a
    /// mutex-guarded vector. Wakeups are coalesced: once the loop is running,
    /// every call from another thread increments exactly one of `wakeups` (it
    /// won the coalescing flag and issued the wake syscall) or
    /// `wakeups_coalesced`. A call made before the loop starts that wins the
    /// flag increments neither (there is nothing to wake: the loop drains the
    /// queue before its first poll); one that loses it counts as coalesced. A call from the loop's own thread joins the
    /// post-callback queue and touches no atomics. `Err(LoopStopped)` after the
    /// loop has exited (counted in `backlog_drops`).
    pub fn call_from_thread(&self, f: impl FnOnce(&mut Loop) + Send + 'static) -> Result<()> {
        self.submit_task(Task::Call(Box::new(f)))
    }

    /// A snapshot of this loop's counters.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.0.metrics.snapshot()
    }

    /// True once the loop thread has exited.
    pub fn is_stopped(&self) -> bool {
        self.0.stopped.load(Acquire)
    }

    pub(crate) fn submit_task(&self, task: Task) -> Result<()> {
        let r = &*self.0;
        if r.stopped.load(Acquire) {
            r.metrics.backlog_drops.fetch_add(1, Relaxed);
            return Err(Error::LoopStopped);
        }
        if self.in_loop() {
            pending_push(match task {
                Task::Call(f) => Pending::Call(f),
                Task::Op(id, op) => Pending::Op(id, op),
                Task::Shutdown => Pending::Call(Box::new(|lp: &mut Loop| lp.begin_shutdown())),
            });
            return Ok(());
        }
        // While the overflow vector is non-empty every producer appends there so
        // a single producer's tasks keep FIFO order across the spill.
        let spill = if r.overflow_active.load(Acquire) {
            Some(task)
        } else {
            r.tx.push(task).err()
        };
        if let Some(t) = spill {
            let mut g = r.overflow.lock().unwrap_or_else(|e| e.into_inner());
            g.push(t);
            r.overflow_active.store(true, SeqCst);
        }
        // The loop's final drain may already have run: the task would be lost
        // silently, so report it (it is dropped with the queue).
        if r.stopped.load(SeqCst) {
            r.metrics.backlog_drops.fetch_add(1, Relaxed);
            return Err(Error::LoopStopped);
        }
        if r.wake_pending
            .compare_exchange(false, true, SeqCst, SeqCst)
            .is_ok()
        {
            // Before the loop thread exists there is nothing to wake: it drains the
            // queue before its first poll.
            if let Some(w) = r.waker.get() {
                r.metrics.wakeups.fetch_add(1, Relaxed);
                let _ = w.wake();
            }
        } else {
            r.metrics.wakeups_coalesced.fetch_add(1, Relaxed);
        }
        Ok(())
    }
}

/// Loop state other than the connection table (kept separate so the two borrow
/// disjointly).
pub(crate) struct Rt {
    pub(crate) id: usize,
    pub(crate) handle: LoopHandle,
    pub(crate) engine: EngineHandle,
    pub(crate) opts: Arc<Options>,
    pub(crate) metrics: Arc<LoopMetrics>,
    pub(crate) poller: Poller,
    pub(crate) alloc: twine_slab::Allocator,
    pub(crate) wheel: Wheel<TimerKind>,
    pub(crate) tasks: twine_mpsc::Consumer<Task>,
    pub(crate) epoch: Instant,
    pub(crate) now: Instant,
    pub(crate) now_ns: u64,
    /// Conns needing a flush this iteration.
    pub(crate) dirty: Vec<ConnId>,
    /// Conns to re-read (or listeners to re-accept) next iteration.
    pub(crate) ready: Vec<ConnId>,
    pub(crate) ready_next: Vec<ConnId>,
    /// Slot indices to recycle at the end of the iteration.
    pub(crate) closing: Vec<u32>,
    /// Zero-delay timers queued this iteration (fire next iteration).
    pub(crate) due: Vec<TimerId>,
    /// The snapshot of `due` being fired this iteration.
    pub(crate) due_next: Vec<TimerId>,
    pub(crate) iov: [libc::iovec; 64],
    pub(crate) locals: Vec<Option<Rc<dyn Any>>>,
    /// Interned protocol type names: (ptr, len, index + 1).
    pub(crate) names: Vec<(usize, usize, u32)>,
    pub(crate) nconns: usize,
    /// Recycled boxed slots, at most `slots_peak` of them.
    #[allow(clippy::vec_box)] // the boxes move between table and list without copying
    pub(crate) free_slots: Vec<Box<ConnSlot>>,
    /// Table entries currently holding a slot.
    pub(crate) slots_live: usize,
    /// Running maximum of `slots_live`; caps the free list.
    pub(crate) slots_peak: usize,
    pub(crate) stopping: bool,
    pub(crate) stop_deadline_ns: u64,
    pub(crate) more_tasks: bool,
    pub(crate) overflow_scratch: Vec<Task>,
}

impl Rt {
    pub(crate) fn refresh_now(&mut self) {
        self.now = Instant::now();
        self.now_ns = self.now.duration_since(self.epoch).as_nanos() as u64;
    }

    /// Interns a protocol type name for the watchdog; returns index + 1.
    pub(crate) fn intern_name(&mut self, name: &'static str) -> u32 {
        let (p, l) = (name.as_ptr() as usize, name.len());
        for &(np, nl, idx) in &self.names {
            if np == p && nl == l {
                return idx;
            }
        }
        let mut g = self.metrics.names.lock().unwrap_or_else(|e| e.into_inner());
        g.push(name);
        let idx = g.len() as u32;
        drop(g);
        self.names.push((p, l, idx));
        idx
    }

    fn has_queued_work(&self) -> bool {
        self.more_tasks
            || !self.ready.is_empty()
            || !self.dirty.is_empty()
            || !self.closing.is_empty()
            || !self.due.is_empty()
            || !self.tasks.is_empty()
            || self.handle.0.overflow_active.load(Acquire)
            || !pending_is_empty()
    }

    fn timer_due(&self) -> bool {
        self.wheel.next().is_some_and(|t| t <= self.now_ns)
    }
}

/// The reactor. Lives on its thread; `!Send`. User code receives `&mut Loop` in
/// timer, task and Deferred callbacks and via `Transport::loop_mut()`.
pub struct Loop {
    pub(crate) conns: Vec<SlotEntry>,
    pub(crate) rt: Rt,
}

/// The loop thread's private state: the `Loop` plus the buffers that must not be
/// reachable through `&mut Loop` (so `&mut [u8]` and `&mut Loop` coexist).
pub(crate) struct LoopThread {
    pub(crate) lp: Loop,
    pub(crate) rdbuf: Box<[u8]>,
    pub(crate) events: [Event; MAX_EVENTS],
}

impl Loop {
    /// The loop's index within its engine.
    pub fn id(&self) -> usize {
        self.rt.id
    }

    /// This loop's cross-thread handle.
    pub fn handle(&self) -> &LoopHandle {
        &self.rt.handle
    }

    /// The owning engine.
    pub fn engine(&self) -> &EngineHandle {
        &self.rt.engine
    }

    /// Cached at the start of the iteration; cheap; what timers fire against.
    pub fn now(&self) -> Instant {
        self.rt.now
    }

    /// A snapshot of this loop's counters.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.rt.metrics.snapshot()
    }

    /// True once shutdown has begun on this loop.
    pub fn is_stopping(&self) -> bool {
        self.rt.stopping
    }

    /// A view of another connection on this loop (`None` if closed, stale or on
    /// another loop).
    pub fn conn(&mut self, id: ConnId) -> Option<Transport<'_>> {
        if id.loop_id as usize != self.rt.id {
            return None;
        }
        let io = self.io(id)?;
        if io.kind != Kind::Tcp || io.flags & flags::CLOSED != 0 {
            return None;
        }
        let mut t = Transport::new(self, id);
        t.foreign = true;
        Some(t)
    }

    /// Runs `f` against the protocol of `id` (downcast to `P`) with its transport.
    /// `Err(Busy)` if that protocol is currently inside its own callback;
    /// `Err(Closed)` if the connection is gone; an `Io` error of kind
    /// `InvalidInput` if the protocol is not a `P`. A panic in `f` propagates
    /// after the protocol is put back.
    pub fn with_protocol<P: Protocol, R>(
        &mut self,
        id: ConnId,
        f: impl FnOnce(&mut P, &mut Transport<'_>) -> R,
    ) -> Result<R> {
        if id.loop_id as usize != self.rt.id {
            return Err(Error::Closed);
        }
        match self.io(id) {
            Some(io) if io.kind == Kind::Tcp && io.flags & flags::CLOSED == 0 => {}
            _ => return Err(Error::Closed),
        }
        let fd = id.fd as usize;
        let Some(mut proto) = self.conns.slot(fd as u32).proto.take() else {
            return Err(Error::Busy);
        };
        let mut f = Some(f);
        let mut out: Option<R> = None;
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut t = Transport::new(self, id);
            proto.visit(&mut t, &mut |any, t| {
                if let (Some(p), Some(f)) = (any.downcast_mut::<P>(), f.take()) {
                    out = Some(f(p, t));
                }
            });
        }));
        if let Some(s) = self.conns[fd].slot.as_deref_mut() {
            s.proto = Some(proto);
        }
        match res {
            Ok(()) => out.ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "protocol type mismatch",
                ))
            }),
            Err(p) => std::panic::resume_unwind(p),
        }
    }

    /// Runs `f` after the current callback returns (post-callback queue).
    pub fn call_soon(&mut self, f: impl FnOnce(&mut Loop) + 'static) {
        pending_push(Pending::Call(Box::new(f)));
    }

    /// Per-loop user state, lazily created from the key's initializer.
    pub fn local<T: 'static>(&mut self, key: LoopLocal<T>) -> Rc<T> {
        if let Some(Some(rc)) = self.rt.locals.get(key.idx) {
            if let Ok(t) = rc.clone().downcast::<T>() {
                return t;
            }
        }
        let init = self.rt.engine.local_init(key.idx);
        let b: Box<dyn Any> = init();
        let v = *b
            .downcast::<T>()
            .expect("LoopLocal initializer returned the wrong type");
        let rc = Rc::new(v);
        if self.rt.locals.len() <= key.idx {
            self.rt.locals.resize(key.idx + 1, None);
        }
        self.rt.locals[key.idx] = Some(rc.clone());
        rc
    }

    // --- slot access ----------------------------------------------------------

    /// The io state of `id` if the generation matches and the slot is in use.
    #[inline]
    pub(crate) fn io(&self, id: ConnId) -> Option<&crate::conn::ConnIo> {
        match self.conns.get(id.fd as usize) {
            Some(e) if e.gen == id.gen && id.loop_id as usize == self.rt.id => e
                .slot
                .as_deref()
                .map(|s| &s.io)
                .filter(|io| io.kind != Kind::Empty),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn io_mut(&mut self, id: ConnId) -> Option<&mut crate::conn::ConnIo> {
        if id.loop_id as usize != self.rt.id {
            return None;
        }
        self.conns
            .find(id)
            .map(|s| &mut s.io)
            .filter(|io| io.kind != Kind::Empty)
    }

    /// The id for the slot at `fd` (current generation).
    #[inline]
    pub(crate) fn id_at(&self, fd: usize) -> ConnId {
        ConnId {
            fd: fd as u32,
            gen: self.conns[fd].gen,
            loop_id: self.rt.id as u16,
        }
    }

    // --- callbacks ------------------------------------------------------------

    /// Runs `f` against the protocol of `id` with panic recovery and watchdog
    /// bookkeeping. The protocol is moved out of its slot for the duration and
    /// put back after. Returns false if the id is stale or the protocol is
    /// already out. A panic aborts the connection with `Error::Panic`.
    pub(crate) fn dispatch(
        &mut self,
        id: ConnId,
        f: impl FnOnce(&mut dyn Protocol, &mut Transport<'_>),
    ) -> bool {
        if id.loop_id as usize != self.rt.id {
            return false;
        }
        let fd = id.fd as usize;
        let Some(slot) = self.conns.find(id) else {
            return false;
        };
        let Some(mut proto) = slot.proto.take() else {
            return false;
        };
        let name = slot.io.proto_name;
        let m = &*self.rt.metrics;
        m.cb_proto.store(name, Relaxed);
        m.cb_start.store(self.rt.now_ns + 1, Relaxed);
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut t = Transport::new(self, id);
            f(&mut *proto, &mut t);
        }));
        self.rt.metrics.cb_start.store(0, Relaxed);
        if let Some(s) = self.conns[fd].slot.as_deref_mut() {
            s.proto = Some(proto);
        }
        if let Err(payload) = res {
            self.proto_panicked(id, payload);
        }
        true
    }

    /// Turns a protocol panic into `connection_lost(Some(Panic))` for that
    /// connection only.
    pub(crate) fn proto_panicked(&mut self, id: ConnId, payload: Box<dyn Any + Send>) {
        metrics::bump(&self.rt.metrics.panics);
        let pe = PanicError::from_payload(payload);
        log::error!(
            "twine: panic in protocol (loop {} fd {}): {}",
            self.rt.id,
            id.fd,
            pe.message
        );
        let Loop { conns, rt } = &mut *self;
        if id.loop_id as usize != rt.id {
            return;
        }
        if let Some(slot) = conns.find(id) {
            if slot.io.kind != Kind::Empty {
                if slot.io.flags & flags::CLOSED == 0 {
                    slot.io.lost = Some(Error::Panic(pe));
                }
                crate::conn::abort_io(rt, &mut slot.io, id.fd);
            }
        }
    }

    /// Runs a standalone closure (timer, task, hook) with panic recovery and
    /// watchdog bookkeeping.
    pub(crate) fn run_guarded(&mut self, what: &'static str, f: impl FnOnce(&mut Loop)) {
        let m = &*self.rt.metrics;
        m.cb_proto.store(0, Relaxed);
        m.cb_start.store(self.rt.now_ns + 1, Relaxed);
        let res = catch_unwind(AssertUnwindSafe(|| f(self)));
        self.rt.metrics.cb_start.store(0, Relaxed);
        if let Err(payload) = res {
            metrics::bump(&self.rt.metrics.panics);
            let pe = PanicError::from_payload(payload);
            log::error!(
                "twine: panic in {what} (loop {}): {}",
                self.rt.id,
                pe.message
            );
        }
    }

    /// Drains the post-callback queue, one item at a time, never holding the
    /// queue borrow while user code runs.
    ///
    /// At most [`MAX_PENDING_PER_DRAIN`] items run per call so a `call_soon`
    /// that keeps re-queueing itself cannot starve I/O; when the cap is hit the
    /// rest waits for the next drain and the loop polls with a zero timeout.
    pub(crate) fn drain_pending(&mut self) {
        let mut budget = MAX_PENDING_PER_DRAIN;
        while budget > 0 {
            let Some(p) = pending_pop() else { return };
            budget -= 1;
            match p {
                Pending::Call(f) => self.run_guarded("call_soon task", f),
                Pending::Notify(id, n) => self.deliver_notify(id, n),
                Pending::Op(id, op) => self.apply_op(id, op),
            }
        }
        if !pending_is_empty() {
            self.rt.more_tasks = true;
        }
    }

    // --- shutdown -------------------------------------------------------------

    /// Starts graceful shutdown (design Section 8 rule 7).
    pub(crate) fn begin_shutdown(&mut self) {
        if self.rt.stopping {
            return;
        }
        self.rt.stopping = true;
        self.rt.stop_deadline_ns = self.rt.now_ns + self.rt.opts.shutdown_timeout.as_nanos() as u64;
        for fd in 0..self.conns.len() {
            let Some(kind) = self.conns[fd].slot.as_ref().map(|s| s.io.kind) else {
                continue;
            };
            let id = self.id_at(fd);
            match kind {
                Kind::Listener => self.close_listener_fd(id),
                Kind::Udp => crate::udp::close_udp(self, id),
                Kind::Connecting => crate::connect::reject_connect(self, id, Error::EngineStopped),
                Kind::Tcp => {
                    if self.conns.slot(fd as u32).io.flags & flags::CLOSED == 0 {
                        self.lose_connection(id);
                    }
                }
                Kind::Empty => {}
            }
        }
        self.drain_pending();
    }

    /// True when every connection is gone or the deadline passed (in which case
    /// the remaining connections are aborted with `Error::Shutdown`).
    pub(crate) fn shutdown_complete(&mut self) -> bool {
        if self.rt.nconns == 0 && self.rt.closing.is_empty() {
            return true;
        }
        if self.rt.now_ns >= self.rt.stop_deadline_ns {
            let Loop { conns, rt } = &mut *self;
            for (fd, slot) in conns
                .iter_mut()
                .enumerate()
                .filter_map(|(fd, e)| e.slot.as_deref_mut().map(|s| (fd, s)))
            {
                if slot.io.kind == Kind::Tcp && slot.io.flags & flags::CLOSED == 0 {
                    slot.io.lost = Some(Error::Shutdown);
                    crate::conn::abort_io(rt, &mut slot.io, fd as u32);
                }
            }
            self.recycle_closed();
            return true;
        }
        false
    }

    /// Finishes closing every connection queued this iteration.
    pub(crate) fn recycle_closed(&mut self) {
        let mut i = 0;
        while i < self.rt.closing.len() {
            let fd = self.rt.closing[i] as usize;
            i += 1;
            self.finish_close(fd);
            self.drain_pending();
        }
        self.rt.closing.clear();
    }

    // --- tasks ----------------------------------------------------------------

    fn run_task(&mut self, task: Task) {
        metrics::bump(&self.rt.metrics.tasks_run);
        match task {
            Task::Call(f) => self.run_guarded("call_from_thread task", f),
            Task::Op(id, op) => self.apply_op(id, op),
            Task::Shutdown => self.begin_shutdown(),
        }
        self.drain_pending();
    }

    /// Runs queued cross-thread tasks, at most `MAX_TASKS_PER_ITERATION`.
    pub(crate) fn drain_tasks(&mut self) {
        self.rt.more_tasks = false;
        let remote = self.rt.handle.0.clone();
        remote.wake_pending.store(false, SeqCst);
        fence(SeqCst);
        self.drain_pending();
        let mut budget = MAX_TASKS_PER_ITERATION;
        while budget > 0 {
            let Some(t) = self.rt.tasks.pop() else { break };
            budget -= 1;
            self.run_task(t);
        }
        if budget > 0 && remote.overflow_active.load(Acquire) {
            // Overflowed tasks are older than anything pushed to the ring after
            // them, and while overflow is active producers keep appending there,
            // so by the time the ring is empty the overflow vector is the FIFO.
            let mut ov = std::mem::take(&mut self.rt.overflow_scratch);
            {
                let mut g = remote.overflow.lock().unwrap_or_else(|e| e.into_inner());
                std::mem::swap(&mut *g, &mut ov);
                if ov.is_empty() {
                    remote.overflow_active.store(false, SeqCst);
                }
            }
            let mut rest: Vec<Task> = Vec::new();
            let mut it = ov.drain(..);
            while let Some(t) = it.next() {
                if budget == 0 {
                    rest.push(t);
                    rest.extend(it.by_ref());
                    break;
                }
                budget -= 1;
                self.run_task(t);
            }
            drop(it);
            if !rest.is_empty() {
                let mut g = remote.overflow.lock().unwrap_or_else(|e| e.into_inner());
                rest.append(&mut g);
                std::mem::swap(&mut *g, &mut rest);
            }
            self.rt.overflow_scratch = ov;
        }
        if budget == 0 {
            self.rt.more_tasks = true;
        }
    }

    /// Runs whatever was queued while the loop was exiting so that callbacks and
    /// hooks are not lost silently.
    fn drain_tasks_after_stop(&mut self) {
        loop {
            self.drain_tasks();
            if !self.rt.more_tasks
                && self.rt.tasks.is_empty()
                && !self.rt.handle.0.overflow_active.load(Acquire)
                && pending_is_empty()
            {
                return;
            }
        }
    }

    // --- timers ---------------------------------------------------------------

    /// Advances the wheel and fires every expired timer, draining the
    /// post-callback queue after each.
    fn fire_timers(&mut self) {
        self.fire_due();
        self.rt.wheel.advance(self.rt.now_ns);
        while let Some((id, kind)) = self.rt.wheel.pop_due() {
            self.fire_timer(id, kind);
        }
    }
}

impl LoopThread {
    fn new(lp: Loop) -> LoopThread {
        let n = lp.rt.opts.read_buffer_size;
        LoopThread {
            lp,
            rdbuf: vec![0u8; n].into_boxed_slice(),
            events: [Event::default(); MAX_EVENTS],
        }
    }

    /// How long the loop may block: until the next timer or the shutdown
    /// deadline, rounded up to the wheel tick; `None` = forever.
    fn poll_timeout(&self) -> Option<Duration> {
        let rt = &self.lp.rt;
        let mut next = rt.wheel.next();
        if rt.stopping {
            next = Some(next.map_or(rt.stop_deadline_ns, |n| n.min(rt.stop_deadline_ns)));
        }
        let next = next?;
        let real = rt.epoch.elapsed().as_nanos() as u64;
        if next <= real {
            return Some(Duration::ZERO);
        }
        let d = next - real;
        Some(Duration::from_nanos(
            d.div_ceil(WHEEL_TICK_NS) * WHEEL_TICK_NS,
        ))
    }

    /// Runs the loop until shutdown completes.
    fn iterate(&mut self) {
        let spin = self.lp.rt.opts.spin_iterations;
        loop {
            // Zero-delay timers queued so far fire this iteration; any queued from
            // here on wait for the next one.
            self.lp.take_due();

            // 1. Drain the task queue (bounded) and the post-callback queue.
            self.lp.drain_tasks();
            if self.lp.rt.stopping && self.lp.shutdown_complete() {
                return;
            }

            // 2. Advance the wheel and fire expired timers.
            self.lp.rt.refresh_now();
            let iter_start = self.lp.rt.now_ns;
            self.lp.fire_timers();

            // 3. Poll: spin with timeout 0, then block until the next timer.
            let mut n = 0usize;
            let mut poll_failed = None;
            let immediate = self.lp.rt.has_queued_work() || self.lp.rt.timer_due();
            if immediate {
                match self
                    .lp
                    .rt
                    .poller
                    .wait(&mut self.events, Some(Duration::ZERO))
                {
                    Ok(k) => n = k,
                    Err(e) => poll_failed = Some(e),
                }
            } else {
                let mut s = 0;
                loop {
                    match self
                        .lp
                        .rt
                        .poller
                        .wait(&mut self.events, Some(Duration::ZERO))
                    {
                        Ok(k) => n = k,
                        Err(e) => poll_failed = Some(e),
                    }
                    if n > 0 || poll_failed.is_some() || s >= spin {
                        break;
                    }
                    // A zero-timeout wait consumes a wakeup without reporting it;
                    // stop spinning as soon as a task is queued.
                    if self.lp.rt.has_queued_work() {
                        break;
                    }
                    s += 1;
                }
                if n == 0 && poll_failed.is_none() {
                    // The spin may have consumed a wakeup whose task was already
                    // drained; clear the coalescing flag so the next producer wakes
                    // us, then re-check the queue before blocking.
                    self.lp.rt.handle.0.wake_pending.store(false, SeqCst);
                    fence(SeqCst);
                    if !self.lp.rt.has_queued_work() && !self.lp.rt.timer_due() {
                        let timeout = self.poll_timeout();
                        let t0 = Instant::now();
                        match self.lp.rt.poller.wait(&mut self.events, timeout) {
                            Ok(k) => n = k,
                            Err(e) => poll_failed = Some(e),
                        }
                        metrics::add(
                            &self.lp.rt.metrics.poll_block_nanos,
                            t0.elapsed().as_nanos() as u64,
                        );
                    }
                }
            }
            self.lp.rt.refresh_now();
            let busy_pre = self.lp.rt.now_ns.saturating_sub(iter_start);
            if let Some(e) = poll_failed {
                log::error!("twine: poll failed (loop {}): {e}", self.lp.rt.id);
            }

            // 4. Dispatch I/O.
            for i in 0..n {
                let ev = self.events[i];
                self.dispatch_event(ev);
            }
            // Conns whose read cap was hit last iteration, resumed readers, listeners.
            if !self.lp.rt.ready.is_empty() {
                std::mem::swap(&mut self.lp.rt.ready, &mut self.lp.rt.ready_next);
                for i in 0..self.lp.rt.ready_next.len() {
                    let id = self.lp.rt.ready_next[i];
                    self.handle_ready(id);
                }
                self.lp.rt.ready_next.clear();
            }

            // 5. Flush dirty connections with writev.
            self.flush_dirty();

            // 6. Recycle closed connections.
            self.lp.recycle_closed();

            let m = &*self.lp.rt.metrics;
            metrics::bump(&m.iterations);
            let end_ns = self.lp.rt.epoch.elapsed().as_nanos() as u64;
            let busy = busy_pre + end_ns.saturating_sub(self.lp.rt.now_ns);
            m.hist.observe(busy, end_ns / 1_000_000_000);
            let st = self.lp.rt.alloc.stats();
            m.slab_in_use.store(st.in_use as u64, Relaxed);
            m.slab_free.store(st.free as u64, Relaxed);
            m.timers_active
                .store(self.lp.rt.wheel.len() as u64, Relaxed);

            if self.lp.rt.stopping && self.lp.shutdown_complete() {
                return;
            }
        }
    }

    /// Re-runs the read-side handler for a conn whose per-event cap was hit
    /// last iteration, whose reading was just resumed, or a listener to re-poll.
    fn handle_ready(&mut self, id: ConnId) {
        let Some(io) = self.lp.io_mut(id) else { return };
        io.flags &= !flags::IN_READY;
        let (kind, fl) = (io.kind, io.flags);
        match kind {
            Kind::Listener => self.lp.handle_accept(id),
            Kind::Udp => crate::udp::handle_udp_read(&mut self.lp, id),
            Kind::Tcp if fl & (flags::CLOSED | flags::READ_PAUSED) == 0 => {
                self.handle_read(id, false)
            }
            _ => {}
        }
    }

    /// Routes one poll event to the right handler.
    fn dispatch_event(&mut self, ev: Event) {
        let fd = ev.fd as usize;
        let Some(slot) = self.lp.conns.get(fd).and_then(|e| e.slot.as_deref()) else {
            return;
        };
        let kind = slot.io.kind;
        if kind == Kind::Empty {
            return;
        }
        let id = self.lp.id_at(fd);
        match kind {
            Kind::Listener => {
                if ev.flags & (twine_poll::READABLE | twine_poll::ERROR | twine_poll::HANGUP) != 0 {
                    self.lp.handle_accept(id);
                }
                return;
            }
            Kind::Udp => {
                if ev.flags & twine_poll::READABLE != 0 {
                    crate::udp::handle_udp_read(&mut self.lp, id);
                }
                if ev.flags & twine_poll::WRITABLE != 0 {
                    self.lp.mark_dirty(id);
                }
                return;
            }
            Kind::Connecting => {
                crate::connect::handle_connect_ready(&mut self.lp, id, ev.flags);
                return;
            }
            _ => {}
        }
        if slot.io.flags & flags::CLOSED != 0 {
            return;
        }
        if ev.flags & twine_poll::ERROR != 0 {
            if let Some(e) = crate::conn::sock_error(slot.io.fd) {
                self.lp.fail(id, e);
                return;
            }
        }
        // Hangup is folded into the read path: read(2) returns 0 (half-close) or an
        // error (reset), which closes with the right reason and never discards data
        // still queued in the socket. While reading is paused the hangup is
        // discovered when the protocol resumes.
        let paused = slot.io.flags & flags::READ_PAUSED != 0;
        let hangup = ev.flags & twine_poll::HANGUP != 0;
        if hangup && paused {
            // Remember the hangup for the read that follows resume_reading.
            if let Some(io) = self.lp.io_mut(id) {
                io.flags |= flags::HANGUP;
            }
        }
        if ev.flags & (twine_poll::READABLE | twine_poll::HANGUP) != 0 && !paused {
            self.handle_read(id, hangup);
        }
        if ev.flags & twine_poll::WRITABLE != 0 {
            self.lp.on_writable(id);
        }
    }

    /// Issues one writev per dirty conn. Conns re-marked during the pass wait for
    /// the next iteration so one fast producer cannot monopolise the loop.
    fn flush_dirty(&mut self) {
        let n = self.lp.rt.dirty.len();
        for i in 0..n {
            let id = self.lp.rt.dirty[i];
            let Some(io) = self.lp.io_mut(id) else {
                continue;
            };
            io.flags &= !flags::DIRTY;
            if io.flags & flags::CLOSED != 0 {
                continue;
            }
            match io.kind {
                Kind::Udp => crate::udp::flush_udp(&mut self.lp, id),
                Kind::Tcp => {
                    self.lp.flush(id);
                    self.lp.drain_pending();
                }
                _ => {}
            }
        }
        self.lp.rt.dirty.drain(..n);
    }
}

#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu: usize) -> std::io::Result<()> {
    // SAFETY: `set` is a zeroed cpu_set_t (a plain bit array, valid when all zero),
    // CPU_SET writes within it, and sched_setaffinity(0, ..) reads exactly
    // `size_of::<cpu_set_t>()` bytes from it for the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(_cpu: usize) -> std::io::Result<()> {
    Ok(())
}

/// Everything a loop thread needs from the engine.
pub(crate) struct ThreadArgs {
    pub(crate) id: usize,
    pub(crate) engine: EngineHandle,
    pub(crate) opts: Arc<Options>,
    pub(crate) handle: LoopHandle,
    pub(crate) tasks: twine_mpsc::Consumer<Task>,
    pub(crate) ready: std::sync::mpsc::Sender<Result<()>>,
}

/// The loop thread body (design Section 4).
pub(crate) fn thread_main(a: ThreadArgs) {
    let ThreadArgs {
        id,
        engine,
        opts,
        handle,
        tasks,
        ready,
    } = a;
    if !opts.pin_cpus.is_empty() {
        let cpu = opts.pin_cpus[id % opts.pin_cpus.len()];
        if let Err(e) = pin_to_cpu(cpu) {
            log::warn!("twine: pinning loop {id} to cpu {cpu}: {e}");
        }
    }
    let poller = match Poller::new() {
        Ok(p) => p,
        Err(e) => {
            handle.0.stopped.store(true, SeqCst);
            let _ = ready.send(Err(Error::from(e)));
            return;
        }
    };
    let _ = handle.0.waker.set(poller.waker());
    let remote_addr = Arc::as_ptr(&handle.0) as usize;
    CURRENT_LOOP.with(|c| c.set(remote_addr));
    let metrics = handle.0.metrics.clone();
    let epoch = metrics.epoch;
    let now = Instant::now();
    let now_ns = now.duration_since(epoch).as_nanos() as u64;
    let zero_iov = libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    };
    let rt = Rt {
        id,
        handle: handle.clone(),
        engine,
        opts: opts.clone(),
        metrics,
        poller,
        alloc: twine_slab::Allocator::new(),
        wheel: Wheel::new(WHEEL_TICK, now_ns),
        tasks,
        epoch,
        now,
        now_ns,
        dirty: Vec::with_capacity(256),
        ready: Vec::with_capacity(256),
        ready_next: Vec::with_capacity(256),
        closing: Vec::with_capacity(256),
        due: Vec::with_capacity(64),
        due_next: Vec::with_capacity(64),
        iov: [zero_iov; 64],
        locals: Vec::new(),
        names: Vec::new(),
        nconns: 0,
        free_slots: Vec::new(),
        slots_live: 0,
        slots_peak: 0,
        stopping: false,
        stop_deadline_ns: 0,
        more_tasks: false,
        overflow_scratch: Vec::new(),
    };
    let mut conns = Vec::new();
    conns.resize_with(1024, || SlotEntry { gen: 0, slot: None });
    rt.metrics.slots_table.store(1024, Relaxed);
    let mut lt = LoopThread::new(Loop { conns, rt });
    handle.0.started.store(true, SeqCst);
    let _ = ready.send(Ok(()));
    drop(ready);

    let res = catch_unwind(AssertUnwindSafe(|| {
        lt.iterate();
        handle.0.stopped.store(true, SeqCst);
        lt.lp.drain_tasks_after_stop();
    }));
    if res.is_err() {
        log::error!("twine: panic in loop-internal code (loop {id}); aborting");
        std::process::abort();
    }
    handle.0.stopped.store(true, SeqCst);
    CURRENT_LOOP.with(|c| c.set(0));
    // Dropping queued closures can push to PENDING again (a dropped
    // `LocalResolver`), so drop them outside the borrow.
    let q = PENDING.with(|q| std::mem::take(&mut *q.borrow_mut()));
    drop(q);
    drop(lt);
    let q = PENDING.with(|q| std::mem::take(&mut *q.borrow_mut()));
    drop(q);
}
