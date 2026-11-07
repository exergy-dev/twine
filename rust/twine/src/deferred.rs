//! [`Deferred`], its settle sides ([`Resolver`], [`LocalResolver`]), the
//! off-loop [`Awaitable`], `all` / `any`, and the Loop and LoopHandle methods
//! that return them (design Sections 6.2, 6.3 and 8 rule 2).
//!
//! A `Deferred` is an `Rc` cell owned by one loop thread. Callbacks never run
//! inside `resolve` or `then`: settling pushes the callback onto the loop's
//! post-callback queue. A `Resolver` (which is `Send`) cannot reach the `Rc`
//! cell directly, so it parks the outcome in an `Arc` slot and posts a task
//! through `LoopHandle::call_from_thread`; the task looks the cell up in a
//! thread-local registry (keyed by a process-unique id) and settles it there.

use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::errors::{Error, PanicError, Result};
use crate::event_loop::{pending_push, ConnId, Loop, LoopHandle, Pending};
use crate::options::debug_enabled;
use crate::pool::{pool_for, Job};
use crate::protocol::Protocol;
use crate::transport::Transport;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// --- the cell ---------------------------------------------------------------------

type Waiter<T> = Box<dyn FnOnce(&mut Loop, Result<T>)>;

struct Inner<T> {
    /// Settled (the value may already have been handed to the waiter).
    done: bool,
    value: Option<Result<T>>,
    waiter: Option<Waiter<T>>,
    /// Creation backtrace (`TWINE_DEBUG=1` only).
    bt: Option<Backtrace>,
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // A rejection nobody consumed (no handler ever took it) is a lost error.
        if let Some(Err(e)) = &self.value {
            if let Some(bt) = &self.bt {
                log::warn!(
                    "twine: Deferred rejected with no handler attached: {e}\ncreated at:\n{bt}"
                );
            }
        }
    }
}

type Cell<T> = Rc<RefCell<Inner<T>>>;

fn new_cell<T>() -> Cell<T> {
    Rc::new(RefCell::new(Inner {
        done: false,
        value: None,
        waiter: None,
        bt: debug_enabled().then(Backtrace::force_capture),
    }))
}

/// Settles the cell once; later calls are no-ops. The waiter, if any, is queued
/// on the post-callback queue, never run here.
fn settle<T: 'static>(c: &Cell<T>, r: Result<T>) {
    let mut g = c.borrow_mut();
    if g.done {
        return;
    }
    g.done = true;
    match g.waiter.take() {
        Some(w) => {
            drop(g);
            pending_push(Pending::Call(Box::new(move |lp| w(lp, r))));
        }
        None => g.value = Some(r),
    }
}

/// Installs the single consumer.
fn attach<T: 'static>(c: &Cell<T>, w: Waiter<T>) {
    let mut g = c.borrow_mut();
    if g.done {
        if let Some(r) = g.value.take() {
            drop(g);
            pending_push(Pending::Call(Box::new(move |lp| w(lp, r))));
        }
        // else: already consumed (cannot happen: `attach` takes `self`)
    } else {
        g.waiter = Some(w);
    }
}

/// Runs a user callback, turning a panic into `Error::Panic` (the loop survives).
fn guard<R>(lp: &mut Loop, f: impl FnOnce(&mut Loop) -> Result<R>) -> Result<R> {
    match catch_unwind(AssertUnwindSafe(|| f(lp))) {
        Ok(r) => r,
        Err(p) => {
            crate::metrics::bump(&lp.rt.metrics.panics);
            let pe = PanicError::from_payload(p);
            log::error!(
                "twine: panic in Deferred callback (loop {}): {}",
                lp.rt.id,
                pe.message
            );
            Err(Error::Panic(pe))
        }
    }
}

// --- Deferred ---------------------------------------------------------------------

/// A single-assignment result whose callbacks run on its loop. `!Send`, tied to
/// the loop that created it. One consumer: `then` and friends take `self`.
///
/// Callbacks never run synchronously inside `resolve` or `then`; they run from
/// the loop's post-callback queue after the current callback returns. A panic
/// in a callback rejects the next Deferred in the chain with [`Error::Panic`].
#[must_use = "a Deferred does nothing unless it is consumed with then/finally/awaitable"]
pub struct Deferred<T: 'static> {
    cell: Cell<T>,
}

impl<T: 'static> Deferred<T> {
    /// An already-resolved Deferred (callbacks still run from the queue).
    pub fn resolved(_lp: &mut Loop, v: T) -> Deferred<T> {
        let cell = new_cell();
        settle(&cell, Ok(v));
        Deferred { cell }
    }

    /// An already-rejected Deferred.
    pub fn rejected(_lp: &mut Loop, e: Error) -> Deferred<T> {
        let cell = new_cell();
        settle(&cell, Err(e));
        Deferred { cell }
    }

    /// Transforms the value on the loop; a panic rejects the result with
    /// [`Error::Panic`]. A rejection skips `f` and propagates.
    pub fn then<U: 'static>(self, f: impl FnOnce(&mut Loop, T) -> U + 'static) -> Deferred<U> {
        self.try_then(move |lp, v| Ok(f(lp, v)))
    }

    /// Like [`Deferred::then`] with a fallible transform.
    pub fn try_then<U: 'static>(
        self,
        f: impl FnOnce(&mut Loop, T) -> Result<U> + 'static,
    ) -> Deferred<U> {
        let next = new_cell::<U>();
        let n2 = next.clone();
        attach(
            &self.cell,
            Box::new(move |lp, r| match r {
                Ok(v) => {
                    let out = guard(lp, move |lp| f(lp, v));
                    settle(&n2, out);
                }
                Err(e) => settle(&n2, Err(e)),
            }),
        );
        Deferred { cell: next }
    }

    /// Chains a Deferred-returning step; the result settles with the inner one.
    pub fn and_then<U: 'static>(
        self,
        f: impl FnOnce(&mut Loop, T) -> Deferred<U> + 'static,
    ) -> Deferred<U> {
        let next = new_cell::<U>();
        let n2 = next.clone();
        attach(
            &self.cell,
            Box::new(move |lp, r| match r {
                Ok(v) => match guard(lp, move |lp| Ok(f(lp, v))) {
                    Ok(inner) => attach(&inner.cell, Box::new(move |_, r| settle(&n2, r))),
                    Err(e) => settle(&n2, Err(e)),
                },
                Err(e) => settle(&n2, Err(e)),
            }),
        );
        Deferred { cell: next }
    }

    /// Handles (and possibly recovers from) a rejection. A value passes through.
    pub fn catch(self, f: impl FnOnce(&mut Loop, Error) -> Result<T> + 'static) -> Deferred<T> {
        let next = new_cell::<T>();
        let n2 = next.clone();
        attach(
            &self.cell,
            Box::new(move |lp, r| match r {
                Ok(v) => settle(&n2, Ok(v)),
                Err(e) => {
                    let out = guard(lp, move |lp| f(lp, e));
                    settle(&n2, out);
                }
            }),
        );
        Deferred { cell: next }
    }

    /// Terminal: observes the outcome. A panic in `f` is logged by the loop.
    pub fn finally(self, f: impl FnOnce(&mut Loop, Result<T>) + 'static) {
        attach(&self.cell, Box::new(f));
    }

    /// Terminal: delivers the outcome to a connection's protocol through
    /// `with_protocol`; dropped silently if the connection is gone (or its
    /// protocol is not a `P`).
    pub fn on_conn<P: Protocol>(
        self,
        id: ConnId,
        f: impl FnOnce(&mut P, &mut Transport<'_>, Result<T>) + 'static,
    ) {
        self.finally(move |lp, r| {
            let _ = lp.with_protocol::<P, _>(id, move |p, t| f(p, t, r));
        });
    }

    /// Hands the outcome to another thread.
    pub fn awaitable(self) -> Awaitable<T>
    where
        T: Send,
    {
        let (aw, tx) = awaitable_pair();
        attach(&self.cell, Box::new(move |_, r| tx.send(r)));
        aw
    }

    /// True once settled (resolved or rejected).
    pub fn is_done(&self) -> bool {
        self.cell.borrow().done
    }
}

// --- LocalResolver ----------------------------------------------------------------

/// Settles a Deferred from the loop thread (no `Send` requirement on `T`).
/// Dropping it unsettled rejects with [`Error::Abandoned`].
pub struct LocalResolver<T: 'static> {
    cell: Cell<T>,
}

impl<T: 'static> LocalResolver<T> {
    /// Resolves the Deferred.
    pub fn resolve(self, v: T) {
        settle(&self.cell, Ok(v));
    }

    /// Rejects the Deferred.
    pub fn reject(self, e: Error) {
        settle(&self.cell, Err(e));
    }
}

impl<T: 'static> Drop for LocalResolver<T> {
    fn drop(&mut self) {
        if !self.cell.borrow().done {
            if let Some(bt) = &self.cell.borrow().bt {
                log::warn!("twine: LocalResolver dropped unsettled; Deferred created at:\n{bt}");
            }
            // May run during thread teardown, when the queue's TLS is gone.
            let c = self.cell.clone();
            let _ = catch_unwind(AssertUnwindSafe(|| settle(&c, Err(Error::Abandoned))));
        }
    }
}

// --- Resolver (Send) --------------------------------------------------------------

struct Shared<T> {
    settled: AtomicBool,
    slot: Mutex<Option<Result<T>>>,
}

type Fire = Box<dyn FnOnce()>;

thread_local! {
    /// Cells awaiting a cross-thread settle on this loop thread, by process-unique
    /// id (`NEXT_ID`), so entries of different engines cannot alias. An entry is
    /// removed when its settle task runs; a `Deferred` dropped unresolved while its
    /// `Resolver` lives keeps the entry until that settle (or the loop thread's
    /// exit), which is bounded by the Resolver's lifetime and acceptable.
    static REGISTRY: RefCell<HashMap<u64, Fire>> = RefCell::new(HashMap::new());
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn fire(id: u64) {
    let f = REGISTRY.with(|r| r.borrow_mut().remove(&id));
    if let Some(f) = f {
        f();
    }
}

struct ResInner<T: Send + 'static> {
    sh: Arc<Shared<T>>,
    h: LoopHandle,
    id: u64,
    /// Creation backtrace (`TWINE_DEBUG=1` only).
    bt: Option<Backtrace>,
}

impl<T: Send + 'static> ResInner<T> {
    fn settle(&self, r: Result<T>) {
        if self.sh.settled.swap(true, Ordering::AcqRel) {
            return;
        }
        *lock(&self.sh.slot) = Some(r);
        let id = self.id;
        // Err(LoopStopped): the loop is gone, nobody can observe the Deferred.
        let _ = self.h.call_from_thread(move |_| fire(id));
    }
}

impl<T: Send + 'static> Drop for ResInner<T> {
    fn drop(&mut self) {
        if let Some(bt) = &self.bt {
            if !self.sh.settled.load(Ordering::Acquire) {
                log::warn!("twine: Resolver dropped unsettled; Deferred created at:\n{bt}");
            }
        }
        self.settle(Err(Error::Abandoned));
    }
}

/// Settles a Deferred from any thread. `Clone + Send + Sync`, idempotent.
/// Dropping the last clone without settling rejects with [`Error::Abandoned`].
pub struct Resolver<T: Send + 'static> {
    inner: Arc<ResInner<T>>,
}

impl<T: Send + 'static> Clone for Resolver<T> {
    fn clone(&self) -> Self {
        Resolver {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Send + 'static> Resolver<T> {
    /// Resolves the Deferred (the first settle wins; later calls are no-ops).
    pub fn resolve(&self, v: T) {
        self.inner.settle(Ok(v));
    }

    /// Rejects the Deferred (the first settle wins; later calls are no-ops).
    pub fn reject(&self, e: Error) {
        self.inner.settle(Err(e));
    }
}

impl Loop {
    /// A Deferred plus a `Send` resolver that may settle it from any thread.
    pub fn deferred<T: Send + 'static>(&mut self) -> (Deferred<T>, Resolver<T>) {
        let cell = new_cell::<T>();
        let sh = Arc::new(Shared {
            settled: AtomicBool::new(false),
            slot: Mutex::new(None),
        });
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let (c2, s2) = (cell.clone(), sh.clone());
        REGISTRY.with(|r| {
            r.borrow_mut().insert(
                id,
                Box::new(move || {
                    if let Some(v) = lock(&s2.slot).take() {
                        settle(&c2, v);
                    }
                }),
            );
        });
        let res = Resolver {
            inner: Arc::new(ResInner {
                sh,
                h: self.rt.handle.clone(),
                id,
                bt: debug_enabled().then(Backtrace::force_capture),
            }),
        };
        (Deferred { cell }, res)
    }

    /// A Deferred plus a loop-thread-only resolver (`T` need not be `Send`).
    pub fn deferred_local<T: 'static>(&mut self) -> (Deferred<T>, LocalResolver<T>) {
        let cell = new_cell::<T>();
        (Deferred { cell: cell.clone() }, LocalResolver { cell })
    }

    /// Runs `f` on the engine's worker pool and resolves with its result. A
    /// panic in `f` rejects with [`Error::Panic`]; after the engine stops (or
    /// when the pool shuts down before `f` starts) it rejects with
    /// [`Error::EngineStopped`]. Never blocks the loop.
    pub fn defer_to_thread<T: Send + 'static>(
        &mut self,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Deferred<T> {
        if self.rt.stopping || self.rt.engine.is_stopped() {
            return Deferred::rejected(self, Error::EngineStopped);
        }
        let (d, r) = self.deferred::<T>();
        let pool = pool_for(&self.rt.engine);
        let job: Job = Box::new(move |run| {
            if !run {
                r.reject(Error::EngineStopped);
                return;
            }
            match catch_unwind(AssertUnwindSafe(f)) {
                Ok(Ok(v)) => r.resolve(v),
                Ok(Err(e)) => r.reject(e),
                Err(p) => r.reject(Error::Panic(PanicError::from_payload(p))),
            }
        });
        if let Err(job) = pool.submit(job, true) {
            job(false);
        }
        d
    }

    /// Resolves after `d` (a timer on this loop).
    pub fn sleep(&mut self, d: Duration) -> Deferred<()> {
        let (def, r) = self.deferred_local::<()>();
        self.call_later(d, move |_| r.resolve(()));
        def
    }
}

// --- all / any --------------------------------------------------------------------

/// Resolves with every value in input order when every input resolves; rejects
/// on the first rejection. An empty input resolves with an empty vector.
pub fn all<T: 'static>(_lp: &mut Loop, ds: Vec<Deferred<T>>) -> Deferred<Vec<T>> {
    let out = new_cell::<Vec<T>>();
    let n = ds.len();
    if n == 0 {
        settle(&out, Ok(Vec::new()));
        return Deferred { cell: out };
    }
    struct St<T> {
        vals: Vec<Option<T>>,
        left: usize,
    }
    let st = Rc::new(RefCell::new(St {
        vals: (0..n).map(|_| None).collect(),
        left: n,
    }));
    for (i, d) in ds.into_iter().enumerate() {
        let (st, out) = (st.clone(), out.clone());
        attach(
            &d.cell,
            Box::new(move |_, r| match r {
                Err(e) => settle(&out, Err(e)),
                Ok(v) => {
                    let mut s = st.borrow_mut();
                    s.vals[i] = Some(v);
                    s.left -= 1;
                    if s.left == 0 {
                        let vals: Vec<T> = s.vals.drain(..).flatten().collect();
                        drop(s);
                        settle(&out, Ok(vals));
                    }
                }
            }),
        );
    }
    Deferred { cell: out }
}

/// Resolves with the first value; rejects only if every input rejects (with the
/// last error). An empty input rejects with [`Error::Abandoned`].
pub fn any<T: 'static>(_lp: &mut Loop, ds: Vec<Deferred<T>>) -> Deferred<T> {
    let out = new_cell::<T>();
    let n = ds.len();
    if n == 0 {
        settle(&out, Err(Error::Abandoned));
        return Deferred { cell: out };
    }
    let left = Rc::new(std::cell::Cell::new(n));
    for d in ds {
        let (left, out) = (left.clone(), out.clone());
        attach(
            &d.cell,
            Box::new(move |_, r| match r {
                Ok(v) => settle(&out, Ok(v)),
                Err(e) => {
                    left.set(left.get() - 1);
                    if left.get() == 0 {
                        settle(&out, Err(e));
                    }
                }
            }),
        );
    }
    Deferred { cell: out }
}

// --- Awaitable --------------------------------------------------------------------

struct AwState<T> {
    val: Option<Result<T>>,
    done: bool,
    waker: Option<Waker>,
}

struct AwShared<T> {
    m: Mutex<AwState<T>>,
    cv: Condvar,
}

/// Sends the outcome to an [`Awaitable`]; dropped unsent it delivers
/// [`Error::LoopStopped`] (the loop discarded the work).
pub(crate) struct AwSender<T> {
    sh: Option<Arc<AwShared<T>>>,
}

impl<T> AwSender<T> {
    pub(crate) fn send(mut self, r: Result<T>) {
        if let Some(sh) = self.sh.take() {
            put(&sh, r);
        }
    }
}

impl<T> Drop for AwSender<T> {
    fn drop(&mut self) {
        if let Some(sh) = self.sh.take() {
            put(&sh, Err(Error::LoopStopped));
        }
    }
}

fn put<T>(sh: &AwShared<T>, r: Result<T>) {
    let w = {
        let mut g = lock(&sh.m);
        if g.done {
            return;
        }
        g.done = true;
        g.val = Some(r);
        g.waker.take()
    };
    sh.cv.notify_all();
    if let Some(w) = w {
        w.wake();
    }
}

pub(crate) fn awaitable_pair<T>() -> (Awaitable<T>, AwSender<T>) {
    let sh = Arc::new(AwShared {
        m: Mutex::new(AwState {
            val: None,
            done: false,
            waker: None,
        }),
        cv: Condvar::new(),
    });
    (Awaitable { sh: sh.clone() }, AwSender { sh: Some(sh) })
}

/// The off-loop side of a Deferred. `Send` (when `T` is). Also a
/// `std::future::Future<Output = Result<T>>`.
pub struct Awaitable<T> {
    sh: Arc<AwShared<T>>,
}

impl<T> Awaitable<T> {
    /// Blocks the calling thread until the outcome arrives.
    ///
    /// # Panics
    /// Panics with the value [`Error::AwaitOnLoop`] on any loop thread: blocking
    /// there would stall every connection of that loop.
    pub fn wait(self) -> Result<T> {
        if crate::event_loop::on_any_loop_thread() {
            std::panic::panic_any(Error::AwaitOnLoop);
        }
        let mut g = lock(&self.sh.m);
        loop {
            if let Some(r) = g.val.take() {
                return r;
            }
            if g.done {
                return Err(Error::Closed); // already taken through `try_take`
            }
            g = self.sh.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Like [`Awaitable::wait`] but gives up after `d`, returning the Awaitable.
    ///
    /// # Panics
    /// Panics with [`Error::AwaitOnLoop`] on a loop thread.
    pub fn wait_timeout(self, d: Duration) -> std::result::Result<Result<T>, Awaitable<T>> {
        if crate::event_loop::on_any_loop_thread() {
            std::panic::panic_any(Error::AwaitOnLoop);
        }
        let deadline = Instant::now() + d;
        let mut g = lock(&self.sh.m);
        loop {
            if let Some(r) = g.val.take() {
                return Ok(r);
            }
            if g.done {
                return Ok(Err(Error::Closed));
            }
            let now = Instant::now();
            if now >= deadline {
                drop(g);
                return Err(self);
            }
            g = self
                .sh
                .cv
                .wait_timeout(g, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// Takes the outcome if it has arrived (once).
    pub fn try_take(&self) -> Option<Result<T>> {
        lock(&self.sh.m).val.take()
    }
}

impl<T> Future for Awaitable<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T>> {
        let mut g = lock(&self.sh.m);
        if let Some(r) = g.val.take() {
            return Poll::Ready(r);
        }
        if g.done {
            return Poll::Ready(Err(Error::Closed));
        }
        match &g.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => g.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

// --- LoopHandle -------------------------------------------------------------------

impl LoopHandle {
    /// Runs `f` on the loop and returns its result to the caller. A panic in `f`
    /// yields [`Error::Panic`]; [`Error::LoopStopped`] if the loop is gone.
    pub fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Loop) -> T + Send + 'static,
    ) -> Awaitable<T> {
        let (aw, tx) = awaitable_pair();
        // On Err the closure (and `tx`) is dropped, which delivers LoopStopped.
        let _ = self.call_from_thread(move |lp| {
            let r = guard(lp, move |lp| Ok(f(lp)));
            tx.send(r);
        });
        aw
    }

    /// Runs `f` on the loop and forwards the Deferred it returns.
    pub fn submit<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Loop) -> Deferred<T> + Send + 'static,
    ) -> Awaitable<T> {
        let (aw, tx) = awaitable_pair();
        let _ = self.call_from_thread(move |lp| match guard(lp, move |lp| Ok(f(lp))) {
            Ok(d) => attach(&d.cell, Box::new(move |_, r| tx.send(r))),
            Err(e) => tx.send(Err(e)),
        });
        aw
    }
}
