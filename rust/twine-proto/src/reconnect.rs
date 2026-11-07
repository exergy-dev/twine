//! [`ReconnectingClient`]: Twisted's `ReconnectingClientFactory` for one loop.
//!
//! The client dials `addr` with [`Loop::connect_tcp`]. Whenever a connect fails
//! or an established connection is lost it waits for an exponentially growing,
//! jittered delay (`Loop::call_later`) and dials again. The n-th consecutive
//! retry waits a uniformly random time in `[0, min(max_delay, initial *
//! factor^(n-1))]` (full jitter; with [`ReconnectOptions::jitter`]`(false)` it
//! waits exactly the ceiling).
//!
//! The protocol built by the user's closure is wrapped in a thin protocol that
//! reports `connection_made` / `connection_lost` back to the shared state, and
//! forwards everything else (including `visit`, so `Loop::with_protocol` and
//! `TransportHandle::call_proto` still reach the inner protocol).
//!
//! A successful connect resets the backoff automatically (as the Go port does;
//! Twisted asks the protocol to call `resetDelay`); [`ReconnectHandle::reset_delay`]
//! restarts it manually.
//!
//! Everything is loop-affine: [`ReconnectHandle`] is `!Send`. For observation
//! from other threads use [`ReconnectHandle::status`].

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use twine::{ConnId, DialOptions, Error, Loop, Protocol, TimerId, Transport};

/// Default initial delay: 1 s.
pub const DEFAULT_INITIAL_DELAY: Duration = Duration::from_secs(1);
/// Default delay ceiling: 60 s.
pub const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(60);
/// Default backoff growth factor: 1.6.
pub const DEFAULT_FACTOR: f64 = 1.6;

/// How many recent retry delays [`ReconnectStatus::delays`] keeps.
pub const DELAY_HISTORY: usize = 64;

type StateCb = Rc<dyn Fn(&mut Loop, bool)>;
type GiveUpCb = Rc<dyn Fn(&mut Loop)>;
type FailCb = Rc<dyn Fn(&mut Loop, &Error)>;
/// Builds the protocol for each successful connect; runs on the loop.
pub type BuildFn = Rc<dyn Fn(&mut Loop, SocketAddr) -> Box<dyn Protocol>>;

/// Configuration for [`ReconnectingClient::start`].
#[derive(Clone)]
pub struct ReconnectOptions {
    initial: Duration,
    max: Duration,
    factor: f64,
    max_retries: u32,
    jitter: bool,
    seed: Option<u64>,
    dial: DialOptions,
    on_state: Option<StateCb>,
    on_give_up: Option<GiveUpCb>,
    on_fail: Option<FailCb>,
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        ReconnectOptions {
            initial: DEFAULT_INITIAL_DELAY,
            max: DEFAULT_MAX_DELAY,
            factor: DEFAULT_FACTOR,
            max_retries: 0,
            jitter: true,
            seed: None,
            dial: DialOptions::default(),
            on_state: None,
            on_give_up: None,
            on_fail: None,
        }
    }
}

impl ReconnectOptions {
    /// Backoff ceiling for the first retry. Default 1 s.
    pub fn initial_delay(mut self, d: Duration) -> Self {
        self.initial = d;
        self
    }

    /// Cap on the backoff ceiling. Default 60 s.
    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max = d;
        self
    }

    /// Growth factor; values below 1 are treated as 1. Default 1.6.
    pub fn factor(mut self, f: f64) -> Self {
        self.factor = f;
        self
    }

    /// Retries at most `n` times after consecutive failures (connect errors or
    /// connection losses with no successful connect in between), i.e. gives up
    /// after `n + 1` consecutive failed attempts. 0, the default, retries forever.
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Full jitter on (the default) or off (the delay is exactly the ceiling).
    pub fn jitter(mut self, on: bool) -> Self {
        self.jitter = on;
        self
    }

    /// Fixes the jitter generator's seed (default: time and address based).
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Options for every `connect_tcp` attempt.
    pub fn dial_options(mut self, d: DialOptions) -> Self {
        self.dial = d;
        self
    }

    /// Called on the loop with `true` after each successful connect (before the
    /// inner protocol's `connection_made`) and `false` after each connection loss
    /// (after the inner protocol's `connection_lost`). When that loss is the one
    /// that makes the client give up, [`ReconnectStatus::gave_up`] is already
    /// true during the `false` call. A connect that completes after
    /// [`ReconnectHandle::stop_trying`] is aborted without any call.
    pub fn on_state_change(mut self, f: impl Fn(&mut Loop, bool) + 'static) -> Self {
        self.on_state = Some(Rc::new(f));
        self
    }

    /// Called on the loop, once, when the client gives up (`max_retries`
    /// exceeded, or the engine/loop is shutting down), after
    /// [`ReconnectStatus::gave_up`] became true and after any final
    /// `on_state_change(false)`. Not called for [`ReconnectHandle::stop_trying`].
    pub fn on_give_up(mut self, f: impl Fn(&mut Loop) + 'static) -> Self {
        self.on_give_up = Some(Rc::new(f));
        self
    }

    /// Called on the loop after every failed connect attempt (not for lost
    /// connections), with the error, before the retry is scheduled or the client
    /// gives up.
    pub fn on_connect_failed(mut self, f: impl Fn(&mut Loop, &Error) + 'static) -> Self {
        self.on_fail = Some(Rc::new(f));
        self
    }
}

/// The thread-safe observable part of a client.
#[derive(Default)]
struct Observed {
    connected: AtomicBool,
    attempts: AtomicU32,
    gave_up: AtomicBool,
    last_error: Mutex<Option<String>>,
    delays: Mutex<VecDeque<Duration>>,
}

/// A `Send + Sync` view of a client's progress, from [`ReconnectHandle::status`].
#[derive(Clone)]
pub struct ReconnectStatus(Arc<Observed>);

impl ReconnectStatus {
    /// True while a connection is established.
    pub fn connected(&self) -> bool {
        self.0.connected.load(Ordering::Acquire)
    }
    /// Total connect attempts started so far.
    pub fn attempts(&self) -> u32 {
        self.0.attempts.load(Ordering::Acquire)
    }
    /// True once the client stopped because `max_retries` was exceeded or the
    /// engine stopped.
    pub fn gave_up(&self) -> bool {
        self.0.gave_up.load(Ordering::Acquire)
    }
    /// The most recent connect or connection-loss error, rendered.
    pub fn last_error(&self) -> Option<String> {
        self.0.last_error.lock().ok().and_then(|g| g.clone())
    }
    /// The most recent retry delays chosen, oldest first: at most
    /// [`DELAY_HISTORY`] (64) are kept.
    pub fn delays(&self) -> Vec<Duration> {
        self.0
            .delays
            .lock()
            .map(|g| g.iter().copied().collect())
            .unwrap_or_default()
    }
}

struct Inner {
    addr: SocketAddr,
    build: BuildFn,
    opts: ReconnectOptions,
    failures: u32,
    stopped: bool,
    timer: Option<TimerId>,
    conn: Option<ConnId>,
    rng: u64,
    obs: Arc<Observed>,
}

type Shared = Rc<RefCell<Inner>>;

/// Namespace for [`start`](Self::start).
pub struct ReconnectingClient;

/// Loop-affine control handle for a running client. Cheap to clone.
#[derive(Clone)]
pub struct ReconnectHandle {
    st: Shared,
}

impl ReconnectingClient {
    /// Starts connecting to `addr` immediately. `build` makes the protocol for
    /// every successful connect (it is called on the loop, once per attempt that
    /// reaches the connected state).
    pub fn start(
        lp: &mut Loop,
        addr: SocketAddr,
        build: BuildFn,
        mut opts: ReconnectOptions,
    ) -> ReconnectHandle {
        if opts.factor < 1.0 || opts.factor.is_nan() {
            opts.factor = 1.0;
        }
        if opts.initial.is_zero() {
            opts.initial = Duration::from_millis(1);
        }
        if opts.max < opts.initial {
            opts.max = opts.initial;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let obs = Arc::new(Observed::default());
        let rng = opts
            .seed
            .unwrap_or(nanos ^ (addr.port() as u64).rotate_left(48) ^ (Arc::as_ptr(&obs) as u64))
            | 1;
        let st = Rc::new(RefCell::new(Inner {
            addr,
            build,
            opts,
            failures: 0,
            stopped: false,
            timer: None,
            conn: None,
            rng,
            obs,
        }));
        connect(lp, &st);
        ReconnectHandle { st }
    }
}

impl ReconnectHandle {
    /// Stops reconnecting: cancels a pending retry; an established connection
    /// stays open but is not retried after it is lost.
    ///
    /// An in-flight dial is **not** cancelled (the OS connect runs to its end),
    /// but its outcome is ignored: if it succeeds the new connection is aborted
    /// at once, without `on_state_change(true)` and without the user's protocol
    /// seeing `connection_made`; if it fails nothing is retried.
    pub fn stop_trying(&self, lp: &mut Loop) {
        let mut s = self.st.borrow_mut();
        s.stopped = true;
        if let Some(t) = s.timer.take() {
            lp.cancel_timer(t);
        }
    }

    /// Aborts the current connection, if any. Unless
    /// [`stop_trying`](Self::stop_trying) was called, the client then reconnects
    /// after the usual backoff (the loss counts as a failure for `max_retries`).
    pub fn disconnect(&self, lp: &mut Loop) {
        let id = self.st.borrow().conn;
        if let Some(mut t) = id.and_then(|id| lp.conn(id)) {
            t.abort();
        }
    }

    /// Restarts the backoff at the initial delay (also done on every successful
    /// connect).
    pub fn reset_delay(&self) {
        self.st.borrow_mut().failures = 0;
    }

    /// True while a connection is established.
    pub fn connected(&self) -> bool {
        self.status().connected()
    }

    /// Total connect attempts started so far.
    pub fn attempts(&self) -> u32 {
        self.status().attempts()
    }

    /// True once the client gave up (`max_retries` exceeded, or engine stopped).
    pub fn gave_up(&self) -> bool {
        self.status().gave_up()
    }

    /// The most recent connect or connection-loss error, rendered.
    pub fn last_error(&self) -> Option<String> {
        self.status().last_error()
    }

    /// The current connection's id, if connected.
    pub fn conn_id(&self) -> Option<ConnId> {
        self.st.borrow().conn
    }

    /// A `Send + Sync` view for observing the client from other threads.
    pub fn status(&self) -> ReconnectStatus {
        ReconnectStatus(self.st.borrow().obs.clone())
    }
}

fn set_last_error(st: &Shared, e: Option<&Error>) {
    if let Some(e) = e {
        if let Ok(mut g) = st.borrow().obs.last_error.lock() {
            *g = Some(e.to_string());
        }
    }
}

/// Marks the client as given up. Does not run `on_give_up`; see [`finish`].
fn mark_give_up(st: &Shared) {
    let mut s = st.borrow_mut();
    s.stopped = true;
    s.obs.gave_up.store(true, Ordering::Release);
}

/// What to do after a failure.
enum Step {
    /// Stopped by the user: do nothing.
    Stop,
    /// Gave up (already marked): run `on_give_up`.
    GiveUp,
    /// Schedule a retry after this delay.
    Retry(Duration),
}

/// Runs the follow-up of a failure, after any state callback has seen it.
fn finish(lp: &mut Loop, st: &Shared, step: Step) {
    match step {
        Step::Stop => {}
        Step::GiveUp => {
            let cb = st.borrow().opts.on_give_up.clone();
            if let Some(cb) = cb {
                cb(lp);
            }
        }
        Step::Retry(d) => schedule(lp, st, d),
    }
}

fn connect(lp: &mut Loop, st: &Shared) {
    let (addr, build, dial) = {
        let s = st.borrow();
        s.obs.attempts.fetch_add(1, Ordering::AcqRel);
        (s.addr, s.build.clone(), s.opts.dial.clone())
    };
    let made = Rc::new(Cell::new(false));
    let (st2, made2) = (st.clone(), made.clone());
    let d = lp.connect_tcp(
        addr,
        move |lp, peer| {
            let inner = build(lp, peer);
            Box::new(ReconnectProto {
                st: st2,
                made: made2,
                refused: false,
                inner,
            })
        },
        dial,
    );
    let st3 = st.clone();
    d.finally(move |lp, r| {
        // Once the connection was made, `connection_lost` drives the retry.
        let Err(e) = r else { return };
        if made.get() {
            return;
        }
        set_last_error(&st3, Some(&e));
        let cb = st3.borrow().opts.on_fail.clone();
        if let Some(cb) = cb {
            cb(lp, &e);
        }
        let step = if matches!(e, Error::EngineStopped | Error::LoopStopped) {
            mark_give_up(&st3);
            Step::GiveUp
        } else {
            next_step(&st3)
        };
        finish(lp, &st3, step);
    });
}

fn ceiling(o: &ReconnectOptions, n: u32) -> Duration {
    let max = o.max.as_secs_f64();
    if o.factor.is_nan() || o.factor <= 1.0 {
        return o.initial.min(o.max);
    }
    let mut c = o.initial.as_secs_f64();
    for _ in 1..n {
        if c >= max {
            break;
        }
        c *= o.factor;
    }
    Duration::from_secs_f64(c.min(max))
}

fn xorshift(x: &mut u64) -> u64 {
    let mut v = *x;
    v ^= v >> 12;
    v ^= v << 25;
    v ^= v >> 27;
    *x = v;
    v.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// Appends to the bounded delay history.
fn push_delay(obs: &Observed, d: Duration) {
    if let Ok(mut g) = obs.delays.lock() {
        if g.len() == DELAY_HISTORY {
            g.pop_front();
        }
        g.push_back(d);
    }
}

/// Counts a failure and decides what comes next (marking a give-up).
fn next_step(st: &Shared) -> Step {
    let mut s = st.borrow_mut();
    if s.stopped {
        return Step::Stop;
    }
    s.failures += 1;
    if s.opts.max_retries > 0 && s.failures > s.opts.max_retries {
        drop(s);
        mark_give_up(st);
        return Step::GiveUp;
    }
    let ceil = ceiling(&s.opts, s.failures);
    let d = if s.opts.jitter {
        let r = xorshift(&mut s.rng) >> 11; // 53 bits
        ceil.mul_f64(r as f64 / (1u64 << 53) as f64)
    } else {
        ceil
    };
    Step::Retry(d)
}

fn schedule(lp: &mut Loop, st: &Shared, delay: Duration) {
    {
        let s = st.borrow();
        if s.stopped {
            return; // a state callback called stop_trying
        }
        push_delay(&s.obs, delay);
    }
    let st2 = st.clone();
    let id = lp.call_later(delay, move |lp| {
        {
            let mut s = st2.borrow_mut();
            s.timer = None;
            if s.stopped {
                return;
            }
        }
        connect(lp, &st2);
    });
    st.borrow_mut().timer = Some(id);
}

fn notify(lp: &mut Loop, st: &Shared, up: bool) {
    let cb = {
        let s = st.borrow();
        s.obs.connected.store(up, Ordering::Release);
        s.opts.on_state.clone()
    };
    if let Some(cb) = cb {
        cb(lp, up);
    }
}

/// Wraps the user's protocol and reports its life to the client.
struct ReconnectProto {
    st: Shared,
    made: Rc<Cell<bool>>,
    /// The client was stopped while this dial was in flight: the connection was
    /// aborted in `connection_made` and the inner protocol never saw it.
    refused: bool,
    inner: Box<dyn Protocol>,
}

impl Protocol for ReconnectProto {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.made.set(true);
        if self.st.borrow().stopped {
            self.refused = true;
            t.abort();
            return;
        }
        {
            let mut s = self.st.borrow_mut();
            s.conn = Some(t.id());
            s.failures = 0;
        }
        notify(t.loop_mut(), &self.st, true);
        self.inner.connection_made(t);
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        self.inner.data_received(t, data);
    }

    fn connection_lost(&mut self, t: &mut Transport<'_>, reason: Option<Error>) {
        if self.refused {
            return;
        }
        // A graceful close during engine stop ends with a plain loss, so also
        // ask the loop; either way there is nothing left to reconnect to.
        let shutdown = matches!(reason, Some(Error::Shutdown)) || t.loop_mut().is_stopping();
        set_last_error(&self.st, reason.as_ref());
        self.inner.connection_lost(t, reason);
        self.st.borrow_mut().conn = None;
        // Decide first, so that `gave_up` is already visible to the callbacks.
        let step = if shutdown {
            mark_give_up(&self.st);
            Step::GiveUp
        } else {
            next_step(&self.st)
        };
        let lp = t.loop_mut();
        notify(lp, &self.st, false);
        finish(lp, &self.st, step);
    }

    fn pause_producing(&mut self, t: &mut Transport<'_>) {
        self.inner.pause_producing(t);
    }

    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        self.inner.resume_producing(t);
    }

    fn stop_producing(&mut self, t: &mut Transport<'_>) {
        self.inner.stop_producing(t);
    }

    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        self.inner.timer_fired(t, token);
    }

    fn visit(
        &mut self,
        t: &mut Transport<'_>,
        f: &mut dyn FnMut(&mut dyn Any, &mut Transport<'_>),
    ) {
        self.inner.visit(t, f);
    }

    fn type_name(&self) -> &'static str {
        self.inner.type_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_growth_and_cap() {
        let o = ReconnectOptions::default()
            .initial_delay(Duration::from_secs(1))
            .factor(2.0)
            .max_delay(Duration::from_secs(5));
        let v: Vec<f64> = (1..=5).map(|n| ceiling(&o, n).as_secs_f64()).collect();
        assert_eq!(v, vec![1.0, 2.0, 4.0, 5.0, 5.0]);
    }

    #[test]
    fn ceiling_without_growth_is_initial() {
        for f in [1.0, 0.5, -3.0, f64::NAN] {
            let o = ReconnectOptions::default()
                .initial_delay(Duration::from_secs(2))
                .max_delay(Duration::from_secs(60))
                .factor(f);
            assert_eq!(ceiling(&o, 1), Duration::from_secs(2));
            assert_eq!(ceiling(&o, u32::MAX), Duration::from_secs(2));
        }
        // No O(n) loop even with huge n and growth that has capped.
        let o = ReconnectOptions::default().factor(2.0);
        assert_eq!(ceiling(&o, u32::MAX), DEFAULT_MAX_DELAY);
    }

    #[test]
    fn delay_history_is_a_ring() {
        let obs = Observed::default();
        for i in 0..(DELAY_HISTORY as u64 + 10) {
            push_delay(&obs, Duration::from_millis(i));
        }
        let g = obs.delays.lock().unwrap();
        assert_eq!(g.len(), DELAY_HISTORY);
        assert_eq!(g.front(), Some(&Duration::from_millis(10)));
        assert_eq!(
            g.back(),
            Some(&Duration::from_millis(DELAY_HISTORY as u64 + 9))
        );
    }

    #[test]
    fn xorshift_spreads() {
        let mut s = 1u64;
        let a = xorshift(&mut s);
        let b = xorshift(&mut s);
        assert_ne!(a, b);
    }
}
