//! [`Engine`], [`EngineHandle`], [`Listener`] and the listener/accept path
//! (design Sections 6.2 and 7.6; Go `engine.go` and `listener.go`).

use std::any::Any;
use std::io::ErrorKind;
use std::net::{SocketAddr, ToSocketAddrs};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicU32, AtomicU8,
    Ordering::{AcqRel, Acquire, Relaxed, SeqCst},
};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::conn::{flags, queue_ready, release_slot, Kind, Table};
use crate::errors::{Error, PanicError, Result};
use crate::event_loop::{thread_main, ConnId, Loop, LoopHandle, LoopLocal, Task, ThreadArgs};
use crate::metrics::{bump, LoopMetrics, MetricsSnapshot};
use crate::options::{ListenOptions, Options};
use crate::protocol::{DatagramProtocol, Factory};

const NEW: u8 = 0;
const RUNNING: u8 = 1;
const STOPPING: u8 = 2;
const STOPPED: u8 = 3;

type LocalInit = dyn Fn() -> Box<dyn Any> + Send + Sync;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) struct EngineInner {
    pub(crate) opts: Arc<Options>,
    pub(crate) loops: Vec<LoopHandle>,
    consumers: Mutex<Vec<Option<twine_mpsc::Consumer<Task>>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) listeners: Mutex<Vec<Arc<ListenerInner>>>,
    hooks: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    state: AtomicU8,
    next: AtomicU32,
    stop_flag: Mutex<bool>,
    stop_cv: Condvar,
    done: Mutex<bool>,
    done_cv: Condvar,
    locals: Mutex<Vec<Arc<LocalInit>>>,
    /// The `defer_to_thread` worker pool, created on first use.
    pub(crate) pool: OnceLock<Arc<crate::pool::Pool>>,
}

/// `Clone + Send + Sync`; everything on [`Engine`] except `run`.
#[derive(Clone)]
pub struct EngineHandle(pub(crate) Arc<EngineInner>);

/// Owns the loops, listeners and lifecycle. `new`, then `listen_*`, then `run`
/// (which blocks until `stop`).
pub struct Engine {
    h: EngineHandle,
}

/// Builder-style entry: `Engine::builder().loops(2).build()`.
impl Options {
    /// Creates the engine (same as [`Engine::new`]).
    pub fn build(self) -> Result<Engine> {
        Engine::new(self)
    }
}

impl Engine {
    /// Creates an engine. Loops exist as queues now (so `listen_tcp` and
    /// `call_from_thread` work before `run`); their threads start in `run`.
    pub fn new(opts: Options) -> Result<Engine> {
        if cfg!(panic = "abort") {
            log::warn!("twine: built with panic = \"abort\": protocol panics cannot be contained");
        }
        let opts = opts.finalize();
        if opts.loops == 0 || opts.loops > u16::MAX as usize {
            return Err(Error::Addr(format!("invalid loop count {}", opts.loops)));
        }
        let epoch = Instant::now();
        let mut loops = Vec::with_capacity(opts.loops);
        let mut consumers = Vec::with_capacity(opts.loops);
        for i in 0..opts.loops {
            let (tx, rx) = twine_mpsc::channel::<Task>(opts.task_queue_size.max(2));
            loops.push(LoopHandle::new(i, tx, Arc::new(LoopMetrics::new(i, epoch))));
            consumers.push(Some(rx));
        }
        Ok(Engine {
            h: EngineHandle(Arc::new(EngineInner {
                opts: Arc::new(opts),
                loops,
                consumers: Mutex::new(consumers),
                threads: Mutex::new(Vec::new()),
                listeners: Mutex::new(Vec::new()),
                hooks: Mutex::new(Vec::new()),
                state: AtomicU8::new(NEW),
                next: AtomicU32::new(0),
                stop_flag: Mutex::new(false),
                stop_cv: Condvar::new(),
                done: Mutex::new(false),
                done_cv: Condvar::new(),
                locals: Mutex::new(Vec::new()),
                pool: OnceLock::new(),
            })),
        })
    }

    /// Default options, for builder chaining: `Engine::builder().loops(1).build()`.
    pub fn builder() -> Options {
        Options::default()
    }

    /// A cloneable, thread-safe handle to this engine.
    pub fn handle(&self) -> EngineHandle {
        self.h.clone()
    }

    /// Starts the loops and blocks until `stop()`. Returns the error that
    /// prevented a loop from starting, otherwise Ok after a clean shutdown.
    /// Returns Ok immediately if `stop()` was called before `run`.
    pub fn run(&self) -> Result<()> {
        self.h.run()
    }

    /// See [`EngineHandle::listen_tcp`].
    pub fn listen_tcp(
        &self,
        addr: impl ToSocketAddrs,
        f: impl Factory,
        opts: ListenOptions,
    ) -> Result<Listener> {
        self.h.listen_tcp(addr, f, opts)
    }

    /// See [`EngineHandle::listen_udp`].
    pub fn listen_udp(
        &self,
        addr: impl ToSocketAddrs,
        build: impl FnOnce(&mut Loop) -> Box<dyn DatagramProtocol> + Send + 'static,
        opts: ListenOptions,
    ) -> Result<Listener> {
        self.h.listen_udp(addr, build, opts)
    }

    /// One handle per loop.
    pub fn loop_handles(&self) -> Vec<LoopHandle> {
        self.h.loop_handles()
    }

    /// Picks a loop round-robin.
    pub fn next_loop(&self) -> LoopHandle {
        self.h.next_loop()
    }

    /// Declares per-loop user state; see [`EngineHandle::loop_local`].
    pub fn loop_local<T: 'static>(
        &self,
        init: impl Fn() -> T + Send + Sync + 'static,
    ) -> LoopLocal<T> {
        self.h.loop_local(init)
    }

    /// Registers a hook to run once when shutdown begins.
    pub fn on_shutdown(&self, f: impl FnOnce() + Send + 'static) {
        self.h.on_shutdown(f)
    }

    /// Begins graceful shutdown; idempotent; valid before `run`.
    pub fn stop(&self) {
        self.h.stop()
    }

    /// A snapshot for every loop.
    pub fn metrics(&self) -> Vec<MetricsSnapshot> {
        self.h.metrics()
    }

    /// True once the engine has fully stopped.
    pub fn is_stopped(&self) -> bool {
        self.h.is_stopped()
    }
}

impl EngineHandle {
    fn state(&self) -> u8 {
        self.0.state.load(Acquire)
    }

    /// True once shutdown has begun (SeqCst: pairs with the pool hand-off in `shutdown`).
    pub(crate) fn is_stopping_or_stopped(&self) -> bool {
        self.0.state.load(SeqCst) >= STOPPING
    }

    fn run(&self) -> Result<()> {
        let inner = &*self.0;
        match inner.state.compare_exchange(NEW, RUNNING, AcqRel, Acquire) {
            Ok(_) => {}
            Err(STOPPED) => return Ok(()),
            Err(_) => return Err(Error::Io(std::io::Error::other("Engine::run called twice"))),
        }
        let consumers: Vec<_> = lock(&inner.consumers)
            .iter_mut()
            .map(|c| c.take())
            .collect();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<()>>();
        let mut start_err: Option<Error> = None;
        let mut spawned = 0usize;
        for (i, rx) in consumers.into_iter().enumerate() {
            let Some(tasks) = rx else { continue };
            let args = ThreadArgs {
                id: i,
                engine: self.clone(),
                opts: inner.opts.clone(),
                handle: inner.loops[i].clone(),
                tasks,
                ready: ready_tx.clone(),
            };
            match std::thread::Builder::new()
                .name(format!("twine-loop-{i}"))
                .spawn(move || thread_main(args))
            {
                Ok(j) => {
                    lock(&inner.threads).push(j);
                    spawned += 1;
                }
                Err(e) => {
                    start_err = Some(Error::Io(e));
                    // No thread will ever drain these loops' queues.
                    for l in &inner.loops[i..] {
                        l.0.stopped.store(true, SeqCst);
                    }
                    break;
                }
            }
        }
        drop(ready_tx);
        for _ in 0..spawned {
            match ready_rx.recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    start_err.get_or_insert(e);
                }
                Err(_) => {
                    start_err.get_or_insert(Error::Io(std::io::Error::other(
                        "loop thread died during startup",
                    )));
                }
            }
        }
        if let Some(e) = start_err {
            self.shutdown();
            return Err(e);
        }
        // One watchdog per engine, exiting with it (`Options::watchdog(None)` starts none).
        let _ = crate::watchdog::start(self);
        {
            let mut g = lock(&inner.stop_flag);
            while !*g {
                g = inner.stop_cv.wait(g).unwrap_or_else(|e| e.into_inner());
            }
        }
        self.shutdown();
        Ok(())
    }

    /// Design Section 8 rule 7: close listeners, run hooks, shut loops down, join.
    fn shutdown(&self) {
        let inner = &*self.0;
        inner.state.store(STOPPING, SeqCst);
        let listeners: Vec<_> = std::mem::take(&mut *lock(&inner.listeners));
        for l in &listeners {
            l.close();
        }
        let hooks: Vec<_> = std::mem::take(&mut *lock(&inner.hooks));
        for h in hooks {
            if let Err(p) = catch_unwind(AssertUnwindSafe(h)) {
                log::error!(
                    "twine: panic in on_shutdown hook: {}",
                    PanicError::from_payload(p).message
                );
            }
        }
        // Cancel queued blocking jobs while the loops still run, so their
        // Deferreds reject with `EngineStopped` on a live loop.
        if let Some(p) = inner.pool.get() {
            p.shutdown();
        }
        for l in &inner.loops {
            let _ = l.submit_task(Task::Shutdown);
        }
        let threads: Vec<_> = std::mem::take(&mut *lock(&inner.threads));
        for t in threads {
            if t.join().is_err() {
                log::error!("twine: a loop thread panicked");
            }
        }
        inner.state.store(STOPPED, SeqCst);
        *lock(&inner.done) = true;
        inner.done_cv.notify_all();
    }

    /// Binds `addr` and starts accepting on every loop (one SO_REUSEPORT listener
    /// per loop) or, when `reuse_port` is off or there is one loop, on loop 0 with
    /// accepted connections handed to loops round-robin. Works before and after `run`.
    pub fn listen_tcp(
        &self,
        addr: impl ToSocketAddrs,
        f: impl Factory,
        opts: ListenOptions,
    ) -> Result<Listener> {
        if self.state() >= STOPPING {
            return Err(Error::EngineStopped);
        }
        let mut addr = resolve(addr)?;
        let inner = &*self.0;
        let loops = inner.loops.len();
        let n = if inner.opts.reuse_port && loops > 1 {
            loops
        } else {
            1
        };
        let mut socks: Vec<(i32, SocketAddr)> = Vec::with_capacity(n);
        for _ in 0..n {
            match twine_sock::listen_tcp(addr, inner.opts.reuse_port, opts.backlog) {
                Ok((fd, laddr)) => {
                    if let Some(d) = opts.defer_accept {
                        let _ = twine_sock::set_defer_accept(fd, d);
                    }
                    if socks.is_empty() && n > 1 && laddr.port() != 0 {
                        addr = laddr; // bind the remaining sockets to the resolved port
                    }
                    socks.push((fd, laddr));
                }
                Err(e) => {
                    for (fd, _) in socks {
                        twine_sock::close(fd);
                    }
                    return Err(e.into());
                }
            }
        }
        let bound = socks[0].1;
        let lst = Arc::new(ListenerInner {
            addr: bound,
            factory: Some(Arc::new(f)),
            opts,
            udp: false,
            closed: AtomicBool::new(false),
            fds: socks
                .iter()
                .enumerate()
                .map(|(i, (fd, _))| ListenerFd {
                    lh: inner.loops[i].clone(),
                    fd: AtomicI32::new(*fd),
                })
                .collect(),
            engine: Arc::downgrade(&self.0),
        });
        self.register_listener(&lst);
        Ok(Listener { inner: lst })
    }

    /// Posts the per-loop registration of a listener and records it.
    pub(crate) fn register_listener(&self, lst: &Arc<ListenerInner>) {
        lock(&self.0.listeners).push(lst.clone());
        for (idx, f) in lst.fds.iter().enumerate() {
            let l2 = lst.clone();
            if f.lh
                .call_from_thread(move |lp| lp.register_listener(&l2, idx))
                .is_err()
            {
                let fd = f.fd.swap(-1, SeqCst);
                if fd >= 0 {
                    twine_sock::close(fd);
                }
            }
        }
    }

    /// Binds a UDP socket on one loop and delivers datagrams to the protocol
    /// built by `build`.
    pub fn listen_udp(
        &self,
        addr: impl ToSocketAddrs,
        build: impl FnOnce(&mut Loop) -> Box<dyn DatagramProtocol> + Send + 'static,
        opts: ListenOptions,
    ) -> Result<Listener> {
        if self.state() >= STOPPING {
            return Err(Error::EngineStopped);
        }
        let addr = resolve(addr)?;
        crate::udp::listen(self, addr, Box::new(build), opts)
    }

    /// One handle per loop.
    pub fn loop_handles(&self) -> Vec<LoopHandle> {
        self.0.loops.clone()
    }

    /// Picks a loop round-robin, for outbound connections.
    pub fn next_loop(&self) -> LoopHandle {
        let n = self.0.next.fetch_add(1, Relaxed) as usize;
        self.0.loops[n % self.0.loops.len()].clone()
    }

    /// Declares per-loop user state. `init` runs lazily, once per loop, on that
    /// loop's thread the first time `Loop::local(key)` is called.
    pub fn loop_local<T: 'static>(
        &self,
        init: impl Fn() -> T + Send + Sync + 'static,
    ) -> LoopLocal<T> {
        let mut g = lock(&self.0.locals);
        g.push(Arc::new(move || Box::new(init()) as Box<dyn Any>));
        LoopLocal {
            idx: g.len() - 1,
            _p: std::marker::PhantomData,
        }
    }

    pub(crate) fn local_init(&self, idx: usize) -> Arc<LocalInit> {
        lock(&self.0.locals)[idx].clone()
    }

    /// Registers a hook run once, on the `run` thread, after listeners close and
    /// before connections are closed. A panic in a hook is logged.
    pub fn on_shutdown(&self, f: impl FnOnce() + Send + 'static) {
        lock(&self.0.hooks).push(Box::new(f));
    }

    /// Begins graceful shutdown. Idempotent, non-blocking, safe from any thread
    /// (including loop callbacks) and valid before `run` (then `run` returns Ok
    /// immediately and the listener sockets are closed).
    pub fn stop(&self) {
        let inner = &*self.0;
        if inner
            .state
            .compare_exchange(NEW, STOPPED, AcqRel, Acquire)
            .is_ok()
        {
            // Never ran: release the listener sockets created before `run`.
            let ls: Vec<_> = std::mem::take(&mut *lock(&inner.listeners));
            for l in ls {
                l.closed.store(true, SeqCst);
                for f in &l.fds {
                    let fd = f.fd.swap(-1, SeqCst);
                    if fd >= 0 {
                        twine_sock::close(fd);
                    }
                }
            }
            // No loop thread will ever drain these queues.
            for l in &inner.loops {
                l.0.stopped.store(true, SeqCst);
            }
            *lock(&inner.done) = true;
            inner.done_cv.notify_all();
            return;
        }
        let mut g = lock(&inner.stop_flag);
        *g = true;
        inner.stop_cv.notify_all();
    }

    /// A snapshot for every loop.
    pub fn metrics(&self) -> Vec<MetricsSnapshot> {
        self.0.loops.iter().map(|l| l.metrics()).collect()
    }

    /// True once the engine has fully stopped.
    pub fn is_stopped(&self) -> bool {
        self.state() == STOPPED
    }

    /// Blocks until the engine has stopped (`run` has finished shutting down).
    pub fn wait(&self) {
        let mut g = lock(&self.0.done);
        while !*g {
            g = self.0.done_cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }
}

fn resolve(addr: impl ToSocketAddrs) -> Result<SocketAddr> {
    addr.to_socket_addrs()
        .map_err(|e| Error::Addr(e.to_string()))?
        .next()
        .ok_or_else(|| Error::Addr("address resolved to nothing".into()))
}

/// One listening socket and the loop it is registered on.
pub(crate) struct ListenerFd {
    pub(crate) lh: LoopHandle,
    /// The fd, or -1 once closed. Taken with `swap(-1)` by whoever closes it.
    pub(crate) fd: AtomicI32,
}

/// State shared by every loop a listener lives on.
pub(crate) struct ListenerInner {
    pub(crate) addr: SocketAddr,
    pub(crate) factory: Option<Arc<dyn Factory>>,
    pub(crate) opts: ListenOptions,
    /// True for UDP listeners (filled in by `udp.rs`).
    #[allow(dead_code)] // read by the UDP implementation (Wave 2b)
    pub(crate) udp: bool,
    pub(crate) closed: AtomicBool,
    pub(crate) fds: Vec<ListenerFd>,
    pub(crate) engine: Weak<EngineInner>,
}

impl ListenerInner {
    /// Claims this loop's fd entry: returns the fd and leaves -1 behind, so
    /// exactly one closer ever calls `close(2)` on it. Returns -1 if already claimed.
    pub(crate) fn take_fd(&self, loop_id: usize) -> i32 {
        self.fds
            .iter()
            .find(|f| f.lh.id() == loop_id)
            .map_or(-1, |f| f.fd.swap(-1, SeqCst))
    }

    /// Stops accepting: closes the listening fd on each owning loop.
    pub(crate) fn close(self: &Arc<Self>) {
        if self.closed.swap(true, SeqCst) {
            return;
        }
        if let Some(e) = self.engine.upgrade() {
            lock(&e.listeners).retain(|x| !Arc::ptr_eq(x, self));
        }
        for f in &self.fds {
            let me = self.clone();
            // If the loop has exited its shutdown already closed the fd; any
            // leftover is closed by Drop.
            let _ = f.lh.call_from_thread(move |lp| lp.close_listener(&me));
        }
    }
}

impl Drop for ListenerInner {
    fn drop(&mut self) {
        for f in &self.fds {
            let fd = f.fd.swap(-1, SeqCst);
            if fd >= 0 {
                twine_sock::close(fd);
            }
        }
    }
}

/// A bound TCP or UDP socket. `close()` stops accepting; existing connections
/// are unaffected. Safe from any thread.
#[derive(Clone)]
pub struct Listener {
    pub(crate) inner: Arc<ListenerInner>,
}

impl Listener {
    /// The bound address (the OS-assigned port if port 0 was requested).
    pub fn addr(&self) -> SocketAddr {
        self.inner.addr
    }

    /// Stops accepting; completes asynchronously on each loop. Idempotent.
    pub fn close(&self) {
        self.inner.close();
    }
}

/// Owns an accepted fd until it is attached to a connection; closes it if the
/// hand-off closure is dropped without running.
struct FdGuard(i32);

impl FdGuard {
    fn take(&mut self) -> i32 {
        std::mem::replace(&mut self.0, -1)
    }
}

impl Drop for FdGuard {
    fn drop(&mut self) {
        if self.0 >= 0 {
            twine_sock::close(self.0);
        }
    }
}

impl Loop {
    /// Adds a listening fd to this loop's poller (runs as a task on the loop).
    pub(crate) fn register_listener(&mut self, lst: &Arc<ListenerInner>, idx: usize) {
        let fd = lst.fds[idx].fd.load(SeqCst);
        if fd < 0 {
            return;
        }
        if self.rt.stopping || lst.closed.load(SeqCst) {
            let fd = lst.fds[idx].fd.swap(-1, SeqCst);
            if fd >= 0 {
                twine_sock::close(fd);
            }
            return;
        }
        let i = fd as usize;
        self.claim_slot(i);
        let id = self.id_at(i);
        let Loop { conns, rt } = &mut *self;
        let io = &mut conns.slot(i as u32).io;
        debug_assert!(io.kind == Kind::Empty);
        io.fd = fd;
        io.kind = Kind::Listener;
        io.flags = 0;
        io.listener = Some(lst.clone());
        io.host = lst.addr;
        rt.nconns += 1;
        if let Err(e) = rt.poller.add(fd, twine_poll::READABLE) {
            log::error!("twine: registering listener on loop {}: {e}", rt.id);
            release_slot(conns, rt, i);
            rt.nconns -= 1;
            let fd = lst.fds[idx].fd.swap(-1, SeqCst);
            if fd >= 0 {
                twine_sock::close(fd);
            }
            return;
        }
        // Drain any backlog that accumulated before registration (edge-triggered
        // epoll would otherwise wait for the next arrival).
        self.handle_accept(id);
    }

    /// Closes this loop's socket of `lst`, wherever it is in its registration.
    pub(crate) fn close_listener(&mut self, lst: &Arc<ListenerInner>) {
        let Some(entry) = lst.fds.iter().find(|f| f.lh.id() == self.rt.id) else {
            return;
        };
        // Peek first: a UDP socket is closed by `close_udp`, which claims the
        // entry itself (see `ListenerInner::take_fd`), so the entry must not be
        // swapped here or the fd would never be closed.
        let fd = entry.fd.load(SeqCst);
        if fd < 0 {
            return;
        }
        // A UDP socket owned by this listener: its slot owns the fd and
        // `close_udp` runs `stop_protocol`, unregisters and closes it.
        if let Some(slot) = self.conns.get(fd as usize).and_then(|e| e.slot.as_deref()) {
            let udp_mine = slot.io.kind == Kind::Udp
                && slot
                    .io
                    .udp
                    .as_deref()
                    .is_some_and(|st| Arc::ptr_eq(st.listener(), lst));
            if udp_mine {
                let id = self.id_at(fd as usize);
                crate::udp::close_udp(self, id);
                return;
            }
        }
        // TCP listener (or a socket never registered): claim and close.
        let fd = entry.fd.swap(-1, SeqCst);
        if fd < 0 {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        if let Some(slot) = conns
            .get_mut(fd as usize)
            .and_then(|e| e.slot.as_deref_mut())
        {
            let mine = slot.io.kind == Kind::Listener
                && slot
                    .io
                    .listener
                    .as_ref()
                    .is_some_and(|l| Arc::ptr_eq(l, lst));
            if mine {
                let _ = rt.poller.delete(fd);
                release_slot(conns, rt, fd as usize);
                rt.nconns -= 1;
            }
        }
        twine_sock::close(fd);
    }

    /// Used by shutdown: close the listener registered in slot `id`.
    pub(crate) fn close_listener_fd(&mut self, id: ConnId) {
        let Some(lst) = self.io(id).and_then(|io| io.listener.clone()) else {
            return;
        };
        self.close_listener(&lst);
    }

    /// Accepts until EAGAIN or `max_accepts_per_event`.
    pub(crate) fn handle_accept(&mut self, id: ConnId) {
        let (lfd, lst) = match self.io(id) {
            Some(io) if io.kind == Kind::Listener && io.fd >= 0 => match &io.listener {
                Some(l) => (io.fd, l.clone()),
                None => return,
            },
            _ => return,
        };
        let Some(factory) = lst.factory.as_deref() else {
            return;
        };
        let max = self.rt.opts.max_accepts_per_event;
        let direct = self.rt.opts.reuse_port || self.rt.engine.0.loops.len() == 1;
        for _ in 0..max {
            match twine_sock::accept(lfd) {
                Ok((fd, peer)) => {
                    bump(&self.rt.metrics.accepts);
                    self.setup_accepted(fd, &lst);
                    if direct {
                        self.attach_tcp(fd, peer, lst.addr, factory, lst.opts.no_delay);
                        continue;
                    }
                    // Single listener: hand the connection to the next loop.
                    let target = self.rt.engine.next_loop();
                    if target.id() == self.rt.id {
                        self.attach_tcp(fd, peer, lst.addr, factory, lst.opts.no_delay);
                        continue;
                    }
                    let mut guard = FdGuard(fd);
                    let l2 = lst.clone();
                    let _ = target.call_from_thread(move |lp| {
                        let fd = guard.take();
                        lp.attach_accepted(fd, peer, &l2);
                    });
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) => {
                    bump(&self.rt.metrics.accept_errors);
                    if matches!(
                        e.raw_os_error(),
                        Some(libc::EMFILE)
                            | Some(libc::ENFILE)
                            | Some(libc::ENOBUFS)
                            | Some(libc::ENOMEM)
                    ) {
                        log::warn!("twine: accept failed on loop {}: {e}", self.rt.id);
                        self.schedule_accept_retry(id);
                        return;
                    }
                }
            }
        }
        // Cap hit; more may be pending. Re-check next iteration.
        let Loop { conns, rt } = &mut *self;
        queue_ready(rt, &mut conns.slot(id.fd).io, id);
    }

    /// Edge-triggered epoll will not report the connection still in the backlog
    /// after EMFILE, so retry shortly (cold path; one pending retry per listener).
    fn schedule_accept_retry(&mut self, id: ConnId) {
        let Some(io) = self.io_mut(id) else { return };
        if io.flags & flags::ACCEPT_RETRY != 0 {
            return;
        }
        io.flags |= flags::ACCEPT_RETRY;
        self.call_later(Duration::from_millis(10), move |lp| {
            if let Some(io) = lp.io_mut(id) {
                io.flags &= !flags::ACCEPT_RETRY;
            }
            lp.handle_accept(id);
        });
    }

    /// Per-socket options for an accepted fd.
    fn setup_accepted(&mut self, fd: i32, lst: &ListenerInner) {
        if lst.opts.no_delay {
            let _ = twine_sock::set_nodelay(fd, true);
        }
        if lst.opts.rcv_buf > 0 || lst.opts.snd_buf > 0 {
            let _ = twine_sock::set_buffers(fd, lst.opts.rcv_buf, lst.opts.snd_buf);
        }
        if let Some(d) = self.rt.opts.busy_poll {
            let _ = twine_sock::set_busy_poll(fd, d);
        }
    }

    /// The target-loop half of the single-listener hand-off.
    fn attach_accepted(&mut self, fd: i32, peer: SocketAddr, lst: &Arc<ListenerInner>) {
        let Some(factory) = lst.factory.as_deref() else {
            twine_sock::close(fd);
            return;
        };
        if self.rt.stopping {
            twine_sock::close(fd);
            return;
        }
        self.attach_tcp(fd, peer, lst.addr, factory, lst.opts.no_delay);
    }
}
