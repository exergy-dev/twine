//! [`Transport`] (the loop-affine per-callback view of a connection),
//! [`TransportHandle`] (the stored, cross-thread handle), [`Payload`] and the
//! operations a handle can queue (design Sections 6.1 and 8).

use std::any::Any;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::conn::{flags, Kind, Table};
use crate::errors::{Error, Result};
use crate::event_loop::{ConnId, Loop, LoopHandle, Task, TimerId};
use crate::protocol::{Producer, Protocol, WriteFilter};
use crate::timer::TimerKind;

/// The loop-affine view of a connection, built on the stack for the duration of
/// one callback. `!Send`; cannot outlive the callback. Also obtainable for any
/// other connection on the same loop via [`Loop::conn`].
pub struct Transport<'a> {
    pub(crate) lp: &'a mut Loop,
    pub(crate) id: ConnId,
    pub(crate) filter: Option<&'a mut dyn WriteFilter>,
    pub(crate) foreign: bool,
}

impl<'a> Transport<'a> {
    /// The view a protocol sees in its own callbacks.
    #[inline]
    pub(crate) fn new(lp: &'a mut Loop, id: ConnId) -> Transport<'a> {
        Transport {
            lp,
            id,
            filter: None,
            foreign: false,
        }
    }

    /// Copies to the outbound ring only if an immediate write(2) would block.
    /// The caller may reuse `b` as soon as `write` returns.
    pub fn write(&mut self, b: &[u8]) {
        if let Some(f) = self.filter.as_deref_mut() {
            let mut raw = Transport {
                lp: &mut *self.lp,
                id: self.id,
                filter: None,
                foreign: false,
            };
            f.write(&mut raw, b);
            return;
        }
        if self.foreign && self.is_filtered() {
            self.queue_write(Payload::Owned(b.to_vec()));
            return;
        }
        self.lp.conn_write(self.id, b);
    }

    /// Writes several slices with one writev(2) when possible.
    pub fn writev(&mut self, bufs: &[&[u8]]) {
        if let Some(f) = self.filter.as_deref_mut() {
            let mut raw = Transport {
                lp: &mut *self.lp,
                id: self.id,
                filter: None,
                foreign: false,
            };
            f.writev(&mut raw, bufs);
            return;
        }
        if self.foreign && self.is_filtered() {
            let mut v = Vec::with_capacity(bufs.iter().map(|b| b.len()).sum());
            for b in bufs {
                v.extend_from_slice(b);
            }
            self.queue_write(Payload::Owned(v));
            return;
        }
        self.lp.conn_writev(self.id, bufs);
    }

    fn is_filtered(&self) -> bool {
        self.lp
            .io(self.id)
            .is_some_and(|io| io.flags & flags::FILTERED != 0)
    }

    fn queue_write(&mut self, p: Payload) {
        crate::event_loop::pending_push(crate::event_loop::Pending::Op(self.id, ConnOp::Write(p)));
    }

    /// Half-close after flushing pending writes (SHUT_WR, then close when the
    /// peer closes or after `Options::close_linger`).
    pub fn lose_connection(&mut self) {
        if let Some(f) = self.filter.as_deref_mut() {
            let mut raw = Transport {
                lp: &mut *self.lp,
                id: self.id,
                filter: None,
                foreign: false,
            };
            f.lose_connection(&mut raw);
            return;
        }
        if self.foreign && self.is_filtered() {
            // Reach the wrapper's `WriteFilter::lose_connection` (close_notify).
            crate::event_loop::pending_push(crate::event_loop::Pending::Op(
                self.id,
                ConnOp::LoseConnection,
            ));
            return;
        }
        self.lp.lose_connection(self.id);
    }

    /// Immediate close; pending writes discarded; `connection_lost(None)` is
    /// delivered at the end of the iteration.
    pub fn abort(&mut self) {
        self.lp.abort(self.id);
    }

    /// The remote address (unspecified for a stale id).
    pub fn peer(&self) -> SocketAddr {
        self.lp
            .io(self.id)
            .map_or(crate::conn::UNSPECIFIED, |io| io.peer)
    }

    /// The local address (the listener's address for accepted connections).
    pub fn host(&self) -> SocketAddr {
        self.lp
            .io(self.id)
            .map_or(crate::conn::UNSPECIFIED, |io| io.host)
    }

    /// This connection's id.
    pub fn id(&self) -> ConnId {
        self.id
    }

    /// A cheap `Send + Sync` handle for storing or cross-thread use.
    pub fn handle(&self) -> TransportHandle {
        TransportHandle {
            lp: self.lp.rt.handle.clone(),
            id: self.id,
        }
    }

    /// The owning loop.
    pub fn loop_mut(&mut self) -> &mut Loop {
        self.lp
    }

    /// The owning loop's cross-thread handle.
    pub fn loop_handle(&self) -> &LoopHandle {
        &self.lp.rt.handle
    }

    /// The loop's cached iteration time.
    pub fn now(&self) -> Instant {
        self.lp.rt.now
    }

    /// Arms an idle timer on the timing wheel; `None` disables. Any read or write
    /// resets it. Expiry aborts with `Error::IdleTimeout`.
    pub fn set_idle_timeout(&mut self, d: Option<Duration>) {
        let id = self.id;
        let Loop { conns, rt } = &mut *self.lp;
        let Some(s) = conns.find(id) else {
            return;
        };
        if id.loop_id as usize != rt.id || s.io.kind != Kind::Tcp || s.io.flags & flags::CLOSED != 0
        {
            return;
        }
        // The idle timer is the half-close linger deadline: leave it alone.
        if s.io.flags & flags::LINGER != 0 {
            return;
        }
        let io = &mut s.io;
        match d.filter(|d| !d.is_zero()) {
            Some(d) => {
                io.idle_timeout = Some(d);
                crate::conn::arm_idle(rt, io, id, d);
            }
            None => {
                io.idle_timeout = None;
                if let Some(t) = io.idle.take() {
                    rt.wheel.remove(t);
                }
            }
        }
    }

    /// Connection-scoped timer: `Protocol::timer_fired(t, token)` after `d`. No
    /// closure, no allocation; inert once the connection closes. `ZERO` = next
    /// iteration.
    pub fn call_later(&mut self, d: Duration, token: u64) -> TimerId {
        let id = self.id;
        let tid = self.lp.add_timer(d, TimerKind::Conn(id, token));
        // Tracked so the close path can take it off the wheel.
        match self.lp.io_mut(id) {
            Some(io) if io.kind == Kind::Tcp && io.flags & flags::CLOSED == 0 => {
                io.timers.push(tid);
            }
            _ => {
                self.lp.rt.wheel.remove(tid);
            }
        }
        tid
    }

    /// Cancels a timer; true if it was still pending.
    pub fn cancel_timer(&mut self, t: TimerId) -> bool {
        self.lp.cancel_timer(t)
    }

    /// Drops read interest; `data_received` stops being called.
    pub fn pause_reading(&mut self) {
        self.lp.pause_reading(self.id);
    }

    /// Restores read interest and performs one immediate read (at the start of
    /// the next iteration, without blocking) so an edge missed while paused is
    /// not lost.
    pub fn resume_reading(&mut self) {
        self.lp.resume_reading(self.id);
    }

    /// Attaches a producer. A streaming (push) producer is paused above
    /// `write_high_water` and resumed below `write_low_water`; a non-streaming
    /// (pull) producer gets `resume_producing` whenever the ring drains, and once
    /// immediately. Notifications are delivered after the current callback.
    /// Panics if a producer is already registered.
    pub fn register_producer(&mut self, p: Producer, streaming: bool) {
        self.lp.register_producer(self.id, p, streaming);
    }

    /// Detaches the producer.
    pub fn unregister_producer(&mut self) {
        self.lp.unregister_producer(self.id);
    }

    /// Bytes buffered in the outbound ring (below any installed filter).
    pub fn pending_write(&self) -> usize {
        self.lp.io(self.id).map_or(0, |io| io.ring.len())
    }

    /// Like [`pending_write`](Self::pending_write) but asks the installed
    /// [`WriteFilter`] (if any) how many bytes it holds, which `&self` cannot do.
    pub fn pending_write_filtered(&mut self) -> usize {
        match self.filter.as_deref() {
            Some(f) => {
                let raw = Transport {
                    lp: &mut *self.lp,
                    id: self.id,
                    filter: None,
                    foreign: false,
                };
                f.pending_write(&raw)
            }
            None => self.pending_write(),
        }
    }

    /// `pending_write() > write_high_water`: the synchronous check (producer
    /// notifications themselves are delivered after the current callback).
    pub fn write_blocked(&self) -> bool {
        self.pending_write() > self.lp.rt.opts.write_high_water
    }

    /// True while the connection can still carry I/O (not closed, not stale).
    pub fn is_open(&self) -> bool {
        self.lp
            .io(self.id)
            .is_some_and(|io| io.flags & flags::CLOSED == 0)
    }

    /// True while reading is paused ([`pause_reading`](Self::pause_reading))
    /// or after the peer's FIN was seen; false for a closed connection.
    pub fn is_reading_paused(&self) -> bool {
        self.lp
            .io(self.id)
            .is_some_and(|io| io.flags & (flags::CLOSED | flags::READ_PAUSED) == flags::READ_PAUSED)
    }

    /// True once `lose_connection` was called, the peer closed, or the
    /// connection is closed.
    pub fn is_closing(&self) -> bool {
        self.lp
            .io(self.id)
            .is_none_or(|io| io.flags & (flags::CLOSED | flags::CLOSING) != 0)
    }

    /// Toggles TCP_NODELAY on the socket.
    pub fn set_nodelay(&mut self, on: bool) -> Result<()> {
        let id = self.id;
        match self.lp.io_mut(id) {
            Some(io) if io.flags & flags::CLOSED == 0 => {
                twine_sock::set_nodelay(io.fd, on)?;
                if on {
                    io.flags |= flags::NODELAY;
                } else {
                    io.flags &= !flags::NODELAY;
                }
                Ok(())
            }
            _ => Err(Error::Closed),
        }
    }

    /// The socket fd while open, for setsockopt-style use only (kTLS).
    pub fn fd(&self) -> Option<RawFd> {
        self.lp
            .io(self.id)
            .filter(|io| io.flags & flags::CLOSED == 0)
            .map(|io| io.fd)
    }

    /// Marks this connection as filtered: writes from other connections' views and
    /// from handles are routed through the protocol's `visit` so the wrapper's
    /// `WriteFilter` sees them.
    pub fn set_filtered(&mut self, on: bool) {
        if let Some(io) = self.lp.io_mut(self.id) {
            if on {
                io.flags |= flags::FILTERED;
            } else {
                io.flags &= !flags::FILTERED;
            }
        }
    }

    /// A shorter-lived copy of this view (keeps the installed filter).
    pub fn reborrow(&mut self) -> Transport<'_> {
        Transport {
            lp: &mut *self.lp,
            id: self.id,
            filter: self.filter.as_deref_mut(),
            foreign: self.foreign,
        }
    }

    /// A view of the same connection whose writes pass through `f`.
    pub fn with_filter<'b>(&'b mut self, f: &'b mut dyn WriteFilter) -> Transport<'b> {
        Transport {
            lp: &mut *self.lp,
            id: self.id,
            filter: Some(f),
            foreign: self.foreign,
        }
    }
}

/// Bytes queued through a [`TransportHandle`].
pub enum Payload {
    /// An owned buffer.
    Owned(Vec<u8>),
    /// A shared buffer (the cross-loop broadcast currency).
    Shared(Arc<[u8]>),
    /// A static buffer.
    Static(&'static [u8]),
}

impl Payload {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Payload::Owned(v) => v,
            Payload::Shared(a) => a,
            Payload::Static(s) => s,
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Payload::Owned(v)
    }
}
impl From<String> for Payload {
    fn from(v: String) -> Self {
        Payload::Owned(v.into_bytes())
    }
}
impl From<Arc<[u8]>> for Payload {
    fn from(v: Arc<[u8]>) -> Self {
        Payload::Shared(v)
    }
}
impl From<&'static [u8]> for Payload {
    fn from(v: &'static [u8]) -> Self {
        Payload::Static(v)
    }
}
impl From<&'static str> for Payload {
    fn from(v: &'static str) -> Self {
        Payload::Static(v.as_bytes())
    }
}

/// An operation queued for a connection, applied through `Protocol::visit` (design
/// rule R4) on the owning loop.
pub(crate) type CallAnyFn = Box<dyn FnOnce(&mut dyn Any, &mut Transport<'_>) + Send>;

pub(crate) enum ConnOp {
    Write(Payload),
    LoseConnection,
    Abort,
    PauseReading,
    ResumeReading,
    Call(Box<dyn FnOnce(&mut Transport<'_>) + Send>),
    CallAny(CallAnyFn),
}

impl Loop {
    /// Applies a queued op. Inert if the connection is stale or closed.
    pub(crate) fn apply_op(&mut self, id: ConnId, op: ConnOp) {
        match self.io(id) {
            Some(io) if io.kind == Kind::Tcp && io.flags & flags::CLOSED == 0 => {}
            _ => {
                // Stale or closed: inert, but a lost write is visible in the metrics.
                if matches!(op, ConnOp::Write(_)) {
                    crate::metrics::bump(&self.rt.metrics.write_after_close);
                }
                return;
            }
        }
        self.dispatch(
            id,
            move |p: &mut dyn Protocol, t: &mut Transport<'_>| match op {
                ConnOp::Write(pl) => p.visit(t, &mut |_, t| t.write(pl.as_slice())),
                ConnOp::LoseConnection => p.visit(t, &mut |_, t| t.lose_connection()),
                ConnOp::Abort => p.visit(t, &mut |_, t| t.abort()),
                ConnOp::PauseReading => p.visit(t, &mut |_, t| t.pause_reading()),
                ConnOp::ResumeReading => p.visit(t, &mut |_, t| t.resume_reading()),
                ConnOp::Call(f) => {
                    let mut f = Some(f);
                    p.visit(t, &mut |_, t| {
                        if let Some(f) = f.take() {
                            f(t)
                        }
                    })
                }
                ConnOp::CallAny(f) => {
                    let mut f = Some(f);
                    p.visit(t, &mut |any, t| {
                        if let Some(f) = f.take() {
                            f(any, t)
                        }
                    })
                }
            },
        );
    }
}

/// Stored / cross-thread handle. `Clone + Send + Sync`. Every operation is queued
/// to the owning loop (same-loop: the post-callback queue; other threads: the MPSC
/// queue) and enters through `Protocol::visit`. Generation-checked: inert after close.
#[derive(Clone)]
pub struct TransportHandle {
    pub(crate) lp: LoopHandle,
    pub(crate) id: ConnId,
}

impl TransportHandle {
    fn op(&self, op: ConnOp) {
        let _ = self.lp.submit_task(Task::Op(self.id, op));
    }

    /// Queues `data` for writing.
    pub fn write(&self, data: impl Into<Payload>) {
        self.op(ConnOp::Write(data.into()));
    }

    /// Queues a graceful close.
    pub fn lose_connection(&self) {
        self.op(ConnOp::LoseConnection);
    }

    /// Queues an immediate close.
    pub fn abort(&self) {
        self.op(ConnOp::Abort);
    }

    /// Queues `pause_reading` on the owning loop.
    pub fn pause_reading(&self) {
        self.op(ConnOp::PauseReading);
    }

    /// Queues `resume_reading` on the owning loop.
    pub fn resume_reading(&self) {
        self.op(ConnOp::ResumeReading);
    }

    /// Runs `f` on the loop with the connection's transport.
    pub fn call(&self, f: impl FnOnce(&mut Transport<'_>) + Send + 'static) {
        self.op(ConnOp::Call(Box::new(f)));
    }

    /// Runs `f` on the loop against the innermost protocol (downcast to `P`;
    /// skipped if it is not a `P`).
    pub fn call_proto<P: Protocol>(
        &self,
        f: impl FnOnce(&mut P, &mut Transport<'_>) + Send + 'static,
    ) {
        self.op(ConnOp::CallAny(Box::new(move |any, t| {
            if let Some(p) = any.downcast_mut::<P>() {
                f(p, t)
            }
        })));
    }

    /// The connection's id.
    pub fn id(&self) -> ConnId {
        self.id
    }

    /// The owning loop's handle.
    pub fn loop_handle(&self) -> &LoopHandle {
        &self.lp
    }
}
