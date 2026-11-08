//! The connection table: slots indexed by fd, flags, and the read, write,
//! flush and close paths (design Sections 7.1, 7.2 and 8 rules 9 and 10).

use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use twine_ring::Ring;

use crate::connect::DialState;
use crate::engine::ListenerInner;
use crate::errors::{Error, PanicError};
use crate::event_loop::{pending_push, ConnId, Loop, LoopThread, Notify, Pending, Rt};
use crate::metrics::{self, add, bump};
use crate::protocol::{Factory, Producer, Protocol, PushProducer};
use crate::timer::TimerKind;
use crate::transport::Transport;
use crate::udp::UdpState;

/// Per-connection flag bits (`ConnIo::flags`).
pub(crate) mod flags {
    /// No more I/O; queued for recycle.
    pub const CLOSED: u32 = 1 << 0;
    /// `lose_connection` requested; flush then shut down.
    pub const CLOSING: u32 = 1 << 1;
    /// `SHUT_WR` already sent.
    pub const SHUT_WR: u32 = 1 << 2;
    /// The peer closed its side.
    pub const EOF: u32 = 1 << 3;
    /// In the loop's dirty list.
    pub const DIRTY: u32 = 1 << 4;
    /// WRITABLE interest armed.
    pub const WANT_WRITE: u32 = 1 << 5;
    /// READABLE interest dropped.
    pub const READ_PAUSED: u32 = 1 << 6;
    /// In the loop's ready list.
    pub const IN_READY: u32 = 1 << 7;
    /// Inside `flush`.
    pub const FLUSHING: u32 = 1 << 8;
    /// `connection_lost` was delivered.
    pub const LOST_CALLED: u32 = 1 << 9;
    /// `pause_producing` was queued and not yet followed by a resume.
    pub const PRODUCER_PAUSED: u32 = 1 << 10;
    /// The idle timer is acting as the half-close linger deadline.
    pub const LINGER: u32 = 1 << 11;
    /// Writes from other views and handles must go through `Protocol::visit`.
    pub const FILTERED: u32 = 1 << 12;
    /// TCP_NODELAY requested.
    pub const NODELAY: u32 = 1 << 13;
    /// The boxed producer is out of its slot, inside a notification.
    pub const PRODUCER_OUT: u32 = 1 << 14;
    /// A listener accept retry (after EMFILE and friends) is scheduled.
    pub const ACCEPT_RETRY: u32 = 1 << 15;
    /// A poll event carried a hangup; reads must continue to EOF even after a
    /// short read, across the per-event cap and across a pause.
    pub const HANGUP: u32 = 1 << 16;
}

use flags::*;

/// What a slot currently holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Empty,
    Tcp,
    Listener,
    // Constructed by udp.rs / connect.rs (Wave 2b).
    #[allow(dead_code)]
    Udp,
    #[allow(dead_code)]
    Connecting,
}

/// Everything about a connection except its protocol box (kept apart so the
/// protocol can be moved out while the rest stays reachable).
pub(crate) struct ConnIo {
    /// The socket, or -1 for an empty slot.
    pub(crate) fd: i32,
    pub(crate) kind: Kind,
    pub(crate) flags: u32,
    pub(crate) ring: Ring,
    pub(crate) idle: Option<twine_wheel::TimerId>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) producer: Option<Producer>,
    pub(crate) streaming: bool,
    pub(crate) lost: Option<Error>,
    pub(crate) peer: SocketAddr,
    pub(crate) host: SocketAddr,
    pub(crate) listener: Option<Arc<ListenerInner>>,
    /// Interned protocol type name index + 1 (watchdog); 0 = none.
    pub(crate) proto_name: u32,
    pub(crate) udp: Option<Box<UdpState>>,
    pub(crate) dial: Option<Box<DialState>>,
    /// Live `Transport::call_later` timers of this connection, removed from the
    /// wheel at close. The capacity survives slot reuse (no allocation per timer
    /// after warm-up).
    pub(crate) timers: Vec<twine_wheel::TimerId>,
}

/// One entry of the fd-indexed connection table.
pub(crate) struct ConnSlot {
    pub(crate) proto: Option<Box<dyn Protocol>>,
    pub(crate) io: ConnIo,
}

/// One entry of the fd-indexed table: the per-fd generation plus the boxed
/// slot, present only while the fd is in use on this loop. An unused fd costs
/// 16 bytes. The generation lives here (not in the box) so it keeps increasing
/// per fd when a different box lands in the same entry.
pub(crate) struct SlotEntry {
    /// Bumped when the fd is released, so stale `ConnId`s are inert.
    pub(crate) gen: u32,
    pub(crate) slot: Option<Box<ConnSlot>>,
}

/// Accessors over the fd-indexed table.
pub(crate) trait Table {
    /// The live slot for `id` if the fd is in range, occupied and the
    /// generation matches.
    fn find(&mut self, id: ConnId) -> Option<&mut ConnSlot>;
    /// The occupied slot at `fd`. Panics if the fd is not in use (an internal
    /// invariant: callers only ask for slots they know are claimed).
    fn slot(&mut self, fd: u32) -> &mut ConnSlot;
}

impl Table for [SlotEntry] {
    #[inline]
    fn find(&mut self, id: ConnId) -> Option<&mut ConnSlot> {
        match self.get_mut(id.fd as usize) {
            Some(e) if e.gen == id.gen => e.slot.as_deref_mut(),
            _ => None,
        }
    }

    #[inline]
    fn slot(&mut self, fd: u32) -> &mut ConnSlot {
        match self
            .get_mut(fd as usize)
            .and_then(|e| e.slot.as_deref_mut())
        {
            Some(s) => s,
            None => unreachable!("connection table slot {fd} is not in use"),
        }
    }
}

/// Clears the slot at `fd`, bumps the fd's generation and moves the box to the
/// free list (or drops it when the list already holds the loop's peak). The
/// caller closes the socket and adjusts `nconns`.
pub(crate) fn release_slot(conns: &mut [SlotEntry], rt: &mut Rt, fd: usize) {
    let Some(entry) = conns.get_mut(fd) else {
        return;
    };
    let Some(mut b) = entry.slot.take() else {
        return;
    };
    b.io.ring.reset(&mut rt.alloc);
    b.io.clear();
    b.proto = None;
    entry.gen = entry.gen.wrapping_add(1);
    rt.slots_live -= 1;
    if rt.free_slots.len() < rt.slots_peak {
        rt.free_slots.push(b);
    }
    rt.metrics.slots_live.store(rt.slots_live as u64, Relaxed);
}

impl Loop {
    /// Makes sure the table covers `idx` and that entry holds a slot, taking it
    /// from the free list when possible.
    pub(crate) fn claim_slot(&mut self, idx: usize) {
        let Loop { conns, rt } = &mut *self;
        if idx >= conns.len() {
            let n = (idx + 1).next_power_of_two().max(1024);
            conns.resize_with(n, || SlotEntry { gen: 0, slot: None });
            rt.metrics.slots_table.store(n as u64, Relaxed);
        }
        let entry = &mut conns[idx];
        if entry.slot.is_none() {
            entry.slot = Some(
                rt.free_slots
                    .pop()
                    .unwrap_or_else(|| Box::new(ConnSlot::empty())),
            );
            rt.slots_live += 1;
            rt.slots_peak = rt.slots_peak.max(rt.slots_live);
            rt.metrics.slots_live.store(rt.slots_live as u64, Relaxed);
        }
    }
}

pub(crate) const UNSPECIFIED: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));

impl ConnIo {
    pub(crate) fn empty() -> ConnIo {
        ConnIo {
            fd: -1,
            kind: Kind::Empty,
            flags: 0,
            ring: Ring::new(),
            idle: None,
            idle_timeout: None,
            producer: None,
            streaming: false,
            lost: None,
            peer: UNSPECIFIED,
            host: UNSPECIFIED,
            listener: None,
            proto_name: 0,
            udp: None,
            dial: None,
            timers: Vec::new(),
        }
    }

    /// Resets every field except the ring (which the caller has already reset
    /// into the allocator) so the slot can be reused.
    pub(crate) fn clear(&mut self) {
        self.fd = -1;
        self.kind = Kind::Empty;
        self.flags = 0;
        self.idle = None;
        self.idle_timeout = None;
        self.producer = None;
        self.streaming = false;
        self.lost = None;
        self.peer = UNSPECIFIED;
        self.host = UNSPECIFIED;
        self.listener = None;
        self.proto_name = 0;
        self.udp = None;
        self.dial = None;
        self.timers.clear();
    }

    fn has_producer(&self) -> bool {
        self.producer.is_some() || self.flags & PRODUCER_OUT != 0
    }

    /// The poller interest set implied by the flags.
    pub(crate) fn interest(&self) -> u32 {
        let mut f = 0;
        if self.flags & READ_PAUSED == 0 {
            f |= twine_poll::READABLE;
        }
        if self.flags & WANT_WRITE != 0 {
            f |= twine_poll::WRITABLE;
        }
        f
    }
}

impl ConnSlot {
    pub(crate) fn empty() -> ConnSlot {
        ConnSlot {
            proto: None,
            io: ConnIo::empty(),
        }
    }
}

// --- helpers over (rt, io) -----------------------------------------------------

/// Marks the connection closed and queues it for recycling at the end of the
/// iteration. Pending writes are discarded. `lost` is left untouched.
#[inline]
pub(crate) fn abort_io(rt: &mut Rt, io: &mut ConnIo, fd: u32) {
    if io.flags & CLOSED != 0 {
        return;
    }
    io.flags |= CLOSED;
    rt.closing.push(fd);
}

/// Aborts with `e` delivered to `connection_lost`.
pub(crate) fn fail_io(rt: &mut Rt, io: &mut ConnIo, fd: u32, e: Error) {
    if io.flags & CLOSED != 0 {
        return;
    }
    io.lost = Some(e);
    abort_io(rt, io, fd);
}

#[inline]
fn mark_dirty_io(rt: &mut Rt, io: &mut ConnIo, id: ConnId) {
    if io.flags & DIRTY == 0 {
        io.flags |= DIRTY;
        rt.dirty.push(id);
    }
}

#[inline]
fn arm_write(rt: &mut Rt, io: &mut ConnIo) {
    if io.flags & WANT_WRITE != 0 {
        return;
    }
    io.flags |= WANT_WRITE;
    let _ = rt.poller.modify(io.fd, io.interest());
}

#[inline]
fn disarm_write(rt: &mut Rt, io: &mut ConnIo) {
    if io.flags & WANT_WRITE == 0 {
        return;
    }
    io.flags &= !WANT_WRITE;
    let _ = rt.poller.modify(io.fd, io.interest());
}

pub(crate) fn queue_ready(rt: &mut Rt, io: &mut ConnIo, id: ConnId) {
    if io.flags & IN_READY == 0 {
        io.flags |= IN_READY;
        rt.ready.push(id);
    }
}

#[inline]
fn deadline_ns(rt: &Rt, d: Duration) -> u64 {
    rt.now_ns.saturating_add(d.as_nanos().min(1 << 60) as u64)
}

/// Arms (or re-links) the idle timer `d` from now.
pub(crate) fn arm_idle(rt: &mut Rt, io: &mut ConnIo, id: ConnId, d: Duration) {
    let at = deadline_ns(rt, d);
    match io.idle {
        Some(t) if rt.wheel.reset(t, at) => {}
        _ => io.idle = Some(rt.wheel.add(at, TimerKind::Idle(id))),
    }
}

/// Resets the idle timer after activity.
#[inline]
fn touch_idle(rt: &mut Rt, io: &mut ConnIo, id: ConnId) {
    // Activity must not extend the half-close linger deadline.
    if io.flags & LINGER != 0 {
        return;
    }
    if let Some(d) = io.idle_timeout {
        arm_idle(rt, io, id, d);
    }
}

/// Reads `SO_ERROR`; `Some` when a socket error is pending.
pub(crate) fn sock_error(fd: i32) -> Option<Error> {
    match twine_sock::so_error(fd) {
        Ok(Some(errno)) => Some(Error::from_errno(errno)),
        Ok(None) => None,
        Err(e) => Some(Error::from(e)),
    }
}

/// Runs after bytes were appended to the ring.
fn after_buffer(rt: &mut Rt, io: &mut ConnIo, id: ConnId) {
    mark_dirty_io(rt, io, id);
    let pending = io.ring.len();
    let m = &*rt.metrics;
    if pending as u64 > m.ring_high_water.load(std::sync::atomic::Ordering::Relaxed) {
        m.ring_high_water
            .store(pending as u64, std::sync::atomic::Ordering::Relaxed);
    }
    if pending > rt.opts.write_buffer_limit {
        fail_io(rt, io, id.fd, Error::WriteBufferFull);
        return;
    }
    arm_write(rt, io);
    if io.has_producer()
        && io.streaming
        && io.flags & PRODUCER_PAUSED == 0
        && pending > rt.opts.write_high_water
    {
        io.flags |= PRODUCER_PAUSED;
        pending_push(Pending::Notify(id, Notify::Pause));
    }
}

fn fail_write(rt: &mut Rt, io: &mut ConnIo, id: ConnId, e: std::io::Error) {
    fail_io(rt, io, id.fd, Error::from(e));
}

/// Called when the ring is empty and CLOSING is set: send SHUT_WR and wait for
/// the peer to close (bounded by the linger timer), or close right away.
fn finish_lose(rt: &mut Rt, io: &mut ConnIo, id: ConnId) {
    if io.flags & CLOSED != 0 {
        return;
    }
    if io.flags & EOF != 0 {
        abort_io(rt, io, id.fd); // the peer already closed; nothing to wait for
        return;
    }
    if io.flags & SHUT_WR != 0 {
        return;
    }
    io.flags |= SHUT_WR;
    let linger = rt.opts.close_linger;
    if linger.is_zero() || twine_sock::shutdown_write(io.fd).is_err() {
        abort_io(rt, io, id.fd);
        return;
    }
    // Keep reading until the peer closes (a read of 0 completes the close with
    // connection_lost(None)), but not forever: reuse the idle timer as the
    // linger deadline.
    io.flags |= LINGER;
    io.idle_timeout = Some(linger);
    arm_idle(rt, io, id, linger);
    if io.flags & READ_PAUSED != 0 && io.flags & EOF == 0 {
        // Reading was paused by the protocol; we still need to see the peer's FIN.
        io.flags &= !READ_PAUSED;
        let _ = rt.poller.modify(io.fd, io.interest());
        queue_ready(rt, io, id);
    }
}

/// Handles a zero-length read: the peer closed its side.
fn eof(rt: &mut Rt, io: &mut ConnIo, id: ConnId) {
    if io.flags & (CLOSED | EOF) != 0 {
        return;
    }
    io.flags |= EOF | READ_PAUSED;
    if io.ring.is_empty() {
        abort_io(rt, io, id.fd); // nothing left to send: clean close
        return;
    }
    // Pending writes remain: keep flushing, then close (the peer may still read).
    io.flags |= CLOSING;
    mark_dirty_io(rt, io, id);
}

impl Loop {
    pub(crate) fn mark_dirty(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        if let Some(s) = conns.find(id) {
            if s.io.kind != Kind::Empty {
                mark_dirty_io(rt, &mut s.io, id);
            }
        }
    }

    /// Aborts with `e` delivered to `connection_lost`.
    pub(crate) fn fail(&mut self, id: ConnId, e: Error) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        if let Some(s) = conns.find(id) {
            if s.io.kind != Kind::Empty {
                fail_io(rt, &mut s.io, id.fd, e);
            }
        }
    }

    /// Closes at the end of the iteration without touching `lost`.
    pub(crate) fn abort(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        if let Some(s) = conns.find(id) {
            if s.io.kind != Kind::Empty {
                abort_io(rt, &mut s.io, id.fd);
            }
        }
    }

    /// Graceful close: flush, SHUT_WR, close when the peer closes.
    pub(crate) fn lose_connection(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id) else {
            return;
        };
        if s.io.kind != Kind::Tcp {
            return;
        }
        let io = &mut s.io;
        if io.flags & (CLOSED | CLOSING) != 0 {
            return;
        }
        io.flags |= CLOSING;
        if io.ring.is_empty() {
            finish_lose(rt, io, id);
        } else {
            mark_dirty_io(rt, io, id);
        }
    }

    pub(crate) fn pause_reading(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id) else {
            return;
        };
        if s.io.kind != Kind::Tcp {
            return;
        }
        let io = &mut s.io;
        if io.flags & (CLOSED | READ_PAUSED | EOF) != 0 {
            return;
        }
        io.flags |= READ_PAUSED;
        let _ = rt.poller.modify(io.fd, io.interest());
    }

    /// Restores read interest and queues one immediate read so an edge missed
    /// while paused is not lost.
    pub(crate) fn resume_reading(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id) else {
            return;
        };
        if s.io.kind != Kind::Tcp {
            return;
        }
        let io = &mut s.io;
        if io.flags & CLOSED != 0 || io.flags & READ_PAUSED == 0 || io.flags & EOF != 0 {
            return;
        }
        io.flags &= !READ_PAUSED;
        let _ = rt.poller.modify(io.fd, io.interest());
        queue_ready(rt, io, id);
    }

    /// WRITABLE edge: flush if data is pending, otherwise drop write interest.
    pub(crate) fn on_writable(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id) else {
            return;
        };
        if s.io.kind != Kind::Tcp || s.io.flags & CLOSED != 0 {
            return;
        }
        let io = &mut s.io;
        if !io.ring.is_empty() {
            mark_dirty_io(rt, io, id);
        } else if io.flags & WANT_WRITE != 0 {
            // Spurious or post-flush edge: drop write interest.
            disarm_write(rt, io);
            if io.flags & CLOSING != 0 {
                finish_lose(rt, io, id);
            }
        }
    }

    // --- write path -----------------------------------------------------------

    /// `Transport::write`: direct syscall when the ring is empty, else append.
    pub(crate) fn conn_write(&mut self, id: ConnId, b: &[u8]) {
        if b.is_empty() {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns
            .find(id)
            .filter(|s| s.io.kind == Kind::Tcp && id.loop_id as usize == rt.id)
        else {
            bump(&rt.metrics.write_after_close);
            return;
        };
        let io = &mut s.io;
        if io.flags & (CLOSED | CLOSING) != 0 {
            bump(&rt.metrics.write_after_close);
            return;
        }
        let mut rest = b;
        if io.ring.is_empty() && io.flags & FLUSHING == 0 {
            match twine_sock::write(io.fd, b) {
                Ok(n) if n == b.len() => {
                    add(&rt.metrics.write_bytes, n as u64);
                    bump(&rt.metrics.write_direct_hit);
                    touch_idle(rt, io, id);
                    if io.has_producer() && !io.streaming {
                        mark_dirty_io(rt, io, id); // flush pokes the pull producer for more
                    }
                    return;
                }
                Ok(n) => {
                    add(&rt.metrics.write_bytes, n as u64);
                    rest = &b[n..];
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
                Err(e) => {
                    fail_write(rt, io, id, e);
                    return;
                }
            }
        }
        io.ring.write(&mut rt.alloc, rest);
        after_buffer(rt, io, id);
    }

    /// `Transport::writev`: one writev(2) when the ring is empty, else append.
    pub(crate) fn conn_writev(&mut self, id: ConnId, bufs: &[&[u8]]) {
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns
            .find(id)
            .filter(|s| s.io.kind == Kind::Tcp && id.loop_id as usize == rt.id)
        else {
            bump(&rt.metrics.write_after_close);
            return;
        };
        let io = &mut s.io;
        if io.flags & (CLOSED | CLOSING) != 0 {
            bump(&rt.metrics.write_after_close);
            return;
        }
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return;
        }
        if io.ring.is_empty() && io.flags & FLUSHING == 0 && bufs.len() <= rt.iov.len() {
            let mut cnt = 0;
            for b in bufs.iter().filter(|b| !b.is_empty()) {
                // Raw pointers into the caller's slices; they stay valid for the
                // writev call below and are never dereferenced here.
                rt.iov[cnt] = libc::iovec {
                    iov_base: b.as_ptr() as *mut libc::c_void,
                    iov_len: b.len(),
                };
                cnt += 1;
            }
            let mut n = match twine_sock::writev(io.fd, &rt.iov[..cnt]) {
                Ok(n) => n,
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => 0,
                Err(e) => {
                    fail_write(rt, io, id, e);
                    return;
                }
            };
            add(&rt.metrics.write_bytes, n as u64);
            if n == total {
                bump(&rt.metrics.write_direct_hit);
                touch_idle(rt, io, id);
                if io.has_producer() && !io.streaming {
                    mark_dirty_io(rt, io, id);
                }
                return;
            }
            // Buffer the unwritten tail.
            for b in bufs {
                if n >= b.len() {
                    n -= b.len();
                    continue;
                }
                io.ring.write(&mut rt.alloc, &b[n..]);
                n = 0;
            }
            after_buffer(rt, io, id);
            return;
        }
        io.ring.writev(&mut rt.alloc, bufs);
        after_buffer(rt, io, id);
    }

    /// Writes as much of the ring as the socket accepts with one writev, then
    /// updates write interest and producer state.
    pub(crate) fn flush(&mut self, id: ConnId) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id) else {
            return;
        };
        if s.io.kind != Kind::Tcp {
            return;
        }
        let io = &mut s.io;
        if io.ring.is_empty() {
            disarm_write(rt, io);
            if io.flags & CLOSING != 0 {
                finish_lose(rt, io, id);
                return;
            }
            if io.has_producer() && !io.streaming {
                // Pull producer: ask for more once per drain, after the callback
                // that produced the data, never re-entrantly from write().
                pending_push(Pending::Notify(id, Notify::Resume));
            }
            return;
        }
        io.flags |= FLUSHING;
        let cnt = io.ring.iovecs(&mut rt.iov);
        let covered: usize = rt.iov[..cnt].iter().map(|v| v.iov_len).sum();
        bump(&rt.metrics.writev);
        match twine_sock::writev(io.fd, &rt.iov[..cnt]) {
            Ok(n) => {
                if n > 0 {
                    add(&rt.metrics.write_bytes, n as u64);
                    io.ring.consume(&mut rt.alloc, n);
                    touch_idle(rt, io, id);
                }
                if n == covered && !io.ring.is_empty() {
                    // The socket took every iovec but the ring holds more than one
                    // writev covers; no WRITABLE edge will follow, so go again.
                    mark_dirty_io(rt, io, id);
                }
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {}
            Err(e) => {
                io.flags &= !FLUSHING;
                fail_write(rt, io, id, e);
                return;
            }
        }
        if io.ring.is_empty() {
            io.ring.reset(&mut rt.alloc); // release the slab so idle conns hold no buffer
            disarm_write(rt, io);
            if io.flags & CLOSING != 0 {
                io.flags &= !FLUSHING;
                finish_lose(rt, io, id);
                return;
            }
        } else {
            arm_write(rt, io);
        }
        if io.has_producer() {
            if io.streaming {
                if io.flags & PRODUCER_PAUSED != 0 && io.ring.len() <= rt.opts.write_low_water {
                    io.flags &= !PRODUCER_PAUSED;
                    pending_push(Pending::Notify(id, Notify::Resume));
                }
            } else if io.ring.is_empty() {
                pending_push(Pending::Notify(id, Notify::Resume));
            }
        }
        io.flags &= !FLUSHING;
    }

    // --- producers ------------------------------------------------------------

    /// Attaches a producer (see `Transport::register_producer`).
    pub(crate) fn register_producer(&mut self, id: ConnId, p: Producer, streaming: bool) {
        if id.loop_id as usize != self.rt.id {
            return;
        }
        if let Producer::Conn(c) | Producer::PauseReading(c) = &p {
            if c.loop_id as usize != self.rt.id {
                log::debug!(
                    "twine: register_producer on loop {} ignored a producer target from loop {}",
                    self.rt.id,
                    c.loop_id
                );
                return;
            }
        }
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id).filter(|s| s.io.kind != Kind::Empty) else {
            return;
        };
        let io = &mut s.io;
        assert!(
            !io.has_producer(),
            "twine: register_producer called with a producer already registered"
        );
        io.producer = Some(p);
        io.streaming = streaming;
        io.flags &= !PRODUCER_PAUSED;
        if io.flags & CLOSED != 0 {
            pending_push(Pending::Notify(id, Notify::Stop));
            return;
        }
        if streaming {
            if io.ring.len() > rt.opts.write_high_water {
                io.flags |= PRODUCER_PAUSED;
                pending_push(Pending::Notify(id, Notify::Pause));
            }
        } else if io.ring.is_empty() {
            pending_push(Pending::Notify(id, Notify::Resume));
        }
    }

    pub(crate) fn unregister_producer(&mut self, id: ConnId) {
        if let Some(io) = self.io_mut(id) {
            io.producer = None;
            io.flags &= !(PRODUCER_PAUSED | PRODUCER_OUT);
        }
    }

    /// Delivers a queued producer notification (user code; post-callback queue).
    pub(crate) fn deliver_notify(&mut self, id: ConnId, n: Notify) {
        if n == Notify::Stop {
            // Takes the producer first, so it is stopped exactly once however
            // many stops are queued or raced by the close path.
            self.stop_producer_at_close(id);
            return;
        }
        let Some(io) = self.io_mut(id) else { return };
        enum Target {
            This,
            Conn(ConnId),
            PauseReading(ConnId),
            Boxed(Box<dyn PushProducer>),
        }
        let target = match &io.producer {
            None => return,
            Some(Producer::This) => Target::This,
            Some(Producer::Conn(c)) => Target::Conn(*c),
            Some(Producer::PauseReading(c)) => Target::PauseReading(*c),
            Some(Producer::Boxed(_)) => {
                let Some(Producer::Boxed(b)) = io.producer.take() else {
                    return;
                };
                io.flags |= PRODUCER_OUT;
                Target::Boxed(b)
            }
        };
        match target {
            Target::This => self.notify_proto(id, id, n),
            Target::Conn(c) => self.notify_proto(c, id, n),
            Target::PauseReading(c) => match n {
                Notify::Pause => self.pause_reading(c),
                Notify::Resume => self.resume_reading(c),
                Notify::Stop => {}
            },
            Target::Boxed(mut b) => {
                self.call_boxed_producer(id, &mut *b, n);
                if let Some(io) = self.io_mut(id) {
                    if io.flags & PRODUCER_OUT != 0 && n != Notify::Stop {
                        io.flags &= !PRODUCER_OUT;
                        io.producer = Some(Producer::Boxed(b));
                    } else if n == Notify::Stop {
                        io.flags &= !PRODUCER_OUT;
                    }
                }
            }
        }
    }

    fn notify_proto(&mut self, target: ConnId, _consumer: ConnId, n: Notify) {
        self.dispatch(target, |p, t| match n {
            Notify::Pause => p.pause_producing(t),
            Notify::Resume => p.resume_producing(t),
            Notify::Stop => p.stop_producing(t),
        });
    }

    fn call_boxed_producer(&mut self, consumer: ConnId, b: &mut dyn PushProducer, n: Notify) {
        self.rt
            .metrics
            .cb_start
            .store(self.rt.now_ns + 1, std::sync::atomic::Ordering::Relaxed);
        let filtered = self.io(consumer).is_some_and(|io| io.flags & FILTERED != 0);
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut t = Transport::new(self, consumer);
            // On a filtered connection the hook's writes must pass through the
            // wrapper's `WriteFilter`: route them through `Protocol::visit`.
            t.foreign = filtered;
            match n {
                Notify::Pause => b.pause_producing(&mut t),
                Notify::Resume => b.resume_producing(&mut t),
                Notify::Stop => b.stop_producing(&mut t),
            }
        }));
        self.rt
            .metrics
            .cb_start
            .store(0, std::sync::atomic::Ordering::Relaxed);
        if let Err(payload) = res {
            self.proto_panicked(consumer, payload);
        }
    }

    /// Twisted order: the producer is stopped before `connection_lost`. Runs
    /// directly (this is top-level code).
    fn stop_producer_at_close(&mut self, id: ConnId) {
        let Some(io) = self.io_mut(id) else { return };
        let prod = io.producer.take();
        io.flags &= !(PRODUCER_OUT | PRODUCER_PAUSED);
        match prod {
            None | Some(Producer::PauseReading(_)) => {}
            Some(Producer::This) => self.notify_proto(id, id, Notify::Stop),
            Some(Producer::Conn(c)) => self.notify_proto(c, id, Notify::Stop),
            Some(Producer::Boxed(mut b)) => self.call_boxed_producer(id, &mut *b, Notify::Stop),
        }
    }

    // --- lifecycle ------------------------------------------------------------

    /// Installs an accepted or connected socket in its slot, registers it with
    /// the poller and delivers `connection_made`. The slot must exist and be empty.
    pub(crate) fn attach_tcp(
        &mut self,
        fd: i32,
        peer: SocketAddr,
        host: SocketAddr,
        factory: &dyn Factory,
        no_delay: bool,
    ) -> Option<ConnId> {
        let built = catch_unwind(AssertUnwindSafe(|| {
            let p = factory.build_protocol(self, peer);
            let name = self.rt.intern_name(p.type_name());
            (p, name)
        }));
        let (proto, name) = match built {
            Ok(v) => v,
            Err(payload) => {
                bump(&self.rt.metrics.panics);
                let pe = PanicError::from_payload(payload);
                log::error!(
                    "twine: panic in Factory::build_protocol (loop {}): {}",
                    self.rt.id,
                    pe.message
                );
                twine_sock::close(fd);
                return None;
            }
        };
        let idx = fd as usize;
        self.claim_slot(idx);
        let id = self.id_at(idx);
        let Loop { conns, rt } = &mut *self;
        let slot = conns.slot(idx as u32);
        debug_assert!(
            slot.io.kind == Kind::Empty && slot.proto.is_none(),
            "fd slot reused while occupied"
        );
        let io = &mut slot.io;
        io.fd = fd;
        io.kind = Kind::Tcp;
        io.flags = if no_delay { NODELAY } else { 0 };
        io.peer = peer;
        io.host = host;
        io.proto_name = name;
        slot.proto = Some(proto);
        rt.nconns += 1;
        bump(&rt.metrics.connections);
        if let Err(e) = rt.poller.add(fd, twine_poll::READABLE) {
            fail_io(rt, io, fd as u32, Error::from(e));
            return Some(id);
        }
        self.dispatch(id, |p, t| p.connection_made(t));
        self.drain_pending();
        Some(id)
    }

    /// Completes the close of the connection in slot `fd`: poller delete,
    /// producer stop, `connection_lost` (exactly once), protocol drop, fd close,
    /// generation bump (design Section 8 rule 10).
    pub(crate) fn finish_close(&mut self, fd: usize) {
        let id = self.id_at(fd);
        match self
            .conns
            .get(fd)
            .and_then(|e| e.slot.as_ref())
            .map(|s| s.io.kind)
        {
            None | Some(Kind::Empty) => return,
            Some(Kind::Udp) => {
                crate::udp::close_udp(self, id);
                return;
            }
            Some(Kind::Listener | Kind::Connecting) => return,
            Some(Kind::Tcp) => {}
        }
        {
            let Loop { conns, rt } = &mut *self;
            let io = &mut conns.slot(fd as u32).io;
            if let Some(t) = io.idle.take() {
                rt.wheel.remove(t);
            }
            for t in io.timers.drain(..) {
                rt.wheel.remove(t);
            }
            let _ = rt.poller.delete(io.fd);
            bump(&rt.metrics.closes);
            metrics::dec(&rt.metrics.connections);
        }
        self.stop_producer_at_close(id);
        if let Some(mut proto) = self.conns.slot(fd as u32).proto.take() {
            let io = &mut self.conns.slot(fd as u32).io;
            if io.flags & LOST_CALLED == 0 {
                io.flags |= LOST_CALLED;
                let lost = io.lost.take();
                let name = io.proto_name;
                self.rt
                    .metrics
                    .cb_proto
                    .store(name, std::sync::atomic::Ordering::Relaxed);
                self.rt
                    .metrics
                    .cb_start
                    .store(self.rt.now_ns + 1, std::sync::atomic::Ordering::Relaxed);
                let res = catch_unwind(AssertUnwindSafe(|| {
                    let mut t = Transport::new(self, id);
                    proto.connection_lost(&mut t, lost);
                }));
                self.rt
                    .metrics
                    .cb_start
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                if let Err(payload) = res {
                    bump(&self.rt.metrics.panics);
                    let pe = PanicError::from_payload(payload);
                    log::error!(
                        "twine: panic in connection_lost (loop {} fd {fd}): {}",
                        self.rt.id,
                        pe.message
                    );
                }
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(proto))) {
                bump(&self.rt.metrics.panics);
                let pe = PanicError::from_payload(payload);
                log::error!(
                    "twine: panic dropping protocol (loop {} fd {fd}): {}",
                    self.rt.id,
                    pe.message
                );
            }
        }
        let Loop { conns, rt } = &mut *self;
        twine_sock::close(conns.slot(fd as u32).io.fd);
        release_slot(conns, rt, fd);
        rt.nconns -= 1;
    }
}

impl LoopThread {
    /// Reads until EAGAIN, a short read or the per-event cap, calling
    /// `data_received` per chunk with a sub-slice of the shared buffer. When the
    /// event carried a hangup a short read is not treated as "drained": the FIN
    /// behind the data must be read in the same pass because edge-triggered
    /// polling will not report it again.
    pub(crate) fn handle_read(&mut self, id: ConnId, hangup: bool) {
        let max_reads = self.lp.rt.opts.max_reads_per_event;
        let buf_len = self.rdbuf.len();
        // A hangup seen once sticks to the conn: edge-triggered polling will not
        // report it again, so a read re-armed after the per-event cap or after a
        // pause must still read to EOF instead of stopping at a short read.
        let hangup = match self.lp.io_mut(id) {
            Some(io) => {
                if hangup {
                    io.flags |= HANGUP;
                }
                io.flags & HANGUP != 0
            }
            None => return,
        };
        let mut i = 0;
        while i < max_reads {
            let Some(io) = self.lp.io(id) else { return };
            let fd = io.fd;
            match twine_sock::read(fd, &mut self.rdbuf) {
                Ok(0) => {
                    let Loop { conns, rt } = &mut self.lp;
                    eof(rt, &mut conns.slot(id.fd).io, id);
                    return;
                }
                Ok(n) => {
                    {
                        let Loop { conns, rt } = &mut self.lp;
                        add(&rt.metrics.read_bytes, n as u64);
                        bump(&rt.metrics.reads);
                        touch_idle(rt, &mut conns.slot(id.fd).io, id);
                    }
                    let data = &mut self.rdbuf[..n];
                    self.lp.dispatch(id, |p, t| p.data_received(t, data));
                    self.lp.drain_pending();
                    match self.lp.io(id) {
                        Some(io) if io.flags & (CLOSED | READ_PAUSED) == 0 => {}
                        _ => return,
                    }
                    if n < buf_len && !hangup {
                        return; // short read: socket drained
                    }
                    i += 1;
                }
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => return,
                    ErrorKind::Interrupted => continue,
                    _ => {
                        self.lp.fail(id, Error::from(e));
                        return;
                    }
                },
            }
        }
        // Cap hit with the socket possibly still readable: re-arm next iteration.
        let Loop { conns, rt } = &mut self.lp;
        queue_ready(rt, &mut conns.slot(id.fd).io, id);
    }
}
