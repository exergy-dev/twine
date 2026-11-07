//! UDP support (design Sections 6.5 and 8): one socket per `listen_udp`, bound on
//! one loop, with `recvmmsg` batches in and `sendmmsg` batches out.
//!
//! The datagram protocol lives in [`UdpState`] (not in the slot's `Protocol` box)
//! and is moved out for the duration of a callback, exactly like TCP protocols
//! (design Section 14), so [`DatagramTransport`] can borrow the whole loop.

use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::Arc;

use twine_sock::Batch;

use crate::conn::{flags, queue_ready, release_slot, Kind, Table, UNSPECIFIED};
use crate::engine::{EngineHandle, Listener, ListenerFd, ListenerInner};
use crate::errors::{Error, PanicError, Result};
use crate::event_loop::{ConnId, Loop, Rt};
use crate::metrics::{add, bump};
use crate::options::ListenOptions;
use crate::protocol::DatagramProtocol;

/// Builds the protocol of a UDP listener on its loop.
pub(crate) type DatagramBuilder = Box<dyn FnOnce(&mut Loop) -> Box<dyn DatagramProtocol> + Send>;

/// Receive buffer per inbound message (the maximum UDP payload fits).
const IN_BUF: usize = 64 * 1024;
/// Payload capacity per outbound batch message; larger datagrams use `sendto`.
const OUT_BUF: usize = 8 * 1024;

/// Per-socket UDP state stored in `ConnIo`.
pub(crate) struct UdpState {
    /// The protocol; `None` while it runs a callback or after it stopped.
    proto: Option<Box<dyn DatagramProtocol>>,
    /// Receive batch; `None` while a read event is dispatching out of it.
    inbound: Option<Batch>,
    /// Pending datagrams to send, `out_n` of them.
    outbound: Batch,
    out_n: usize,
    /// The listener handle that owns this socket.
    listener: Arc<ListenerInner>,
}

impl UdpState {
    /// The `Listener` this socket belongs to (used by the engine's listener
    /// close path to find the right slot).
    pub(crate) fn listener(&self) -> &Arc<ListenerInner> {
        &self.listener
    }
}

/// Closes a raw fd that was never registered if dropped.
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

/// The loop-affine view of a UDP socket passed to `DatagramProtocol` callbacks.
pub struct DatagramTransport<'a> {
    pub(crate) lp: &'a mut Loop,
    pub(crate) id: ConnId,
}

impl<'a> DatagramTransport<'a> {
    /// Buffered and flushed with sendmmsg at the end of the iteration (Linux);
    /// sendto elsewhere. A full batch is flushed at once; datagrams larger than
    /// 8 KiB go straight to `sendto`. UDP is lossy: a datagram the socket
    /// refuses is dropped.
    pub fn write_to(&mut self, b: &[u8], to: SocketAddr) {
        write_to(self.lp, self.id, b, to);
    }

    /// The local address of the socket.
    pub fn host(&self) -> SocketAddr {
        self.lp.io(self.id).map_or(UNSPECIFIED, |io| io.host)
    }

    /// The owning loop.
    pub fn loop_mut(&mut self) -> &mut Loop {
        self.lp
    }

    /// Closes the socket at the end of the iteration; `stop_protocol` follows.
    pub fn close(&mut self) {
        self.lp.abort(self.id);
    }
}

fn is_closed(lp: &Loop, id: ConnId) -> bool {
    !matches!(lp.io(id), Some(io) if io.kind == Kind::Udp && io.flags & flags::CLOSED == 0)
}

fn panic_msg(lp: &Loop, what: &str, id: ConnId, payload: Box<dyn std::any::Any + Send>) {
    bump(&lp.rt.metrics.panics);
    let pe = PanicError::from_payload(payload);
    log::error!(
        "twine: panic in {what} (loop {} fd {}): {}",
        lp.rt.id,
        id.fd,
        pe.message
    );
}

// --- write path -----------------------------------------------------------------

fn write_to(lp: &mut Loop, id: ConnId, b: &[u8], to: SocketAddr) {
    let Loop { conns, rt } = &mut *lp;
    let Some(slot) = conns.find(id).filter(|s| s.io.kind == Kind::Udp) else {
        bump(&rt.metrics.write_after_close);
        return;
    };
    let io = &mut slot.io;
    if io.flags & flags::CLOSED != 0 {
        bump(&rt.metrics.write_after_close);
        return;
    }
    let fd = io.fd;
    let Some(st) = io.udp.as_deref_mut() else {
        return;
    };
    if b.len() > OUT_BUF {
        match twine_sock::send_to(fd, b, to) {
            Ok(_) => bump(&rt.metrics.datagrams_sent),
            Err(e) => log::debug!("twine: udp sendto {to}: {e}"),
        }
        return;
    }
    if st.out_n == st.outbound.len() {
        send_pending(rt, io, id);
    }
    let Some(st) = io.udp.as_deref_mut() else {
        return;
    };
    if st.out_n == st.outbound.len() {
        return; // the socket is blocked and the batch is full: drop (UDP)
    }
    let n = st.out_n;
    st.outbound.buf_mut(n)[..b.len()].copy_from_slice(b);
    st.outbound.set_len(n, b.len());
    st.outbound.set_addr(n, to);
    st.out_n += 1;
    let full = st.out_n == st.outbound.len();
    if full {
        send_pending(rt, io, id);
    }
    lp.mark_dirty(id);
}

/// Sends the buffered datagrams; keeps what the socket would not take and arms
/// WRITABLE (`WouldBlock`), or drops the datagram a hard error names.
fn send_pending(rt: &mut Rt, io: &mut crate::conn::ConnIo, _id: ConnId) {
    let fd = io.fd;
    let Some(st) = io.udp.as_deref_mut() else {
        return;
    };
    let mut blocked = false;
    while st.out_n > 0 {
        match st.outbound.send_mmsg(fd, st.out_n) {
            Ok(0) => {
                blocked = true;
                break;
            }
            Ok(k) => {
                add(&rt.metrics.datagrams_sent, k as u64);
                shift_out(st, k);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                blocked = true;
                break;
            }
            Err(e) => {
                log::debug!("twine: udp send: {e}");
                shift_out(st, 1); // the first message failed; drop it, go on
            }
        }
    }
    let want = io.flags & flags::WANT_WRITE != 0;
    if blocked && !want {
        io.flags |= flags::WANT_WRITE;
        let _ = rt.poller.modify(io.fd, io.interest());
    } else if !blocked && want {
        io.flags &= !flags::WANT_WRITE;
        let _ = rt.poller.modify(io.fd, io.interest());
    }
}

/// Drops the first `k` pending messages, moving the rest to the front.
fn shift_out(st: &mut UdpState, k: usize) {
    let n = st.out_n;
    if k >= n {
        st.out_n = 0;
        return;
    }
    for j in k..n {
        let (p, a) = st.outbound.msg(j);
        let payload = p.to_vec(); // cold path: partial send
        let len = payload.len();
        st.outbound.buf_mut(j - k)[..len].copy_from_slice(&payload);
        st.outbound.set_len(j - k, len);
        if let Some(a) = a {
            st.outbound.set_addr(j - k, a);
        }
    }
    st.out_n = n - k;
}

/// Flushes buffered datagrams with sendmmsg.
pub(crate) fn flush_udp(lp: &mut Loop, id: ConnId) {
    let Loop { conns, rt } = &mut *lp;
    let Some(slot) = conns.find(id).filter(|s| s.io.kind == Kind::Udp) else {
        return;
    };
    if slot.io.flags & flags::CLOSED != 0 {
        return;
    }
    send_pending(rt, &mut slot.io, id);
}

// --- read path ------------------------------------------------------------------

/// Reads and dispatches datagrams: up to `max_reads_per_event` batches.
pub(crate) fn handle_udp_read(lp: &mut Loop, id: ConnId) {
    let max_rounds = lp.rt.opts.max_reads_per_event;
    let (fd, mut inb, mut proto) = {
        let Some(io) = lp.io_mut(id) else { return };
        if io.kind != Kind::Udp || io.flags & flags::CLOSED != 0 {
            return;
        }
        let fd = io.fd;
        let Some(st) = io.udp.as_deref_mut() else {
            return;
        };
        match (st.inbound.take(), st.proto.take()) {
            (Some(i), Some(p)) => (fd, i, p),
            (i, p) => {
                st.inbound = i;
                st.proto = p;
                return;
            }
        }
    };
    let mut capped = true;
    let mut panicked = false;
    let mut stop = false;
    'rounds: for _ in 0..max_rounds {
        let n = match inb.recv_mmsg(fd) {
            Ok(n) => n,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    log::debug!("twine: udp recv: {e}");
                }
                capped = false;
                break;
            }
        };
        add(&lp.rt.metrics.datagrams, n as u64);
        for i in 0..n {
            let (data, from) = inb.msg_mut(i);
            let Some(from) = from else { continue };
            lp.rt.metrics.cb_proto.store(0, Relaxed);
            lp.rt.metrics.cb_start.store(lp.rt.now_ns + 1, Relaxed);
            let res = catch_unwind(AssertUnwindSafe(|| {
                let mut t = DatagramTransport { lp: &mut *lp, id };
                proto.datagram_received(&mut t, data, from);
            }));
            lp.rt.metrics.cb_start.store(0, Relaxed);
            if let Err(payload) = res {
                panic_msg(lp, "datagram_received", id, payload);
                panicked = true;
                stop = true;
                break 'rounds;
            }
            lp.drain_pending();
            if is_closed(lp, id) {
                stop = true;
                break 'rounds;
            }
        }
        if n < inb.len() {
            capped = false; // short batch: the socket is drained
            break;
        }
    }
    restore(lp, id, inb, proto);
    if panicked {
        close_udp(lp, id);
    } else if capped && !stop {
        let Loop { conns, rt } = &mut *lp;
        queue_ready(rt, &mut conns.slot(id.fd).io, id);
    }
}

/// Puts the batch and protocol back; stops the protocol if the slot is gone.
fn restore(lp: &mut Loop, id: ConnId, inb: Batch, mut proto: Box<dyn DatagramProtocol>) {
    if let Some(st) = lp.io_mut(id).and_then(|io| io.udp.as_deref_mut()) {
        st.inbound = Some(inb);
        st.proto = Some(proto);
    } else if let Err(p) = catch_unwind(AssertUnwindSafe(|| proto.stop_protocol())) {
        panic_msg(lp, "stop_protocol", id, p);
    }
}

// --- lifecycle --------------------------------------------------------------------

/// Closes a UDP socket and calls `stop_protocol` (once). Idempotent.
pub(crate) fn close_udp(lp: &mut Loop, id: ConnId) {
    if !matches!(lp.io(id), Some(io) if io.kind == Kind::Udp) {
        return;
    }
    flush_udp_final(lp, id);
    let Loop { conns, rt } = &mut *lp;
    let slot = conns.slot(id.fd);
    let fd = slot.io.fd;
    let _ = rt.poller.delete(fd);
    let st = slot.io.udp.take();
    // Claim the listener's fd entry so `Listener::close` can never close this
    // fd number again after the kernel has reused it.
    if let Some(st) = st.as_deref() {
        let _ = st.listener.take_fd(rt.id);
    }
    twine_sock::close(fd);
    release_slot(conns, rt, id.fd as usize);
    rt.nconns -= 1;
    let Some(mut st) = st else { return };
    if let Some(mut proto) = st.proto.take() {
        lp.rt.metrics.cb_proto.store(0, Relaxed);
        lp.rt.metrics.cb_start.store(lp.rt.now_ns + 1, Relaxed);
        let res = catch_unwind(AssertUnwindSafe(|| proto.stop_protocol()));
        lp.rt.metrics.cb_start.store(0, Relaxed);
        if let Err(p) = res {
            panic_msg(lp, "stop_protocol", id, p);
        }
        if let Err(p) = catch_unwind(AssertUnwindSafe(|| drop(proto))) {
            panic_msg(lp, "dropping datagram protocol", id, p);
        }
    }
}

/// One last best-effort flush before the socket closes.
fn flush_udp_final(lp: &mut Loop, id: ConnId) {
    let Loop { conns, rt } = &mut *lp;
    if let Some(slot) = conns.find(id) {
        if slot.io.kind == Kind::Udp {
            send_pending(rt, &mut slot.io, id);
        }
    }
}

/// Runs on the owning loop: installs the socket in its slot and starts the protocol.
fn register(lp: &mut Loop, mut guard: FdGuard, build: DatagramBuilder, lst: Arc<ListenerInner>) {
    if lp.rt.stopping || lst.closed.load(SeqCst) {
        let _ = lst.take_fd(lp.rt.id); // the guard closes the fd; nobody else may
        return;
    }
    let proto = match catch_unwind(AssertUnwindSafe(|| build(lp))) {
        Ok(p) => p,
        Err(payload) => {
            bump(&lp.rt.metrics.panics);
            log::error!(
                "twine: panic building the datagram protocol (loop {}): {}",
                lp.rt.id,
                PanicError::from_payload(payload).message
            );
            let _ = lst.take_fd(lp.rt.id); // the guard closes the fd
            return;
        }
    };
    let fd = guard.take();
    let idx = fd as usize;
    lp.claim_slot(idx);
    let id = lp.id_at(idx);
    let batch = lp.rt.opts.udp_batch;
    let Loop { conns, rt } = &mut *lp;
    let io = &mut conns.slot(idx as u32).io;
    debug_assert!(io.kind == Kind::Empty);
    io.fd = fd;
    io.kind = Kind::Udp;
    io.flags = 0;
    io.host = lst.addr;
    io.udp = Some(Box::new(UdpState {
        proto: Some(proto),
        inbound: Some(Batch::new(batch, IN_BUF)),
        outbound: Batch::new(batch, OUT_BUF),
        out_n: 0,
        listener: lst,
    }));
    rt.nconns += 1;
    if let Err(e) = rt.poller.add(fd, twine_poll::READABLE) {
        log::error!("twine: registering udp socket on loop {}: {e}", rt.id);
        close_udp(lp, id);
        return;
    }
    // start_protocol runs before any datagram is delivered.
    if let Some(mut proto) = lp
        .io_mut(id)
        .and_then(|io| io.udp.as_deref_mut())
        .and_then(|st| st.proto.take())
    {
        let res = catch_unwind(AssertUnwindSafe(|| {
            let mut t = DatagramTransport { lp: &mut *lp, id };
            proto.start_protocol(&mut t);
        }));
        restore_proto(lp, id, proto);
        if let Err(p) = res {
            panic_msg(lp, "start_protocol", id, p);
            close_udp(lp, id);
            return;
        }
    }
    lp.drain_pending();
    // Datagrams that arrived before registration raised no edge.
    handle_udp_read(lp, id);
}

fn restore_proto(lp: &mut Loop, id: ConnId, mut proto: Box<dyn DatagramProtocol>) {
    if let Some(st) = lp.io_mut(id).and_then(|io| io.udp.as_deref_mut()) {
        st.proto = Some(proto);
    } else if let Err(p) = catch_unwind(AssertUnwindSafe(|| proto.stop_protocol())) {
        panic_msg(lp, "stop_protocol", id, p);
    }
}

/// Binds a UDP listener on the next loop.
pub(crate) fn listen(
    eng: &EngineHandle,
    addr: SocketAddr,
    build: DatagramBuilder,
    opts: ListenOptions,
) -> Result<Listener> {
    let (fd, laddr) = twine_sock::listen_udp(addr, false)?;
    let mut guard = FdGuard(fd);
    if opts.rcv_buf > 0 || opts.snd_buf > 0 {
        let _ = twine_sock::set_buffers(fd, opts.rcv_buf, opts.snd_buf);
    }
    if let Some(d) = eng.0.opts.busy_poll {
        let _ = twine_sock::set_busy_poll(fd, d);
    }
    let lh = eng.next_loop();
    let inner = Arc::new(ListenerInner {
        addr: laddr,
        factory: None,
        opts,
        udp: true,
        closed: AtomicBool::new(false),
        // The fd is recorded so `Loop::close_listener` can find the slot; the
        // socket itself is owned by the loop slot and closed by `close_udp`.
        fds: vec![ListenerFd {
            lh: lh.clone(),
            fd: AtomicI32::new(fd),
        }],
        engine: Arc::downgrade(&eng.0),
    });
    eng.0
        .listeners
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(inner.clone());
    let l2 = inner.clone();
    let g = FdGuard(guard.take());
    if lh
        .call_from_thread(move |lp| register(lp, g, build, l2))
        .is_err()
    {
        eng.0
            .listeners
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|x| !Arc::ptr_eq(x, &inner));
        return Err(Error::EngineStopped);
    }
    Ok(Listener { inner })
}
