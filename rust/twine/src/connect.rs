//! Outbound TCP connects (design Section 6.2; Go reference `connect.go`).
//!
//! `Loop::connect_tcp` starts a non-blocking connect, parks the socket in an
//! fd-indexed slot of kind [`Kind::Connecting`] with a [`DialState`], and waits
//! for the poller to report it writable. `handle_connect_ready` converts the
//! slot to a live TCP connection (through `attach_tcp`, which builds the
//! protocol and calls `connection_made`) and only then resolves the Deferred, so
//! `connection_made` always runs before the caller's callback.

use std::cell::RefCell;
use std::net::SocketAddr;

use crate::conn::{flags, release_slot, Kind, Table, UNSPECIFIED};
use crate::deferred::{awaitable_pair, Awaitable, Deferred, LocalResolver};
use crate::errors::{Error, PanicError};
use crate::event_loop::{ConnId, Loop, LoopHandle, TimerId};
use crate::options::DialOptions;
use crate::protocol::{Factory, Protocol};
use crate::transport::TransportHandle;

type Build = Box<dyn FnOnce(&mut Loop, SocketAddr) -> Box<dyn Protocol>>;

/// Per-connection dial state stored in `ConnIo` while the connect is in flight.
pub(crate) struct DialState {
    build: Build,
    opts: DialOptions,
    resolver: LocalResolver<ConnId>,
    timer: Option<TimerId>,
}

thread_local! {
    /// The `build` closure being handed to `attach_tcp`. `Factory` requires
    /// `Send + Sync` but `build` is not, so it travels through this slot.
    static STASH: RefCell<Option<Build>> = const { RefCell::new(None) };
}

/// A `Factory` that takes the stashed build closure.
struct StashFactory;

impl Factory for StashFactory {
    fn build_protocol(&self, lp: &mut Loop, peer: SocketAddr) -> Box<dyn Protocol> {
        let b = STASH.with(|s| s.borrow_mut().take());
        match b {
            Some(b) => b(lp, peer),
            None => unreachable!("StashFactory used without a stashed build closure"),
        }
    }
}

impl Loop {
    /// Non-blocking connect. Resolves with the new connection's id after
    /// `connection_made`; rejects with [`Error::ConnectTimeout`], the socket
    /// error (an [`Error::Io`], e.g. ECONNREFUSED), or [`Error::EngineStopped`].
    pub fn connect_tcp(
        &mut self,
        addr: SocketAddr,
        build: impl FnOnce(&mut Loop, SocketAddr) -> Box<dyn Protocol> + 'static,
        opts: DialOptions,
    ) -> Deferred<ConnId> {
        if self.rt.stopping || self.rt.engine.is_stopped() {
            return Deferred::rejected(self, Error::EngineStopped);
        }
        let (fd, _in_progress) = match twine_sock::connect(addr, opts.local) {
            Ok(x) => x,
            Err(e) => return Deferred::rejected(self, Error::from(e)),
        };
        let idx = fd as usize;
        self.claim_slot(idx);
        if let Err(e) = self.rt.poller.add(fd, twine_poll::WRITABLE) {
            twine_sock::close(fd);
            return Deferred::rejected(self, Error::from(e));
        }
        let (d, resolver) = self.deferred_local::<ConnId>();
        let id = self.id_at(idx);
        let timer = opts
            .timeout
            .map(|t| self.call_later(t, move |lp| reject_connect(lp, id, Error::ConnectTimeout)));
        let slot = self.conns.slot(idx as u32);
        debug_assert!(slot.io.kind == Kind::Empty, "fd slot reused while occupied");
        slot.io.fd = fd;
        slot.io.kind = Kind::Connecting;
        slot.io.peer = addr;
        slot.io.dial = Some(Box::new(DialState {
            build: Box::new(build),
            opts,
            resolver,
            timer,
        }));
        d
    }
}

/// Handles a poll event for a connecting socket.
pub(crate) fn handle_connect_ready(lp: &mut Loop, id: ConnId, ev_flags: u32) {
    let Some(io) = lp.io(id) else { return };
    if io.kind != Kind::Connecting || io.dial.is_none() {
        return;
    }
    let fd = io.fd;
    if let Err(e) = twine_sock::connect_error(fd) {
        reject_connect(lp, id, Error::from(e));
        return;
    }
    let bad = twine_poll::ERROR | twine_poll::HANGUP;
    if ev_flags & bad != 0 && ev_flags & twine_poll::WRITABLE == 0 {
        reject_connect(lp, id, Error::from_errno(libc::ECONNREFUSED));
        return;
    }
    complete_connect(lp, id);
}

/// Turns a connected `Connecting` slot into a live TCP connection and resolves
/// the Deferred after `connection_made`.
fn complete_connect(lp: &mut Loop, id: ConnId) {
    let idx = id.fd as usize;
    let (fd, peer, dial) = {
        let io = &mut lp.conns.slot(idx as u32).io;
        let Some(dial) = io.dial.take() else { return };
        (io.fd, io.peer, dial)
    };
    let DialState {
        build,
        opts,
        resolver,
        timer,
    } = *dial;
    if let Some(t) = timer {
        lp.cancel_timer(t);
    }
    // Back to an empty slot (same generation) so `attach_tcp` can claim it. The
    // connecting socket was registered for WRITABLE only; `attach_tcp` adds it
    // again with the steady-state interest set.
    let _ = lp.rt.poller.delete(fd);
    lp.conns.slot(idx as u32).io.clear();
    if opts.no_delay {
        let _ = twine_sock::set_nodelay(fd, true);
    }
    let host = twine_sock::local_addr(fd).unwrap_or(UNSPECIFIED);
    STASH.with(|s| *s.borrow_mut() = Some(build));
    let attached = lp.attach_tcp(fd, peer, host, &StashFactory, opts.no_delay);
    STASH.with(|s| *s.borrow_mut() = None);
    match attached {
        None => resolver.reject(Error::Panic(PanicError {
            message: "connect_tcp: protocol builder panicked".into(),
            backtrace: None,
        })),
        Some(nid) => {
            let closed = lp.io(nid).is_none_or(|io| io.flags & flags::CLOSED != 0);
            if closed {
                // The poller refused the fd or `connection_made` aborted.
                resolver.reject(Error::Closed);
            } else {
                resolver.resolve(nid);
            }
        }
    }
}

/// Fails a pending connect (shutdown, timeout, socket error) and recycles the
/// slot. Stale ids and slots that are no longer connecting are ignored.
pub(crate) fn reject_connect(lp: &mut Loop, id: ConnId, e: Error) {
    let idx = id.fd as usize;
    let Some(slot) = lp.conns.find(id) else {
        return;
    };
    if slot.io.kind != Kind::Connecting {
        return;
    }
    let Some(dial) = slot.io.dial.take() else {
        return;
    };
    let fd = slot.io.fd;
    let Loop { conns, rt } = &mut *lp;
    release_slot(conns, rt, idx);
    if let Some(t) = dial.timer {
        lp.cancel_timer(t);
    }
    let _ = lp.rt.poller.delete(fd);
    twine_sock::close(fd);
    dial.resolver.reject(e);
}

impl LoopHandle {
    /// Connects from any thread. The Awaitable yields a [`TransportHandle`] for
    /// the new connection once `connection_made` has run.
    pub fn connect_tcp(
        &self,
        addr: SocketAddr,
        f: impl Factory,
        opts: DialOptions,
    ) -> Awaitable<TransportHandle> {
        let (aw, tx) = awaitable_pair();
        let _ = self.call_from_thread(move |lp| {
            let d = lp.connect_tcp(addr, move |lp, peer| f.build_protocol(lp, peer), opts);
            d.finally(move |lp, r| {
                let h = lp.handle().clone();
                tx.send(r.map(|id| TransportHandle { lp: h, id }));
            });
        });
        aw
    }
}
