//! Timer plumbing on the loop's timing wheel (design Section 7.4): closure
//! timers, connection token timers and idle/linger timers.

use std::time::Duration;

use crate::conn::{flags, Kind, Table};
use crate::errors::Error;
use crate::event_loop::{ConnId, Loop, TimerId};
use crate::metrics::bump;

/// Wheel expiry used as a marker for timers parked in the zero-delay list.
const ZERO_AT: u64 = 1 << 60;

/// The payload stored in the wheel.
pub(crate) enum TimerKind {
    /// The connection's idle (or half-close linger) deadline.
    Idle(ConnId),
    /// `Transport::call_later(d, token)`.
    Conn(ConnId, u64),
    /// `Loop::call_later(d, f)`.
    Fn(Box<dyn FnOnce(&mut Loop)>),
}

impl Loop {
    fn deadline(&self, d: Duration) -> u64 {
        self.rt
            .now_ns
            .saturating_add(d.as_nanos().min(1 << 60) as u64)
    }

    /// Schedules `kind` after `d`. A zero delay parks the timer in the due list so
    /// it fires on the next iteration: never re-entrantly, and once per
    /// iteration however often a callback re-arms itself.
    pub(crate) fn add_timer(&mut self, d: Duration, kind: TimerKind) -> TimerId {
        if d.is_zero() {
            let id = self.rt.wheel.add(ZERO_AT, kind);
            self.rt.due.push(id);
            id
        } else {
            let at = self.deadline(d);
            self.rt.wheel.add(at, kind)
        }
    }

    /// Snapshots the zero-delay timers queued so far.
    pub(crate) fn take_due(&mut self) {
        std::mem::swap(&mut self.rt.due, &mut self.rt.due_next);
        self.rt.due.clear();
    }

    /// Fires the zero-delay timers snapshotted by `take_due`.
    pub(crate) fn fire_due(&mut self) {
        let mut i = 0;
        while i < self.rt.due_next.len() {
            let id = self.rt.due_next[i];
            i += 1;
            // Cancelled, or moved onto the wheel by `reset_timer`, if no longer parked.
            if self.rt.wheel.expires(id) == Some(ZERO_AT) {
                if let Some(kind) = self.rt.wheel.remove(id) {
                    self.fire_timer(id, kind);
                }
            }
        }
        self.rt.due_next.clear();
    }

    /// Runs `f` on this loop after `d`. `ZERO` runs on the next iteration (never
    /// re-entrantly: the wheel files a timer that is already due under its next
    /// advance).
    pub fn call_later(&mut self, d: Duration, f: impl FnOnce(&mut Loop) + 'static) -> TimerId {
        self.add_timer(d, TimerKind::Fn(Box::new(f)))
    }

    /// Cancels a timer; true if it was still pending.
    pub fn cancel_timer(&mut self, t: TimerId) -> bool {
        match self.rt.wheel.remove(t) {
            Some(kind) => {
                if let TimerKind::Conn(id, _) = &kind {
                    self.forget_conn_timer(*id, t);
                }
                true
            }
            None => false,
        }
    }

    /// Drops `t` from the connection's tracked token timers.
    fn forget_conn_timer(&mut self, id: ConnId, t: TimerId) {
        if let Some(io) = self.io_mut(id) {
            if let Some(i) = io.timers.iter().position(|x| *x == t) {
                io.timers.swap_remove(i);
            }
        }
    }

    /// Reschedules a pending timer to fire `d` from now; false if it already
    /// fired or was cancelled.
    pub fn reset_timer(&mut self, t: TimerId, d: Duration) -> bool {
        let at = self.deadline(d);
        self.rt.wheel.reset(t, at)
    }

    /// Fires one expired timer, then drains the post-callback queue.
    pub(crate) fn fire_timer(&mut self, tid: TimerId, kind: TimerKind) {
        bump(&self.rt.metrics.timers_fired);
        match kind {
            TimerKind::Idle(id) => self.on_idle(tid, id),
            TimerKind::Conn(id, token) => {
                self.forget_conn_timer(id, tid);
                let live = self
                    .io(id)
                    .is_some_and(|io| io.kind == Kind::Tcp && io.flags & flags::CLOSED == 0);
                if live {
                    self.dispatch(id, |p, t| p.timer_fired(t, token));
                }
            }
            TimerKind::Fn(f) => self.run_guarded("timer callback", f),
        }
        self.drain_pending();
    }

    /// The idle deadline passed: abort (or, for the linger deadline, close cleanly).
    fn on_idle(&mut self, tid: TimerId, id: ConnId) {
        let Loop { conns, rt } = &mut *self;
        let Some(s) = conns.find(id) else {
            return;
        };
        if s.io.kind != Kind::Tcp {
            return;
        }
        let io = &mut s.io;
        if io.idle == Some(tid) {
            io.idle = None;
        }
        if io.flags & flags::CLOSED != 0 {
            return;
        }
        if io.flags & flags::LINGER == 0 {
            io.lost = Some(Error::IdleTimeout);
        }
        crate::conn::abort_io(rt, io, id.fd);
    }
}
