//! Linux io_uring readiness backend (`uring` feature), with a runtime
//! fallback to [`crate::epoll`].
//!
//! # Design
//!
//! Readiness only: every registered fd has one multishot
//! `IORING_OP_POLL_ADD` armed whose `user_data` is `generation << 32 | fd`.
//!
//! * **Generations.** Each fd slot has a generation that is bumped by
//!   [`Poller::modify`] and [`Poller::delete`]. A CQE whose generation does not
//!   match the slot (or whose slot is unregistered) is stale and dropped, so a
//!   completion already queued when the caller changed its mind, or one that
//!   belongs to a closed-and-reused fd number, is never reported.
//! * **modify** is `POLL_REMOVE(old)` + `POLL_ADD(new, bumped generation)`
//!   rather than `POLL_UPDATE`: the fresh arm re-evaluates readiness exactly
//!   like `EPOLL_CTL_MOD`. Both SQEs stay queued and are submitted by the next
//!   `wait` (one `io_uring_enter` per loop iteration).
//! * **delete** queues `POLL_REMOVE` and submits it immediately, with
//!   `GETEVENTS`. An armed poll holds a reference on the file, so until it is
//!   removed `close(2)` would not tear the socket down (the peer would see no
//!   FIN/RST). The kernel answers `-EALREADY` (leaving the poll armed!) when
//!   the poll has a completion in flight, e.g. an arm that was immediately
//!   ready; the enter with `GETEVENTS` runs that pending task_work and the
//!   `-EALREADY` removes are re-queued and re-submitted (bounded retries; any
//!   leftover is retried by the next `wait`).
//! * **Edge semantics.** A multishot poll posts a CQE per wakeup of the
//!   file's wait queue and re-arms itself, so a second `wait` reports nothing
//!   until new data (a new wakeup) arrives, matching `EPOLLET`. No per-fd
//!   dedup state is needed. As with the Go port, a CQE carries the wakeup's
//!   poll key, so only the triggering direction is reported (new data on an
//!   already writable socket yields `READABLE`, not `READABLE | WRITABLE`).
//! * **Terminated multishot** (CQE without `IORING_CQE_F_MORE`) is re-armed
//!   with the same generation; the new arm re-reports current readiness.
//! * Completions harvested outside `wait` (during `delete` or a full SQ) are
//!   kept in a stash and delivered by the next `wait`; a wake CQE seen there
//!   is remembered so the wakeup is not lost.
//! * The wake eventfd is registered the same way, drained and never reported.
//! * Non-zero timeouts round up to whole milliseconds, as with epoll.
//! * `wait(Some(ZERO))` does not enter the kernel when nothing is queued
//!   (it only peeks the CQ ring); a wakeup whose task_work is still pending
//!   shows up on a later `wait`.
//! * Registration errors such as `EBADF` are asynchronous: `add` succeeds and
//!   the next `wait` reports `ERROR | HANGUP` for that fd.
//!
//! # Fallback
//!
//! [`Poller::new`] returns an epoll-backed poller (`Backend::Epoll`) when
//! `io_uring_setup` fails (`EPERM` in containers, `ENOSYS` on old kernels),
//! the kernel lacks `IORING_FEAT_NODROP`/`IORING_FEAT_EXT_ARG`, or multishot
//! poll (Linux 5.13) is rejected.

use crate::{cvt, epoll, Event, ERROR, HANGUP, MAX_EVENTS, READABLE, WRITABLE};
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use crate::epoll::Waker;

/// Submission queue entries (the CQ is twice as large).
const RING_ENTRIES: u32 = 256;
/// Generations use 31 bits; bit 63 of `user_data` marks internal requests.
const GEN_MASK: u32 = 0x7fff_ffff;
const INTERNAL: u64 = 1 << 63;
/// `user_data` of the wake eventfd poll. Its fd half (`u32::MAX`) is never a
/// valid descriptor.
const WAKE_UD: u64 = !INTERNAL;
/// `user_data` of a completed internal request whose result is ignored.
const IGNORE_UD: u64 = u64::MAX;
/// Upper bound on follow-up SQEs collected per drain pass (no allocation).
const TODO_CAP: usize = 64;
/// Synchronous `POLL_REMOVE` retries in `delete`.
const DEL_RETRIES: usize = 8;

const POLL_BASE: u32 = (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR) as u32;

fn poll_mask(interest: u32) -> u32 {
    let mut m = POLL_BASE;
    if interest & READABLE != 0 {
        m |= libc::POLLIN as u32;
    }
    if interest & WRITABLE != 0 {
        m |= libc::POLLOUT as u32;
    }
    m
}

fn translate(res: u32) -> u32 {
    let mut f = 0;
    if res & (libc::POLLIN | libc::POLLPRI) as u32 != 0 {
        f |= READABLE;
    }
    if res & libc::POLLOUT as u32 != 0 {
        f |= WRITABLE;
    }
    if res & (libc::POLLHUP | libc::POLLRDHUP) as u32 != 0 {
        f |= HANGUP;
    }
    if res & (libc::POLLERR | libc::POLLNVAL) as u32 != 0 {
        f |= ERROR;
    }
    f
}

fn ud_for(fd: i32, gen: u32) -> u64 {
    (u64::from(gen & GEN_MASK) << 32) | u64::from(fd as u32)
}

/// The poll backend actually in use.
enum Backend {
    Uring(Box<Ring>),
    Epoll(Box<epoll::Poller>),
}

/// Readiness poller: io_uring when available, epoll otherwise. Owned by one
/// loop thread; only [`Waker::wake`] may be used from other threads. See the
/// crate-level and `uring` module docs for the semantics.
pub struct Poller {
    backend: Backend,
}

impl Poller {
    /// Creates a poller, preferring io_uring and falling back to epoll when
    /// io_uring is unusable.
    pub fn new() -> io::Result<Poller> {
        match Ring::new() {
            Ok(r) => Ok(Poller {
                backend: Backend::Uring(Box::new(r)),
            }),
            Err(_) => Self::new_epoll(),
        }
    }

    /// Creates an epoll-backed poller regardless of io_uring availability.
    pub(crate) fn new_epoll() -> io::Result<Poller> {
        Ok(Poller {
            backend: Backend::Epoll(Box::new(epoll::Poller::new()?)),
        })
    }

    /// `"io_uring"` or `"epoll"`: the backend this poller uses.
    pub fn backend_name(&self) -> &'static str {
        match &self.backend {
            Backend::Uring(_) => "io_uring",
            Backend::Epoll(p) => p.backend_name(),
        }
    }

    /// Registers `fd` with the given interest ([`READABLE`] and/or
    /// [`WRITABLE`]); [`HANGUP`] and [`ERROR`] are always reported. Fails with
    /// `EEXIST` if `fd` is already registered.
    pub fn add(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        match &mut self.backend {
            Backend::Uring(r) => r.add(fd, interest),
            Backend::Epoll(p) => p.add(fd, interest),
        }
    }

    /// Replaces `fd`'s interest set. If `fd` is already ready for a newly
    /// requested direction, the next `wait` reports it. Fails with `ENOENT`
    /// if `fd` is not registered.
    pub fn modify(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        match &mut self.backend {
            Backend::Uring(r) => r.modify(fd, interest),
            Backend::Epoll(p) => p.modify(fd, interest),
        }
    }

    /// Removes `fd`. The caller closes the fd itself, after `delete`; with
    /// io_uring the poll is cancelled before this returns so `close(2)`
    /// really closes the socket.
    pub fn delete(&mut self, fd: i32) -> io::Result<()> {
        match &mut self.backend {
            Backend::Uring(r) => r.delete(fd),
            Backend::Epoll(p) => p.delete(fd),
        }
    }

    /// Blocks up to `timeout` (`None` = forever, `Some(ZERO)` = poll) and
    /// fills `events` with at most `min(events.len(), MAX_EVENTS)` entries,
    /// returning the count. The wake eventfd is drained internally and never
    /// reported. `EINTR` is retried. Does not allocate.
    pub fn wait(&mut self, events: &mut [Event], timeout: Option<Duration>) -> io::Result<usize> {
        match &mut self.backend {
            Backend::Uring(r) => {
                let max = events.len().min(MAX_EVENTS);
                if max == 0 {
                    return Err(io::Error::from_raw_os_error(libc::EINVAL));
                }
                r.wait(&mut events[..max], timeout)
            }
            Backend::Epoll(p) => p.wait(events, timeout),
        }
    }

    /// Returns a handle other threads use to wake this poller.
    pub fn waker(&self) -> Waker {
        match &self.backend {
            Backend::Uring(r) => Waker {
                fd: Arc::clone(&r.wake),
            },
            Backend::Epoll(p) => p.waker(),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    gen: u32,
    reg: bool,
    mask: u32,
}

struct Ring {
    state: State,
    wake: Arc<OwnedFd>,
}

struct State {
    ring: IoUring,
    slots: Vec<Slot>,
    /// Events harvested outside `wait`.
    stash: VecDeque<Event>,
    /// Follow-up work collected while draining: values with `INTERNAL` set
    /// are `POLL_REMOVE` targets to re-submit, others are polls to re-arm.
    todo: Vec<u64>,
    /// A wake CQE was consumed outside `wait`.
    woke: bool,
    wake_fd: RawFd,
}

fn errno_of(e: &io::Error) -> i32 {
    e.raw_os_error().unwrap_or(0)
}

impl Ring {
    fn new() -> io::Result<Ring> {
        let ring = IoUring::new(RING_ENTRIES)?;
        let p = ring.params();
        if !p.is_feature_nodrop() || !p.is_feature_ext_arg() {
            return Err(io::Error::from_raw_os_error(libc::ENOSYS));
        }
        // SAFETY: eventfd has no pointer arguments; on success it returns a
        // fresh fd that we exclusively own.
        let wake = unsafe {
            OwnedFd::from_raw_fd(cvt(libc::eventfd(
                0,
                libc::EFD_NONBLOCK | libc::EFD_CLOEXEC,
            ))?)
        };
        let wake_fd = wake.as_raw_fd();
        let mut st = State {
            ring,
            slots: Vec::new(),
            stash: VecDeque::with_capacity(MAX_EVENTS),
            todo: Vec::with_capacity(TODO_CAP),
            woke: false,
            wake_fd,
        };
        st.push(&arm_entry(wake_fd, libc::POLLIN as u32, WAKE_UD))?;
        st.enter(Some(Duration::ZERO))?;
        // The eventfd is idle, so any CQE now is an immediate failure, e.g.
        // -EINVAL when multishot poll is unsupported.
        if let Some(cqe) = st.ring.completion().next() {
            return Err(io::Error::from_raw_os_error(-cqe.result().min(-1)));
        }
        Ok(Ring {
            state: st,
            wake: Arc::new(wake),
        })
    }

    fn add(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        if fd < 0 {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        let st = &mut self.state;
        let idx = fd as usize;
        if st.slots.len() <= idx {
            st.slots.resize(idx + 1, Slot::default());
        }
        if st.slots[idx].reg {
            return Err(io::Error::from_raw_os_error(libc::EEXIST));
        }
        let mask = poll_mask(interest);
        let ud = ud_for(fd, st.slots[idx].gen);
        st.push(&arm_entry(fd, mask, ud))?;
        st.slots[idx].reg = true;
        st.slots[idx].mask = mask;
        Ok(())
    }

    fn modify(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        let st = &mut self.state;
        let slot = st.slot(fd)?;
        let old = ud_for(fd, slot.gen);
        let gen = slot.gen.wrapping_add(1) & GEN_MASK;
        let mask = poll_mask(interest);
        st.push(&remove_entry(old))?;
        st.push(&arm_entry(fd, mask, ud_for(fd, gen)))?;
        let s = &mut st.slots[fd as usize];
        s.gen = gen;
        s.mask = mask;
        Ok(())
    }

    fn delete(&mut self, fd: i32) -> io::Result<()> {
        let st = &mut self.state;
        let slot = st.slot(fd)?;
        let old = ud_for(fd, slot.gen);
        st.push(&remove_entry(old))?;
        let s = &mut st.slots[fd as usize];
        s.reg = false;
        s.gen = s.gen.wrapping_add(1) & GEN_MASK;
        // Get the poll cancelled now: it pins the file until removed.
        for _ in 0..=DEL_RETRIES {
            st.enter(Some(Duration::ZERO))?;
            st.drain(None);
            if st.sq_len() == 0 {
                break;
            }
        }
        Ok(())
    }

    fn wait(&mut self, events: &mut [Event], timeout: Option<Duration>) -> io::Result<usize> {
        let st = &mut self.state;
        let max = events.len();
        // Same contract as epoll: non-zero timeouts round up to whole ms.
        let timeout = timeout.map(|d| {
            if d.is_zero() {
                d
            } else {
                Duration::from_millis(u64::try_from(epoll::timeout_ms(Some(d))).unwrap_or(0))
                    .max(Duration::from_millis(1))
            }
        });
        let deadline = timeout
            .filter(|d| !d.is_zero())
            .and_then(|d| Instant::now().checked_add(d));
        let mut out = 0;
        while out < max {
            let Some(ev) = st.stash.pop_front() else {
                break;
            };
            events[out] = ev;
            out += 1;
        }
        let mut last = false;
        loop {
            st.drain(Some((events, &mut out)));
            let woke = std::mem::take(&mut st.woke);
            if out > 0 || woke {
                if st.sq_len() > 0 {
                    // Flush queued modifies/re-arms without blocking.
                    st.enter(Some(Duration::ZERO))?;
                }
                return Ok(out);
            }
            if last {
                return Ok(0);
            }
            let rem = match timeout {
                None => None,
                Some(d) if d.is_zero() => Some(Duration::ZERO),
                Some(d) => match deadline {
                    Some(dl) => Some(dl.saturating_duration_since(Instant::now())),
                    None => Some(d), // unrepresentable deadline: effectively forever
                },
            };
            if let Some(r) = rem {
                if r.is_zero() {
                    if !st.needs_enter() {
                        return Ok(0);
                    }
                    last = true;
                }
            }
            st.enter(rem)?;
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    last = true;
                }
            }
        }
    }
}

fn arm_entry(fd: RawFd, mask: u32, ud: u64) -> squeue::Entry {
    opcode::PollAdd::new(types::Fd(fd), mask)
        .multi(true)
        .build()
        .user_data(ud)
}

fn remove_entry(target: u64) -> squeue::Entry {
    opcode::PollRemove::new(target)
        .build()
        .user_data(INTERNAL | target)
}

impl State {
    fn slot(&self, fd: i32) -> io::Result<Slot> {
        match self.slots.get(fd as usize) {
            Some(s) if fd >= 0 && s.reg => Ok(*s),
            _ => Err(io::Error::from_raw_os_error(libc::ENOENT)),
        }
    }

    fn sq_len(&mut self) -> usize {
        self.ring.submission().len()
    }

    /// Whether a zero-timeout wait must enter the kernel: SQEs are queued, the
    /// kernel holds overflowed CQEs, or task_work is waiting.
    fn needs_enter(&mut self) -> bool {
        let sq = self.ring.submission();
        !sq.is_empty() || sq.cq_overflow() || sq.taskrun()
    }

    /// Queues one SQE; when the SQ is full, submits and retries.
    fn push(&mut self, e: &squeue::Entry) -> io::Result<()> {
        for _ in 0..1000 {
            // SAFETY: the entry is a POLL_ADD/POLL_REMOVE, which reference no
            // user memory (only an fd, flags and a user_data value), so there
            // is nothing that must outlive the submission.
            if unsafe { self.ring.submission().push(e) }.is_ok() {
                return Ok(());
            }
            self.enter(Some(Duration::ZERO))?;
            self.drain(None);
        }
        Err(io::Error::from_raw_os_error(libc::EBUSY))
    }

    /// Submits queued SQEs and waits for at least one CQE or `timeout`
    /// (`None` = forever). Always enters with `GETEVENTS`, which also runs this
    /// thread's pending poll task_work. Timeouts, `EINTR`, `EAGAIN` and `EBUSY`
    /// are not errors: the caller re-examines the CQ.
    fn enter(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let r = match timeout {
            None => self.ring.submitter().submit_and_wait(1),
            Some(d) => {
                let ts = types::Timespec::from(d.min(Duration::from_secs(1 << 40)));
                let args = types::SubmitArgs::new().timespec(&ts);
                self.ring.submitter().submit_with_args(1, &args)
            }
        };
        match r {
            Ok(_) => Ok(()),
            Err(e)
                if matches!(
                    errno_of(&e),
                    libc::ETIME | libc::EINTR | libc::EAGAIN | libc::EBUSY
                ) =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn deliver(
        stash: &mut VecDeque<Event>,
        dst: &mut Option<(&mut [Event], &mut usize)>,
        ev: Event,
    ) {
        match dst {
            Some((buf, out)) => {
                buf[**out] = ev;
                **out += 1;
            }
            None => stash.push_back(ev),
        }
    }

    /// Consumes CQEs: with `dst`, translated events fill the slice (stopping
    /// when full, leaving the rest in the ring); without it they go to the
    /// stash. Then queues the follow-up removes and re-arms.
    fn drain(&mut self, mut dst: Option<(&mut [Event], &mut usize)>) {
        {
            let State {
                ring,
                slots,
                stash,
                todo,
                woke,
                wake_fd,
            } = &mut *self;
            let mut cq = ring.completion();
            loop {
                if todo.len() >= TODO_CAP {
                    break;
                }
                if let Some((buf, out)) = &dst {
                    if **out >= buf.len() {
                        break;
                    }
                }
                let Some(cqe) = cq.next() else { break };
                let (ud, res) = (cqe.user_data(), cqe.result());
                let more = cqueue::more(cqe.flags());
                if ud & INTERNAL != 0 {
                    if ud != IGNORE_UD && res == -libc::EALREADY {
                        // Completion in flight: the poll is still armed.
                        todo.push(ud);
                    }
                    continue;
                }
                if ud == WAKE_UD {
                    *woke = true;
                    drain_eventfd(*wake_fd);
                    if !more {
                        todo.push(ud);
                    }
                    continue;
                }
                let fd = ud as u32;
                let gen = (ud >> 32) as u32;
                match slots.get(fd as usize) {
                    Some(s) if s.reg && s.gen == gen => {}
                    _ => continue,
                }
                let flags = if res < 0 {
                    if res == -libc::ECANCELED {
                        // Terminated under us (not by our remove: that would
                        // be stale): re-arm.
                        todo.push(ud);
                        continue;
                    }
                    ERROR | HANGUP
                } else {
                    if !more {
                        todo.push(ud);
                    }
                    translate(res as u32)
                };
                if flags != 0 {
                    let ev = Event {
                        fd: fd as i32,
                        flags,
                    };
                    Self::deliver(stash, &mut dst, ev);
                }
            }
        }
        while let Some(t) = self.todo.pop() {
            let entry = if t & INTERNAL != 0 {
                remove_entry(t & !INTERNAL)
            } else if t == WAKE_UD {
                arm_entry(self.wake_fd, libc::POLLIN as u32, WAKE_UD)
            } else {
                let (fd, gen) = (t as u32, (t >> 32) as u32);
                match self.slots.get(fd as usize) {
                    Some(s) if s.reg && s.gen == gen => arm_entry(fd as i32, s.mask, t),
                    _ => continue,
                }
            };
            if self.push(&entry).is_err() {
                // SQ and CQ both wedged; the next wait retries via the CQ.
                break;
            }
        }
    }
}

/// Resets the eventfd counter with one 8-byte read (`EAGAIN` = already zero).
fn drain_eventfd(fd: RawFd) {
    let mut buf = [0u8; 8];
    loop {
        // SAFETY: `buf` is a valid 8-byte writable buffer, the length passed
        // equals its size, and `fd` is the eventfd owned by the Ring.
        let r = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), 8) };
        if r < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return;
    }
}
