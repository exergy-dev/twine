//! kqueue backend (macOS and the BSDs). Compile-checked on Linux with
//! `--target x86_64-apple-darwin`; not executed by the test suite.

use crate::{cvt, Event, ERROR, HANGUP, MAX_EVENTS, READABLE, WRITABLE};
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `EVFILT_USER` identifier for wakeups. User filters have their own ident
/// namespace, so this cannot clash with an fd.
#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly"))]
const WAKE_IDENT: usize = 0;

/// One kqueue instance plus its wake channel (`EVFILT_USER`, or a
/// non-blocking pipe where `EVFILT_USER` is unavailable). Owned by one loop
/// thread; only [`Waker::wake`] may be used from other threads.
///
/// Registrations are edge-triggered (`EV_CLEAR`): read or write until
/// `EWOULDBLOCK` before expecting another event for the same fd.
pub struct Poller {
    kq: Arc<OwnedFd>,
    /// Read end of the wake pipe (registered with the kqueue).
    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
    wake_rd: OwnedFd,
    /// Write end of the wake pipe, shared with every [`Waker`].
    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
    wake_wr: Arc<OwnedFd>,
    /// Kernel event buffer, embedded so `wait` never allocates.
    kevs: [libc::kevent; MAX_EVENTS],
}

/// Cross-thread wake handle. `Clone + Send + Sync`.
///
/// It shares ownership of the kqueue fd (`EVFILT_USER`) or the pipe's write
/// end, so it stays valid after the [`Poller`] is dropped.
#[derive(Clone)]
pub struct Waker {
    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly"))]
    kq: Arc<OwnedFd>,
    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
    wake_wr: Arc<OwnedFd>,
}

/// Builds a zeroed `kevent` with the given fields. The `as _` casts absorb
/// the per-platform integer widths of the struct fields.
fn kev(ident: usize, filter: i16, flags: u16, fflags: u32) -> libc::kevent {
    // SAFETY: kevent is a plain C struct of integers and (nullable) pointers;
    // the all-zero bit pattern is valid.
    let mut k: libc::kevent = unsafe { mem::zeroed() };
    k.ident = ident as _;
    k.filter = filter as _;
    k.flags = flags as _;
    k.fflags = fflags as _;
    k
}

/// Submits `changes` one at a time. A failing change with no event list makes
/// `kevent` return -1 with errno set, so one failure cannot hide another.
/// `ENOENT` (deleting an unregistered filter) is ignored.
fn apply(kq: RawFd, changes: &[libc::kevent]) -> io::Result<()> {
    for ch in changes {
        // SAFETY: `ch` points to one valid kevent; nevents is 0 so the
        // (null) event list is never written; a null timeout is allowed.
        let r = unsafe { libc::kevent(kq, ch, 1, ptr::null_mut(), 0, ptr::null()) };
        if let Err(e) = cvt(r) {
            if e.raw_os_error() != Some(libc::ENOENT) {
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Converts a duration to a `timespec`, saturating `tv_sec`.
fn timespec(d: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: d.as_secs().min(i64::MAX as u64) as _,
        tv_nsec: d.subsec_nanos() as _,
    }
}

#[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
fn set_cloexec_nonblock(fd: RawFd) -> io::Result<()> {
    // SAFETY: fcntl with integer arguments on an fd we own.
    unsafe {
        cvt(libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC))?;
        let fl = cvt(libc::fcntl(fd, libc::F_GETFL))?;
        cvt(libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK))?;
    }
    Ok(())
}

/// Creates a non-blocking, close-on-exec pipe `(read, write)`.
#[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
fn new_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` is a valid array of two c_ints, as pipe(2) requires.
    cvt(unsafe { libc::pipe(fds.as_mut_ptr()) })?;
    // SAFETY: pipe succeeded, so both fds are fresh and exclusively ours;
    // wrapping them immediately means they are closed on any later error.
    let (rd, wr) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    set_cloexec_nonblock(rd.as_raw_fd())?;
    set_cloexec_nonblock(wr.as_raw_fd())?;
    Ok((rd, wr))
}

impl Poller {
    /// Creates the kqueue instance and its wake channel. No fd is leaked on
    /// error.
    pub fn new() -> io::Result<Poller> {
        // SAFETY: kqueue() takes no arguments; on success it returns a fresh
        // fd that we exclusively own.
        let kq = unsafe { OwnedFd::from_raw_fd(cvt(libc::kqueue())?) };
        // SAFETY: fcntl with integer arguments on an fd we own.
        cvt(unsafe { libc::fcntl(kq.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) })?;
        let kevs = [kev(0, 0, 0, 0); MAX_EVENTS];

        #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly"))]
        {
            // EV_CLEAR resets the user event when it is retrieved, so no
            // drain is needed.
            let add = kev(
                WAKE_IDENT,
                libc::EVFILT_USER as _,
                (libc::EV_ADD | libc::EV_CLEAR) as _,
                0,
            );
            apply(kq.as_raw_fd(), &[add])?;
            Ok(Poller {
                kq: Arc::new(kq),
                kevs,
            })
        }
        #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
        {
            let (rd, wr) = new_pipe()?;
            let add = kev(
                rd.as_raw_fd() as usize,
                libc::EVFILT_READ as _,
                (libc::EV_ADD | libc::EV_CLEAR) as _,
                0,
            );
            apply(kq.as_raw_fd(), &[add])?;
            Ok(Poller {
                kq: Arc::new(kq),
                wake_rd: rd,
                wake_wr: Arc::new(wr),
                kevs,
            })
        }
    }

    /// Builds the change list for `fd`: for each of READ and WRITE,
    /// `EV_ADD | EV_CLEAR` when wanted, else `EV_DELETE` if `del` is set.
    fn changes(fd: i32, interest: u32, del: bool) -> ([libc::kevent; 2], usize) {
        let mut out = [kev(0, 0, 0, 0); 2];
        let mut n = 0;
        for (bit, filter) in [
            (READABLE, libc::EVFILT_READ),
            (WRITABLE, libc::EVFILT_WRITE),
        ] {
            if interest & bit != 0 {
                out[n] = kev(
                    fd as usize,
                    filter as _,
                    (libc::EV_ADD | libc::EV_CLEAR) as _,
                    0,
                );
                n += 1;
            } else if del {
                out[n] = kev(fd as usize, filter as _, libc::EV_DELETE as _, 0);
                n += 1;
            }
        }
        (out, n)
    }

    /// Registers `fd` edge-triggered (`EV_CLEAR`) with the given interest
    /// ([`READABLE`] and/or [`WRITABLE`]). [`HANGUP`] and [`ERROR`] are
    /// reported on the registered filters; an fd added with no interest
    /// receives no notifications on kqueue. Re-adding silently succeeds.
    pub fn add(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        let (ch, n) = Self::changes(fd, interest, false);
        apply(self.kq.as_raw_fd(), &ch[..n])
    }

    /// Replaces `fd`'s interest set statelessly: wanted filters are
    /// (re-)added, unwanted ones deleted (`ENOENT` ignored). Re-adding
    /// re-evaluates readiness, so an already-ready direction is reported by
    /// the next `wait`.
    pub fn modify(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        let (ch, n) = Self::changes(fd, interest, true);
        apply(self.kq.as_raw_fd(), &ch[..n])
    }

    /// Removes `fd` (both filters). The caller closes the fd itself, after
    /// `delete`.
    pub fn delete(&mut self, fd: i32) -> io::Result<()> {
        let (ch, n) = Self::changes(fd, 0, true);
        let mut removed = 0usize;
        for c in &ch[..n] {
            // SAFETY: one valid kevent change, no event list, null timeout.
            let r =
                unsafe { libc::kevent(self.kq.as_raw_fd(), c, 1, ptr::null_mut(), 0, ptr::null()) };
            match cvt(r) {
                Ok(_) => removed += 1,
                Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
                Err(e) => return Err(e),
            }
        }
        if removed == 0 {
            // Match epoll: deleting an unregistered fd is an error.
            return Err(io::Error::from_raw_os_error(libc::ENOENT));
        }
        Ok(())
    }

    /// Blocks up to `timeout` (`None` = forever, `Some(ZERO)` = poll) and
    /// fills `events` with at most `min(events.len(), MAX_EVENTS)` entries,
    /// returning the count.
    ///
    /// The wake channel is consumed internally and never reported, so a wait
    /// woken only by [`Waker::wake`] returns `Ok(0)`. `EINTR` is retried with
    /// the remaining timeout. READ and WRITE kevents of the same fd are
    /// merged into one [`Event`] when adjacent. Does not allocate.
    pub fn wait(&mut self, events: &mut [Event], timeout: Option<Duration>) -> io::Result<usize> {
        let max = events.len().min(MAX_EVENTS);
        if max == 0 {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        // `checked_add`: a huge timeout such as Duration::MAX means "forever" and must
        // not panic; without a deadline an EINTR simply retries with the clamped ms.
        let deadline = timeout
            .filter(|d| !d.is_zero())
            .and_then(|d| Instant::now().checked_add(d));
        let mut ts = timeout.map(timespec);
        let n = loop {
            let tsp = ts
                .as_ref()
                .map_or(ptr::null(), |t| t as *const libc::timespec);
            // SAFETY: `kevs` is a valid buffer of MAX_EVENTS kevents and
            // `max <= MAX_EVENTS`; the change list is empty (null, 0);
            // `tsp` is null or points to `ts`, which outlives the call.
            let r = unsafe {
                libc::kevent(
                    self.kq.as_raw_fd(),
                    ptr::null(),
                    0,
                    self.kevs.as_mut_ptr(),
                    max as i32,
                    tsp,
                )
            };
            match cvt(r) {
                Ok(n) => break n as usize,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                    if let Some(dl) = deadline {
                        let rem = dl.saturating_duration_since(Instant::now());
                        if rem.is_zero() {
                            return Ok(0);
                        }
                        ts = Some(timespec(rem));
                    }
                }
                Err(e) => return Err(e),
            }
        };
        let mut out = 0;
        for i in 0..n {
            let k = self.kevs[i];
            if self.is_wake(&k) {
                continue;
            }
            let mut f = 0;
            if k.filter == libc::EVFILT_READ {
                f |= READABLE;
            } else if k.filter == libc::EVFILT_WRITE {
                f |= WRITABLE;
            }
            if k.flags & libc::EV_EOF != 0 {
                f |= HANGUP;
                if k.fflags != 0 {
                    f |= ERROR; // socket error (e.g. ECONNREFUSED) in fflags
                }
            }
            if k.flags & libc::EV_ERROR != 0 {
                f |= ERROR;
            }
            let fd = k.ident as i32;
            if out > 0 && events[out - 1].fd == fd {
                events[out - 1].flags |= f;
                continue;
            }
            events[out] = Event { fd, flags: f };
            out += 1;
        }
        Ok(out)
    }

    /// Recognises (and, for the pipe fallback, drains) the wake event.
    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly"))]
    fn is_wake(&self, k: &libc::kevent) -> bool {
        // EV_CLEAR already reset the user event: nothing to drain.
        k.filter == libc::EVFILT_USER && k.ident as usize == WAKE_IDENT
    }

    /// Recognises (and, for the pipe fallback, drains) the wake event.
    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
    fn is_wake(&self, k: &libc::kevent) -> bool {
        if k.filter != libc::EVFILT_READ || k.ident as i32 != self.wake_rd.as_raw_fd() {
            return false;
        }
        let mut buf = [0u8; 64];
        loop {
            // SAFETY: `buf` is a valid writable buffer of the length passed;
            // the fd is owned by `self`.
            let r =
                unsafe { libc::read(self.wake_rd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            // Read until the pipe is empty (EAGAIN) or closed.
            if r <= 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                return true;
            }
        }
    }

    /// Returns a handle other threads use to wake this poller.
    pub fn waker(&self) -> Waker {
        Waker {
            #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly"))]
            kq: Arc::clone(&self.kq),
            #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
            wake_wr: Arc::clone(&self.wake_wr),
        }
    }
}

impl Waker {
    /// Makes a blocked or future [`Poller::wait`] return promptly: one
    /// `kevent(NOTE_TRIGGER)` on `EVFILT_USER`, or one `write(2)` on the
    /// pipe fallback (a full pipe is not an error). Coalescing is the
    /// caller's job. Does not allocate.
    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly"))]
    pub fn wake(&self) -> io::Result<()> {
        let trig = kev(WAKE_IDENT, libc::EVFILT_USER as _, 0, libc::NOTE_TRIGGER);
        loop {
            // SAFETY: `trig` is one valid kevent; nevents is 0 so the null
            // event list is never written; the kq fd is kept alive by `self`.
            let r = unsafe {
                libc::kevent(
                    self.kq.as_raw_fd(),
                    &trig,
                    1,
                    ptr::null_mut(),
                    0,
                    ptr::null(),
                )
            };
            match cvt(r) {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Makes a blocked or future [`Poller::wait`] return promptly: one
    /// `kevent(NOTE_TRIGGER)` on `EVFILT_USER`, or one `write(2)` on the
    /// pipe fallback (a full pipe is not an error). Coalescing is the
    /// caller's job. Does not allocate.
    #[cfg(not(any(target_os = "macos", target_os = "freebsd", target_os = "dragonfly")))]
    pub fn wake(&self) -> io::Result<()> {
        let byte = 1u8;
        loop {
            // SAFETY: `byte` is a valid 1-byte readable buffer and the fd is
            // kept alive by `self`.
            let r =
                unsafe { libc::write(self.wake_wr.as_raw_fd(), (&byte as *const u8).cast(), 1) };
            if r >= 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            match e.kind() {
                io::ErrorKind::Interrupted => continue,
                // Pipe full: a wakeup is already pending.
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(e),
            }
        }
    }
}
