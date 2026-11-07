//! Linux backend: epoll plus an eventfd wakeup.

use crate::{cvt, Event, ERROR, HANGUP, MAX_EVENTS, READABLE, WRITABLE};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `epoll_event.u64` marker identifying the wake eventfd. Real fds are
/// non-negative `i32`s stored zero-extended, so this can never collide.
const WAKE_MARKER: u64 = u64::MAX;

/// One epoll instance plus its eventfd wakeup channel. Owned by one loop
/// thread; only [`Waker::wake`] may be used from other threads.
///
/// Registrations are edge-triggered (`EPOLLET`): read or write until
/// `EWOULDBLOCK` before expecting another event for the same fd.
pub struct Poller {
    epfd: OwnedFd,
    wake: Arc<OwnedFd>,
    /// Kernel event buffer, embedded so `wait` never allocates.
    kevs: [libc::epoll_event; MAX_EVENTS],
}

/// Cross-thread wake handle. `Clone + Send + Sync`.
///
/// It shares ownership of the eventfd, so it stays valid (and harmless) after
/// the [`crate::Poller`] is dropped.
#[derive(Clone)]
pub struct Waker {
    pub(crate) fd: Arc<OwnedFd>,
}

fn interest_bits(interest: u32) -> u32 {
    // EPOLLRDHUP and EPOLLET are always set so a half-close is reported even
    // without read interest; EPOLLHUP and EPOLLERR are implicit.
    let mut ev = (libc::EPOLLRDHUP | libc::EPOLLET) as u32;
    if interest & READABLE != 0 {
        ev |= libc::EPOLLIN as u32;
    }
    if interest & WRITABLE != 0 {
        ev |= libc::EPOLLOUT as u32;
    }
    ev
}

/// Converts a timeout to the `epoll_wait` millisecond argument: `None` blocks
/// forever (-1); positive sub-millisecond values round *up* so they never
/// become a non-blocking poll; the result is clamped to `i32::MAX`.
pub(crate) fn timeout_ms(timeout: Option<Duration>) -> i32 {
    let Some(d) = timeout else { return -1 };
    let mut ms = d.as_millis();
    if d.subsec_nanos() % 1_000_000 != 0 {
        ms += 1;
    }
    i32::try_from(ms).unwrap_or(i32::MAX)
}

impl Poller {
    /// Name of this backend: always `"epoll"`.
    pub fn backend_name(&self) -> &'static str {
        "epoll"
    }

    /// Creates the epoll instance and the eventfd used by [`Waker::wake`].
    /// No fd is leaked on error.
    pub fn new() -> io::Result<Poller> {
        // SAFETY: epoll_create1 has no pointer arguments; on success it
        // returns a fresh fd that we exclusively own.
        let epfd = unsafe { OwnedFd::from_raw_fd(cvt(libc::epoll_create1(libc::EPOLL_CLOEXEC))?) };
        // SAFETY: eventfd has no pointer arguments; on success it returns a
        // fresh fd that we exclusively own.
        let wake = unsafe {
            OwnedFd::from_raw_fd(cvt(libc::eventfd(
                0,
                libc::EFD_NONBLOCK | libc::EFD_CLOEXEC,
            ))?)
        };
        let p = Poller {
            epfd,
            wake: Arc::new(wake),
            kevs: [libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS],
        };
        p.ctl(
            libc::EPOLL_CTL_ADD,
            p.wake.as_raw_fd(),
            (libc::EPOLLIN | libc::EPOLLET) as u32,
            WAKE_MARKER,
        )?;
        Ok(p)
    }

    fn ctl(&self, op: i32, fd: RawFd, events: u32, data: u64) -> io::Result<()> {
        let mut ev = libc::epoll_event { events, u64: data };
        // SAFETY: `ev` is a valid, initialised epoll_event that outlives the
        // call; the kernel only reads it (and ignores it for CTL_DEL).
        cvt(unsafe { libc::epoll_ctl(self.epfd.as_raw_fd(), op, fd, &mut ev) }).map(drop)
    }

    /// Registers `fd` edge-triggered with the given interest ([`READABLE`]
    /// and/or [`WRITABLE`]). [`HANGUP`] and [`ERROR`] are always reported.
    /// Fails with `EEXIST` if `fd` is already registered.
    pub fn add(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        self.ctl(
            libc::EPOLL_CTL_ADD,
            fd,
            interest_bits(interest),
            fd as u32 as u64,
        )
    }

    /// Replaces `fd`'s interest set. If `fd` is already ready for a newly
    /// requested direction, the next `wait` reports it.
    pub fn modify(&mut self, fd: i32, interest: u32) -> io::Result<()> {
        self.ctl(
            libc::EPOLL_CTL_MOD,
            fd,
            interest_bits(interest),
            fd as u32 as u64,
        )
    }

    /// Removes `fd`. The caller closes the fd itself, after `delete`.
    pub fn delete(&mut self, fd: i32) -> io::Result<()> {
        self.ctl(libc::EPOLL_CTL_DEL, fd, 0, 0)
    }

    /// Blocks up to `timeout` (`None` = forever, `Some(ZERO)` = poll) and
    /// fills `events` with at most `min(events.len(), MAX_EVENTS)` entries,
    /// returning the count.
    ///
    /// The wake eventfd is drained internally and never reported, so a wait
    /// woken only by [`Waker::wake`] returns `Ok(0)`. `EINTR` is retried with
    /// the remaining timeout. Non-zero timeouts round up to whole
    /// milliseconds. Does not allocate.
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
        let mut ms = timeout_ms(timeout);
        let n = loop {
            // SAFETY: `kevs` is a valid buffer of MAX_EVENTS epoll_events and
            // `max <= MAX_EVENTS`, so the kernel writes only within it.
            let r = unsafe {
                libc::epoll_wait(
                    self.epfd.as_raw_fd(),
                    self.kevs.as_mut_ptr(),
                    max as i32,
                    ms,
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
                        ms = timeout_ms(Some(rem));
                    }
                }
                Err(e) => return Err(e),
            }
        };
        let mut out = 0;
        for i in 0..n {
            // Copy the (packed) struct out before reading its fields.
            let kev = self.kevs[i];
            let (bits, data) = (kev.events, kev.u64);
            if data == WAKE_MARKER {
                self.drain_wake();
                continue;
            }
            let mut f = 0;
            if bits & (libc::EPOLLIN | libc::EPOLLPRI) as u32 != 0 {
                f |= READABLE;
            }
            if bits & libc::EPOLLOUT as u32 != 0 {
                f |= WRITABLE;
            }
            if bits & (libc::EPOLLHUP | libc::EPOLLRDHUP) as u32 != 0 {
                f |= HANGUP;
            }
            if bits & libc::EPOLLERR as u32 != 0 {
                f |= ERROR;
            }
            events[out] = Event {
                fd: data as u32 as i32,
                flags: f,
            };
            out += 1;
        }
        Ok(out)
    }

    /// Resets the eventfd counter to zero with a single 8-byte read (a
    /// non-semaphore eventfd returns and clears the whole counter; `EAGAIN`
    /// means it was already zero). Required so the counter never saturates.
    fn drain_wake(&self) {
        let mut buf = [0u8; 8];
        loop {
            // SAFETY: `buf` is a valid 8-byte writable buffer, the length
            // passed equals its size, and the fd is owned by `self`.
            let r = unsafe { libc::read(self.wake.as_raw_fd(), buf.as_mut_ptr().cast(), 8) };
            if r < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }
    }

    /// Returns a handle other threads use to wake this poller.
    pub fn waker(&self) -> Waker {
        Waker {
            fd: Arc::clone(&self.wake),
        }
    }
}

impl Waker {
    /// Makes a blocked or future [`crate::Poller::wait`] return promptly: one
    /// `write(2)` on the eventfd. Coalescing is the caller's job. A saturated
    /// counter (`EAGAIN`) is not an error. Does not allocate.
    pub fn wake(&self) -> io::Result<()> {
        let one = 1u64.to_ne_bytes();
        loop {
            // SAFETY: `one` is a valid 8-byte readable buffer, the length
            // passed equals its size, and the fd is kept alive by `self.fd`.
            let r = unsafe { libc::write(self.fd.as_raw_fd(), one.as_ptr().cast(), 8) };
            if r >= 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            match e.kind() {
                io::ErrorKind::Interrupted => continue,
                // Counter saturated: the reader is already guaranteed to wake.
                io::ErrorKind::WouldBlock => return Ok(()),
                _ => return Err(e),
            }
        }
    }
}
