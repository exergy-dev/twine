//! Twine internal crate `twine-poll`. See `rust/docs/INTERNALS.md` for the
//! contract this crate implements. Not part of the public API.
//!
//! A thin, allocation-free abstraction over epoll (Linux) and kqueue
//! (macOS/BSD), plus a cross-thread wakeup.
//!
//! # Edge-triggered semantics
//!
//! Both backends are edge-triggered (`EPOLLET` / `EV_CLEAR`): a registered fd
//! is reported once per readiness transition, so the caller must read or write
//! until `EWOULDBLOCK` before expecting another event for that fd.
//!
//! # io_uring backend
//!
//! With the `uring` cargo feature (Linux only; a no-op elsewhere),
//! [`Poller::new`] tries an io_uring readiness backend (multishot
//! `IORING_OP_POLL_ADD`) and falls back to epoll at runtime when io_uring is
//! unavailable. [`Poller::backend_name`] reports which one is in use. The API
//! and semantics are identical.
//!
//! # Wake fd
//!
//! The wake channel (eventfd / `EVFILT_USER` / pipe) is registered internally
//! and drained inside [`Poller::wait`]. It is never reported to the caller: a
//! `wait` woken only by [`Waker::wake`] returns `Ok(0)`.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(test)]
#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

#[cfg(target_os = "linux")]
mod epoll;
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
mod kqueue;
#[cfg(all(target_os = "linux", feature = "uring"))]
mod uring;

#[cfg(all(target_os = "linux", not(feature = "uring")))]
pub use epoll::{Poller, Waker};
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
pub use kqueue::{Poller, Waker};
#[cfg(all(target_os = "linux", feature = "uring"))]
pub use uring::{Poller, Waker};

/// The fd has data to read (`EPOLLIN | EPOLLPRI` / `EVFILT_READ`). As an
/// interest flag it requests read readiness.
pub const READABLE: u32 = 1 << 0;
/// The fd can accept writes (`EPOLLOUT` / `EVFILT_WRITE`). As an interest flag
/// it requests write readiness.
pub const WRITABLE: u32 = 1 << 1;
/// The peer closed or this side was shut down (`EPOLLHUP | EPOLLRDHUP` /
/// `EV_EOF`). Always reported; need not be requested.
pub const HANGUP: u32 = 1 << 2;
/// A pending socket error (`EPOLLERR` / `EV_ERROR`). Always reported; need not
/// be requested.
pub const ERROR: u32 = 1 << 3;

/// Maximum number of events [`Poller::wait`] translates per call. The kernel
/// event array is a fixed-size array inside the poller, so `wait` never
/// allocates.
pub const MAX_EVENTS: usize = 256;

/// One ready notification. [`Poller::wait`] fills a caller-provided slice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Event {
    /// The file descriptor the event refers to.
    pub fd: i32,
    /// A combination of [`READABLE`], [`WRITABLE`], [`HANGUP`] and [`ERROR`].
    pub flags: u32,
}

/// Wraps a `-1`-on-error libc return value into an `io::Result`.
#[inline]
fn cvt(ret: i32) -> std::io::Result<i32> {
    if ret < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
#[cfg(all(test, target_os = "linux", feature = "uring"))]
mod uring_tests;
