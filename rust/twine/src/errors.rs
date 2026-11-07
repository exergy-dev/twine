//! Error values (design Section 11). Every variant is matchable; `Io` carries
//! the OS error for anything not covered by a dedicated variant.

use std::fmt;

/// Result alias used throughout twine.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type passed to `connection_lost`, used to reject a `Deferred`, and
/// returned by fallible API calls.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// `write` on a closed or stale transport. The write is a no-op and is
    /// counted in `MetricsSnapshot::write_after_close`.
    Closed,
    /// A loop-affine call was made after the loop exited.
    LoopStopped,
    /// `connect_tcp` or `listen_tcp` after the engine was stopped.
    EngineStopped,
    /// Pending writes exceeded `Options::write_buffer_limit`; the connection was aborted.
    WriteBufferFull,
    /// `Awaitable::wait` was called on a loop thread (panic value).
    AwaitOnLoop,
    /// The idle timer fired (`Transport::set_idle_timeout`).
    IdleTimeout,
    /// A `connect_tcp` attempt exceeded `DialOptions::timeout`.
    ConnectTimeout,
    /// The peer reset the connection (ECONNRESET / EPIPE).
    ConnectionReset,
    /// The engine shut down a connection that still had pending writes after
    /// `Options::shutdown_timeout`.
    Shutdown,
    /// A `Resolver` was dropped without resolving its `Deferred`.
    Abandoned,
    /// The protocol addressed by a `ConnId` is currently running its own callback
    /// and cannot be re-entered (`Loop::with_protocol`).
    Busy,
    /// A framing limit was exceeded (`LineReceiver` max line length).
    LineTooLong,
    /// A framing limit was exceeded (length-prefixed / netstring frame too large).
    FrameTooLarge,
    /// Protocol or callback code panicked; the connection was aborted.
    Panic(PanicError),
    /// An OS error not covered above.
    Io(std::io::Error),
    /// An address could not be parsed or resolved.
    Addr(String),
}

impl Error {
    /// Maps a raw `errno` to an `Error`, folding ECONNRESET/EPIPE into
    /// [`Error::ConnectionReset`].
    pub fn from_errno(errno: i32) -> Error {
        match errno {
            libc::ECONNRESET | libc::EPIPE => Error::ConnectionReset,
            _ => Error::Io(std::io::Error::from_raw_os_error(errno)),
        }
    }

    /// Returns the last OS error as an `Error`.
    pub fn last_os_error() -> Error {
        let e = std::io::Error::last_os_error();
        match e.raw_os_error() {
            Some(code) => Error::from_errno(code),
            None => Error::Io(e),
        }
    }

    /// Returns the raw OS error code when this is an I/O error carrying one.
    pub fn raw_os_error(&self) -> Option<i32> {
        match self {
            Error::Io(e) => e.raw_os_error(),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Closed => f.write_str("twine: transport closed"),
            Error::LoopStopped => f.write_str("twine: loop stopped"),
            Error::EngineStopped => f.write_str("twine: engine stopped"),
            Error::WriteBufferFull => f.write_str("twine: write buffer full"),
            Error::AwaitOnLoop => f.write_str("twine: Awaitable::wait called on a loop thread"),
            Error::IdleTimeout => f.write_str("twine: idle timeout"),
            Error::ConnectTimeout => f.write_str("twine: connect timeout"),
            Error::ConnectionReset => f.write_str("twine: connection reset by peer"),
            Error::Shutdown => f.write_str("twine: engine shutdown"),
            Error::Abandoned => f.write_str("twine: resolver dropped without settling"),
            Error::Busy => f.write_str("twine: protocol is busy in its own callback"),
            Error::LineTooLong => f.write_str("twine: line too long"),
            Error::FrameTooLarge => f.write_str("twine: frame too large"),
            Error::Panic(p) => write!(f, "twine: panic: {}", p.message),
            Error::Io(e) => write!(f, "twine: {e}"),
            Error::Addr(s) => write!(f, "twine: bad address: {s}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        match e.raw_os_error() {
            Some(libc::ECONNRESET) | Some(libc::EPIPE) => Error::ConnectionReset,
            _ => Error::Io(e),
        }
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(e: std::net::AddrParseError) -> Self {
        Error::Addr(e.to_string())
    }
}

/// A panic recovered from protocol or callback code (design Section 8 rule 8).
#[derive(Debug, Clone)]
pub struct PanicError {
    /// The panic message (`&str` or `String` payloads; `"<non-string panic>"` otherwise).
    pub message: String,
    /// The captured backtrace when `RUST_BACKTRACE` or `TWINE_DEBUG` is set.
    pub backtrace: Option<String>,
}

impl PanicError {
    /// Builds a `PanicError` from a `catch_unwind` payload.
    pub fn from_payload(payload: Box<dyn std::any::Any + Send>) -> PanicError {
        let message = if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic>".to_string()
        };
        let backtrace = if crate::options::debug_enabled() {
            Some(std::backtrace::Backtrace::force_capture().to_string())
        } else {
            None
        };
        PanicError { message, backtrace }
    }
}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "panic: {}", self.message)
    }
}

impl std::error::Error for PanicError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errno_mapping() {
        assert!(matches!(
            Error::from_errno(libc::EPIPE),
            Error::ConnectionReset
        ));
        assert!(matches!(
            Error::from_errno(libc::ECONNRESET),
            Error::ConnectionReset
        ));
        assert_eq!(
            Error::from_errno(libc::EAGAIN).raw_os_error(),
            Some(libc::EAGAIN)
        );
    }

    #[test]
    fn panic_payload_message() {
        let p = std::panic::catch_unwind(|| panic!("boom {}", 1)).unwrap_err();
        assert_eq!(PanicError::from_payload(p).message, "boom 1");
    }
}
