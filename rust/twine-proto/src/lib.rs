//! Framing protocols for twine (design Section 9): [`Lines`], [`Netstrings`] and
//! [`LengthPrefixed`].
//!
//! Each framing protocol comes in two layers:
//!
//! * a pure **parser** ([`LineParser`], [`NetstringParser`], [`LengthPrefixParser`])
//!   with no dependency on a running loop, so it can be unit-tested and fuzzed
//!   directly. Parsers work in place on the loop's read buffer: a frame that lies
//!   entirely inside the chunk is delivered with no copy and no allocation, and
//!   only an incomplete trailing frame is copied into a small per-connection
//!   buffer (cleared, and released when large, as soon as the frame completes);
//! * a **wrapper** [`Protocol`](twine::Protocol) that owns the parser and a user
//!   handler (`LineHandler`, `NetstringHandler`, `FrameHandler`). Wrappers forward
//!   every `Protocol` method to the handler, abort the connection on a framing
//!   violation and report the framing error as the `reason` of `connection_lost`.
//!
//! Framing errors are [`twine::Error::LineTooLong`] and
//! [`twine::Error::FrameTooLarge`] for size violations; structurally invalid input
//! (a bad netstring, a length prefix smaller than the prefix itself) is an
//! [`twine::Error::Io`] with kind `InvalidData`, see [`is_malformed`].
#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod amp;
pub mod broadcast;
pub mod lengthprefix;
pub mod line;
pub mod netstring;
pub mod reconnect;

pub use amp::{
    arg_str, args, remote_error, Amp, AmpBox, AmpHandler, AmpParser, AmpReply, Args, RemoteError,
    DEFAULT_MAX_BOX_SIZE, DEFAULT_MAX_PAIRS, MALFORMED_ERROR_CODE,
};
pub use broadcast::Broadcast;
pub use lengthprefix::{
    Endian, FrameFormat, FrameHandler, LengthPrefixParser, LengthPrefixed, DEFAULT_MAX_FRAME,
};
pub use line::{Framed, LineHandler, LineParser, Lines, DEFAULT_MAX_LINE_LENGTH};
pub use netstring::{NetstringHandler, NetstringParser, Netstrings, DEFAULT_MAX_NETSTRING};
pub use reconnect::{
    BuildFn, ReconnectHandle, ReconnectOptions, ReconnectStatus, ReconnectingClient,
};

/// What a handler (or a parser callback) tells the framing layer to do next.
///
/// Returned from `LineHandler::line_received`, `LineHandler::raw_data_received`
/// and from the callbacks passed to the parsers' `feed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    /// Keep parsing in the current mode.
    Continue,
    /// Line parsers only: switch to raw mode. The rest of the current chunk and
    /// all later data is delivered as raw data.
    RawMode,
    /// Raw callbacks only: switch back to line mode and parse the *last* `n`
    /// bytes of the raw chunk just delivered as if they had just been received
    /// (the bytes the raw handler did not consume).
    LineMode(usize),
    /// Stop: discard the rest of the chunk and all later input. The wrapper
    /// protocols then call `Transport::lose_connection`.
    Close,
}

/// Retained capacity of a partial-frame buffer between frames; larger buffers
/// are released when the frame completes.
pub(crate) const RETAIN: usize = 4096;

pub(crate) fn recycle(buf: &mut Vec<u8>) {
    if buf.capacity() > RETAIN {
        *buf = Vec::new();
    } else {
        buf.clear();
    }
}

/// Error for a structurally invalid netstring.
pub(crate) fn malformed_netstring() -> twine::Error {
    twine::Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "twine-proto: malformed netstring",
    ))
}

/// Error for an invalid length prefix.
pub(crate) fn malformed_frame() -> twine::Error {
    twine::Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "twine-proto: malformed length-prefixed frame",
    ))
}

/// True when `e` is one of the "malformed input" errors produced by this crate
/// (an `Error::Io` with kind `InvalidData` and a `twine-proto:` message), as
/// opposed to a size-limit error.
pub fn is_malformed(e: &twine::Error) -> bool {
    match e {
        twine::Error::Io(io) => {
            io.kind() == std::io::ErrorKind::InvalidData
                && io.to_string().starts_with("twine-proto: malformed")
        }
        _ => false,
    }
}
