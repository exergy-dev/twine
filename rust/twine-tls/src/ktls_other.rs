//! Non-Linux stand-in: kTLS is never available, so every connection stays on
//! rustls in user space.

use std::io;
use std::ops::Range;
use std::os::fd::RawFd;

use rustls::ConnectionTrafficSecrets;

/// Kernel record protection kinds (never usable here).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    /// AES-128-GCM.
    Aes128,
    /// AES-256-GCM.
    Aes256,
    /// ChaCha20-Poly1305.
    Chacha,
}

/// Placeholder for the kernel crypto info.
pub(crate) struct CryptoInfo;

pub(crate) fn crypto_info(_: u64, _: &ConnectionTrafficSecrets) -> Option<(Kind, CryptoInfo)> {
    None
}
pub(crate) fn attach_ulp(_: RawFd) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}
pub(crate) fn set_crypto(_: RawFd, _: bool, _: &CryptoInfo) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}
pub(crate) const fn supported() -> bool {
    false
}
pub(crate) fn available() -> bool {
    false
}
pub(crate) fn cipher_usable(_: Kind) -> bool {
    false
}
pub(crate) fn warm() {}
pub(crate) fn send_close_notify(_: RawFd) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// Record boundary tracker (unused off Linux).
#[derive(Default)]
pub(crate) struct RecordTracker;
impl RecordTracker {
    pub(crate) fn feed(&mut self, _: &[u8]) {}
    pub(crate) fn partial(&self) -> Option<&[u8]> {
        None
    }
}

/// Decrypted record (unused off Linux).
pub(crate) enum Rec {
    /// Application data.
    Data(Range<usize>),
    /// close_notify.
    CloseNotify,
    /// Skip.
    Ignore,
}

/// User-space receive decryptor (unused off Linux).
pub(crate) struct UserRx;
impl UserRx {
    pub(crate) fn new(_: u64, _: &ConnectionTrafficSecrets) -> Option<UserRx> {
        None
    }
    pub(crate) fn push(&mut self, _: &[u8]) {}
    pub(crate) fn slice(&mut self, _: Range<usize>) -> &mut [u8] {
        &mut []
    }
    pub(crate) fn next(&mut self) -> Result<Option<Rec>, String> {
        Ok(None)
    }
}
