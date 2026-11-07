//! Linux kernel TLS (kTLS) plumbing: the kernel ABI structs, the capability
//! probes, `setsockopt` wrappers, the close_notify sender, a record boundary
//! tracker and a small user-space TLS 1.3 record decryptor used when only the
//! transmit direction is handed to the kernel.
//!
//! Layouts follow `linux/tls.h`; the byte layout is the one the Go port
//! (`tls/ktls_linux.go`) verified against a 6.x kernel.

use std::io;
use std::ops::Range;
use std::os::fd::RawFd;
use std::sync::OnceLock;

use ring::aead;
use rustls::ConnectionTrafficSecrets;

/// `SOL_TLS` socket level.
const SOL_TLS: libc::c_int = 282;
/// `TCP_ULP` socket option.
const TCP_ULP: libc::c_int = 31;
/// `TLS_TX` option.
const TLS_TX: libc::c_int = 1;
/// `TLS_RX` option.
const TLS_RX: libc::c_int = 2;
/// cmsg type that selects the record content type on `sendmsg`.
const TLS_SET_RECORD_TYPE: libc::c_int = 1;
/// `TLS_1_3_VERSION` (0x0304).
const TLS_1_3_VERSION: u16 = 0x0304;
/// `TLS_CIPHER_AES_GCM_128`.
const TLS_CIPHER_AES_GCM_128: u16 = 51;
/// `TLS_CIPHER_AES_GCM_256`.
const TLS_CIPHER_AES_GCM_256: u16 = 52;
/// `TLS_CIPHER_CHACHA20_POLY1305`.
const TLS_CIPHER_CHACHA20_POLY1305: u16 = 54;
/// TLS record content type: alert.
const CT_ALERT: u8 = 21;
/// TLS record content type: handshake.
const CT_HANDSHAKE: u8 = 22;
/// TLS record content type: application data.
const CT_APPDATA: u8 = 23;
/// TLS record content type: change_cipher_spec (middlebox compatibility).
const CT_CCS: u8 = 20;

/// Largest legal TLS 1.3 ciphertext record body (2^14 + 256).
const MAX_RECORD: usize = 16384 + 256;

/// `struct tls12_crypto_info_aes_gcm_128`.
#[repr(C)]
pub(crate) struct Aes128Info {
    version: u16,
    cipher_type: u16,
    iv: [u8; 8],
    key: [u8; 16],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

/// `struct tls12_crypto_info_aes_gcm_256`.
#[repr(C)]
pub(crate) struct Aes256Info {
    version: u16,
    cipher_type: u16,
    iv: [u8; 8],
    key: [u8; 32],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

/// `struct tls12_crypto_info_chacha20_poly1305` (no salt: the whole 12-byte IV
/// is `iv`).
#[repr(C)]
pub(crate) struct ChachaInfo {
    version: u16,
    cipher_type: u16,
    iv: [u8; 12],
    key: [u8; 32],
    rec_seq: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<Aes128Info>() == 40);
const _: () = assert!(std::mem::size_of::<Aes256Info>() == 56);
const _: () = assert!(std::mem::size_of::<ChachaInfo>() == 56);

/// The record protection algorithms the kernel offers for TLS 1.3.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    /// AES-128-GCM.
    Aes128,
    /// AES-256-GCM.
    Aes256,
    /// ChaCha20-Poly1305.
    Chacha,
}

impl Kind {
    fn index(self) -> usize {
        match self {
            Kind::Aes128 => 0,
            Kind::Aes256 => 1,
            Kind::Chacha => 2,
        }
    }
}

/// A serialized `tls12_crypto_info_*` ready for `setsockopt`.
pub(crate) enum CryptoInfo {
    /// AES-128-GCM.
    A128(Aes128Info),
    /// AES-256-GCM.
    A256(Aes256Info),
    /// ChaCha20-Poly1305.
    Chacha(ChachaInfo),
}

/// Builds the kernel crypto info for one direction. The 12-byte TLS 1.3 static
/// IV splits into `salt` (first 4 bytes) and `iv` (last 8) for AES-GCM; for
/// ChaCha the kernel takes all 12 bytes as `iv`. `rec_seq` is the record
/// sequence number, big-endian.
pub(crate) fn crypto_info(seq: u64, s: &ConnectionTrafficSecrets) -> Option<(Kind, CryptoInfo)> {
    let rec_seq = seq.to_be_bytes();
    match s {
        ConnectionTrafficSecrets::Aes128Gcm { key, iv } => {
            let (key, iv) = (key.as_ref(), iv.as_ref());
            Some((
                Kind::Aes128,
                CryptoInfo::A128(Aes128Info {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_AES_GCM_128,
                    iv: iv.get(4..12)?.try_into().ok()?,
                    key: key.try_into().ok()?,
                    salt: iv.get(..4)?.try_into().ok()?,
                    rec_seq,
                }),
            ))
        }
        ConnectionTrafficSecrets::Aes256Gcm { key, iv } => {
            let (key, iv) = (key.as_ref(), iv.as_ref());
            Some((
                Kind::Aes256,
                CryptoInfo::A256(Aes256Info {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_AES_GCM_256,
                    iv: iv.get(4..12)?.try_into().ok()?,
                    key: key.try_into().ok()?,
                    salt: iv.get(..4)?.try_into().ok()?,
                    rec_seq,
                }),
            ))
        }
        ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => Some((
            Kind::Chacha,
            CryptoInfo::Chacha(ChachaInfo {
                version: TLS_1_3_VERSION,
                cipher_type: TLS_CIPHER_CHACHA20_POLY1305,
                iv: iv.as_ref().try_into().ok()?,
                key: key.as_ref().try_into().ok()?,
                rec_seq,
            }),
        )),
        _ => None,
    }
}

fn last_err() -> io::Error {
    io::Error::last_os_error()
}

/// `setsockopt(fd, level, name, ptr, len)` with error mapping.
fn setsockopt_raw(
    fd: RawFd,
    level: libc::c_int,
    name: libc::c_int,
    ptr: *const libc::c_void,
    len: usize,
) -> io::Result<()> {
    // SAFETY: `ptr` points at `len` readable bytes owned by the caller for the
    // duration of the call; the kernel copies them and keeps no reference.
    let r = unsafe { libc::setsockopt(fd, level, name, ptr, len as libc::socklen_t) };
    if r == 0 {
        Ok(())
    } else {
        Err(last_err())
    }
}

/// Attaches the `tls` upper layer protocol to an established TCP socket.
/// `EEXIST` (already attached) counts as success.
pub(crate) fn attach_ulp(fd: RawFd) -> io::Result<()> {
    let name = b"tls";
    match setsockopt_raw(fd, libc::SOL_TCP, TCP_ULP, name.as_ptr().cast(), name.len()) {
        Err(e) if e.raw_os_error() == Some(libc::EEXIST) => Ok(()),
        r => r,
    }
}

/// Installs the keys for one direction (`tx` selects `TLS_TX`, else `TLS_RX`).
pub(crate) fn set_crypto(fd: RawFd, tx: bool, info: &CryptoInfo) -> io::Result<()> {
    let opt = if tx { TLS_TX } else { TLS_RX };
    // SAFETY: each struct is `#[repr(C)]`, fully initialised, contains only
    // integer/byte-array fields (no padding: sizes are asserted above to match
    // the kernel's), and outlives the call. The pointer/length pair therefore
    // names exactly the bytes of one `tls12_crypto_info_*`.
    match info {
        CryptoInfo::A128(i) => setsockopt_raw(
            fd,
            SOL_TLS,
            opt,
            (i as *const Aes128Info).cast(),
            std::mem::size_of::<Aes128Info>(),
        ),
        CryptoInfo::A256(i) => setsockopt_raw(
            fd,
            SOL_TLS,
            opt,
            (i as *const Aes256Info).cast(),
            std::mem::size_of::<Aes256Info>(),
        ),
        CryptoInfo::Chacha(i) => setsockopt_raw(
            fd,
            SOL_TLS,
            opt,
            (i as *const ChachaInfo).cast(),
            std::mem::size_of::<ChachaInfo>(),
        ),
    }
}

/// An owned fd closed on drop.
struct Fd(RawFd);
impl Drop for Fd {
    fn drop(&mut self) {
        // SAFETY: the fd is owned by this guard and closed exactly once.
        unsafe { libc::close(self.0) };
    }
}

/// Returns one end of a connected loopback TCP pair (both ends owned).
fn loopback_pair() -> io::Result<(Fd, Fd)> {
    // SAFETY: plain socket(2).
    let lfd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if lfd < 0 {
        return Err(last_err());
    }
    let lfd = Fd(lfd);
    // SAFETY: sockaddr_in is plain data; all-zero is valid.
    let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    sa.sin_family = libc::AF_INET as libc::sa_family_t;
    sa.sin_addr.s_addr = u32::from_ne_bytes([127, 0, 0, 1]);
    let sl = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    // SAFETY: `sa`/`sl` describe a valid sockaddr_in that lives across the call.
    if unsafe { libc::bind(lfd.0, (&sa as *const libc::sockaddr_in).cast(), sl) } != 0 {
        return Err(last_err());
    }
    // SAFETY: listening on a bound socket we own.
    if unsafe { libc::listen(lfd.0, 1) } != 0 {
        return Err(last_err());
    }
    let mut len = sl;
    // SAFETY: `sa` has room for `len` bytes; the kernel writes the bound address.
    if unsafe { libc::getsockname(lfd.0, (&mut sa as *mut libc::sockaddr_in).cast(), &mut len) }
        != 0
    {
        return Err(last_err());
    }
    // SAFETY: plain socket(2).
    let cfd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if cfd < 0 {
        return Err(last_err());
    }
    let cfd = Fd(cfd);
    // A blocking connect to a listening loopback socket completes from the
    // backlog without an accept.
    // SAFETY: `sa`/`len` hold the listener's address.
    if unsafe { libc::connect(cfd.0, (&sa as *const libc::sockaddr_in).cast(), len) } != 0 {
        return Err(last_err());
    }
    Ok((cfd, lfd))
}

/// Compile-time: this build can attempt kernel TLS (Linux).
pub(crate) const fn supported() -> bool {
    true
}

/// Probed once and cached: does the kernel accept the `tls` ULP?
pub(crate) fn available() -> bool {
    static AVAIL: OnceLock<bool> = OnceLock::new();
    *AVAIL.get_or_init(|| match loopback_pair() {
        Ok((c, _l)) => attach_ulp(c.0).is_ok(),
        Err(_) => false,
    })
}

/// Probed once per kind and cached: does the kernel accept both `TLS_TX` and
/// `TLS_RX` for `kind` (dummy keys on a throwaway loopback socket)?
pub(crate) fn cipher_usable(kind: Kind) -> bool {
    static OK: [OnceLock<bool>; 3] = [OnceLock::new(), OnceLock::new(), OnceLock::new()];
    *OK[kind.index()].get_or_init(|| {
        if !available() {
            return false;
        }
        let Ok((c, _l)) = loopback_pair() else {
            return false;
        };
        if attach_ulp(c.0).is_err() {
            return false;
        }
        let mk = || match kind {
            Kind::Aes128 => CryptoInfo::A128(Aes128Info {
                version: TLS_1_3_VERSION,
                cipher_type: TLS_CIPHER_AES_GCM_128,
                iv: [0; 8],
                key: [0; 16],
                salt: [0; 4],
                rec_seq: [0; 8],
            }),
            Kind::Aes256 => CryptoInfo::A256(Aes256Info {
                version: TLS_1_3_VERSION,
                cipher_type: TLS_CIPHER_AES_GCM_256,
                iv: [0; 8],
                key: [0; 32],
                salt: [0; 4],
                rec_seq: [0; 8],
            }),
            Kind::Chacha => CryptoInfo::Chacha(ChachaInfo {
                version: TLS_1_3_VERSION,
                cipher_type: TLS_CIPHER_CHACHA20_POLY1305,
                iv: [0; 12],
                key: [0; 32],
                rec_seq: [0; 8],
            }),
        };
        set_crypto(c.0, true, &mk()).is_ok() && set_crypto(c.0, false, &mk()).is_ok()
    })
}

/// Runs the availability and cipher probes now (they are cached), so the first
/// handshake on a loop thread does not pay for them.
pub(crate) fn warm() {
    if available() {
        for k in [Kind::Aes128, Kind::Aes256, Kind::Chacha] {
            cipher_usable(k);
        }
    }
}

/// Sends a close_notify alert as a kernel-encrypted record, using a
/// `TLS_SET_RECORD_TYPE` control message. Never blocks (`EAGAIN` if the send
/// buffer is full). TX keys must already be installed.
pub(crate) fn send_close_notify(fd: RawFd) -> io::Result<()> {
    let mut data = [1u8, 0u8]; // warning, close_notify
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr().cast(),
        iov_len: data.len(),
    };
    // SAFETY: CMSG_SPACE is a pure size computation.
    let space = unsafe { libc::CMSG_SPACE(1) } as usize;
    // u64 elements give the 8-byte alignment cmsghdr needs.
    let mut cbuf = vec![0u64; space.div_ceil(8)];
    // SAFETY: msghdr is plain data; all-zero is valid before the fields are set.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cbuf.as_mut_ptr().cast();
    msg.msg_controllen = space as _;
    // SAFETY: `msg_control` points at `space` zeroed, aligned bytes, enough for
    // one cmsghdr plus one data byte, so CMSG_FIRSTHDR is non-null and
    // CMSG_DATA stays inside the buffer.
    unsafe {
        let h = libc::CMSG_FIRSTHDR(&msg);
        (*h).cmsg_level = SOL_TLS;
        (*h).cmsg_type = TLS_SET_RECORD_TYPE;
        (*h).cmsg_len = libc::CMSG_LEN(1) as _;
        *libc::CMSG_DATA(h) = CT_ALERT;
    }
    // SAFETY: `msg` and everything it points at (iov, data, cbuf) are alive and
    // valid for the call.
    let r = unsafe { libc::sendmsg(fd, &msg, libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) };
    if r < 0 {
        Err(last_err())
    } else {
        Ok(())
    }
}

/// Follows TLS record framing over the bytes fed to rustls so the code knows,
/// at handshake completion, whether rustls holds a partial record and what it
/// is (rustls has no API for its deframer's buffered length).
#[derive(Default)]
pub(crate) struct RecordTracker {
    /// Bytes of the current, incomplete record (header included).
    partial: Vec<u8>,
    /// Body bytes of the current record still to come once the header is in.
    remaining: usize,
    /// Set when a record header was out of range; offload is then refused.
    bad: bool,
}

impl RecordTracker {
    /// Feeds bytes that were just handed to rustls.
    pub(crate) fn feed(&mut self, mut data: &[u8]) {
        while !data.is_empty() && !self.bad {
            if self.partial.len() < 5 {
                let need = 5 - self.partial.len();
                let n = need.min(data.len());
                self.partial.extend_from_slice(&data[..n]);
                data = &data[n..];
                if self.partial.len() == 5 {
                    let len = u16::from_be_bytes([self.partial[3], self.partial[4]]) as usize;
                    if len > MAX_RECORD {
                        self.bad = true;
                        return;
                    }
                    self.remaining = len;
                    if len == 0 {
                        self.partial.clear();
                    }
                }
            } else {
                let n = self.remaining.min(data.len());
                self.partial.extend_from_slice(&data[..n]);
                data = &data[n..];
                self.remaining -= n;
                if self.remaining == 0 {
                    self.partial.clear();
                }
            }
        }
    }

    /// The incomplete record bytes consumed so far, or `None` if the stream
    /// framing was invalid.
    pub(crate) fn partial(&self) -> Option<&[u8]> {
        (!self.bad).then_some(self.partial.as_slice())
    }
}

/// One decrypted record from [`UserRx`].
pub(crate) enum Rec {
    /// Application data at this range of [`UserRx::slice`] (never empty).
    Data(Range<usize>),
    /// The peer's close_notify. [`UserRx`] ignores all later input.
    CloseNotify,
    /// A record to skip (post-handshake handshake messages such as
    /// NewSessionTicket, middlebox CCS, empty application data, user_canceled).
    Ignore,
}

/// The only change_cipher_spec record TLS 1.3 allows (RFC 8446 Appendix D.4).
const CCS_RECORD: [u8; 6] = [CT_CCS, 3, 3, 0, 1, 1];

/// Handshake message type: KeyUpdate.
const HS_KEY_UPDATE: u8 = 24;

/// Alert description: close_notify.
const ALERT_CLOSE_NOTIFY: u8 = 0;
/// Alert description: user_canceled.
const ALERT_USER_CANCELED: u8 = 90;

/// Name of a TLS alert description, for error messages.
fn alert_name(d: u8) -> &'static str {
    match d {
        0 => "close_notify",
        10 => "unexpected_message",
        20 => "bad_record_mac",
        22 => "record_overflow",
        40 => "handshake_failure",
        42 => "bad_certificate",
        43 => "unsupported_certificate",
        44 => "certificate_revoked",
        45 => "certificate_expired",
        46 => "certificate_unknown",
        47 => "illegal_parameter",
        48 => "unknown_ca",
        49 => "access_denied",
        50 => "decode_error",
        51 => "decrypt_error",
        70 => "protocol_version",
        71 => "insufficient_security",
        80 => "internal_error",
        86 => "inappropriate_fallback",
        90 => "user_canceled",
        109 => "missing_extension",
        110 => "unsupported_extension",
        112 => "unrecognized_name",
        113 => "bad_certificate_status_response",
        115 => "unknown_psk_identity",
        116 => "certificate_required",
        120 => "no_application_protocol",
        _ => "unknown",
    }
}

/// A minimal TLS 1.3 record decryptor for the receive direction, used when the
/// kernel owns only the transmit direction (the default, a client, or a server
/// whose peer had already sent data when the handshake finished).
pub(crate) struct UserRx {
    key: aead::LessSafeKey,
    iv: [u8; 12],
    seq: u64,
    buf: Vec<u8>,
    start: usize,
    /// Handshake-message header bytes gathered so far (a header may straddle
    /// records).
    hs_hdr: [u8; 4],
    hs_hdr_len: usize,
    /// Body bytes of the current handshake message still to skip; carried
    /// across records.
    hs_remain: usize,
    /// A compatibility change_cipher_spec is still acceptable: only before the
    /// first protected record (the handshake is still being finished).
    ccs_ok: bool,
    /// The stream ended (close_notify) or failed: all further input is ignored.
    done: bool,
}

impl UserRx {
    /// Builds a decryptor from the extracted receive secrets.
    pub(crate) fn new(seq: u64, s: &ConnectionTrafficSecrets) -> Option<UserRx> {
        let (alg, key, iv) = match s {
            ConnectionTrafficSecrets::Aes128Gcm { key, iv } => {
                (&aead::AES_128_GCM, key.as_ref(), iv)
            }
            ConnectionTrafficSecrets::Aes256Gcm { key, iv } => {
                (&aead::AES_256_GCM, key.as_ref(), iv)
            }
            ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => {
                (&aead::CHACHA20_POLY1305, key.as_ref(), iv)
            }
            _ => return None,
        };
        Some(UserRx {
            key: aead::LessSafeKey::new(aead::UnboundKey::new(alg, key).ok()?),
            iv: iv.as_ref().try_into().ok()?,
            seq,
            buf: Vec::new(),
            start: 0,
            hs_hdr: [0; 4],
            hs_hdr_len: 0,
            hs_remain: 0,
            ccs_ok: true,
            done: false,
        })
    }

    /// Appends ciphertext (dropped once the stream is done).
    pub(crate) fn push(&mut self, data: &[u8]) {
        if self.done {
            return;
        }
        if self.start == self.buf.len() {
            self.buf.clear();
            self.start = 0;
        } else if self.start > 64 * 1024 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        self.buf.extend_from_slice(data);
    }

    /// The bytes of a [`Rec::Data`] range.
    pub(crate) fn slice(&mut self, r: Range<usize>) -> &mut [u8] {
        &mut self.buf[r]
    }

    /// Decrypts the next complete record, `Ok(None)` if more input is needed
    /// (or the stream is done). An `Err` is fatal (bad MAC, framing, a peer
    /// alert, or an unsupported KeyUpdate) and also ends the stream.
    pub(crate) fn next(&mut self) -> Result<Option<Rec>, String> {
        if self.done {
            return Ok(None);
        }
        let r = self.next_record();
        if r.is_err() {
            self.done = true;
        }
        r
    }

    fn next_record(&mut self) -> Result<Option<Rec>, String> {
        let avail = &self.buf[self.start..];
        if avail.len() < 5 {
            return Ok(None);
        }
        let outer = avail[0];
        let len = u16::from_be_bytes([avail[3], avail[4]]) as usize;
        if len > MAX_RECORD {
            return Err("tls: record too large".into());
        }
        if avail.len() < 5 + len {
            return Ok(None);
        }
        let hdr = [avail[0], avail[1], avail[2], avail[3], avail[4]];
        let ccs_exact = len == 1 && avail[..6] == CCS_RECORD;
        let body = self.start + 5..self.start + 5 + len;
        self.start += 5 + len;
        if outer == CT_CCS {
            if !ccs_exact {
                return Err("tls: malformed change_cipher_spec".into());
            }
            if !self.ccs_ok {
                return Err("tls: unexpected change_cipher_spec (unexpected_message)".into());
            }
            self.ccs_ok = false;
            return Ok(Some(Rec::Ignore));
        }
        self.ccs_ok = false;
        if outer != CT_APPDATA {
            return Err("tls: unexpected record type".into());
        }
        let mut nonce = self.iv;
        for (n, s) in nonce[4..].iter_mut().zip(self.seq.to_be_bytes()) {
            *n ^= s;
        }
        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| "tls: record sequence number exhausted".to_string())?;
        let start = body.start;
        let plain_len = self
            .key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(hdr),
                &mut self.buf[body],
            )
            .map_err(|_| "tls: bad record MAC".to_string())?
            .len();
        let plain = &self.buf[start..start + plain_len];
        let Some(ct_at) = plain.iter().rposition(|&b| b != 0) else {
            return Err("tls: empty inner plaintext".into());
        };
        let ct = plain[ct_at];
        let body = start..start + ct_at;
        let mid_handshake = self.hs_hdr_len != 0 || self.hs_remain != 0;
        if ct != CT_HANDSHAKE && mid_handshake {
            return Err("tls: handshake message interleaved with another record type".into());
        }
        match ct {
            CT_APPDATA if body.is_empty() => Ok(Some(Rec::Ignore)),
            CT_APPDATA => Ok(Some(Rec::Data(body))),
            CT_ALERT => {
                let a = &self.buf[body];
                let [level, desc] = *a else {
                    return Err("tls: malformed alert".into());
                };
                match (level, desc) {
                    (1, ALERT_CLOSE_NOTIFY) => {
                        self.done = true;
                        Ok(Some(Rec::CloseNotify))
                    }
                    (_, ALERT_USER_CANCELED) => Ok(Some(Rec::Ignore)),
                    _ => Err(format!(
                        "tls: peer alert {} (description {desc}, level {level})",
                        alert_name(desc)
                    )),
                }
            }
            CT_HANDSHAKE => {
                // Messages may be fragmented across records: carry the header
                // bytes and the remaining body length, and look at a message
                // type only at a message boundary. KeyUpdate (24) cannot be
                // honoured because the new receive key is not derived here.
                let Self {
                    buf,
                    hs_hdr,
                    hs_hdr_len,
                    hs_remain,
                    ..
                } = self;
                let mut m = &buf[body];
                while !m.is_empty() {
                    if *hs_remain > 0 {
                        let n = (*hs_remain).min(m.len());
                        *hs_remain -= n;
                        m = &m[n..];
                        continue;
                    }
                    if *hs_hdr_len == 0 && m[0] == HS_KEY_UPDATE {
                        return Err("tls: KeyUpdate not supported under kTLS".into());
                    }
                    let n = (4 - *hs_hdr_len).min(m.len());
                    hs_hdr[*hs_hdr_len..*hs_hdr_len + n].copy_from_slice(&m[..n]);
                    *hs_hdr_len += n;
                    m = &m[n..];
                    if *hs_hdr_len == 4 {
                        *hs_hdr_len = 0;
                        *hs_remain = (hs_hdr[1] as usize) << 16
                            | (hs_hdr[2] as usize) << 8
                            | hs_hdr[3] as usize;
                    }
                }
                Ok(Some(Rec::Ignore))
            }
            _ => Err("tls: unexpected inner record type".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::crypto::cipher::{AeadKey, Iv};

    const KEY: [u8; 32] = [7; 32];
    const IV: [u8; 12] = [9; 12];

    fn secrets(chacha: bool) -> ConnectionTrafficSecrets {
        let (key, iv) = (AeadKey::from(KEY), Iv::new(IV));
        if chacha {
            ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv }
        } else {
            ConnectionTrafficSecrets::Aes256Gcm { key, iv }
        }
    }

    /// The sending side: encrypts TLS 1.3 records with the same key/iv/seq.
    struct Enc {
        key: aead::LessSafeKey,
        seq: u64,
    }

    impl Enc {
        fn new(chacha: bool, seq: u64) -> Enc {
            let alg = if chacha {
                &aead::CHACHA20_POLY1305
            } else {
                &aead::AES_256_GCM
            };
            Enc {
                key: aead::LessSafeKey::new(aead::UnboundKey::new(alg, &KEY).unwrap()),
                seq,
            }
        }

        /// One protected record carrying `content` of inner type `ct` plus
        /// `pad` zero bytes of padding.
        fn seal(&mut self, ct: u8, content: &[u8], pad: usize) -> Vec<u8> {
            let mut plain = content.to_vec();
            plain.push(ct);
            plain.extend(std::iter::repeat_n(0, pad));
            let len = plain.len() + 16;
            let hdr = [CT_APPDATA, 3, 3, (len >> 8) as u8, len as u8];
            let mut nonce = IV;
            for (n, s) in nonce[4..].iter_mut().zip(self.seq.to_be_bytes()) {
                *n ^= s;
            }
            self.seq = self.seq.wrapping_add(1);
            self.key
                .seal_in_place_append_tag(
                    aead::Nonce::assume_unique_for_key(nonce),
                    aead::Aad::from(hdr),
                    &mut plain,
                )
                .unwrap();
            let mut out = hdr.to_vec();
            out.extend_from_slice(&plain);
            out
        }

        fn data(&mut self, d: &[u8]) -> Vec<u8> {
            self.seal(CT_APPDATA, d, 0)
        }
    }

    #[derive(Debug, PartialEq)]
    enum Out {
        Data(Vec<u8>),
        Close,
        Ignore,
    }

    /// Feeds `chunks` and drains after each; stops at the first error.
    fn run(rx: &mut UserRx, chunks: &[&[u8]]) -> Result<Vec<Out>, String> {
        let mut out = Vec::new();
        for c in chunks {
            rx.push(c);
            while let Some(r) = rx.next()? {
                out.push(match r {
                    Rec::Data(r) => Out::Data(rx.slice(r).to_vec()),
                    Rec::CloseNotify => Out::Close,
                    Rec::Ignore => Out::Ignore,
                });
            }
        }
        Ok(out)
    }

    fn data_of(o: &[Out]) -> Vec<u8> {
        o.iter()
            .filter_map(|o| match o {
                Out::Data(d) => Some(d.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    /// Runs `wire` whole, byte by byte and split at every offset.
    fn all_splits(chacha: bool, wire: &[u8]) -> Vec<Result<Vec<Out>, String>> {
        let mk = || UserRx::new(0, &secrets(chacha)).unwrap();
        let mut res = vec![run(&mut mk(), &[wire])];
        let bytes: Vec<&[u8]> = wire.chunks(1).collect();
        res.push(run(&mut mk(), &bytes));
        for i in 0..=wire.len() {
            res.push(run(&mut mk(), &[&wire[..i], &wire[i..]]));
        }
        res
    }

    fn merged(res: Vec<Result<Vec<Out>, String>>) -> Vec<Out> {
        // Split points must not change the decoded content.
        let want = data_of(res[0].as_ref().unwrap());
        for r in &res {
            assert_eq!(data_of(r.as_ref().unwrap()), want);
        }
        res.into_iter().next().unwrap().unwrap()
    }

    #[test]
    fn records_whole_bytewise_and_every_split() {
        for chacha in [false, true] {
            let mut e = Enc::new(chacha, 0);
            let mut wire = e.data(b"hello");
            wire.extend(e.seal(CT_APPDATA, b"padded", 17));
            wire.extend(e.data(&vec![0xab; 16384]));
            wire.extend(e.data(b"x"));
            let o = merged(all_splits(chacha, &wire));
            let mut want = b"hello".to_vec();
            want.extend_from_slice(b"padded");
            want.extend(vec![0xab; 16384]);
            want.push(b'x');
            assert_eq!(data_of(&o), want);
            assert_eq!(o.len(), 4);
        }
    }

    #[test]
    fn nonzero_start_sequence() {
        let mut e = Enc::new(false, 41);
        let wire = e.data(b"later");
        let mut rx = UserRx::new(41, &secrets(false)).unwrap();
        assert_eq!(
            run(&mut rx, &[&wire]).unwrap(),
            [Out::Data(b"later".into())]
        );
        let mut rx = UserRx::new(40, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&wire]).is_err());
    }

    #[test]
    fn multi_record_chunk_in_order() {
        let mut e = Enc::new(true, 0);
        let mut wire = Vec::new();
        for i in 0..50u8 {
            wire.extend(e.data(&[i; 3]));
        }
        let mut rx = UserRx::new(0, &secrets(true)).unwrap();
        let o = run(&mut rx, &[&wire]).unwrap();
        assert_eq!(o.len(), 50);
        for (i, o) in o.iter().enumerate() {
            assert_eq!(*o, Out::Data(vec![i as u8; 3]));
        }
    }

    #[test]
    fn zero_length_record_is_ignored() {
        let mut e = Enc::new(false, 0);
        let mut wire = e.seal(CT_APPDATA, b"", 0);
        wire.extend(e.seal(CT_APPDATA, b"", 9));
        wire.extend(e.data(b"after"));
        for r in all_splits(false, &wire) {
            let o = r.unwrap();
            assert_eq!(o.len(), 3);
            assert_eq!(o[0], Out::Ignore);
            assert_eq!(o[1], Out::Ignore);
            assert_eq!(o[2], Out::Data(b"after".into()));
        }
    }

    #[test]
    fn all_zero_inner_plaintext_is_fatal() {
        let mut e = Enc::new(false, 0);
        // content type byte 0 and nothing but padding.
        let wire = e.seal(0, b"", 3);
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&wire]).is_err());
    }

    #[test]
    fn tampering_is_an_error() {
        for chacha in [false, true] {
            let mut e = Enc::new(chacha, 0);
            let wire = e.data(b"secret data");
            for i in 5..wire.len() {
                let mut w = wire.clone();
                w[i] ^= 0x01;
                let mut rx = UserRx::new(0, &secrets(chacha)).unwrap();
                let err = run(&mut rx, &[&w]).unwrap_err();
                assert!(err.contains("MAC"), "{err}");
                // The failure is latched: nothing more is ever produced.
                let mut e2 = Enc::new(chacha, 1);
                rx.push(&e2.data(b"more"));
                assert!(rx.next().unwrap().is_none());
            }
            // Header tampering (AAD) fails too.
            let mut w = wire.clone();
            w[1] = 0x04;
            let mut rx = UserRx::new(0, &secrets(chacha)).unwrap();
            assert!(run(&mut rx, &[&w]).is_err());
            // A replayed record (wrong sequence number).
            let mut rx = UserRx::new(0, &secrets(chacha)).unwrap();
            assert!(run(&mut rx, &[&wire, &wire]).is_err());
        }
    }

    #[test]
    fn record_too_large_and_bad_type() {
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&[CT_APPDATA, 3, 3, 0xff, 0xff]]).is_err());
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&[CT_HANDSHAKE, 3, 3, 0, 1, 0]]).is_err());
    }

    #[test]
    fn close_notify_latches() {
        let mut e = Enc::new(false, 0);
        let mut wire = e.data(b"before");
        wire.extend(e.seal(CT_ALERT, &[1, ALERT_CLOSE_NOTIFY], 0));
        wire.extend(e.data(b"after"));
        wire.extend_from_slice(b"\x00garbage that is not a record");
        for r in all_splits(false, &wire) {
            assert_eq!(
                r.unwrap(),
                [Out::Data(b"before".into()), Out::Close],
                "nothing after close_notify"
            );
        }
        // Even pushing more later yields nothing.
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        run(&mut rx, &[&wire]).unwrap();
        rx.push(&e.data(b"x"));
        assert!(rx.next().unwrap().is_none());
    }

    #[test]
    fn alerts_user_canceled_ignored_others_fatal() {
        let mut e = Enc::new(false, 0);
        let mut wire = e.seal(CT_ALERT, &[1, ALERT_USER_CANCELED], 0);
        wire.extend(e.data(b"still here"));
        for r in all_splits(false, &wire) {
            assert_eq!(r.unwrap(), [Out::Ignore, Out::Data(b"still here".into())]);
        }
        for (level, desc, name) in [
            (2u8, 40u8, "handshake_failure"),
            (1, 40, "handshake_failure"),
            (2, 0, "close_notify"),
            (2, 80, "internal_error"),
            (1, 200, "unknown"),
        ] {
            let mut e = Enc::new(false, 0);
            let w = e.seal(CT_ALERT, &[level, desc], 0);
            let mut rx = UserRx::new(0, &secrets(false)).unwrap();
            let err = run(&mut rx, &[&w]).unwrap_err();
            assert!(err.contains(name), "{err}");
        }
        // Wrong alert length.
        let mut e = Enc::new(false, 0);
        let w = e.seal(CT_ALERT, &[1], 0);
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&w]).is_err());
    }

    fn hs_msg(typ: u8, body_len: usize) -> Vec<u8> {
        let mut m = vec![
            typ,
            (body_len >> 16) as u8,
            (body_len >> 8) as u8,
            body_len as u8,
        ];
        // A body full of 24s: a scan that looked inside bodies would see KeyUpdate.
        m.extend(std::iter::repeat_n(HS_KEY_UPDATE, body_len));
        m
    }

    #[test]
    fn fragmented_new_session_ticket_across_records() {
        let mut stream = hs_msg(4, 300);
        stream.extend(hs_msg(4, 0));
        stream.extend(hs_msg(4, 5));
        // Split the message stream into two records at every offset, and into
        // three records at a spread of offset pairs.
        for i in 0..=stream.len() {
            let mut e = Enc::new(false, 0);
            let mut wire = e.seal(CT_HANDSHAKE, &stream[..i], 0);
            wire.extend(e.seal(CT_HANDSHAKE, &stream[i..], 2));
            wire.extend(e.data(b"app"));
            let mut rx = UserRx::new(0, &secrets(false)).unwrap();
            let o = run(&mut rx, &[&wire]).unwrap();
            assert_eq!(data_of(&o), b"app", "split {i}");
        }
        for i in (0..stream.len()).step_by(7) {
            for j in (i..stream.len()).step_by(11) {
                let mut e = Enc::new(true, 0);
                let mut wire = e.seal(CT_HANDSHAKE, &stream[..i], 0);
                wire.extend(e.seal(CT_HANDSHAKE, &stream[i..j], 0));
                wire.extend(e.seal(CT_HANDSHAKE, &stream[j..], 0));
                wire.extend(e.data(b"app"));
                for r in all_splits_chunky(&wire) {
                    assert_eq!(data_of(&r.unwrap()), b"app", "split {i},{j}");
                }
            }
        }
    }

    /// A cheaper split set: whole and byte-by-byte (ChaCha).
    fn all_splits_chunky(wire: &[u8]) -> Vec<Result<Vec<Out>, String>> {
        let mk = || UserRx::new(0, &secrets(true)).unwrap();
        let bytes: Vec<&[u8]> = wire.chunks(1).collect();
        vec![run(&mut mk(), &[wire]), run(&mut mk(), &bytes)]
    }

    #[test]
    fn key_update_is_fatal_even_when_fragmented() {
        let ku = hs_msg(HS_KEY_UPDATE, 1);
        for i in 0..=ku.len() {
            let mut e = Enc::new(false, 0);
            let mut wire = e.seal(CT_HANDSHAKE, &hs_msg(4, 10), 0);
            wire.extend(e.seal(CT_HANDSHAKE, &ku[..i], 0));
            wire.extend(e.seal(CT_HANDSHAKE, &ku[i..], 0));
            wire.extend(e.data(b"never"));
            let mut rx = UserRx::new(0, &secrets(false)).unwrap();
            let err = run(&mut rx, &[&wire]).unwrap_err();
            assert!(err.contains("KeyUpdate"), "split {i}: {err}");
        }
        // Behind a NewSessionTicket in the same record.
        let mut m = hs_msg(4, 3);
        m.extend(&ku);
        let mut e = Enc::new(false, 0);
        let w = e.seal(CT_HANDSHAKE, &m, 0);
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&w]).is_err());
    }

    #[test]
    fn data_inside_a_fragmented_handshake_message_is_fatal() {
        let m = hs_msg(4, 40);
        let mut e = Enc::new(false, 0);
        let mut wire = e.seal(CT_HANDSHAKE, &m[..10], 0);
        wire.extend(e.data(b"interleaved"));
        let mut rx = UserRx::new(0, &secrets(false)).unwrap();
        assert!(run(&mut rx, &[&wire]).is_err());
    }

    #[test]
    fn change_cipher_spec_rules() {
        let mut e = Enc::new(false, 0);
        let data = e.data(b"d");
        // Exactly one well-formed CCS before the first protected record.
        for r in all_splits(false, &[&CCS_RECORD[..], &data].concat()) {
            assert_eq!(r.unwrap(), [Out::Ignore, Out::Data(b"d".into())]);
        }
        let run1 = |wire: &[u8]| run(&mut UserRx::new(0, &secrets(false)).unwrap(), &[wire]);
        // A second CCS is rejected.
        assert!(run1(&[&CCS_RECORD[..], &CCS_RECORD[..]].concat()).is_err());
        // Malformed ones are rejected: wrong body, version, length.
        assert!(run1(&[CT_CCS, 3, 3, 0, 1, 2]).is_err());
        assert!(run1(&[CT_CCS, 3, 1, 0, 1, 1]).is_err());
        assert!(run1(&[CT_CCS, 3, 3, 0, 2, 1, 1]).is_err());
        assert!(run1(&[CT_CCS, 3, 3, 0, 0]).is_err());
        // After a protected record a CCS is unexpected.
        let err = run1(&[&data[..], &CCS_RECORD[..]].concat()).unwrap_err();
        assert!(err.contains("unexpected"), "{err}");
    }

    #[test]
    fn sequence_overflow_is_fatal() {
        let mut e = Enc::new(false, u64::MAX);
        let wire = e.data(b"last");
        let mut rx = UserRx::new(u64::MAX, &secrets(false)).unwrap();
        let err = run(&mut rx, &[&wire]).unwrap_err();
        assert!(err.contains("sequence"), "{err}");
    }
}
