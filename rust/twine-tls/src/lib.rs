//! twine-tls: TLS for twine on top of rustls (design Sections 10 and 14).
//!
//! rustls is sans-IO, so [`TlsProtocol`] runs it directly on the reactor loop:
//! ciphertext from `data_received` goes into `read_tls` / `process_new_packets`,
//! decrypted plaintext is delivered to the wrapped inner protocol through a
//! filtered [`Transport`], and the inner protocol's writes pass
//! through a [`WriteFilter`] that encrypts into the raw
//! socket. There is no handshake thread.
//!
//! * Server side: [`server`] wraps a factory, [`listen`] is the one-call form.
//! * Client side: [`client`] / [`client_factory`] build the wrapper; [`connect`]
//!   dials with it.
//! * kTLS (Linux): with [`TlsOptions::kernel_offload`] and a config built with
//!   `enable_secret_extraction` (see [`enable_ktls`] / [`enable_ktls_client`]),
//!   a finished TLS 1.3 AES-GCM / ChaCha20-Poly1305 handshake hands record
//!   protection to the kernel; every failure falls back silently to rustls in
//!   user space. See [`TlsOptions::kernel_offload`] for the exact rules.
//!
//! Reserved timer tokens: `u64::MAX` (handshake deadline, honoured only while
//! the handshake runs) and `u64::MAX - 1` (deferred kTLS close_notify retry,
//! honoured only while that retry is armed). Outside those windows the tokens
//! are forwarded to the inner protocol like any other, but inner protocols
//! should avoid both values.
//!
//! Crypto provider: `ring` (rustls feature), selected through the crate's own
//! `rustls` dependency.
#![deny(missing_docs)]
#![warn(clippy::all)]

#[cfg(target_os = "linux")]
mod ktls;
#[cfg(not(target_os = "linux"))]
#[path = "ktls_other.rs"]
mod ktls;

use std::any::Any;
use std::cell::Cell;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, Connection, ServerConfig, ServerConnection};
use twine::{
    ConnId, DialOptions, Error, Factory, ListenOptions, Listener, Loop, Protocol, TimerId,
    Transport, WriteFilter,
};

/// Default handshake timeout (10 seconds).
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Message carried by the `io::ErrorKind::TimedOut` error reported when the
/// handshake does not finish in time.
pub const ERR_HANDSHAKE_TIMEOUT: &str = "tls: handshake timeout";

/// The connection-scoped timer token reserved for the handshake deadline
/// (`u64::MAX`). Honoured only while the handshake is running.
const HANDSHAKE_TOKEN: u64 = u64::MAX;

/// Reserved token for retrying a kernel-offloaded close_notify (`u64::MAX - 1`).
/// Honoured only while that retry is armed.
const CLOSE_TOKEN: u64 = u64::MAX - 1;

/// Retry interval and attempt cap for a deferred kTLS close_notify.
const CLOSE_RETRY: Duration = Duration::from_millis(2);
const CLOSE_RETRIES: u32 = 5000;

/// Reports whether this build can attempt kernel TLS at all (compile time:
/// true on Linux). Whether a given connection is offloaded also depends on the
/// running kernel ([`ktls_available`]), the negotiated version and cipher, and
/// the connection state; [`TlsProtocol::kernel_offloaded`] reports the outcome.
pub fn ktls_supported() -> bool {
    ktls::supported()
}

/// Reports whether the running kernel accepts the `tls` upper layer protocol
/// (`setsockopt(SOL_TCP, TCP_ULP, "tls")`), probed once on a throwaway loopback
/// socket pair and cached. Always `false` off Linux.
pub fn ktls_available() -> bool {
    ktls::available()
}

/// Sets `enable_secret_extraction` on a server config, which kTLS offload
/// requires (rustls refuses to hand out traffic secrets otherwise).
pub fn enable_ktls(cfg: &mut ServerConfig) {
    cfg.enable_secret_extraction = true;
}

/// Sets `enable_secret_extraction` on a client config (see [`enable_ktls`]).
pub fn enable_ktls_client(cfg: &mut ClientConfig) {
    cfg.enable_secret_extraction = true;
}

/// Size the plaintext scratch buffer grows to on first use.
const PLAIN_BUF: usize = 16 * 1024;

type HandshakeErrorFn = Arc<dyn Fn(&Error) + Send + Sync>;

/// Returns true if `e` is the error produced by a handshake timeout.
pub fn is_handshake_timeout(e: &Error) -> bool {
    match e {
        Error::Io(io) => {
            io.kind() == io::ErrorKind::TimedOut
                && io
                    .get_ref()
                    .is_some_and(|r| r.to_string() == ERR_HANDSHAKE_TIMEOUT)
        }
        _ => false,
    }
}

fn handshake_timeout_error() -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        ERR_HANDSHAKE_TIMEOUT,
    ))
}

/// Options for the TLS wrapper. Builder style.
#[derive(Clone)]
pub struct TlsOptions {
    handshake_timeout: Duration,
    kernel_offload: bool,
    kernel_rx: bool,
    on_handshake_error: Option<HandshakeErrorFn>,
}

impl Default for TlsOptions {
    fn default() -> Self {
        TlsOptions {
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            kernel_offload: false,
            kernel_rx: false,
            on_handshake_error: None,
        }
    }
}

impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsOptions")
            .field("handshake_timeout", &self.handshake_timeout)
            .field("kernel_offload", &self.kernel_offload)
            .field("kernel_rx", &self.kernel_rx)
            .field("on_handshake_error", &self.on_handshake_error.is_some())
            .finish()
    }
}

impl TlsOptions {
    /// Time allowed for the handshake to complete before the connection is
    /// aborted (default [`DEFAULT_HANDSHAKE_TIMEOUT`]). Zero is treated as the default.
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = if d.is_zero() {
            DEFAULT_HANDSHAKE_TIMEOUT
        } else {
            d
        };
        self
    }

    /// Requests Linux kernel TLS (kTLS) offload once the handshake completes.
    ///
    /// The config must have been built with `enable_secret_extraction = true`
    /// (see [`enable_ktls`] / [`enable_ktls_client`]); without it the request is
    /// ignored. Offload happens only for TLS 1.3 with AES-128-GCM, AES-256-GCM or
    /// ChaCha20-Poly1305, when the kernel's `tls` module accepts the keys, and
    /// when rustls has nothing buffered to lose; TLS 1.2 is never offloaded. On
    /// any failure the connection silently stays on rustls in user space.
    ///
    /// Once offloaded the kernel encrypts everything the inner protocol writes
    /// and inbound records are decrypted in user space by a small built-in
    /// record decryptor, which keeps clean close semantics (a peer close_notify
    /// is answered with ours; see [`kernel_rx`](Self::kernel_rx) for the
    /// alternative). A KeyUpdate from the peer is fatal under kTLS, as is any
    /// alert other than close_notify and user_canceled. Session tickets received
    /// after an offloaded handshake are discarded.
    pub fn kernel_offload(mut self, on: bool) -> Self {
        self.kernel_offload = on;
        self
    }

    /// With [`kernel_offload`](Self::kernel_offload), also lets the kernel
    /// decrypt inbound records (default `false`: transmit-only offload).
    ///
    /// Kernel receive applies only to a server whose peer had sent nothing past
    /// the handshake; the inner protocol then sees plain `read` data. The cost
    /// is close semantics: the kernel surfaces a peer alert (including its
    /// close_notify) or KeyUpdate as `EIO`, so the connection is lost with an
    /// I/O error (`connection_lost(Some(err))`) and no close_notify is sent in
    /// reply. A client never uses kernel receive.
    pub fn kernel_rx(mut self, on: bool) -> Self {
        self.kernel_rx = on;
        self
    }

    /// Called at most once per connection when the handshake fails (protocol
    /// error, timeout, or the connection closing before it completed). It runs on
    /// the loop thread, so it must not block.
    pub fn on_handshake_error(mut self, f: impl Fn(&Error) + Send + Sync + 'static) -> Self {
        self.on_handshake_error = Some(Arc::new(f));
        self
    }
}

/// `io::Write` over the raw transport: every `write` is accepted in full (the
/// transport buffers what the socket cannot take).
struct RawWriter<'a, 'b>(&'a mut Transport<'b>);

impl Write for RawWriter<'_, '_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The outbound half: owns the rustls connection and encrypts what the inner
/// protocol writes. Installed on the transport with `Transport::with_filter`.
struct Filter {
    /// `None` once the secrets were extracted for kTLS (rustls is consumed).
    conn: Option<Connection>,
    /// True once the kernel encrypts our writes: `write` is a pass-through.
    tx_off: bool,
    /// A deferred kernel close_notify is waiting to be sent.
    close_wait: u32,
    /// The close_notify is deferred (retry timer armed) and has not been sent
    /// yet; further `lose_connection` calls are no-ops until it has run.
    close_pending: bool,
    sent_close: bool,
    /// Plaintext rustls would not take while the handshake is still running
    /// (its internal buffers are capped at 64 KiB). Drained, in order, by
    /// `pump` once the handshake completes; while non-empty every later write
    /// is appended here so ordering is preserved.
    pre: Vec<u8>,
    /// Plaintext accepted into rustls' own pre-handshake buffer (rustls offers
    /// no length API for it); reset when the handshake completes.
    queued: usize,
}

impl Filter {
    /// Writes every pending TLS record to the raw transport.
    fn flush(&mut self, raw: &mut Transport<'_>) {
        let Some(conn) = self.conn.as_mut() else {
            return;
        };
        while conn.wants_write() {
            match conn.write_tls(&mut RawWriter(raw)) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    }

    /// Encrypts all of `plain`, never dropping bytes: rustls accepts at most
    /// 64 KiB of ciphertext at a time, so a partial write is followed by a
    /// drain into the raw transport and a retry. If nothing can drain yet
    /// (handshake running) the remainder is parked in `pre`. A rustls error
    /// aborts the connection.
    fn write_plain(&mut self, raw: &mut Transport<'_>, mut plain: &[u8]) {
        if self.sent_close || plain.is_empty() {
            return;
        }
        if self.tx_off {
            raw.write(plain);
            return;
        }
        if self.conn.is_none() {
            return;
        }
        if !self.pre.is_empty() {
            self.pre.extend_from_slice(plain);
            return;
        }
        let mut stalls = 0;
        while !plain.is_empty() {
            let Some(conn) = self.conn.as_mut() else {
                return;
            };
            let n = match conn.writer().write(plain) {
                Ok(n) => n,
                Err(e) => {
                    log::debug!("twine-tls: write: {e}");
                    self.kill(raw);
                    return;
                }
            };
            let handshaking = conn.is_handshaking();
            if handshaking {
                self.queued += n;
            }
            plain = &plain[n..];
            if plain.is_empty() {
                break;
            }
            if handshaking {
                self.pre.extend_from_slice(plain);
                break;
            }
            self.flush(raw);
            if n == 0 {
                stalls += 1;
                if stalls > 1 {
                    log::debug!("twine-tls: write made no progress; aborting");
                    self.kill(raw);
                    return;
                }
            } else {
                stalls = 0;
            }
        }
        self.flush(raw);
    }

    /// Drains the pre-handshake buffer once the handshake has completed.
    fn drain_pre(&mut self, raw: &mut Transport<'_>) {
        self.queued = 0;
        if !self.pre.is_empty() {
            let pre = std::mem::take(&mut self.pre);
            self.write_plain(raw, &pre);
        }
    }

    fn kill(&mut self, raw: &mut Transport<'_>) {
        self.sent_close = true;
        raw.abort();
    }

    /// Kernel-offloaded close: send close_notify as a kernel record, then close.
    /// The alert must not overtake bytes still queued in the transport, so it
    /// waits (polling on a timer) until the transport has drained.
    fn kernel_close(&mut self, raw: &mut Transport<'_>) {
        if raw.pending_write() == 0 {
            if let Some(fd) = raw.fd() {
                match ktls::send_close_notify(fd) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        self.defer_close(raw);
                        return;
                    }
                    Err(e) => log::debug!("twine-tls: kTLS close_notify: {e}"),
                }
            }
            self.close_pending = false;
            raw.lose_connection();
        } else {
            self.defer_close(raw);
        }
    }

    fn defer_close(&mut self, raw: &mut Transport<'_>) {
        self.close_wait += 1;
        if self.close_wait > CLOSE_RETRIES {
            log::debug!("twine-tls: kTLS close_notify gave up");
            self.close_pending = false;
            raw.lose_connection();
        } else {
            self.close_pending = true;
            raw.call_later(CLOSE_RETRY, CLOSE_TOKEN);
        }
    }
}

impl WriteFilter for Filter {
    fn write(&mut self, raw: &mut Transport<'_>, plain: &[u8]) {
        self.write_plain(raw, plain);
    }

    fn writev(&mut self, raw: &mut Transport<'_>, bufs: &[&[u8]]) {
        if self.tx_off {
            if !self.sent_close {
                raw.writev(bufs);
            }
            return;
        }
        // Slices go through the same loop in order; rustls coalesces what fits
        // into records on its own.
        for b in bufs {
            self.write_plain(raw, b);
        }
    }

    fn lose_connection(&mut self, raw: &mut Transport<'_>) {
        if self.sent_close {
            // A deferred close_notify must still go out before the close.
            if !self.close_pending {
                raw.lose_connection();
            }
            return;
        }
        self.sent_close = true;
        if self.tx_off {
            self.kernel_close(raw);
            return;
        }
        if let Some(conn) = self.conn.as_mut() {
            conn.send_close_notify();
        }
        self.flush(raw);
        raw.lose_connection();
    }

    fn pending_write(&self, raw: &Transport<'_>) -> usize {
        raw.pending_write() + self.pre.len() + self.queued
    }
}

thread_local! {
    /// Plaintext scratch buffer shared by every TLS connection on this loop
    /// thread (one loop = one thread), taken for the duration of a delivery
    /// and put back afterwards. The TLS factories have no engine handle to
    /// create a `LoopLocal` with, and a thread-local gives the same
    /// one-buffer-per-loop behaviour without per-connection memory.
    static SCRATCH: Cell<Vec<u8>> = const { Cell::new(Vec::new()) };

    /// Set by [`with_tls`] so `TlsProtocol::visit` exposes the wrapper itself
    /// (instead of the inner protocol) to exactly one visitor call.
    static PROBE: Cell<bool> = const { Cell::new(false) };
}

/// A protocol that terminates TLS for an inner protocol (see the crate docs).
///
/// Two connection timer tokens are reserved: `u64::MAX` (the handshake
/// deadline, honoured only while the handshake runs) and `u64::MAX - 1` (the
/// deferred kTLS close_notify retry, honoured only while armed). Otherwise
/// they reach the inner protocol's `timer_fired` like any token, but inner
/// protocols should avoid both values.
pub struct TlsProtocol {
    inner: Box<dyn Protocol>,
    filter: Filter,
    /// True once the peer's `close_notify` has been received.
    peer_close_notify: bool,
    /// Error to hand the inner protocol instead of a `None` loss reason.
    fail_err: Option<Error>,
    /// True until the handshake completes.
    handshaking: bool,
    /// True once `inner.connection_made` has run.
    inner_started: bool,
    /// True after a fatal TLS error; further input is ignored.
    failed: bool,
    /// True once `on_handshake_error` has fired (or is no longer needed).
    reported: bool,
    timer: Option<TimerId>,
    opts: TlsOptions,
    client: bool,
    /// The config allowed secret extraction (`enable_secret_extraction`).
    can_extract: bool,
    /// Set by `pump` when the handshake finished in this call; consumed by
    /// `data_received`, which decides on kTLS with the leftover input in hand.
    just_completed: bool,
    /// Record framing of what rustls was fed (only while an offload is possible).
    tracker: Option<ktls::RecordTracker>,
    /// Receive-side state once offloaded.
    rx: RxMode,
    /// What the handshake negotiated, kept because rustls is consumed by an offload.
    negotiated: Option<Negotiated>,
}

/// Who decrypts inbound records.
enum RxMode {
    /// rustls in user space (not offloaded).
    Rustls,
    /// The kernel: reads return plaintext.
    Kernel,
    /// User-space decryption with the extracted receive key (TX-only offload).
    User(Box<ktls::UserRx>),
    /// The handover failed after rustls was consumed: all input is ignored.
    Dead,
}

/// Handshake results retained across an offload.
struct Negotiated {
    alpn: Option<Vec<u8>>,
    certs: Option<Vec<CertificateDer<'static>>>,
}

impl TlsProtocol {
    fn new(
        conn: Connection,
        inner: Box<dyn Protocol>,
        opts: TlsOptions,
        can_extract: bool,
    ) -> Self {
        let client = matches!(conn, Connection::Client(_));
        let track = opts.kernel_offload && can_extract && ktls::supported();
        TlsProtocol {
            inner,
            filter: Filter {
                conn: Some(conn),
                tx_off: false,
                close_wait: 0,
                close_pending: false,
                sent_close: false,
                pre: Vec::new(),
                queued: 0,
            },
            peer_close_notify: false,
            fail_err: None,
            handshaking: true,
            inner_started: false,
            failed: false,
            reported: false,
            timer: None,
            opts,
            client,
            can_extract,
            just_completed: false,
            tracker: track.then(ktls::RecordTracker::default),
            rx: RxMode::Rustls,
            negotiated: None,
        }
    }

    /// The underlying rustls connection (negotiated version, SNI, ...), or
    /// `None` once record protection was handed to the kernel (rustls' state is
    /// consumed by the secret extraction; [`alpn_protocol`](Self::alpn_protocol)
    /// and [`peer_certificates`](Self::peer_certificates) keep working).
    pub fn connection(&self) -> Option<&Connection> {
        self.filter.conn.as_ref()
    }

    /// True until the handshake has completed.
    pub fn is_handshaking(&self) -> bool {
        self.handshaking
    }

    /// The negotiated ALPN protocol, if any.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match &self.filter.conn {
            Some(c) => c.alpn_protocol(),
            None => self.negotiated.as_ref()?.alpn.as_deref(),
        }
    }

    /// The peer's certificate chain, if it sent one.
    pub fn peer_certificates(&self) -> Option<&[CertificateDer<'_>]> {
        match &self.filter.conn {
            Some(c) => c.peer_certificates(),
            None => self.negotiated.as_ref()?.certs.as_deref(),
        }
    }

    /// Whether record protection has been handed to the kernel (kTLS): the
    /// kernel encrypts everything written. Reads are decrypted by the kernel
    /// too unless this was a transmit-only offload (see
    /// [`TlsOptions::kernel_offload`]).
    pub fn kernel_offloaded(&self) -> bool {
        self.filter.tx_off
    }

    /// Whether the kernel also decrypts inbound records (kTLS receive offload).
    pub fn kernel_rx_offloaded(&self) -> bool {
        matches!(self.rx, RxMode::Kernel)
    }

    /// The wrapped protocol.
    pub fn inner(&self) -> &dyn Protocol {
        &*self.inner
    }

    /// The wrapped protocol, mutably.
    pub fn inner_mut(&mut self) -> &mut dyn Protocol {
        &mut *self.inner
    }

    fn report(&mut self, e: &Error) {
        if self.reported {
            return;
        }
        self.reported = true;
        match &self.opts.on_handshake_error {
            Some(f) => f(e),
            None => log::debug!("twine-tls: handshake failed: {e}"),
        }
    }

    /// Fatal TLS error: flush the alert rustls queued, report if still
    /// handshaking, then close. The close is a graceful `lose_connection` so the
    /// alert reaches the peer; the handshake timer (if still armed) bounds it.
    fn fail(&mut self, t: &mut Transport<'_>, e: rustls::Error) {
        self.failed = true;
        self.filter.flush(t);
        let err = Error::Io(io::Error::new(io::ErrorKind::InvalidData, e.clone()));
        if self.handshaking {
            self.report(&err);
        } else {
            log::debug!("twine-tls: {err}");
            self.fail_err = Some(Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)));
        }
        self.filter.sent_close = true;
        t.lose_connection();
    }

    /// Flushes output, completes the handshake if it just finished, and delivers
    /// all available plaintext to the inner protocol.
    fn pump(&mut self, t: &mut Transport<'_>, lenient: bool) {
        self.filter.flush(t);
        if self.handshaking && !self.conn().is_handshaking() {
            self.handshaking = false;
            if let Some(id) = self.timer.take() {
                t.cancel_timer(id);
            }
            // Anything parked before the handshake goes out first.
            self.filter.drain_pre(t);
            self.inner_started = true;
            self.just_completed = true;
            let Self { inner, filter, .. } = self;
            inner.connection_made(&mut t.with_filter(filter));
        }
        if self.handshaking {
            return;
        }
        self.deliver(t, lenient);
        self.filter.flush(t);
    }

    fn conn(&mut self) -> &mut Connection {
        self.filter
            .conn
            .as_mut()
            .expect("rustls connection present until kTLS offload")
    }

    /// Called right after the handshake completed (and the first plaintext was
    /// delivered) with `rest`, the input not yet given to rustls. Attempts the
    /// kTLS handover; returns true if it took over (any `rest` and any partial
    /// record rustls held were routed to the new receive path).
    fn try_offload(&mut self, t: &mut Transport<'_>, rest: &[u8]) -> bool {
        let tracker = self.tracker.take();
        let Some(tracker) = tracker else {
            return false;
        };
        if !self.opts.kernel_offload || !self.can_extract || !ktls::available() {
            return false;
        }
        if self.failed
            || self.peer_close_notify
            || self.fail_err.is_some()
            || self.filter.sent_close
            || !self.filter.pre.is_empty()
            || !t.is_open()
            || t.pending_write() != 0
        {
            return false;
        }
        let Some(fd) = t.fd() else {
            return false;
        };
        let client = self.client;
        let conn = self.conn();
        if conn.is_handshaking() || conn.wants_write() {
            return false;
        }
        if conn.protocol_version() != Some(rustls::ProtocolVersion::TLSv1_3) {
            return false;
        }
        let kind = match conn.negotiated_cipher_suite().map(|s| s.suite()) {
            Some(rustls::CipherSuite::TLS13_AES_128_GCM_SHA256) => ktls::Kind::Aes128,
            Some(rustls::CipherSuite::TLS13_AES_256_GCM_SHA384) => ktls::Kind::Aes256,
            Some(rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256) => ktls::Kind::Chacha,
            _ => return false,
        };
        if !ktls::cipher_usable(kind) {
            return false;
        }
        // Unread input: a partial record inside rustls, and `rest`. The kernel
        // can only take over receiving at a record boundary with nothing queued.
        let Some(partial) = tracker.partial() else {
            return false;
        };
        let kernel_rx = self.opts.kernel_rx && !client && partial.is_empty() && rest.is_empty();
        let mut tail = partial.to_vec();
        tail.extend_from_slice(rest);
        if let Err(e) = ktls::attach_ulp(fd) {
            log::debug!("twine-tls: kTLS: attach ULP: {e}");
            return false;
        }
        // Everything cheap that can say no has run; snapshot the handshake
        // results (rustls is consumed by the extraction below).
        let conn = self.conn();
        let negotiated = Negotiated {
            alpn: conn.alpn_protocol().map(<[u8]>::to_vec),
            certs: conn
                .peer_certificates()
                .map(|c| c.iter().map(|d| d.clone().into_owned()).collect()),
        };
        // Point of no return: extraction consumes the rustls connection.
        let Some(conn) = self.filter.conn.take() else {
            return false;
        };
        let secrets = match conn.dangerous_extract_secrets() {
            Ok(s) => s,
            Err(e) => {
                log::debug!("twine-tls: kTLS: extract secrets: {e}");
                return self.offload_failed(t);
            }
        };
        let (Some((_, tx)), Some((_, rx))) = (
            ktls::crypto_info(secrets.tx.0, &secrets.tx.1),
            ktls::crypto_info(secrets.rx.0, &secrets.rx.1),
        ) else {
            return self.offload_failed(t);
        };
        if let Err(e) = ktls::set_crypto(fd, true, &tx) {
            log::debug!("twine-tls: kTLS: TLS_TX: {e}");
            return self.offload_failed(t);
        }
        self.filter.tx_off = true;
        self.negotiated = Some(negotiated);
        if kernel_rx {
            match ktls::set_crypto(fd, false, &rx) {
                Ok(()) => {
                    self.rx = RxMode::Kernel;
                    return true;
                }
                Err(e) => log::debug!("twine-tls: kTLS: TLS_RX: {e} (user-space receive)"),
            }
        }
        match ktls::UserRx::new(secrets.rx.0, &secrets.rx.1) {
            Some(u) => self.rx = RxMode::User(Box::new(u)),
            None => return self.offload_failed(t),
        }
        if !tail.is_empty() {
            self.data_offloaded(t, &mut tail);
        }
        true
    }

    /// The secrets are gone and the kernel refused them: rustls cannot be
    /// resurrected, so the connection ends.
    fn offload_failed(&mut self, t: &mut Transport<'_>) -> bool {
        self.fail_err = Some(Error::Io(io::Error::other("tls: kTLS handover failed")));
        self.rx = RxMode::Dead;
        self.filter.sent_close = true;
        t.abort();
        true
    }

    /// Inbound bytes after an offload: plaintext from the kernel, or
    /// ciphertext for the user-space receive decryptor.
    fn data_offloaded(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        let Self {
            inner,
            filter,
            rx,
            fail_err,
            peer_close_notify,
            ..
        } = self;
        match rx {
            RxMode::Kernel => inner.data_received(&mut t.with_filter(filter), data),
            RxMode::User(u) => {
                u.push(data);
                loop {
                    if !t.is_open() {
                        return;
                    }
                    match u.next() {
                        Ok(None) => return,
                        Ok(Some(ktls::Rec::Ignore)) => {}
                        Ok(Some(ktls::Rec::Data(r))) => {
                            inner.data_received(&mut t.with_filter(filter), u.slice(r))
                        }
                        Ok(Some(ktls::Rec::CloseNotify)) => {
                            *peer_close_notify = true;
                            WriteFilter::lose_connection(filter, t);
                            return;
                        }
                        Err(e) => {
                            log::debug!("twine-tls: {e}");
                            *fail_err =
                                Some(Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)));
                            t.abort();
                            return;
                        }
                    }
                }
            }
            RxMode::Rustls | RxMode::Dead => {}
        }
    }

    /// Hands decoded plaintext to the inner protocol. With `lenient` (used after
    /// a fatal record error) a read error just stops delivery.
    fn deliver(&mut self, t: &mut Transport<'_>, lenient: bool) {
        let mut buf = SCRATCH.with(Cell::take);
        if buf.len() < PLAIN_BUF {
            buf.resize(PLAIN_BUF, 0);
        }
        loop {
            if !t.is_open() {
                break;
            }
            let Self {
                inner,
                filter,
                peer_close_notify,
                ..
            } = self;
            let Some(conn) = filter.conn.as_mut() else {
                break;
            };
            match conn.reader().read(&mut buf) {
                Ok(0) => {
                    // Peer close_notify: answer with ours and close.
                    *peer_close_notify = true;
                    WriteFilter::lose_connection(filter, t);
                    break;
                }
                Ok(n) => inner.data_received(&mut t.with_filter(filter), &mut buf[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) if lenient => break,
                Err(e) => {
                    log::debug!("twine-tls: read error: {e}");
                    self.fail_err = Some(Error::Io(e));
                    t.abort();
                    break;
                }
            }
        }
        SCRATCH.with(|c| c.set(buf));
    }

    /// `process_new_packets` failed: deliver what was already decoded, then
    /// close with the error.
    fn fail_with_delivery(&mut self, t: &mut Transport<'_>, e: rustls::Error) {
        // The handshake may have completed (and data arrived) in the same
        // batch as the bad record; run the normal path so the inner protocol
        // is started and sees everything decoded so far.
        if !self.conn().is_handshaking() {
            self.pump(t, true);
        }
        if t.is_open() {
            self.fail(t, e);
        }
    }
}

impl Protocol for TlsProtocol {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_filtered(true);
        self.timer = Some(t.call_later(self.opts.handshake_timeout, HANDSHAKE_TOKEN));
        if self.client {
            // Sends the ClientHello.
            self.filter.flush(t);
        }
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if !matches!(self.rx, RxMode::Rustls) {
            self.data_offloaded(t, data);
            return;
        }
        if self.filter.conn.is_none() {
            return;
        }
        let mut off = 0;
        let mut stalled = false;
        while off < data.len() && !self.failed {
            let mut cur = &data[off..];
            let n = match self.conn().read_tls(&mut cur) {
                Ok(n) => n,
                Err(e) => {
                    log::debug!("twine-tls: read_tls: {e}");
                    t.abort();
                    return;
                }
            };
            if n == 0 {
                // rustls' input buffer is full. Process what it holds (and
                // deliver the plaintext) and retry once; never drop ciphertext.
                if stalled {
                    log::debug!("twine-tls: rustls refused input twice; aborting");
                    self.fail_err = Some(Error::Io(io::Error::other(
                        "tls: input buffer full and not draining",
                    )));
                    t.abort();
                    return;
                }
                stalled = true;
            } else {
                stalled = false;
                if let Some(tr) = self.tracker.as_mut() {
                    tr.feed(&data[off..off + n]);
                }
                off += n;
            }
            if let Err(e) = self.conn().process_new_packets() {
                self.fail_with_delivery(t, e);
                return;
            }
            self.pump(t, false);
            if !t.is_open() {
                return;
            }
            if self.just_completed {
                self.just_completed = false;
                if self.try_offload(t, &data[off..]) {
                    return;
                }
                self.tracker = None;
            }
        }
    }

    fn connection_lost(&mut self, t: &mut Transport<'_>, reason: Option<Error>) {
        if self.inner_started {
            let Self {
                inner,
                filter,
                fail_err,
                peer_close_notify,
                ..
            } = self;
            // A TLS failure explains a bare close; a TCP close with neither our
            // own close nor the peer's close_notify is a truncation. (An
            // inner `abort()` also looks like this and is reported the same.)
            let reason = reason.or_else(|| fail_err.take()).or_else(|| {
                (!*peer_close_notify && !filter.sent_close)
                    .then(|| Error::Io(io::Error::from(io::ErrorKind::UnexpectedEof)))
            });
            inner.connection_lost(&mut t.with_filter(filter), reason);
        } else if !self.reported {
            let e = reason.unwrap_or_else(|| {
                Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "tls: connection closed during handshake",
                ))
            });
            self.report(&e);
        }
    }

    fn pause_producing(&mut self, t: &mut Transport<'_>) {
        if self.inner_started {
            let Self { inner, filter, .. } = self;
            inner.pause_producing(&mut t.with_filter(filter));
        }
    }

    fn resume_producing(&mut self, t: &mut Transport<'_>) {
        if self.inner_started {
            let Self { inner, filter, .. } = self;
            inner.resume_producing(&mut t.with_filter(filter));
        }
    }

    fn stop_producing(&mut self, t: &mut Transport<'_>) {
        if self.inner_started {
            let Self { inner, filter, .. } = self;
            inner.stop_producing(&mut t.with_filter(filter));
        }
    }

    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        if token == CLOSE_TOKEN && self.filter.close_pending {
            self.filter.kernel_close(t);
        } else if token == HANDSHAKE_TOKEN && self.handshaking {
            self.timer = None;
            self.report(&handshake_timeout_error());
            t.abort();
        } else if self.inner_started {
            let Self { inner, filter, .. } = self;
            inner.timer_fired(&mut t.with_filter(filter), token);
        }
    }

    fn visit(
        &mut self,
        t: &mut Transport<'_>,
        f: &mut dyn FnMut(&mut dyn Any, &mut Transport<'_>),
    ) {
        if PROBE.with(|p| p.replace(false)) {
            f(self, t);
            return;
        }
        let Self { inner, filter, .. } = self;
        inner.visit(&mut t.with_filter(filter), f)
    }

    fn type_name(&self) -> &'static str {
        "twine_tls::TlsProtocol"
    }
}

/// Resets the probe flag even if the visitor never ran.
struct ProbeGuard;
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        PROBE.with(|p| p.set(false));
    }
}

/// Runs `f` against the [`TlsProtocol`] of connection `id` (which must be on
/// this loop) via [`Loop::with_protocol`]. The transport `f` receives is the
/// raw, unfiltered one; writes through it bypass encryption, so use
/// `tls.inner_mut()` / the inner protocol's own view for application data.
///
/// Errors as `Loop::with_protocol` (`Closed`, `Busy`, or an `Io` error of kind
/// `InvalidInput` if the connection is not TLS).
pub fn with_tls<R>(
    lp: &mut Loop,
    id: ConnId,
    f: impl FnOnce(&mut TlsProtocol, &mut Transport<'_>) -> R,
) -> twine::Result<R> {
    PROBE.with(|p| p.set(true));
    let _g = ProbeGuard;
    lp.with_protocol::<TlsProtocol, R>(id, f)
}

/// A stand-in used when rustls cannot create a connection: aborts immediately.
struct Reject;
impl Protocol for Reject {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.abort();
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _data: &mut [u8]) {}
}

/// Wraps `inner` so every connection it builds is TLS-terminated with `cfg`.
pub fn server(cfg: Arc<ServerConfig>, inner: impl Factory, opts: TlsOptions) -> impl Factory {
    warm_probes(opts.kernel_offload && cfg.enable_secret_extraction);
    move |lp: &mut Loop, peer: SocketAddr| -> Box<dyn Protocol> {
        match ServerConnection::new(cfg.clone()) {
            Ok(c) => {
                let p = inner.build_protocol(lp, peer);
                Box::new(TlsProtocol::new(
                    Connection::Server(c),
                    p,
                    opts.clone(),
                    cfg.enable_secret_extraction,
                ))
            }
            Err(e) => {
                log::error!("twine-tls: ServerConnection::new: {e}");
                Box::new(Reject)
            }
        }
    }
}

/// Runs the (cached) kTLS capability probes up front, off the first handshake.
fn warm_probes(wanted: bool) {
    if wanted {
        ktls::warm();
    }
}

fn client_protocol(
    cfg: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    inner: Box<dyn Protocol>,
    opts: TlsOptions,
) -> twine::Result<Box<dyn Protocol>> {
    let can_extract = cfg.enable_secret_extraction;
    let c = ClientConnection::new(cfg, server_name)
        .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?;
    Ok(Box::new(TlsProtocol::new(
        Connection::Client(c),
        inner,
        opts,
        can_extract,
    )))
}

/// Builds a client-side wrapper protocol around `inner` (for use with
/// `Loop::connect_tcp` / `LoopHandle::connect_tcp`).
pub fn client(
    cfg: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    inner: Box<dyn Protocol>,
    opts: TlsOptions,
) -> twine::Result<Box<dyn Protocol>> {
    warm_probes(opts.kernel_offload && cfg.enable_secret_extraction);
    client_protocol(cfg, server_name, inner, opts)
}

/// Like [`client`], as a factory (for `LoopHandle::connect_tcp`).
pub fn client_factory(
    cfg: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    inner: impl Factory,
    opts: TlsOptions,
) -> impl Factory {
    warm_probes(opts.kernel_offload && cfg.enable_secret_extraction);
    move |lp: &mut Loop, peer: SocketAddr| -> Box<dyn Protocol> {
        let p = inner.build_protocol(lp, peer);
        match client_protocol(cfg.clone(), server_name.clone(), p, opts.clone()) {
            Ok(p) => p,
            Err(e) => {
                log::error!("twine-tls: {e}");
                Box::new(Reject)
            }
        }
    }
}

/// Binds `addr` and serves `inner` over TLS (shorthand for
/// `eng.listen_tcp(addr, server(cfg, inner, opts), lopts)`).
pub fn listen(
    eng: &twine::EngineHandle,
    addr: impl ToSocketAddrs,
    cfg: Arc<ServerConfig>,
    inner: impl Factory,
    opts: TlsOptions,
    lopts: ListenOptions,
) -> twine::Result<Listener> {
    eng.listen_tcp(addr, server(cfg, inner, opts), lopts)
}

/// Dials `addr` and runs TLS client-side around the protocol `build` returns.
/// The `Deferred` resolves when the TCP connection is established (after the
/// wrapper's `connection_made`), not when the handshake finishes; the inner
/// protocol's `connection_made` runs on handshake completion.
pub fn connect(
    lp: &mut Loop,
    addr: SocketAddr,
    cfg: Arc<ClientConfig>,
    server_name: ServerName<'static>,
    build: impl FnOnce(&mut Loop, SocketAddr) -> Box<dyn Protocol> + 'static,
    opts: TlsOptions,
    dopts: DialOptions,
) -> twine::Deferred<ConnId> {
    warm_probes(opts.kernel_offload && cfg.enable_secret_extraction);
    lp.connect_tcp(
        addr,
        move |lp, peer| {
            let inner = build(lp, peer);
            match client_protocol(cfg, server_name, inner, opts) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("twine-tls: {e}");
                    Box::new(Reject)
                }
            }
        },
        dopts,
    )
}
