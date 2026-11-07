//! Shared helpers: self-signed cert, configs, a TLS echo/probe server.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig, SignatureScheme};
use twine::{factory, Engine, ListenOptions, Options, Protocol, Transport, TransportHandle};
use twine_tls::TlsOptions;

pub struct Certs {
    pub chain: Vec<CertificateDer<'static>>,
    pub key_der: Vec<u8>,
}

impl Certs {
    pub fn new() -> Certs {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        Certs {
            chain: vec![ck.cert.der().clone()],
            key_der: ck.signing_key.serialize_der(),
        }
    }

    pub fn server_config(&self, alpn: &[&[u8]]) -> Arc<ServerConfig> {
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(self.key_der.clone()));
        let mut cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(self.chain.clone(), key)
            .unwrap();
        cfg.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
        Arc::new(cfg)
    }

    pub fn client_config(
        &self,
        versions: &[&'static rustls::SupportedProtocolVersion],
        alpn: &[&[u8]],
    ) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(self.chain[0].clone()).unwrap();
        let mut cfg = ClientConfig::builder_with_protocol_versions(versions)
            .with_root_certificates(roots)
            .with_no_client_auth();
        cfg.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
        Arc::new(cfg)
    }
}

pub fn localhost() -> rustls::pki_types::ServerName<'static> {
    "localhost".try_into().unwrap()
}

#[allow(dead_code)]
pub fn _unused(_: SignatureScheme) {}

/// Events reported by [`Probe`].
pub enum Ev {
    Made(TransportHandle),
    Data(Vec<u8>),
    Lost(bool),
}

#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    /// Echo everything.
    Echo,
    /// Echo; on a chunk containing "bye", reply "farewell" and `lose_connection`.
    ByeCloses,
    /// Record only.
    Silent,
}

pub struct Probe {
    pub tx: Sender<Ev>,
    pub mode: Mode,
}

impl Protocol for Probe {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let _ = self.tx.send(Ev::Made(t.handle()));
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        let _ = self.tx.send(Ev::Data(data.to_vec()));
        match self.mode {
            Mode::Echo => t.write(data),
            Mode::ByeCloses => {
                if data.windows(3).any(|w| w == b"bye") {
                    t.write(b"farewell");
                    t.lose_connection();
                } else {
                    t.write(data);
                }
            }
            Mode::Silent => {}
        }
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, reason: Option<twine::Error>) {
        let _ = self.tx.send(Ev::Lost(reason.is_some()));
    }
}

pub struct TlsServer {
    pub eng: Arc<Engine>,
    pub addr: SocketAddr,
    pub events: Receiver<Ev>,
    join: Option<JoinHandle<twine::Result<()>>>,
}

impl std::fmt::Debug for Ev {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ev::Made(_) => f.write_str("Made"),
            Ev::Data(d) => write!(f, "Data({})", d.len()),
            Ev::Lost(r) => write!(f, "Lost({r})"),
        }
    }
}

impl TlsServer {
    pub fn start(cfg: Arc<ServerConfig>, mode: Mode, loops: usize, opts: TlsOptions) -> TlsServer {
        TlsServer::start_custom(cfg, loops, opts, move |tx| Box::new(Probe { tx, mode }))
    }

    /// Like [`start`](Self::start) with a caller-built inner protocol per
    /// connection (it reports through the given sender).
    pub fn start_custom(
        cfg: Arc<ServerConfig>,
        loops: usize,
        opts: TlsOptions,
        mk: impl Fn(Sender<Ev>) -> Box<dyn Protocol> + Send + Sync + 'static,
    ) -> TlsServer {
        let (tx, events) = channel();
        let tx = Mutex::new(tx);
        let inner = factory(move |_, _| mk(tx.lock().unwrap().clone()));
        let eng = Arc::new(
            Engine::new(
                Options::default()
                    .loops(loops)
                    .shutdown_timeout(Duration::from_millis(500)),
            )
            .unwrap(),
        );
        let lst = twine_tls::listen(
            &eng.handle(),
            "127.0.0.1:0",
            cfg,
            inner,
            opts,
            ListenOptions::default(),
        )
        .unwrap();
        let addr = lst.addr();
        let e2 = eng.clone();
        let join = Some(std::thread::spawn(move || e2.run()));
        TlsServer {
            eng,
            addr,
            events,
            join,
        }
    }

    pub fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().unwrap().unwrap();
        }
    }

    /// Waits for the next event matching `pred`, skipping others.
    pub fn wait_ev(&self, mut pred: impl FnMut(&Ev) -> bool) -> Ev {
        let end = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let left = end.saturating_duration_since(std::time::Instant::now());
            let ev = self.events.recv_timeout(left).expect("event timeout");
            if pred(&ev) {
                return ev;
            }
        }
    }
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Deterministic byte pattern.
pub fn pattern(i: usize) -> u8 {
    (i.wrapping_mul(31) ^ (i >> 7)) as u8
}

/// Server config with secret extraction enabled.
pub fn ktls_server_config(certs: &Certs) -> Arc<ServerConfig> {
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(certs.key_der.clone()));
    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.chain.clone(), key)
        .unwrap();
    twine_tls::enable_ktls(&mut cfg);
    Arc::new(cfg)
}

/// Client config limited to one TLS 1.3 suite (or TLS 1.2 when `suite` is None).
pub fn client_cfg(
    certs: &Certs,
    suite: Option<rustls::SupportedCipherSuite>,
    extract: bool,
) -> Arc<ClientConfig> {
    use rustls::crypto::{ring as ring_provider, CryptoProvider};
    let mut roots = RootCertStore::empty();
    roots.add(certs.chain[0].clone()).unwrap();
    let mut cfg = match suite {
        Some(s) => {
            let mut p: CryptoProvider = ring_provider::default_provider();
            p.cipher_suites = vec![s];
            ClientConfig::builder_with_provider(Arc::new(p))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
        }
        None => ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12]),
    }
    .with_root_certificates(roots)
    .with_no_client_auth();
    if extract {
        twine_tls::enable_ktls_client(&mut cfg);
    }
    Arc::new(cfg)
}

/// (kernel_offloaded, kernel_rx_offloaded) of the connection behind `h`.
pub fn query(h: &TransportHandle) -> (bool, bool) {
    let (tx, rx) = channel();
    let id = h.id();
    h.loop_handle()
        .call_from_thread(move |lp| {
            let r = twine_tls::with_tls(lp, id, |tls, _| {
                (tls.kernel_offloaded(), tls.kernel_rx_offloaded())
            });
            tx.send(r.unwrap()).unwrap();
        })
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap()
}

/// Waits for the server's `connection_made` (the handshake has completed and
/// the kTLS decision was taken) and returns the handle.
pub fn made(srv: &TlsServer) -> TransportHandle {
    match srv.wait_ev(|e| matches!(e, Ev::Made(_))) {
        Ev::Made(h) => h,
        _ => unreachable!(),
    }
}
