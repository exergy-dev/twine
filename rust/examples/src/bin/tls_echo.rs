//! TLS echo server with a self-signed certificate generated at startup (rcgen).
//!
//! ```text
//! cargo run --release -p twine-examples --bin tls_echo -- --addr 127.0.0.1:9443
//! openssl s_client -connect 127.0.0.1:9443 -quiet
//! ```
//!
//! `--cert-out FILE` writes the certificate PEM so a client can trust it
//! (`openssl s_client -CAfile FILE -verify_return_error -servername localhost`).

use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::ServerConfig;
use twine::{Engine, ListenOptions, Protocol, Transport};
use twine_examples::common::{init_logger, Args};
use twine_tls::TlsOptions;

const USAGE: &str =
    "usage: tls_echo [--addr 0.0.0.0:9443] [--loops N] [--name localhost] [--cert-out FILE]";

struct Echo;

impl Protocol for Echo {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_idle_timeout(Some(Duration::from_secs(30)));
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

fn main() -> twine::Result<()> {
    init_logger();
    let a = Args::parse(USAGE, &["addr", "loops", "name", "cert-out"]);
    let name = a.str("name", "localhost");
    let ck = rcgen::generate_simple_self_signed(vec![name.clone()]).expect("generate certificate");
    let cert_pem = ck.cert.pem();
    let out = a.str("cert-out", "");
    if !out.is_empty() {
        std::fs::write(&out, &cert_pem)?;
        log::info!("tls_echo: certificate written to {out}");
    }
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_no_client_auth()
        .with_single_cert(vec![ck.cert.der().clone()], key)
        .expect("server config");

    let eng = Engine::builder().loops(a.usize("loops", 0)).build()?;
    let lst = twine_tls::listen(
        &eng.handle(),
        a.str("addr", "0.0.0.0:9443").as_str(),
        Arc::new(cfg),
        twine::factory(|_, _| Box::new(Echo)),
        TlsOptions::default(),
        ListenOptions::default(),
    )?;
    log::info!("tls_echo: listening on {} (cert for {name:?})", lst.addr());
    eng.run()
}
