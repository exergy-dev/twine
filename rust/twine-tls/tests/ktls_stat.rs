//! kernel TLS counters: an offloaded connection moves `/proc/net/tls_stat`.
//! This binary holds a single test so no other test in the process touches the
//! counters (the file is per network namespace and system wide).

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use common::*;
use rustls::crypto::ring as ring_provider;
use rustls::{ClientConnection, StreamOwned};
use twine_tls::TlsOptions;

/// Reads one counter of `/proc/net/tls_stat`; `None` if the file or the
/// counter is missing.
fn stat(name: &str) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/net/tls_stat").ok()?;
    text.lines().find_map(|l| {
        let mut it = l.split_whitespace();
        (it.next()? == name).then(|| it.next()?.parse().ok())?
    })
}

#[test]
fn offloaded_connection_moves_tls_stat_counters() {
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        ktls_server_config(&certs),
        Mode::Echo,
        1,
        TlsOptions::default().kernel_offload(true),
    );
    let before = (stat("TlsTxSw"), stat("TlsCurrTxSw"));
    let cfg = client_cfg(
        &certs,
        Some(ring_provider::cipher_suite::TLS13_AES_128_GCM_SHA256),
        false,
    );
    let s = TcpStream::connect(srv.addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let mut c = StreamOwned::new(ClientConnection::new(cfg, localhost()).unwrap(), s);
    c.write_all(b"count me").unwrap();
    let mut b = [0u8; 8];
    c.read_exact(&mut b).unwrap();
    let (off, _) = query(&made(&srv));
    if !twine_tls::ktls_available() {
        assert!(!off);
        eprintln!("SKIP: kTLS unavailable");
        return;
    }
    assert!(off, "connection offloaded");
    let after = (stat("TlsTxSw"), stat("TlsCurrTxSw"));
    let (Some(tx0), Some(cur0), Some(tx1), Some(cur1)) = (before.0, before.1, after.0, after.1)
    else {
        eprintln!("SKIP: /proc/net/tls_stat missing or without TlsTxSw/TlsCurrTxSw");
        return;
    };
    eprintln!("TlsTxSw {tx0} -> {tx1}, TlsCurrTxSw {cur0} -> {cur1}");
    assert!(tx1 > tx0, "TlsTxSw moved");
    assert!(
        cur1 > cur0,
        "TlsCurrTxSw moved while the connection is open"
    );
    srv.stop();
}
