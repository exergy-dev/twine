//! Interop with `openssl s_client` (skipped when openssl is not installed).

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use common::*;
use twine_tls::TlsOptions;

fn have_openssl() -> bool {
    Command::new("openssl")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn interop(flag: &str) {
    if !have_openssl() {
        eprintln!("SKIP: openssl not found");
        return;
    }
    let certs = Certs::new();
    let mut srv = TlsServer::start(
        certs.server_config(&[]),
        Mode::Echo,
        1,
        TlsOptions::default(),
    );
    let mut child = Command::new("openssl")
        .args([
            "s_client",
            "-connect",
            &srv.addr.to_string(),
            flag,
            "-quiet",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());
    // Read on a helper thread so a regression fails after 10 s instead of hanging.
    let (ltx, lrx) = std::sync::mpsc::channel();
    stdin.write_all(b"hello twine\n").unwrap();
    stdin.flush().unwrap();
    std::thread::spawn(move || {
        let mut line = String::new();
        let r = out.read_line(&mut line);
        let _ = ltx.send((r.is_ok(), line));
    });
    let got = lrx.recv_timeout(std::time::Duration::from_secs(10));
    if got.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("openssl s_client: no echo within 10 s");
    }
    let (ok, line) = got.unwrap();
    assert!(ok);
    assert_eq!(line, "hello twine\n");
    drop(stdin);
    let _ = child.kill();
    let _ = child.wait();
    srv.stop();
}

#[test]
fn openssl_tls13() {
    interop("-tls1_3");
}

#[test]
fn openssl_tls12() {
    interop("-tls1_2");
}
