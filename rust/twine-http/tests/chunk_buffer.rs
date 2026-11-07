//! Chunked bodies are decoded incrementally: the raw bytes of consumed chunks
//! are not retained, so the reassembly buffer stays small however the body is
//! chunked.

mod common;

use std::io::Write;
use std::time::Duration;

use common::*;
use twine_http::{buffer_high_water, handler, reset_buffer_high_water, Server};

fn echo_len(max_body: usize) -> TestServer {
    TestServer::start(
        Server::new(handler(|ctx| {
            let n = ctx.body().len();
            ctx.set_body_str(&format!("{n}"));
        }))
        .max_body_bytes(max_body),
    )
}

fn send_chunks(srv: &TestServer, total: usize, ext: usize) -> Option<Resp> {
    let s = srv.connect();
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let mut w = s.try_clone().unwrap();
    let mut c = Reader::new(s);
    let t = std::thread::spawn(move || {
        let _ = w.write_all(b"POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n");
        let mut line = Vec::new();
        if ext > 0 {
            line.extend_from_slice(b"1;");
            line.extend(std::iter::repeat_n(b'e', ext));
            line.extend_from_slice(b"\r\nA\r\n");
        } else {
            line.extend_from_slice(b"1\r\nA\r\n");
        }
        let mut batch = Vec::new();
        for _ in 0..total {
            batch.extend_from_slice(&line);
            if batch.len() >= 256 << 10 {
                if w.write_all(&batch).is_err() {
                    return;
                }
                batch.clear();
            }
        }
        batch.extend_from_slice(b"0\r\n\r\n");
        let _ = w.write_all(&batch);
    });
    let r = c.read_response();
    let _ = t.join();
    r
}

#[test]
fn chunked_buffer_stays_bounded() {
    let max_body = 2 << 20;
    let srv = echo_len(max_body);
    reset_buffer_high_water();
    // 1 MiB in one-byte chunks: 6 raw bytes per payload byte.
    let r = send_chunks(&srv, 1 << 20, 0).expect("response");
    assert_eq!(r.status, 200);
    assert_eq!(r.body_str(), (1 << 20).to_string());
    let hw = buffer_high_water();
    assert!(hw < 2 * max_body, "buffer high water {hw}");
    // 4 KiB chunk extensions: ~4 KiB of raw framing per payload byte.
    reset_buffer_high_water();
    let r = send_chunks(&srv, 16 << 10, 4000).expect("response");
    assert_eq!(r.status, 200);
    assert_eq!(r.body_str(), (16 << 10).to_string());
    let hw = buffer_high_water();
    assert!(hw < 2 * max_body, "buffer high water {hw}");
    // The decoded size limit still applies.
    let r = send_chunks(&srv, max_body + 1, 0).expect("response");
    assert_eq!(r.status, 413);
}
