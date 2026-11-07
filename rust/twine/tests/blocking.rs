//! The thread-per-connection `blocking` adapter.

mod common;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::*;
use twine::blocking;

fn echo_loop(mut c: twine::BlockingConn) {
    let mut buf = [0u8; 4096];
    loop {
        let n = match c.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if c.write_all(&buf[..n]).is_err() {
            break;
        }
    }
}

#[test]
fn echo_50_clients_100_messages() {
    let mut srv = Server::start(2, blocking(echo_loop));
    let addr = srv.addr;
    let hs: Vec<_> = (0..50)
        .map(|c| {
            thread::spawn(move || {
                let mut s = TcpStream::connect(addr).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
                for m in 0..100usize {
                    let msg: Vec<u8> = (0..32 + m % 13)
                        .map(|i| pattern(c * 977 + m * 31 + i))
                        .collect();
                    write_all(&mut s, &msg);
                    assert_eq!(read_n(&mut s, msg.len()), msg, "client {c} msg {m}");
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    srv.stop();
}

#[test]
fn slow_handler_backpressure_2mib() {
    const TOTAL: usize = 2 << 20;
    let mut srv = Server::start(
        1,
        blocking(|mut c| {
            // Let the loop fill the 64-chunk channel and pause reading.
            thread::sleep(Duration::from_millis(400));
            let mut buf = [0u8; 1024];
            loop {
                match c.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if c.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                }
                if buf[0] == 0xAA {
                    thread::sleep(Duration::from_micros(200));
                }
            }
        }),
    );
    let s = srv.connect();
    let mut r = s.try_clone().unwrap();
    let reader = thread::spawn(move || {
        let mut got = vec![0u8; TOTAL];
        r.read_exact(&mut got).unwrap();
        got
    });
    let mut w = s;
    let data: Vec<u8> = (0..TOTAL).map(pattern).collect();
    for chunk in data.chunks(512) {
        w.write_all(chunk).unwrap();
    }
    assert!(reader.join().unwrap() == data, "echo differs");
    srv.stop();
}

#[test]
fn handler_panic_aborts_only_that_connection() {
    let mut srv = Server::start(
        1,
        blocking(|mut c| {
            let mut b = [0u8; 1];
            if c.read(&mut b).unwrap_or(0) == 1 && b[0] == b'P' {
                panic!("handler panic");
            }
            let _ = c.write_all(&b);
            echo_loop(c);
        }),
    );
    let mut ok = srv.connect();
    write_all(&mut ok, b"a");
    assert_eq!(read_n(&mut ok, 1), b"a");
    let mut bad = srv.connect();
    write_all(&mut bad, b"P");
    assert!(read_to_end_lossy(&mut bad).is_empty());
    write_all(&mut ok, b"bc");
    assert_eq!(read_n(&mut ok, 2), b"bc");
    srv.stop();
}

#[test]
fn lose_connection_delivers_eof() {
    let mut srv = Server::start(
        1,
        blocking(|mut c| {
            let mut b = [0u8; 16];
            let n = c.read(&mut b).unwrap();
            c.write_all(&b[..n]).unwrap();
            c.write_all(b"-bye").unwrap();
            c.lose_connection();
            // Keep the thread alive a moment: the close must not depend on return.
            thread::sleep(Duration::from_millis(200));
        }),
    );
    let mut s = srv.connect();
    write_all(&mut s, b"hi");
    assert_eq!(read_to_end_lossy(&mut s), b"hi-bye");
    srv.stop();
}

#[test]
fn read_timeout_then_data() {
    let mut srv = Server::start(
        1,
        blocking(|mut c| {
            c.set_read_timeout(Some(Duration::from_millis(100)));
            let mut b = [0u8; 16];
            let e = c.read(&mut b).unwrap_err();
            assert!(matches!(
                e.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ));
            c.write_all(b"timeout").unwrap();
            c.set_read_timeout(None);
            echo_loop(c);
        }),
    );
    let mut s = srv.connect();
    assert_eq!(read_n(&mut s, 7), b"timeout");
    write_all(&mut s, b"later");
    assert_eq!(read_n(&mut s, 5), b"later");
    srv.stop();
}

struct Live(Arc<AtomicUsize>);
impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, SeqCst);
    }
}

#[test]
fn engine_shutdown_ends_handler_threads() {
    let live = Arc::new(AtomicUsize::new(0));
    let l2 = live.clone();
    let mut srv = Server::start(
        2,
        blocking(move |c| {
            l2.fetch_add(1, SeqCst);
            let _g = Live(l2.clone());
            echo_loop(c);
        }),
    );
    let mut conns: Vec<TcpStream> = (0..6).map(|_| srv.connect()).collect();
    for s in &mut conns {
        write_all(s, b"x");
        assert_eq!(read_n(s, 1), b"x");
    }
    assert_eq!(live.load(SeqCst), 6);
    srv.stop();
    assert!(wait_until(Duration::from_secs(2), || live.load(SeqCst) == 0));
    drop(conns);
}

#[test]
fn peer_and_host_are_set() {
    let mut srv = Server::start(
        1,
        blocking(|mut c| {
            let s = format!("{}|{}", c.peer(), c.host());
            let _ = c.write_all(s.as_bytes());
            let mut b = [0u8; 1];
            let _ = c.read(&mut b);
        }),
    );
    let mut s = srv.connect();
    let me = s.local_addr().unwrap();
    let want = format!("{}|{}", me, srv.addr);
    assert_eq!(read_n(&mut s, want.len()), want.as_bytes());
    let _ = s.shutdown(Shutdown::Write);
    srv.stop();
}
