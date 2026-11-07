//! Echo round trips across 1, 2 and 4 loops.

mod common;

use std::sync::Arc;
use std::thread;

use common::*;
use twine::{factory, Protocol, Transport};

struct Echo;
impl Protocol for Echo {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

fn run_echo(loops: usize) {
    let mut srv = Server::start(loops, factory(|_, _| Box::new(Echo)));
    let addr = srv.addr;
    let handles: Vec<_> = (0..50)
        .map(|c| {
            thread::spawn(move || {
                let mut s = std::net::TcpStream::connect(addr).unwrap();
                s.set_nodelay(true).unwrap();
                s.set_read_timeout(Some(std::time::Duration::from_secs(30)))
                    .unwrap();
                for m in 0..100usize {
                    let msg: Vec<u8> = (0..64 + (m % 7))
                        .map(|i| pattern(c * 1000 + m * 31 + i))
                        .collect();
                    write_all(&mut s, &msg);
                    let got = read_n(&mut s, msg.len());
                    assert_eq!(got, msg, "client {c} message {m}");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let snaps = srv.eng.metrics();
    let accepts: u64 = snaps.iter().map(|s| s.accepts).sum();
    assert_eq!(accepts, 50);
    if loops > 1 {
        // SO_REUSEPORT spreads connections; at minimum every loop has a listener.
        assert_eq!(snaps.len(), loops);
    }
    srv.stop();
    assert!(srv.eng.is_stopped());
}

#[test]
fn echo_one_loop() {
    run_echo(1);
}

#[test]
fn echo_two_loops() {
    run_echo(2);
}

#[test]
fn echo_four_loops() {
    run_echo(4);
}

/// Single listener on loop 0 handing accepted fds to other loops round-robin.
#[test]
fn echo_handoff_without_reuseport() {
    let opts = twine::Options::default().loops(3).reuse_port(false);
    let mut srv = Server::start_with(
        opts,
        twine::ListenOptions::default(),
        factory(|_, _| Box::new(Echo)),
    );
    let addr = srv.addr;
    let srv_eng = Arc::clone(&srv.eng);
    let handles: Vec<_> = (0..12)
        .map(|c| {
            thread::spawn(move || {
                let mut s = std::net::TcpStream::connect(addr).unwrap();
                s.set_read_timeout(Some(std::time::Duration::from_secs(30)))
                    .unwrap();
                for m in 0..20usize {
                    let msg = vec![(c + m) as u8; 100];
                    write_all(&mut s, &msg);
                    assert_eq!(read_n(&mut s, 100), msg);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let busy = srv_eng.metrics().iter().filter(|m| m.reads > 0).count();
    assert_eq!(busy, 3, "connections must be spread over all loops");
    srv.stop();
}
