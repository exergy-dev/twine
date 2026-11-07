//! `Broadcast` across a four-loop engine.

mod common;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, ConnId, Error, Protocol, Transport};
use twine_proto::Broadcast;

#[derive(Default)]
struct Seen {
    ids: Mutex<Vec<ConnId>>,
    loops: Mutex<HashSet<usize>>,
}

/// Joins the broadcast; relays every received chunk to the others.
struct Member {
    bc: Broadcast,
    seen: Arc<Seen>,
    eager_remove: bool,
}

impl Protocol for Member {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.bc.add(t.handle());
        self.bc.add(t.handle()); // idempotent: len() counts each connection once
        self.seen.ids.lock().unwrap().push(t.id());
        self.seen.loops.lock().unwrap().insert(t.id().loop_id());
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        self.bc.send_from(&*data, t.id());
    }
    fn connection_lost(&mut self, t: &mut Transport<'_>, _r: Option<Error>) {
        if self.eager_remove {
            self.bc.remove_id(t.id());
        }
    }
}

fn setup(eager: bool, n: usize) -> (Rig, Broadcast, Arc<Seen>, Vec<TcpStream>) {
    let rig = Rig::start(4);
    let bc = Broadcast::new();
    let seen = Arc::new(Seen::default());
    let (b2, s2) = (bc.clone(), seen.clone());
    let lst = rig.listen(
        "127.0.0.1:0",
        factory(move |_, _| {
            Box::new(Member {
                bc: b2.clone(),
                seen: s2.clone(),
                eager_remove: eager,
            })
        }),
    );
    let addr = lst.addr();
    let clients: Vec<TcpStream> = (0..n)
        .map(|_| {
            let s = TcpStream::connect(addr).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            s
        })
        .collect();
    assert!(wait_until(Duration::from_secs(10), || bc.len() == n));
    (rig, bc, seen, clients)
}

fn expect_nothing(s: &mut TcpStream) {
    s.set_read_timeout(Some(Duration::from_millis(30))).unwrap();
    let mut b = [0u8; 16];
    match s.read(&mut b) {
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) => {}
        other => panic!("unexpected data or EOF: {other:?}"),
    }
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
}

#[test]
fn send_from_thread_reaches_all_fifty_clients_byte_exact() {
    let (_rig, bc, seen, mut clients) = setup(true, 50);
    assert!(
        seen.loops.lock().unwrap().len() >= 2,
        "connections landed on one loop only: {:?}",
        seen.loops.lock().unwrap()
    );
    let msg: Vec<u8> = (0..20_000u32).map(|i| (i % 253) as u8).collect();
    let bc2 = bc.clone();
    let m2 = msg.clone();
    std::thread::spawn(move || bc2.send(m2)).join().unwrap(); // Vec<u8>: one copy
    bc.send(&b"tail"[..]);
    let mut want = msg.clone();
    want.extend_from_slice(b"tail");
    for c in clients.iter_mut() {
        assert_eq!(read_n(c, want.len()), want);
    }
    for c in clients.iter_mut() {
        expect_nothing(c);
    }
}

#[test]
fn send_from_excludes_the_sender() {
    let (_rig, bc, _seen, mut clients) = setup(true, 50);
    let _ = &bc;
    clients[7].write_all(b"ping").unwrap();
    for (i, c) in clients.iter_mut().enumerate() {
        if i != 7 {
            assert_eq!(read_n(c, 4), b"ping", "client {i}");
        }
    }
    expect_nothing(&mut clients[7]);
}

#[test]
fn closed_members_are_pruned_lazily() {
    // Members that do not remove themselves on close.
    let (_rig, bc, _seen, mut clients) = setup(false, 20);
    let rest = clients.split_off(8);
    drop(clients); // closes 8
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(bc.len(), 20, "pruning is lazy");
    bc.send(&b"x"[..]);
    assert!(
        wait_until(Duration::from_secs(5), || bc.len() == 12),
        "len {}",
        bc.len()
    );
    for mut c in rest {
        assert_eq!(read_n(&mut c, 1), b"x");
    }
}

#[test]
fn explicit_remove_and_eager_removal() {
    let (_rig, bc, seen, mut clients) = setup(true, 10);
    let ids = seen.ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 10);
    // Explicitly removing a member that is still connected: it no longer receives.
    // Find which client has ids[0] by elimination: remove then broadcast and count
    // how many clients got data.
    bc.remove_id(ids[0]);
    bc.remove_id(ids[0]); // idempotent
    assert_eq!(bc.len(), 9);
    bc.send(&b"m"[..]);
    let mut got = 0;
    for c in clients.iter_mut() {
        c.set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        let mut b = [0u8; 1];
        if c.read(&mut b).is_ok() {
            got += 1;
        }
    }
    assert_eq!(got, 9);
    // Eager removal on close.
    drop(clients);
    assert!(
        wait_until(Duration::from_secs(5), || bc.is_empty()),
        "len {}",
        bc.len()
    );
    assert!(bc.is_empty());
    bc.send(&b"nobody"[..]); // no members: no-op
}

#[test]
fn send_after_engine_stopped_prunes_without_panic() {
    let (rig, bc, _seen, clients) = setup(true, 4);
    assert_eq!(bc.len(), 4);
    drop(clients);
    drop(rig); // engine stopped, loops gone
    bc.send(&b"late"[..]);
    assert_eq!(bc.len(), 0);
}

#[test]
fn many_members_add_and_remove_in_any_order() {
    // Swap-remove bookkeeping: remove in a scrambled order, then check that
    // exactly the survivors receive a message.
    let (_rig, bc, seen, mut clients) = setup(true, 40);
    let ids = seen.ids.lock().unwrap().clone();
    let mut order: Vec<usize> = (0..ids.len()).collect();
    let mut rng = Rng::new(5);
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i + 1));
    }
    for &i in order.iter().take(25) {
        bc.remove_id(ids[i]);
    }
    assert_eq!(bc.len(), 15);
    bc.send(&b"s"[..]);
    std::thread::sleep(Duration::from_millis(300));
    let mut got = 0;
    for c in clients.iter_mut() {
        c.set_nonblocking(true).unwrap();
        let mut b = [0u8; 1];
        if c.read(&mut b).is_ok() {
            got += 1;
        }
    }
    assert_eq!(got, 15);
    // Removing again is a no-op.
    for &i in order.iter().take(25) {
        bc.remove_id(ids[i]); // no-op
    }
    assert_eq!(bc.len(), 15);
}

#[test]
fn second_engine_is_refused_instead_of_colliding() {
    // Two engines, both with a loop 0. One Broadcast shared by both: the second
    // engine's members are refused (with a warning) rather than merged into the
    // first engine's shard.
    let bc = Broadcast::new();
    let mk = |bc: Broadcast| {
        let rig = Rig::start(1);
        let seen = Arc::new(Seen::default());
        let (b2, s2) = (bc, seen.clone());
        let lst = rig.listen(
            "127.0.0.1:0",
            factory(move |_, _| {
                Box::new(Member {
                    bc: b2.clone(),
                    seen: s2.clone(),
                    eager_remove: true,
                })
            }),
        );
        (rig, lst.addr())
    };
    let (_r1, a1) = mk(bc.clone());
    let (_r2, a2) = mk(bc.clone());
    let mut c1 = TcpStream::connect(a1).unwrap();
    assert!(wait_until(Duration::from_secs(5), || bc.len() == 1));
    let mut c2 = TcpStream::connect(a2).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(bc.len(), 1, "second engine's connection must not be added");
    bc.send(&b"x"[..]);
    c1.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(read_n(&mut c1, 1), b"x");
    expect_nothing(&mut c2);
}
