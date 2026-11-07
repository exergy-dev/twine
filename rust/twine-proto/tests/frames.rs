//! `Netstrings` and `LengthPrefixed` against a real engine.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, Error, Transport};
use twine_proto::{FrameFormat, FrameHandler, LengthPrefixed, NetstringHandler, Netstrings};

struct NsEcho {
    lost: Arc<Mutex<Vec<String>>>,
}
impl NetstringHandler for NsEcho {
    fn string_received(&mut self, t: &mut Transport<'_>, s: &[u8]) {
        Netstrings::<NsEcho>::send_string(t, s);
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, reason: Option<Error>) {
        let m = match reason {
            Some(e) if twine_proto::is_malformed(&e) => "malformed".to_string(),
            r => format!("{r:?}"),
        };
        self.lost.lock().unwrap().push(m);
    }
}

fn ns_wire(s: &[u8]) -> Vec<u8> {
    let mut v = format!("{}:", s.len()).into_bytes();
    v.extend_from_slice(s);
    v.push(b',');
    v
}

#[test]
fn netstring_echo() {
    let lost = Arc::new(Mutex::new(Vec::new()));
    let l2 = lost.clone();
    let srv = Server::start(factory(move |_, _| {
        Box::new(Netstrings::new(NsEcho { lost: l2.clone() }))
    }));
    let mut s = srv.connect();
    let mut rng = Rng::new(11);
    let mut payloads: Vec<Vec<u8>> = vec![vec![], b"hello".to_vec(), vec![b','; 3]];
    payloads.push((0..200_000).map(|i| (i % 251) as u8).collect());
    for _ in 0..50 {
        let n = rng.below(500);
        payloads.push((0..n).map(|_| rng.next() as u8).collect());
    }
    let mut wire = Vec::new();
    let mut want = Vec::new();
    for p in &payloads {
        wire.extend(ns_wire(p));
        want.extend(ns_wire(p));
    }
    let mut w = s.try_clone().unwrap();
    let writer = std::thread::spawn(move || write_chunked(&mut w, &wire, &[1, 7, 1000, 3, 4096]));
    assert_eq!(read_n(&mut s, want.len()), want);
    writer.join().unwrap();
}

#[test]
fn netstring_malformed_and_too_large_abort() {
    for (bad, expect) in [
        (&b"3:abc;"[..], "malformed"),
        (b"x", "malformed"),
        (b"99999999:", "Some(FrameTooLarge)"),
    ] {
        let lost = Arc::new(Mutex::new(Vec::new()));
        let l2 = lost.clone();
        let srv = Server::start(factory(move |_, _| {
            Box::new(Netstrings::new(NsEcho { lost: l2.clone() }).with_max_length(1024))
        }));
        let mut s = srv.connect();
        use std::io::Write;
        s.write_all(&ns_wire(b"fine")).unwrap();
        s.write_all(bad).unwrap();
        assert_eq!(read_to_close(&mut s), ns_wire(b"fine"));
        assert!(wait_until(Duration::from_secs(5), || !lost
            .lock()
            .unwrap()
            .is_empty()));
        assert_eq!(*lost.lock().unwrap(), vec![expect.to_string()]);
    }
}

struct FrameEcho {
    fmt: FrameFormat,
}
impl FrameHandler for FrameEcho {
    fn frame_received(&mut self, t: &mut Transport<'_>, f: &[u8]) {
        self.fmt.send_frame(t, f).unwrap();
    }
}

fn frame_wire(body: &[u8]) -> Vec<u8> {
    let mut v = (body.len() as u32).to_be_bytes().to_vec();
    v.extend_from_slice(body);
    v
}

#[test]
fn length_prefixed_echo_4_byte_big_endian() {
    let fmt = FrameFormat::new();
    let srv = Server::start(factory(move |_, _| {
        Box::new(LengthPrefixed::new(FrameEcho { fmt }))
    }));
    let mut s = srv.connect();
    let mut rng = Rng::new(12);
    let mut bodies: Vec<Vec<u8>> = vec![vec![], b"x".to_vec(), vec![0; 300_000]];
    for _ in 0..80 {
        let n = rng.below(700);
        bodies.push((0..n).map(|_| rng.next() as u8).collect());
    }
    let mut wire = Vec::new();
    for b in &bodies {
        wire.extend(frame_wire(b));
    }
    let mut w = s.try_clone().unwrap();
    let want = wire.clone();
    let writer = std::thread::spawn(move || write_chunked(&mut w, &wire, &[1, 2, 5, 997, 8192]));
    assert_eq!(read_n(&mut s, want.len()), want);
    writer.join().unwrap();
}

struct TooBig {
    fmt: FrameFormat,
    results: Arc<Mutex<Vec<String>>>,
    lost: Arc<Mutex<Vec<String>>>,
}
impl FrameHandler for TooBig {
    fn frame_received(&mut self, t: &mut Transport<'_>, f: &[u8]) {
        let r = self.fmt.send_frame(t, &[7u8; 100]);
        self.results.lock().unwrap().push(format!("{r:?}"));
        self.fmt.send_frame(t, f).unwrap();
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, reason: Option<Error>) {
        self.lost.lock().unwrap().push(format!("{reason:?}"));
    }
}

#[test]
fn send_frame_too_large_errors_and_writes_nothing() {
    let results = Arc::new(Mutex::new(Vec::new()));
    let lost = Arc::new(Mutex::new(Vec::new()));
    let (r2, l2) = (results.clone(), lost.clone());
    let fmt = FrameFormat::new().with_max_frame(16);
    let srv = Server::start(factory(move |_, _| {
        Box::new(LengthPrefixed::with_format(
            TooBig {
                fmt,
                results: r2.clone(),
                lost: l2.clone(),
            },
            fmt,
        ))
    }));
    let mut s = srv.connect();
    use std::io::Write;
    s.write_all(&frame_wire(b"go")).unwrap();
    // Only the small echo arrives; the 100-byte frame was never written.
    assert_eq!(read_n(&mut s, 6), frame_wire(b"go"));
    assert_eq!(
        *results.lock().unwrap(),
        vec!["Err(FrameTooLarge)".to_string()]
    );
    // An incoming oversized frame aborts with FrameTooLarge.
    s.write_all(&frame_wire(&[1u8; 17])).unwrap();
    assert_eq!(read_to_close(&mut s), b"");
    assert!(wait_until(Duration::from_secs(5), || !lost
        .lock()
        .unwrap()
        .is_empty()));
    assert_eq!(
        *lost.lock().unwrap(),
        vec!["Some(FrameTooLarge)".to_string()]
    );
}
