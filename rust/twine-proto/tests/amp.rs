//! AMP against a real engine, plus property tests for the box codec.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use twine::{factory, ConnId, Deferred, DialOptions, Error, LocalResolver, Loop, Transport};
use twine_proto::amp::MALFORMED_ERROR_CODE;
use twine_proto::amp::UNHANDLED_ERROR_CODE;
use twine_proto::{
    arg_str, is_malformed, remote_error, Amp, AmpBox, AmpHandler, AmpParser, AmpReply, Args, Flow,
};

fn args(kv: &[(&str, &str)]) -> Args {
    twine_proto::args(kv.iter().copied())
}

fn num(a: &Args, k: &str) -> i64 {
    arg_str(a, k).unwrap().parse().unwrap()
}

#[derive(Default)]
struct Log {
    notes: Mutex<Vec<String>>,
    lost: Mutex<Vec<String>>,
}

struct Srv {
    log: Arc<Log>,
    hung: Vec<LocalResolver<Args>>,
}

fn lost_str(r: &Option<Error>) -> String {
    match r {
        None => "clean".into(),
        Some(e) if is_malformed(e) => "malformed".into(),
        Some(Error::FrameTooLarge) => "toolarge".into(),
        Some(e) => format!("{e:?}"),
    }
}

impl AmpHandler for Srv {
    fn handle(&mut self, t: &mut Transport<'_>, cmd: &str, a: Args) -> AmpReply {
        match cmd {
            "Sum" => {
                let (x, y) = (num(&a, "a"), num(&a, "b"));
                AmpReply::Answer(args(&[("total", &(x + y).to_string())]))
            }
            "Coded" => AmpReply::error("ZERO_DIVISION", "divide by zero"),
            "Slow" => {
                let d = t
                    .loop_mut()
                    .sleep(Duration::from_millis(20))
                    .then(|_, _| args(&[("slept", "yes")]));
                AmpReply::Deferred(d)
            }
            "Echo" => {
                let i = num(&a, "i") as u64;
                let ms = num(&a, "ms") as u64;
                let d = t
                    .loop_mut()
                    .sleep(Duration::from_millis(ms))
                    .then(move |_, _| args(&[("i", &i.to_string())]));
                AmpReply::Deferred(d)
            }
            "Reject" => {
                let d = Deferred::rejected(t.loop_mut(), Error::Closed);
                AmpReply::Deferred(d)
            }
            "Note" => {
                let msg = arg_str(&a, "msg").unwrap().to_owned();
                self.log.notes.lock().unwrap().push(msg);
                AmpReply::Answer(Args::new())
            }
            "Hang" => {
                let (d, r) = t.loop_mut().deferred_local::<Args>();
                self.hung.push(r);
                AmpReply::Deferred(d)
            }
            "Die" => {
                t.abort();
                AmpReply::Answer(Args::new())
            }
            "Panic" => panic!("handler exploded"),
            "Huge" => AmpReply::Answer(args(&[("v", &"x".repeat(70_000))])),
            // Echoes the keys the handler saw and the raw bytes of "bin".
            "Keys" => {
                let keys = a.keys().cloned().collect::<Vec<_>>().join(",");
                let mut r = args(&[("keys", &keys)]);
                if let Some(b) = a.get("bin") {
                    r.insert("bin".into(), b.clone());
                }
                AmpReply::Answer(r)
            }
            _ => AmpReply::unhandled(cmd),
        }
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        self.log.lost.lock().unwrap().push(lost_str(&r));
    }
}

struct Cli {
    lost: Arc<Mutex<Vec<String>>>,
}
impl AmpHandler for Cli {
    fn handle(&mut self, _t: &mut Transport<'_>, cmd: &str, _a: Args) -> AmpReply {
        AmpReply::unhandled(cmd)
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, r: Option<Error>) {
        self.lost.lock().unwrap().push(lost_str(&r));
    }
}

fn start_server(rig: &Rig) -> (std::net::SocketAddr, Arc<Log>) {
    let log = Arc::new(Log::default());
    let l2 = log.clone();
    let lst = rig.listen(
        "127.0.0.1:0",
        factory(move |_, _| {
            Box::new(Amp::new(Srv {
                log: l2.clone(),
                hung: Vec::new(),
            }))
        }),
    );
    (lst.addr(), log)
}

/// Connects an AMP client on loop 0; returns the conn id and its lost log.
fn connect_client(rig: &Rig, addr: std::net::SocketAddr) -> (ConnId, Arc<Mutex<Vec<String>>>) {
    let lost = Arc::new(Mutex::new(Vec::new()));
    let l2 = lost.clone();
    let id = rig
        .lh(0)
        .submit(move |lp| {
            lp.connect_tcp(
                addr,
                move |_, _| Box::new(Amp::new(Cli { lost: l2 })),
                DialOptions::default(),
            )
        })
        .wait()
        .expect("connect");
    (id, lost)
}

fn call(rig: &Rig, id: ConnId, cmd: &'static str, a: Args) -> Result<Args, Error> {
    rig.lh(0)
        .submit(move |lp| Amp::<Cli>::call_on(lp, id, cmd, a))
        .wait()
}

#[test]
fn sum_roundtrip() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    let r = call(&rig, id, "Sum", args(&[("a", "13"), ("b", "29")])).unwrap();
    assert_eq!(r, args(&[("total", "42")]));
    let r = call(&rig, id, "Sum", args(&[("a", "-5"), ("b", "2")])).unwrap();
    assert_eq!(arg_str(&r, "total"), Some("-3"));
}

#[test]
fn remote_errors() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    let e = call(&rig, id, "Coded", Args::new()).unwrap_err();
    let re = remote_error(&e).expect("RemoteError");
    assert_eq!(re.code, "ZERO_DIVISION");
    assert_eq!(re.description, "divide by zero");
    let e = call(&rig, id, "Nope", Args::new()).unwrap_err();
    let re = remote_error(&e).unwrap();
    assert_eq!(re.code, UNHANDLED_ERROR_CODE);
    assert_eq!(re.description, "Unhandled Command: \"Nope\"");
    // A rejected Deferred reply becomes UNKNOWN with the error text.
    let e = call(&rig, id, "Reject", Args::new()).unwrap_err();
    assert_eq!(remote_error(&e).unwrap().code, "UNKNOWN");
    // A panicking handler is UNKNOWN and the connection survives.
    let e = call(&rig, id, "Panic", Args::new()).unwrap_err();
    let re = remote_error(&e).unwrap();
    assert_eq!(re.code, "UNKNOWN");
    assert_eq!(re.description, "internal error", "panic text must not leak");
    // An over-long answer is turned into an UNKNOWN error, not a bad box.
    let e = call(&rig, id, "Huge", Args::new()).unwrap_err();
    assert_eq!(remote_error(&e).unwrap().code, "UNKNOWN");
    let r = call(&rig, id, "Sum", args(&[("a", "1"), ("b", "1")])).unwrap();
    assert_eq!(arg_str(&r, "total"), Some("2"));
}

#[test]
fn bad_call_args() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    for a in [
        args(&[("_ask", "1")]),
        args(&[("", "v")]),
        args(&[(&"k".repeat(256), "v")]),
        args(&[("k", &"v".repeat(65536))]),
    ] {
        let e = call(&rig, id, "Sum", a).unwrap_err();
        assert!(matches!(&e, Error::Io(io) if io.kind() == std::io::ErrorKind::InvalidInput));
    }
    assert!(call(&rig, id, "Sum", args(&[("a", "1"), ("b", "2")])).is_ok());
}

#[test]
fn bad_command_names_are_rejected_not_truncated() {
    let rig = Rig::start(1);
    let (addr, log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    // The &'static str signature of `call` is only a test helper convenience.
    let long: &'static str = Box::leak("c".repeat(65536).into_boxed_str());
    for cmd in ["", long] {
        let e = call(&rig, id, cmd, Args::new()).unwrap_err();
        assert!(matches!(&e, Error::Io(io) if io.kind() == std::io::ErrorKind::InvalidInput));
    }
    let r = rig
        .lh(0)
        .call(move |lp| {
            lp.with_protocol::<Amp<Cli>, _>(id, |a, t| {
                (
                    a.call_no_answer(t, "", Args::new()),
                    a.call_no_answer(t, long, Args::new()),
                    a.pending(),
                )
            })
            .unwrap()
        })
        .wait()
        .unwrap();
    for e in [r.0, r.1] {
        assert!(matches!(&e, Err(Error::Io(io)) if io.kind() == std::io::ErrorKind::InvalidInput));
    }
    assert_eq!(r.2, 0);
    // The longest legal name still goes out (and is unhandled, not truncated).
    let ok: &'static str = Box::leak("c".repeat(65535).into_boxed_str());
    let e = call(&rig, id, ok, Args::new()).unwrap_err();
    assert_eq!(remote_error(&e).unwrap().code, UNHANDLED_ERROR_CODE);
    assert!(log.notes.lock().unwrap().is_empty());
}

#[test]
fn binary_values_and_reserved_keys_are_stripped() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    // A raw request carrying every reserved key plus a non-UTF-8 value.
    let mut wire = Vec::new();
    for (k, v) in [
        (&b"_ask"[..], &b"t"[..]),
        (b"_command", b"Keys"),
        (b"_error_code", b"X"),
        (b"_error_description", b"Y"),
        (b"bin", &[0xff, 0xfe, 0x00, 0x80]),
        (b"plain", b"p"),
    ] {
        wire.extend_from_slice(&(k.len() as u16).to_be_bytes());
        wire.extend_from_slice(k);
        wire.extend_from_slice(&(v.len() as u16).to_be_bytes());
        wire.extend_from_slice(v);
    }
    wire.extend_from_slice(&[0, 0]);
    s.write_all(&wire).unwrap();
    let b = &read_boxes(&mut s, 1)[0];
    assert_eq!(arg_str(b, "_answer"), Some("t"));
    assert_eq!(arg_str(b, "keys"), Some("bin,plain"));
    assert_eq!(b["bin"], vec![0xff, 0xfe, 0x00, 0x80]);
}

#[test]
fn bad_key_box_gets_malformed_error_and_connection_survives() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut wire = Vec::new();
    wire.extend_from_slice(b"\x00\x04_ask\x00\x01q");
    wire.extend_from_slice(&[0, 1, 0xff, 0, 1, b'v', 0, 0]);
    // A bad box without _ask is silently dropped; a good one still works.
    wire.extend_from_slice(&[0, 1, 0xff, 0, 1, b'v', 0, 0]);
    wire.extend(twisted_box(&[
        "_ask", "z", "_command", "Sum", "a", "1", "b", "2",
    ]));
    s.write_all(&wire).unwrap();
    let boxes = read_boxes(&mut s, 2);
    assert_eq!(arg_str(&boxes[0], "_error"), Some("q"));
    assert_eq!(
        arg_str(&boxes[0], "_error_code"),
        Some(MALFORMED_ERROR_CODE)
    );
    assert_eq!(arg_str(&boxes[1], "total"), Some("3"));
}

#[test]
fn too_many_pairs_is_rejected_per_box() {
    let rig = Rig::start(1);
    let log = Arc::new(Log::default());
    let l2 = log.clone();
    let lst = rig.listen(
        "127.0.0.1:0",
        factory(move |_, _| {
            Box::new(
                Amp::new(Srv {
                    log: l2.clone(),
                    hung: Vec::new(),
                })
                .max_pairs(4),
            )
        }),
    );
    let mut s = std::net::TcpStream::connect(lst.addr()).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    // _ask + _command + 3 extras = 5 pairs > 4.
    let mut wire = twisted_box(&["_ask", "a", "_command", "Sum", "x", "1", "y", "2", "z", "3"]);
    // Exactly 4 pairs is fine.
    wire.extend(twisted_box(&[
        "_ask", "b", "_command", "Sum", "a", "1", "b", "2",
    ]));
    s.write_all(&wire).unwrap();
    let boxes = read_boxes(&mut s, 2);
    assert_eq!(
        arg_str(&boxes[0], "_error_code"),
        Some(MALFORMED_ERROR_CODE)
    );
    assert_eq!(arg_str(&boxes[1], "total"), Some("3"));
}

#[test]
fn deferred_reply_after_sleep() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    let t0 = Instant::now();
    let r = call(&rig, id, "Slow", Args::new()).unwrap();
    assert_eq!(arg_str(&r, "slept"), Some("yes"));
    assert!(t0.elapsed() >= Duration::from_millis(18));
}

#[test]
fn call_no_answer_runs_handler() {
    let rig = Rig::start(1);
    let (addr, log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    rig.lh(0)
        .call(move |lp| {
            lp.with_protocol::<Amp<Cli>, _>(id, |a, t| {
                a.call_no_answer(t, "Note", args(&[("msg", "hi")])).unwrap();
                assert_eq!(a.pending(), 0);
            })
            .unwrap()
        })
        .wait()
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        log.notes.lock().unwrap().as_slice() == ["hi"]
    }));
    // The connection still works and no stray reply confused the client.
    assert!(call(&rig, id, "Sum", args(&[("a", "1"), ("b", "2")])).is_ok());
}

#[test]
fn connection_loss_rejects_pending() {
    let rig = Rig::start(1);
    let (addr, log) = start_server(&rig);
    let (id, lost) = connect_client(&rig, addr);
    let res = rig
        .lh(0)
        .submit(move |lp| {
            let d = Amp::<Cli>::call_on(lp, id, "Hang", Args::new());
            let _ = lp.with_protocol::<Amp<Cli>, _>(id, |a, t| {
                assert_eq!(a.pending(), 1);
                a.call_no_answer(t, "Die", Args::new())
            });
            d
        })
        .wait();
    assert!(matches!(res, Err(Error::Closed)), "{res:?}");
    assert!(wait_until(Duration::from_secs(5), || {
        !lost.lock().unwrap().is_empty() && !log.lost.lock().unwrap().is_empty()
    }));
    // Calls after the loss fail immediately.
    let r = call(&rig, id, "Sum", Args::new());
    assert!(matches!(r, Err(Error::Closed)), "{r:?}");
}

#[test]
fn hundred_concurrent_calls_map_to_the_right_ask() {
    let rig = Rig::start(1);
    let (addr, _log) = start_server(&rig);
    let (id, _) = connect_client(&rig, addr);
    let n = 100u64;
    let got = rig
        .lh(0)
        .submit(move |lp: &mut Loop| {
            let mut v = Vec::new();
            for i in 0..n {
                // Answers come back in a shuffled order: delay varies per call.
                let ms = (i * 37) % 23;
                v.push(Amp::<Cli>::call_on(
                    lp,
                    id,
                    "Echo",
                    args(&[("i", &i.to_string()), ("ms", &ms.to_string())]),
                ));
            }
            let pending = lp
                .with_protocol::<Amp<Cli>, _>(id, |a, _| a.pending())
                .unwrap();
            assert_eq!(pending, n as usize);
            twine::all(lp, v)
        })
        .wait()
        .unwrap();
    for (i, a) in got.iter().enumerate() {
        assert_eq!(
            arg_str(a, "i"),
            Some(i.to_string().as_str()),
            "call {i} got the wrong answer"
        );
    }
    let p = rig
        .lh(0)
        .call(move |lp| lp.with_protocol::<Amp<Cli>, _>(id, |a, _| a.pending()))
        .wait()
        .unwrap()
        .unwrap();
    assert_eq!(p, 0);
}

fn twisted_box(kv: &[&str]) -> Vec<u8> {
    let mut b = Vec::new();
    for s in kv {
        b.extend_from_slice(&(s.len() as u16).to_be_bytes());
        b.extend_from_slice(s.as_bytes());
    }
    b.extend_from_slice(&[0, 0]);
    b
}

fn read_boxes(s: &mut std::net::TcpStream, n: usize) -> Vec<Args> {
    use std::io::Read;
    let mut p = AmpParser::new();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while out.len() < n {
        let k = s.read(&mut buf).expect("read");
        assert!(k > 0, "eof");
        p.feed(&buf[..k], &mut |a| {
            out.push(a);
            Flow::Continue
        })
        .unwrap();
    }
    out
}

#[test]
fn twisted_wire_requests() {
    let rig = Rig::start(1);
    let (addr, log) = start_server(&rig);
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut wire = twisted_box(&["_ask", "a", "_command", "Sum", "a", "13", "b", "29"]);
    wire.extend(twisted_box(&["_ask", "b", "_command", "Nope"]));
    wire.extend(twisted_box(&["_command", "Note", "msg", "no-ask"])); // requiresAnswer=False
    wire.extend(twisted_box(&["_ask", "d", "_command", "Coded"]));
    write_chunked(&mut s, &wire, &[1, 5, 3, 64]);
    let boxes = read_boxes(&mut s, 3);
    assert_eq!(boxes[0], args(&[("_answer", "a"), ("total", "42")]));
    assert_eq!(
        boxes[1],
        args(&[
            ("_error", "b"),
            ("_error_code", "UNHANDLED"),
            ("_error_description", "Unhandled Command: \"Nope\"")
        ])
    );
    assert_eq!(
        boxes[2],
        args(&[
            ("_error", "d"),
            ("_error_code", "ZERO_DIVISION"),
            ("_error_description", "divide by zero")
        ])
    );
    assert_eq!(log.notes.lock().unwrap().as_slice(), ["no-ask"]);
}

#[test]
fn malformed_and_oversize_abort() {
    let rig = Rig::start(1);
    let (addr, log) = start_server(&rig);
    // Key longer than 255 cannot be framed.
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.write_all(&[0x01, 0x00]).unwrap();
    assert!(read_to_close(&mut s).is_empty());
    // Box over the 1 MiB default (many 60000-byte values).
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    let big = "x".repeat(60_000);
    let mut wire = Vec::new();
    for i in 0..30 {
        // Pairs only, never terminating the box.
        let k = format!("k{i}");
        wire.extend_from_slice(&(k.len() as u16).to_be_bytes());
        wire.extend_from_slice(k.as_bytes());
        wire.extend_from_slice(&(big.len() as u16).to_be_bytes());
        wire.extend_from_slice(big.as_bytes());
    }
    let _ = s.write_all(&wire);
    assert!(read_to_close(&mut s).is_empty());
    assert!(wait_until(Duration::from_secs(5), || {
        log.lost.lock().unwrap().len() == 2
    }));
    let mut l = log.lost.lock().unwrap().clone();
    l.sort();
    assert_eq!(l, ["toolarge", "toolarge"]);
}

// ---- properties -----------------------------------------------------------

fn rand_string(rng: &mut Rng, max_bytes: usize, min_chars: usize) -> String {
    const POOL: [char; 8] = ['a', 'Z', '0', '_', ' ', 'é', '€', '😀'];
    let mut s = String::new();
    let n = rng.range(min_chars, min_chars + 40);
    for _ in 0..n {
        let c = if rng.below(3) == 0 {
            POOL[rng.below(POOL.len())]
        } else {
            (b'a' + rng.below(26) as u8) as char
        };
        if s.len() + c.len_utf8() > max_bytes {
            break;
        }
        s.push(c);
    }
    s
}

#[test]
fn property_round_trip_random_boxes() {
    let mut rng = Rng::new(0xA11CE);
    for round in 0..300 {
        let mut boxes = Vec::new();
        let mut wire = Vec::new();
        for _ in 0..rng.range(1, 6) {
            let mut b = AmpBox::new();
            for _ in 0..rng.below(8) {
                let mut k = rand_string(&mut rng, 255, 1);
                if k.is_empty() {
                    k.push('k');
                }
                let v = match rng.below(10) {
                    0 => "z".repeat(rng.range(1000, 65535)),
                    1 => String::new(),
                    _ => rand_string(&mut rng, 65535, 0),
                };
                b.insert(k, v);
            }
            b.encode_into(&mut wire).unwrap();
            boxes.push(b.args);
        }
        // Random chunking.
        let mut p = AmpParser::new();
        let mut got = Vec::new();
        let mut i = 0;
        while i < wire.len() {
            let n = rng.range(1, 700).min(wire.len() - i);
            p.feed(&wire[i..i + n], &mut |a| {
                got.push(a);
                Flow::Continue
            })
            .unwrap();
            i += n;
        }
        assert_eq!(got, boxes, "round {round}");
        assert_eq!(p.buffered(), 0);
    }
}

#[test]
fn property_random_garbage_never_panics() {
    let mut rng = Rng::new(99);
    for _ in 0..2000 {
        let n = rng.below(300);
        let data: Vec<u8> = (0..n)
            .map(|_| {
                if rng.below(3) == 0 {
                    0
                } else {
                    rng.next() as u8
                }
            })
            .collect();
        let mut p = AmpParser::with_max_box_size(512);
        let mut i = 0;
        while i < data.len() {
            let k = rng.range(1, 17).min(data.len() - i);
            if p.feed(&data[i..i + k], &mut |_| Flow::Continue).is_err() {
                assert!(p.is_stopped());
                break;
            }
            i += k;
        }
    }
}
