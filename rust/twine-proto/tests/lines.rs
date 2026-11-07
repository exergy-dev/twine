//! `Lines` against a real engine.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, Error, Transport};
use twine_proto::{Flow, LineHandler, Lines};

struct Echo;
impl LineHandler for Echo {
    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow {
        Lines::<Echo>::send_line(t, line);
        Flow::Continue
    }
}

#[test]
fn echo_with_odd_chunk_sizes() {
    let srv = Server::start(factory(|_, _| Box::new(Lines::new(Echo))));
    let mut rng = Rng::new(5);
    let mut clients = Vec::new();
    for c in 0..8u64 {
        let addr = srv.addr;
        let seed = rng.next() ^ c;
        clients.push(std::thread::spawn(move || {
            let mut rng = Rng::new(seed);
            let s = std::net::TcpStream::connect(addr).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
            s.set_nodelay(true).unwrap();
            let lines: Vec<String> = (0..200)
                .map(|i| format!("line {i} {}", "x".repeat(rng.below(300))))
                .collect();
            let mut wire = Vec::new();
            for l in &lines {
                wire.extend_from_slice(l.as_bytes());
                wire.extend_from_slice(b"\r\n");
            }
            let mut w = s.try_clone().unwrap();
            let sizes = [1, 3, 7, 2, 61, 5];
            let writer = std::thread::spawn(move || write_chunked(&mut w, &wire, &sizes));
            let mut r = BufReader::new(s);
            for l in &lines {
                let mut got = String::new();
                r.read_line(&mut got).unwrap();
                assert_eq!(got, format!("{l}\r\n"));
            }
            writer.join().unwrap();
        }));
    }
    for c in clients {
        c.join().unwrap();
    }
}

struct Recorder {
    lost: Arc<Mutex<Vec<String>>>,
}
impl LineHandler for Recorder {
    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow {
        Lines::<Recorder>::send_line(t, line);
        Flow::Continue
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, reason: Option<Error>) {
        self.lost.lock().unwrap().push(format!("{reason:?}"));
    }
}

#[test]
fn too_long_line_aborts_with_line_too_long() {
    let lost = Arc::new(Mutex::new(Vec::new()));
    let l2 = lost.clone();
    let srv = Server::start(factory(move |_, _| {
        Box::new(Lines::new(Recorder { lost: l2.clone() }).with_max_line_length(32))
    }));
    let mut s = srv.connect();
    // A good line first, then an endless one that never sends a delimiter.
    s.write_all(b"ok\r\n").unwrap();
    s.write_all(&[b'x'; 100]).unwrap();
    let got = read_to_close(&mut s);
    assert_eq!(got, b"ok\r\n");
    assert!(wait_until(Duration::from_secs(5), || !lost
        .lock()
        .unwrap()
        .is_empty()));
    assert_eq!(*lost.lock().unwrap(), vec!["Some(LineTooLong)".to_string()]);
}

struct RawSwitch;
impl LineHandler for RawSwitch {
    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow {
        if line == b"RAW" {
            Lines::<RawSwitch>::send_line(t, b"OK");
            Flow::RawMode
        } else {
            Lines::<RawSwitch>::send_line(t, line);
            Flow::Continue
        }
    }
    fn raw_data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) -> Flow {
        t.write(data);
        Flow::Continue
    }
}

#[test]
fn raw_mode_switch_then_echo_raw_bytes() {
    let srv = Server::start(factory(|_, _| Box::new(Lines::new(RawSwitch))));
    let mut s = srv.connect();
    // Everything after "RAW\r\n" (even in the same segment) is raw, delimiters included.
    let wire = b"hello\r\nRAW\r\nabc\r\ndef\x00\xff";
    write_chunked(&mut s, wire, &[4, 1, 9, 3]);
    let want = b"hello\r\nOK\r\nabc\r\ndef\x00\xff";
    assert_eq!(read_n(&mut s, want.len()), want);
    write_chunked(&mut s, b"more\r\n", &[2]);
    assert_eq!(read_n(&mut s, 6), b"more\r\n");
}

struct Closer;
impl LineHandler for Closer {
    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow {
        Lines::<Closer>::send_line(t, line);
        if line == b"quit" {
            Flow::Close
        } else {
            Flow::Continue
        }
    }
}

#[test]
fn flow_close_stops_parsing_and_closes_gracefully() {
    let srv = Server::start(factory(|_, _| Box::new(Lines::new(Closer))));
    let mut s = srv.connect();
    s.write_all(b"a\r\nquit\r\nnever\r\n").unwrap();
    assert_eq!(read_to_close(&mut s), b"a\r\nquit\r\n");
}

struct LfEcho {
    delim: Lines<Echo>,
}
impl LineHandler for LfEcho {
    fn line_received(&mut self, t: &mut Transport<'_>, line: &[u8]) -> Flow {
        self.delim.send_line_with(t, line);
        Flow::Continue
    }
}

#[test]
fn custom_delimiter_and_send_line_with() {
    let srv = Server::start(factory(|_, _| {
        Box::new(
            Lines::new(LfEcho {
                delim: Lines::new(Echo).with_delimiter(b"\n"),
            })
            .with_delimiter(b"\n"),
        )
    }));
    let mut s = srv.connect();
    write_chunked(&mut s, b"one\ntwo\r\n\nx", &[3]);
    assert_eq!(read_n(&mut s, 10), b"one\ntwo\r\n\n");
}
