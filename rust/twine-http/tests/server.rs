//! Integration tests: a real `twine::Engine` serving HTTP to `std::net` clients.

mod common;

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use twine_http::{handler, RequestCtx, Server};

fn echo_server() -> Server<impl twine_http::Handler> {
    Server::new(handler(|ctx| {
        let b = ctx.body().to_vec();
        ctx.write(&b);
    }))
}

fn hello() -> Server<impl twine_http::Handler> {
    Server::new(handler(|ctx| ctx.set_body_str("Hello, World!")))
}

#[test]
fn hello_world_headers() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status_line, "HTTP/1.1 200 OK");
    assert_eq!(r.body_str(), "Hello, World!");
    assert_eq!(r.header("content-length"), Some("13"));
    assert_eq!(r.header("server"), Some("twine"));
    assert_eq!(r.header("content-type"), Some("text/plain; charset=utf-8"));
    let d = r.header("date").expect("Date");
    assert!(d.ends_with(" GMT") && d.len() == 29, "{d}");
    assert!(r.header("connection").is_none());
}

#[test]
fn server_name_none_and_custom() {
    let srv = TestServer::start(hello().server_name(None));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert!(r.header("server").is_none());
    assert!(r.header("date").is_some());
}

#[test]
fn post_body_echoed() {
    let srv = TestServer::start(echo_server());
    let mut c = Reader::new(srv.connect());
    c.write(b"POST /e HTTP/1.1\r\nHost: x\r\nContent-Length: 11\r\n\r\nhello world");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.body_str(), "hello world");
}

#[test]
fn chunked_post() {
    let srv = TestServer::start(echo_server());
    let mut c = Reader::new(srv.connect());
    c.write(
        b"POST /e HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n\
          5;ext=1\r\nhello\r\n6\r\n world\r\n0\r\nX-Trailer: 1\r\n\r\n",
    );
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.body_str(), "hello world");
}

#[test]
fn keep_alive_sequence_of_100() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        let p = ctx.path().to_vec();
        ctx.write(&p);
    })));
    let mut c = Reader::new(srv.connect());
    for i in 0..100 {
        c.write(format!("GET /r{i} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes());
        let r = c.read_response().unwrap();
        assert_eq!(r.body_str(), format!("/r{i}"));
    }
}

#[test]
fn sixteen_pipelined_in_order() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        let p = ctx.path().to_vec();
        ctx.write(&p);
    })));
    let mut c = Reader::new(srv.connect());
    let mut req = Vec::new();
    for i in 0..16 {
        req.extend_from_slice(format!("GET /p{i} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes());
    }
    c.write(&req);
    for i in 0..16 {
        assert_eq!(c.read_response().unwrap().body_str(), format!("/p{i}"));
    }
}

#[test]
fn deep_pipeline_exceeds_cap_and_still_answers_all() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        let p = ctx.path().to_vec();
        ctx.write(&p);
    })));
    let mut c = Reader::new(srv.connect());
    let mut req = Vec::new();
    for i in 0..200 {
        req.extend_from_slice(format!("GET /q{i} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes());
    }
    c.write(&req);
    for i in 0..200 {
        assert_eq!(c.read_response().unwrap().body_str(), format!("/q{i}"));
    }
}

#[test]
fn one_byte_writes() {
    let srv = TestServer::start(echo_server());
    let mut c = Reader::new(srv.connect());
    let raw = b"POST /e HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nabcde\
                POST /e HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nfgh\r\n0\r\n\r\n";
    for b in raw.iter() {
        c.write(&[*b]);
    }
    assert_eq!(c.read_response().unwrap().body_str(), "abcde");
    assert_eq!(c.read_response().unwrap().body_str(), "fgh");
}

#[test]
fn header_too_large_431() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    let big = "a".repeat(9 << 10);
    c.write(format!("GET / HTTP/1.1\r\nHost: x\r\nX: {big}\r\n\r\n").as_bytes());
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 431);
    assert_eq!(r.header("connection"), Some("close"));
    assert!(c.read_to_end().is_empty());
}

#[test]
fn body_too_large_413() {
    let srv = TestServer::start(echo_server().max_body_bytes(1024));
    let mut c = Reader::new(srv.connect());
    c.write(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 2000\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 413);
    assert_eq!(r.header("connection"), Some("close"));
}

#[test]
fn chunked_body_too_large_413() {
    let srv = TestServer::start(echo_server().max_body_bytes(8));
    let mut c = Reader::new(srv.connect());
    c.write(b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n5\r\nworld\r\n0\r\n\r\n");
    assert_eq!(c.read_response().unwrap().status, 413);
}

#[test]
fn garbage_400() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(b"\x00\x01garbage\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 400);
    assert_eq!(r.header("connection"), Some("close"));
}

#[test]
fn unsupported_version_505() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/2.0\r\nHost: x\r\n\r\n");
    assert_eq!(c.read_response().unwrap().status, 505);
}

#[test]
fn connection_close_honoured() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.header("connection"), Some("close"));
    assert!(c.read_to_end().is_empty()); // EOF
}

#[test]
fn http10_closes_unless_keep_alive() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.0\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.header("connection"), Some("close"));
    assert!(c.read_to_end().is_empty());

    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.header("connection"), Some("keep-alive"));
    c.write(b"GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n");
    assert_eq!(c.read_response().unwrap().status, 200);
}

#[test]
fn handler_panic_500_and_server_survives() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        if ctx.path() == b"/panic" {
            ctx.write(b"partial");
            panic!("boom");
        }
        ctx.set_body_str("fine");
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET /panic HTTP/1.1\r\nHost: x\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 500);
    assert!(!r.body_str().contains("partial"));
    assert_eq!(r.header("connection"), Some("close"));
    assert!(
        c.read_to_end().is_empty(),
        "pipelined request served after panic"
    );

    let mut c2 = Reader::new(srv.connect());
    c2.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(c2.read_response().unwrap().body_str(), "fine");
}

#[test]
fn request_accessors() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let s2 = seen.clone();
    let srv = TestServer::start(Server::new(move |ctx: &mut RequestCtx<'_>| {
        let mut v = s2.lock().unwrap();
        for b in [
            ctx.method(),
            ctx.path(),
            ctx.raw_query(),
            ctx.request_uri(),
            ctx.version(),
            ctx.host(),
            ctx.header("CONTENT-type").unwrap_or(b""),
            ctx.header("x-missing").unwrap_or(b""),
            ctx.body(),
        ] {
            v.push(String::from_utf8_lossy(b).into_owned());
        }
        let mut names = Vec::new();
        ctx.visit_headers(|n, val| {
            names.push(format!(
                "{}={}",
                String::from_utf8_lossy(n),
                String::from_utf8_lossy(val)
            ))
        });
        v.push(names.join(","));
        v.push(format!(
            "{} {} {:?} {}",
            ctx.is_method("PUT"),
            ctx.is_http11(),
            ctx.content_length(),
            ctx.remote_addr().ip()
        ));
        ctx.set_body_str("ok");
    }));
    let mut c = Reader::new(srv.connect());
    c.write(b"PUT /a%20b/c?x=1&y=2 HTTP/1.1\r\nHost:  example.com:8080 \r\nContent-Type: text/x\r\nContent-Length: 3\r\n\r\nabc");
    c.read_response().unwrap();
    let v = seen.lock().unwrap().join("|");
    assert_eq!(
        v,
        "PUT|/a%20b/c|x=1&y=2|/a%20b/c?x=1&y=2|HTTP/1.1|example.com:8080|text/x||abc|\
         Host=example.com:8080,Content-Type=text/x,Content-Length=3|true true Some(3) 127.0.0.1"
    );
}

#[test]
fn response_headers() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        ctx.set_header("X-A", "1");
        ctx.set_header("x-a", "2");
        ctx.add_header("Set-Cookie", "a=1");
        ctx.add_header("Set-Cookie", "b=2");
        ctx.set_header("Content-Length", "999");
        ctx.set_header("X-Inject", "v\r\nEvil: 1");
        ctx.set_header("Server", "custom");
        ctx.set_header("X-Del", "gone");
        ctx.del_header("x-del");
        ctx.set_content_type("application/json");
        ctx.set_status(201);
        ctx.write_str("{}");
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status_line, "HTTP/1.1 201 Created");
    let h = &r.raw_head;
    for w in [
        "x-a: 2\r\n",
        "Set-Cookie: a=1\r\nSet-Cookie: b=2\r\n",
        "Content-Length: 2\r\n",
        "X-Inject: v  Evil: 1\r\n",
        "Server: custom\r\n",
        "Content-Type: application/json\r\n",
    ] {
        assert!(h.contains(w), "missing {w:?} in {h}");
    }
    for bad in ["X-A: 1", "999", "Server: twine", "\r\nEvil", "gone"] {
        assert!(!h.contains(bad), "unexpected {bad:?} in {h}");
    }
}

#[test]
fn head_and_bodyless_statuses() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        if ctx.path() == b"/204" {
            ctx.set_status(204);
        }
        ctx.write_str("body");
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n");
    c.head_only = true;
    let r = c.read_response().unwrap();
    assert_eq!(r.header("content-length"), Some("4"));
    // HEAD: the body is not sent; the next response starts right after the head.
    c.write(b"GET /204 HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 204);
    assert!(r.header("content-length").is_none());
}

#[test]
fn expect_continue() {
    let srv = TestServer::start(echo_server());
    let mut c = Reader::new(srv.connect());
    c.write(b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status_line, "HTTP/1.1 100 Continue");
    c.write(b"abc");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(r.body_str(), "abc");
}

#[test]
fn large_response_body_writev() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        ctx.set_body(&vec![b'z'; 300_000]);
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n");
    for _ in 0..2 {
        let r = c.read_response().unwrap();
        assert_eq!(r.body.len(), 300_000);
        assert!(r.body.iter().all(|&b| b == b'z'));
    }
}

#[test]
fn keep_alive_disabled() {
    let srv = TestServer::start(hello().keep_alive(false));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.header("connection"), Some("close"));
    assert!(c.read_to_end().is_empty());
}

#[test]
fn slow_client_pipelined_pause_resume() {
    // Large responses to a client that does not read: the outbound ring passes
    // the high-water mark, serving pauses, and nothing is lost once the client
    // drains.
    let served = Arc::new(AtomicUsize::new(0));
    let s2 = served.clone();
    let srv = TestServer::start_with(
        twine::Options::default()
            .loops(1)
            .write_high_water(64 << 10)
            .write_low_water(16 << 10)
            .shutdown_timeout(Duration::from_millis(500)),
        Server::new(handler(move |ctx| {
            let i = s2.fetch_add(1, SeqCst);
            let mut body = vec![b'a' + (i % 26) as u8; 100_000];
            body[..8].copy_from_slice(format!("{i:08}").as_bytes());
            ctx.set_body(&body);
        })),
    );
    const N: usize = 120;
    let mut c = Reader::new(srv.connect());
    let mut req = Vec::new();
    for i in 0..N {
        req.extend_from_slice(format!("GET /{i} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes());
    }
    // Write from another thread so the main thread can stay slow.
    let mut w = c.s.try_clone().unwrap();
    let wt = std::thread::spawn(move || w.write_all(&req).unwrap());
    std::thread::sleep(Duration::from_millis(300));
    // The server must have stopped well short of N while the client is not reading.
    let mid = served.load(SeqCst);
    assert!(
        mid < N,
        "no backpressure: {mid} served before the client read"
    );
    for i in 0..N {
        let r = c.read_response().expect("response lost");
        assert_eq!(r.body.len(), 100_000);
        assert_eq!(&r.body[..8], format!("{i:08}").as_bytes());
        if i % 20 == 0 {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    wt.join().unwrap();
    let mut b = [0u8; 1];
    c.s.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    assert!(c.s.read(&mut b).is_err(), "unexpected extra bytes");
}

#[test]
fn many_connections_concurrently() {
    let srv = TestServer::start_with(
        twine::Options::default()
            .loops(2)
            .shutdown_timeout(Duration::from_millis(500)),
        echo_server(),
    );
    let addr = srv.addr;
    let ts: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                let s = std::net::TcpStream::connect(addr).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                let mut c = Reader::new(s);
                for i in 0..50 {
                    let body = format!("t{t}-{i}");
                    c.write(
                        format!(
                            "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    );
                    assert_eq!(c.read_response().unwrap().body_str(), body);
                }
            })
        })
        .collect();
    for t in ts {
        t.join().unwrap();
    }
}

#[test]
fn date_header_refreshes() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let d1 = c
        .read_response()
        .unwrap()
        .header("date")
        .unwrap()
        .to_string();
    std::thread::sleep(Duration::from_millis(1100));
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let d2 = c
        .read_response()
        .unwrap()
        .header("date")
        .unwrap()
        .to_string();
    assert_ne!(d1, d2, "Date did not advance across a second");
}

#[test]
fn error_discards_pipelined_followers() {
    let srv = TestServer::start(hello());
    let mut c = Reader::new(srv.connect());
    c.write(
        b"GET / HTTP/1.1\r\nHost: x\r\n\r\nBAD REQUEST\r\n\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n",
    );
    assert_eq!(c.read_response().unwrap().status, 200);
    assert_eq!(c.read_response().unwrap().status, 400);
    assert!(c.read_to_end().is_empty());
}

#[test]
fn peer_closing_while_serving_is_paused() {
    // Fill the ring so the connection pauses, then drop the client: the server
    // must clean up and keep serving others.
    let srv = TestServer::start_with(
        twine::Options::default()
            .loops(1)
            .write_high_water(32 << 10)
            .write_low_water(8 << 10)
            .shutdown_timeout(Duration::from_millis(500)),
        Server::new(handler(|ctx| ctx.set_body(&vec![b'q'; 200_000]))),
    );
    {
        let mut s = srv.connect();
        let mut req = Vec::new();
        for _ in 0..50 {
            req.extend_from_slice(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        }
        s.write_all(&req).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    } // dropped with unread data: RST
    std::thread::sleep(Duration::from_millis(100));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert_eq!(c.read_response().unwrap().body.len(), 200_000);
}

#[test]
fn idle_timeout_closes() {
    let srv = TestServer::start(hello().idle_timeout(Some(Duration::from_millis(100))));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    c.read_response().unwrap();
    assert!(c.read_to_end().is_empty()); // EOF after the idle timeout
}

#[test]
fn listen_via_factory_and_two_servers() {
    // `factory` composes with the engine directly.
    let eng = Arc::new(
        twine::Engine::new(
            twine::Options::default()
                .loops(1)
                .shutdown_timeout(Duration::from_millis(300)),
        )
        .unwrap(),
    );
    let f = Server::new(handler(|ctx| ctx.set_body_str("a"))).factory(&eng.handle());
    let l1 = eng
        .listen_tcp("127.0.0.1:0", f.clone(), twine::ListenOptions::default())
        .unwrap();
    let l2 = eng
        .listen_tcp("127.0.0.1:0", f, twine::ListenOptions::default())
        .unwrap();
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    for l in [&l1, &l2] {
        let s = std::net::TcpStream::connect(l.addr()).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut c = Reader::new(s);
        c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        assert_eq!(c.read_response().unwrap().body_str(), "a");
    }
    eng.stop();
    j.join().unwrap().unwrap();
}

// ---- handler Connection header, header name/value sanitising ----

#[test]
fn handler_connection_close_token_in_list() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        ctx.set_header("Connection", "Foo, Close");
        ctx.set_body_str("x");
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.header("connection"), Some("close"));
    assert!(c.read_to_end().is_empty());
}

#[test]
fn handler_keep_alive_on_http10() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        if ctx.path() == b"/ka" {
            ctx.set_header("Connection", "keep-alive");
        }
        ctx.set_body_str("x");
    })));
    // Without the handler's say-so an HTTP/1.0 request closes.
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.0\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.header("connection"), Some("close"));
    // With it the connection stays open for another request.
    let mut c = Reader::new(srv.connect());
    for _ in 0..2 {
        c.write(b"GET /ka HTTP/1.0\r\n\r\n");
        let r = c.read_response().unwrap();
        assert_eq!(r.header("connection"), Some("keep-alive"));
        assert_eq!(r.body_str(), "x");
    }
    // An explicit close wins over keep-alive.
    let srv = TestServer::start(Server::new(handler(|ctx| {
        ctx.set_header("Connection", "keep-alive, close");
        ctx.set_body_str("x");
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.header("connection"), Some("close"));
    assert!(c.read_to_end().is_empty());
}

#[test]
fn response_header_names_and_values_are_sanitised() {
    let srv = TestServer::start(Server::new(handler(|ctx| {
        ctx.set_header("Bad Name", "v");
        ctx.add_header("X:Colon", "v");
        ctx.set_header("", "v");
        ctx.set_header("X-Ok", "a\x01b\x7fc\td\r\nInjected: 1");
        ctx.set_body_str("x");
    })));
    let mut c = Reader::new(srv.connect());
    c.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    let r = c.read_response().unwrap();
    assert_eq!(r.status, 200);
    assert!(r.header("bad name").is_none() && r.header("x:colon").is_none());
    assert!(!r.raw_head.contains("Bad Name") && !r.raw_head.contains("Colon"));
    assert_eq!(r.header("x-ok"), Some("abc\td  Injected: 1"));
    assert!(r.header("injected").is_none());
}
