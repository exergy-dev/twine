//! Watchdog, metrics counters, Prometheus rendering and panic accounting.

mod common;

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use common::*;
use twine::{factory, ListenOptions, MetricsSnapshot, Options, Protocol, Transport};

struct Echo;
impl Protocol for Echo {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

/// Echoes after busy-waiting (or sleeping) for `dur` inside `data_received`.
struct Slow {
    dur: Duration,
    spin: bool,
}
impl Protocol for Slow {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if self.spin {
            let start = Instant::now();
            while start.elapsed() < self.dur {
                std::hint::spin_loop();
            }
        } else {
            std::thread::sleep(self.dur);
        }
        t.write(data);
    }
}

struct Boom;
impl Protocol for Boom {
    fn data_received(&mut self, _t: &mut Transport<'_>, _data: &mut [u8]) {
        panic!("observability test panic");
    }
}

fn total(srv: &Server, f: impl Fn(&MetricsSnapshot) -> u64) -> u64 {
    srv.eng.metrics().iter().map(f).sum()
}

fn ping(s: &mut std::net::TcpStream) {
    s.write_all(b"x").unwrap();
    assert_eq!(read_n(s, 1), b"x");
}

#[test]
fn watchdog_detects_a_slow_callback() {
    let opts = Options::default()
        .loops(1)
        .watchdog(Some(Duration::from_millis(1)));
    let mut srv = Server::start_with(
        opts,
        ListenOptions::default(),
        factory(|_, _| {
            Box::new(Slow {
                dur: Duration::from_millis(5),
                spin: false,
            })
        }),
    );
    // `Engine::run` starts the watchdog itself.
    let mut c = srv.connect();
    for _ in 0..5 {
        ping(&mut c);
    }
    assert!(
        wait_until(Duration::from_secs(5), || total(&srv, |m| m.slow_callbacks)
            >= 1),
        "5 ms sleeps must be reported at a 1 ms threshold"
    );
    drop(c);
    srv.stop();
}

#[test]
fn watchdog_ignores_a_fast_callback() {
    // 100 us callbacks against a 25 ms threshold (250x margin) so a scheduling
    // hiccup on a shared CI machine cannot flake the test.
    let opts = Options::default()
        .loops(1)
        .watchdog(Some(Duration::from_millis(25)));
    let mut srv = Server::start_with(
        opts,
        ListenOptions::default(),
        factory(|_, _| {
            Box::new(Slow {
                dur: Duration::from_micros(100),
                spin: true,
            })
        }),
    );
    let mut c = srv.connect();
    for _ in 0..200 {
        ping(&mut c);
    }
    assert_eq!(total(&srv, |m| m.slow_callbacks), 0);
    drop(c);
    srv.stop();
}

#[test]
fn watchdog_none_starts_nothing() {
    let opts = Options::default().loops(1).watchdog(None);
    let mut srv = Server::start_with(
        opts,
        ListenOptions::default(),
        factory(|_, _| {
            Box::new(Slow {
                dur: Duration::from_millis(5),
                spin: false,
            })
        }),
    );
    // `Engine::run` started none, and asking for one with the watchdog disabled
    // yields none.
    assert!(twine::watchdog::spawn(&srv.eng.handle()).is_none());
    let mut c = srv.connect();
    for _ in 0..4 {
        ping(&mut c);
    }
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(total(&srv, |m| m.slow_callbacks), 0);
    drop(c);
    srv.stop();
}

#[test]
fn counters_after_an_echo_session() {
    let mut srv = Server::start(2, factory(|_, _| Box::new(Echo)));
    let posted = 50u64;
    let lhs = srv.eng.loop_handles();
    for i in 0..posted {
        lhs[(i % 2) as usize].call_from_thread(|_| {}).unwrap();
    }
    let n_clients = 6;
    let mut clients: Vec<_> = (0..n_clients).map(|_| srv.connect()).collect();
    for round in 0..20 {
        for c in clients.iter_mut() {
            let msg = vec![round as u8; 1000 + round];
            c.write_all(&msg).unwrap();
            let mut got = vec![0u8; msg.len()];
            c.read_exact(&mut got).unwrap();
            assert_eq!(got, msg);
        }
    }
    drop(clients);
    assert!(
        wait_until(Duration::from_secs(10), || {
            total(&srv, |m| m.connections) == 0 && total(&srv, |m| m.closes) == n_clients as u64
        }),
        "all connections must close: {:?}",
        srv.eng.metrics()
    );
    let m = srv.eng.metrics();
    let sum = |f: fn(&MetricsSnapshot) -> u64| m.iter().map(f).sum::<u64>();
    assert_eq!(sum(|s| s.accepts), n_clients as u64);
    assert_eq!(sum(|s| s.accepts), sum(|s| s.closes));
    assert_eq!(sum(|s| s.connections), 0);
    assert_eq!(sum(|s| s.read_bytes), sum(|s| s.write_bytes));
    assert!(sum(|s| s.write_direct_hit) > 0);
    assert!(sum(|s| s.iterations) > 0);
    assert!(sum(|s| s.tasks_run) >= posted);
    assert!(sum(|s| s.reads) > 0);
    srv.stop();
}

#[test]
fn prometheus_render_has_expected_series() {
    let mut srv = Server::start(2, factory(|_, _| Box::new(Echo)));
    let mut c = srv.connect();
    ping(&mut c);
    drop(c);
    assert!(wait_until(Duration::from_secs(5), || total(&srv, |m| m
        .closes)
        == 1));
    let out = twine::prometheus::render_engine(&srv.eng.handle());
    for l in 0..2 {
        for series in [
            "twine_accepts_total",
            "twine_closes_total",
            "twine_read_bytes_total",
            "twine_connections",
            "twine_iterations_total",
            "twine_iteration_max_seconds",
        ] {
            assert!(
                out.contains(&format!("{series}{{loop=\"{l}\"}} ")),
                "missing {series} loop {l}:\n{out}"
            );
        }
        assert!(out.contains(&format!(
            "twine_iteration_seconds{{loop=\"{l}\",quantile=\"0.99\"}} "
        )));
    }
    assert!(out.contains("# TYPE twine_accepts_total counter"));
    assert!(out.contains("# TYPE twine_connections gauge"));
    let accepts: u64 = out
        .lines()
        .filter(|l| l.starts_with("twine_accepts_total{"))
        .map(|l| l.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
        .sum();
    assert_eq!(accepts, 1);
    srv.stop();
}

#[test]
fn panic_counter_increments_on_protocol_panic() {
    let mut srv = Server::start(1, factory(|_, _| Box::new(Boom)));
    let mut c = srv.connect();
    c.write_all(b"x").unwrap();
    let mut buf = [0u8; 1];
    // The connection is aborted: EOF or reset.
    match c.read(&mut buf) {
        Ok(0) | Err(_) => {}
        Ok(n) => panic!("unexpected {n} bytes"),
    }
    assert!(wait_until(Duration::from_secs(5), || total(&srv, |m| m
        .panics)
        == 1));
    srv.stop();
}
