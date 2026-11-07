//! Many idle connections: all accepted, all still echo, and closing returns the
//! slots (and slab memory) for reuse. The 10K version is `--ignored`.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::*;
use twine::{factory, MetricsSnapshot, Protocol, Transport};

struct Echo;
impl Protocol for Echo {
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

/// RSS is process-wide, so the tests that measure it run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

/// The soft `RLIMIT_NOFILE`, or 0 if it cannot be read.
fn nofile_limit() -> u64 {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `rl` is a valid, writable rlimit for the duration of the call.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
    if rc == 0 {
        rl.rlim_cur
    } else {
        0
    }
}

/// Resident set size of this process in bytes (server and clients share it).
fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    pages * 4096
}

fn total(srv: &Server, f: impl Fn(&MetricsSnapshot) -> u64) -> u64 {
    srv.eng.metrics().iter().map(f).sum()
}

fn open_all(srv: &Server, n: usize) -> Vec<TcpStream> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let s = TcpStream::connect(srv.addr).unwrap_or_else(|e| panic!("connect #{i}: {e}"));
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        v.push(s);
    }
    v
}

fn echo_all(conns: &mut [TcpStream]) {
    for (i, c) in conns.iter_mut().enumerate() {
        c.write_all(&(i as u32).to_le_bytes()).unwrap();
    }
    for (i, c) in conns.iter_mut().enumerate() {
        let mut b = [0u8; 4];
        c.read_exact(&mut b).unwrap();
        assert_eq!(
            u32::from_le_bytes(b),
            i as u32,
            "connection {i} echoed wrong data"
        );
    }
}

/// Opens `n` connections against `loops` loops and checks that the per-loop
/// fd-indexed tables are sparse: only occupied fds hold a boxed slot, and the
/// process pays well under `max_bytes_per_conn` of RSS per connection (the
/// kernel's socket state is not resident memory).
fn sparse_table(loops: usize, n: usize, max_bytes_per_conn: f64) {
    let need = 2 * n as u64 + 512;
    if nofile_limit() < need {
        eprintln!("SKIP sparse_table: RLIMIT_NOFILE too low for {n} connections");
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut srv = Server::start(loops, factory(|_, _| Box::new(Echo)));
    // Warm every loop's thread-local state so the baseline includes it.
    let mut warm = open_all(&srv, 2 * loops);
    echo_all(&mut warm);
    drop(warm);
    assert!(wait_until(Duration::from_secs(10), || total(&srv, |m| m
        .connections)
        == 0));
    let rss0 = rss_bytes();

    let mut conns = open_all(&srv, n);
    assert!(
        wait_until(Duration::from_secs(60), || total(&srv, |m| m.connections)
            == n as u64),
        "only {} of {n} connections accepted",
        total(&srv, |m| m.connections)
    );
    echo_all(&mut conns);
    let rss1 = rss_bytes();
    let per_conn = rss1.saturating_sub(rss0) as f64 / n as f64;

    let snaps = srv.eng.metrics();
    for m in &snaps {
        // Open connections plus at most the loop's listener socket.
        assert!(
            m.slots_live >= m.connections && m.slots_live <= m.connections + 1,
            "loop {}: slots_live {} vs connections {}",
            m.loop_id,
            m.slots_live,
            m.connections
        );
        // The table spans the highest fd, but only live fds hold a box.
        assert!(m.slots_table >= m.slots_live, "loop {}", m.loop_id);
    }
    let live: u64 = snaps.iter().map(|m| m.slots_live).sum();
    println!(
        "sparse_table({loops} loops, {n} conns): {per_conn:.0} B/conn RSS, slots_live {live}, tables {:?}",
        snaps.iter().map(|m| m.slots_table).collect::<Vec<_>>()
    );
    assert!(
        per_conn < max_bytes_per_conn,
        "{per_conn:.0} B/conn with {loops} loops exceeds {max_bytes_per_conn}: connection tables are not sparse"
    );

    drop(conns);
    assert!(wait_until(Duration::from_secs(60), || total(&srv, |m| m
        .connections)
        == 0));
    // Closed boxes leave the table; at most the loop's peak stays on the free list.
    assert!(wait_until(Duration::from_secs(10), || srv
        .eng
        .metrics()
        .iter()
        .all(|m| m.slots_live <= 1)));
    srv.stop();
}

#[test]
fn sparse_table_two_loops() {
    sparse_table(2, 2000, 1536.0);
}

#[test]
fn sparse_table_eight_loops() {
    sparse_table(8, 2000, 2048.0);
}

fn soak(n: usize, hold: Duration) {
    let need = 2 * n as u64 + 512;
    let have = nofile_limit();
    if have < 4096.max(need) {
        eprintln!(
            "SKIP soak({n}): RLIMIT_NOFILE {have} < {} required",
            4096.max(need)
        );
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut srv = Server::start(2, factory(|_, _| Box::new(Echo)));
    let rss0 = rss_bytes();

    let mut conns = open_all(&srv, n);
    assert!(
        wait_until(Duration::from_secs(60), || total(&srv, |m| m.connections)
            == n as u64),
        "only {} of {n} connections accepted",
        total(&srv, |m| m.connections)
    );
    let t0 = Instant::now();
    while t0.elapsed() < hold {
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            total(&srv, |m| m.connections),
            n as u64,
            "a connection was dropped while idle"
        );
    }
    let rss_held = rss_bytes();
    let per_loop: Vec<u64> = srv.eng.metrics().iter().map(|m| m.connections).collect();
    echo_all(&mut conns);
    println!(
        "soak({n}): RSS before {:.1} MiB, with {n} idle conns {:.1} MiB ({:.0} B/conn incl. client side), per-loop {per_loop:?}",
        rss0 as f64 / 1048576.0,
        rss_held as f64 / 1048576.0,
        (rss_held.saturating_sub(rss0)) as f64 / n as f64,
    );

    drop(conns);
    assert!(
        wait_until(Duration::from_secs(60), || total(&srv, |m| m.connections)
            == 0
            && total(&srv, |m| m.closes) == n as u64),
        "connections must all close"
    );
    let rss_closed = rss_bytes();
    assert_eq!(
        total(&srv, |m| m.slab_in_use as u64),
        0,
        "slab memory is returned on close"
    );

    // A second wave reuses the freed slots: memory must not keep growing.
    let mut conns = open_all(&srv, n);
    assert!(wait_until(Duration::from_secs(60), || total(&srv, |m| m
        .connections)
        == n as u64));
    echo_all(&mut conns);
    let rss_again = rss_bytes();
    println!(
        "soak({n}): RSS after close {:.1} MiB, second wave {:.1} MiB",
        rss_closed as f64 / 1048576.0,
        rss_again as f64 / 1048576.0
    );
    assert!(
        rss_again <= rss_held + rss_held / 4 + (8 << 20),
        "second wave grew RSS from {rss_held} to {rss_again}: slots are not reused"
    );
    drop(conns);
    assert!(wait_until(Duration::from_secs(60), || total(&srv, |m| m
        .closes)
        == 2 * n as u64));
    srv.stop();
}

#[test]
fn soak_1k_idle_connections() {
    soak(1_000, Duration::from_millis(500));
}

#[test]
#[ignore = "10K connections held for 5 s; run with --ignored"]
fn soak_10k_idle_connections() {
    soak(10_000, Duration::from_secs(5));
}
