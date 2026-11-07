//! Idle-connection client for the 1M-idle-connections soak. Opens `-c N` TCP
//! connections to `--addr` (non-blocking connects driven by epoll, paced by `--rate`),
//! each socket bound to a rotating source address from `--src` so the 64K ports per
//! source are not exhausted, holds them idle, then pings a random sample and checks
//! the echo.
//!
//! Flags: `-c` (1000), `--addr` (127.0.0.1:9000), `--threads` (4), `--src`
//! (127.0.0.2-127.0.0.65, last-octet range), `--rate` (connects/s total, 20000),
//! `--hold` (seconds to hold), `--go FILE` (end the hold early once FILE exists),
//! `--sample` (200), `--inflight` (pending connects per thread, 512), `--stall` (60s), `--done FILE` (after the sample, stay connected until FILE exists).
//! Prints `ESTABLISHED n=..` when connected and `RESULT established=.. failed=.. sampled_ok=..`.
#![allow(unsafe_code)]

use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use twine_bench::Args;

const USAGE: &str = "usage: idle_client -c N [--addr 127.0.0.1:9000] [--threads 4] \
[--src 127.0.0.2-127.0.0.65] [--rate 20000] [--hold 10s] [--go FILE] [--sample 200] \
[--inflight 512] [--stall 60s] [--done FILE]";

fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn raise_nofile() {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `rl` is a valid rlimit struct for both calls.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 {
            rl.rlim_cur = rl.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
        }
    }
}

fn parse_src(s: &str) -> Option<(Ipv4Addr, u8, u8)> {
    let (a, b) = s.split_once('-')?;
    let a: Ipv4Addr = a.parse().ok()?;
    let last = match b.parse::<Ipv4Addr>() {
        Ok(ip) => ip.octets()[3],
        Err(_) => b.parse().ok()?,
    };
    let first = a.octets()[3];
    (first <= last).then_some((a, first, last))
}

fn sockaddr(ip: Ipv4Addr, port: u16) -> libc::sockaddr_in {
    // SAFETY: sockaddr_in is plain data; all-zero is a valid value.
    let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    sa.sin_family = libc::AF_INET as libc::sa_family_t;
    sa.sin_port = port.to_be();
    sa.sin_addr.s_addr = u32::from(ip).to_be();
    sa
}

struct Shared {
    progress: AtomicU64,
    retries: AtomicU64,
    stop: AtomicBool,
}

struct Plan {
    dst: libc::sockaddr_in,
    base: Ipv4Addr,
    nsrc: usize,
    inflight: usize,
    rate: f64,
}

/// Starts one non-blocking connect from a rotating source; returns the fd or -1.
fn start_connect(p: &Plan, seq: usize) -> i32 {
    let o = p.base.octets();
    let src = Ipv4Addr::new(o[0], o[1], o[2], o[3].wrapping_add((seq % p.nsrc) as u8));
    // SAFETY: plain socket(2).
    let fd = unsafe {
        libc::socket(
            libc::AF_INET,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return -1;
    }
    let one: libc::c_int = 1;
    let sa = sockaddr(src, 0);
    // SAFETY: `one`, `sa` and `p.dst` are valid for the sizes passed; fd is open.
    let r = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_IP,
            libc::IP_BIND_ADDRESS_NO_PORT,
            (&one as *const libc::c_int).cast(),
            4,
        );
        if libc::bind(
            fd,
            (&sa as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as u32,
        ) != 0
        {
            -1
        } else {
            libc::connect(
                fd,
                (&p.dst as *const libc::sockaddr_in).cast(),
                std::mem::size_of::<libc::sockaddr_in>() as u32,
            )
        }
    };
    if r != 0 && errno() != libc::EINPROGRESS {
        // SAFETY: fd is open and owned here.
        unsafe { libc::close(fd) };
        return -1;
    }
    fd
}

fn worker(tid: usize, nthreads: usize, want: usize, p: Arc<Plan>, sh: Arc<Shared>) -> Vec<i32> {
    // SAFETY: plain epoll_create1.
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    assert!(ep >= 0, "epoll_create1: {}", io::Error::last_os_error());
    let mut done: Vec<i32> = Vec::with_capacity(want);
    let mut pending = 0usize;
    let mut started = 0usize; // successful starts (not counting retries)
    let mut retry = 0usize; // connects to re-issue
    let rate = p.rate.max(1.0);
    let t0 = Instant::now();
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; 1024];
    while done.len() < want && !sh.stop.load(Ordering::Relaxed) {
        let allowed = (t0.elapsed().as_secs_f64() * rate) as usize + 1;
        while pending < p.inflight && (retry > 0 || (started < want && started < allowed)) {
            let seq = tid + (started + sh.retries.load(Ordering::Relaxed) as usize) * nthreads;
            let fd = start_connect(&p, seq);
            if fd < 0 {
                sh.retries.fetch_add(1, Ordering::Relaxed);
                break;
            }
            let mut ev = libc::epoll_event {
                events: libc::EPOLLOUT as u32,
                u64: fd as u64,
            };
            // SAFETY: `ev` is valid for the call; ep and fd are open.
            unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, fd, &mut ev) };
            pending += 1;
            if retry > 0 {
                retry -= 1;
            } else {
                started += 1;
            }
        }
        // SAFETY: `events` has room for 1024 entries.
        let n = unsafe { libc::epoll_wait(ep, events.as_mut_ptr(), events.len() as i32, 5) };
        for e in events.iter().take(n.max(0) as usize) {
            let fd = e.u64 as i32;
            let mut err: libc::c_int = 0;
            let mut len = 4u32;
            // SAFETY: `err`/`len` are valid out-parameters; fd is open.
            unsafe {
                libc::getsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_ERROR,
                    (&mut err as *mut libc::c_int).cast(),
                    &mut len,
                );
                libc::epoll_ctl(ep, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
            }
            pending -= 1;
            if err == 0 {
                done.push(fd);
                sh.progress.fetch_add(1, Ordering::Relaxed);
            } else {
                // SAFETY: fd is open and owned here.
                unsafe { libc::close(fd) };
                sh.retries.fetch_add(1, Ordering::Relaxed);
                retry += 1;
            }
        }
    }
    // SAFETY: ep is open and owned here.
    unsafe { libc::close(ep) };
    done
}

/// Sends a 16-byte ping and checks the echo, with a 10 s timeout.
fn ping(fd: i32, tag: u64) -> bool {
    let mut msg = [0u8; 16];
    msg[..8].copy_from_slice(&tag.to_le_bytes());
    msg[8..].copy_from_slice(b"idleping");
    let tv = libc::timeval {
        tv_sec: 10,
        tv_usec: 0,
    };
    // SAFETY: buffers are valid for the lengths passed; fd is an open socket.
    unsafe {
        let fl = libc::fcntl(fd, libc::F_GETFL);
        libc::fcntl(fd, libc::F_SETFL, fl & !libc::O_NONBLOCK);
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            (&tv as *const libc::timeval).cast(),
            std::mem::size_of::<libc::timeval>() as u32,
        );
        if libc::send(fd, msg.as_ptr().cast(), 16, libc::MSG_NOSIGNAL) != 16 {
            return false;
        }
        let mut got = [0u8; 16];
        let mut off = 0usize;
        while off < 16 {
            let r = libc::recv(fd, got[off..].as_mut_ptr().cast(), 16 - off, 0);
            if r <= 0 {
                return false;
            }
            off += r as usize;
        }
        got == msg
    }
}

fn main() {
    let a = Args::parse(
        USAGE,
        &[
            "c", "addr", "threads", "src", "rate", "hold", "go", "sample", "inflight", "stall",
            "done",
        ],
    );
    raise_nofile();
    let total = a.usize("c", 1000);
    let threads = a.usize("threads", 4).max(1);
    let dst_s = a.str("addr", "127.0.0.1:9000");
    let SocketAddr::V4(dst) = dst_s.parse().expect("bad --addr") else {
        panic!("--addr must be IPv4");
    };
    let (base, first, last) = parse_src(&a.str("src", "127.0.0.2-127.0.0.65")).expect("bad --src");
    let o = base.octets();
    let plan = Arc::new(Plan {
        dst: sockaddr(*dst.ip(), dst.port()),
        base: Ipv4Addr::new(o[0], o[1], o[2], first),
        nsrc: (last - first) as usize + 1,
        inflight: a.usize("inflight", 512),
        rate: a.f64("rate", 20000.0) / threads as f64,
    });
    let sh = Arc::new(Shared {
        progress: AtomicU64::new(0),
        retries: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    });
    let stall = a.duration("stall", Duration::from_secs(60));
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let want = total / threads + usize::from(t < total % threads);
            let (p, s) = (plan.clone(), sh.clone());
            std::thread::spawn(move || worker(t, threads, want, p, s))
        })
        .collect();
    // Watch for stalls while the workers run.
    let t0 = Instant::now();
    let (mut last_n, mut last_t) = (0u64, Instant::now());
    while !hs.iter().all(|h| h.is_finished()) {
        std::thread::sleep(Duration::from_millis(200));
        let n = sh.progress.load(Ordering::Relaxed);
        if n != last_n {
            (last_n, last_t) = (n, Instant::now());
        } else if last_t.elapsed() > stall {
            eprintln!("idle_client: no progress for {stall:?} at {n} connections, aborting");
            sh.stop.store(true, Ordering::Relaxed);
        }
        if t0.elapsed().as_secs() % 5 == 0 && t0.elapsed().subsec_millis() < 200 {
            eprintln!(
                "idle_client: {n}/{total} established, {} retries",
                sh.retries.load(Ordering::Relaxed)
            );
        }
    }
    let fds: Vec<i32> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
    let est = fds.len();
    println!(
        "ESTABLISHED n={est} secs={:.1} retries={}",
        t0.elapsed().as_secs_f64(),
        sh.retries.load(Ordering::Relaxed)
    );
    let _ = io::stdout().flush();

    let hold = a.duration("hold", Duration::from_secs(10));
    let go = a.get("go").map(str::to_string);
    let h0 = Instant::now();
    while h0.elapsed() < hold && !go.as_ref().is_some_and(|g| Path::new(g).exists()) {
        std::thread::sleep(Duration::from_millis(200));
    }

    // Deterministic xorshift sample of K distinct-ish connections.
    let k = a.usize("sample", 200).min(est);
    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (est as u64);
    let mut ok = 0usize;
    for i in 0..k {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        if ping(fds[(x % est as u64) as usize], i as u64) {
            ok += 1;
        }
    }
    println!(
        "RESULT established={est} failed={} sampled_ok={ok}/{k}",
        total - est
    );
    let _ = io::stdout().flush();
    // Keep the connections open until --done FILE exists, so the server can report
    // its RESULT while they are still connected.
    if let Some(d) = a.get("done") {
        while !Path::new(d).exists() {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    std::process::exit(0);
}
