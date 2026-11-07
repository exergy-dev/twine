//! Idle-connection memory probe: a twine echo server that reports its own RSS and
//! connection count, for the 1M-idle-connections target (design Section 2).
//!
//! Flags: `--addr` (127.0.0.1:9000), `--loops` (2), `--idle` (idle timeout, 0 = off),
//! `--read-buffer` (65536), `--report` (report interval, 5s), `--hold` (exit after
//! this long, 0 = run until SIGTERM), `--stdin-close` (1 = also exit when stdin closes),
//! `--backlog` (65535). Prints `RESULT conns=.. rss_bytes=.. bytes_per_conn=..` on exit.
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use twine::{Engine, EngineHandle, ListenOptions, Protocol, Transport};
use twine_bench::Args;

const USAGE: &str = "usage: idle_server [--addr 127.0.0.1:9000] [--loops 2] [--idle 0] \
[--read-buffer 65536] [--report 5s] [--hold 0] [--stdin-close 0] [--backlog 65535]";

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

struct Echo(Option<Duration>);

impl Protocol for Echo {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        if self.0.is_some() {
            t.set_idle_timeout(self.0);
        }
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data);
    }
}

/// Resident set size in bytes from `/proc/self/statm`.
fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    // SAFETY: sysconf has no memory-safety preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as u64;
    pages * page
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
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rl) != 0 {
                eprintln!(
                    "setrlimit(NOFILE) failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            eprintln!("idle_server: RLIMIT_NOFILE = {}", rl.rlim_max);
        }
    }
}

fn conns(h: &EngineHandle) -> u64 {
    h.metrics().iter().map(|m| m.connections).sum()
}

fn main() -> twine::Result<()> {
    let a = Args::parse(
        USAGE,
        &[
            "addr",
            "loops",
            "idle",
            "read-buffer",
            "report",
            "hold",
            "stdin-close",
            "backlog",
        ],
    );
    raise_nofile();
    let idle = a.duration("idle", Duration::ZERO);
    let idle = (!idle.is_zero()).then_some(idle);
    let report = a.duration("report", Duration::from_secs(5));
    let hold = a.duration("hold", Duration::ZERO);
    let eng = Engine::builder()
        .loops(a.usize("loops", 2))
        .watchdog(None)
        .read_buffer_size(a.usize("read-buffer", 65536))
        .build()?;
    let lst = eng.listen_tcp(
        a.str("addr", "127.0.0.1:9000").as_str(),
        twine::factory(move |_, _| Box::new(Echo(idle))),
        ListenOptions::default().backlog(a.usize("backlog", 65535) as i32),
    )?;
    // SAFETY: the handler only stores to an atomic, which is async-signal-safe.
    unsafe {
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
    }
    if a.usize("stdin-close", 0) == 1 {
        std::thread::spawn(|| {
            let mut buf = [0u8; 256];
            let mut si = std::io::stdin();
            while matches!(si.read(&mut buf), Ok(n) if n > 0) {}
            STOP.store(true, Ordering::SeqCst);
        });
    }
    let h = eng.handle();
    let base = rss_bytes();
    eprintln!(
        "idle_server listening on {} loops={} rss_base_bytes={base}",
        lst.addr(),
        eng.loop_handles().len()
    );
    let hh = h.clone();
    std::thread::spawn(move || {
        let start = Instant::now();
        let mut last = Instant::now();
        let mut peak = 0u64;
        loop {
            std::thread::sleep(Duration::from_millis(100));
            peak = peak.max(rss_bytes());
            let done = STOP.load(Ordering::SeqCst) || (!hold.is_zero() && start.elapsed() >= hold);
            if done {
                let c = conns(&hh);
                let rss = rss_bytes();
                let per = rss.saturating_sub(base) / c.max(1);
                println!(
                    "RESULT conns={c} rss_bytes={rss} bytes_per_conn={per} rss_base_bytes={base} rss_peak_bytes={peak}"
                );
                std::process::exit(0);
            }
            if last.elapsed() >= report {
                last = Instant::now();
                let c = conns(&hh);
                let rss = rss_bytes();
                println!(
                    "REPORT t={:.0}s connections={c} rss_bytes={rss} bytes_per_conn={}",
                    start.elapsed().as_secs_f64(),
                    rss.saturating_sub(base) / c.max(1)
                );
            }
        }
    });
    eng.run()
}
