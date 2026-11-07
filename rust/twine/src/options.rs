//! Engine, listener and dial options (design Section 6.2). Builder style
//! replaces Go's functional options.

use std::sync::OnceLock;
use std::time::Duration;

/// Returns the number of CPUs available to this process (the Rust equivalent
/// of `GOMAXPROCS`).
pub fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Reports whether debug mode is on: `TWINE_DEBUG=1` in the environment at
/// first use. Debug mode captures `Deferred` creation backtraces, warns about
/// unhandled rejections and abandoned deferreds, and records panic backtraces.
pub fn debug_enabled() -> bool {
    static DEBUG: OnceLock<bool> = OnceLock::new();
    *DEBUG.get_or_init(|| {
        std::env::var("TWINE_DEBUG")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

/// Engine configuration. Construct with [`Options::default`] (or
/// `Engine::builder()`), adjust with the builder methods, and pass to
/// `Engine::new`.
#[derive(Debug, Clone)]
pub struct Options {
    pub(crate) loops: usize,
    pub(crate) reuse_port: bool,
    pub(crate) spin_iterations: usize,
    pub(crate) spin_set: bool,
    pub(crate) worker_pool: usize,
    pub(crate) read_buffer_size: usize,
    pub(crate) write_high_water: usize,
    pub(crate) write_low_water: usize,
    pub(crate) write_buffer_limit: usize,
    pub(crate) watchdog: Option<Duration>,
    pub(crate) max_reads_per_event: usize,
    pub(crate) max_accepts_per_event: usize,
    pub(crate) task_queue_size: usize,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) pin_cpus: Vec<usize>,
    pub(crate) close_linger: Duration,
    pub(crate) busy_poll: Option<Duration>,
    pub(crate) udp_batch: usize,
}

impl Default for Options {
    fn default() -> Self {
        let n = available_cpus();
        Options {
            loops: n,
            reuse_port: cfg!(target_os = "linux"),
            spin_iterations: 50,
            spin_set: false,
            worker_pool: 4 * n,
            read_buffer_size: 64 << 10,
            write_high_water: 256 << 10,
            write_low_water: 64 << 10,
            write_buffer_limit: 16 << 20,
            watchdog: Some(Duration::from_millis(1)),
            max_reads_per_event: 8,
            max_accepts_per_event: 64,
            task_queue_size: 64 << 10,
            shutdown_timeout: Duration::from_secs(5),
            pin_cpus: Vec::new(),
            close_linger: Duration::from_secs(10),
            busy_poll: None,
            udp_batch: 64,
        }
    }
}

impl Options {
    /// Number of event loops. 0 means one per available CPU (the default).
    pub fn loops(mut self, n: usize) -> Self {
        self.loops = n;
        self
    }
    /// Toggle SO_REUSEPORT listeners (one per loop). Default true on Linux.
    /// When off a single listener hands accepted connections to loops round-robin.
    pub fn reuse_port(mut self, on: bool) -> Self {
        self.reuse_port = on;
        self
    }
    /// How many zero-timeout polls a loop performs before blocking. Default 50;
    /// 0 blocks immediately. Forced to 0 when `loops > cpus / 2` unless set explicitly.
    pub fn spin_iterations(mut self, n: usize) -> Self {
        self.spin_iterations = n;
        self.spin_set = true;
        self
    }
    /// Size of the thread pool behind `defer_to_thread`. Default `4 * cpus`.
    pub fn worker_pool(mut self, size: usize) -> Self {
        self.worker_pool = size;
        self
    }
    /// Per-loop shared read buffer. Default 64 KiB.
    pub fn read_buffer_size(mut self, n: usize) -> Self {
        self.read_buffer_size = n;
        self
    }
    /// Pending-write level above which a registered push producer is paused. Default 256 KiB.
    pub fn write_high_water(mut self, n: usize) -> Self {
        self.write_high_water = n;
        self
    }
    /// Pending-write level below which a paused producer is resumed. Default 64 KiB.
    pub fn write_low_water(mut self, n: usize) -> Self {
        self.write_low_water = n;
        self
    }
    /// Pending-write level at which a connection is aborted with
    /// `Error::WriteBufferFull`. Default 16 MiB.
    pub fn write_buffer_limit(mut self, n: usize) -> Self {
        self.write_buffer_limit = n;
        self
    }
    /// Slow-callback threshold. Default 1 ms; `None` disables the watchdog.
    pub fn watchdog(mut self, threshold: Option<Duration>) -> Self {
        self.watchdog = threshold;
        self
    }
    /// Cap on `read(2)` calls per fd per iteration. Default 8.
    pub fn max_reads_per_event(mut self, n: usize) -> Self {
        self.max_reads_per_event = n.max(1);
        self
    }
    /// Cap on `accept4(2)` calls per listener per iteration. Default 64.
    pub fn max_accepts_per_event(mut self, n: usize) -> Self {
        self.max_accepts_per_event = n.max(1);
        self
    }
    /// Capacity of the per-loop lock-free `call_from_thread` ring. Default 64K.
    /// Overflow spills to a mutex-guarded vector; nothing is dropped.
    pub fn task_queue_size(mut self, n: usize) -> Self {
        self.task_queue_size = n;
        self
    }
    /// How long `stop` waits for pending writes to flush. Default 5 s.
    pub fn shutdown_timeout(mut self, d: Duration) -> Self {
        self.shutdown_timeout = d;
        self
    }
    /// Pin loop `i` to `cpus[i % cpus.len()]` with `sched_setaffinity` (Linux;
    /// ignored elsewhere). Default: no pinning.
    pub fn pin_loops(mut self, cpus: Vec<usize>) -> Self {
        self.pin_cpus = cpus;
        self
    }
    /// How long `lose_connection` waits after SHUT_WR for the peer to close.
    /// Default 10 s; zero closes as soon as pending writes are flushed (Twisted's behaviour).
    pub fn close_linger(mut self, d: Duration) -> Self {
        self.close_linger = d;
        self
    }
    /// Set SO_BUSY_POLL on accepted sockets (Linux). Default off.
    pub fn busy_poll(mut self, d: Option<Duration>) -> Self {
        self.busy_poll = d;
        self
    }
    /// recvmmsg/sendmmsg batch size. Default 64.
    pub fn udp_batch(mut self, n: usize) -> Self {
        self.udp_batch = n.max(1);
        self
    }

    /// Applies the auto-configuration rules that depend on other fields
    /// (design Section 7.6). Called by `Engine::new`.
    pub(crate) fn finalize(mut self) -> Self {
        let cpus = available_cpus();
        if self.loops == 0 {
            self.loops = cpus;
        }
        if !self.spin_set && self.loops > cpus / 2 {
            self.spin_iterations = 0;
        }
        if self.worker_pool == 0 {
            self.worker_pool = 4 * cpus;
        }
        if self.write_low_water > self.write_high_water {
            self.write_low_water = self.write_high_water / 4;
        }
        if self.read_buffer_size < 4096 {
            self.read_buffer_size = 4096;
        }
        self
    }
}

/// Options for `listen_tcp` / `listen_udp`.
#[derive(Debug, Clone)]
pub struct ListenOptions {
    pub(crate) no_delay: bool,
    pub(crate) defer_accept: Option<Duration>,
    pub(crate) backlog: i32,
    pub(crate) rcv_buf: usize,
    pub(crate) snd_buf: usize,
}

impl Default for ListenOptions {
    fn default() -> Self {
        ListenOptions {
            no_delay: true,
            defer_accept: None,
            backlog: 4096,
            rcv_buf: 0,
            snd_buf: 0,
        }
    }
}

impl ListenOptions {
    /// TCP_NODELAY on accepted sockets. Default true.
    pub fn no_delay(mut self, on: bool) -> Self {
        self.no_delay = on;
        self
    }
    /// TCP_DEFER_ACCEPT (Linux only; ignored elsewhere).
    pub fn defer_accept(mut self, d: Option<Duration>) -> Self {
        self.defer_accept = d;
        self
    }
    /// `listen(2)` backlog. Default 4096.
    pub fn backlog(mut self, n: i32) -> Self {
        self.backlog = n;
        self
    }
    /// SO_RCVBUF / SO_SNDBUF on accepted sockets; 0 leaves the kernel default.
    pub fn socket_buffers(mut self, rcv: usize, snd: usize) -> Self {
        self.rcv_buf = rcv;
        self.snd_buf = snd;
        self
    }
}

/// Options for `connect_tcp`.
#[derive(Debug, Clone)]
pub struct DialOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) no_delay: bool,
    pub(crate) local: Option<std::net::SocketAddr>,
}

impl Default for DialOptions {
    fn default() -> Self {
        DialOptions {
            timeout: Some(Duration::from_secs(30)),
            no_delay: true,
            local: None,
        }
    }
}

impl DialOptions {
    /// Bound on a connect attempt. Default 30 s; `None` means no timeout.
    pub fn timeout(mut self, d: Option<Duration>) -> Self {
        self.timeout = d;
        self
    }
    /// TCP_NODELAY on the connected socket. Default true.
    pub fn no_delay(mut self, on: bool) -> Self {
        self.no_delay = on;
        self
    }
    /// Bind the outbound socket to a local address before connecting.
    pub fn local_addr(mut self, addr: Option<std::net::SocketAddr>) -> Self {
        self.local = addr;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalize_rules() {
        let o = Options::default().loops(0).finalize();
        assert_eq!(o.loops, available_cpus());
        let o = Options::default().loops(available_cpus()).finalize();
        assert_eq!(
            o.spin_iterations, 0,
            "spin auto-disabled when loops > cpus/2"
        );
        let o = Options::default().loops(1).spin_iterations(7).finalize();
        assert_eq!(o.spin_iterations, 7);
        let o = Options::default()
            .write_high_water(100)
            .write_low_water(1000)
            .finalize();
        assert_eq!(o.write_low_water, 25);
    }
}
