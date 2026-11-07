//! Line client that keeps a connection alive with exponential backoff and full
//! jitter (initial 1 s, factor 1.6, max 60 s), sends a line every second and logs
//! every state change and received line.
//!
//! ```text
//! cargo run --release -p twine-examples --bin chat -- --addr 127.0.0.1:9001 &
//! cargo run --release -p twine-examples --bin client_reconnect -- --addr 127.0.0.1:9001
//! ```
//!
//! Stop and restart the server to watch it reconnect.
//!
//! Built on `twine_proto::ReconnectingClient`. `--initial` is clamped to at least
//! 10 ms and `--max` to at least `--initial`; `--retries N` gives up (and stops
//! the engine) after `N` consecutive failures.

use std::net::ToSocketAddrs;
use std::rc::Rc;
use std::time::Duration;

use twine::{DialOptions, Engine, Error, Transport};
use twine_examples::common::{init_logger, Args};
use twine_proto::{BuildFn, Flow, LineHandler, Lines, ReconnectOptions, ReconnectingClient};

const USAGE: &str = "usage: client_reconnect [--addr 127.0.0.1:9001] [--initial 1s] [--max 60s] [--retries N (0 = forever)]";
const MIN_INITIAL: Duration = Duration::from_millis(10);
const PING: u64 = 1;

struct Client {
    sent: u64,
}

impl LineHandler for Client {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        Lines::<Client>::send_line(t, b"hello from client_reconnect");
        t.call_later(Duration::from_secs(1), PING);
    }
    fn line_received(&mut self, _t: &mut Transport<'_>, line: &[u8]) -> Flow {
        log::info!("recv: {}", String::from_utf8_lossy(line));
        Flow::Continue
    }
    fn timer_fired(&mut self, t: &mut Transport<'_>, token: u64) {
        if token == PING {
            self.sent += 1;
            Lines::<Client>::send_line(t, format!("ping {}", self.sent).as_bytes());
            t.call_later(Duration::from_secs(1), PING);
        }
    }
    fn connection_lost(&mut self, _t: &mut Transport<'_>, reason: Option<Error>) {
        match reason {
            Some(e) => log::warn!("state: disconnected ({e}); retrying"),
            None => log::warn!("state: disconnected (closed by peer); retrying"),
        }
    }
}

fn main() -> twine::Result<()> {
    init_logger();
    let a = Args::parse(USAGE, &["addr", "initial", "max", "retries"]);
    let addr_s = a.str("addr", "127.0.0.1:9001");
    let addr = addr_s
        .to_socket_addrs()
        .ok()
        .and_then(|mut i| i.next())
        .ok_or_else(|| Error::Addr(addr_s.clone()))?;
    let initial = a
        .duration("initial", Duration::from_secs(1))
        .max(MIN_INITIAL);
    let max = a.duration("max", Duration::from_secs(60)).max(initial);
    let retries = u32::try_from(a.usize("retries", 0)).unwrap_or(u32::MAX);
    let eng = Engine::builder().loops(1).build()?;
    let handle = eng.loop_handles().remove(0);
    handle.call_from_thread(move |lp| {
        // `Rc` state and the options' callbacks are created on the loop thread.
        let build: BuildFn = Rc::new(|_, _| Box::new(Lines::new(Client { sent: 0 })));
        let opts = ReconnectOptions::default()
            .initial_delay(initial)
            .max_delay(max)
            .max_retries(retries)
            .dial_options(DialOptions::default().timeout(Some(Duration::from_secs(5))))
            .on_state_change(move |_, up| {
                if up {
                    log::info!("state: connected to {addr}");
                }
            })
            .on_connect_failed(move |_, e| {
                log::warn!("state: connect to {addr} failed ({e}); will retry");
            })
            .on_give_up(|lp| {
                log::error!("giving up");
                lp.engine().stop();
            });
        // The client lives for the whole run; its handle is not needed here.
        let _ = ReconnectingClient::start(lp, addr, build, opts);
    })?;
    eng.run()
}
