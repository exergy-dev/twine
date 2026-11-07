//! TCP echo server (design Section 6.4): every received chunk is written
//! straight back.
//!
//! ```text
//! cargo run --release -p twine-examples --bin echo -- --addr 0.0.0.0:9000 --loops 4 --idle 30s
//! ```
//!
//! Flags: `--addr` (default 0.0.0.0:9000), `--loops` (0 = one per CPU),
//! `--idle` (idle timeout, `0` disables; default 30s).

use std::time::Duration;

use twine::{Engine, ListenOptions, Protocol, Transport};
use twine_examples::common::{init_logger, Args};

const USAGE: &str = "usage: echo [--addr 0.0.0.0:9000] [--loops N] [--idle 30s]";

struct Echo {
    idle: Option<Duration>,
}

impl Protocol for Echo {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.set_idle_timeout(self.idle);
    }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        t.write(data); // copies only if the socket cannot take it now
    }
}

fn main() -> twine::Result<()> {
    init_logger();
    let a = Args::parse(USAGE, &["addr", "loops", "idle"]);
    let idle = a.duration("idle", Duration::from_secs(30));
    let idle = (!idle.is_zero()).then_some(idle);
    let eng = Engine::builder().loops(a.usize("loops", 0)).build()?;
    let lst = eng.listen_tcp(
        a.str("addr", "0.0.0.0:9000").as_str(),
        twine::factory(move |_, _| Box::new(Echo { idle })),
        ListenOptions::default(),
    )?;
    log::info!(
        "echo: listening on {} with {} loops",
        lst.addr(),
        eng.loop_handles().len()
    );
    eng.run()
}
