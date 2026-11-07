//! Twine echo server for the benchmarks (design Section 12). Drive it with `loadgen echo`.
//!
//! Flags: `--addr` (127.0.0.1:9000), `--loops` (0 = one per CPU), `--pin a,b,c`
//! (loop i runs on cpus[i % len]), `--spin N` (poll spin iterations before
//! blocking; default is the engine's auto rule), `--idle 30s` (idle timeout, 0 = off).
#![forbid(unsafe_code)]

use std::time::Duration;

use twine::{Engine, ListenOptions, Protocol, Transport};
use twine_bench::Args;

const USAGE: &str =
    "usage: echo_twine [--addr 127.0.0.1:9000] [--loops N] [--pin 0,1,2] [--spin N] [--idle 0]";

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

fn main() -> twine::Result<()> {
    let a = Args::parse(USAGE, &["addr", "loops", "pin", "spin", "idle"]);
    let mut o = Engine::builder().loops(a.usize("loops", 0));
    let pin = a.cpus("pin");
    if !pin.is_empty() {
        o = o.pin_loops(pin);
    }
    if a.get("spin").is_some() {
        o = o.spin_iterations(a.usize("spin", 0));
    }
    let idle = a.duration("idle", Duration::ZERO);
    let idle = (!idle.is_zero()).then_some(idle);
    let eng = o.build()?;
    let lst = eng.listen_tcp(
        a.str("addr", "127.0.0.1:9000").as_str(),
        twine::factory(move |_, _| Box::new(Echo(idle))),
        ListenOptions::default(),
    )?;
    eprintln!(
        "echo_twine listening on {} with {} loops",
        lst.addr(),
        eng.loop_handles().len()
    );
    eng.run()
}
