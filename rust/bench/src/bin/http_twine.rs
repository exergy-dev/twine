//! twine-http hello-world server for the benchmarks. Drive it with `loadgen http`.
//!
//! Flags: `--addr` (127.0.0.1:8080), `--loops` (0 = one per CPU), `--pin a,b,c`, `--spin N`.
#![forbid(unsafe_code)]

use twine::{Engine, ListenOptions};
use twine_bench::Args;
use twine_http::{handler, Server};

const USAGE: &str =
    "usage: http_twine [--addr 127.0.0.1:8080] [--loops N] [--pin 0,1,2] [--spin N]";

fn main() -> twine::Result<()> {
    let a = Args::parse(USAGE, &["addr", "loops", "pin", "spin"]);
    let mut o = Engine::builder().loops(a.usize("loops", 0));
    let pin = a.cpus("pin");
    if !pin.is_empty() {
        o = o.pin_loops(pin);
    }
    if a.get("spin").is_some() {
        o = o.spin_iterations(a.usize("spin", 0));
    }
    let eng = o.build()?;
    let srv = Server::new(handler(|ctx| {
        ctx.set_content_type("text/plain");
        ctx.write_str("Hello, World!");
    }));
    let lst = srv.listen(
        &eng,
        a.str("addr", "127.0.0.1:8080").as_str(),
        ListenOptions::default(),
    )?;
    eprintln!(
        "http_twine listening on {} with {} loops",
        lst.addr(),
        eng.loop_handles().len()
    );
    eng.run()
}
