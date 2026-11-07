//! Minimal twine-http server: every request gets `Hello, World!`.
//!
//! ```text
//! cargo run --release -p twine-examples --bin http_hello -- --addr 0.0.0.0:8080 --loops 4
//! curl -i localhost:8080/
//! ```

use twine::{Engine, ListenOptions};
use twine_examples::common::{init_logger, Args};
use twine_http::{handler, Server};

const USAGE: &str = "usage: http_hello [--addr 0.0.0.0:8080] [--loops N]";

fn main() -> twine::Result<()> {
    init_logger();
    let a = Args::parse(USAGE, &["addr", "loops"]);
    let eng = Engine::builder().loops(a.usize("loops", 0)).build()?;
    let srv = Server::new(handler(|ctx| {
        ctx.set_content_type("text/plain");
        ctx.write_str("Hello, World!");
    }));
    let lst = srv.listen(
        &eng,
        a.str("addr", "0.0.0.0:8080").as_str(),
        ListenOptions::default(),
    )?;
    log::info!(
        "http_hello: listening on {} with {} loops",
        lst.addr(),
        eng.loop_handles().len()
    );
    eng.run()
}
