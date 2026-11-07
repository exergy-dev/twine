//! UDP echo server: every datagram is sent back to its sender. Datagrams are
//! received with `recvmmsg` and replies batched with `sendmmsg` at the end of
//! each loop iteration (Linux).
//!
//! ```text
//! cargo run --release -p twine-examples --bin udp_echo -- --addr 0.0.0.0:9001
//! echo hello | nc -u -w1 127.0.0.1 9001
//! ```

use std::net::SocketAddr;

use twine::{DatagramProtocol, DatagramTransport, Engine, ListenOptions};
use twine_examples::common::{init_logger, Args};

const USAGE: &str = "usage: udp_echo [--addr 0.0.0.0:9001] [--loops N]";

struct UdpEcho;

impl DatagramProtocol for UdpEcho {
    fn datagram_received(
        &mut self,
        t: &mut DatagramTransport<'_>,
        data: &mut [u8],
        from: SocketAddr,
    ) {
        t.write_to(data, from); // copied into the outbound batch
    }
}

fn main() -> twine::Result<()> {
    init_logger();
    let a = Args::parse(USAGE, &["addr", "loops"]);
    let eng = Engine::builder().loops(a.usize("loops", 1)).build()?;
    let lst = eng.listen_udp(
        a.str("addr", "0.0.0.0:9001").as_str(),
        |_lp| Box::new(UdpEcho),
        ListenOptions::default(),
    )?;
    log::info!("udp_echo: listening on {}", lst.addr());
    eng.run()
}
