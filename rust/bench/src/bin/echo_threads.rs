//! Thread-per-connection blocking echo server: the classic baseline.
//!
//! Flags: `--addr` (127.0.0.1:9000).
#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use twine_bench::Args;

const USAGE: &str = "usage: echo_threads [--addr 127.0.0.1:9000]";

fn serve(mut s: TcpStream) {
    let _ = s.set_nodelay(true);
    let mut buf = [0u8; 16 * 1024];
    while let Ok(n) = s.read(&mut buf) {
        if n == 0 || s.write_all(&buf[..n]).is_err() {
            break;
        }
    }
}

fn main() -> std::io::Result<()> {
    let a = Args::parse(USAGE, &["addr"]);
    let l = TcpListener::bind(a.str("addr", "127.0.0.1:9000"))?;
    eprintln!("echo_threads listening on {}", l.local_addr()?);
    for s in l.incoming() {
        match s {
            Ok(s) => {
                // Small stacks so 10K connections do not exhaust address space.
                if let Err(e) = thread::Builder::new()
                    .stack_size(64 * 1024)
                    .spawn(move || serve(s))
                {
                    eprintln!("spawn: {e}");
                }
            }
            Err(e) => eprintln!("accept: {e}"),
        }
    }
    Ok(())
}
