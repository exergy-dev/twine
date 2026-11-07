//! Tokio multi-thread echo server: the async-runtime baseline.
//!
//! Flags: `--addr` (127.0.0.1:9000), `--workers N` (runtime worker threads; 0 = one per CPU).
#![forbid(unsafe_code)]

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use twine_bench::Args;

const USAGE: &str = "usage: echo_tokio [--addr 127.0.0.1:9000] [--workers N]";

async fn serve(mut s: TcpStream) {
    let _ = s.set_nodelay(true);
    let mut buf = [0u8; 16 * 1024];
    while let Ok(n) = s.read(&mut buf).await {
        if n == 0 || s.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
}

fn main() -> std::io::Result<()> {
    let a = Args::parse(USAGE, &["addr", "workers"]);
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_io();
    if a.usize("workers", 0) > 0 {
        rt.worker_threads(a.usize("workers", 0));
    }
    rt.build()?.block_on(async {
        let l = TcpListener::bind(a.str("addr", "127.0.0.1:9000")).await?;
        eprintln!("echo_tokio listening on {}", l.local_addr()?);
        loop {
            let (s, _) = l.accept().await?;
            tokio::spawn(serve(s));
        }
    })
}
