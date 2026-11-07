//! The thread-per-connection escape hatch (design Section 6.6): each
//! connection's handler runs on its own thread against a blocking
//! [`BlockingConn`] stream. The loop side only copies chunks into a bounded
//! channel and never blocks.

use std::io;
use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;

use crate::event_loop::Loop;
use crate::protocol::{Factory, Protocol};
use crate::transport::{Transport, TransportHandle};

/// Chunks buffered between the loop and the handler thread.
const DEPTH: usize = 64;

type Handler = Arc<dyn Fn(BlockingConn) + Send + Sync>;

/// State shared by the loop-side protocol and the handler thread.
struct Shared {
    /// The loop paused reading because the channel was full.
    paused: AtomicBool,
    /// `connection_lost` was delivered.
    lost: AtomicBool,
}

/// Wraps `f` as a [`Factory`]: each connection's handler runs on its own thread
/// (named `twine-blocking`) with a blocking stream, at thread-per-connection
/// cost. The loop copies each received chunk into a bounded channel (64 chunks)
/// and pauses reading while it is full. When `f` returns the connection is
/// closed gracefully; a panic in `f` aborts only that connection.
pub fn blocking<F>(f: F) -> impl Factory
where
    F: Fn(BlockingConn) + Send + Sync + 'static,
{
    let h: Handler = Arc::new(f);
    move |_lp: &mut Loop, _peer: SocketAddr| -> Box<dyn Protocol> {
        Box::new(BlockingProto {
            f: h.clone(),
            tx: None,
            stash: None,
            shared: Arc::new(Shared {
                paused: AtomicBool::new(false),
                lost: AtomicBool::new(false),
            }),
        })
    }
}

/// The loop-side protocol behind [`blocking`].
struct BlockingProto {
    f: Handler,
    tx: Option<SyncSender<Vec<u8>>>,
    /// One chunk that did not fit while reading is paused.
    stash: Option<Vec<u8>>,
    shared: Arc<Shared>,
}

impl BlockingProto {
    /// Queues `chunk`, or pauses reading and keeps it when the channel is full.
    fn push(&mut self, t: &mut Transport<'_>, chunk: Vec<u8>) {
        let Some(tx) = &self.tx else { return };
        let chunk = match tx.try_send(chunk) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(c)) => c,
        };
        // Publish "paused" before the last attempt: the thread checks the flag
        // after every receive, so either it sees the flag, or the retry finds
        // the room it freed.
        self.shared.paused.store(true, SeqCst);
        match tx.try_send(chunk) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => {
                self.shared.paused.store(false, SeqCst);
            }
            Err(TrySendError::Full(c)) => {
                self.stash = Some(c);
                t.pause_reading();
            }
        }
    }

    /// The handler thread consumed a chunk while reading was paused.
    fn on_drained(&mut self, t: &mut Transport<'_>) {
        if let Some(c) = self.stash.take() {
            self.push(t, c);
        }
        if self.stash.is_none() {
            t.resume_reading();
        }
    }
}

impl Protocol for BlockingProto {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        let (tx, rx) = sync_channel(DEPTH);
        self.tx = Some(tx);
        let conn = BlockingConn {
            rx,
            handle: t.handle(),
            peer: t.peer(),
            host: t.host(),
            pending_read: Vec::new(),
            pos: 0,
            timeout: None,
            shared: self.shared.clone(),
        };
        let f = self.f.clone();
        let handle = t.handle();
        let spawned = std::thread::Builder::new()
            .name("twine-blocking".into())
            .spawn(move || {
                match catch_unwind(AssertUnwindSafe(|| f(conn))) {
                    Ok(()) => handle.lose_connection(),
                    Err(_) => {
                        log::error!("twine: panic in blocking handler; aborting connection");
                        handle.abort();
                    }
                };
            });
        if let Err(e) = spawned {
            log::error!("twine: spawning blocking handler thread: {e}");
            t.abort();
        }
    }

    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) {
        if data.is_empty() {
            return;
        }
        if let Some(s) = &mut self.stash {
            s.extend_from_slice(data); // not expected while paused; keep order
            return;
        }
        self.push(t, data.to_vec());
    }

    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<crate::Error>) {
        self.shared.lost.store(true, SeqCst);
        self.tx = None; // EOF for the handler once it drained the channel
        self.stash = None;
    }
}

/// A blocking, thread-owned view of one connection: [`io::Read`] yields the
/// received bytes (EOF when the connection is gone), [`io::Write`] queues bytes
/// to the loop. Not tied to the loop thread; never call it from a callback.
pub struct BlockingConn {
    rx: Receiver<Vec<u8>>,
    handle: TransportHandle,
    peer: SocketAddr,
    host: SocketAddr,
    pending_read: Vec<u8>,
    pos: usize,
    timeout: Option<Duration>,
    shared: Arc<Shared>,
}

impl BlockingConn {
    /// The remote address.
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// The local address.
    pub fn host(&self) -> SocketAddr {
        self.host
    }

    /// The connection's cross-thread handle.
    pub fn handle(&self) -> &TransportHandle {
        &self.handle
    }

    /// Gracefully closes after pending writes; the reader then sees EOF.
    pub fn lose_connection(&self) {
        self.handle.lose_connection();
    }

    /// Closes the connection immediately, discarding pending writes.
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// Bounds how long `read` blocks; it then fails with `ErrorKind::TimedOut`.
    /// `None` (the default) blocks until data or EOF.
    pub fn set_read_timeout(&mut self, d: Option<Duration>) {
        self.timeout = d;
    }

    /// Tells the loop the channel has room again, if it paused reading.
    fn signal_drained(&self) {
        if self.shared.paused.swap(false, SeqCst) {
            self.handle
                .call_proto::<BlockingProto>(|p, t| p.on_drained(t));
        }
    }
}

impl io::Read for BlockingConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.pending_read.len() {
            let chunk = match self.timeout {
                None => self.rx.recv().ok(),
                Some(d) => match self.rx.recv_timeout(d) {
                    Ok(c) => Some(c),
                    Err(RecvTimeoutError::Disconnected) => None,
                    Err(RecvTimeoutError::Timeout) => {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out"))
                    }
                },
            };
            let Some(chunk) = chunk else { return Ok(0) };
            self.pending_read = chunk;
            self.pos = 0;
            self.signal_drained();
        }
        let rest = &self.pending_read[self.pos..];
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        self.pos += n;
        Ok(n)
    }
}

impl io::Write for BlockingConn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.shared.lost.load(SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection closed",
            ));
        }
        if !buf.is_empty() {
            self.handle.write(buf.to_vec());
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
