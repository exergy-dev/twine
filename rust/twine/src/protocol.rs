//! The user-facing traits: [`Protocol`], [`Factory`], [`PushProducer`],
//! [`WriteFilter`] and [`DatagramProtocol`] (design Sections 6.1 and 6.5), and
//! the [`Producer`] enum.

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::errors::Error;
use crate::event_loop::{ConnId, Loop};
use crate::transport::Transport;
pub use crate::udp::DatagramTransport;

/// Blanket-implemented downcast support for [`Protocol`].
pub trait AsAny {
    /// `self` as `&dyn Any`.
    fn as_any(&self) -> &dyn Any;
    /// `self` as `&mut dyn Any`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Any> AsAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Implemented by user code. All methods run on the owning loop's thread; the
/// transport is passed in, never stored (store a `TransportHandle` instead).
pub trait Protocol: AsAny + 'static {
    /// Called once, after the transport is ready.
    fn connection_made(&mut self, _t: &mut Transport<'_>) {}

    /// Called with a sub-slice of the loop's shared read buffer. `data` is valid
    /// only for the duration of the call (enforced by the borrow checker); it is
    /// mutable so parsers and TLS can transform it in place.
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]);

    /// Called exactly once. `reason` is `None` on a clean close. The transport is
    /// already closed: writes are counted no-ops; `id()`, `peer()` and
    /// `loop_mut()` still work.
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _reason: Option<Error>) {}

    /// Producer hook, used when this protocol is registered as `Producer::This`
    /// (or as `Producer::Conn(id)` of another connection): stop producing.
    fn pause_producing(&mut self, _t: &mut Transport<'_>) {}

    /// Producer hook: resume producing.
    fn resume_producing(&mut self, _t: &mut Transport<'_>) {}

    /// Producer hook: the consumer is gone; stop for good.
    fn stop_producing(&mut self, _t: &mut Transport<'_>) {}

    /// A connection-scoped timer armed with `Transport::call_later(d, token)` fired.
    fn timer_fired(&mut self, _t: &mut Transport<'_>, _token: u64) {}

    /// Runs `f` against the innermost protocol with the view it normally sees.
    /// Wrapper protocols (TLS) override this to forward with a filtered view.
    fn visit(
        &mut self,
        t: &mut Transport<'_>,
        f: &mut dyn FnMut(&mut dyn Any, &mut Transport<'_>),
    ) {
        f(self.as_any_mut(), t)
    }

    /// Name reported by the watchdog and metrics. Default: `std::any::type_name::<Self>()`.
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Builds a [`Protocol`] per accepted or connected transport. Called on the
/// owning loop, so it may hand out per-loop state via `lp.local(key)`.
pub trait Factory: Send + Sync + 'static {
    /// Creates the protocol for a new connection from `peer`.
    fn build_protocol(&self, lp: &mut Loop, peer: SocketAddr) -> Box<dyn Protocol>;
}

impl<F> Factory for F
where
    F: Fn(&mut Loop, SocketAddr) -> Box<dyn Protocol> + Send + Sync + 'static,
{
    fn build_protocol(&self, lp: &mut Loop, peer: SocketAddr) -> Box<dyn Protocol> {
        self(lp, peer)
    }
}

impl Factory for Arc<dyn Factory> {
    fn build_protocol(&self, lp: &mut Loop, peer: SocketAddr) -> Box<dyn Protocol> {
        (**self).build_protocol(lp, peer)
    }
}

/// Pins a closure's signature so `factory(|_, _| Box::new(Echo))` infers.
pub fn factory<F>(f: F) -> F
where
    F: Fn(&mut Loop, SocketAddr) -> Box<dyn Protocol> + Send + Sync + 'static,
{
    f
}

/// Who gets pause/resume/stop notifications for a connection's outbound ring.
pub enum Producer {
    /// This connection's own protocol (`Protocol::{pause,resume,stop}_producing`).
    This,
    /// Another connection's protocol on the same loop, called with *its* transport.
    Conn(ConnId),
    /// Built in: pause/resume reading of that connection (proxy pattern), no user code.
    PauseReading(ConnId),
    /// A standalone producer (file sender, ...).
    Boxed(Box<dyn PushProducer>),
}

/// A standalone producer registered with `Transport::register_producer`
/// (Twisted's `IPushProducer`). `consumer` is the connection being fed.
pub trait PushProducer: 'static {
    /// The outbound buffer is above the high-water mark: stop producing.
    fn pause_producing(&mut self, consumer: &mut Transport<'_>);
    /// The outbound buffer drained below the low-water mark (or, for a pull
    /// producer, drained completely): produce more.
    fn resume_producing(&mut self, consumer: &mut Transport<'_>);
    /// The consumer is closing; release resources.
    fn stop_producing(&mut self, consumer: &mut Transport<'_>);
}

/// Implemented by wrapper protocols (TLS) to transform outbound bytes.
pub trait WriteFilter: 'static {
    /// Transforms and writes `plain` to the raw transport.
    fn write(&mut self, raw: &mut Transport<'_>, plain: &[u8]);
    /// Transforms and writes several slices.
    fn writev(&mut self, raw: &mut Transport<'_>, bufs: &[&[u8]]) {
        for b in bufs {
            self.write(raw, b)
        }
    }
    /// Closes the connection after pending data (default: the raw close).
    fn lose_connection(&mut self, raw: &mut Transport<'_>) {
        raw.lose_connection()
    }
    /// Bytes pending below the filter (default: the raw ring).
    fn pending_write(&self, raw: &Transport<'_>) -> usize {
        raw.pending_write()
    }
}

/// UDP receiver (design Section 6.5). The datagram machinery is implemented in
/// `udp.rs`.
pub trait DatagramProtocol: 'static {
    /// Called once when the socket is registered with its loop.
    fn start_protocol(&mut self, _t: &mut DatagramTransport<'_>) {}
    /// `data` is valid only for the call.
    fn datagram_received(
        &mut self,
        t: &mut DatagramTransport<'_>,
        data: &mut [u8],
        from: SocketAddr,
    );
    /// Called once when the socket closes.
    fn stop_protocol(&mut self) {}
}
