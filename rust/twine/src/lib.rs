//! Twine: Twisted's programming model (Protocol, Transport, Factory, Deferred,
//! producers and consumers, reactor timers) on a shared-nothing, one-loop-per-core
//! reactor built directly on epoll/kqueue.
//!
//! See `rust/design.md` for the design and `rust/README.md` for the Twisted → Twine
//! concept map. The crate depends on `libc` and `log` only.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]

mod blocking;
mod conn;
mod connect;
mod deferred;
pub mod engine;
pub mod errors;
pub mod event_loop;
pub mod metrics;
pub mod options;
mod pool;
pub mod prometheus;
pub mod protocol;
mod timer;
pub mod transport;
mod udp;
pub mod watchdog;

pub use blocking::{blocking, BlockingConn};
pub use deferred::{all, any, Awaitable, Deferred, LocalResolver, Resolver};
pub use engine::{Engine, EngineHandle, Listener};
pub use errors::{Error, PanicError, Result};
pub use event_loop::{ConnId, Loop, LoopHandle, LoopLocal, TimerId};
pub use metrics::{LoopMetrics, MetricsSnapshot};
pub use options::{DialOptions, ListenOptions, Options};
pub use protocol::{
    factory, AsAny, DatagramProtocol, Factory, Producer, Protocol, PushProducer, WriteFilter,
};
pub use transport::{Payload, Transport, TransportHandle};
pub use udp::DatagramTransport;
