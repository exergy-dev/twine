# Twine (Rust)

Twine is an event-loop networking framework that reproduces Twisted's programming
model (Protocol, Transport, Factory, Deferred, producers and consumers, reactor
timers) on a shared-nothing, one-loop-per-core reactor built directly on
epoll/kqueue. This directory is the Rust port of the Go implementation in the
parent directory; the adapted design is [design.md](design.md) and the internal
crate contracts are [docs/INTERNALS.md](docs/INTERNALS.md).

It targets workloads where thread-per-connection and work-stealing async runtimes
lose: hundreds of thousands to millions of mostly idle connections, very small
messages at very high rates, and tight tail-latency budgets.

- Rust 2021, stable toolchain. Linux (epoll) primary; macOS/BSD (kqueue) compile-checked.
- Core dependencies: `libc` and the `log` facade. `twine-tls` adds `rustls`.
- Loop affinity and read-buffer lifetime are enforced by the borrow checker, not by
  debug assertions: a `Transport<'_>` cannot leave its callback and `Loop` is `!Send`.

```rust
use twine::{Engine, ListenOptions, Protocol, Transport};
use std::time::Duration;

struct Echo;
impl Protocol for Echo {
    fn connection_made(&mut self, t: &mut Transport<'_>) { t.set_idle_timeout(Some(Duration::from_secs(30))); }
    fn data_received(&mut self, t: &mut Transport<'_>, data: &mut [u8]) { t.write(data); } // copies only if it must buffer
}

fn main() -> twine::Result<()> {
    let eng = Engine::builder().loops(0).build()?; // 0 = one loop per CPU
    eng.listen_tcp("0.0.0.0:9000", twine::factory(|_, _| Box::new(Echo)), ListenOptions::default())?;
    eng.run()
}
```

## Twisted → Twine (Rust)

| Twisted | Twine (Rust) | Notes |
| --- | --- | --- |
| `reactor` (global) | `Engine` + N `Loop`s | No global. An Engine owns loops; a Transport knows its Loop. |
| `reactor.run()` | `engine.run()` | Blocks until `stop()`. |
| `reactor.callLater(d, f)` | `loop.call_later(d, f)`; `transport.call_later(d, token)` | Timing wheel, O(1) insert/cancel. |
| `reactor.callFromThread(f)` | `loop_handle.call_from_thread(f)` | Lock-free MPSC queue + coalesced eventfd wakeup. |
| `reactor.listenTCP(port, factory)` | `engine.listen_tcp(addr, factory, opts)` | One listener per loop via SO_REUSEPORT. |
| `reactor.connectTCP(host, port, factory)` | `loop.connect_tcp(addr, build, opts) -> Deferred<ConnId>` | Non-blocking connect, completes on EPOLLOUT. |
| `reactor.listenUDP(port, proto)` | `engine.listen_udp(addr, build, opts)` | `recvmmsg`/`sendmmsg` batching. |
| `Protocol` | `trait Protocol` | `connection_made`, `data_received`, `connection_lost`; the transport is a parameter. |
| `Factory` | `trait Factory` | `build_protocol(&self, loop, peer) -> Box<dyn Protocol>`; `twine::factory(closure)`. |
| `ReconnectingClientFactory` | `twine_proto::ReconnectingClient` | Exponential backoff with jitter. |
| `ITransport` | `Transport<'_>` in callbacks, `TransportHandle` to store | write, writev, lose_connection, abort, peer, host. |
| `IPushProducer` / `IConsumer` | `Producer` / `PushProducer`; `Transport` is the consumer | Same pause/resume/stop contract; notifications arrive after the current callback. |
| `Deferred` | `Deferred<T>` | Typed; callbacks run on the owning loop; `then` returns a new `Deferred<U>`. |
| `DeferredList` | `twine::all` / `twine::any` | — |
| `threads.deferToThread(f)` | `loop.defer_to_thread(f)` | Bounded worker pool. |
| `inlineCallbacks` | `Awaitable<T>` (`wait()` or `.await`, off-loop only) | `wait` on a loop thread panics. |
| `LineReceiver` | `twine_proto::Lines<H>` | Wrapper owning the line buffer. |
| `NetstringReceiver` | `twine_proto::Netstrings<H>` | — |
| `Int32StringReceiver` | `twine_proto::LengthPrefixed<H>` | 1/2/4-byte prefix, either endianness. |
| `AMP` | `twine_proto::Amp` | Twisted-compatible boxes over length-prefixed frames. |
| `DatagramProtocol` | `trait DatagramProtocol` | `datagram_received(t, data, from)`. |
| `twisted.internet.ssl` | `twine-tls` | rustls on the loop; optional kTLS offload. |
| `reactor.addSystemEventTrigger` | `engine.on_shutdown(f)` | — |

## Layout

See design.md Section 5. Internal crates under `twine/internal/` are implementation
details; only `twine`, `twine-proto`, `twine-http` and `twine-tls` are public.

## Developing

```sh
cargo test --workspace                                  # unit, property, loopback integration tests
cargo test --workspace --release -- --ignored           # timing gates and long stress tests
cargo clippy --workspace --all-targets -- -D warnings
cargo check -p twine-poll -p twine-sock --target x86_64-apple-darwin   # kqueue path (compile only)
scripts/ci-local.sh                                     # everything the CI workflow runs
```

Status against the design's phases: [docs/STATUS.md](docs/STATUS.md). Measurements:
[BENCHMARKS.md](BENCHMARKS.md).
