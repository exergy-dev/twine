# Twisted to Twine

Twine reproduces Twisted's programming model (Protocol, Transport, Factory,
Deferred, producers and consumers, reactor timers) on a shared-nothing,
one-loop-per-core reactor. This table maps each Twisted name to its Twine
counterpart; the [README](../README.md) teaches the Twine API on its own
terms, and [API.md](API.md) lists the whole exported surface.

| Twisted | Twine | Notes |
| --- | --- | --- |
| `reactor` (global) | `twine.Engine` + N `*twine.Loop` | No global. An Engine owns loops; a Transport knows its Loop. |
| `reactor.run()` | `engine.Run(ctx)` | Blocks until ctx is cancelled or `engine.Stop()`. |
| `reactor.callLater(d, f)` | `loop.CallLater(d, f) *Timer` | Timing wheel, O(1) insert/cancel. |
| `reactor.callFromThread(f)` | `loop.CallFromThread(f)` | Lock-free MPSC queue + coalesced wakeup (eventfd on Linux, `EVFILT_USER` or a pipe on kqueue). |
| `reactor.listenTCP(port, factory)` | `engine.ListenTCP(addr, factory, opts...)` | Linux: one listener per loop via `SO_REUSEPORT`. Elsewhere loop 0 accepts and hands connections to loops round-robin. |
| `reactor.connectTCP(host, port, factory)` | `loop.ConnectTCP(addr, factory) *Future[Transport]` | Non-blocking connect on that loop; the Future resolves once connected. |
| `reactor.connectTCP` (any loop) | `engine.Dial(addr, factory) *Future[Transport]` | `ConnectTCP` on the next loop, round-robin. |
| `reactor.listenUDP(port, proto)` | `engine.ListenUDP(addr, proto)` | Linux: `recvmmsg`/`sendmmsg` batching. Elsewhere one datagram per syscall. |
| `Protocol` | `twine.Protocol` interface | Same three methods: ConnectionMade, DataReceived, ConnectionLost. |
| `Factory` / `ServerFactory` | `twine.Factory` interface | `BuildProtocol(addr) Protocol`. |
| `ReconnectingClientFactory` | `proto.ReconnectingClient` | Exponential backoff with jitter. |
| `ITransport` | `twine.Transport` interface | Write, Writev, LoseConnection, Abort, RemoteAddr, LocalAddr. |
| `IPushProducer` / `IConsumer` | `twine.Producer` / `twine.Consumer` | Same pause/resume/stop contract. |
| `Deferred` | `twine.Future[T]` | Typed, generic. Callbacks run on the owning loop. |
| `Deferred.addCallback/addErrback` | `Future.Then / Future.Catch` | Chainable; returns the same Future. `Map` transforms. |
| `DeferredList` | `twine.All(loop, fs...)` / `twine.Any(loop, fs...)` | The leading `*Loop` owns the combined Future. |
| `threads.deferToThread(f)` | `twine.DeferToThread(loop, f) *Future[T]` | Runs `f` on a bounded worker pool. |
| `inlineCallbacks` / `async def` | goroutine + `Future.Await(ctx)` | Blocking style, only off-loop. |
| `LineReceiver` | `proto.LineReceiver` | Embeddable struct with a `LineReceived` callback. |
| `NetstringReceiver` | `proto.NetstringReceiver` | — |
| `Int32StringReceiver` | `proto.LengthPrefixed` | Configurable 1/2/4-byte prefix and endianness. |
| `AMP` | `proto.AMP` | Request/response multiplexing over length-prefixed frames. |
| `DatagramProtocol` | `twine.DatagramProtocol` | `DatagramReceived(b []byte, addr)`. |
| `reactor.spawnProcess` | out of scope | Use `os/exec` from a goroutine. |
| `twisted.internet.ssl` | `twtls` package | Memory-BIO over `crypto/tls`; handshake off-loop. |
| `reactor.addSystemEventTrigger` | `engine.OnShutdown(f)` | — |
