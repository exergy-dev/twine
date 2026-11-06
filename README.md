# Twine

Twine is a Go event-loop networking framework that reproduces Twisted's
programming model (Protocol, Transport, Factory, Deferred, producers and
consumers, reactor timers) on a shared-nothing, one-loop-per-core reactor
built directly on epoll/kqueue.

It targets workloads where goroutine-per-connection loses: hundreds of
thousands to millions of mostly idle connections, very small messages at very
high rates, and tight tail-latency budgets. If your service has 10K
connections and calls a database per request, use goroutines and `net`.

- Go 1.24+, Linux (epoll) and macOS/BSD (kqueue). Windows is out of scope.
- Dependencies: standard library plus `golang.org/x/sys/unix`.
- Design document: [design.md](design.md). Internal contracts: [docs/INTERNALS.md](docs/INTERNALS.md).

```go
type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t; t.SetIdleTimeout(30 * time.Second) }
func (e *echo) DataReceived(b []byte)             { e.t.Write(b) } // copies only if it must buffer
func (e *echo) ConnectionLost(err error)          {}

func main() {
    eng := twine.New(twine.Loops(0)) // 0 = GOMAXPROCS
    eng.ListenTCP(":9000", twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} }))
    log.Fatal(eng.Run(context.Background()))
}
```

## Twisted → Twine

| Twisted | Twine | Notes |
| --- | --- | --- |
| `reactor` (global) | `twine.Engine` + N `*twine.Loop` | No global. An Engine owns loops; a Transport knows its Loop. |
| `reactor.run()` | `engine.Run(ctx)` | Blocks until ctx is cancelled or `engine.Stop()`. |
| `reactor.callLater(d, f)` | `loop.CallLater(d, f) *Timer` | Timing wheel, O(1) insert/cancel. |
| `reactor.callFromThread(f)` | `loop.CallFromThread(f)` | Lock-free MPSC queue + coalesced eventfd wakeup. |
| `reactor.listenTCP(port, factory)` | `engine.ListenTCP(addr, factory, opts...)` | One listener per loop via SO_REUSEPORT. |
| `reactor.connectTCP(host, port, factory)` | `loop.ConnectTCP(addr, factory) *Future[Transport]` | Non-blocking connect, completes on EPOLLOUT. |
| `reactor.listenUDP(port, proto)` | `engine.ListenUDP(addr, proto)` | `recvmmsg`/`sendmmsg` batching. |
| `Protocol` | `twine.Protocol` interface | Same three methods: ConnectionMade, DataReceived, ConnectionLost. |
| `Factory` / `ServerFactory` | `twine.Factory` interface | `BuildProtocol(addr) Protocol`. |
| `ReconnectingClientFactory` | `proto.ReconnectingClient` | Exponential backoff with jitter. |
| `ITransport` | `twine.Transport` interface | Write, Writev, LoseConnection, Abort, GetPeer, GetHost. |
| `IPushProducer` / `IConsumer` | `twine.Producer` / `twine.Consumer` | Same pause/resume/stop contract. |
| `Deferred` | `twine.Future[T]` | Typed, generic. Callbacks run on the owning loop. |
| `Deferred.addCallback/addErrback` | `Future.Then / Future.Catch` | Chainable; returns the same Future. `Map` transforms. |
| `DeferredList` | `twine.All[T](...)` / `twine.Any[T](...)` | — |
| `threads.deferToThread(f)` | `twine.DeferToThread(loop, f) *Future[T]` | Runs `f` on a bounded worker pool. |
| `inlineCallbacks` / `async def` | goroutine + `Future.Await(ctx)` | Blocking style, only off-loop. |
| `LineReceiver` | `proto.LineReceiver` | Embeddable struct with a `LineReceived` callback. |
| `NetstringReceiver` | `proto.NetstringReceiver` | — |
| `Int32StringReceiver` | `proto.LengthPrefixed` | Configurable 1/2/4-byte prefix and endianness. |
| `AMP` | `proto.AMP` | Request/response multiplexing over length-prefixed frames. |
| `DatagramProtocol` | `twine.DatagramProtocol` | `DatagramReceived(b []byte, addr)`. |
| `reactor.spawnProcess` | out of scope | Use `os/exec` from a goroutine. |
| `twisted.internet.ssl` | `tls` package | Memory-BIO over `crypto/tls`; handshake off-loop. |
| `reactor.addSystemEventTrigger` | `engine.OnShutdown(f)` | — |

## Ownership rules (short form)

1. A connection belongs to one loop for life. Protocol and Transport methods,
   Timer.Stop/Reset and Future callbacks run only on that loop's thread.
2. `CallFromThread` is the only door into a loop from another goroutine.
3. `DeferToThread` is the only door out for blocking work.
4. The slice passed to `DataReceived` is dead when the method returns.
5. `Write(b)` never retains `b`; reuse it immediately.

Set `TWINE_DEBUG=1` (or build with `-tags twine_debug`) to turn on
loop-affinity assertions, read-buffer poisoning and Future creation stacks.

## GC tuning

Twine does not set `GOGC` or `GOMEMLIMIT`. For large connection counts we
recommend `GOGC=200`–`400` together with a `GOMEMLIMIT` at roughly 80% of the
container's memory. Per-connection state is pointer-light so GC scan cost
grows slowly with connections; the slabs behind write rings are allocated
once per loop and reused.

## Development

```
make test        # unit + integration tests
make test-race   # same under -race (MPSC stress is shortened with -short)
make lint        # vet, cross-build for darwin/freebsd, golangci-lint, staticcheck
make fuzz        # framing parser fuzzers (FUZZTIME=30s)
make bench       # writes bench/results/<git-sha>.txt
```
