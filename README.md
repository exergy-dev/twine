# Twine

[![ci](https://github.com/exergy-dev/twine/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/exergy-dev/twine/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/exergy-dev/twine.svg)](https://pkg.go.dev/github.com/exergy-dev/twine)

Twine is a Go event-loop networking framework that reproduces Twisted's
programming model (Protocol, Transport, Factory, Deferred, producers and
consumers, reactor timers) on a shared-nothing, one-loop-per-core reactor
built directly on epoll/kqueue.

It targets workloads where goroutine-per-connection loses: hundreds of
thousands to millions of mostly idle connections, very small messages at very
high rates, and tight tail-latency budgets. If your service has 10K
connections and calls a database per request, use goroutines and `net`.

**Status:** v0.x. The API may change before v1.0.0; every breaking change is
listed in [CHANGELOG.md](CHANGELOG.md). Bugs and questions go to the
[issue tracker](https://github.com/exergy-dev/twine/issues).

```
go get github.com/exergy-dev/twine@latest
```

- Go 1.24+. Dependencies: standard library plus `golang.org/x/sys/unix`.
- Platforms: Linux (epoll): primary, tested in CI. macOS (kqueue): tested in
  CI. FreeBSD/NetBSD/OpenBSD/DragonFly: compile-only. Windows: unsupported.

```go
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/exergy-dev/twine"
)

type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t; t.SetIdleTimeout(30 * time.Second) }
func (e *echo) DataReceived(b []byte)            { e.t.Write(b) } // copies only if it must buffer
func (e *echo) ConnectionLost(err error)         {}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	eng := twine.New() // one loop per core
	if _, err := eng.ListenTCP(":9000", twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} })); err != nil {
		log.Fatal(err)
	}
	if err := eng.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Twisted → Twine

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
| `ITransport` | `twine.Transport` interface | Write, Writev, LoseConnection, Abort, GetPeer, GetHost. |
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
| `twisted.internet.ssl` | `tls` package | Memory-BIO over `crypto/tls`; handshake off-loop. |
| `reactor.addSystemEventTrigger` | `engine.OnShutdown(f)` | — |

## Packages

| Package | What it holds |
| --- | --- |
| `twine` | Engine, Loop, Protocol/Transport/Factory, Future, timers, CallFromThread, DeferToThread, UDP, per-loop metrics, watchdog, `Blocking` adapter |
| `metrics` | Prometheus text-format handler over the engine's per-loop metrics (kept out of the root package so `net/http` is linked only when you use it) |
| `proto` | `LineReceiver`, `NetstringReceiver`, `LengthPrefixed`, `AMP`, `ReconnectingClient`, `Broadcast` |
| `http` | HTTP/1.1 server with no per-request allocation in the steady state (`Server`, `RequestCtx`), plus `Adapt(net/http.Handler)`, a migration bridge that allocates per request |
| `tls` | Memory-BIO TLS over `crypto/tls` (`Server`, `Client`, `Listen`, `Connect`) and opt-in kTLS offload |
| `internal/*` | poller (epoll/kqueue/io_uring), sockets, slab allocator, write ring, MPSC queue, timing wheel |
| `bench` | separate module with the comparison harness (goroutine-per-conn, gnet, net/http); Linux only |
| `examples` | `echo`, `chat`, `http_hello`, `udp_echo` and `client_reconnect` |

The `http` and `tls` packages share their names with `net/http` and
`crypto/tls`. Import them as `twhttp` and `twtls`, so the standard packages
keep their usual names:

```go
import (
	"crypto/tls"
	"net/http"

	twhttp "github.com/exergy-dev/twine/http"
	twtls "github.com/exergy-dev/twine/tls"
)
```

Build tags: `twine_debug` (assertions, buffer poisoning, Future stacks) and
`twine_uring` (io_uring readiness backend; falls back to epoll when io_uring
is unavailable).

Two features are **Linux-only and experimental**:

- io_uring (`-tags twine_uring`). CI runs the root and `internal/poll` tests
  with the tag on GitHub's Ubuntu runners.
- kTLS (`twtls.KernelOffload(true)`): the kernel does the record encryption
  for TLS 1.3 connections using AES-GCM. The record keys are read from
  `crypto/tls` by reflection and checked at runtime on every access; any
  mismatch, TLS 1.2, any other cipher suite (including ChaCha20-Poly1305) or
  a kernel refusal stays on the memory BIO. See `tls/doc.go` for the exact
  scope. CI loads the kernel `tls` module on an Ubuntu runner and fails if
  the kTLS tests cannot offload.

## Ownership rules (short form)

1. A connection belongs to one loop for life. Protocol and Transport methods,
   Timer.Stop/Reset, Loop.CallLater, Loop.Now and Future callbacks run only
   on that loop's thread.
2. `CallFromThread` is the only door into a loop from another goroutine.
3. `DeferToThread` is the only door out for blocking work.
4. The slice passed to `DataReceived` is dead when the method returns, and
   so are the slice and the address passed to `DatagramReceived`; copy what
   you keep.
5. `Write(b)` never retains `b`; reuse it immediately.

`Engine.Stop` closes listeners, runs `OnShutdown` hooks once on the goroutine
that called `Run` (while loops are still live, so hooks may `CallFromThread`),
then asks every loop to `LoseConnection` its connections, flush for up to
`ShutdownTimeout`, and exit. `LoseConnection` half-closes (SHUT_WR) after the
flush and waits up to `CloseLinger` (default 10 s) for the peer to close.

Set `TWINE_DEBUG=1` (or build with `-tags twine_debug`) to turn on
loop-affinity assertions, read-buffer poisoning and Future creation stacks.

## GC tuning

Twine does not set `GOGC` or `GOMEMLIMIT`. For large connection counts we
recommend `GOGC=200`–`400` together with a `GOMEMLIMIT` at roughly 80% of the
container's memory. Per-connection state is pointer-light so GC scan cost
grows slowly with connections; the slabs behind write rings are allocated
once per loop and reused.

## Documentation

- [pkg.go.dev](https://pkg.go.dev/github.com/exergy-dev/twine): API
  reference with runnable examples.
- [docs/API.md](docs/API.md): the whole exported surface on one page.
- [docs/INTERNALS.md](docs/INTERNALS.md): internal contracts.
- [BENCHMARKS.md](BENCHMARKS.md): measured results against the targets.
- [examples/](examples/): runnable programs.
- [CHANGELOG.md](CHANGELOG.md): what changed in each release.
- [Design rationale (historical)](docs/history/design.md): the original
  design document, not maintained.

## Development

```
make test        # unit + integration tests
make test-race   # the same under -race (CI adds TESTFLAGS=-short to shorten the MPSC stress)
make test-debug  # with -tags twine_debug
make test-uring  # with -tags twine_uring (Linux io_uring backend)
make lint        # vet, crossbuild, golangci-lint, staticcheck (LINT_STRICT=1 fails on a missing tool)
make crossbuild  # build and vet for darwin, freebsd, netbsd, openbsd and dragonfly
make fuzz        # parser fuzzers (FUZZTIME=30s each)
make api         # regenerate docs/API.md (CI fails when it is stale)
make bench       # writes bench/results/<git-sha>.txt
```

CI runs these same targets. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
module layout and what a change needs, and [SECURITY.md](SECURITY.md) to
report a vulnerability privately.

## License

MIT; see [LICENSE](LICENSE).
