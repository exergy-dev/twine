# Twine

[![ci](https://github.com/exergy-dev/twine/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/exergy-dev/twine/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/exergy-dev/twine.svg)](https://pkg.go.dev/github.com/exergy-dev/twine)

Twine is a Go event-loop networking framework: one loop per core, built
directly on epoll/kqueue, with a Protocol/Transport programming model in the
style of Twisted.

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

## An echo server

A `Protocol` has three methods; the `Transport` passed to `ConnectionMade` is
the connection. The engine runs one loop per core, each connection lives on
one of them, and every callback for a connection runs on that loop.

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
	if _, err := eng.ListenTCP(":9000", func(net.Addr) twine.Protocol { return &echo{} }); err != nil {
		log.Fatal(err)
	}
	if err := eng.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Every snippet below is an Example in this repository's test files, so it
compiles and, where it has an output, runs in CI.

## A client

`Engine.Dial` returns a `Future`. From `main` (or any goroutine that is not a
loop), `Await` blocks until the connection is made; after that, Transport
methods run on the connection's loop, entered through `CallFromThread`. Call
`dial` once `eng.Run` is going in another goroutine.

```go
type printer struct{ t twine.Transport }

func (p *printer) ConnectionMade(t twine.Transport) { p.t = t }
func (p *printer) DataReceived(b []byte)            { fmt.Printf("reply: %s", b); p.t.Close() }
func (p *printer) ConnectionLost(error)             {}

func dial(eng *twine.Engine, addr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t, err := eng.Dial(addr, func(net.Addr) twine.Protocol { return &printer{} }).Await(ctx)
	if err != nil {
		log.Fatal(err)
	}
	t.Loop().CallFromThread(func() { t.Write([]byte("hello\n")) }) // Transport methods run on its loop
}
```

## A timer

`Loop.CallLater` arms a timer on the loop's timing wheel (O(1), no
allocation in the steady state) and must be called on that loop. Inside a
callback you are already there; from anywhere else, go through
`CallFromThread`:

```go
// after runs fn on loop once d has passed. CallLater is loop-only, so another
// goroutine enters the loop through CallFromThread first.
func after(loop *twine.Loop, d time.Duration, fn func()) {
	loop.CallFromThread(func() { loop.CallLater(d, fn) })
}
```

## Blocking work

Nothing on a loop may block. `DeferToThread` runs a function on the engine's
worker pool and delivers its result back on the loop as a `Future`, so the
callbacks may use the Transport again:

```go
type resolver struct{ t twine.Transport }

func (r *resolver) ConnectionMade(t twine.Transport) { r.t = t }
func (r *resolver) ConnectionLost(error)             {}

// The lookup blocks, so it runs on the worker pool and answers on the loop.
func (r *resolver) DataReceived(b []byte) {
	host := strings.TrimSpace(string(b))
	lookup := func() ([]string, error) { return net.LookupHost(host) }
	twine.DeferToThread(r.t.Loop(), lookup).Then(func(addrs []string) {
		r.t.Write([]byte(strings.Join(addrs, " ") + "\n"))
	}).Catch(func(err error) {
		r.t.Write([]byte(err.Error() + "\n"))
	})
}
```

## An HTTP server

The HTTP and TLS packages are named `twhttp` and `twtls`, so they sit next
to `net/http` and `crypto/tls` in an import block without aliases:

```go
import (
	"crypto/tls"
	"net/http"

	"github.com/exergy-dev/twine/twhttp"
	"github.com/exergy-dev/twine/twtls"
)
```

`twhttp.Server` is an HTTP/1.1 server with no per-request allocation in the
steady state. The `Handler` gets a `RequestCtx` that is reused for every
request on its connection; the response is whatever it holds when the
handler returns.

```go
func main() {
	eng := twine.New()
	srv := &twhttp.Server{Handler: func(ctx *twhttp.RequestCtx) {
		ctx.SetContentType("text/plain")
		ctx.WriteString("hello from twine\n")
	}}
	if _, err := srv.Listen(eng, ":8080"); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}
```

## A TLS listener

`twtls.Server` wraps a plaintext `Factory` for `ListenTCP`: the `echo`
Protocol above runs unchanged, seeing decrypted bytes. The handshake runs off
the loop. Like `crypto/tls.Listen`, it refuses a config with no certificate
(`Server` panics).

```go
func main() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal(err)
	}
	eng := twine.New()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	factory := func(net.Addr) twine.Protocol { return &echo{} }
	if _, err := eng.ListenTCP(":8443", twtls.Server(cfg, factory)); err != nil {
		log.Fatal(err)
	}
	log.Fatal(eng.Run(context.Background()))
}
```

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
then asks every loop to `Close` its connections, flush for up to
`ShutdownTimeout`, and exit. Unlike `net.Conn.Close`, `Transport.Close` is
graceful: it flushes pending writes, half-closes (SHUT_WR) and waits up to
`CloseLinger` (default 10 s) for the peer to close; `Abort` drops pending
writes and closes at once.

Set `TWINE_DEBUG=1` (or build with `-tags twine_debug`) to turn on
loop-affinity assertions, read-buffer poisoning and Future creation stacks.

## Packages

| Package | What it holds |
| --- | --- |
| `twine` | Engine, Loop, Protocol/Transport/Factory, Future, timers, CallFromThread, DeferToThread, UDP, per-loop metrics, watchdog, `Blocking` adapter |
| `metrics` | Prometheus text-format handler over the engine's per-loop metrics (kept out of the root package so `net/http` is linked only when you use it) |
| `proto` | `LineReceiver`, `NetstringReceiver`, `LengthPrefixed`, `AMP`, `ReconnectingClient`, `Broadcast` |
| `twhttp` | HTTP/1.1 server with no per-request allocation in the steady state (`Server`, `RequestCtx`), plus `Adapt(net/http.Handler)`, a migration bridge that allocates per request |
| `twtls` | Memory-BIO TLS over `crypto/tls` (`Server`, `Client`, `Dial`, `Connect`) and opt-in kTLS offload |
| `internal/*` | poller (epoll/kqueue/io_uring), sockets, slab allocator, write ring, MPSC queue, timing wheel |
| `bench` | separate module with the comparison harness (goroutine-per-conn, gnet, net/http); Linux only |
| `examples` | `echo`, `chat`, `http_hello`, `udp_echo` and `client_reconnect` |

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
  a kernel refusal stays on the memory BIO. See `twtls/doc.go` for the exact
  scope. CI loads the kernel `tls` module on an Ubuntu runner and fails if
  the kTLS tests cannot offload.

## Documentation

- [pkg.go.dev](https://pkg.go.dev/github.com/exergy-dev/twine): API
  reference with runnable examples.
- [docs/API.md](docs/API.md): the whole exported surface on one page.
- [docs/TWISTED.md](docs/TWISTED.md): the Twisted-to-Twine name mapping.
- [docs/INTERNALS.md](docs/INTERNALS.md): internal contracts.
- [BENCHMARKS.md](BENCHMARKS.md): measured results against the targets, and
  GC tuning for large connection counts.
- [examples/](examples/): runnable programs.
- [CHANGELOG.md](CHANGELOG.md): what changed in each release.
- [Design rationale (historical)](docs/history/design.md): the original
  design document, not maintained.

## Development

`make lint test` runs what CI runs; [CONTRIBUTING.md](CONTRIBUTING.md) lists
every make target, the module layout and what a change needs, and
[SECURITY.md](SECURITY.md) says how to report a vulnerability privately.

## License

MIT; see [LICENSE](LICENSE).
