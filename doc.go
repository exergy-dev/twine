// Package twine is an event-loop networking framework with Twisted's
// programming model (Protocol, Transport, Factory, Deferred-style Futures,
// producers and consumers, reactor timers) on a shared-nothing,
// one-loop-per-core reactor built directly on epoll (Linux) and kqueue
// (macOS and the BSDs).
//
// It targets workloads where a goroutine per connection loses: hundreds of
// thousands to millions of mostly idle connections, very small messages at
// very high rates, and tight tail-latency budgets. For a service with ten
// thousand connections that calls a database per request, use goroutines
// and package net.
//
// # Model
//
// An Engine owns N loops (Loops, default GOMAXPROCS), a worker pool for
// blocking work, and the listeners. Each Loop is one goroutine locked to its
// own OS thread, with its own poller, connections, read buffer, write rings
// and timing wheel; nothing on the hot path is shared between loops. A
// listener accepts on every loop (SO_REUSEPORT on Linux) or on one loop that
// hands connections out round-robin. User code implements Protocol (one
// value per connection, built by a Factory) and talks to the connection
// through its Transport. Engine.Run blocks until its context is cancelled or
// Engine.Stop is called, then shuts down gracefully.
//
// Callbacks never run concurrently with each other on a loop, so protocol
// state needs no locks. In exchange, a callback must not block: it stalls
// every connection on that loop. The watchdog (Watchdog, default 1 ms) logs
// callbacks that run too long, with the loop's stack.
//
// # Ownership rules
//
//  1. A connection belongs to one loop for life. Protocol and Transport
//     methods (except Transport.Loop), Timer.Stop/Reset and Future callbacks
//     run only on that loop.
//  2. Loop.CallFromThread is the only door into a loop from another
//     goroutine: to touch a connection from elsewhere, send a closure to its
//     Loop.
//  3. DeferToThread is the door out for blocking work: it runs a function
//     on the worker pool and settles a Future back on the loop. Blocking
//     wraps a whole Factory so each connection runs on its own goroutine.
//  4. The slice passed to DataReceived (and the slice and address passed to
//     DatagramReceived) are valid only during the call. Copy what you keep.
//  5. Write and Writev never retain their argument; the caller may reuse it
//     as soon as the call returns.
//
// A panic in protocol code aborts only that connection: it reaches
// ConnectionLost as a *PanicError, is logged, and is counted in
// MetricsSnapshot.Panics.
//
// # Build tags and debugging
//
// Set TWINE_DEBUG=1 in the environment, or build with -tags twine_debug, to
// turn on loop-affinity assertions (a Transport method called off its loop
// panics with ErrNotInLoop), poisoning of read buffers and datagram source
// addresses after each callback, and Future creation stacks with warnings
// for unhandled rejections. TWINE_DEBUG_FUTURES=1 enables only the Future
// checks. Build with -tags twine_uring for the experimental Linux io_uring
// readiness backend, which falls back to epoll when io_uring is unavailable.
//
// Metrics are available per loop from Loop.Metrics and Engine.Metrics; the
// metrics subpackage serves them in the Prometheus text format. The proto
// package holds framing protocols (lines, netstrings, length prefixes, AMP)
// and a reconnecting client, twhttp a zero-allocation HTTP/1.1 server, and
// twtls a memory-BIO TLS layer over crypto/tls.
package twine
