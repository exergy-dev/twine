// Package http is a zero-allocation HTTP/1.1 server for the twine event
// loop.
//
// A Server's Factory builds one protocol object per connection. That object
// embeds the connection's RequestCtx and owns a few byte buffers that grow
// on demand and are reused for every request on the connection, so the
// steady state (keep-alive requests, pipelined requests, Content-Length
// bodies) performs no heap allocation per request:
//
//   - The request line and headers are parsed by a hand-written state
//     machine directly in the loop's shared read buffer. Headers are stored
//     in a fixed array of 64 offset pairs (pointer-free, so the GC does not
//     scan it); Header performs a case-insensitive lookup with a
//     precomputed ASCII lower-case table, no map and no string conversion.
//   - A request that is incomplete at the end of a read is copied into a
//     per-connection reassembly buffer and parsing continues there when more
//     bytes arrive. Complete requests are never copied. A Content-Length
//     body is delivered once it is complete; a chunked body is decoded
//     incrementally into a per-connection buffer.
//   - The Handler writes into a per-connection response buffer. When it
//     returns, the status line, headers (with a per-loop cached Date line,
//     refreshed once per second by a loop timer) and body are appended to an
//     output buffer, and all responses produced from one read are sent with
//     a single Transport.Write. Bodies above 32 KiB go out with Writev next
//     to the head instead of being copied.
//   - Buffers that grew beyond 64 KiB (large bodies) are released once they
//     drain so that idle connections stay small; such requests allocate.
//
// Every []byte a RequestCtx returns is valid only while the Handler runs.
//
// # Protocol behaviour
//
// HTTP/1.1 connections are persistent unless the request or the Handler
// says "Connection: close"; HTTP/1.0 connections close after the response
// unless the request asked for keep-alive. Expect: 100-continue is answered
// when the head arrives without its body. HEAD responses carry the
// Content-Length of the body the Handler wrote but not the body; when the
// Handler wrote nothing, Content-Length is omitted (as in net/http), so a
// HEAD Handler that knows the length should write the body as for GET.
// 204 and 304 responses carry neither. A Handler cannot send a 1xx interim
// response: such a status is sent as 500.
//
// Limits: MaxHeaderBytes (8 KiB) or more than 64 header lines yields 431;
// MaxBodyBytes (4 MiB) yields 413, checked against Content-Length as soon
// as the head arrives, and against the decoded size of a chunked body as it
// arrives; a chunked request whose raw size (framing included) passes
// MaxHeaderBytes + 2*MaxBodyBytes + 64 KiB also yields 413, so no
// connection buffers much more than twice MaxBodyBytes. Malformed requests yield 400, unsupported transfer
// codings 501, unsupported versions 505. Each of these responses carries
// "Connection: close" and the connection is half-closed after it; anything
// the peer sends afterwards is discarded. The parser is deliberately
// strict: bare LF line endings, obs-fold, whitespace before a header colon,
// a request with both Content-Length and Transfer-Encoding, and an HTTP/1.1
// request without Host are all rejected with 400.
//
// Timeouts: a connection with no I/O for IdleTimeout (90 s) is closed, and
// a request still incomplete ReadTimeout (60 s) after its first bytes were
// buffered is answered with 408 and closed, so neither an idle keep-alive
// connection nor a peer trickling a request byte by byte holds a
// connection forever. Negative values disable them. Neither bounds the
// Handler, which runs on the loop and must not block.
//
// Pipelining: at most MaxPipelinedRequests (16) requests are served from one
// read. The remainder is copied into the reassembly buffer, reading is
// paused and serving resumes on the next loop iteration (Loop.CallLater
// with zero delay), so a deep pipeline cannot monopolise the loop. The
// connection also registers itself as a streaming producer: when the
// outbound ring passes the engine's WriteHighWater, reading pauses until it
// drains.
//
// Panics: a panic in the Handler is recovered by the server, logged to
// Server.Logger, and answered with 500 followed by a close. Because the
// response is buffered until the Handler returns, nothing of the failed
// response has been sent at that point. Such panics therefore do not reach
// the core's protocol panic recovery (or its Panics metric); a panic
// elsewhere in the protocol is recovered by the core, which aborts the
// connection.
//
// # net/http compatibility
//
// The package uses no net/http types on its fast path. Adapt runs a
// net/http.Handler on the server at a documented cost: each request
// allocates a *net/http.Request, its URL and Header map, strings for every
// header, a copy of the body and a ResponseWriter (a few dozen allocations
// per request). Use it for migration, not for hot endpoints.
package http
