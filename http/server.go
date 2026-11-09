package http

import (
	"log/slog"
	"net"
	nethttp "net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/exergy-dev/twine"
)

// Default limits.
const (
	// DefaultMaxHeaderBytes bounds the request line plus headers. Larger
	// heads are answered with 431 and the connection is closed.
	DefaultMaxHeaderBytes = 8 << 10
	// DefaultMaxBodyBytes bounds a request body (Content-Length or decoded
	// chunked). Larger bodies are answered with 413 and the connection is
	// closed.
	DefaultMaxBodyBytes = 4 << 20
	// DefaultMaxPipelinedRequests bounds the requests served from one read
	// before the connection yields to the loop.
	DefaultMaxPipelinedRequests = 16
	// DefaultMaxResponseBytes bounds a response body. It equals the
	// engine's default twine.WriteBufferLimit.
	DefaultMaxResponseBytes = 16 << 20
)

// Default timeouts.
const (
	// DefaultIdleTimeout closes a connection with no I/O for this long.
	DefaultIdleTimeout = 90 * time.Second
	// DefaultReadTimeout bounds how long a request may take to arrive once
	// its first bytes are buffered.
	DefaultReadTimeout = 60 * time.Second
)

// maxRetained is the largest buffer a connection keeps between requests.
// Buffers grow as needed and are reused; one that grew past this (a large
// body or response) is released when it drains so idle connections stay
// small.
const maxRetained = 64 << 10

// largeBody is the response body size above which the body is handed to
// Writev next to the head instead of being copied after it.
const largeBody = 32 << 10

// Server is an HTTP/1.1 server running on a twine Engine. Configure the
// exported fields, then call Factory or Listen; fields must not change
// afterwards. One Server may serve any number of listeners and engines.
type Server struct {
	// Handler is called on the connection's loop for every request. It
	// must not block (use twine.DeferToThread for blocking work, though the
	// response is still sent when Handler returns). A nil Handler answers
	// 404.
	Handler func(ctx *RequestCtx)

	// Name, when set, is sent as the Server response header.
	Name string

	// MaxHeaderBytes bounds the request head (default
	// DefaultMaxHeaderBytes). Exceeding it, or sending more than 64 header
	// lines, yields 431.
	MaxHeaderBytes int

	// MaxBodyBytes bounds the request body (default DefaultMaxBodyBytes).
	// Exceeding it yields 413, as does a chunked request whose raw size
	// passes MaxHeaderBytes + 2*MaxBodyBytes + 64 KiB.
	MaxBodyBytes int

	// MaxPipelinedRequests bounds how many requests are served from one
	// read (default DefaultMaxPipelinedRequests). The rest stay buffered,
	// reading is paused and serving continues on the next loop iteration.
	MaxPipelinedRequests int

	// MaxResponseBytes bounds the response body a Handler may produce
	// (default DefaultMaxResponseBytes). The whole response is queued on
	// the connection at once, and the engine aborts a connection whose
	// pending output passes its WriteBufferLimit (16 MiB by default), which
	// would cut the body off after Content-Length was sent. A larger body
	// is therefore replaced by a 500 response, the connection is closed
	// after it, and the event is logged. When you lower
	// twine.WriteBufferLimit, set this below it, leaving room for headers
	// and pipelined responses. HEAD responses, which carry no body, are
	// exempt.
	MaxResponseBytes int

	// IdleTimeout closes a connection that has had no I/O for this long
	// (default DefaultIdleTimeout when zero; negative disables it). It is
	// the engine's per-connection idle timer, so it bounds idle keep-alive
	// connections but restarts on every byte a slow peer sends.
	IdleTimeout time.Duration

	// ReadTimeout bounds how long one request may take to arrive once its
	// first bytes are buffered (default DefaultReadTimeout when zero;
	// negative disables it). A request still incomplete when it expires is
	// answered with 408 and the connection is closed. Unlike IdleTimeout it
	// does not restart on activity, so a peer trickling a head or body byte
	// by byte cannot hold the connection and its reassembly buffer. The
	// timer is armed only while an incomplete request is buffered: requests
	// that arrive in one read never touch it.
	ReadTimeout time.Duration

	// Logger receives Handler panics and responses over MaxResponseBytes.
	// Defaults to slog.Default().
	Logger *slog.Logger

	once        sync.Once
	maxHdr      int
	maxBody     int
	maxPipe     int
	maxRaw      int
	maxResp     int
	idleTimeout time.Duration
	readTimeout time.Duration
	serverLine  []byte
	logger      *slog.Logger

	mu    sync.Mutex
	dates map[*twine.Loop]*dateState
}

func (s *Server) init() {
	s.once.Do(func() {
		s.maxHdr = s.MaxHeaderBytes
		if s.maxHdr <= 0 {
			s.maxHdr = DefaultMaxHeaderBytes
		}
		s.maxBody = s.MaxBodyBytes
		if s.maxBody <= 0 {
			s.maxBody = DefaultMaxBodyBytes
		}
		s.maxPipe = s.MaxPipelinedRequests
		if s.maxPipe <= 0 {
			s.maxPipe = DefaultMaxPipelinedRequests
		}
		// Raw bytes an incomplete chunked request may occupy: the head,
		// the payload, and framing up to the payload size again. A
		// Content-Length request needs no raw cap: its length is checked
		// against maxBody as soon as the head is parsed.
		s.maxRaw = s.maxHdr + 2*s.maxBody + 64<<10
		s.maxResp = s.MaxResponseBytes
		if s.maxResp <= 0 {
			s.maxResp = DefaultMaxResponseBytes
		}
		s.idleTimeout = orDefault(s.IdleTimeout, DefaultIdleTimeout)
		s.readTimeout = orDefault(s.ReadTimeout, DefaultReadTimeout)
		if s.Name != "" {
			s.serverLine = appendSafe(append([]byte(nil), "Server: "...), s.Name)
			s.serverLine = append(s.serverLine, '\r', '\n')
		}
		s.logger = s.Logger
		if s.logger == nil {
			s.logger = slog.Default()
		}
	})
}

// orDefault maps a timeout setting to its effective value: 0 means def and
// a negative value means disabled (0).
func orDefault(d, def time.Duration) time.Duration {
	switch {
	case d == 0:
		return def
	case d < 0:
		return 0
	}
	return d
}

// Factory returns a twine.Factory that serves HTTP on every connection it
// builds. Each connection allocates its protocol state (including its
// RequestCtx) once; requests on it allocate nothing.
func (s *Server) Factory() twine.Factory {
	s.init()
	return twine.FactoryFunc(s.buildProtocol)
}

// Listen binds addr on e and serves HTTP on it. It is shorthand for
// e.ListenTCP(addr, s.Factory(), opts...).
func (s *Server) Listen(e *twine.Engine, addr string, opts ...twine.ListenOption) (twine.Listener, error) {
	lst, err := e.ListenTCP(addr, s.Factory(), opts...)
	if err != nil {
		return nil, err
	}
	loops := e.Loops()
	e.OnShutdown(func() { s.forgetLoops(loops) })
	return lst, nil
}

func (s *Server) buildProtocol(net.Addr) twine.Protocol {
	c := &conn{s: s}
	c.ctx.c = c
	c.ctx.req.contentLength = -1
	c.resumeFn = c.resume
	c.readTimeoutFn = c.onReadTimeout
	return c
}

// conn is the per-connection protocol. It embeds the RequestCtx so that a
// connection costs one allocation.
type conn struct {
	s    *Server
	t    twine.Transport
	loop *twine.Loop
	date *dateState
	ctx  RequestCtx

	cs       chunkState
	headDone bool // the current request's head is parsed
	cont     bool // 100 Continue sent for the current request

	buf       []byte // incomplete request bytes (reassembly)
	chunkBody []byte // decoded chunked body
	wbuf      []byte // responses not yet written
	iov       [2][]byte

	closed        bool
	resumePending bool // a CallLater(0) continuation is scheduled
	prodPaused    bool // the transport asked us to stop producing
	resumeFn      func()

	// ReadTimeout state. readDeadline (loop clock, UnixNano) is set when an
	// incomplete request starts being buffered and cleared when a request
	// completes. readTimer is armed at most once per ReadTimeout and is not
	// stopped when a request completes: it checks readDeadline when it
	// fires and re-arms for the remainder or does nothing.
	readDeadline  int64
	readTimer     *twine.Timer
	readTimeoutFn func()
}

var _ twine.Protocol = (*conn)(nil)
var _ twine.Producer = (*conn)(nil)

// ConnectionMade implements twine.Protocol.
func (c *conn) ConnectionMade(t twine.Transport) {
	c.t = t
	c.loop = t.Loop()
	c.date = c.s.dateFor(c.loop)
	if c.s.idleTimeout > 0 {
		t.SetIdleTimeout(c.s.idleTimeout)
	}
	t.RegisterProducer(c, true)
}

// startReadTimer starts the ReadTimeout clock for the incomplete request
// now in c.buf, unless it is already running. It runs only on the
// reassembly path.
func (c *conn) startReadTimer() {
	if c.s.readTimeout <= 0 || c.readDeadline != 0 {
		return
	}
	c.readDeadline = c.loop.Now().UnixNano() + int64(c.s.readTimeout)
	if c.readTimer == nil {
		c.readTimer = c.loop.CallLater(c.s.readTimeout, c.readTimeoutFn)
	}
}

// onReadTimeout is the ReadTimeout timer callback.
func (c *conn) onReadTimeout() {
	c.readTimer = nil
	if c.closed || c.readDeadline == 0 {
		return // the request completed; the next one restarts the clock
	}
	now := c.loop.Now().UnixNano()
	if c.resumePending || c.prodPaused {
		// The server, not the peer, is behind: buffered requests wait for
		// a continuation or for the client to drain responses.
		c.readDeadline = now + int64(c.s.readTimeout)
	}
	if remain := c.readDeadline - now; remain > 0 {
		c.readTimer = c.loop.CallLater(time.Duration(remain), c.readTimeoutFn)
		return
	}
	c.sendError(nethttp.StatusRequestTimeout)
}

// ConnectionLost implements twine.Protocol.
func (c *conn) ConnectionLost(error) {
	c.closed = true
	if c.date != nil {
		c.date.release()
	}
	if c.readTimer != nil {
		c.readTimer.Stop()
		c.readTimer = nil
	}
	c.buf, c.chunkBody, c.wbuf = nil, nil, nil
	c.ctx.out, c.ctx.hdrs = nil, nil
}

// PauseProducing implements twine.Producer: the outbound ring is above its
// high-water mark, so stop reading (and therefore producing responses).
func (c *conn) PauseProducing() {
	if c.prodPaused || c.closed {
		return
	}
	c.prodPaused = true
	c.t.PauseReading()
}

// ResumeProducing implements twine.Producer.
func (c *conn) ResumeProducing() {
	if !c.prodPaused {
		return
	}
	c.prodPaused = false
	c.schedule()
}

// StopProducing implements twine.Producer.
func (c *conn) StopProducing() {}

// schedule continues serving buffered requests on the next iteration.
func (c *conn) schedule() {
	if c.resumePending || c.closed {
		return
	}
	c.resumePending = true
	c.loop.CallLater(0, c.resumeFn)
}

// resume serves requests buffered by a pipelining yield or a producer
// pause, then resumes reading.
func (c *conn) resume() {
	c.resumePending = false
	if c.closed || c.prodPaused {
		return
	}
	if len(c.buf) > 0 {
		c.processBuf()
		c.flushOut()
	}
	if !c.resumePending && !c.prodPaused && !c.closed {
		c.t.ResumeReading()
	}
}

// DataReceived implements twine.Protocol. Complete requests are parsed in
// place in b; only an incomplete trailing request is copied.
func (c *conn) DataReceived(b []byte) {
	if c.closed {
		return // closing: discard whatever the peer still sends
	}
	if len(c.buf) > 0 || c.resumePending || c.prodPaused {
		c.buf = append(c.buf, b...)
		if !c.resumePending && !c.prodPaused {
			c.processBuf()
		}
	} else {
		n := c.process(b)
		if !c.closed && n < len(b) {
			c.buf = append(c.buf[:0], b[n:]...)
			c.reserve()
			c.startReadTimer()
		}
	}
	c.flushOut()
}

// processBuf serves what it can from c.buf and compacts the rest.
func (c *conn) processBuf() {
	n := c.process(c.buf)
	if c.closed {
		c.buf = c.buf[:0]
		return
	}
	if n > 0 {
		c.readDeadline = 0 // a request completed
	}
	rest := copy(c.buf, c.buf[n:])
	c.buf = c.buf[:rest]
	if rest == 0 {
		if cap(c.buf) > maxRetained {
			c.buf = nil
		}
		return
	}
	c.reserve()
	c.startReadTimer()
}

// reserve grows c.buf once to hold a Content-Length request whose head is
// parsed, so the body does not trigger repeated append growth.
func (c *conn) reserve() {
	r := &c.ctx.req
	if !c.headDone || r.chunked || r.contentLength <= 0 {
		return
	}
	// Pre-allocate at most 64 KiB beyond what has arrived: a declared
	// Content-Length is attacker-controlled, so reserving it in full would
	// let a 60-byte request pin MaxBodyBytes of heap per connection.
	need := r.headLen + int(r.contentLength)
	if lim := len(c.buf) + 64<<10; need > lim {
		need = lim
	}
	if cap(c.buf) < need {
		nb := make([]byte, len(c.buf), need)
		copy(nb, c.buf)
		c.buf = nb
	}
}

// process serves complete requests from data and returns the bytes
// consumed. It stops at an incomplete request, after MaxPipelinedRequests
// (scheduling a continuation), on a producer pause, or on close.
func (c *conn) process(data []byte) int {
	off := 0
	served := 0
	for !c.closed && off < len(data) && !c.prodPaused {
		if served >= c.s.maxPipe {
			c.t.PauseReading()
			c.schedule()
			break
		}
		n, code := c.parseOne(data[off:])
		if code == parseNeedMore {
			break
		}
		if code != parseOK {
			c.sendError(code)
			break
		}
		c.serve(data[off : off+n])
		off += n
		served++
		if c.ctx.close {
			c.closeConn()
		}
	}
	if c.closed {
		return len(data)
	}
	return off
}

// parseOne parses (or continues parsing) the request at the start of data.
// It returns the request length and parseOK, parseNeedMore, or an error
// status code.
func (c *conn) parseOne(data []byte) (int, int) {
	s := c.s
	r := &c.ctx.req
	if !c.headDone {
		// An incomplete head is parsed again from its start when more bytes
		// arrive; heads are at most MaxHeaderBytes, so this is cheap and it
		// keeps error detection independent of how the bytes were split.
		h, code := r.parseHead(data, s.maxHdr)
		if code != parseOK {
			return 0, code
		}
		if r.contentLength > int64(s.maxBody) {
			return 0, 413
		}
		c.headDone = true
		if r.chunked {
			c.cs = chunkState{off: h}
			c.chunkBody = c.chunkBody[:0]
		}
	}
	var total int
	if r.chunked {
		n, code := c.cs.decode(data, &c.chunkBody, s.maxBody, s.maxHdr)
		if code != parseOK {
			if code == parseNeedMore {
				return 0, c.needBody(len(data))
			}
			return 0, code
		}
		total = n
	} else {
		total = r.headLen
		if r.contentLength > 0 {
			total += int(r.contentLength)
		}
		if len(data) < total {
			return 0, c.needBody(len(data))
		}
	}
	return total, parseOK
}

// needBody handles an incomplete body: it enforces the raw size limit of
// a chunked request and answers Expect: 100-continue once.
func (c *conn) needBody(have int) int {
	r := &c.ctx.req
	// The decoded chunked body is bounded by MaxBodyBytes, but the raw
	// framing is not (one-byte chunks cost six raw bytes each), and all of
	// it is buffered until the request completes. Framing heavier than
	// the payload itself is refused with 413 so a connection never pins
	// more than about twice MaxBodyBytes.
	if r.chunked && have > c.s.maxRaw {
		return 413
	}
	if r.expect100 && !c.cont && r.minor == 1 {
		c.cont = true
		c.wbuf = append(c.wbuf, "HTTP/1.1 100 Continue\r\n\r\n"...)
	}
	return parseNeedMore
}

// serve runs the Handler for the complete request req and appends the
// response to c.wbuf.
func (c *conn) serve(req []byte) {
	ctx := &c.ctx
	r := &ctx.req
	r.data = req
	if r.chunked {
		ctx.body = c.chunkBody
	} else {
		ctx.body = req[r.headLen:len(req):len(req)]
	}
	ctx.resetResponse()
	if r.minor == 1 {
		ctx.close = r.connClose
	} else {
		ctx.close = r.connClose || !r.connKeepAlive
	}
	if c.s.Handler == nil {
		ctx.status = nethttp.StatusNotFound
	} else {
		c.callHandler()
	}
	if len(ctx.out) > c.s.maxResp && !r.isHead {
		c.tooLarge()
	}
	c.writeResponse()

	r.data = nil
	ctx.body = nil
	c.headDone = false
	c.cont = false
	if cap(ctx.out) > maxRetained {
		ctx.out = nil
	}
	if cap(c.chunkBody) > maxRetained {
		c.chunkBody = nil
	}
}

// callHandler runs the Handler. The deferred recover is open-coded and
// does not allocate.
func (c *conn) callHandler() {
	defer c.recoverHandler()
	c.s.Handler(&c.ctx)
}

// recoverHandler turns a Handler panic into a 500 response followed by a
// close. Because the response is buffered until the Handler returns,
// nothing of the failed response has reached the wire yet.
func (c *conn) recoverHandler() {
	v := recover()
	if v == nil {
		return
	}
	c.s.logger.Error("twine/http: panic in Handler", "panic", v, "stack", string(debug.Stack()))
	ctx := &c.ctx
	ctx.resetResponse()
	ctx.Error("Internal Server Error\n", nethttp.StatusInternalServerError)
	ctx.close = true
}

// tooLarge replaces a response whose body passes MaxResponseBytes with 500
// followed by a close: sending it would overflow the engine's write
// buffer and abort the connection mid-body.
func (c *conn) tooLarge() {
	ctx := &c.ctx
	c.s.logger.Error("twine/http: response body exceeds MaxResponseBytes; sent 500",
		"bytes", len(ctx.out), "max", c.s.maxResp, "path", string(ctx.Path()))
	ctx.resetResponse()
	ctx.Error("Internal Server Error\n", nethttp.StatusInternalServerError)
	ctx.close = true
}

const defaultContentType = "Content-Type: text/plain; charset=utf-8\r\n"

// writeResponse appends the response for the current request to c.wbuf.
func (c *conn) writeResponse() {
	ctx := &c.ctx
	w := c.wbuf
	st := ctx.status
	if st < 200 || st > 599 {
		// 1xx is an interim response: sending one as the only response
		// would leave the client waiting for the final one.
		st = 500
	}
	w = append(w, statusLines[st]...)
	if !ctx.noServer {
		w = append(w, c.s.serverLine...)
	}
	if !ctx.noDate {
		w = append(w, c.date.line...)
	}
	body := ctx.out
	head := ctx.req.isHead
	if st != 204 && st != 304 {
		if len(ctx.ct) > 0 {
			w = append(w, "Content-Type: "...)
			w = append(w, ctx.ct...)
			w = append(w, '\r', '\n')
		} else if len(body) > 0 {
			w = append(w, defaultContentType...)
		}
		// A HEAD Handler that wrote no body has not said how long the
		// GET body would be; "Content-Length: 0" would be a false claim,
		// so it is omitted (as net/http does).
		if len(body) > 0 || !head {
			w = append(w, "Content-Length: "...)
			w = strconv.AppendInt(w, int64(len(body)), 10)
			w = append(w, '\r', '\n')
		}
	} else {
		body = nil
	}
	w = append(w, ctx.hdrs...)
	if ctx.close {
		w = append(w, "Connection: close\r\n"...)
	} else if ctx.req.minor == 0 {
		w = append(w, "Connection: keep-alive\r\n"...)
	}
	w = append(w, '\r', '\n')
	if head {
		body = nil
	}
	if len(body) > largeBody {
		c.iov[0], c.iov[1] = w, body
		c.t.Writev(c.iov[:]...)
		c.iov = [2][]byte{}
		w = w[:0]
	} else {
		w = append(w, body...)
	}
	c.wbuf = w
}

// sendError answers a request that could not be parsed with a canned
// response and closes.
func (c *conn) sendError(code int) {
	w := append(c.wbuf, statusLines[code]...)
	w = append(w, c.s.serverLine...)
	w = append(w, c.date.line...)
	c.wbuf = append(w, errorTail(code)...)
	c.closeConn()
}

// closeConn flushes and half-closes. Data the peer still sends is
// discarded.
func (c *conn) closeConn() {
	if c.closed {
		return
	}
	c.closed = true
	c.flushOut()
	c.t.LoseConnection()
	// Reading may be paused (pipelining yield or producer pause). The core
	// completes a graceful close when it reads the peer's EOF, so reading
	// must be on; DataReceived discards anything that arrives meanwhile.
	c.t.ResumeReading()
}

// flushOut writes all buffered responses with one Write.
func (c *conn) flushOut() {
	if len(c.wbuf) == 0 {
		return
	}
	c.t.Write(c.wbuf)
	if cap(c.wbuf) > maxRetained {
		c.wbuf = nil
	} else {
		c.wbuf = c.wbuf[:0]
	}
}

// statusLines holds "HTTP/1.1 NNN Reason\r\n" for every code 100..599.
var statusLines = func() (t [600][]byte) {
	for code := 100; code < 600; code++ {
		b := append([]byte("HTTP/1.1 "), strconv.Itoa(code)...)
		b = append(b, ' ')
		b = append(b, nethttp.StatusText(code)...)
		t[code] = append(b, '\r', '\n')
	}
	return t
}()

// cannedError is the part of an error response that follows the status,
// Server and Date lines.
type cannedError struct {
	code int
	tail []byte
}

// cannedErrors covers every status the parser and the timeouts produce.
var cannedErrors = func() []cannedError {
	var t []cannedError
	for _, code := range []int{400, 408, 413, 431, 501, 505} {
		body := nethttp.StatusText(code) + "\n"
		b := append([]byte(nil), defaultContentType...)
		b = append(b, "Content-Length: "...)
		b = strconv.AppendInt(b, int64(len(body)), 10)
		b = append(b, "\r\nConnection: close\r\n\r\n"...)
		t = append(t, cannedError{code, append(b, body...)})
	}
	return t
}()

// errorTail returns the canned tail for code. Every code sendError is
// given has one; anything else is a bug in this package.
func errorTail(code int) []byte {
	for i := range cannedErrors {
		if cannedErrors[i].code == code {
			return cannedErrors[i].tail
		}
	}
	panic("twine/http: no canned response for status " + strconv.Itoa(code))
}
