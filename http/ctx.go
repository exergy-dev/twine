package http

import (
	"net"

	"github.com/exergy-dev/twine"
)

// RequestCtx carries one request and its response. There is one RequestCtx
// per connection, allocated with the connection and reused for every request
// on it; nothing in it is allocated per request.
//
// Every []byte returned by a request accessor (Method, Path, Header, Body,
// ...) aliases the loop's read buffer or the connection's reassembly buffer
// and is valid only until the Handler returns. Copy what you keep. A
// RequestCtx must not be retained or used from another goroutine.
type RequestCtx struct {
	req  request
	body []byte
	c    *conn

	// Response.
	status   int
	hdrs     []byte // "Name: value\r\n" lines set by the handler
	ct       []byte
	out      []byte // response body
	close    bool   // Connection: close after this response
	noServer bool
	noDate   bool
}

var slash = []byte{'/'}

// Method returns the request method, e.g. "GET".
func (ctx *RequestCtx) Method() []byte { return ctx.req.slice(0, ctx.req.methodE) }

// IsMethod reports whether the request method equals m (case-sensitive, as
// methods are).
func (ctx *RequestCtx) IsMethod(m string) bool { return string(ctx.Method()) == m }

// RequestURI returns the raw request target as sent, e.g. "/a/b?x=1".
func (ctx *RequestCtx) RequestURI() []byte { return ctx.req.slice(ctx.req.methodE+1, ctx.req.uriE) }

// Path returns the path of the request target without the query, still
// percent-encoded. For an absolute-form target ("http://host/p") it is the
// path part ("/" when empty); for "*" it is "*".
func (ctx *RequestCtx) Path() []byte {
	if ctx.req.pathSlash {
		return slash
	}
	return ctx.req.slice(ctx.req.pathS, ctx.req.pathE)
}

// RawQuery returns the query string without the leading '?', or nil when
// the target has none.
func (ctx *RequestCtx) RawQuery() []byte {
	if !ctx.req.hasQuery {
		return nil
	}
	return ctx.req.slice(ctx.req.qS, ctx.req.qE)
}

// Version returns the protocol version as sent: "HTTP/1.0" or "HTTP/1.1".
func (ctx *RequestCtx) Version() []byte { return ctx.req.slice(ctx.req.uriE+1, ctx.req.uriE+9) }

// IsHTTP11 reports whether the request is HTTP/1.1.
func (ctx *RequestCtx) IsHTTP11() bool { return ctx.req.minor == 1 }

// Host returns the request host: the authority of an absolute-form target,
// else the Host header (nil if absent, which only HTTP/1.0 allows).
func (ctx *RequestCtx) Host() []byte {
	if !ctx.req.hasHost && !ctx.req.absHost {
		return nil
	}
	return ctx.req.slice(ctx.req.hostS, ctx.req.hostE)
}

// Header returns the value of the first request header whose name equals
// name under ASCII case folding, or nil. Values are trimmed of surrounding
// whitespace. The lookup is a linear scan of at most 64 entries; no map is
// built.
func (ctx *RequestCtx) Header(name string) []byte { return lookupHeader(&ctx.req, name) }

// HeaderBytes is Header with a []byte name.
func (ctx *RequestCtx) HeaderBytes(name []byte) []byte { return lookupHeader(&ctx.req, name) }

// lookupHeader implements Header. Entries whose length or first byte
// differ are skipped before the full compare.
func lookupHeader[N text](r *request, name N) []byte {
	if len(name) == 0 {
		return nil
	}
	first := lowerTable[name[0]]
	for i := 0; i < r.nh; i++ {
		h := &r.hdrs[i]
		if int(h.ne-h.ns) != len(name) || lowerTable[r.data[h.ns]] != first {
			continue
		}
		if equalFold(r.data[h.ns:h.ne], name) {
			return r.slice(h.vs, h.ve)
		}
	}
	return nil
}

// VisitHeaders calls fn for every request header in wire order, repeated
// names included. name and value are valid only during the Handler.
func (ctx *RequestCtx) VisitHeaders(fn func(name, value []byte)) {
	r := &ctx.req
	for i := 0; i < r.nh; i++ {
		h := &r.hdrs[i]
		fn(r.slice(h.ns, h.ne), r.slice(h.vs, h.ve))
	}
}

// NumHeaders returns the number of request header lines.
func (ctx *RequestCtx) NumHeaders() int { return ctx.req.nh }

// ContentLength returns the request's Content-Length, or -1 when the
// request had none (including chunked requests; use len(Body())).
func (ctx *RequestCtx) ContentLength() int64 { return ctx.req.contentLength }

// IsChunked reports whether the request body used chunked transfer coding.
func (ctx *RequestCtx) IsChunked() bool { return ctx.req.chunked }

// Body returns the complete request body: the raw Content-Length bytes or
// the decoded chunked payload. It is valid only during the Handler.
func (ctx *RequestCtx) Body() []byte { return ctx.body }

// RemoteAddr returns the peer address.
func (ctx *RequestCtx) RemoteAddr() net.Addr { return ctx.c.t.GetPeer() }

// LocalAddr returns the local address of the connection.
func (ctx *RequestCtx) LocalAddr() net.Addr { return ctx.c.t.GetHost() }

// Loop returns the loop that owns the connection, for CallLater or
// DeferToThread. Responses are still written when the Handler returns;
// there is no asynchronous response API.
func (ctx *RequestCtx) Loop() *twine.Loop { return ctx.c.loop }

// --- response --------------------------------------------------------------

// SetStatus sets the response status code (default 200). Codes outside
// 200..599 are sent as 500: the response a Handler builds is always the
// final one, and a 1xx interim response (100 Continue, 103 Early Hints)
// cannot be sent from a Handler. The server answers Expect: 100-continue
// itself.
func (ctx *RequestCtx) SetStatus(code int) { ctx.status = code }

// Status returns the response status code set so far.
func (ctx *RequestCtx) Status() int { return ctx.status }

// SetContentType sets the Content-Type response header. Without it a
// non-empty body is sent as "text/plain; charset=utf-8".
func (ctx *RequestCtx) SetContentType(ct string) { ctx.ct = appendSafe(ctx.ct[:0], ct) }

// SetContentTypeBytes is SetContentType with a []byte value.
func (ctx *RequestCtx) SetContentTypeBytes(ct []byte) { ctx.ct = appendSafe(ctx.ct[:0], ct) }

// SetHeader sets a response header, replacing earlier values of the same
// name (ASCII case-insensitive). Content-Type is routed to SetContentType;
// Content-Length and Transfer-Encoding are ignored (the length is computed);
// "Connection: close" closes the connection after the response; Server and
// Date replace the automatic values.
//
// A header whose name is empty or contains a byte that is not an RFC 9110
// token character (space, ':', CR, LF, controls, non-ASCII, ...) is
// dropped, as net/http does. CR and LF in a value are replaced with spaces
// so a header cannot split the response.
func (ctx *RequestCtx) SetHeader(name, value string) { setHeader(ctx, name, value, true) }

// SetHeaderBytes is SetHeader with []byte arguments; it does not allocate.
func (ctx *RequestCtx) SetHeaderBytes(name, value []byte) { setHeader(ctx, name, value, true) }

// AddHeader adds a response header line without replacing existing ones
// (for Set-Cookie and other repeatable headers). Special and invalid names
// behave as in SetHeader.
func (ctx *RequestCtx) AddHeader(name, value string) { setHeader(ctx, name, value, false) }

// DelHeader removes response headers previously set with name.
func (ctx *RequestCtx) DelHeader(name string) { delHeader(ctx, name) }

// SetConnectionClose closes the connection after this response.
func (ctx *RequestCtx) SetConnectionClose() { ctx.close = true }

// Write appends p to the response body. It implements io.Writer and never
// fails.
func (ctx *RequestCtx) Write(p []byte) (int, error) {
	ctx.out = append(ctx.out, p...)
	return len(p), nil
}

// WriteString appends s to the response body. It implements
// io.StringWriter and never fails.
func (ctx *RequestCtx) WriteString(s string) (int, error) {
	ctx.out = append(ctx.out, s...)
	return len(s), nil
}

// SetBody replaces the response body with a copy of b.
func (ctx *RequestCtx) SetBody(b []byte) { ctx.out = append(ctx.out[:0], b...) }

// SetBodyString replaces the response body with s.
func (ctx *RequestCtx) SetBodyString(s string) { ctx.out = append(ctx.out[:0], s...) }

// ResponseBody returns the response body written so far.
func (ctx *RequestCtx) ResponseBody() []byte { return ctx.out }

// Error replaces the response with status code and msg as a plain-text body.
func (ctx *RequestCtx) Error(msg string, code int) {
	ctx.status = code
	ctx.ct = append(ctx.ct[:0], "text/plain; charset=utf-8"...)
	ctx.out = append(ctx.out[:0], msg...)
}

// resetResponse prepares the response half for a new request.
func (ctx *RequestCtx) resetResponse() {
	ctx.status = 200
	ctx.hdrs = ctx.hdrs[:0]
	ctx.ct = ctx.ct[:0]
	ctx.out = ctx.out[:0]
	ctx.close = false
	ctx.noServer = false
	ctx.noDate = false
}

// setHeader implements SetHeader (replace set) and AddHeader.
func setHeader[N, V text](ctx *RequestCtx, name N, value V, replace bool) {
	if !validName(name) || special(ctx, name, value, replace) {
		return
	}
	if replace {
		delHeader(ctx, name)
	}
	ctx.hdrs = append(ctx.hdrs, name...)
	ctx.hdrs = append(ctx.hdrs, ':', ' ')
	ctx.hdrs = appendSafe(ctx.hdrs, value)
	ctx.hdrs = append(ctx.hdrs, '\r', '\n')
}

// special handles header names the server manages itself. It reports
// whether the header was consumed.
func special[N, V text](ctx *RequestCtx, name N, value V, replace bool) bool {
	switch len(name) {
	case 4:
		if equalFold(name, "date") {
			if replace {
				delHeader(ctx, name)
			}
			ctx.noDate = true
		}
	case 6:
		if equalFold(name, "server") {
			if replace {
				delHeader(ctx, name)
			}
			ctx.noServer = true
		}
	case 10:
		if equalFold(name, "connection") {
			if equalFold(value, "close") {
				ctx.close = true
			}
			return true
		}
	case 12:
		if equalFold(name, "content-type") {
			ctx.ct = appendSafe(ctx.ct[:0], value)
			return true
		}
	case 14:
		return equalFold(name, "content-length")
	case 17:
		return equalFold(name, "transfer-encoding")
	}
	return false
}

// delHeader removes every line of ctx.hdrs whose name folds to name.
func delHeader[N text](ctx *RequestCtx, name N) {
	h := ctx.hdrs
	for i := 0; i < len(h); {
		end := i
		for end < len(h) && h[end] != '\n' {
			end++
		}
		end++ // past '\n'
		colon := i
		for h[colon] != ':' {
			colon++
		}
		if equalFold(h[i:colon], name) {
			h = append(h[:i], h[end:]...)
			continue
		}
		i = end
	}
	ctx.hdrs = h
}

// validName reports whether name is a valid header field name: a non-empty
// RFC 9110 token.
func validName[N text](name N) bool {
	if len(name) == 0 {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !tokenTable[name[i]] {
			return false
		}
	}
	return true
}

// appendSafe appends s with CR and LF replaced by spaces so that a header
// value cannot inject extra lines into the response.
func appendSafe[S text](dst []byte, s S) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		dst = append(dst, c)
	}
	return dst
}
