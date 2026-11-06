package http

import (
	"net"

	"github.com/robert-malhotra/twine"
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
	user any

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
func (ctx *RequestCtx) RequestURI() []byte { return ctx.req.slice(ctx.req.uriS, ctx.req.uriE) }

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
func (ctx *RequestCtx) Version() []byte { return ctx.req.slice(ctx.req.verS, ctx.req.verS+8) }

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
func (ctx *RequestCtx) Header(name string) []byte {
	r := &ctx.req
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

// HeaderBytes is Header with a []byte name.
func (ctx *RequestCtx) HeaderBytes(name []byte) []byte {
	r := &ctx.req
	for i := 0; i < r.nh; i++ {
		h := &r.hdrs[i]
		if equalFoldBytes(r.data[h.ns:h.ne], name) {
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

// SetUserValue stores a per-request value; it is cleared before the next
// request. Storing a non-pointer value allocates.
func (ctx *RequestCtx) SetUserValue(v any) { ctx.user = v }

// UserValue returns the value stored with SetUserValue.
func (ctx *RequestCtx) UserValue() any { return ctx.user }

// --- response --------------------------------------------------------------

// SetStatus sets the response status code (default 200). Codes outside
// 100..599 are sent as 500.
func (ctx *RequestCtx) SetStatus(code int) { ctx.status = code }

// Status returns the response status code set so far.
func (ctx *RequestCtx) Status() int { return ctx.status }

// SetContentType sets the Content-Type response header. Without it a
// non-empty body is sent as "text/plain; charset=utf-8".
func (ctx *RequestCtx) SetContentType(ct string) { ctx.ct = appendSafe(ctx.ct[:0], ct) }

// SetContentTypeBytes is SetContentType with a []byte value.
func (ctx *RequestCtx) SetContentTypeBytes(ct []byte) { ctx.ct = appendSafeBytes(ctx.ct[:0], ct) }

// SetHeader sets a response header, replacing earlier values of the same
// name (ASCII case-insensitive). Content-Type is routed to SetContentType;
// Content-Length and Transfer-Encoding are ignored (the length is computed);
// "Connection: close" closes the connection after the response; Server and
// Date replace the automatic values. CR and LF in name or value are replaced
// with spaces so a header cannot split the response.
func (ctx *RequestCtx) SetHeader(name, value string) {
	if ctx.special(name, value, true) {
		return
	}
	ctx.delHeader(name)
	ctx.appendHeader(name, value)
}

// SetHeaderBytes is SetHeader with []byte arguments; it does not allocate.
func (ctx *RequestCtx) SetHeaderBytes(name, value []byte) {
	if ctx.specialBytes(name, value, true) {
		return
	}
	ctx.delHeaderBytes(name)
	ctx.hdrs = appendSafeBytes(ctx.hdrs, name)
	ctx.hdrs = append(ctx.hdrs, ':', ' ')
	ctx.hdrs = appendSafeBytes(ctx.hdrs, value)
	ctx.hdrs = append(ctx.hdrs, '\r', '\n')
}

// AddHeader adds a response header line without replacing existing ones
// (for Set-Cookie and other repeatable headers). Special names behave as in
// SetHeader.
func (ctx *RequestCtx) AddHeader(name, value string) {
	if ctx.special(name, value, false) {
		return
	}
	ctx.appendHeader(name, value)
}

// DelHeader removes response headers previously set with name.
func (ctx *RequestCtx) DelHeader(name string) { ctx.delHeader(name) }

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
	ctx.user = nil
}

// special handles header names the server manages itself. It reports
// whether the header was consumed.
func (ctx *RequestCtx) special(name, value string, replace bool) bool {
	switch len(name) {
	case 4:
		if equalFoldStr(name, "date") {
			if replace {
				ctx.delHeader(name)
			}
			ctx.noDate = true
		}
	case 6:
		if equalFoldStr(name, "server") {
			if replace {
				ctx.delHeader(name)
			}
			ctx.noServer = true
		}
	case 10:
		if equalFoldStr(name, "connection") {
			if equalFoldStr(value, "close") {
				ctx.close = true
			}
			return true
		}
	case 12:
		if equalFoldStr(name, "content-type") {
			ctx.SetContentType(value)
			return true
		}
	case 14:
		return equalFoldStr(name, "content-length")
	case 17:
		return equalFoldStr(name, "transfer-encoding")
	}
	return false
}

func (ctx *RequestCtx) specialBytes(name, value []byte, replace bool) bool {
	switch len(name) {
	case 4:
		if equalFold(name, "date") {
			if replace {
				ctx.delHeaderBytes(name)
			}
			ctx.noDate = true
		}
	case 6:
		if equalFold(name, "server") {
			if replace {
				ctx.delHeaderBytes(name)
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
			ctx.SetContentTypeBytes(value)
			return true
		}
	case 14:
		return equalFold(name, "content-length")
	case 17:
		return equalFold(name, "transfer-encoding")
	}
	return false
}

func (ctx *RequestCtx) appendHeader(name, value string) {
	ctx.hdrs = appendSafe(ctx.hdrs, name)
	ctx.hdrs = append(ctx.hdrs, ':', ' ')
	ctx.hdrs = appendSafe(ctx.hdrs, value)
	ctx.hdrs = append(ctx.hdrs, '\r', '\n')
}

// delHeader removes every line of ctx.hdrs whose name folds to name.
func (ctx *RequestCtx) delHeader(name string) {
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

func (ctx *RequestCtx) delHeaderBytes(name []byte) {
	h := ctx.hdrs
	for i := 0; i < len(h); {
		end := i
		for end < len(h) && h[end] != '\n' {
			end++
		}
		end++
		colon := i
		for h[colon] != ':' {
			colon++
		}
		if equalFoldBytes(h[i:colon], name) {
			h = append(h[:i], h[end:]...)
			continue
		}
		i = end
	}
	ctx.hdrs = h
}

func equalFoldStr(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if lowerTable[a[i]] != lowerTable[b[i]] {
			return false
		}
	}
	return true
}

// appendSafe appends s with CR and LF replaced by spaces so that a header
// name or value cannot inject extra lines into the response.
func appendSafe(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		dst = append(dst, c)
	}
	return dst
}

func appendSafeBytes(dst, s []byte) []byte {
	for _, c := range s {
		if c == '\r' || c == '\n' {
			c = ' '
		}
		dst = append(dst, c)
	}
	return dst
}
