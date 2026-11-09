package http

import (
	"bytes"
	"io"
	nethttp "net/http"
	"net/url"
)

// Adapt wraps a net/http Handler so it can serve requests from a twine
// Server.
//
// It is a compatibility bridge, not a fast path. Every request allocates a
// *net/http.Request with its URL and Header map (one string per header name
// and value), the method, request URI, host and remote-address strings, a
// copy of the body behind an io.Reader, and a ResponseWriter with its own
// Header map: about 10 allocations plus 3 per request header, and a full
// copy of the request body. The body is copied because a net/http handler
// may keep reading it after ServeHTTP returns.
//
// The handler runs on the loop like any Handler and must not block:
// adapting a net/http handler that waits on I/O does not make it safe, so
// keep such handlers on net/http.Server. The response is buffered and sent
// when ServeHTTP returns: Flusher, Hijacker
// and Pusher are not implemented, interim responses (WriteHeader with a
// 1xx code, such as 103 Early Hints) are dropped while the final response
// is still sent, and an unset Content-Type is sniffed with
// net/http.DetectContentType as net/http does. The request's Context is
// context.Background.
func Adapt(h nethttp.Handler) func(*RequestCtx) {
	return func(ctx *RequestCtx) {
		uri := string(ctx.RequestURI())
		u, err := url.ParseRequestURI(uri)
		if err != nil {
			ctx.Error("Bad Request\n", nethttp.StatusBadRequest)
			ctx.SetConnectionClose()
			return
		}
		minor := 0
		proto := "HTTP/1.0"
		if ctx.IsHTTP11() {
			minor, proto = 1, "HTTP/1.1"
		}
		hdr := make(nethttp.Header, ctx.NumHeaders())
		ctx.VisitHeaders(func(name, value []byte) {
			if equalFold(name, "host") {
				return
			}
			hdr.Add(string(name), string(value))
		})
		var body io.ReadCloser = nethttp.NoBody
		if b := ctx.Body(); len(b) > 0 {
			body = io.NopCloser(bytes.NewReader(bytes.Clone(b)))
		}
		req := &nethttp.Request{
			Method:        string(ctx.Method()),
			URL:           u,
			Proto:         proto,
			ProtoMajor:    1,
			ProtoMinor:    minor,
			Header:        hdr,
			Body:          body,
			ContentLength: int64(len(ctx.Body())),
			Host:          string(ctx.Host()),
			RemoteAddr:    ctx.RemoteAddr().String(),
			RequestURI:    uri,
			Close:         ctx.close,
		}
		if ctx.IsChunked() {
			req.TransferEncoding = []string{"chunked"}
		}
		w := &responseWriter{ctx: ctx, header: make(nethttp.Header)}
		h.ServeHTTP(w, req)
		w.finish()
	}
}

// responseWriter implements net/http.ResponseWriter on top of RequestCtx.
type responseWriter struct {
	ctx         *RequestCtx
	header      nethttp.Header
	wroteHeader bool
}

// Header returns the header map sent at WriteHeader.
func (w *responseWriter) Header() nethttp.Header { return w.header }

// WriteHeader copies the header map into the RequestCtx once. Interim
// (1xx) codes are dropped: net/http would send them as extra responses
// before the final one, which the buffered RequestCtx cannot do, and
// recording one as the status would lose the final response.
func (w *responseWriter) WriteHeader(code int) {
	if w.wroteHeader || code >= 100 && code <= 199 {
		return
	}
	w.wroteHeader = true
	w.ctx.SetStatus(code)
	for k, vs := range w.header {
		for _, v := range vs {
			w.ctx.AddHeader(k, v)
		}
	}
}

// Write appends to the buffered response body.
func (w *responseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(nethttp.StatusOK)
	}
	return w.ctx.Write(p)
}

// WriteString appends to the buffered response body.
func (w *responseWriter) WriteString(s string) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(nethttp.StatusOK)
	}
	return w.ctx.WriteString(s)
}

// finish completes the response after ServeHTTP returns.
func (w *responseWriter) finish() {
	if !w.wroteHeader {
		w.WriteHeader(nethttp.StatusOK)
	}
	if len(w.ctx.ct) == 0 && len(w.ctx.out) > 0 {
		if _, set := w.header["Content-Type"]; !set {
			w.ctx.SetContentType(nethttp.DetectContentType(w.ctx.out))
		}
	}
}

var _ io.StringWriter = (*responseWriter)(nil)
