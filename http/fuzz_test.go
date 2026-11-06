package http

import (
	"bufio"
	"bytes"
	"io"
	nethttp "net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// validCorpus are requests both parsers must accept with identical results.
var validCorpus = []string{
	"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
	"GET /a/b%20c?x=1&y=%41 HTTP/1.1\r\nHost: example.com:8080\r\nUser-Agent: test\r\nAccept: */*\r\n\r\n",
	"POST /submit HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 13\r\n\r\n{\"a\":\"hello\"}",
	"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6;name=v\r\n world\r\n0\r\n\r\n",
	"GET /old HTTP/1.0\r\n\r\n",
	"GET /old HTTP/1.0\r\nConnection: keep-alive\r\nHost: h\r\n\r\n",
	"OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n",
	"GET http://example.com/abs?q=1 HTTP/1.1\r\nHost: ignored\r\n\r\n",
	"DELETE /x HTTP/1.1\r\nHost: x\r\nX-Multi: a\r\nx-multi: b\r\nX-Empty:\r\nX-Spaces: \t v \t\r\n\r\n",
	"PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc",
	"GET /utf8 HTTP/1.1\r\nHost: x\r\nX-Obs: caf\xc3\xa9\r\n\r\n",
}

func FuzzParseRequest(f *testing.F) {
	seeds := []struct {
		raw   string
		split uint16
	}{
		{validCorpus[0], 0},  // GET
		{validCorpus[2], 60}, // POST with body, split in the head
		{validCorpus[3], 70}, // chunked, split in the body
		{validCorpus[0] + validCorpus[1] + validCorpus[2], 120},                                  // pipelined
		{"GET /split HTTP/1.1\r\nHost: x\r\nX-A: 1\r\n\r\n", 25},                                 // split headers
		{"GET/ HTTP/1.1\r\nHost: x\r\n\r\n", 0},                                                  // bad request line
		{"GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + strings.Repeat("z", 9<<10) + "\r\n\r\n", 4096}, // oversized header
		{validCorpus[4], 3},  // HTTP/1.0
		{validCorpus[7], 10}, // absolute form
		{"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999\r\n\r\n", 0},
		{"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi", 70},
	}
	for _, s := range seeds {
		f.Add([]byte(s.raw), s.split)
	}
	f.Fuzz(func(t *testing.T, data []byte, split uint16) {
		checkParse(t, data, true)
		checkSplitInvariance(t, data, int(split))
	})
}

// TestValidCorpusMatchesNetHTTP runs the differential check on requests
// that must be accepted.
func TestValidCorpusMatchesNetHTTP(t *testing.T) {
	for _, raw := range validCorpus {
		if !checkParse(t, []byte(raw), false) {
			t.Errorf("valid request rejected or incomplete: %q", raw)
		}
	}
}

// checkParse parses data with the head parser and body decoder. When the
// first request is complete it must agree with net/http.ReadRequest. It
// reports whether a complete request was parsed. With lax set, net/http
// rejecting a request we accept is still reported as a failure (we are
// meant to be stricter, never looser).
func checkParse(t *testing.T, data []byte, lax bool) bool {
	t.Helper()
	const maxHdr, maxBody = DefaultMaxHeaderBytes, DefaultMaxBodyBytes
	var r request
	h, code := r.parseHead(data, maxHdr)
	switch code {
	case parseOK:
	case parseNeedMore:
		if len(data) > maxHdr {
			t.Fatalf("need more with %d bytes > MaxHeaderBytes", len(data))
		}
		return false
	case 400, 431, 501, 505:
		return false
	default:
		t.Fatalf("unexpected parse status %d", code)
	}
	if h > maxHdr || r.nh > maxHeaders || h != r.headLen {
		t.Fatalf("limits: head %d headers %d", h, r.nh)
	}
	var body []byte
	total := 0
	if r.chunked {
		cs := chunkState{off: h}
		n, c := cs.decode(data, &body, maxBody, maxHdr)
		if c != parseOK {
			return false
		}
		if len(body) > maxBody {
			t.Fatalf("chunked body %d over limit", len(body))
		}
		total = n
	} else {
		if r.contentLength > maxBody {
			return false
		}
		total = h + int(max(r.contentLength, 0))
		if len(data) < total {
			return false
		}
		body = data[h:total]
	}
	compareNetHTTP(t, data[:total], &r, body)
	return true
}

func compareNetHTTP(t *testing.T, raw []byte, r *request, body []byte) {
	t.Helper()
	r.data = raw
	ctx := &RequestCtx{req: *r, body: body}
	req, err := nethttp.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		t.Fatalf("we accepted but net/http rejected %q: %v", raw, err)
	}
	if req.Method != string(ctx.Method()) {
		t.Fatalf("method %q vs %q", ctx.Method(), req.Method)
	}
	if req.RequestURI != string(ctx.RequestURI()) {
		t.Fatalf("uri %q vs %q", ctx.RequestURI(), req.RequestURI)
	}
	if req.Proto != string(ctx.Version()) {
		t.Fatalf("proto %q vs %q", ctx.Version(), req.Proto)
	}
	if req.Host != string(ctx.Host()) {
		t.Fatalf("host %q vs %q", ctx.Host(), req.Host)
	}
	if req.URL.RawQuery != string(ctx.RawQuery()) {
		t.Fatalf("query %q vs %q", ctx.RawQuery(), req.URL.RawQuery)
	}
	if p, err := url.PathUnescape(string(ctx.Path())); err != nil {
		t.Fatalf("path %q does not unescape: %v", ctx.Path(), err)
	} else if p != req.URL.Path && (!ctx.req.pathSlash || req.URL.Path != "") {
		t.Fatalf("path %q vs %q", p, req.URL.Path)
	}
	mine := nethttp.Header{}
	ctx.VisitHeaders(func(n, v []byte) {
		mine[textproto.CanonicalMIMEHeaderKey(string(n))] = append(mine[textproto.CanonicalMIMEHeaderKey(string(n))], string(v))
	})
	theirs := req.Header.Clone()
	if theirs == nil {
		theirs = nethttp.Header{}
	}
	for _, h := range []string{"Host", "Transfer-Encoding"} {
		delete(mine, h)
		delete(theirs, h)
	}
	if v := mine["Content-Length"]; len(v) > 1 {
		mine["Content-Length"] = v[:1] // net/http collapses identical repeats
	}
	if _, ok := mine["Cache-Control"]; !ok {
		delete(theirs, "Cache-Control") // net/http derives it from Pragma
	}
	if !reflect.DeepEqual(mine, theirs) {
		t.Fatalf("headers differ:\nours   %v\ntheirs %v\nraw %q", mine, theirs, raw)
	}
	got, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("net/http body error on %q: %v", raw, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body %q vs %q", body, got)
	}
}

// fuzzHandler writes a deterministic summary of the request and pins the
// Date header so outputs are comparable.
func fuzzHandler(ctx *RequestCtx) {
	ctx.SetHeader("Date", "fixed")
	ctx.Write(ctx.Method())
	ctx.WriteString(" ")
	ctx.Write(ctx.RequestURI())
	ctx.VisitHeaders(func(n, v []byte) {
		ctx.WriteString("\n")
		ctx.Write(n)
		ctx.WriteString("=")
		ctx.Write(v)
	})
	ctx.WriteString("\n\n")
	ctx.Write(ctx.Body())
}

// feed runs data through a connection in up to two chunks.
func feed(data []byte, split int) (string, bool) {
	s := &Server{Handler: fuzzHandler}
	ft := &fakeTransport{}
	c := newFakeConn(s, ft)
	if split <= 0 || split >= len(data) {
		c.DataReceived(data)
	} else {
		// Copy each chunk so the protocol cannot depend on adjacency.
		c.DataReceived(append([]byte(nil), data[:split]...))
		c.DataReceived(append([]byte(nil), data[split:]...))
	}
	out := strings.ReplaceAll(ft.String(), "HTTP/1.1 100 Continue\r\n\r\n", "")
	return out, ft.closing
}

// checkSplitInvariance requires the same responses however the bytes are
// split across reads.
func checkSplitInvariance(t *testing.T, data []byte, split int) {
	t.Helper()
	whole, wc := feed(data, -1)
	if len(data) > 0 {
		split %= len(data) + 1
	}
	parts, pc := feed(data, split)
	if whole != parts || wc != pc {
		t.Fatalf("split at %d changes output:\nwhole (close=%v) %q\nsplit (close=%v) %q", split, wc, whole, pc, parts)
	}
	for _, line := range strings.Split(whole, "\r\n") {
		if strings.HasPrefix(line, "HTTP/1.1 ") && len(line) < 12 {
			t.Fatalf("bad status line %q", line)
		}
	}
}
