package twhttp

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/exergy-dev/twine"
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
		{strings.Repeat(validCorpus[0], 5) + validCorpus[3], 50}, // pipelined past the yield
	}
	for _, s := range seeds {
		f.Add([]byte(s.raw), s.split)
	}
	l := testLoop(f)
	f.Fuzz(func(t *testing.T, data []byte, split uint16) {
		checkParse(t, data, true)
		checkSplitInvariance(t, l, data, int(split))
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
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
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
	mine := http.Header{}
	ctx.VisitHeaders(func(n, v []byte) {
		mine[textproto.CanonicalMIMEHeaderKey(string(n))] = append(mine[textproto.CanonicalMIMEHeaderKey(string(n))], string(v))
	})
	theirs := req.Header.Clone()
	if theirs == nil {
		theirs = http.Header{}
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

// feedMode selects how feed delivers its input.
type feedMode struct {
	name    string
	maxPipe int  // Server.MaxPipelinedRequests (0 = default)
	split   bool // deliver in two reads
	eager   bool // deliver the second read without waiting for a pending continuation
}

// feed runs data through a fresh connection on l in up to two chunks and
// returns the responses (without 100 Continue and Date lines, which depend
// on timing) and whether the server closed.
func feed(t *testing.T, l *twine.Loop, m feedMode, data []byte, split int) (string, bool) {
	t.Helper()
	h := newHarness(t, l, &Server{Handler: fuzzHandler, MaxPipelinedRequests: m.maxPipe})
	if !m.split || split <= 0 || split >= len(data) {
		h.send(data)
	} else {
		// Copy each chunk so the protocol cannot depend on adjacency.
		a := append([]byte(nil), data[:split]...)
		b := append([]byte(nil), data[split:]...)
		if m.eager {
			onLoop(t, l, func() {
				h.c.DataReceived(a)
				h.c.DataReceived(b)
			})
			h.settle()
		} else {
			h.send(a, b)
		}
	}
	out := strings.ReplaceAll(h.out(), "HTTP/1.1 100 Continue\r\n\r\n", "")
	out = dateLine.ReplaceAllString(out, "")
	closing := h.closing()
	h.lose()
	return out, closing
}

var dateLine = regexp.MustCompile(`\r\nDate: [^\r]*`)

// splitModes are compared against one read with the default limits. A
// pipelining limit of 2 makes most multi-request inputs yield to the loop.
var splitModes = []feedMode{
	{name: "whole, pipeline limit 2", maxPipe: 2},
	{name: "split", split: true},
	{name: "split, pipeline limit 2", maxPipe: 2, split: true},
	{name: "split, pipeline limit 2, read while yielding", maxPipe: 2, split: true, eager: true},
}

// checkSplitInvariance requires the same responses however the bytes are
// split across reads and however often the connection yields.
func checkSplitInvariance(t *testing.T, l *twine.Loop, data []byte, split int) {
	t.Helper()
	whole, wc := feed(t, l, feedMode{}, data, -1)
	if len(data) > 0 {
		split %= len(data) + 1
	}
	for _, m := range splitModes {
		parts, pc := feed(t, l, m, data, split)
		if whole != parts || wc != pc {
			t.Fatalf("%s at %d changes output:\nwhole (close=%v) %q\nthis  (close=%v) %q", m.name, split, wc, whole, pc, parts)
		}
	}
	for _, line := range strings.Split(whole, "\r\n") {
		if strings.HasPrefix(line, "HTTP/1.1 ") && len(line) < 12 {
			t.Fatalf("bad status line %q", line)
		}
	}
}
