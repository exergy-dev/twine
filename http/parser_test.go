package http

import (
	"bufio"
	"io"
	nethttp "net/http"
	"strings"
	"testing"
)

func TestParserStatus(t *testing.T) {
	l := testLoop(t)
	big := strings.Repeat("a", 9<<10)
	tests := []struct {
		name, raw string
		status    string // expected first status line prefix; "" = no response (incomplete)
		close     bool
	}{
		{"simple", "GET / HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 200", false},
		{"http10", "GET / HTTP/1.0\r\n\r\n", "HTTP/1.1 200", true},
		{"http10 keep-alive", "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n", "HTTP/1.1 200", false},
		{"conn close", "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", "HTTP/1.1 200", true},
		{"conn token list", "GET / HTTP/1.1\r\nHost: x\r\nConnection: upgrade, Close\r\n\r\n", "HTTP/1.1 200", true},
		{"incomplete head", "GET / HTTP/1.1\r\nHost: x\r\n", "", false},
		{"incomplete body", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nab", "", false},
		{"missing host", "GET / HTTP/1.1\r\n\r\n", "HTTP/1.1 400", true},
		{"bad method", "G@T / HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 400", true},
		{"double space", "GET  / HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 400", true},
		{"bare LF", "GET / HTTP/1.1\nHost: x\n\n", "HTTP/1.1 400", true},
		{"bare LF header", "GET / HTTP/1.1\r\nHost: x\n\r\n", "HTTP/1.1 400", true},
		{"obs-fold", "GET / HTTP/1.1\r\nHost: x\r\nX: a\r\n b\r\n\r\n", "HTTP/1.1 400", true},
		{"space before colon", "GET / HTTP/1.1\r\nHost : x\r\n\r\n", "HTTP/1.1 400", true},
		{"bad version", "GET / HTTP/1.x\r\nHost: x\r\n\r\n", "HTTP/1.1 400", true},
		{"http2 version", "GET / HTTP/2.0\r\nHost: x\r\n\r\n", "HTTP/1.1 505", true},
		{"relative target", "GET foo HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 400", true},
		{"bad escape", "GET /%zz HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 400", true},
		{"absolute form", "GET http://example.com/p?q HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 200", false},
		{"asterisk", "OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 200", false},
		{"two hosts", "GET / HTTP/1.1\r\nHost: x\r\nHost: y\r\n\r\n", "HTTP/1.1 400", true},
		{"cl and te", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", "HTTP/1.1 400", true},
		{"bad cl", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: +1\r\n\r\na", "HTTP/1.1 400", true},
		{"conflicting cl", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab", "HTTP/1.1 400", true},
		{"same cl twice", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\na", "HTTP/1.1 200", false},
		{"gzip te", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n", "HTTP/1.1 501", true},
		{"http10 chunked", "POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", "HTTP/1.1 400", true},
		{"ctl in value", "GET / HTTP/1.1\r\nHost: x\r\nX: a\x01b\r\n\r\n", "HTTP/1.1 400", true},
		{"big header", "GET / HTTP/1.1\r\nHost: x\r\nX: " + big + "\r\n\r\n", "HTTP/1.1 431", true},
		{"big incomplete header", "GET / HTTP/1.1\r\nHost: x\r\nX: " + big, "HTTP/1.1 431", true},
		{"too many headers", "GET / HTTP/1.1\r\nHost: x\r\n" + strings.Repeat("A: b\r\n", 64) + "\r\n", "HTTP/1.1 431", true},
		{"body too big", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4194305\r\n\r\n", "HTTP/1.1 413", true},
		{"chunk bad size", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", "HTTP/1.1 400", true},
		{"chunk missing crlf", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nab\r\n0\r\n\r\n", "HTTP/1.1 400", true},
		{"chunk trailer", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n0\r\nX-T: 1\r\n\r\n", "HTTP/1.1 200", false},
	}
	s := &Server{Name: "srv", Handler: func(ctx *RequestCtx) { ctx.Write(ctx.Body()) }}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, closed := serveOnce(t, l, s, tc.raw)
			if tc.status == "" {
				if out != "" {
					t.Fatalf("expected no response, got %q", out)
				}
				return
			}
			if !strings.HasPrefix(out, tc.status) {
				t.Fatalf("got %q, want prefix %q", firstLine(out), tc.status)
			}
			if closed != tc.close {
				t.Fatalf("closed = %v, want %v (%q)", closed, tc.close, out)
			}
			// Every response, canned errors included, is well formed and
			// carries Date (RFC 9110 section 6.6.1) and the Server name.
			resp, err := nethttp.ReadResponse(bufio.NewReader(strings.NewReader(out)), nil)
			if err != nil {
				t.Fatalf("net/http rejects %q: %v", out, err)
			}
			if _, err := nethttp.ParseTime(resp.Header.Get("Date")); err != nil || resp.Header.Get("Server") != "srv" {
				t.Fatalf("Date %q (%v), Server %q", resp.Header.Get("Date"), err, resp.Header.Get("Server"))
			}
		})
	}
}

func firstLine(s string) string {
	if i := strings.Index(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}

func TestRequestAccessors(t *testing.T) {
	l := testLoop(t)
	var seen []string
	s := &Server{Handler: func(ctx *RequestCtx) {
		seen = append(seen,
			string(ctx.Method()), string(ctx.Path()), string(ctx.RawQuery()),
			string(ctx.RequestURI()), string(ctx.Version()), string(ctx.Host()),
			string(ctx.Header("CONTENT-type")), string(ctx.Header("x-missing")),
			string(ctx.Body()))
		if !ctx.IsHTTP11() {
			t.Error("IsHTTP11 = false")
		}
		var names []string
		ctx.VisitHeaders(func(n, v []byte) { names = append(names, string(n)+"="+string(v)) })
		seen = append(seen, strings.Join(names, ","))
	}}
	out, _ := serveOnce(t, l, s, "PUT /a%20b/c?x=1&y=2 HTTP/1.1\r\nHost:  example.com:8080 \r\nContent-Type: text/x\r\nContent-Length: 3\r\n\r\nabc")
	want := []string{"PUT", "/a%20b/c", "x=1&y=2", "/a%20b/c?x=1&y=2", "HTTP/1.1", "example.com:8080",
		"text/x", "", "abc", "Host=example.com:8080,Content-Type=text/x,Content-Length=3"}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Fatalf("got  %q\nwant %q\n%s", seen, want, out)
	}

	seen = nil
	serveOnce(t, l, s, "GET http://h.example:81?q HTTP/1.1\r\nHost: other\r\n\r\n")
	if seen[1] != "/" || seen[2] != "q" || seen[5] != "h.example:81" {
		t.Fatalf("absolute form: %q", seen)
	}
}

func TestResponseHeaders(t *testing.T) {
	l := testLoop(t)
	s := &Server{Name: "srv", Handler: func(ctx *RequestCtx) {
		ctx.SetHeader("X-A", "1")
		ctx.SetHeader("x-a", "2") // replaces
		ctx.AddHeader("Set-Cookie", "a=1")
		ctx.AddHeader("Set-Cookie", "b=2")
		ctx.SetHeader("Content-Length", "999") // ignored
		ctx.SetHeader("X-Inject", "v\r\nEvil: 1")
		ctx.SetHeader("Server", "custom")
		ctx.SetStatus(201)
		ctx.WriteString("ok")
	}}
	out, _ := serveOnce(t, l, s, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	for _, w := range []string{"HTTP/1.1 201 Created\r\n", "x-a: 2\r\n", "Set-Cookie: a=1\r\nSet-Cookie: b=2\r\n",
		"Content-Length: 2\r\n", "X-Inject: v  Evil: 1\r\n", "Server: custom\r\n", "\r\n\r\nok"} {
		if !strings.Contains(out, w) {
			t.Errorf("missing %q in\n%s", w, out)
		}
	}
	for _, bad := range []string{"X-A: 1", "999", "Server: srv", "\r\nEvil"} {
		if strings.Contains(out, bad) {
			t.Errorf("unexpected %q in\n%s", bad, out)
		}
	}
}

// TestInterimStatusIs500 checks that a Handler setting a 1xx status gets a
// 500 instead of an interim response that would leave the client waiting
// and shift every later pipelined response.
func TestInterimStatusIs500(t *testing.T) {
	l := testLoop(t)
	for _, code := range []int{99, 100, 103, 199, 600} {
		s := &Server{Handler: func(ctx *RequestCtx) {
			if ctx.IsMethod("POST") {
				ctx.SetStatus(code)
			}
			ctx.WriteString("x")
		}}
		out, _ := serveOnce(t, l, s, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n")
		br := bufio.NewReader(strings.NewReader(out))
		for _, want := range []int{500, 200} {
			resp, err := nethttp.ReadResponse(br, nil)
			if err != nil {
				t.Fatalf("status %d: %v\n%q", code, err, out)
			}
			io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != want {
				t.Fatalf("status %d: got %d, want %d\n%q", code, resp.StatusCode, want, out)
			}
		}
	}
}

// TestInvalidHeaderNamesDropped checks that a response header with an
// empty or non-token name is dropped instead of corrupting the response.
func TestInvalidHeaderNamesDropped(t *testing.T) {
	s := &Server{Handler: func(ctx *RequestCtx) {
		ctx.SetHeader("", "empty")
		ctx.SetHeader("Bad Name", "space")
		ctx.SetHeader("X:Y", "colon")
		ctx.AddHeader("X-Ctl\x7f", "del")
		ctx.AddHeader("caf\xc3\xa9", "utf8")
		ctx.SetHeaderBytes([]byte("A\r\nB"), []byte("crlf"))
		ctx.SetHeaderBytes(nil, []byte("nil"))
		ctx.SetHeader("X-Good", "1")
		ctx.DelHeader("")
		ctx.WriteString("ok")
	}}
	out, _ := serveOnce(t, testLoop(t), s, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	resp, err := nethttp.ReadResponse(bufio.NewReader(strings.NewReader(out)), nil)
	if err != nil {
		t.Fatalf("net/http rejects the response: %v\n%q", err, out)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "ok" || resp.Header.Get("X-Good") != "1" {
		t.Fatalf("%d %q %v", resp.StatusCode, body, resp.Header)
	}
	for _, v := range []string{"empty", "space", "colon", "del", "utf8", "crlf", "nil"} {
		if strings.Contains(out, v) {
			t.Errorf("invalid header with value %q was sent:\n%q", v, out)
		}
	}
}

func TestHeadAndNoBodyStatuses(t *testing.T) {
	l := testLoop(t)
	s := &Server{Handler: func(ctx *RequestCtx) {
		switch string(ctx.Path()) {
		case "/204":
			ctx.SetStatus(204)
		case "/empty":
			return
		}
		ctx.WriteString("body")
	}}
	out, _ := serveOnce(t, l, s, "HEAD / HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.Contains(out, "Content-Length: 4\r\n") || strings.HasSuffix(out, "body") {
		t.Fatalf("HEAD: %q", out)
	}
	// A HEAD Handler that writes nothing gets no Content-Length (net/http
	// behaviour); GET still gets "Content-Length: 0".
	out, _ = serveOnce(t, l, s, "HEAD /empty HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.HasPrefix(out, "HTTP/1.1 200 ") || strings.Contains(out, "Content-Length") || strings.Contains(out, "Content-Type") {
		t.Fatalf("HEAD without body: %q", out)
	}
	out, _ = serveOnce(t, l, s, "GET /empty HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.Contains(out, "Content-Length: 0\r\n") {
		t.Fatalf("GET without body: %q", out)
	}
	out, _ = serveOnce(t, l, s, "GET /204 HTTP/1.1\r\nHost: x\r\n\r\n")
	if strings.Contains(out, "Content-Length") || strings.HasSuffix(out, "body") {
		t.Fatalf("204: %q", out)
	}
}

func TestExpectContinue(t *testing.T) {
	s := &Server{Handler: func(ctx *RequestCtx) { ctx.Write(ctx.Body()) }}
	h := newHarness(t, testLoop(t), s)
	h.sendString("POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\n")
	if out := h.out(); out != "HTTP/1.1 100 Continue\r\n\r\n" {
		t.Fatalf("got %q", out)
	}
	h.sendString("abc")
	if out := h.out(); !strings.HasSuffix(out, "\r\n\r\nabc") || strings.Count(out, "100 Continue") != 1 {
		t.Fatalf("got %q", out)
	}
}

func TestHandlerPanic500(t *testing.T) {
	l := testLoop(t)
	s := &Server{Logger: discardLogger(), Handler: func(ctx *RequestCtx) {
		ctx.WriteString("partial")
		panic("boom")
	}}
	out, closed := serveOnce(t, l, s, "GET / HTTP/1.1\r\nHost: x\r\n\r\nGET /next HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.HasPrefix(out, "HTTP/1.1 500 ") || !closed || strings.Contains(out, "partial") {
		t.Fatalf("closed=%v out=%q", closed, out)
	}
	if strings.Count(out, "HTTP/1.1") != 1 {
		t.Fatalf("pipelined request served after panic: %q", out)
	}
}

func TestByteAtATime(t *testing.T) {
	var bodies []string
	s := &Server{Handler: func(ctx *RequestCtx) { bodies = append(bodies, string(ctx.Path())+":"+string(ctx.Body())) }}
	raw := "POST /one HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc" +
		"POST /two HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\ndef\r\n2\r\ngh\r\n0\r\n\r\n" +
		"GET /three HTTP/1.1\r\nHost: x\r\n\r\n"
	h := newHarness(t, testLoop(t), s)
	for i := 0; i < len(raw); i++ {
		h.send([]byte{raw[i]})
	}
	if got := strings.Join(bodies, ","); got != "/one:abc,/two:defgh,/three:" {
		t.Fatalf("got %q", got)
	}
	if out := h.out(); h.closing() || strings.Count(out, "200 OK") != 3 {
		t.Fatalf("out %q", out)
	}
}

// TestChunkedRawCap sends one-byte chunks, whose framing is five times the
// payload: the request must be refused with 413 before the reassembly
// buffer grows far beyond twice MaxBodyBytes.
func TestChunkedRawCap(t *testing.T) {
	const maxBody = 1 << 20
	s := &Server{MaxBodyBytes: maxBody, Handler: func(ctx *RequestCtx) {}}
	h := newHarness(t, testLoop(t), s)
	h.sendString("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n")
	piece := []byte(strings.Repeat("1\r\nx\r\n", 256<<10/6))
	maxCap, sent := 0, 0
	for !h.closing() && sent < 8*maxBody {
		h.send(piece)
		sent += len(piece)
		onLoop(t, h.l, func() { maxCap = max(maxCap, cap(h.c.buf)) })
	}
	if out := h.out(); !strings.HasPrefix(out, "HTTP/1.1 413 ") {
		t.Fatalf("after %d raw bytes: %q", sent, firstLine(out))
	}
	if maxCap >= 3<<20 {
		t.Fatalf("reassembly buffer grew to %d bytes (MaxBodyBytes %d)", maxCap, maxBody)
	}
	t.Logf("413 after %d raw bytes; buffer cap peaked at %d", sent, maxCap)
	h.lose()
}
