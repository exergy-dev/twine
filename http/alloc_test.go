package http

import (
	"bytes"
	"strings"
	"testing"

	"github.com/exergy-dev/twine"
)

// allocCase feeds chunks through one connection repeatedly and measures
// allocations per iteration. It runs on a real loop so the per-loop Date
// cache and its timer are the production ones.
type allocCase struct {
	name    string
	chunks  []string
	handler func(*RequestCtx)
	want    []string // substrings of the output of one iteration
	nresp   int      // responses per iteration
}

func helloHandler(ctx *RequestCtx) {
	ctx.SetHeader("X-Test", "1")
	ctx.SetContentType("text/plain")
	ctx.WriteString("hello")
}

func echoBodyHandler(ctx *RequestCtx) {
	ctx.SetHeaderBytes([]byte("X-Len"), ctx.Header("Content-Length"))
	ctx.Write(ctx.Body())
}

var allocCases = []allocCase{
	{
		name:    "GET",
		chunks:  []string{"GET /hello?x=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n"},
		handler: helloHandler,
		want:    []string{"HTTP/1.1 200 OK\r\n", "X-Test: 1\r\n", "Content-Length: 5\r\n", "Date: ", "\r\n\r\nhello"},
		nresp:   1,
	},
	{
		name:    "POST keep-alive Content-Length",
		chunks:  []string{"POST /submit HTTP/1.1\r\nHost: example.com\r\nConnection: keep-alive\r\nContent-Length: 11\r\nContent-Type: text/plain\r\n\r\nhello world"},
		handler: echoBodyHandler,
		want:    []string{"HTTP/1.1 200 OK\r\n", "X-Len: 11\r\n", "\r\n\r\nhello world"},
		nresp:   1,
	},
	{
		name: "two pipelined",
		chunks: []string{"GET /a HTTP/1.1\r\nHost: x\r\n\r\n" +
			"GET /b HTTP/1.1\r\nHost: x\r\n\r\n"},
		handler: helloHandler,
		want:    []string{"hello"},
		nresp:   2,
	},
	{
		name: "split across reads",
		chunks: []string{"POST /split HTTP/1.1\r\nHost: x\r\nContent-Le",
			"ngth: 10\r\n\r\n01234",
			"56789"},
		handler: echoBodyHandler,
		want:    []string{"\r\n\r\n0123456789"},
		nresp:   1,
	},
	{
		name: "chunked",
		chunks: []string{"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n",
			"6;ext=1\r\n world\r\n0\r\n\r\n"},
		handler: echoBodyHandler,
		want:    []string{"\r\n\r\nhello world"},
		nresp:   1,
	},
}

func TestParseRespondZeroAlloc(t *testing.T) {
	e := startEngine(t, twine.Loops(1), twine.Watchdog(0))
	l := e.Loops()[0]
	for _, tc := range allocCases {
		t.Run(tc.name, func(t *testing.T) {
			var allocs float64
			var out string
			var closing bool
			chunks := make([][]byte, len(tc.chunks))
			for i, s := range tc.chunks {
				chunks[i] = []byte(s)
			}
			onLoop(t, l, func() {
				s := &Server{Name: "twine", Handler: tc.handler}
				ft := &fakeTransport{loop: l}
				c := newFakeConn(s, ft)
				run := func() {
					ft.reset()
					for _, ch := range chunks {
						c.DataReceived(ch)
					}
				}
				run() // warm the buffers
				run()
				out = ft.String()
				allocs = testing.AllocsPerRun(1000, run)
				closing = ft.closing
				c.ConnectionLost(nil)
			})
			if closing {
				t.Fatalf("connection closed unexpectedly; output:\n%s", out)
			}
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("output missing %q:\n%s", w, out)
				}
			}
			if n := strings.Count(out, "HTTP/1.1 200 OK"); n != tc.nresp {
				t.Errorf("got %d responses, want %d:\n%s", n, tc.nresp, out)
			}
			t.Logf("allocs/iteration: %v", allocs)
			if allocs != 0 {
				t.Errorf("parse+respond allocates: %v allocs per iteration", allocs)
			}
		})
	}
}

// TestHeaderLookupZeroAlloc checks the case-insensitive lookup and
// VisitHeaders with a non-capturing callback.
func TestHeaderLookupZeroAlloc(t *testing.T) {
	s := &Server{}
	ft := &fakeTransport{}
	c := newFakeConn(s, ft)
	var got []byte
	n := 0
	visit := func(name, value []byte) { n += len(name) + len(value) }
	s.Handler = func(ctx *RequestCtx) {
		got = ctx.Header("x-custom-header")
		ctx.VisitHeaders(visit)
	}
	req := []byte("GET / HTTP/1.1\r\nHost: x\r\nX-Custom-Header: abc\r\n\r\n")
	c.DataReceived(req)
	if !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("Header = %q", got)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		ft.reset()
		c.DataReceived(req)
	})
	if allocs != 0 {
		t.Errorf("lookup allocates: %v", allocs)
	}
}

func BenchmarkParseRespond(b *testing.B) {
	s := &Server{Name: "twine", Handler: helloHandler}
	ft := &fakeTransport{}
	c := newFakeConn(s, ft)
	req := []byte(allocCases[0].chunks[0])
	b.ReportAllocs()
	b.SetBytes(int64(len(req)))
	for i := 0; i < b.N; i++ {
		ft.reset()
		c.DataReceived(req)
	}
}
