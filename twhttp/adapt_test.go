package twhttp

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAdaptMux(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Id", r.PathValue("id"))
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		fmt.Fprintf(w, "<html>item %s q=%s host=%s ua=%s</html>", r.PathValue("id"), r.URL.Query().Get("q"), r.Host, r.UserAgent())
	})
	mux.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"len":%d,"cl":%d,"remote":%t}`, len(b), r.ContentLength, r.RemoteAddr != "")
	})
	s := &Server{Handler: Adapt(mux)}
	addr, _ := startServer(t, s)
	c := newClient()
	defer c.CloseIdleConnections()

	req, _ := http.NewRequest("GET", "http://"+addr+"/items/42?q=x", nil)
	req.Header.Set("User-Agent", "adapt-test")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	want := "<html>item 42 q=x host=" + addr + " ua=adapt-test</html>"
	if resp.StatusCode != 200 || string(body) != want || resp.Header.Get("X-Id") != "42" ||
		len(resp.Header.Values("Set-Cookie")) != 2 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("GET: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	resp, err = c.Post("http://"+addr+"/upload", "text/plain", strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 201 || string(body) != `{"len":5,"cl":5,"remote":true}` || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("POST: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	resp, err = c.Get("http://" + addr + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("404: %d", resp.StatusCode)
	}
}

// TestAdaptEarlyHints runs the net/http Early Hints idiom: the 103 is
// dropped and the final response is sent, without desynchronising a
// pipelined request.
func TestAdaptEarlyHints(t *testing.T) {
	h := Adapt(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Link", "</style.css>; rel=preload; as=style")
		w.WriteHeader(http.StatusEarlyHints)
		w.WriteHeader(http.StatusContinue)
		io.WriteString(w, "page "+r.URL.Path)
	}))
	out, closed := serveOnce(t, testLoop(t), &Server{Handler: h},
		"GET /a HTTP/1.1\r\nHost: x\r\n\r\nGET /b HTTP/1.1\r\nHost: x\r\n\r\n")
	br := bufio.NewReader(strings.NewReader(out))
	for _, path := range []string{"/a", "/b"} {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("%s: %v\n%q", path, err, out)
		}
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || string(body) != "page "+path || resp.Header.Get("Link") == "" {
			t.Fatalf("%s: %d %q %v", path, resp.StatusCode, body, resp.Header)
		}
	}
	if closed || strings.Contains(out, "103") {
		t.Fatalf("closed=%v out=%q", closed, out)
	}
}

// TestAdaptAllocs checks Adapt's documented per-request cost: roughly 10
// allocations plus 3 per request header (2 of slack).
func TestAdaptAllocs(t *testing.T) {
	const headers = 3 // Host, User-Agent, Accept
	h := Adapt(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "hello")
	}))
	s := &Server{Handler: h}
	l := testLoop(t)
	req := []byte("GET /hello?x=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n")
	var allocs float64
	var out string
	onLoop(t, l, func() {
		ft := &fakeTransport{loop: l}
		c := newFakeConn(s, ft)
		c.DataReceived(req)
		allocs = testing.AllocsPerRun(200, func() {
			ft.reset()
			c.DataReceived(req)
		})
		out = ft.String()
		c.ConnectionLost(nil)
	})
	t.Logf("Adapt: %v allocs per request (%d request headers)", allocs, headers)
	if !strings.HasSuffix(out, "hello") {
		t.Fatalf("out %q", out)
	}
	if limit := float64(10 + 3*headers + 2); allocs > limit {
		t.Errorf("Adapt: %v allocs per request, documented about 10+3*%d (limit %v)", allocs, headers, limit)
	}
}
