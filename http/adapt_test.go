package http

import (
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"testing"
)

func TestAdaptMux(t *testing.T) {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.Header().Set("X-Id", r.PathValue("id"))
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		fmt.Fprintf(w, "<html>item %s q=%s host=%s ua=%s</html>", r.PathValue("id"), r.URL.Query().Get("q"), r.Host, r.UserAgent())
	})
	mux.HandleFunc("POST /upload", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			nethttp.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(nethttp.StatusCreated)
		fmt.Fprintf(w, `{"len":%d,"cl":%d,"remote":%t}`, len(b), r.ContentLength, r.RemoteAddr != "")
	})
	s := &Server{Handler: Adapt(mux)}
	addr, _ := startServer(t, s)
	c := newClient()
	defer c.CloseIdleConnections()

	req, _ := nethttp.NewRequest("GET", "http://"+addr+"/items/42?q=x", nil)
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

// TestAdaptAllocs documents Adapt's per-request cost.
func TestAdaptAllocs(t *testing.T) {
	h := Adapt(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "hello")
	}))
	s := &Server{Handler: h}
	ft := &fakeTransport{}
	c := newFakeConn(s, ft)
	req := []byte("GET /hello?x=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n")
	c.DataReceived(req)
	allocs := testing.AllocsPerRun(200, func() {
		ft.reset()
		c.DataReceived(req)
	})
	t.Logf("Adapt: %v allocs per request (3 request headers)", allocs)
	if !strings.HasSuffix(ft.String(), "hello") {
		t.Fatalf("out %q", ft.String())
	}
}
