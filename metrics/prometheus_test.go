package metrics_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/metrics"
)

type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t }
func (e *echo) DataReceived(b []byte)            { e.t.Write(b) }
func (e *echo) ConnectionLost(error)             {}

func TestPrometheusHandler(t *testing.T) {
	eng := twine.New(twine.Loops(2), twine.ReusePort(false), twine.Watchdog(0))
	lst, err := eng.ListenTCP("127.0.0.1:0", func(net.Addr) twine.Protocol { return &echo{} })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	defer func() {
		eng.Stop()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	c, err := net.DialTimeout("tcp", lst.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(metrics.PrometheusHandler(eng))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("Content-Type = %q", ct)
	}
	text := string(body)
	for _, want := range []string{
		"# HELP twine_accepts_total Accepted connections\n# TYPE twine_accepts_total counter\n",
		`twine_accepts_total{loop="0"} 1` + "\n",
		`twine_accepts_total{loop="1"} 0` + "\n",
		`twine_read_bytes_total{loop="`,
		"# TYPE twine_connections gauge\n",
		`twine_iteration_seconds_max{loop="1"} `,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("output lacks %q", want)
		}
	}
	// Every series appears once per loop, and HELP lines are sorted by name.
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if name, ok := strings.CutPrefix(line, "# HELP "); ok {
			names = append(names, strings.Fields(name)[0])
		}
	}
	if len(names) < 20 || !sort.StringsAreSorted(names) {
		t.Errorf("series names not sorted or missing: %v", names)
	}
	if n := strings.Count(text, `{loop="1"}`); n != len(names) {
		t.Errorf("%d samples for loop 1, want %d", n, len(names))
	}
}
