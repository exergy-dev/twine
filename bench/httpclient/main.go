// Command httpclient is a wrk-style HTTP/1.1 load generator: -conns
// keep-alive connections, one request in flight per connection (no
// pipelining), GET -path for -duration. It reports req/s and latency
// percentiles.
//
//	go run ./http_twine -loops 8 &
//	go run ./httpclient -addr 127.0.0.1:8080 -conns 256 -duration 10s
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/robert-malhotra/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "server address")
	conns := flag.Int("conns", 256, "concurrent keep-alive connections")
	path := flag.String("path", "/plaintext", "request path")
	duration := flag.Duration("duration", 10*time.Second, "test duration")
	flag.Parse()

	cs, err := bench.DialAll(*addr, *conns)
	if err != nil {
		log.Fatal(err)
	}
	defer bench.CloseAll(cs)
	req := bench.HTTPRequest(*addr, *path)
	if _, err := bench.HTTPLoad(cs, req, 1, time.Time{}, false); err != nil {
		log.Fatal(err)
	}
	res, err := bench.HTTPLoad(cs, req, 0, time.Now().Add(*duration), true)
	if err != nil {
		log.Fatal(err)
	}
	h := res.Latency
	fmt.Printf("conns=%d duration=%v\n", *conns, res.Elapsed.Round(time.Millisecond))
	fmt.Printf("requests=%d  req/s=%.0f\n", res.Msgs, res.MsgsPerSec())
	fmt.Printf("latency p50=%v p90=%v p99=%v p99.9=%v max=%v\n",
		h.Percentile(0.50), h.Percentile(0.90), h.Percentile(0.99), h.Percentile(0.999), h.Max())
}
