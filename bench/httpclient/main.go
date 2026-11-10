//go:build linux

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
	"os"
	"time"

	"github.com/exergy-dev/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "server address")
	conns := flag.Int("conns", 256, "concurrent keep-alive connections")
	path := flag.String("path", "/plaintext", "request path")
	duration := flag.Duration("duration", 10*time.Second, "test duration")
	serverPID := flag.Int("server-pid", 0, "server process id: report its CPU use over the measured window")
	flag.Parse()

	cs, err := bench.DialAll(*addr, *conns)
	if err != nil {
		log.Fatal(err)
	}
	defer bench.CloseAll(cs)
	req := bench.HTTPRequest(*addr, *path)
	if _, err := bench.HTTPLoad(cs, req, 1, time.Time{}, false); err != nil {
		log.Fatal(err) //nolint:gocritic // exiting closes the sockets anyway
	}
	srv0, _ := bench.ProcCPU(*serverPID)
	cli0, _ := bench.ProcCPU(os.Getpid())
	res, err := bench.HTTPLoad(cs, req, 0, time.Now().Add(*duration), true)
	if err != nil {
		log.Fatal(err)
	}
	srv1, _ := bench.ProcCPU(*serverPID)
	cli1, _ := bench.ProcCPU(os.Getpid())
	h := res.Latency
	defer func() {
		fmt.Printf("RESULT rate=%.0f p50_us=%.0f p99_us=%.0f server_cores=%.2f client_cores=%.2f\n",
			res.MsgsPerSec(), float64(h.Percentile(0.50))/1e3, float64(h.Percentile(0.99))/1e3,
			(srv1-srv0).Seconds()/res.Elapsed.Seconds(), (cli1-cli0).Seconds()/res.Elapsed.Seconds())
	}()
	fmt.Printf("conns=%d duration=%v\n", *conns, res.Elapsed.Round(time.Millisecond))
	fmt.Printf("requests=%d  req/s=%.0f\n", res.Msgs, res.MsgsPerSec())
	fmt.Printf("latency p50=%v p90=%v p99=%v p99.9=%v max=%v\n",
		h.Percentile(0.50), h.Percentile(0.90), h.Percentile(0.99), h.Percentile(0.999), h.Max())
}
