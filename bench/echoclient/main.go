// Command echoclient is the load generator for the echo servers. It opens
// -conns connections, runs a -size byte ping-pong (one message in flight per
// connection) for -duration and reports msgs/s and latency percentiles.
//
//	go run ./echo_twine -loops 8 &
//	go run ./echoclient -addr 127.0.0.1:9000 -conns 1000 -size 64 -duration 10s
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/robert-malhotra/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	conns := flag.Int("conns", 1000, "concurrent connections")
	size := flag.Int("size", 64, "message size in bytes")
	duration := flag.Duration("duration", 10*time.Second, "test duration")
	flag.Parse()

	if *size < 1 {
		log.Fatal("-size must be at least 1")
	}
	cs, err := bench.DialAll(*addr, *conns)
	if err != nil {
		log.Fatal(err)
	}
	defer bench.CloseAll(cs)
	// One warm-up round trip per connection.
	if _, err := bench.PingPong(cs, *size, 1, time.Time{}, false); err != nil {
		log.Fatal(err)
	}
	res, err := bench.PingPong(cs, *size, 0, time.Now().Add(*duration), true)
	if err != nil {
		log.Fatal(err)
	}
	h := res.Latency
	fmt.Printf("conns=%d size=%dB duration=%v\n", *conns, *size, res.Elapsed.Round(time.Millisecond))
	fmt.Printf("msgs=%d  msgs/s=%.0f  MB/s=%.1f (each direction)\n",
		res.Msgs, res.MsgsPerSec(), res.MsgsPerSec()*float64(*size)/1e6)
	fmt.Printf("latency p50=%v p90=%v p99=%v p99.9=%v max=%v\n",
		h.Percentile(0.50), h.Percentile(0.90), h.Percentile(0.99), h.Percentile(0.999), h.Max())
}
