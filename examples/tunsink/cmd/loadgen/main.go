// Command loadgen opens N TCP connections to a tunsink/baseline server and
// streams length-prefixed IPv4/UDP packets as fast as the server accepts
// them for a fixed duration, then reports the sent rate.
//
//	go run ./cmd/loadgen -addr 127.0.0.1:9000 -conns 64 -size 512 -duration 10s
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robert-malhotra/twine/examples/tunsink/internal/tunpkt"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	conns := flag.Int("conns", 64, "connections")
	size := flag.Int("size", 512, "IP packet size in bytes")
	batch := flag.Int("batch", 64, "frames per write")
	dur := flag.Duration("duration", 10*time.Second, "run time")
	flag.Parse()

	pkt := tunpkt.IPv4UDP([4]byte{10, 0, 0, 1}, [4]byte{10, 0, 0, 2}, 4000, 5000, *size)
	frame := make([]byte, 0, (4+len(pkt))*(*batch))
	for i := 0; i < *batch; i++ {
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(pkt)))
		frame = append(frame, pkt...)
	}

	var sent atomic.Uint64
	stopAt := time.Now().Add(*dur)
	var wg sync.WaitGroup
	cs := make([]net.Conn, *conns)
	for i := range cs {
		c, err := net.Dial("tcp", *addr)
		if err != nil {
			log.Fatal(err)
		}
		cs[i] = c
	}
	start := time.Now()
	for _, c := range cs {
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			for time.Now().Before(stopAt) {
				c.SetWriteDeadline(stopAt)
				if _, err := c.Write(frame); err != nil {
					return
				}
				sent.Add(uint64(*batch))
			}
		}(c)
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	n := sent.Load()
	fmt.Fprintf(os.Stdout, "loadgen conns=%d size=%d sent=%d frames/s=%.0f MB/s=%.1f\n",
		*conns, *size, n, float64(n)/el, float64(n)*float64(*size)/el/1e6)
}
