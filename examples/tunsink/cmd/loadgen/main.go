// Command loadgen opens N TCP connections to a tunsink/baseline server and
// streams length-prefixed IPv4/UDP packets as fast as the server accepts
// them for a fixed duration, then reports the sent rate.
//
// With -aes every frame body is nonce(12) || AES-256-GCM ciphertext || tag(16)
// of the -size byte packet. The -batch frames are encrypted once at startup
// with distinct nonces (frame index as a little-endian counter) and the same
// batch is streamed for the whole run: nonce reuse across batches is a
// benchmark shortcut, the servers do not check nonce uniqueness.
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

	"github.com/exergy-dev/twine/examples/tunsink/internal/tunpkt"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	conns := flag.Int("conns", 64, "connections")
	size := flag.Int("size", 512, "IP packet size in bytes")
	batch := flag.Int("batch", 64, "frames per write")
	dur := flag.Duration("duration", 10*time.Second, "run time")
	useAES := flag.Bool("aes", false, "encrypt every frame with AES-256-GCM")
	keyHex := flag.String("key", tunpkt.DefaultKeyHex(), "AES-256 key, 64 hex characters")
	flag.Parse()

	pkt := tunpkt.IPv4UDP([4]byte{10, 0, 0, 1}, [4]byte{10, 0, 0, 2}, 4000, 5000, *size)
	var frame []byte
	if *useAES {
		key, err := tunpkt.ParseKey(*keyHex)
		if err != nil {
			log.Fatal(err)
		}
		gcm, err := tunpkt.NewAEAD(key)
		if err != nil {
			log.Fatal(err)
		}
		frame = make([]byte, 0, (4+len(pkt)+tunpkt.AESOverhead)*(*batch))
		nonce := make([]byte, 12)
		for i := 0; i < *batch; i++ {
			binary.LittleEndian.PutUint64(nonce, uint64(i))
			body := tunpkt.Seal(gcm, nonce, pkt)
			frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
			frame = append(frame, body...)
		}
	} else {
		frame = make([]byte, 0, (4+len(pkt))*(*batch))
		for i := 0; i < *batch; i++ {
			frame = binary.BigEndian.AppendUint32(frame, uint32(len(pkt)))
			frame = append(frame, pkt...)
		}
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
