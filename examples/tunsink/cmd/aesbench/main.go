// Command aesbench measures the standalone cost of one AES-256-GCM Open of a
// -size byte frame (nonce || ciphertext || tag), single thread, into a
// preallocated buffer: the per-frame decrypt cost the -aes servers pay.
//
//	taskset -c 2 go run ./cmd/aesbench -size 512
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/exergy-dev/twine/examples/tunsink/internal/tunpkt"
)

func main() {
	size := flag.Int("size", 512, "plaintext bytes")
	n := flag.Int("n", 5_000_000, "iterations")
	flag.Parse()
	gcm, err := tunpkt.NewAEAD(tunpkt.DefaultKey())
	if err != nil {
		log.Fatal(err)
	}
	body := tunpkt.Seal(gcm, make([]byte, 12), make([]byte, *size))
	dst := make([]byte, 0, *size)
	var ok int
	t := time.Now()
	for i := 0; i < *n; i++ {
		if out, good := tunpkt.Open(gcm, dst, body); good && len(out) == *size {
			ok++
		}
	}
	el := time.Since(t)
	fmt.Printf("go crypto/aes+cipher.GCM size=%d ns/open=%.1f GB/s=%.2f ok=%d\n", *size, float64(el.Nanoseconds())/float64(*n), float64(*n)*float64(*size)/el.Seconds()/1e9, ok)
}
