// Command http_twine is the twine/http plaintext hello-world server used by
// the HTTP benchmarks (design Section 12 http_plaintext). Drive it with
// bench/httpclient or wrk.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/robert-malhotra/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	flag.Parse()

	s, err := bench.StartHTTPTwine(*addr, *loops)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("http_twine listening on %s with %d loops", s.Addr(), len(s.Engine.Loops()))
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	s.Close()
}
