// Command http_twine is the twine/http plaintext hello-world server used by
// the HTTP benchmarks (design Section 12 http_plaintext). Drive it with
// bench/httpclient or wrk.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/robert-malhotra/twine"
	"github.com/robert-malhotra/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	pin := flag.String("pin", "", "CPU list for twine.PinLoops, e.g. 0,2,4,6 (loop i -> cpus[i%len]); empty = no pinning")
	flag.Parse()

	var opts []twine.Option
	if *pin != "" {
		cpus, err := bench.ParseCPUs(*pin)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, twine.PinLoops(cpus...))
	}

	s, err := bench.StartHTTPTwine(*addr, *loops, opts...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("http_twine listening on %s with %d loops", s.Addr(), len(s.Engine.Loops()))
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	s.Close()
}
