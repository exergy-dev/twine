// Command echo_twine is the twine echo server used by the benchmarks
// (design Section 12). Drive it with bench/echoclient.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
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

	s, err := bench.StartTwine(*addr, *loops, opts...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echo_twine listening on %s with %d loops", s.Addr(), len(s.Engine.Loops()))
	waitForSignal()
	s.Close()
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
}
