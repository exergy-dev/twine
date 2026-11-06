// Command echo_twine is the twine echo server used by the benchmarks
// (design Section 12). Drive it with bench/echoclient.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/robert-malhotra/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	flag.Parse()

	s, err := bench.StartTwine(*addr, *loops)
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
