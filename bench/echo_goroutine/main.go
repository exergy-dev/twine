// Command echo_goroutine is the net.Listen + goroutine-per-connection echo
// server used as the baseline in the benchmarks (BENCHMARKS.md). Drive it
// with bench/echoclient.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"

	"github.com/exergy-dev/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "listen address")
	loops := flag.Int("loops", 0, "GOMAXPROCS for the server (0 = leave default); there are no event loops")
	flag.Parse()

	if *loops > 0 {
		runtime.GOMAXPROCS(*loops)
	}
	s, err := bench.StartGoroutine(*addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echo_goroutine listening on %s (GOMAXPROCS=%d)", s.Addr(), runtime.GOMAXPROCS(0))
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	s.Close()
}
