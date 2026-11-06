// Command http_nethttp is the net/http plaintext hello-world server used as
// the baseline in the HTTP benchmarks (design Section 12 http_plaintext).
// Drive it with bench/httpclient or wrk.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"

	"github.com/robert-malhotra/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8081", "listen address")
	procs := flag.Int("loops", 0, "GOMAXPROCS for the server (0 = leave default); net/http has no event loops")
	flag.Parse()

	if *procs > 0 {
		runtime.GOMAXPROCS(*procs)
	}
	s, err := bench.StartHTTPNet(*addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("http_nethttp listening on %s (GOMAXPROCS=%d)", s.Addr(), runtime.GOMAXPROCS(0))
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	s.Close()
}
