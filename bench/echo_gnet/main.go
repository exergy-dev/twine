// Command echo_gnet is the gnet v2 echo server used as a comparison target in
// the benchmarks (design Section 12). Drive it with bench/echoclient.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/exergy-dev/twine/bench"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9002", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	flag.Parse()

	s, err := bench.StartGnet(*addr, *loops)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echo_gnet listening on %s", s.Addr())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	s.Close()
}
