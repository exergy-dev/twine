// Command echo is the reference twine echo server: every chunk a client sends
// is written straight back. Flags set the listen address and loop count, an
// optional runtime trace is written on request, and SIGINT/SIGTERM shut it
// down gracefully.
//
//	go run ./examples/echo -addr :9000 -loops 4 -trace echo.trace
//	go tool trace echo.trace
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/trace"
	"syscall"
	"time"

	"github.com/exergy-dev/twine"
)

// echo writes every received chunk straight back. Write copies only if the
// socket cannot take the bytes immediately.
type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t; t.SetIdleTimeout(30 * time.Second) }
func (e *echo) DataReceived(b []byte)            { e.t.Write(b) }
func (e *echo) ConnectionLost(err error)         {}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	traceFile := flag.String("trace", "", "write a runtime/trace to this file")
	flag.Parse()

	if *traceFile != "" {
		f, err := os.Create(*traceFile)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			log.Fatal(err) //nolint:gocritic // deferred close handled above
		}
		defer trace.Stop()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := twine.New(twine.Loops(*loops))
	ln, err := eng.ListenTCP(*addr, twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} }))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echo: listening on %s with %d loops", ln.Addr(), len(eng.Loops()))
	if err := eng.Run(ctx); err != nil {
		log.Print(err)
		return // deferred trace.Stop and f.Close still run
	}
	log.Print("echo: shut down cleanly")
}
