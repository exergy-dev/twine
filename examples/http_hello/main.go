// Command http_hello is a minimal twine HTTP server.
//
//	go run ./examples/http_hello -addr :8080 -loops 4
//	curl -i localhost:8080/
//
// With --trace=out.trace it records a runtime/trace until it is stopped
// with Ctrl-C (view with `go tool trace out.trace`).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime/trace"
	"syscall"

	"github.com/robert-malhotra/twine"
	"github.com/robert-malhotra/twine/http"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
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
			log.Fatal(err) //nolint:gocritic // deferred close already handled above
		}
		defer trace.Stop()
	}

	hello := []byte("Hello, World!")
	srv := &http.Server{
		Name: "twine",
		Handler: func(ctx *http.RequestCtx) {
			ctx.SetContentType("text/plain")
			_, _ = ctx.Write(hello)
		},
	}

	var opts []twine.Option
	if *loops > 0 {
		opts = append(opts, twine.Loops(*loops))
	}
	eng := twine.New(opts...)
	lst, err := srv.Listen(eng, *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("http_hello listening on %s with %d loops", lst.Addr(), len(eng.Loops()))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := eng.Run(ctx); err != nil {
		log.Print(err)
	}
}
