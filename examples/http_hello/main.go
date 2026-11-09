// Command http_hello is a minimal twine HTTP server.
//
//	go run ./examples/http_hello -addr :8080 -loops 4
//	curl -i localhost:8080/
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/exergy-dev/twine"
	twhttp "github.com/exergy-dev/twine/http"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	flag.Parse()

	hello := []byte("Hello, World!")
	srv := &twhttp.Server{
		Name: "twine",
		Handler: func(ctx *twhttp.RequestCtx) {
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
