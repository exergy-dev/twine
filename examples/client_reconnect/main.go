// Command client_reconnect demonstrates proto.ReconnectingClient: it keeps a
// line-based connection to -addr alive, reconnecting with exponential
// backoff and full jitter, and prints every state change and received line.
//
//	go run ./examples/chat -addr :9001 &
//	go run ./examples/client_reconnect -addr localhost:9001
//
// Stop and restart the server to watch it reconnect. Ctrl-C stops the engine
// and exits. With -retries N the client gives up, and the program exits,
// after N+1 consecutive failed attempts.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/proto"
)

// lineClient prints every line it receives and greets the server.
type lineClient struct {
	proto.LineReceiver
}

func newLineClient(net.Addr) twine.Protocol {
	c := &lineClient{}
	c.OnConnectionMade = func(twine.Transport) {
		c.SendLine([]byte("hello from client_reconnect"))
	}
	c.LineReceived = func(line []byte) { log.Printf("recv: %s", line) }
	return c
}

func main() {
	addr := flag.String("addr", "localhost:9001", "server address")
	initial := flag.Duration("initial", time.Second, "initial backoff")
	maxDelay := flag.Duration("max", 60*time.Second, "maximum backoff")
	retries := flag.Int("retries", 0, "give up after this many consecutive failures (0 = never)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := twine.New(twine.Loops(1))
	rc := proto.NewReconnectingClient(eng.NextLoop(), *addr, newLineClient,
		proto.InitialDelay(*initial),
		proto.MaxDelay(*maxDelay),
		proto.MaxRetries(*retries),
		proto.DialOptions(twine.DialTimeout(5*time.Second)),
		proto.OnStateChange(func(connected bool, err error) {
			// Runs on the client's loop.
			switch {
			case connected:
				log.Printf("state: connected to %s", *addr)
			case ctx.Err() != nil:
				log.Print("state: stopped")
			case errors.Is(err, proto.ErrGaveUp):
				log.Printf("state: %v", err)
				eng.Stop()
			default:
				log.Printf("state: disconnected (%v); retrying", err)
			}
		}))
	rc.Start()
	if err := eng.Run(ctx); err != nil {
		log.Print(err)
	}
}
