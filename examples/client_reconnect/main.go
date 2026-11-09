// Command client_reconnect demonstrates proto.ReconnectingClient: it keeps a
// line-based connection to --addr alive, reconnecting with exponential
// backoff and full jitter, and prints every state change and received line.
//
//	go run ./examples/chat --addr :9001 &
//	go run ./examples/client_reconnect --addr localhost:9001
//
// Stop and restart the server to watch it reconnect. Ctrl-C stops trying and
// exits.
package main

import (
	"context"
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
	c.OnConnectionMade = func(t twine.Transport) {
		c.SendLine(t, []byte("hello from client_reconnect"))
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
	var rc *proto.ReconnectingClient
	rc = proto.NewReconnectingClient(eng.Loops()[0], *addr, twine.FactoryFunc(newLineClient),
		proto.InitialDelay(*initial),
		proto.MaxDelay(*maxDelay),
		proto.MaxRetries(*retries),
		proto.DialOptions(twine.DialTimeout(5*time.Second)),
		proto.OnStateChange(func(connected bool) {
			// Runs on the client's loop, so loop-affine accessors are safe.
			if connected {
				log.Printf("state: connected to %s (attempt %d)", *addr, rc.Attempts())
				return
			}
			log.Printf("state: disconnected (%v); retrying", rc.LastError())
		}))
	rc.Start()

	go func() {
		// Report attempts that never got as far as a connection.
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !rc.Connected() {
					log.Printf("state: not connected after %d attempt(s)", rc.Attempts())
				}
				if rc.GaveUp() {
					log.Print("giving up")
					stop()
					return
				}
			}
		}
	}()

	go func() {
		<-ctx.Done()
		rc.StopTrying()
	}()
	if err := eng.Run(ctx); err != nil {
		log.Print(err)
	}
}
