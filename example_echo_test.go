package twine_test

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/exergy-dev/twine"
)

type echo struct{ t twine.Transport }

func (e *echo) ConnectionMade(t twine.Transport) { e.t = t; t.SetIdleTimeout(30 * time.Second) }
func (e *echo) DataReceived(b []byte)            { e.t.Write(b) } // copies only if it must buffer
func (e *echo) ConnectionLost(err error)         {}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	eng := twine.New() // one loop per core
	if _, err := eng.ListenTCP(":9000", func(net.Addr) twine.Protocol { return &echo{} }); err != nil {
		log.Fatal(err)
	}
	if err := eng.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

// An echo server: the README's quick-start program. It runs until
// interrupted, so this example is compiled but not run.
func Example() { main() }
