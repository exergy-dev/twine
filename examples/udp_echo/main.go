// Command udp_echo is a twine UDP echo server: every datagram is sent back to
// its sender. Datagrams are received with recvmmsg and replies are batched
// with sendmmsg at the end of each loop iteration (Linux).
//
//	go run ./examples/udp_echo -addr :9001
//	echo hello | nc -u -w1 127.0.0.1 9001
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/exergy-dev/twine"
)

// udpEcho implements twine.DatagramProtocol.
type udpEcho struct{ t twine.DatagramTransport }

func (u *udpEcho) StartProtocol(t twine.DatagramTransport) { u.t = t }

// DatagramReceived echoes b to its sender. WriteTo copies b into the
// outbound batch, so the shared read buffer may be reused after return.
func (u *udpEcho) DatagramReceived(b []byte, from net.Addr) { u.t.WriteTo(b, from) }

func (u *udpEcho) StopProtocol() {}

func main() {
	addr := flag.String("addr", ":9001", "UDP listen address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := twine.New(twine.Loops(1))
	ln, err := eng.ListenUDP(*addr, &udpEcho{})
	if err != nil {
		stop()
		log.Fatal(err) //nolint:gocritic // stop() called explicitly above
	}
	log.Printf("udp_echo: listening on %s", ln.Addr())
	if err := eng.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
