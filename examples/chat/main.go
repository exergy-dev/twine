// Command chat is a line-based chat room built on proto.LineReceiver and
// proto.Broadcast. Connect with any line-oriented client:
//
//	go run ./examples/chat --addr :9001
//	nc -C localhost 9001        # or: telnet localhost 9001
//
// Every line a client sends is broadcast to every connected client, across
// all loops. "/nick NAME" renames the sender and "/quit" disconnects.
//
// With --trace FILE the run is recorded with runtime/trace (design Section
// 11); inspect it with "go tool trace FILE".
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/trace"
	"syscall"

	"github.com/robert-malhotra/twine"
	"github.com/robert-malhotra/twine/proto"
)

// client is one chat participant.
type client struct {
	proto.LineReceiver
	room *proto.Broadcast
	nick string
}

func newClient(room *proto.Broadcast) twine.Factory {
	return twine.FactoryFunc(func(peer net.Addr) twine.Protocol {
		c := &client{room: room, nick: peer.String()}
		c.MaxLineLength = 1024
		c.OnConnectionMade = c.connectionMade
		c.LineReceived = c.lineReceived
		c.OnConnectionLost = c.connectionLost
		return c
	})
}

func (c *client) connectionMade(t twine.Transport) {
	c.SendLine(t, fmt.Appendf(nil, "* welcome %s; %d other(s) here. /nick NAME to rename, /quit to leave", c.nick, c.room.Len()))
	c.room.Send(fmt.Appendf(nil, "* %s joined\r\n", c.nick))
	c.room.Add(t)
}

func (c *client) lineReceived(line []byte) {
	switch {
	case bytes.HasPrefix(line, []byte("/nick ")):
		nick := string(bytes.TrimSpace(line[len("/nick "):]))
		if nick == "" {
			return
		}
		c.room.Send(fmt.Appendf(nil, "* %s is now %s\r\n", c.nick, nick))
		c.nick = nick
	case bytes.Equal(line, []byte("/quit")):
		c.SendLine(nil, []byte("* bye"))
		c.Transport().LoseConnection()
	case len(bytes.TrimSpace(line)) == 0:
	default:
		// Send copies the message once; every loop shares that copy.
		c.room.Send(fmt.Appendf(nil, "<%s> %s\r\n", c.nick, line))
	}
}

func (c *client) connectionLost(err error) {
	c.room.Remove(c.Transport())
	reason := ""
	if err != nil {
		reason = " (" + err.Error() + ")"
	}
	c.room.Send(fmt.Appendf(nil, "* %s left%s\r\n", c.nick, reason))
}

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	loops := flag.Int("loops", 0, "event loops (0 = GOMAXPROCS)")
	traceFile := flag.String("trace", "", "write a runtime/trace to this file")
	flag.Parse()

	if *traceFile != "" {
		f, err := os.Create(*traceFile)
		if err != nil {
			log.Fatal(err)
		}
		if err := trace.Start(f); err != nil {
			log.Fatal(err)
		}
		defer func() {
			trace.Stop()
			if err := f.Close(); err != nil {
				log.Print(err)
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	room := proto.NewBroadcast()
	eng := twine.New(twine.Loops(*loops))
	lst, err := eng.ListenTCP(*addr, newClient(room))
	if err != nil {
		stop()
		log.Fatal(err) //nolint:gocritic // stop() called explicitly above
	}
	log.Printf("chat listening on %s with %d loops", lst.Addr(), len(eng.Loops()))
	if err := eng.Run(ctx); err != nil {
		log.Print(err)
	}
}
