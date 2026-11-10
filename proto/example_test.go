package proto_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/proto"
)

// echo is a line protocol: it embeds LineReceiver and sets its callbacks in
// the factory.
type echo struct {
	proto.LineReceiver
}

var echoPrefix = []byte("echo: ")

func newEcho(net.Addr) twine.Protocol {
	e := &echo{}
	e.MaxLineLength = 1024
	e.LineReceived = func(line []byte) {
		// line is valid only during the call; Write and SendLine copy it.
		e.Transport().Write(echoPrefix)
		e.SendLine(line)
	}
	return e
}

func ExampleLineReceiver() {
	eng := twine.New(twine.Loops(1))
	lst, err := eng.ListenTCP("127.0.0.1:0", newEcho)
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()

	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprint(c, "hello\r\n")
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(strings.TrimSpace(line))
	c.Close()

	eng.Stop()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
	// Output: echo: hello
}

// frames speaks 2-byte little-endian length-prefixed frames.
type frames struct {
	proto.LengthPrefixed
}

func ExampleLengthPrefixed() {
	factory := func(net.Addr) twine.Protocol {
		p := &frames{}
		p.PrefixLen = 2 // 1, 2 or 4; 0 means 4
		p.LittleEndian = true
		p.MaxFrame = 4096
		p.FrameReceived = func(f []byte) {
			// Echo the frame. SendFrame fails only when f does not fit
			// the prefix (see MaxSendable).
			if err := p.SendFrame(f); err != nil {
				p.Transport().Abort()
			}
		}
		p.OnConnectionLost = func(err error) {
			if errors.Is(err, proto.ErrFrameTooLarge) {
				log.Print("peer sent an oversized frame")
			}
		}
		return p
	}
	eng := twine.New()
	if _, err := eng.ListenTCP("127.0.0.1:9002", factory); err != nil {
		log.Fatal(err)
	}
	if err := eng.Run(context.Background()); err != nil {
		log.Print(err)
	}
}

// peer is an AMP endpoint; one type serves both sides.
type peer struct {
	proto.AMP
}

func ExampleAMP() {
	eng := twine.New(twine.Loops(1))

	// The server answers Sum at once; reply may also be called later, from
	// any goroutine.
	server := func(net.Addr) twine.Protocol {
		p := &peer{}
		p.Handle = func(cmd string, args proto.Args, reply func(proto.Args, error)) {
			if cmd != "Sum" {
				reply(nil, proto.ErrUnhandledCommand) // the caller gets an UNHANDLED error
				return
			}
			a, _ := strconv.Atoi(args["a"])
			b, _ := strconv.Atoi(args["b"])
			reply(proto.Args{"total": strconv.Itoa(a + b)}, nil)
		}
		return p
	}
	lst, err := eng.ListenTCP("127.0.0.1:0", server)
	if err != nil {
		log.Fatal(err)
	}

	// The client calls Sum once connected; the answer arrives on its loop.
	client := func(net.Addr) twine.Protocol {
		p := &peer{}
		p.OnConnectionMade = func(twine.Transport) {
			p.Call("Sum", proto.Args{"a": "13", "b": "29"}).Then(func(res proto.Args) {
				fmt.Println("total:", res["total"])
				eng.Stop()
			}).Catch(func(err error) {
				log.Print(err)
				eng.Stop()
			})
		}
		return p
	}
	eng.Dial(lst.Addr().String(), client)

	if err := eng.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output: total: 42
}

func ExampleReconnectingClient() {
	eng := twine.New()
	newClient := func(net.Addr) twine.Protocol {
		c := &proto.LineReceiver{}
		c.OnConnectionMade = func(twine.Transport) { c.SendLine([]byte("hello")) }
		c.LineReceived = func(line []byte) { log.Printf("recv: %s", line) }
		return c
	}
	rc := proto.NewReconnectingClient(eng, "127.0.0.1:9001", newClient,
		proto.InitialDelay(500*time.Millisecond),
		proto.MaxRetries(10),
		proto.OnStateChange(func(connected bool, err error) {
			switch {
			case connected:
				log.Print("connected")
			case errors.Is(err, proto.ErrGaveUp):
				log.Print(err)
				eng.Stop()
			default:
				log.Printf("lost (%v); retrying", err)
			}
		}))
	rc.Start()
	if err := eng.Run(context.Background()); err != nil {
		log.Print(err)
	}
}

func ExampleBroadcast() {
	var room proto.Broadcast // the zero value is an empty group
	factory := func(net.Addr) twine.Protocol {
		m := &proto.LineReceiver{}
		m.OnConnectionMade = func(t twine.Transport) { room.Add(t) }
		m.OnConnectionLost = func(error) { room.Remove(m.Transport()) }
		m.LineReceived = func(line []byte) {
			// Send copies the message once and each loop writes that
			// copy to its own members.
			room.Send(fmt.Appendf(nil, "%s\r\n", line))
		}
		return m
	}
	eng := twine.New()
	if _, err := eng.ListenTCP("127.0.0.1:9003", factory); err != nil {
		log.Fatal(err)
	}
	if err := eng.Run(context.Background()); err != nil {
		log.Print(err)
	}
}
