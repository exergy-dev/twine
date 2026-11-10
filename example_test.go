package twine_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/exergy-dev/twine"
)

// run starts eng in the background and returns a function that waits for
// Run to return.
func run(eng *twine.Engine) (wait func()) {
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	return func() {
		if err := <-done; err != nil {
			log.Fatal(err)
		}
	}
}

// greeter writes one line as soon as it is connected and prints the reply.
type greeter struct {
	eng *twine.Engine
	t   twine.Transport
}

func (g *greeter) ConnectionMade(t twine.Transport) { g.t = t; t.Write([]byte("hello\n")) }
func (g *greeter) DataReceived(b []byte) {
	fmt.Print("reply: ", string(b)) // b is valid only during the call
	g.t.LoseConnection()
}
func (g *greeter) ConnectionLost(error) { g.eng.Stop() }

// Dial connects to an echo server on the same engine and exchanges one line.
func ExampleEngine_Dial() {
	eng := twine.New(twine.Loops(2))
	lst, err := eng.ListenTCP("127.0.0.1:0", twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echo{} }))
	if err != nil {
		log.Fatal(err)
	}
	wait := run(eng)

	eng.Dial(lst.Addr().String(), twine.FactoryFunc(func(net.Addr) twine.Protocol {
		return &greeter{eng: eng}
	})).Catch(func(err error) {
		log.Print(err)
		eng.Stop()
	})
	wait()
	// Output: reply: hello
}

// CallLater schedules a function on a loop; it must itself be called on the
// loop, so another goroutine enters through CallFromThread first.
func ExampleLoop_CallLater() {
	eng := twine.New(twine.Loops(1))
	wait := run(eng)

	loop := eng.Loops()[0]
	loop.CallFromThread(func() {
		start := loop.Now()
		loop.CallLater(20*time.Millisecond, func() {
			fmt.Println("fired after at least 20ms:", loop.Now().Sub(start) >= 20*time.Millisecond)
			eng.Stop()
		})
	})
	wait()
	// Output: fired after at least 20ms: true
}

// A Future settles from any goroutine; its callbacks run on its loop.
func ExampleFuture_Then() {
	eng := twine.New(twine.Loops(1))
	wait := run(eng)

	loop := eng.Loops()[0]
	fut, res := twine.NewFuture[int](loop)
	fut.Then(func(v int) {
		fmt.Println("resolved with", v, "on the loop:", loop.InLoop())
	}).Catch(func(err error) {
		fmt.Println("rejected:", err)
	}).Finally(eng.Stop)

	go res.Resolve(42) // any goroutine
	wait()
	// Output: resolved with 42 on the loop: true
}

// DeferToThread runs blocking work on the worker pool and delivers the
// result back on the loop.
func ExampleDeferToThread() {
	eng := twine.New(twine.Loops(1))
	wait := run(eng)

	loop := eng.Loops()[0]
	twine.DeferToThread(loop, func() (string, error) {
		time.Sleep(10 * time.Millisecond) // stands in for a blocking call
		return strings.ToUpper("blocking work done"), nil
	}).Then(func(s string) {
		fmt.Println(s)
	}).Catch(func(err error) {
		fmt.Println("failed:", err)
	}).Finally(eng.Stop)
	wait()
	// Output: BLOCKING WORK DONE
}

// shouter is written in blocking style: under twine.Blocking it runs on its
// own goroutine, so it may sleep or call blocking APIs, and its Transport is
// safe to use from that goroutine.
type shouter struct{ t twine.Transport }

func (s *shouter) ConnectionMade(t twine.Transport) { s.t = t }
func (s *shouter) DataReceived(b []byte) {
	time.Sleep(time.Millisecond) // would stall the loop without Blocking
	s.t.Write([]byte(strings.ToUpper(string(b))))
	s.t.LoseConnection()
}
func (s *shouter) ConnectionLost(error) {}

// Blocking wraps a Factory so each connection's Protocol runs on its own
// goroutine: the migration path for code not ready for callbacks.
func ExampleBlocking() {
	eng := twine.New(twine.Loops(1))
	lst, err := eng.ListenTCP("127.0.0.1:0", twine.Blocking(twine.FactoryFunc(func(net.Addr) twine.Protocol {
		return &shouter{}
	})))
	if err != nil {
		log.Fatal(err)
	}
	wait := run(eng)
	defer wait()
	defer eng.Stop()

	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("quiet please")); err != nil {
		log.Fatal(err)
	}
	reply, err := io.ReadAll(c)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply))
	// Output: QUIET PLEASE
}
