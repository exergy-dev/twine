package proto

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// twistedBox encodes pairs exactly as twisted.protocols.amp.AmpBox.serialize
// does: each key and value as a 16-bit big-endian length plus bytes, then an
// empty key.
func twistedBox(kv ...string) []byte {
	var b []byte
	for _, s := range kv {
		b = append(b, byte(len(s)>>8), byte(len(s)))
		b = append(b, s...)
	}
	return append(b, 0, 0)
}

// decodeBoxes is an independent decoder for boxes written by AMP.
func decodeBoxes(t *testing.T, b []byte) []map[string]string {
	t.Helper()
	var out []map[string]string
	cur := map[string]string{}
	for len(b) > 0 {
		if len(b) < 2 {
			t.Fatalf("truncated box")
		}
		n := int(b[0])<<8 | int(b[1])
		b = b[2:]
		if n == 0 {
			out = append(out, cur)
			cur = map[string]string{}
			continue
		}
		k := string(b[:n])
		b = b[n:]
		n = int(b[0])<<8 | int(b[1])
		cur[k] = string(b[2 : 2+n])
		b = b[2+n:]
	}
	return out
}

func sumHandler(cmd string, args Args) *twine.Future[Args] {
	switch cmd {
	case "Sum":
		a, _ := strconv.Atoi(args["a"])
		b, _ := strconv.Atoi(args["b"])
		return twine.Resolved[Args](nil, Args{"total": strconv.Itoa(a + b)})
	case "Fail":
		return twine.Rejected[Args](nil, errors.New("boom"))
	case "Coded":
		return twine.Rejected[Args](nil, &RemoteError{Code: "ZERO_DIVISION", Description: "divide by zero"})
	case "Panic":
		panic("handler exploded")
	}
	return nil
}

func TestAMPWireTwistedRequest(t *testing.T) {
	a := &AMP{Handle: sumHandler}
	ft := newFake()
	a.ConnectionMade(ft)
	// Requests as Twisted's callRemote would send them (tags are hex).
	in := twistedBox("_ask", "a", "_command", "Sum", "a", "13", "b", "29")
	in = append(in, twistedBox("_ask", "b", "_command", "Nope")...)
	in = append(in, twistedBox("_ask", "c", "_command", "Fail")...)
	in = append(in, twistedBox("_ask", "d", "_command", "Coded")...)
	in = append(in, twistedBox("_command", "Sum", "a", "1", "b", "1")...) // requiresAnswer=False
	in = append(in, twistedBox("_ask", "e", "_command", "Panic")...)
	feed(a, in, 1)
	boxes := decodeBoxes(t, ft.out)
	want := []map[string]string{
		{"_answer": "a", "total": "42"},
		{"_error": "b", "_error_code": "UNHANDLED", "_error_description": `Unhandled Command: "Nope"`},
		{"_error": "c", "_error_code": "UNKNOWN", "_error_description": "boom"},
		{"_error": "d", "_error_code": "ZERO_DIVISION", "_error_description": "divide by zero"},
		{"_error": "e", "_error_code": "UNKNOWN", "_error_description": "panic in AMP handler: handler exploded"},
	}
	if fmt.Sprint(boxes) != fmt.Sprint(want) {
		t.Fatalf("replies\n got %v\nwant %v", boxes, want)
	}
	if ft.aborted {
		t.Fatal("aborted")
	}
}

func TestAMPWireCall(t *testing.T) {
	a := &AMP{}
	ft := newFake()
	a.ConnectionMade(ft)
	f1 := a.Call("Sum", Args{"a": "1", "b": "2"})
	f2 := a.Call("Div", Args{"x": "1"})
	f3 := a.Call("Hang", nil)
	boxes := decodeBoxes(t, ft.out)
	if len(boxes) != 3 || boxes[0]["_ask"] != "1" || boxes[0]["_command"] != "Sum" || boxes[0]["a"] != "1" ||
		boxes[1]["_ask"] != "2" || boxes[2]["_command"] != "Hang" || len(boxes[2]) != 2 {
		t.Fatalf("requests %v", boxes)
	}
	feed(a, twistedBox("_answer", "1", "total", "3"), 2)
	feed(a, twistedBox("_error", "2", "_error_code", "ZERO_DIVISION", "_error_description", "nope"))
	if v, err := f1.Result(); err != nil || v["total"] != "3" || len(v) != 1 {
		t.Fatalf("answer %v %v", v, err)
	}
	_, err := f2.Result()
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != "ZERO_DIVISION" || re.Description != "nope" || !errors.Is(err, ErrRemote) {
		t.Fatalf("error %v", err)
	}
	if f3.Done() || a.Pending() != 1 {
		t.Fatalf("hang settled early; pending %d", a.Pending())
	}
	// An answer for an unknown tag is ignored.
	feed(a, twistedBox("_answer", "ff", "x", "y"))
	a.ConnectionLost(nil)
	if _, err := f3.Result(); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("pending call after lost: %v", err)
	}
	if _, err := a.Call("Sum", nil).Result(); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("call after lost: %v", err)
	}
}

func TestAMPBadArgs(t *testing.T) {
	a := &AMP{}
	if _, err := a.Call("x", nil).Result(); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("before connect: %v", err)
	}
	ft := newFake()
	a.ConnectionMade(ft)
	for _, args := range []Args{
		{"": "x"},
		{"_ask": "1"},
		{"_error_code": "1"},
		{string(make([]byte, 256)): "x"},
		{"k": string(make([]byte, 65536))},
	} {
		if _, err := a.Call("x", args).Result(); !errors.Is(err, ErrBadArgs) {
			t.Fatalf("args %.20q: %v", args, err)
		}
		if err := a.CallNoAnswer("x", args); !errors.Is(err, ErrBadArgs) {
			t.Fatalf("args %.20q: %v", args, err)
		}
	}
	if len(ft.out) != 0 {
		t.Fatal("invalid call was written")
	}
}

func TestAMPFramingLimits(t *testing.T) {
	var lost error
	a := &AMP{MaxBoxSize: 64, OnConnectionLost: func(err error) { lost = err }}
	ft := newFake()
	a.ConnectionMade(ft)
	feed(a, twistedBox("_command", "x", "big", string(make([]byte, 100))))
	if !ft.aborted || !errors.Is(a.Err(), twine.ErrFrameTooLarge) {
		t.Fatalf("aborted %v err %v", ft.aborted, a.Err())
	}
	a.ConnectionLost(nil)
	if !errors.Is(lost, twine.ErrFrameTooLarge) {
		t.Fatalf("lost %v", lost)
	}
}

func TestTransportAccessors(t *testing.T) {
	ft := newFake()
	for _, p := range []interface {
		twine.Protocol
		Transport() twine.Transport
	}{&LineReceiver{}, &NetstringReceiver{}, &LengthPrefixed{}, &AMP{}} {
		if p.Transport() != nil {
			t.Fatalf("%T: transport before ConnectionMade", p)
		}
		p.ConnectionMade(ft)
		if p.Transport() != ft {
			t.Fatalf("%T: Transport is not the one passed to ConnectionMade", p)
		}
	}
	re := &RemoteError{Code: "C", Description: "d"}
	if re.Error() != "proto: AMP remote error C: d" {
		t.Fatalf("RemoteError.Error() = %q", re.Error())
	}
}

// TestAMPCallNoAnswer covers CallNoAnswer on the loop (a fake transport
// has no loop, so every call is inline).
func TestAMPCallNoAnswer(t *testing.T) {
	a := &AMP{}
	if err := a.CallNoAnswer("Ping", nil); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("before connect: %v", err)
	}
	ft := newFake()
	a.ConnectionMade(ft)
	args := Args{"x": "1"}
	if err := a.CallNoAnswer("Ping", args); err != nil {
		t.Fatal(err)
	}
	args["x"] = "2" // already encoded
	boxes := decodeBoxes(t, ft.out)
	if fmt.Sprint(boxes) != fmt.Sprint([]map[string]string{{"_command": "Ping", "x": "1"}}) {
		t.Fatalf("boxes %v", boxes)
	}
	if a.Pending() != 0 {
		t.Fatal("CallNoAnswer left a pending call")
	}
	ft.reset()
	a.ConnectionLost(errors.New("reset"))
	if err := a.CallNoAnswer("Ping", nil); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("after loss: %v", err)
	}
	if len(ft.out) != 0 {
		t.Fatal("wrote after loss")
	}
}

// TestAMPOnLoopAndEngineStop calls from the client's own loop, then stops
// the engine with a call pending.
func TestAMPOnLoopAndEngineStop(t *testing.T) {
	e := startEngine(t, 2)
	notes := make(chan string, 4)
	handle := func(cmd string, args Args) *twine.Future[Args] {
		switch cmd {
		case "Note":
			notes <- args["v"]
			return twine.Resolved[Args](nil, nil)
		case "Hang":
			f, _ := twine.NewFuture[Args](nil)
			return f
		}
		return sumHandler(cmd, args)
	}
	lst, err := e.ListenTCP("127.0.0.1:0", ampFactory(handle, nil))
	if err != nil {
		t.Fatal(err)
	}
	made := make(chan *ampPeer, 1)
	tr, err := e.Loops()[1].ConnectTCP(lst.Addr().String(), ampFactory(nil, made)).Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := <-made
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var sum *twine.Future[Args]
	onLoop(t, tr.Loop(), func() {
		if err := client.CallNoAnswer("Note", Args{"v": "from the loop"}); err != nil {
			t.Error(err)
		}
		sum = client.Call("Sum", Args{"a": "1", "b": "2"})
	})
	if v, err := sum.Await(ctx); err != nil || v["total"] != "3" {
		t.Fatalf("Sum from the loop: %v, %v", v, err)
	}
	select {
	case v := <-notes:
		if v != "from the loop" {
			t.Fatalf("note %q", v)
		}
	case <-ctx.Done():
		t.Fatal("note not delivered")
	}

	pending := client.Call("Hang", nil)
	waitFor(t, "Hang to be pending", func() bool {
		n := -1
		onLoop(t, tr.Loop(), func() { n = client.Pending() })
		return n == 1
	})
	e.Stop()
	<-e.Done()
	if _, err := pending.Await(ctx); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("pending call after engine stop: %v", err)
	}
	if _, err := client.Call("Sum", nil).Await(ctx); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("call after engine stop: %v", err)
	}
}

// TestAMPLateAnswerAfterLoss: a handler that settles after the connection
// is gone writes nothing, whether it resolves or rejects, and the partial
// box buffer is released.
func TestAMPLateAnswerAfterLoss(t *testing.T) {
	var rs []twine.Resolver[Args]
	a := &AMP{Handle: func(string, Args) *twine.Future[Args] {
		f, r := twine.NewFuture[Args](nil)
		rs = append(rs, r)
		return f
	}}
	ft := newFake()
	a.ConnectionMade(ft)
	feed(a, twistedBox("_ask", "1", "_command", "A"))
	feed(a, twistedBox("_ask", "2", "_command", "B"))
	feed(a, []byte{0, 5, 'k'}) // a partial key frame
	a.ConnectionLost(nil)
	if a.lp.buf != nil {
		t.Fatal("partial frame buffer kept after ConnectionLost")
	}
	rs[0].Resolve(Args{"late": "1"})
	rs[1].Reject(errors.New("late"))
	if len(ft.out) != 0 {
		t.Fatalf("wrote %q after the connection was lost", ft.out)
	}
}

// ampPeer is an AMP protocol that reports itself once connected.
type ampPeer struct {
	AMP
}

func ampFactory(handle func(string, Args) *twine.Future[Args], made chan<- *ampPeer) twine.Factory {
	return func(net.Addr) twine.Protocol {
		p := &ampPeer{}
		p.Handle = handle
		p.OnConnectionMade = func(twine.Transport) {
			if made != nil {
				made <- p
			}
		}
		return p
	}
}

func TestAMPRoundTrip(t *testing.T) {
	e := startEngine(t, 2)
	server := e.Loops()[0]
	var hangMu sync.Mutex
	var hung []twine.Resolver[Args]
	handle := func(cmd string, args Args) *twine.Future[Args] {
		switch cmd {
		case "Slow":
			// Answer from the worker pool: the reply is routed back to the loop.
			return twine.DeferToThread(server, func() (Args, error) {
				time.Sleep(5 * time.Millisecond)
				return Args{"echo": args["v"]}, nil
			})
		case "Hang":
			f, r := twine.NewFuture[Args](server)
			hangMu.Lock()
			hung = append(hung, r)
			hangMu.Unlock()
			return f
		}
		return sumHandler(cmd, args)
	}
	lst, err := e.ListenTCP("127.0.0.1:0", ampFactory(handle, nil))
	if err != nil {
		t.Fatal(err)
	}
	made := make(chan *ampPeer, 1)
	tr, err := e.Loops()[1].ConnectTCP(lst.Addr().String(), ampFactory(nil, made)).Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := <-made
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Many concurrent calls from several goroutines.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				v, err := client.Call("Sum", Args{"a": strconv.Itoa(g), "b": strconv.Itoa(i)}).Await(ctx)
				if err != nil || v["total"] != strconv.Itoa(g+i) {
					t.Errorf("Sum(%d,%d) = %v, %v", g, i, v, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if v, err := client.Call("Slow", Args{"v": "\x00binary\xff"}).Await(ctx); err != nil || v["echo"] != "\x00binary\xff" {
		t.Fatalf("Slow = %v, %v", v, err)
	}
	if _, err := client.Call("Nope", nil).Await(ctx); !errors.Is(err, ErrUnhandledCommand) {
		t.Fatalf("unknown command: %v", err)
	}
	_, err = client.Call("Fail", nil).Await(ctx)
	var re *RemoteError
	if !errors.As(err, &re) || re.Description != "boom" || re.Code != UnknownErrorCode {
		t.Fatalf("remote error: %v", err)
	}

	// A call pending when the connection drops is rejected.
	pending := client.Call("Hang", nil)
	waitFor(t, "Hang to reach the server", func() bool {
		hangMu.Lock()
		defer hangMu.Unlock()
		return len(hung) == 1
	})
	tr.Loop().CallFromThread(tr.Abort)
	if _, err := pending.Await(ctx); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("pending after abort: %v", err)
	}
	// Answering after the connection is gone is harmless.
	hung[0].Resolve(Args{"late": "1"})
	if _, err := client.Call("Sum", nil).Await(ctx); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("call after loss: %v", err)
	}
}

// TestAMPCallArgsNotRetained mutates the Args map right after each Call and
// CallNoAnswer from a goroutine that is not the loop. The request must have
// been encoded before Call returned (README ownership rule 5), so -race
// stays quiet and every answer reflects the value at call time.
func TestAMPCallArgsNotRetained(t *testing.T) {
	e := startEngine(t, 2)
	lst, err := e.ListenTCP("127.0.0.1:0", ampFactory(sumHandler, nil))
	if err != nil {
		t.Fatal(err)
	}
	made := make(chan *ampPeer, 1)
	if _, err := e.Loops()[1].ConnectTCP(lst.Addr().String(), ampFactory(nil, made)).Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := <-made
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := Args{"a": "0", "b": "1"}
	futs := make([]*twine.Future[Args], 0, 200)
	for i := range 200 {
		args["a"] = strconv.Itoa(i)
		futs = append(futs, client.Call("Sum", args))
		if err := client.CallNoAnswer("Sum", args); err != nil {
			t.Fatal(err)
		}
		args["a"] = "garbage"
		args["extra"] = strconv.Itoa(i)
		delete(args, "extra")
	}
	for i, f := range futs {
		v, err := f.Await(ctx)
		if err != nil || v["total"] != strconv.Itoa(i+1) {
			t.Fatalf("call %d: %v, %v", i, v, err)
		}
	}
}
