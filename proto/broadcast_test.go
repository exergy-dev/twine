package proto

import (
	"bufio"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// member joins the group on connect and leaves it in ConnectionLost, as
// documented (except for one deliberately forgetful member in the test).
type member struct {
	LineReceiver
	b *Broadcast
}

func TestBroadcast(t *testing.T) {
	loops := max(2, runtime.GOMAXPROCS(0))
	e := startEngine(t, loops)
	b := &Broadcast{}
	var forgetMu sync.Mutex
	forgetPeer := "" // this member closes without calling Remove
	lst, err := e.ListenTCP("127.0.0.1:0", func(peer net.Addr) twine.Protocol {
		m := &member{b: b}
		m.OnConnectionMade = func(t twine.Transport) { b.Add(t); b.Add(t) } // double Add is a no-op
		m.OnConnectionLost = func(error) {
			forgetMu.Lock()
			forget := peer.String() == forgetPeer
			forgetMu.Unlock()
			if !forget {
				b.Remove(m.Transport())
			}
		}
		// A line from a client is broadcast from that client's loop.
		m.LineReceived = func(line []byte) { b.Send(append(append([]byte{}, line...), '\n')) }
		return m
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 24
	conns := make([]net.Conn, n)
	readers := make([]*bufio.Reader, n)
	for i := range conns {
		c, err := net.Dial("tcp", lst.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		conns[i], readers[i] = c, bufio.NewReader(c)
	}
	forgetMu.Lock()
	forgetPeer = conns[n-1].LocalAddr().String()
	forgetMu.Unlock()
	waitFor(t, "all members", func() bool { return b.Len() == n })

	// Member 0 leaves cleanly (Removed); member n-1 closes without Remove.
	conns[0].Close()
	waitFor(t, "removal", func() bool { return b.Len() == n-1 })
	conns[n-1].Close()
	time.Sleep(50 * time.Millisecond) // let the close reach the server
	live := conns[1 : n-1]
	liveR := readers[1 : n-1]

	// Off-loop sends, in order, from one goroutine; the buffer is reused.
	msg := make([]byte, 0, 32)
	for k := 0; k < 100; k++ {
		msg = fmt.Appendf(msg[:0], "msg %d\n", k)
		b.Send(msg)
		msg[0] = 'X' // the caller may reuse its buffer immediately
	}
	for i, r := range liveR {
		live[i].SetReadDeadline(time.Now().Add(10 * time.Second))
		for k := 0; k < 100; k++ {
			got, err := r.ReadString('\n')
			if err != nil {
				t.Fatalf("member %d: %v", i+1, err)
			}
			if want := fmt.Sprintf("msg %d\n", k); got != want {
				t.Fatalf("member %d: got %q want %q", i+1, got, want)
			}
		}
	}

	// An on-loop send (from a protocol callback) reaches everyone too.
	if _, err := live[0].Write([]byte("from a client\r\n")); err != nil {
		t.Fatal(err)
	}
	for i, r := range liveR {
		got, err := r.ReadString('\n')
		if err != nil || got != "from a client\n" {
			t.Fatalf("member %d: %q %v", i+1, got, err)
		}
	}

	// Removing everything empties the group; Send is then a no-op.
	for _, c := range live {
		c.Close()
	}
	waitFor(t, "empty group", func() bool { return b.Len() == 1 }) // only the forgotten member
	b.Send([]byte("nobody\n"))
}

func TestBroadcastMembership(t *testing.T) {
	b := &Broadcast{}
	f1, f2 := newFake(), newFake()
	b.Add(f1)
	b.Add(f2)
	b.Remove(f1)
	b.Remove(f1)
	if b.Len() != 1 {
		t.Fatalf("Len %d", b.Len())
	}
	b.Remove(newFake())
	b.Remove(f2)
	if b.Len() != 0 {
		t.Fatalf("Len %d", b.Len())
	}
}

func TestBroadcastZeroValue(t *testing.T) {
	var b Broadcast
	b.Send([]byte("nobody\n")) // empty: a no-op
	b.Remove(newFake())
	f := newFake()
	b.Add(f)
	if b.Len() != 1 {
		t.Fatalf("Len %d", b.Len())
	}
	b.Remove(f)
	if b.Len() != 0 {
		t.Fatalf("Len %d", b.Len())
	}
}
