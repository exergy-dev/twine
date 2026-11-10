package proto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

// lineEcho replies to each line with "echo: <line>".
type lineEcho struct {
	LineReceiver
	lost chan<- error
}

func newLineEcho(lost chan<- error) twine.Factory {
	return func(net.Addr) twine.Protocol {
		p := &lineEcho{lost: lost}
		p.MaxLineLength = 64
		p.LineReceived = p.line
		p.OnConnectionLost = func(err error) {
			if p.lost != nil {
				p.lost <- err
			}
		}
		return p
	}
}

var echoPrefix = []byte("echo: ")

func (p *lineEcho) line(b []byte) {
	t := p.Transport()
	// Two writes: the reply is assembled by the kernel/ring, not by us.
	t.Write(echoPrefix)
	p.SendLine(b)
}

func TestLineReceiverEchoIntegration(t *testing.T) {
	e := startEngine(t, max(2, runtime.GOMAXPROCS(0)))
	lost := make(chan error, 64)
	lst, err := e.ListenTCP("127.0.0.1:0", newLineEcho(lost))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", lst.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			r := bufio.NewReader(c)
			var all bytes.Buffer
			for j := 0; j < 50; j++ {
				fmt.Fprintf(&all, "client %d line %d\r\n", i, j)
			}
			data := all.Bytes()
			// Send in awkward pieces so lines and delimiters straddle reads.
			go func() {
				for k := 0; k < len(data); {
					n := min(1+(k*7)%13, len(data)-k)
					if _, err := c.Write(data[k : k+n]); err != nil {
						return
					}
					k += n
					if k%5 == 0 {
						time.Sleep(time.Millisecond)
					}
				}
			}()
			for j := 0; j < 50; j++ {
				got, err := r.ReadString('\n')
				if err != nil {
					t.Error(err)
					return
				}
				if want := fmt.Sprintf("echo: client %d line %d\r\n", i, j); got != want {
					t.Errorf("got %q want %q", got, want)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	// Drain the clean-close notifications.
	for i := 0; i < 8; i++ {
		select {
		case err := <-lost:
			if err != nil {
				t.Errorf("clean close reported %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ConnectionLost not delivered")
		}
	}

	// An over-long line aborts the connection with ErrLineTooLong.
	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write(bytes.Repeat([]byte("x"), 200)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-lost:
		if !errors.Is(err, twine.ErrLineTooLong) {
			t.Fatalf("ConnectionLost(%v), want ErrLineTooLong", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("over-long line did not abort")
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection still open after abort")
	}
}

func TestNetstringIntegration(t *testing.T) {
	e := startEngine(t, 2)
	lst, err := e.ListenTCP("127.0.0.1:0", func(net.Addr) twine.Protocol {
		p := &NetstringReceiver{}
		p.StringReceived = func(s []byte) { p.SendString(bytes.ToUpper(s)) }
		return p
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var out, want bytes.Buffer
	for i := 0; i < 100; i++ {
		s := "payload " + strconv.Itoa(i) + string(bytes.Repeat([]byte("z"), i*37))
		fmt.Fprintf(&out, "%d:%s,", len(s), s)
		u := bytes.ToUpper([]byte(s))
		fmt.Fprintf(&want, "%d:%s,", len(u), u)
	}
	go func() {
		b := out.Bytes()
		for len(b) > 0 {
			n := min(len(b), 333)
			c.Write(b[:n])
			b = b[n:]
		}
	}()
	got := make([]byte, want.Len())
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatal("netstring echo mismatch")
	}
}

func TestLengthPrefixedIntegration(t *testing.T) {
	e := startEngine(t, 2)
	lst, err := e.ListenTCP("127.0.0.1:0", func(net.Addr) twine.Protocol {
		p := &LengthPrefixed{}
		p.FrameReceived = func(f []byte) {
			if err := p.SendFrame(f); err != nil {
				panic(err)
			}
		}
		return p
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var out bytes.Buffer
	for i := 0; i < 50; i++ {
		body := bytes.Repeat([]byte{byte(i)}, i*1013) // up to ~50 KiB, spans reads
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
		out.Write(hdr[:])
		out.Write(body)
	}
	want := append([]byte(nil), out.Bytes()...)
	go c.Write(out.Bytes())
	got := make([]byte, len(want))
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("length-prefixed echo mismatch")
	}
}
