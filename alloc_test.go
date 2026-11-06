package twine

import (
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// socketpairConn attaches one end of a socketpair to l as a TCP conn with the
// given protocol and returns the conn and the peer fd. Must run on the loop.
func socketpairConn(t testing.TB, l *Loop, p Protocol) (*conn, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		panic(err) // runs on the loop; Fatal would kill the loop goroutine
	}
	for _, fd := range fds {
		if err := unix.SetNonblock(fd, true); err != nil {
			panic(err)
		}
	}
	c := l.attachTCP(fds[0], &net.TCPAddr{}, &net.TCPAddr{}, FactoryFunc(func(net.Addr) Protocol { return p }), false)
	return c, fds[1]
}

type countProto struct {
	t     Transport
	bytes int
	calls int
}

func (p *countProto) ConnectionMade(t Transport) { p.t = t }
func (p *countProto) DataReceived(b []byte)      { p.bytes += len(b); p.calls++ }
func (p *countProto) ConnectionLost(error)       {}

func TestReadPathZeroAlloc(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	onLoop(t, l, func() {
		p := &countProto{}
		c, peer := socketpairConn(t, l, p)
		defer unix.Close(peer)
		c.h.SetIdleTimeout(30e9)
		msg := make([]byte, 64)
		// Warm up once.
		unix.Write(peer, msg)
		l.handleRead(c)
		allocs := testing.AllocsPerRun(1000, func() {
			unix.Write(peer, msg)
			l.handleRead(c)
		})
		if allocs != 0 {
			t.Errorf("read path allocates: %v", allocs)
		}
		if p.calls < 1000 {
			t.Errorf("DataReceived called %d times", p.calls)
		}
	})
}

func TestWriteHitPathZeroAlloc(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	onLoop(t, l, func() {
		c, peer := socketpairConn(t, l, &countProto{})
		defer unix.Close(peer)
		msg := make([]byte, 64)
		drain := make([]byte, 64<<10)
		c.h.Write(msg)
		allocs := testing.AllocsPerRun(1000, func() {
			c.h.Write(msg)
			unix.Read(peer, drain)
		})
		if allocs != 0 {
			t.Errorf("write hit path allocates: %v", allocs)
		}
		if c.ring.Len() != 0 {
			t.Errorf("ring should be empty, has %d", c.ring.Len())
			return
		}
		allocs = testing.AllocsPerRun(1000, func() {
			c.h.Writev(msg[:32], msg[32:])
			unix.Read(peer, drain)
		})
		if allocs != 0 {
			t.Errorf("writev hit path allocates: %v", allocs)
		}
	})
}

func TestWriteBufferedPathZeroAlloc(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	onLoop(t, l, func() {
		c, peer := socketpairConn(t, l, &countProto{})
		defer unix.Close(peer)
		big := make([]byte, 1<<20)
		// Fill the socket until the kernel pushes back so the ring is used.
		for c.ring.Len() == 0 {
			c.h.Write(big)
		}
		// Warm slab free lists at every class that 64 KiB of 64-byte writes
		// will touch, then drain the ring.
		c.h.Write(make([]byte, 128<<10))
		c.ring.Reset()
		c.h.Write([]byte{1}) // ring non-empty again, append path from here on
		msg := make([]byte, 64)
		allocs := testing.AllocsPerRun(1000, func() { c.h.Write(msg) })
		if allocs != 0 {
			t.Errorf("write buffered path allocates: %v", allocs)
		}
		// Flush path: let the peer drain and run one flush.
		drain := make([]byte, 1<<20)
		for {
			n, err := unix.Read(peer, drain)
			if n <= 0 || err != nil {
				break
			}
		}
		allocs = testing.AllocsPerRun(10, func() {
			c.flush()
			for {
				n, err := unix.Read(peer, drain)
				if n <= 0 || err != nil {
					break
				}
			}
			c.h.Write(big[:4096])
		})
		if allocs != 0 {
			t.Errorf("flush path allocates: %v", allocs)
		}
	})
}

var sinkCounter int

func bumpSink() { sinkCounter++ }

func TestTimerFireZeroAlloc(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	var fire, cancel, due float64
	onLoop(t, l, func() {
		fn := bumpSink
		l.CallLater(time.Millisecond, fn)
		l.nowNs += 2e6
		l.wheel.Advance(l.nowNs)
		fire = testing.AllocsPerRun(1000, func() {
			l.CallLater(time.Millisecond, fn)
			l.nowNs += 2e6
			l.wheel.Advance(l.nowNs)
		})
		cancel = testing.AllocsPerRun(1000, func() {
			tm := l.CallLater(time.Second, fn)
			tm.Stop()
		})
		// Zero-delay timers go through the due list.
		l.CallLater(0, fn)
		l.fireDue()
		due = testing.AllocsPerRun(1000, func() {
			l.CallLater(0, fn)
			l.fireDue()
		})
	})
	if fire != 0 {
		t.Errorf("timer insert+fire allocates: %v", fire)
	}
	if cancel != 0 {
		t.Errorf("timer insert+cancel allocates: %v", cancel)
	}
	if due != 0 {
		t.Errorf("CallLater(0) insert+fire allocates: %v", due)
	}
}

func TestCallFromThreadZeroAlloc(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	fn := bumpSink
	// Push from off-loop.
	allocs := testing.AllocsPerRun(1000, func() { l.CallFromThread(fn) })
	if allocs != 0 {
		t.Fatalf("CallFromThread push allocates: %v", allocs)
	}
	// Drain on the loop with items pre-pushed.
	onLoop(t, l, func() {
		for i := 0; i < 500; i++ {
			l.tasks.Push(fn)
		}
		allocs := testing.AllocsPerRun(1, func() { l.drainTasks() })
		if allocs != 0 {
			t.Errorf("CallFromThread drain allocates: %v", allocs)
		}
	})
}
