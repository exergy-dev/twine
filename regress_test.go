package twine

import (
	"net"
	"testing"
	"time"
)

// idleProducer is a pull producer that never writes anything.
type idleProducer struct{ resumes int }

func (p *idleProducer) PauseProducing()  {}
func (p *idleProducer) ResumeProducing() { p.resumes++ }
func (p *idleProducer) StopProducing()   {}

// TestPullProducerThatWritesNothingDoesNotSpin guards against the flush step
// re-marking a conn dirty forever when its pull producer has nothing to give,
// which would freeze every connection on the loop.
func TestPullProducerThatWritesNothingDoesNotSpin(t *testing.T) {
	e := startEngine(t, Loops(1))
	l := e.loops[0]
	var tr Transport
	made := make(chan struct{}, 1)
	f := FactoryFunc(func(net.Addr) Protocol {
		return &hookProto{made: func(_ *hookProto, tp Transport) { tr = tp; made <- struct{}{} }}
	})
	lst, err := e.ListenTCP("127.0.0.1:0", f)
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	<-made
	p := &idleProducer{}
	onLoop(t, l, func() {
		tr.RegisterProducer(p, false)
		tr.Write([]byte("x")) // direct hit; marks dirty so flush pokes the producer once
	})
	// Another connection on the same loop must keep echoing promptly.
	lst2, err := e.ListenTCP("127.0.0.1:0", echoFactory())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := net.Dial("tcp", lst2.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := 0; i < 5; i++ {
		start := time.Now()
		if _, err := c2.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		c2.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := c2.Read(buf); err != nil {
			t.Fatalf("echo stalled: %v", err)
		}
		if d := time.Since(start); d > 200*time.Millisecond {
			t.Errorf("echo round trip %v with an idle pull producer registered", d)
		}
	}
	var resumes int
	onLoop(t, l, func() { resumes = p.resumes })
	if resumes == 0 || resumes > 3 {
		t.Errorf("ResumeProducing called %d times for one drain, want 1 (±1)", resumes)
	}
}

// TestIdleLoopBlocks asserts that a loop with one idle connection actually
// blocks in poll instead of spinning: the block decision has several inputs
// (task queue, due list, dirty/ready lists, wake flag) and a regression in
// any of them shows up as a busy loop.
func TestIdleLoopBlocks(t *testing.T) {
	e := startEngine(t, Loops(1), SpinIterations(0))
	l := e.loops[0]
	lst, err := e.ListenTCP("127.0.0.1:0", echoFactory())
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", lst.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	onLoop(t, l, func() {}) // settle
	before := l.Metrics().Iterations
	time.Sleep(300 * time.Millisecond)
	delta := l.Metrics().Iterations - before
	if delta > 50 {
		t.Errorf("idle loop ran %d iterations in 300ms; it should be blocked in poll", delta)
	}
}
