package twine

import (
	"context"
	"net"
	"testing"
	"time"
)

// Listener.Close racing with Run must neither race on the listener's socket
// (run with -race) nor leak it: once the engine has stopped, the port is
// free again.
func TestListenerCloseConcurrentWithRun(t *testing.T) {
	for i := 0; i < 20; i++ {
		e := New(Loops(2), ReusePort(i%2 == 0), Watchdog(0))
		lst, err := e.ListenTCP("127.0.0.1:0", echoFactory())
		if err != nil {
			t.Fatal(err)
		}
		u := newUDPEcho()
		ulst, err := e.ListenUDP("127.0.0.1:0", u)
		if err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() { errCh <- e.Run(context.Background()) }()
		if i%3 == 0 {
			time.Sleep(time.Duration(i) * 50 * time.Microsecond)
		}
		_ = lst.Close()
		_ = ulst.Close()
		e.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
		pl, err := net.Listen("tcp", lst.Addr().String())
		if err != nil {
			t.Fatalf("round %d: port still bound after Close and Stop: %v", i, err)
		}
		pl.Close()
		pu, err := net.ListenPacket("udp", ulst.Addr().String())
		if err != nil {
			t.Fatalf("round %d: UDP port still bound after Close and Stop: %v", i, err)
		}
		pu.Close()
	}
}

// Close before Run, then Stop without ever running: the socket is released.
func TestListenerCloseBeforeRun(t *testing.T) {
	e := New(Loops(2), Watchdog(0))
	lst, err := e.ListenTCP("127.0.0.1:0", echoFactory())
	if err != nil {
		t.Fatal(err)
	}
	_ = lst.Close()
	pl, err := net.Listen("tcp", lst.Addr().String())
	if err != nil {
		t.Fatalf("port still bound after Close before Run: %v", err)
	}
	pl.Close()
	e.Stop()
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("Run after Stop: %v", err)
	}
}
