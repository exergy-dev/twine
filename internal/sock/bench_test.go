package sock

import (
	"testing"

	"golang.org/x/sys/unix"
)

// BenchmarkAcceptEAGAIN measures Accept on an idle listener: the call that
// ends every accept burst in the loop.
func BenchmarkAcceptEAGAIN(b *testing.B) {
	lfd, _ := listen(b, "tcp4", "127.0.0.1:0")
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := Accept(lfd); err != unix.EAGAIN {
			b.Fatalf("Accept = %v, want EAGAIN", err)
		}
	}
}

// Datagram batches for the mmsg benchmarks: benchMsgs datagrams of
// benchMsgSize bytes per op, and benchRounds ops between timer pauses (the
// receive buffer must hold benchRounds*benchMsgs datagrams).
const (
	benchMsgs    = 8
	benchMsgSize = 64
	benchRounds  = 16
)

// mmsgPair returns a loopback UDP sender and receiver and a send Batch of
// benchMsgs full-size datagrams addressed to the receiver.
func mmsgPair(b *testing.B) (afd, bfd int, sb *Batch) {
	afd, _ = listenUDP(b)
	bfd, baddr := listenUDP(b)
	if err := SetBuffers(bfd, 1<<20, 0); err != nil {
		b.Fatal(err)
	}
	sb = NewBatch(benchMsgs, benchMsgSize)
	for i := range sb.Msgs() {
		m := &sb.Msgs()[i]
		m.Buf = m.Buf[:benchMsgSize]
		m.SetAddr(baddr)
	}
	return afd, bfd, sb
}

// sendRounds sends rounds full batches from sb on afd.
func sendRounds(b *testing.B, sb *Batch, afd, rounds int) {
	for range rounds {
		if n, err := sb.SendMmsg(afd, benchMsgs); n != benchMsgs {
			b.Fatalf("SendMmsg = %d, %v", n, err)
		}
	}
}

// drainUDP reads fd until EAGAIN.
func drainUDP(rb *Batch, fd int) {
	for {
		if _, err := rb.RecvMmsg(fd); err != nil {
			return
		}
	}
}

// BenchmarkSendMmsg measures one SendMmsg of benchMsgs datagrams of
// benchMsgSize bytes over loopback. The receiver is drained with the timer
// stopped.
func BenchmarkSendMmsg(b *testing.B) {
	afd, bfd, sb := mmsgPair(b)
	rb := NewBatch(benchMsgs, benchMsgSize)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i%benchRounds == 0 && i > 0 {
			b.StopTimer()
			drainUDP(rb, bfd)
			b.StartTimer()
		}
		if n, err := sb.SendMmsg(afd, benchMsgs); n != benchMsgs {
			b.Fatalf("SendMmsg = %d, %v", n, err)
		}
	}
}

// BenchmarkRecvMmsg measures one RecvMmsg of benchMsgs queued datagrams of
// benchMsgSize bytes. The datagrams are queued with the timer stopped.
func BenchmarkRecvMmsg(b *testing.B) {
	afd, bfd, sb := mmsgPair(b)
	rb := NewBatch(benchMsgs, benchMsgSize)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if i%benchRounds == 0 {
			b.StopTimer()
			sendRounds(b, sb, afd, benchRounds)
			b.StartTimer()
		}
		if n, err := rb.RecvMmsg(bfd); n != benchMsgs {
			b.Fatalf("RecvMmsg = %d, %v", n, err)
		}
	}
}
