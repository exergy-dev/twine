//go:build linux

package poll

import (
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

// benchWaitWake runs the Wait/Wake benchmarks against Pollers made by mk,
// as sub-benchmarks prefixed with name:
//
//   - wait0: Wait(0) with nothing ready (the loop's spin phase).
//   - wake+wait: Wake then Wait(-1) on one goroutine (eventfd write, one
//     wait syscall, eventfd drain).
//   - pingpong (only when unlocked is set): two goroutines on two Pollers
//     alternately Wake each other and block in Wait(-1); ns/op is one full
//     round trip (two wakeups).
//   - pingpong-locked: the same with each Poller created and waited on by a
//     goroutine locked to its OS thread, as Loop.run does.
//   - mod+wait: Mod(Readable) then Wait(-1) on a socket with data pending
//     (registration change plus readiness report).
func benchWaitWake(b *testing.B, name string, mk func(*testing.B) *Poller, unlocked bool) {
	b.Run(name+"/wait0", func(b *testing.B) {
		p := mk(b)
		evs := make([]Event, 64)
		b.ReportAllocs()
		for b.Loop() {
			p.Wait(evs, 0)
		}
	})
	b.Run(name+"/wake+wait", func(b *testing.B) {
		p := mk(b)
		evs := make([]Event, 64)
		b.ReportAllocs()
		for b.Loop() {
			p.Wake()
			p.Wait(evs, -1)
		}
	})
	// pingpong: p1 and p2 are unlocked goroutines' Pollers, as in the tests.
	// pingpong-locked mirrors the real loop: each Poller is created and
	// waited on by a goroutine locked to its OS thread (Loop.run calls
	// LockOSThread before poll.New). Both sides must run exactly b.N
	// rounds, so these keep the b.N loop.
	modes := []bool{true}
	if unlocked {
		modes = []bool{false, true}
	}
	for _, locked := range modes {
		sub := name + "/pingpong"
		if locked {
			sub += "-locked"
		}
		b.Run(sub, func(b *testing.B) {
			var p1, p2 *Poller
			if !locked {
				p1, p2 = mk(b), mk(b)
			}
			created := make(chan *Poller)
			start := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				if locked {
					runtime.LockOSThread()
					defer runtime.UnlockOSThread()
					created <- mk(b)
				}
				<-start
				evs := make([]Event, 8)
				for i := 0; i < b.N; i++ {
					p1.Wait(evs, -1)
					p2.Wake()
				}
			}()
			if locked {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
				p1 = <-created
				p2 = mk(b)
			}
			close(start)
			evs := make([]Event, 8)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p1.Wake()
				p2.Wait(evs, -1)
			}
			<-done
		})
	}
	b.Run(name+"/mod+wait", func(b *testing.B) {
		p := mk(b)
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK, 0)
		if err != nil {
			b.Fatal(err)
		}
		defer unix.Close(fds[0])
		defer unix.Close(fds[1])
		unix.Write(fds[1], []byte("x"))
		if err := p.Add(fds[0], Readable); err != nil {
			b.Fatal(err)
		}
		evs := make([]Event, 8)
		b.ReportAllocs()
		for b.Loop() {
			p.Mod(fds[0], Readable)
			if n, _ := p.Wait(evs, -1); n != 1 {
				b.Fatalf("Wait = %d", n)
			}
		}
	})
}
