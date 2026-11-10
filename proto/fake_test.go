package proto

import (
	"net"
	"time"

	"github.com/exergy-dev/twine"
)

// fakeTransport is an in-memory twine.Transport for parser unit tests. It
// needs no engine: Loop returns nil. Writes are appended to out; the
// capacity is retained across reset so steady-state writes do not allocate.
type fakeTransport struct {
	out     []byte
	writes  int
	aborted bool
	closing bool
}

var _ twine.Transport = (*fakeTransport)(nil)

func newFake() *fakeTransport { return &fakeTransport{out: make([]byte, 0, 4096)} }

func (f *fakeTransport) Write(b []byte) {
	if f.aborted || f.closing {
		return
	}
	f.writes++
	f.out = append(f.out, b...)
}

func (f *fakeTransport) Writev(bs ...[]byte) {
	if f.aborted || f.closing {
		return
	}
	f.writes++
	for _, b := range bs {
		f.out = append(f.out, b...)
	}
}

func (f *fakeTransport) reset()                                { f.out = f.out[:0]; f.writes = 0 }
func (f *fakeTransport) LoseConnection()                       { f.closing = true }
func (f *fakeTransport) Abort()                                { f.aborted = true }
func (f *fakeTransport) RemoteAddr() net.Addr                  { return &net.TCPAddr{} }
func (f *fakeTransport) LocalAddr() net.Addr                   { return &net.TCPAddr{} }
func (f *fakeTransport) Loop() *twine.Loop                     { return nil }
func (f *fakeTransport) SetIdleTimeout(time.Duration)          {}
func (f *fakeTransport) PauseReading()                         {}
func (f *fakeTransport) ResumeReading()                        {}
func (f *fakeTransport) RegisterProducer(twine.Producer, bool) {}
func (f *fakeTransport) UnregisterProducer()                   {}
func (f *fakeTransport) PendingWrite() int                     { return 0 }

// feed delivers data to p in chunks whose sizes cycle through sizes (a size
// of 0 is treated as 1). Each chunk is copied into a scratch buffer that is
// poisoned after the call, like the debug-mode loop buffer, so a parser that
// retains a slice of its input is caught by the comparisons that follow.
func feed(p twine.Protocol, data []byte, sizes ...int) {
	if len(sizes) == 0 {
		sizes = []int{len(data) + 1}
	}
	scratch := make([]byte, 0, 64)
	for i := 0; len(data) > 0; i++ {
		n := max(sizes[i%len(sizes)], 1)
		n = min(n, len(data))
		scratch = append(scratch[:0], data[:n]...)
		data = data[n:]
		p.DataReceived(scratch)
		for j := range scratch {
			scratch[j] = 0xDE
		}
	}
}

// collector records copies of every frame delivered to it.
type collector struct {
	frames [][]byte
}

func (c *collector) add(b []byte) { c.frames = append(c.frames, append([]byte{}, b...)) }
