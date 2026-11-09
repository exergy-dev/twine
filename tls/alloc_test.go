package tls_test

import (
	"bytes"
	"crypto/tls"
	"math"
	"net"
	"testing"

	"github.com/exergy-dev/twine"
	twtls "github.com/exergy-dev/twine/tls"
)

// captureConn passes the handshake through to the socket and, once capture
// is set, keeps each Write (one TLS record per Write) instead of sending it.
// Test goroutine only.
type captureConn struct {
	net.Conn
	capture bool
	recs    [][]byte
}

func (c *captureConn) Write(p []byte) (int, error) {
	if c.capture {
		c.recs = append(c.recs, bytes.Clone(p))
		return len(p), nil
	}
	return c.Conn.Write(p)
}

// sinkProto counts plaintext, hands its transport to the test and reports
// ConnectionLost.
type sinkProto struct {
	made chan *twtls.Transport
	lost chan error
	n    int // loop-only
}

func (p *sinkProto) ConnectionMade(t twine.Transport) { p.made <- t.(*twtls.Transport) }
func (p *sinkProto) DataReceived(b []byte)            { p.n += len(b) }
func (p *sinkProto) ConnectionLost(err error) {
	select {
	case p.lost <- err:
	default:
	}
}

// recordRig is a TLS 1.3 server connection on a one-loop engine, plus a
// crypto/tls client that mints fresh ciphertext records for it (each record
// carries its own sequence number, so a record cannot be fed twice).
type recordRig struct {
	loop   *twine.Loop
	server *twtls.Transport
	sink   *sinkProto
	client *tls.Conn
	cap    *captureConn
}

func newRecordRig(tb testing.TB) *recordRig {
	tb.Helper()
	e := startEngine(tb, twine.Loops(1))
	sink := &sinkProto{made: make(chan *twtls.Transport, 1), lost: make(chan error, 1)}
	addr := listen(tb, e, serverConfig(tb), twine.FactoryFunc(func(net.Addr) twine.Protocol { return sink }))
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { raw.Close() })
	cc := &captureConn{Conn: raw}
	client := tls.Client(cc, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
	if err := client.Handshake(); err != nil {
		tb.Fatal(err)
	}
	cc.capture = true
	// Discard everything the server sends (session tickets, Write output)
	// with a fixed buffer, so that its socket never backs up into the ring.
	go func() {
		buf := make([]byte, 64<<10)
		for {
			if _, err := raw.Read(buf); err != nil {
				return
			}
		}
	}()
	return &recordRig{loop: e.Loops()[0], server: <-sink.made, sink: sink, client: client, cap: cc}
}

// mint returns n fresh records, each carrying payload.
func (r *recordRig) mint(tb testing.TB, n int, payload []byte) [][]byte {
	r.cap.recs = r.cap.recs[:0]
	for i := 0; i < n; i++ {
		if _, err := r.client.Write(payload); err != nil {
			tb.Fatal(err)
		}
	}
	if len(r.cap.recs) != n {
		tb.Fatalf("minted %d records for %d writes", len(r.cap.recs), n)
	}
	return r.cap.recs
}

// onLoop runs fn on l and waits for it to return.
func onLoop(l *twine.Loop, fn func()) {
	done := make(chan struct{})
	l.CallFromThread(func() {
		defer close(done)
		fn()
	})
	<-done
}

const recordPayload = 1 << 10

// minAllocsPerRun is testing.AllocsPerRun taken as the minimum of a few
// attempts. AllocsPerRun counts process-wide, so a background goroutine (the
// GC, the watchdog, a runtime worker under -race on a busy CI runner) can add
// allocations to one attempt; the minimum is the code path's own count.
func minAllocsPerRun(runs int, fn func()) float64 {
	best := math.Inf(1)
	for i := 0; i < 3; i++ {
		if a := testing.AllocsPerRun(runs, fn); a < best {
			best = a
		}
	}
	return best
}

// TestRecordWriteAllocs: encrypting and sending a record allocates nothing.
// The measurement runs on the loop goroutine, with nothing else allocating
// (AllocsPerRun counts process-wide).
func TestRecordWriteAllocs(t *testing.T) {
	r := newRecordRig(t)
	payload := bytes.Repeat([]byte("w"), recordPayload)
	var allocs float64
	onLoop(r.loop, func() {
		allocs = minAllocsPerRun(50, func() { r.server.Write(payload) })
	})
	if allocs != 0 {
		t.Fatalf("Transport.Write: %v allocs per record, want 0", allocs)
	}
}

// TestRecordReadAllocs: decrypting a record and delivering its plaintext
// allocates at most maxReadAllocs (crypto/tls itself allocates in Conn.Read
// before Go 1.27).
func TestRecordReadAllocs(t *testing.T) {
	r := newRecordRig(t)
	const runs = 50
	recs := r.mint(t, runs+1, bytes.Repeat([]byte("r"), recordPayload))
	var allocs float64
	var got int
	i := 0
	onLoop(r.loop, func() {
		allocs = minAllocsPerRun(runs, func() {
			r.server.FeedCiphertext(recs[i])
			i++
		})
		got = r.sink.n
	})
	if got != (runs+1)*recordPayload {
		t.Fatalf("delivered %d plaintext bytes, want %d", got, (runs+1)*recordPayload)
	}
	t.Logf("%v allocs per record read", allocs)
	if allocs > maxReadAllocs {
		t.Fatalf("record read: %v allocs per record, want <= %d", allocs, maxReadAllocs)
	}
}

// TestDrainScratchBuffer: a drain decrypts into the loop's shared buffer and
// releases it afterwards; a drain that finds it in use (a drain further up
// the stack, as with TLS nested in TLS) uses a buffer of its own and leaves
// the shared one marked in use.
func TestDrainScratchBuffer(t *testing.T) {
	r := newRecordRig(t)
	recs := r.mint(t, 2, []byte("scratch"))
	var own1, busy1, own2, busy2 bool
	var got int
	onLoop(r.loop, func() {
		r.server.FeedCiphertext(recs[0])
		own1, busy1 = r.server.OwnPlainBuffer(), r.server.ScratchBusy()
		r.server.SetScratchBusy(true)
		r.server.FeedCiphertext(recs[1])
		own2, busy2 = r.server.OwnPlainBuffer(), r.server.ScratchBusy()
		r.server.SetScratchBusy(false)
		got = r.sink.n
	})
	if got != 2*len("scratch") {
		t.Fatalf("delivered %d plaintext bytes, want %d", got, 2*len("scratch"))
	}
	if own1 || busy1 {
		t.Fatalf("free shared buffer: own buffer=%v, still busy=%v; want false, false", own1, busy1)
	}
	if !own2 || !busy2 {
		t.Fatalf("busy shared buffer: own buffer=%v, still busy=%v; want true, true", own2, busy2)
	}
}

func BenchmarkRecordWrite(b *testing.B) {
	r := newRecordRig(b)
	payload := bytes.Repeat([]byte("w"), recordPayload)
	b.SetBytes(recordPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for left := b.N; left > 0; {
		onLoop(r.loop, func() {
			// Return to the loop whenever ciphertext backs up, so that it
			// flushes the ring instead of growing it.
			for left > 0 && r.server.PendingWrite() < 64<<10 {
				r.server.Write(payload)
				left--
			}
		})
	}
}

func BenchmarkRecordRead(b *testing.B) {
	r := newRecordRig(b)
	payload := bytes.Repeat([]byte("r"), recordPayload)
	b.SetBytes(recordPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for left := b.N; left > 0; {
		n := min(left, 1024)
		b.StopTimer()
		recs := r.mint(b, n, payload)
		b.StartTimer()
		onLoop(r.loop, func() {
			for _, rec := range recs {
				r.server.FeedCiphertext(rec)
			}
		})
		left -= n
	}
}
