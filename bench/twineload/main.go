//go:build linux

// Command twineload is an event-loop load generator built on twine itself,
// cheaper per message than a goroutine-per-connection client. It opens -conns
// connections spread over -loops loops (optionally pinned), keeps exactly one
// request in flight per connection, and counts completed round trips.
//
//	-proto echo: -size byte messages echoed back
//	-proto http: "GET / HTTP/1.1" keep-alive requests, responses delimited by Content-Length
//
// After connecting and a -warmup period it measures for -duration and prints
// a human summary plus one machine-readable RESULT line. With -server-pid it
// also reports the server process's CPU use over the same window.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/exergy-dev/twine"
	"github.com/exergy-dev/twine/bench"
)

// loopState is touched only by its own loop, except count (read by main).
type loopState struct {
	count atomic.Uint64
	hist  bench.Histogram
	n     uint32 // messages seen on this loop, for latency sampling
	// open-loop (-rate) mode
	idle     []*httpConn   // connections with no request in flight
	nextAt   time.Time     // scheduled time of the next request
	interval time.Duration // 1 / per-loop rate
	overdue  atomic.Uint64 // pacer ticks that found every connection busy
	_        [64]byte
}

// openLoop is true when -rate > 0: requests are issued on a fixed schedule
// regardless of responses (an open-loop load), so latency includes queueing
// and the measurement is "p99 at X% load" rather than closed-loop saturation.
var openLoop bool

var (
	recording atomic.Bool
	connected atomic.Int64
	failed    atomic.Int64
	protoErrs atomic.Int64
)

// sampleMask: record the latency of every 16th round trip on a loop so that
// the clock reads do not dominate the client's cost.
const sampleMask = 15

type echoConn struct {
	ls   *loopState
	t    twine.Transport
	msg  []byte
	got  int
	sent time.Time
	samp bool
}

func (c *echoConn) send() {
	c.ls.n++
	c.samp = c.ls.n&sampleMask == 0 && recording.Load()
	if c.samp {
		c.sent = time.Now()
	}
	c.t.Write(c.msg)
}

func (c *echoConn) ConnectionMade(t twine.Transport) {
	c.t = t
	connected.Add(1)
	c.send()
}

func (c *echoConn) DataReceived(b []byte) {
	c.got += len(b)
	for c.got >= len(c.msg) {
		c.got -= len(c.msg)
		c.ls.count.Add(1)
		if c.samp {
			c.ls.hist.Record(time.Since(c.sent))
		}
		c.send()
	}
}

func (c *echoConn) ConnectionLost(err error) {
	if err != nil {
		protoErrs.Add(1)
	}
}

type httpConn struct {
	ls    *loopState
	t     twine.Transport
	req   []byte // one request, or depth requests back to back when pipelining
	depth int    // pipeline depth (1 = one request in flight)
	left  int    // responses still expected for the current batch
	buf   []byte // partial response carried between reads
	sent  time.Time
	samp  bool
}

// send writes one batch of depth requests; the next batch goes out once all
// depth responses have arrived (TechEmpower "plaintext" style pipelining).
func (c *httpConn) send() {
	c.ls.n++
	c.samp = openLoop || (c.ls.n&sampleMask == 0 && recording.Load())
	if c.samp {
		c.sent = time.Now()
	}
	c.left = c.depth
	c.t.Write(c.req)
}

func (c *httpConn) ConnectionMade(t twine.Transport) {
	c.t = t
	connected.Add(1)
	if openLoop {
		c.ls.idle = append(c.ls.idle, c)
		return
	}
	c.send()
}

// pace runs once per loop iteration (a zero-delay timer re-armed from itself;
// the client loops spin, so iterations are microseconds apart) and issues
// every request whose scheduled time has passed to an idle connection. Latency is measured from the scheduled send time, as
// wrk2 does, so client-side queueing when all connections are busy counts
// against the server (coordinated omission is not hidden). When every
// connection is busy the tick is counted as overdue and the backlog drains as
// responses arrive.
func pace(l *twine.Loop, ls *loopState) {
	l.CallLater(0, func() { pace(l, ls) })
	now := time.Now()
	for !ls.nextAt.After(now) {
		if len(ls.idle) == 0 {
			ls.overdue.Add(1)
			return
		}
		c := ls.idle[len(ls.idle)-1]
		ls.idle = ls.idle[:len(ls.idle)-1]
		c.sendAt(ls.nextAt)
		ls.nextAt = ls.nextAt.Add(ls.interval)
	}
}

// sendAt is send with the latency clock started at the scheduled time.
func (c *httpConn) sendAt(scheduled time.Time) {
	c.ls.n++
	c.samp = true
	c.sent = scheduled
	c.left = c.depth
	c.t.Write(c.req)
}

// respLen returns the total length of the response at the start of b, or -1
// if b does not yet hold the whole response.
func respLen(b []byte) int {
	end := bytes.Index(b, []byte("\r\n\r\n"))
	if end < 0 {
		return -1
	}
	hdr := b[:end+2] // keep the final CRLF so every header line ends in CRLF
	cl := 0
	for len(hdr) > 0 {
		i := bytes.Index(hdr, []byte("\r\n"))
		if i < 0 { // unreachable: hdr ends in CRLF
			break
		}
		line := hdr[:i]
		hdr = hdr[i+2:]
		if len(line) > 15 && (line[0]|0x20) == 'c' && bytes.EqualFold(line[:15], []byte("content-length:")) {
			v, err := strconv.Atoi(string(bytes.TrimSpace(line[15:])))
			if err != nil {
				return -2
			}
			cl = v
		}
	}
	total := end + 4 + cl
	if len(b) < total {
		return -1
	}
	return total
}

func (c *httpConn) DataReceived(b []byte) {
	if len(c.buf) > 0 {
		c.buf = append(c.buf, b...)
		b = c.buf
	}
	for len(b) > 0 {
		n := respLen(b)
		if n == -2 {
			protoErrs.Add(1)
			c.t.Abort()
			return
		}
		if n < 0 {
			c.buf = append(c.buf[:0], b...) // overlap-safe: b may alias c.buf
			return
		}
		b = b[n:]
		c.ls.count.Add(1)
		c.left--
		if c.left > 0 {
			continue
		}
		if c.samp && (recording.Load() || !openLoop) {
			c.ls.hist.Record(time.Since(c.sent)) // latency of the whole batch
		}
		if openLoop {
			if now := time.Now(); !c.ls.nextAt.After(now) {
				// Backlog: a scheduled request was waiting for a connection.
				c.sendAt(c.ls.nextAt)
				c.ls.nextAt = c.ls.nextAt.Add(c.ls.interval)
			} else {
				c.ls.idle = append(c.ls.idle, c)
			}
			continue
		}
		c.send()
	}
	c.buf = c.buf[:0]
}

func (c *httpConn) ConnectionLost(err error) {
	if err != nil {
		protoErrs.Add(1)
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	proto := flag.String("proto", "echo", "echo or http")
	conns := flag.Int("conns", 1000, "connections")
	size := flag.Int("size", 64, "echo message size")
	loops := flag.Int("loops", 8, "client event loops")
	pin := flag.String("pin", "", "CPU list for PinLoops (loop i -> cpus[i%len]); empty = no pinning")
	spin := flag.Int("spin", -1, "client twine.SpinIterations (zero-timeout polls before blocking; -1 = default)")
	duration := flag.Duration("duration", 10*time.Second, "measured window")
	warmup := flag.Duration("warmup", 2*time.Second, "warm-up after all connections are up")
	serverPID := flag.Int("server-pid", 0, "server process id: report its CPU use over the measured window")
	pipeline := flag.Int("pipeline", 1, "http: requests sent back to back per connection before waiting for responses (16 = TechEmpower plaintext)")
	offered := flag.Float64("rate", 0, "http: open-loop offered load in requests/s spread over the loops (0 = closed loop, one batch in flight per connection)")
	flag.Parse()
	openLoop = *offered > 0 && *proto == "http"
	if *pipeline < 1 {
		*pipeline = 1
	}

	opts := []twine.Option{twine.Loops(*loops), twine.Watchdog(0), twine.ShutdownTimeout(200 * time.Millisecond)}
	if *spin >= 0 {
		opts = append(opts, twine.SpinIterations(*spin))
	}
	if *pin != "" {
		cpus, err := bench.ParseCPUs(*pin)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, twine.PinLoops(cpus...))
	}
	eng := twine.New(opts...)
	runErr := make(chan error, 1)
	go func() { runErr <- eng.Run(context.Background()) }()
	lps := eng.Loops()
	states := make([]*loopState, len(lps))
	for i := range states {
		states[i] = new(loopState)
	}
	for _, l := range lps { // barrier: loops are running
		done := make(chan struct{})
		l.CallFromThread(func() { close(done) })
		<-done
	}

	msg := bytes.Repeat([]byte{'x'}, *size)
	req := bytes.Repeat([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"), *pipeline)
	connStart := time.Now()
	// Connect in batches so the server's accept backlog is never overrun.
	const batch = 500
	for i := 0; i < *conns; i++ {
		li := i % len(lps)
		var f twine.Factory
		switch *proto {
		case "echo":
			f = twine.FactoryFunc(func(net.Addr) twine.Protocol { return &echoConn{ls: states[li], msg: msg} })
		case "http":
			f = twine.FactoryFunc(func(net.Addr) twine.Protocol { return &httpConn{ls: states[li], req: req, depth: *pipeline} })
		default:
			log.Fatalf("unknown -proto %q", *proto)
		}
		lps[li].ConnectTCP(*addr, f, twine.DialTimeout(10*time.Second), twine.DialNoDelay(true)).
			Catch(func(err error) {
				if failed.Add(1) == 1 {
					fmt.Fprintln(os.Stderr, "connect error:", err)
				}
			})
		if (i+1)%batch == 0 {
			for int(connected.Load()+failed.Load()) < i+1-batch/2 {
				time.Sleep(time.Millisecond)
			}
		}
	}
	for int(connected.Load()+failed.Load()) < *conns {
		time.Sleep(5 * time.Millisecond)
		if time.Since(connStart) > 60*time.Second {
			log.Fatalf("only %d/%d connected", connected.Load(), *conns)
		}
	}
	if failed.Load() > 0 {
		log.Fatalf("%d connections failed", failed.Load())
	}
	if openLoop {
		perLoop := *offered / float64(len(lps)) / float64(*pipeline)
		for i, l := range lps {
			ls := states[i]
			ls.interval = time.Duration(float64(time.Second) / perLoop)
			l.CallFromThread(func() { ls.nextAt = time.Now(); pace(l, ls) })
		}
	}
	time.Sleep(*warmup)

	total := func() uint64 {
		var n uint64
		for _, s := range states {
			n += s.count.Load()
		}
		return n
	}
	recording.Store(true)
	time.Sleep(50 * time.Millisecond) // let recording take effect
	srv0, _ := bench.ProcCPU(*serverPID)
	cli0, _ := bench.ProcCPU(os.Getpid())
	t0, n0 := time.Now(), total()
	time.Sleep(*duration)
	n1, t1 := total(), time.Now()
	srv1, _ := bench.ProcCPU(*serverPID)
	cli1, _ := bench.ProcCPU(os.Getpid())
	recording.Store(false)

	eng.Stop()
	<-eng.Done()
	h := new(bench.Histogram)
	for _, s := range states {
		h.Merge(&s.hist)
	}
	el := t1.Sub(t0).Seconds()
	rate := float64(n1-n0) / el
	srvCores := (srv1 - srv0).Seconds() / el
	cliCores := (cli1 - cli0).Seconds() / el
	fmt.Printf("proto=%s conns=%d client-loops=%d window=%.2fs errors=%d\n", *proto, *conns, len(lps), el, protoErrs.Load())
	fmt.Printf("msgs=%d rate=%.0f/s p50=%v p99=%v (sampled 1/%d)\n", n1-n0, rate, h.Percentile(0.5), h.Percentile(0.99), sampleMask+1)
	if openLoop {
		var over uint64
		for _, s := range states {
			over += s.overdue.Load()
		}
		fmt.Printf("offered=%.0f/s achieved=%.0f/s overdue_ticks=%d p999_us=%.0f\n", *offered, rate, over, float64(h.Percentile(0.999))/1e3)
	}
	fmt.Printf("RESULT rate=%.0f p50_us=%.0f p99_us=%.0f server_cores=%.2f client_cores=%.2f\n",
		rate, float64(h.Percentile(0.5))/1e3, float64(h.Percentile(0.99))/1e3, srvCores, cliCores)
}
