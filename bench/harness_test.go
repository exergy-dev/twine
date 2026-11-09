//go:build linux

package bench

import (
	"flag"
	"fmt"
	"net"
	"runtime"
	"runtime/debug"
	"syscall"
	"testing"
	"time"

	"github.com/exergy-dev/twine"
)

var (
	idleConns  = flag.Int("idle-conns", 10000, "connections opened by BenchmarkEchoIdle")
	idleSettle = flag.Duration("idle-settle", time.Second, "wait after the last idle connection is accepted before measuring memory")
	echoLoops  = flag.Int("echo-loops", 8, "event loops for the twine and gnet servers in EchoSmall/EchoStream/EchoIdle")
	scaleSpin  = flag.Int("scaling-spin", -1, "twine SpinIterations for EchoScaling (-1 = engine default)")
)

const loopback = "127.0.0.1:0"

// server is one benchmark target.
type server struct {
	name   string
	loops  bool // has an event-loop count (msgs/s/loop is meaningful)
	start  func(loops int) (Server, error)
	inIdle bool // only in the idle-memory benchmark
}

var servers = []server{
	{name: "twine", loops: true, start: func(n int) (Server, error) { return StartTwine(loopback, n) }},
	{name: "goroutine", start: func(int) (Server, error) { return StartGoroutine(loopback) }},
	{name: "gnet", loops: true, start: func(n int) (Server, error) { return StartGnet(loopback, n) }},
	{name: "raw-baseline", inIdle: true, start: func(int) (Server, error) { return StartRaw(loopback) }},
}

// startConns starts a server and dials conns client connections. Cleanups
// close the clients before the server.
func startConns(b *testing.B, start func() (Server, error), conns int) (Server, []net.Conn) {
	b.Helper()
	s, err := start()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(s.Close)
	cs, err := DialAll(s.Addr(), conns)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { CloseAll(cs) })
	return s, cs
}

// pingPong runs the ping-pong load with one op = one round trip on every
// connection and reports msgs/s and latency percentiles.
func pingPong(b *testing.B, cs []net.Conn, size int) PingPongResult {
	b.Helper()
	// Warm up: one round trip per connection (faults in buffers, settles the
	// accept path) outside the timed region.
	if _, err := PingPong(cs, size, 1, time.Time{}, false); err != nil {
		b.Fatal(err)
	}
	cpu0 := processCPU()
	b.ResetTimer()
	res, err := PingPong(cs, size, b.N, time.Time{}, true)
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(cpuUtil(cpu0, res.Elapsed), "cpu-util")
	b.ReportMetric(res.MsgsPerSec(), "msgs/s")
	b.ReportMetric(float64(res.Latency.Percentile(0.50).Nanoseconds())/1e3, "p50-µs")
	b.ReportMetric(float64(res.Latency.Percentile(0.99).Nanoseconds())/1e3, "p99-µs")
	return res
}

// BenchmarkEchoSmall is the echo_small benchmark: 1000 connections, 64 B
// ping-pong (one message in flight per connection), client in-process. One
// op is one round trip on every connection.
func BenchmarkEchoSmall(b *testing.B) {
	for _, srv := range servers {
		if srv.inIdle {
			continue
		}
		b.Run(srv.name, func(b *testing.B) {
			_, cs := startConns(b, func() (Server, error) { return srv.start(*echoLoops) }, 1000)
			res := pingPong(b, cs, 64)
			if srv.loops {
				b.ReportMetric(res.MsgsPerSec()/float64(*echoLoops), "msgs/s/loop")
			}
		})
	}
}

// scalingBase holds the loops=1 throughput of the latest scaling run, per
// server.
var scalingBase = map[string]float64{}

// BenchmarkEchoScaling is the Phase 2 gate: echo_small against twine with
// 1, 2, 4 and 8 loops. "linearity" is msgs/s(n) / (n * msgs/s(1)); the gate
// is >= 0.8 at 8 loops. "loop-busy" is the fraction of loop time not spent
// blocked in poll (spin polls count as busy, so it overstates real work when
// spinning is on). "cpu-util" is process CPU time over wall time times
// NumCPU: the client runs in-process, so a value near 1 means the machine,
// not the server, is saturated and linearity is not a server verdict.
func BenchmarkEchoScaling(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("loops=%d", n), func(b *testing.B) {
			var opts []twine.Option
			if *scaleSpin >= 0 {
				opts = append(opts, twine.SpinIterations(*scaleSpin))
			}
			var ts *TwineServer
			start := func() (Server, error) {
				s, err := StartTwine(loopback, n, opts...)
				ts = s
				return s, err
			}
			block := func() uint64 {
				var t uint64
				for _, m := range ts.Engine.Metrics() {
					t += m.PollBlockNanos
				}
				return t
			}
			scaling(b, "twine", n, start, block)
		})
	}
}

// BenchmarkEchoScalingGnet is the control for BenchmarkEchoScaling: the same
// load against gnet with 1, 2, 4 and 8 loops. If gnet loses linearity at the
// same rate, the loss comes from the host (shared cores with the in-process
// client), not from twine.
func BenchmarkEchoScalingGnet(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("loops=%d", n), func(b *testing.B) {
			scaling(b, "gnet", n, func() (Server, error) { return StartGnet(loopback, n) }, nil)
		})
	}
}

// scaling runs one point of a scaling curve. block, when non-nil, returns the
// server's cumulative poll-block nanoseconds.
func scaling(b *testing.B, name string, n int, start func() (Server, error), block func() uint64) {
	_, cs := startConns(b, start, 1000)
	// Warm up, then sample the clocks around the measured run only.
	if _, err := PingPong(cs, 64, 1, time.Time{}, false); err != nil {
		b.Fatal(err)
	}
	var b0 uint64
	if block != nil {
		b0 = block()
	}
	cpu0 := processCPU()
	b.ResetTimer()
	res, err := PingPong(cs, 64, b.N, time.Time{}, true)
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(cpuUtil(cpu0, res.Elapsed), "cpu-util")
	mps := res.MsgsPerSec()
	b.ReportMetric(mps, "msgs/s")
	b.ReportMetric(mps/float64(n), "msgs/s/loop")
	b.ReportMetric(float64(res.Latency.Percentile(0.99).Nanoseconds())/1e3, "p99-µs")
	if block != nil {
		blocked := float64(block() - b0)
		b.ReportMetric(1-blocked/(float64(n)*float64(res.Elapsed.Nanoseconds())), "loop-busy")
	}
	if n == 1 {
		scalingBase[name] = mps
	} else if base := scalingBase[name]; base > 0 {
		b.ReportMetric(mps/(float64(n)*base), "linearity")
	}
}

// BenchmarkEchoStream is the echo_stream benchmark: 100 connections each
// streaming 1 MiB per op through the echo with a concurrent reader. SetBytes
// and GB/s count one direction (bytes echoed back to the clients).
func BenchmarkEchoStream(b *testing.B) {
	const conns = 100
	for _, srv := range servers {
		if srv.inIdle {
			continue
		}
		b.Run(srv.name, func(b *testing.B) {
			_, cs := startConns(b, func() (Server, error) { return srv.start(*echoLoops) }, conns)
			if _, err := Stream(cs, 64<<10); err != nil { // warm-up
				b.Fatal(err)
			}
			b.SetBytes(conns << 20)
			b.ResetTimer()
			el, err := Stream(cs, int64(b.N)<<20)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N)*float64(conns<<20)/el.Seconds()/1e9, "GB/s")
		})
	}
}

// processCPU returns the user+system CPU time consumed by this process.
func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// cpuUtil returns the fraction of all CPUs this process used since cpu0.
func cpuUtil(cpu0, wall time.Duration) float64 {
	return float64(processCPU()-cpu0) / (float64(wall) * float64(runtime.NumCPU()))
}

// httpBase holds the net/http req/s of the latest run, per concurrency.
var httpBase = map[int]float64{}

// BenchmarkHTTPPlaintext is the http_plaintext benchmark with an
// in-process, wrk-style client (keep-alive, no pipelining, one request in
// flight per connection) at 256 and 10000 connections. One op is one request
// on every connection. On the twine rows "x-nethttp" is req/s over the
// net/http row at the same concurrency (the Phase 5 gate is >= 5 at c=256).
// The client shares the machine with the server; check cpu-util.
func BenchmarkHTTPPlaintext(b *testing.B) {
	for _, c := range []int{256, 10000} {
		for _, name := range []string{"nethttp", "twine"} {
			b.Run(fmt.Sprintf("c=%d/%s", c, name), func(b *testing.B) {
				start := func() (Server, error) { return StartHTTPNet(loopback) }
				if name == "twine" {
					start = func() (Server, error) { return StartHTTPTwine(loopback, *echoLoops) }
				}
				s, cs := startConns(b, start, c)
				req := HTTPRequest(s.Addr(), "/plaintext")
				if _, err := HTTPLoad(cs, req, 1, time.Time{}, false); err != nil {
					b.Fatal(err)
				}
				cpu0 := processCPU()
				b.ResetTimer()
				res, err := HTTPLoad(cs, req, b.N, time.Time{}, true)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				rps := res.MsgsPerSec()
				b.ReportMetric(cpuUtil(cpu0, res.Elapsed), "cpu-util")
				b.ReportMetric(rps, "req/s")
				b.ReportMetric(float64(res.Latency.Percentile(0.50).Nanoseconds())/1e3, "p50-µs")
				b.ReportMetric(float64(res.Latency.Percentile(0.99).Nanoseconds())/1e3, "p99-µs")
				if name == "nethttp" {
					httpBase[c] = rps
					return
				}
				b.ReportMetric(rps/float64(*echoLoops), "req/s/loop")
				if base := httpBase[c]; base > 0 {
					b.ReportMetric(rps/base, "x-nethttp")
				}
			})
		}
	}
}

// memSample forces a full collection, returns freed pages to the OS and
// reads heap, stack and RSS.
func memSample() (heap, stack, rss uint64) {
	runtime.GC()
	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse, ms.StackInuse, ProcessRSS()
}

// BenchmarkEchoIdle opens -idle-conns idle connections (default 10000) to
// each server and reports server memory per connection: Go heap in use,
// goroutine stacks in use, and process RSS deltas divided by the connection
// count. The client side uses raw file descriptors so it adds only 8 B/conn
// of Go memory (see the raw-baseline row, whose server holds nothing but the
// accepted fds). Kernel socket memory is not in RSS and is not counted. The
// body runs once (setup plus the settle delay exceed the 1 s benchtime).
func BenchmarkEchoIdle(b *testing.B) {
	for _, srv := range servers {
		b.Run(srv.name, func(b *testing.B) {
			n := *idleConns
			s, err := srv.start(*echoLoops)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			// Let stacks and spans freed by earlier benchmarks (thousands of
			// client goroutines) be released before the baseline sample.
			for i := 0; i < 3; i++ {
				memSample()
				time.Sleep(100 * time.Millisecond)
			}
			h0, st0, r0 := memSample()
			fds, err := OpenIdle(s.Addr(), n)
			if err != nil {
				b.Fatal(err)
			}
			defer CloseFDs(fds)
			deadline := time.Now().Add(60 * time.Second)
			for s.Conns() < n {
				if time.Now().After(deadline) {
					b.Fatalf("server accepted %d of %d connections", s.Conns(), n)
				}
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(*idleSettle)
			h1, st1, r1 := memSample()
			per := func(after, before uint64) float64 {
				return (float64(after) - float64(before)) / float64(n)
			}
			b.ReportMetric(per(h1, h0), "heap-B/conn")
			b.ReportMetric(per(st1, st0), "stack-B/conn")
			b.ReportMetric(per(h1+st1, h0+st0), "go-B/conn")
			b.ReportMetric(per(r1, r0), "rss-B/conn")
			b.ReportMetric(per(r1, r0)*1e6/1e9, "rss-GB@1M")
			for i := 0; i < b.N; i++ {
			}
		})
	}
}
