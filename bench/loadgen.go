package bench

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// dialConcurrency bounds concurrent connection attempts so a burst of
// thousands of dials never overflows a listen backlog.
const dialConcurrency = 128

// DialAll opens n TCP connections to addr with bounded concurrency and
// TCP_NODELAY. On error every connection opened so far is closed.
func DialAll(addr string, n int) ([]net.Conn, error) {
	conns := make([]net.Conn, n)
	var (
		wg      sync.WaitGroup
		next    atomic.Int64
		errOnce sync.Once
		first   error
	)
	workers := min(dialConcurrency, n)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := net.Dialer{Timeout: 10 * time.Second}
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				c, err := d.Dial("tcp", addr)
				if err != nil {
					errOnce.Do(func() { first = err })
					return
				}
				conns[i] = c
			}
		}()
	}
	wg.Wait()
	if first != nil {
		CloseAll(conns)
		return nil, first
	}
	return conns, nil
}

// CloseAll closes every non-nil connection with SO_LINGER 0 (a reset).
// Benchmarks dial tens of thousands of connections per run; an orderly
// close would leave each in TIME_WAIT on the client side for 60 s, and with
// -count=5 those exhaust the loopback ephemeral port range and stall later
// dials.
func CloseAll(conns []net.Conn) {
	for _, c := range conns {
		if c == nil {
			continue
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		c.Close()
	}
}

// PingPongResult is the outcome of a ping-pong run.
type PingPongResult struct {
	Msgs    uint64        // round trips completed
	Elapsed time.Duration // wall time of the run
	Latency *Histogram    // per-round-trip latency, nil if not recorded
}

// MsgsPerSec returns round trips per second.
func (r PingPongResult) MsgsPerSec() float64 {
	return float64(r.Msgs) / r.Elapsed.Seconds()
}

// PingPong runs one goroutine per connection, each writing a size-byte
// message and reading the full echo before sending the next (one message in
// flight per connection). Each connection performs perConn round trips, or
// runs until deadline when perConn is 0. A message mismatch or I/O error
// stops the run and is returned.
func PingPong(conns []net.Conn, size, perConn int, deadline time.Time, latency bool) (PingPongResult, error) {
	var (
		wg      sync.WaitGroup
		total   atomic.Uint64
		errOnce sync.Once
		first   error
		stop    atomic.Bool
	)
	hists := make([]*Histogram, len(conns))
	start := time.Now()
	for i, c := range conns {
		var h *Histogram
		if latency {
			h = new(Histogram)
			hists[i] = h
		}
		wg.Add(1)
		go func(c net.Conn, h *Histogram) {
			defer wg.Done()
			out := make([]byte, size)
			in := make([]byte, size)
			for j := range out {
				out[j] = byte(j)
			}
			var n uint64
			for k := 0; perConn == 0 || k < perConn; k++ {
				if stop.Load() || (perConn == 0 && k&63 == 0 && time.Now().After(deadline)) {
					break
				}
				out[0] = byte(k)
				var t0 time.Time
				if h != nil {
					t0 = time.Now()
				}
				_, err := c.Write(out)
				if err == nil {
					_, err = io.ReadFull(c, in)
				}
				if err == nil && in[0] != out[0] {
					err = errors.New("echo mismatch")
				}
				if err != nil {
					errOnce.Do(func() { first = err })
					stop.Store(true)
					break
				}
				if h != nil {
					h.Record(time.Since(t0))
				}
				n++
			}
			total.Add(n)
		}(c, h)
	}
	wg.Wait()
	res := PingPongResult{Msgs: total.Load(), Elapsed: time.Since(start)}
	if latency {
		res.Latency = new(Histogram)
		for _, h := range hists {
			res.Latency.Merge(h)
		}
	}
	return res, first
}

// streamChunk is the write size used by Stream.
const streamChunk = 64 << 10

// Stream pushes perConn bytes through every connection with a writer and a
// concurrent reader goroutine per connection, verifying the byte count of
// the echo. It returns the wall time. Throughput is perConn*len(conns) bytes
// in each direction.
func Stream(conns []net.Conn, perConn int64) (time.Duration, error) {
	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		first   error
	)
	fail := func(err error) { errOnce.Do(func() { first = err }) }
	start := time.Now()
	for _, c := range conns {
		wg.Add(2)
		go func(c net.Conn) {
			defer wg.Done()
			buf := make([]byte, streamChunk)
			for i := range buf {
				buf[i] = byte(i)
			}
			for left := perConn; left > 0; {
				n := int64(len(buf))
				if left < n {
					n = left
				}
				if _, err := c.Write(buf[:n]); err != nil {
					fail(err)
					return
				}
				left -= n
			}
		}(c)
		go func(c net.Conn) {
			defer wg.Done()
			buf := make([]byte, streamChunk)
			var got int64
			for got < perConn {
				n, err := c.Read(buf)
				got += int64(n)
				if err != nil {
					fail(fmt.Errorf("stream read after %d of %d bytes: %w", got, perConn, err))
					return
				}
			}
		}(c)
	}
	wg.Wait()
	return time.Since(start), first
}

// idlePerSourceIP bounds connections per loopback source address. The
// kernel's connect-time port search slows down sharply once a source
// address's ephemeral range is crowded (live plus TIME_WAIT sockets from
// earlier runs), so idle connections are spread over 127.0.0.2, 127.0.0.3...
const idlePerSourceIP = 10000

// OpenIdle opens n connections to the IPv4 addr with raw blocking syscalls
// and returns their file descriptors. The client side holds no Go objects
// beyond the returned slice, so heap and RSS deltas measured afterwards are
// dominated by the server. Every 10000 connections bind to the next
// loopback source address (127.0.0.2, 127.0.0.3, ...) to keep the kernel's
// ephemeral port search fast.
func OpenIdle(addr string, n int) ([]int, error) {
	ta, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}
	dst := &unix.SockaddrInet4{Port: ta.Port}
	copy(dst.Addr[:], ta.IP.To4())
	fds := make([]int, n)
	for i := range fds {
		fds[i] = -1
	}
	var (
		wg      sync.WaitGroup
		next    atomic.Int64
		errOnce sync.Once
		first   error
	)
	for w := 0; w < min(dialConcurrency, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				fd, err := dialRaw(dst, i/idlePerSourceIP)
				if err != nil {
					errOnce.Do(func() { first = fmt.Errorf("idle conn %d: %w", i, err) })
					return
				}
				fds[i] = fd
			}
		}()
	}
	wg.Wait()
	if first != nil {
		CloseFDs(fds)
		return nil, first
	}
	return fds, nil
}

func dialRaw(dst *unix.SockaddrInet4, srcIdx int) (int, error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT, 1)
	ip := srcIdx + 2 // 127.0.0.2, 127.0.0.3, ...
	src := &unix.SockaddrInet4{Addr: [4]byte{127, 0, byte(ip >> 8), byte(ip & 0xff)}}
	if err := unix.Bind(fd, src); err != nil {
		unix.Close(fd)
		return -1, err
	}
	for {
		err = unix.Connect(fd, dst)
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// CloseFDs closes every non-negative descriptor with SO_LINGER 0 (a reset),
// so that a large idle run leaves no TIME_WAIT sockets behind to slow down
// the next run's port search.
func CloseFDs(fds []int) {
	for _, fd := range fds {
		if fd >= 0 {
			_ = unix.SetsockoptLinger(fd, unix.SOL_SOCKET, unix.SO_LINGER, &unix.Linger{Onoff: 1, Linger: 0})
			unix.Close(fd)
		}
	}
}

// ProcessRSS returns this process's resident set size in bytes from
// /proc/self/status (VmRSS), or 0 where unavailable.
func ProcessRSS() uint64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		line := sc.Bytes()
		if !bytes.HasPrefix(line, []byte("VmRSS:")) {
			continue
		}
		f := bytes.Fields(line[len("VmRSS:"):])
		if len(f) == 0 {
			return 0
		}
		kb, err := strconv.ParseUint(string(f[0]), 10, 64)
		if err != nil {
			return 0
		}
		return kb << 10
	}
	return 0
}

// Histogram is a log-linear latency histogram: 16 linear sub-buckets per
// power of two, so a reported percentile is within about 6% of the true
// value. The zero value is ready to use; it is not safe for concurrent use.
type Histogram struct {
	counts [64 * histSub]uint64
	n      uint64
	max    time.Duration
}

const (
	histSubBits = 4
	histSub     = 1 << histSubBits
)

// Record adds one observation.
func (h *Histogram) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.counts[histIndex(uint64(d))]++
	h.n++
	if d > h.max {
		h.max = d
	}
}

func histIndex(v uint64) int {
	if v < histSub {
		return int(v)
	}
	e := bits.Len64(v) - histSubBits // >= 1
	return e*histSub + int(v>>uint(e-1))&(histSub-1)
}

// histUpper returns the exclusive upper bound of bucket i.
func histUpper(i int) uint64 {
	e, s := i/histSub, i%histSub
	if e == 0 {
		return uint64(s) + 1
	}
	return uint64(histSub+s+1) << uint(e-1)
}

// Merge adds o's observations to h.
func (h *Histogram) Merge(o *Histogram) {
	if o == nil {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.n += o.n
	if o.max > h.max {
		h.max = o.max
	}
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.n }

// Max returns the largest observation.
func (h *Histogram) Max() time.Duration { return h.max }

// Percentile returns the bucket upper bound at quantile q in [0, 1].
func (h *Histogram) Percentile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	target := uint64(q * float64(h.n))
	if target == 0 {
		target = 1
	}
	var acc uint64
	for i, c := range h.counts {
		acc += c
		if acc >= target {
			d := time.Duration(histUpper(i))
			if d > h.max {
				d = h.max
			}
			return d
		}
	}
	return h.max
}

// HTTPRequest returns a keep-alive GET request for path on host, the shape
// wrk sends.
func HTTPRequest(host, path string) []byte {
	return []byte("GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\n\r\n")
}

// HTTPLoad is the HTTP counterpart of PingPong, modelled on wrk without
// pipelining: one goroutine per keep-alive connection sends req, reads the
// full response (status line, headers, Content-Length body) and repeats.
// Each connection performs perConn requests, or runs until deadline when
// perConn is 0. A non-200 status, a response without Content-Length or an
// I/O error stops the run and is returned.
func HTTPLoad(conns []net.Conn, req []byte, perConn int, deadline time.Time, latency bool) (PingPongResult, error) {
	var (
		wg      sync.WaitGroup
		total   atomic.Uint64
		errOnce sync.Once
		first   error
		stop    atomic.Bool
	)
	hists := make([]*Histogram, len(conns))
	start := time.Now()
	for i, c := range conns {
		var h *Histogram
		if latency {
			h = new(Histogram)
			hists[i] = h
		}
		wg.Add(1)
		go func(c net.Conn, h *Histogram) {
			defer wg.Done()
			br := bufio.NewReaderSize(c, 4096)
			var n uint64
			for k := 0; perConn == 0 || k < perConn; k++ {
				if stop.Load() || (perConn == 0 && k&63 == 0 && time.Now().After(deadline)) {
					break
				}
				var t0 time.Time
				if h != nil {
					t0 = time.Now()
				}
				_, err := c.Write(req)
				if err == nil {
					err = readHTTPResponse(br)
				}
				if err != nil {
					errOnce.Do(func() { first = err })
					stop.Store(true)
					break
				}
				if h != nil {
					h.Record(time.Since(t0))
				}
				n++
			}
			total.Add(n)
		}(c, h)
	}
	wg.Wait()
	res := PingPongResult{Msgs: total.Load(), Elapsed: time.Since(start)}
	if latency {
		res.Latency = new(Histogram)
		for _, h := range hists {
			res.Latency.Merge(h)
		}
	}
	return res, first
}

var contentLength = []byte("content-length:")

// readHTTPResponse consumes one response with a Content-Length body.
func readHTTPResponse(br *bufio.Reader) error {
	line, err := br.ReadSlice('\n')
	if err != nil {
		return err
	}
	if len(line) < 12 || !bytes.HasPrefix(line, []byte("HTTP/1.")) || string(line[9:12]) != "200" {
		return fmt.Errorf("unexpected status line %q", line)
	}
	cl := -1
	for {
		line, err = br.ReadSlice('\n')
		if err != nil {
			return err
		}
		if len(line) <= 2 { // CRLF: end of head
			break
		}
		if len(line) > len(contentLength) && bytes.EqualFold(line[:len(contentLength)], contentLength) {
			v := bytes.TrimSpace(line[len(contentLength):])
			n, err := strconv.Atoi(string(v))
			if err != nil {
				return fmt.Errorf("bad Content-Length %q", v)
			}
			cl = n
		}
	}
	if cl < 0 {
		return errors.New("response without Content-Length")
	}
	_, err = br.Discard(cl)
	return err
}
