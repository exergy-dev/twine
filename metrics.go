package twine

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// loopMetrics holds a loop's counters. The loop updates them without
// contention; Loop.Metrics copies them into a MetricsSnapshot from any
// goroutine. Each counter is documented on the MetricsSnapshot field of the
// same name.
type loopMetrics struct {
	connections      atomic.Uint64
	accepts          atomic.Uint64
	closes           atomic.Uint64
	reads            atomic.Uint64
	readBytes        atomic.Uint64
	writeBytes       atomic.Uint64
	writev           atomic.Uint64
	writeDirectHit   atomic.Uint64
	writeAfterClose  atomic.Uint64
	timersFired      atomic.Uint64
	tasksRun         atomic.Uint64
	wakeups          atomic.Uint64
	wakeupsCoalesced atomic.Uint64
	iterations       atomic.Uint64
	pollBlockNanos   atomic.Uint64
	panics           atomic.Uint64
	backlogDrops     atomic.Uint64
	acceptErrors     atomic.Uint64
	datagrams        atomic.Uint64
	datagramsSent    atomic.Uint64
	slowCallbacks    atomic.Uint64

	// Published by the loop at the end of each iteration.
	slabInUse     atomic.Int64
	slabFree      atomic.Int64
	ringHighWater atomic.Int64

	iterHist histogram
}

// MetricsSnapshot is a point-in-time copy of one loop's counters plus
// derived values. Counters only grow; gauges are marked as such.
type MetricsSnapshot struct {
	// LoopID is the index of the loop within its Engine.
	LoopID int
	// Connections is the number of currently open connections (gauge).
	Connections uint64
	// Accepts is the number of accepted connections.
	Accepts uint64
	// Closes is the number of closed connections.
	Closes uint64
	// Reads is the number of read(2) calls that returned data.
	Reads uint64
	// ReadBytes is the number of bytes read.
	ReadBytes uint64
	// WriteBytes is the number of bytes written.
	WriteBytes uint64
	// Writev is the number of flush writev(2) calls.
	Writev uint64
	// WriteDirectHit is the number of writes that bypassed the ring.
	WriteDirectHit uint64
	// WriteAfterClose is the number of writes on closed or stale transports (ErrClosed).
	WriteAfterClose uint64
	// TimersFired is the number of timers fired.
	TimersFired uint64
	// TasksRun is the number of CallFromThread tasks run.
	TasksRun uint64
	// Wakeups is the number of eventfd wakeups issued.
	Wakeups uint64
	// WakeupsCoalesced is the number of wakeups avoided by coalescing.
	WakeupsCoalesced uint64
	// Iterations is the number of loop iterations.
	Iterations uint64
	// PollBlockNanos is the number of nanoseconds spent blocked in poll.
	PollBlockNanos uint64
	// Panics is the number of recovered panics.
	Panics uint64
	// BacklogDrops is the number of tasks dropped after loop exit.
	BacklogDrops uint64
	// AcceptErrors is the number of accept(2) errors.
	AcceptErrors uint64
	// Datagrams is the number of datagrams received.
	Datagrams uint64
	// DatagramsSent is the number of datagrams sent.
	DatagramsSent uint64
	// SlowCallbacks is the number of watchdog reports.
	SlowCallbacks uint64

	// IterationP50 is the median loop iteration duration over the last 10
	// seconds (log2 buckets, so it is an upper bound within a factor of 2).
	IterationP50 time.Duration
	// IterationP99 is the 99th-percentile loop iteration duration over the
	// last 10 seconds, with the same resolution as IterationP50.
	IterationP99 time.Duration
	// IterationMax is the longest loop iteration over the last 10 seconds.
	IterationMax time.Duration

	// SlabInUse is the number of slab bytes held by write rings (gauge).
	SlabInUse int
	// SlabFree is the number of slab bytes sitting in free lists (gauge).
	SlabFree int
	// RingHighWater is the largest pending-write backlog of any connection
	// on the loop since it started, in bytes.
	RingHighWater int
}

func (m *loopMetrics) snapshot() MetricsSnapshot {
	s := MetricsSnapshot{
		Connections:      m.connections.Load(),
		Accepts:          m.accepts.Load(),
		Closes:           m.closes.Load(),
		Reads:            m.reads.Load(),
		ReadBytes:        m.readBytes.Load(),
		WriteBytes:       m.writeBytes.Load(),
		Writev:           m.writev.Load(),
		WriteDirectHit:   m.writeDirectHit.Load(),
		WriteAfterClose:  m.writeAfterClose.Load(),
		TimersFired:      m.timersFired.Load(),
		TasksRun:         m.tasksRun.Load(),
		Wakeups:          m.wakeups.Load(),
		WakeupsCoalesced: m.wakeupsCoalesced.Load(),
		Iterations:       m.iterations.Load(),
		PollBlockNanos:   m.pollBlockNanos.Load(),
		Panics:           m.panics.Load(),
		BacklogDrops:     m.backlogDrops.Load(),
		AcceptErrors:     m.acceptErrors.Load(),
		Datagrams:        m.datagrams.Load(),
		DatagramsSent:    m.datagramsSent.Load(),
		SlowCallbacks:    m.slowCallbacks.Load(),
	}
	s.IterationP50, s.IterationP99, s.IterationMax = m.iterHist.percentiles()
	s.SlabInUse = int(m.slabInUse.Load())
	s.SlabFree = int(m.slabFree.Load())
	s.RingHighWater = int(m.ringHighWater.Load())
	return s
}

// histogram is a log2-bucketed histogram over a 10-second sliding window of
// one-second slots. observe is called on the loop; percentiles from anywhere.
type histogram struct {
	// buckets[slot][bucket]; bucket i counts values in [2^i, 2^(i+1)) ns.
	buckets [histSlots][histBuckets]atomic.Uint64
	maxNs   [histSlots]atomic.Int64
	slotSec atomic.Int64 // wall second currently being written
	cur     int
}

const (
	histSlots   = 10
	histBuckets = 40 // up to ~1000 s
)

func (h *histogram) observe(ns int64, sec int64) {
	if ns < 0 {
		ns = 0
	}
	if sec != h.slotSec.Load() {
		h.slotSec.Store(sec)
		h.cur = int(sec % histSlots)
		for i := range h.buckets[h.cur] {
			h.buckets[h.cur][i].Store(0)
		}
		h.maxNs[h.cur].Store(0)
	}
	b := bits.Len64(uint64(ns))
	if b >= histBuckets {
		b = histBuckets - 1
	}
	h.buckets[h.cur][b].Add(1)
	if ns > h.maxNs[h.cur].Load() {
		h.maxNs[h.cur].Store(ns)
	}
}

func (h *histogram) percentiles() (p50, p99, maxv time.Duration) {
	var merged [histBuckets]uint64
	var total uint64
	for s := 0; s < histSlots; s++ {
		for b := 0; b < histBuckets; b++ {
			v := h.buckets[s][b].Load()
			merged[b] += v
			total += v
		}
		if m := h.maxNs[s].Load(); time.Duration(m) > maxv {
			maxv = time.Duration(m)
		}
	}
	if total == 0 {
		return 0, 0, 0
	}
	pick := func(q float64) time.Duration {
		target := uint64(float64(total) * q)
		var acc uint64
		for b := 0; b < histBuckets; b++ {
			acc += merged[b]
			if acc >= target {
				if b == 0 {
					return 0
				}
				return time.Duration(int64(1) << uint(b)) // upper bound of bucket b-1
			}
		}
		return maxv
	}
	return pick(0.5), pick(0.99), maxv
}
