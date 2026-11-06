package twine

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// LoopMetrics holds per-loop counters. They are updated on the loop without
// contention and may be read from any goroutine via Snapshot.
type LoopMetrics struct {
	// Connections is the number of currently open connections (gauge).
	Connections atomic.Uint64
	// Accepts is the number of accepted connections.
	Accepts atomic.Uint64
	// Closes is the number of closed connections.
	Closes atomic.Uint64
	// Reads is the number of read(2) calls that returned data.
	Reads atomic.Uint64
	// ReadBytes is the number of bytes read.
	ReadBytes atomic.Uint64
	// WriteBytes is the number of bytes written.
	WriteBytes atomic.Uint64
	// Writev is the number of flush writev(2) calls.
	Writev atomic.Uint64
	// WriteDirectHit is the number of writes that bypassed the ring.
	WriteDirectHit atomic.Uint64
	// WriteAfterClose is the number of writes on closed or stale transports (ErrClosed).
	WriteAfterClose atomic.Uint64
	// TimersFired is the number of timers fired.
	TimersFired atomic.Uint64
	// TasksRun is the number of CallFromThread tasks run.
	TasksRun atomic.Uint64
	// Wakeups is the number of eventfd wakeups issued.
	Wakeups atomic.Uint64
	// WakeupsCoalesced is the number of wakeups avoided by coalescing.
	WakeupsCoalesced atomic.Uint64
	// Iterations: loop iterations.
	Iterations atomic.Uint64
	// PollBlockNanos is the number of nanoseconds spent blocked in poll.
	PollBlockNanos atomic.Uint64
	// Panics is the number of recovered panics.
	Panics atomic.Uint64
	// BacklogDrops is the number of tasks dropped after loop exit.
	BacklogDrops atomic.Uint64
	// AcceptErrors is the number of accept(2) errors.
	AcceptErrors atomic.Uint64
	// Datagrams is the number of datagrams received.
	Datagrams atomic.Uint64
	// DatagramsSent is the number of datagrams sent.
	DatagramsSent atomic.Uint64
	// SlowCallbacks is the number of watchdog reports.
	SlowCallbacks atomic.Uint64

	// Published by the loop at the end of each iteration.
	slabInUse     atomic.Int64
	slabFree      atomic.Int64
	ringHighWater atomic.Int64

	iterHist histogram
}

// MetricsSnapshot is a plain copy of LoopMetrics plus derived values.
type MetricsSnapshot struct {
	// LoopID: index of the loop within its Engine.
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
	// Iterations: loop iterations.
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

	// Iteration duration percentiles over the last 10 seconds.
	// IterationP50: iteration duration p50 over the last 10 s.
	IterationP50 time.Duration
	// IterationP99: iteration duration p99 over the last 10 s.
	IterationP99 time.Duration
	// IterationMax: longest iteration over the last 10 s.
	IterationMax time.Duration

	// Memory.
	// SlabInUse: slab bytes handed out to rings.
	SlabInUse int
	// SlabFree: slab bytes sitting in free lists.
	SlabFree int
	// RingHighWater: largest pending-write backlog seen.
	RingHighWater int
}

func (m *LoopMetrics) snapshot() MetricsSnapshot {
	s := MetricsSnapshot{
		Connections:      m.Connections.Load(),
		Accepts:          m.Accepts.Load(),
		Closes:           m.Closes.Load(),
		Reads:            m.Reads.Load(),
		ReadBytes:        m.ReadBytes.Load(),
		WriteBytes:       m.WriteBytes.Load(),
		Writev:           m.Writev.Load(),
		WriteDirectHit:   m.WriteDirectHit.Load(),
		WriteAfterClose:  m.WriteAfterClose.Load(),
		TimersFired:      m.TimersFired.Load(),
		TasksRun:         m.TasksRun.Load(),
		Wakeups:          m.Wakeups.Load(),
		WakeupsCoalesced: m.WakeupsCoalesced.Load(),
		Iterations:       m.Iterations.Load(),
		PollBlockNanos:   m.PollBlockNanos.Load(),
		Panics:           m.Panics.Load(),
		BacklogDrops:     m.BacklogDrops.Load(),
		AcceptErrors:     m.AcceptErrors.Load(),
		Datagrams:        m.Datagrams.Load(),
		DatagramsSent:    m.DatagramsSent.Load(),
		SlowCallbacks:    m.SlowCallbacks.Load(),
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
