// Package wheel implements a hierarchical timing wheel with O(1) insert and
// cancel and no allocation.
//
// The wheel has 4 levels of 256 slots. Level L slot width is 256^L ticks, so
// the horizon is 256^4 ticks (about 49.7 days at a 1 ms tick); timers further
// out are parked in the top level and re-placed on cascade until they fit.
// Timers are intrusive doubly-linked list nodes owned by the caller.
//
// Time is measured in ticks since the wheel's base time. A timer scheduled
// for absolute time at has expiry tick ceil((at-base)/tick) and fires during
// the first Advance(now) whose tick floor((now-base)/tick) reaches it. A
// timer therefore never fires early (at <= now always holds when Fn runs) and
// fires at most one tick late. When at and now are multiples of the tick
// (relative to the base), firing is exact: a timer fires during the first
// Advance(now) with now >= at.
//
// A Wheel is not safe for concurrent use.
package wheel

import (
	"math/bits"
	"time"
)

const (
	levels    = 4
	slotBits  = 8
	slots     = 1 << slotBits
	slotMask  = slots - 1
	horizon   = int64(1) << (levels * slotBits) // ticks
	maxAheadN = uint64(1) << 61                 // clamp for absurd expiries, ns
)

// Location tags stored in Timer.loc. 0 means unscheduled; 1..levels*slots
// encode a wheel slot as index+1.
const (
	locNone   = 0
	locDue    = levels*slots + 1
	locFiring = levels*slots + 2
)

// Timer is an intrusive timer node. Callers embed or allocate it; the wheel
// never allocates one. Fn is invoked by Advance when the timer expires; a nil
// Fn is allowed and fires as a no-op. The zero value is a valid unscheduled
// timer, and a timer that has fired or been removed may be re-added
// immediately without being reset.
//
// A scheduled Timer must not be copied, freed, or added to a different Wheel;
// Remove it first.
type Timer struct {
	// Fn is called by Advance when the timer expires. It may be changed
	// while the timer is scheduled; the value at fire time is used.
	Fn func()
	// FnT, when Fn is nil, is called with the expired timer. A package-level
	// function stored here costs no allocation, unlike a method value in Fn;
	// callers embed Timer as the first field of their own struct and convert
	// the pointer back.
	FnT func(*Timer)

	next, prev *Timer
	expires    int64 // absolute expiry in ticks since the wheel's base
	loc        int32 // locNone, locDue, locFiring or wheel slot index+1
}

// Scheduled reports whether t is currently in a wheel. It is false inside t's
// own Fn unless Fn re-added t.
func (t *Timer) Scheduled() bool { return t.loc != locNone }

// Expires returns the absolute expiry in ticks since the wheel's base time.
// It is meaningful only while t is Scheduled. For a timer added with an at
// that was already in the past it is at most the wheel's current tick.
func (t *Timer) Expires() int64 { return t.expires }

// Wheel is a hierarchical timing wheel of 4 levels × 256 slots. Create one
// with New. A Wheel must not be copied after first use.
type Wheel struct {
	// Slot list sentinels: circular doubly-linked lists, FIFO by insertion.
	wheel [levels][slots]Timer
	// Occupancy bitmaps, one bit per slot, kept exact on every link/unlink.
	occ [levels][slots / 64]uint64

	due    Timer // timers whose at had passed when added; fire next Advance
	firing Timer // timers being fired by the current Advance

	tick  int64 // tick length in ns (> 0)
	base  int64 // monotonic ns of tick 0
	cur   int64 // current tick (ticks since base)
	now   int64 // last time passed to Advance (or New); cur is its tick outside Advance
	count int
}

// New creates a wheel with the given tick length whose tick 0 is at the
// monotonic nanosecond time now. A tick <= 0 is replaced by 1 ms.
func New(tick time.Duration, now int64) *Wheel {
	if tick <= 0 {
		tick = time.Millisecond
	}
	w := &Wheel{tick: int64(tick), base: now, now: now}
	for l := range w.wheel {
		for s := range w.wheel[l] {
			initList(&w.wheel[l][s])
		}
	}
	initList(&w.due)
	initList(&w.firing)
	return w
}

func initList(s *Timer) { s.next, s.prev = s, s }

// pushBack appends t to the list with sentinel s.
func pushBack(s, t *Timer) {
	last := s.prev
	t.prev = last
	t.next = s
	last.next = t
	s.prev = t
}

// Len returns the number of scheduled timers, including timers that are due
// but not yet fired.
func (w *Wheel) Len() int { return w.count }

// Now returns the start of the wheel's current tick as monotonic ns.
func (w *Wheel) Now() int64 { return w.base + w.cur*w.tick }

// Tick returns the wheel's tick length.
func (w *Wheel) Tick() time.Duration { return time.Duration(w.tick) }

// Add schedules t to fire when the wheel is advanced to or past at (absolute
// monotonic nanoseconds). An at that is not after the time last passed to
// Advance (or New) fires on the next Advance; when called from inside an Fn
// during Advance(now) with at <= now, t fires within that same Advance. If t
// is already scheduled it is removed first (Reset semantics). O(1); never
// allocates.
func (w *Wheel) Add(t *Timer, at int64) {
	if t.loc != locNone {
		w.Remove(t)
	}
	w.count++
	if at <= w.now {
		t.expires = w.cur
		t.loc = locDue
		pushBack(&w.due, t)
		return
	}
	// Exact positive difference even if at-w.now overflows int64.
	d := uint64(at) - uint64(w.now)
	if d > maxAheadN {
		d = maxAheadN
	}
	off := uint64(w.now-w.base) + d // ns since base, > 0
	tk := uint64(w.tick)
	e := off / tk
	if off%tk != 0 {
		e++
	}
	t.expires = int64(e)
	w.place(t)
}

// place links t into the wheel slot for t.expires relative to w.cur.
// t.expires must be >= w.cur.
func (w *Wheel) place(t *Timer) {
	e := t.expires
	delta := e - w.cur
	var level int
	switch {
	case delta < 1<<slotBits:
		level = 0
	case delta < 1<<(2*slotBits):
		level = 1
	case delta < 1<<(3*slotBits):
		level = 2
	case delta < horizon:
		level = 3
	default:
		// Beyond the horizon: park in the last top-level slot reachable
		// within the horizon; cascade re-places it.
		level = 3
		e = w.cur + horizon - 1
	}
	s := int((e >> (uint(level) * slotBits)) & slotMask)
	pushBack(&w.wheel[level][s], t)
	w.occ[level][s>>6] |= 1 << (uint(s) & 63)
	t.loc = int32(level*slots + s + 1)
}

// Remove unschedules t. It returns false if t was not scheduled (including
// when called on t from inside t's own Fn). O(1).
func (w *Wheel) Remove(t *Timer) bool {
	loc := t.loc
	if loc == locNone {
		return false
	}
	t.prev.next = t.next
	t.next.prev = t.prev
	t.next, t.prev = nil, nil
	t.loc = locNone
	w.count--
	if loc <= levels*slots {
		i := int(loc - 1)
		level, s := i>>slotBits, i&slotMask
		if sl := &w.wheel[level][s]; sl.next == sl {
			w.occ[level][s>>6] &^= 1 << (uint(s) & 63)
		}
	}
	return true
}

// Advance moves the wheel to now (monotonic ns), cascading levels and calling
// Fn on every expired timer: first timers that were already due, then tick by
// tick in expiry order, FIFO by insertion within a tick. Fn may Add or Remove
// any timer, including the one being fired; timers it adds with at <= now
// fire within this Advance. A now earlier than a previous call is treated as
// that previous time. Advance returns the number of timers fired and never
// allocates.
//
// A Fn that unconditionally re-adds its own timer with at <= now makes
// Advance loop forever. Advance must not be called from inside Fn.
func (w *Wheel) Advance(now int64) int {
	if now <= w.now {
		fired := 0
		for w.due.next != &w.due {
			fired += w.fireList(&w.due)
		}
		return fired
	}
	w.now = now
	target := w.cur + int64((uint64(now)-uint64(w.base+w.cur*w.tick))/uint64(w.tick))

	fired := 0
	for {
		for w.due.next != &w.due {
			fired += w.fireList(&w.due)
		}
		if w.cur >= target {
			break
		}
		if w.count == 0 {
			w.cur = target
			break
		}
		n, ok := w.nextEvent()
		if !ok || n > target {
			w.cur = target
			break
		}
		w.cur = n
		for level := levels - 1; level >= 1; level-- {
			if n&(int64(1)<<(uint(level)*slotBits)-1) == 0 {
				w.cascade(level, int((n>>(uint(level)*slotBits))&slotMask))
			}
		}
		s := int(n & slotMask)
		if sl := &w.wheel[0][s]; sl.next != sl {
			w.spliceAll(&w.firing, sl)
			w.occ[0][s>>6] &^= 1 << (uint(s) & 63)
			fired += w.fireList(&w.firing)
		}
	}
	return fired
}

// spliceAll moves every node of src (a slot list) to the end of dst and
// retags them as being in dst. src is left empty.
func (w *Wheel) spliceAll(dst, src *Timer) {
	loc := int32(locFiring)
	if dst == &w.due {
		loc = locDue
	}
	for t := src.next; t != src; t = t.next {
		t.loc = loc
	}
	first, last := src.next, src.prev
	first.prev = dst.prev
	dst.prev.next = first
	last.next = dst
	dst.prev = last
	initList(src)
}

// fireList fires every timer in l (the due list or the firing list) in FIFO
// order until l is empty. Each timer is unlinked before its Fn runs. If l is
// the due list, timers that Fn adds to it fire too. For the firing list, all
// nodes are first moved to it as a batch so Fn can Remove them.
func (w *Wheel) fireList(l *Timer) int {
	if l == &w.due {
		// Move due timers to the firing list so that Fn-added due timers
		// are fired by the caller's next pass in order, after this batch.
		w.spliceAll(&w.firing, l)
		l = &w.firing
	}
	n := 0
	for t := l.next; t != l; t = l.next {
		t.prev.next = t.next
		t.next.prev = t.prev
		t.next, t.prev = nil, nil
		t.loc = locNone
		w.count--
		n++
		if t.Fn != nil {
			t.Fn()
		} else if t.FnT != nil {
			t.FnT(t)
		}
	}
	return n
}

// cascade re-places every timer in slot s of level (level >= 1) relative to
// the current tick, preserving FIFO order.
func (w *Wheel) cascade(level, s int) {
	sl := &w.wheel[level][s]
	if sl.next == sl {
		return
	}
	first, last := sl.next, sl.prev
	initList(sl)
	w.occ[level][s>>6] &^= 1 << (uint(s) & 63)
	last.next = nil
	for t := first; t != nil; {
		nx := t.next
		w.place(t)
		t = nx
	}
}

// nextEvent returns the smallest tick > w.cur at which something must happen:
// a non-empty level-0 slot fires or a non-empty higher-level slot cascades.
// Every such tick is <= the expiry of every timer it covers.
func (w *Wheel) nextEvent() (int64, bool) {
	best, found := int64(0), false
	for level := 0; level < levels; level++ {
		shift := uint(level) * slotBits
		b := w.cur >> shift
		k, ok := nextSet(&w.occ[level], int((b+1)&slotMask))
		if !ok {
			continue
		}
		at := (b + 1 + int64(k)) << shift
		// All levels are consulted: a higher-level cascade can precede
		// the next non-empty level-0 slot.
		if !found || at < best {
			best, found = at, true
		}
	}
	return best, found
}

// nextSet returns the circular distance from position from to the first set
// bit at or after from in the 256-bit bitmap bm, and false if bm is empty.
func nextSet(bm *[slots / 64]uint64, from int) (int, bool) {
	wi, bi := from>>6, uint(from&63)
	if x := bm[wi] >> bi; x != 0 {
		return bits.TrailingZeros64(x), true
	}
	for i := 1; i <= len(bm); i++ {
		j := (wi + i) & (len(bm) - 1)
		if x := bm[j]; x != 0 {
			pos := j<<6 + bits.TrailingZeros64(x)
			return (pos - from) & slotMask, true
		}
	}
	return 0, false
}

// Next returns a monotonic ns time at or before the time at which the
// earliest scheduled timer becomes fireable, and false when no timer is
// scheduled. The returned time is a tick boundary: either the earliest
// timer's expiry tick (at most one tick after its at) or an earlier cascade
// boundary at which the wheel needs to be advanced to make progress. If a
// timer is already due it returns Now(). Callers converting the result into
// a poll timeout should round up, or they will wake before the tick and
// spin. O(1): it scans at most 4 × 4 bitmap words.
func (w *Wheel) Next() (int64, bool) {
	if w.count == 0 {
		return 0, false
	}
	if w.due.next != &w.due || w.firing.next != &w.firing {
		return w.Now(), true
	}
	n, ok := w.nextEvent()
	if !ok {
		return 0, false
	}
	return w.base + n*w.tick, true
}
