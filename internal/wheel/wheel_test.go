package wheel

import (
	"math/rand"
	"slices"
	"testing"
	"testing/quick"
	"time"
	"unsafe"
)

const ms = int64(time.Millisecond)

// harness tracks a set of timers against a wheel and checks timing
// invariants on every fire.
type harness struct {
	t      *testing.T
	w      *Wheel
	tick   int64
	timers []Timer
	at     []int64 // scheduled at, valid while model[i]
	model  []bool  // reference: scheduled?
	live   int
	now    int64 // now of the Advance in progress
	fired  []int // indices fired during the Advance in progress
	strict bool  // tick-aligned inputs: fire on first Advance with now >= at
	bad    string
}

func newHarness(t *testing.T, n int, base int64, strict bool) *harness {
	h := &harness{
		t: t, w: New(time.Millisecond, base), tick: ms,
		timers: make([]Timer, n), at: make([]int64, n), model: make([]bool, n),
		now: base, strict: strict,
	}
	for i := range h.timers {
		h.timers[i].Fn = func() { h.onFire(i) }
	}
	return h
}

func (h *harness) onFire(i int) {
	if !h.model[i] && h.bad == "" {
		h.bad = "fired a timer that is not scheduled"
	}
	if h.timers[i].Scheduled() && h.bad == "" {
		h.bad = "timer still Scheduled inside its own Fn"
	}
	if h.at[i] > h.now && h.bad == "" {
		h.bad = "fired early"
	}
	h.model[i] = false
	h.live--
	h.fired = append(h.fired, i)
}

func (h *harness) add(i int, at int64) {
	if !h.model[i] {
		h.live++
	}
	h.model[i] = true
	h.at[i] = at
	h.w.Add(&h.timers[i], at)
}

func (h *harness) remove(i int) {
	got := h.w.Remove(&h.timers[i])
	if got != h.model[i] && h.bad == "" {
		h.bad = "Remove result disagrees with model"
	}
	if h.model[i] {
		h.live--
	}
	h.model[i] = false
}

func (h *harness) advance(now int64) {
	h.now = now
	h.fired = h.fired[:0]
	n := h.w.Advance(now)
	if n != len(h.fired) && h.bad == "" {
		h.bad = "Advance return value != number of Fn calls"
	}
	// Late firing is detected here rather than in onFire: in strict mode a
	// timer with at <= now must not survive any Advance(now), so it can
	// never be fired by a later Advance either.
	for i, s := range h.model {
		if !s {
			continue
		}
		if h.strict && h.at[i] <= now && h.bad == "" {
			h.bad = "still scheduled although at <= now (strict)"
		}
		if h.at[i] <= now-h.tick && h.bad == "" {
			h.bad = "still scheduled more than one tick late"
		}
		if !h.timers[i].Scheduled() && h.bad == "" {
			h.bad = "model says scheduled, timer says not"
		}
	}
	if h.w.Len() != h.live && h.bad == "" {
		h.bad = "Len disagrees with model"
	}
}

// propertyRun drives a harness with random operations. In strict mode all
// times are tick-aligned relative to the base.
func propertyRun(t *testing.T, seed int64, strict bool) bool {
	r := rand.New(rand.NewSource(seed))
	base := r.Int63n(1<<50) - 1<<49
	if strict {
		base = base / ms * ms
	}
	const n = 64
	h := newHarness(t, n, base, strict)
	now := base
	for step := 0; step < 300; step++ {
		switch op := r.Intn(10); {
		case op < 5: // add / reset with a random duration up to 2^20 ticks
			var d int64
			switch r.Intn(4) {
			case 0:
				d = r.Int63n(300) * ms
			case 1:
				d = r.Int63n(1<<17) * ms
			default:
				d = r.Int63n(1<<20) * ms
			}
			if !strict {
				d += r.Int63n(ms) - ms/2
			}
			if r.Intn(20) == 0 {
				d = -r.Int63n(10 * ms) // in the past
			}
			h.add(r.Intn(n), now+d)
		case op < 6:
			h.remove(r.Intn(n))
		default: // advance by a random step
			var d int64
			switch r.Intn(5) {
			case 0:
				d = 0
			case 1:
				d = r.Int63n(4) * ms
			case 2:
				d = r.Int63n(300) * ms
			case 3:
				d = r.Int63n(1<<16) * ms
			default:
				d = r.Int63n(1<<20) * ms
			}
			if !strict {
				d += r.Int63n(ms)
			}
			now += d
			h.advance(now)
		}
		if h.bad != "" {
			t.Logf("seed %d step %d: %s", seed, step, h.bad)
			return false
		}
	}
	// Drain: everything must fire by the latest at + one tick.
	last := now
	for i, s := range h.model {
		if s && h.at[i] > last {
			last = h.at[i]
		}
	}
	h.advance(last + h.tick)
	if h.bad == "" && (h.live != 0 || h.w.Len() != 0) {
		h.bad = "timers left after final advance"
	}
	if h.bad != "" {
		t.Logf("seed %d drain: %s", seed, h.bad)
		return false
	}
	return true
}

// TestPropertyAligned: with tick-aligned at and now, a timer fires during
// exactly the first Advance(now) with now >= at.
func TestPropertyAligned(t *testing.T) {
	f := func(seed int64) bool { return propertyRun(t, seed, true) }
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatal(err)
	}
}

// TestPropertyUnaligned: with arbitrary ns at and now, a timer never fires
// early and is never still scheduled after an Advance(now) with
// now >= at + tick.
func TestPropertyUnaligned(t *testing.T) {
	f := func(seed int64) bool { return propertyRun(t, seed, false) }
	if err := quick.Check(f, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatal(err)
	}
}

func TestCascadeBoundaries(t *testing.T) {
	bounds := []int64{256, 65536, 16777216, 1 << 32}
	for _, b := range bounds {
		for _, d := range []int64{b - 1, b, b + 1} {
			for _, start := range []int64{0, 1, 255, 77777} {
				base := int64(12345) * ms
				w := New(time.Millisecond, base)
				w.Advance(base + start*ms) // non-zero current tick
				var tm Timer
				fired := 0
				tm.Fn = func() { fired++ }
				at := w.Now() + d*ms
				w.Add(&tm, at)
				if got := w.Advance(at - 1); got != 0 || fired != 0 {
					t.Fatalf("b=%d d=%d start=%d: fired early at at-1", b, d, start)
				}
				if !tm.Scheduled() || w.Len() != 1 {
					t.Fatalf("b=%d d=%d start=%d: lost timer", b, d, start)
				}
				if tm.Expires() != start+d {
					t.Fatalf("Expires = %d, want %d", tm.Expires(), start+d)
				}
				if nx, ok := w.Next(); !ok || nx > at {
					t.Fatalf("b=%d d=%d: Next = %d,%v > at %d", b, d, nx, ok, at)
				}
				if got := w.Advance(at); got != 1 || fired != 1 {
					t.Fatalf("b=%d d=%d start=%d: did not fire at at", b, d, start)
				}
				if tm.Scheduled() || w.Len() != 0 {
					t.Fatalf("still scheduled after firing")
				}
			}
		}
	}
}

// TestCascadeBoundariesStepping advances one tick at a time across the
// first two cascade boundaries.
func TestCascadeBoundariesStepping(t *testing.T) {
	w := New(time.Millisecond, 0)
	ds := []int64{1, 2, 254, 255, 256, 257, 511, 512, 513, 65535, 65536, 65537}
	timers := make([]Timer, len(ds))
	firedAt := make([]int64, len(ds))
	var cur int64
	for i, d := range ds {
		timers[i].Fn = func() { firedAt[i] = cur }
		w.Add(&timers[i], d*ms)
	}
	for cur = 1; cur <= 65540; cur++ {
		w.Advance(cur * ms)
	}
	for i, d := range ds {
		if firedAt[i] != d {
			t.Errorf("timer at tick %d fired at tick %d", d, firedAt[i])
		}
	}
}

func TestBeyondHorizon(t *testing.T) {
	w := New(time.Millisecond, 0)
	var a, b Timer
	fa, fb := 0, 0
	a.Fn = func() { fa++ }
	b.Fn = func() { fb++ }
	atA := (int64(1)<<34 + 3) * ms // well past the 2^32-tick horizon
	atB := (int64(1)<<32 + 300) * ms
	w.Add(&a, atA)
	w.Add(&b, atB)
	w.Advance(atB - 1)
	if fb != 0 || fa != 0 {
		t.Fatal("fired early")
	}
	w.Advance(atB)
	if fb != 1 || fa != 0 {
		t.Fatalf("fb=%d fa=%d", fb, fa)
	}
	w.Advance(atA - 1)
	if fa != 0 {
		t.Fatal("A fired early")
	}
	w.Advance(atA)
	if fa != 1 || w.Len() != 0 {
		t.Fatalf("fa=%d len=%d", fa, w.Len())
	}
	// Absurd expiry is clamped and does not overflow.
	var c Timer
	w.Add(&c, 1<<62)
	if !c.Scheduled() {
		t.Fatal("not scheduled")
	}
	w.Advance(w.Now() + 1000*ms)
	if !c.Scheduled() || !w.Remove(&c) {
		t.Fatal("absurd timer lost")
	}
}

func TestFIFOWithinSlotAndExpiryOrder(t *testing.T) {
	w := New(time.Millisecond, 0)
	var order []int
	timers := make([]Timer, 6)
	for i := range timers {
		timers[i].Fn = func() { order = append(order, i) }
	}
	// Ticks: 0->5, 1->3, 2->5, 3->300 (level 1), 4->3, 5->5
	ats := []int64{5, 3, 5, 300, 3, 5}
	for i, a := range ats {
		w.Add(&timers[i], a*ms)
	}
	// A sub-tick at inside tick 3 shares the slot; fires with tick 3's batch.
	if n := w.Advance(1000 * ms); n != 6 {
		t.Fatalf("fired %d", n)
	}
	want := []int{1, 4, 0, 2, 5, 3}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestRemoveOtherInsideFnSameSlot(t *testing.T) {
	w := New(time.Millisecond, 0)
	var a, b, c Timer
	var order []string
	var removed bool
	a.Fn = func() {
		order = append(order, "a")
		removed = w.Remove(&b)
	}
	b.Fn = func() { order = append(order, "b") }
	c.Fn = func() { order = append(order, "c") }
	w.Add(&a, 10*ms)
	w.Add(&b, 10*ms)
	w.Add(&c, 10*ms)
	if n := w.Advance(10 * ms); n != 2 {
		t.Fatalf("fired %d, want 2", n)
	}
	if !removed || b.Scheduled() {
		t.Fatal("Remove(b) inside a.Fn failed")
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "c" {
		t.Fatalf("order = %v", order)
	}
	if w.Len() != 0 {
		t.Fatalf("Len = %d", w.Len())
	}
	// b is reusable.
	w.Add(&b, 20*ms)
	if w.Advance(20*ms) != 1 || len(order) != 3 {
		t.Fatal("b did not fire after re-add")
	}
}

func TestRemoveDueTimerInsideFn(t *testing.T) {
	w := New(time.Millisecond, 100*ms)
	var a, b Timer
	bf := false
	a.Fn = func() { w.Remove(&b) }
	b.Fn = func() { bf = true }
	w.Add(&a, 0) // past
	w.Add(&b, 0)
	if n := w.Advance(100 * ms); n != 1 || bf {
		t.Fatalf("n=%d bf=%v", n, bf)
	}
}

func TestSelfInsideFn(t *testing.T) {
	w := New(time.Millisecond, 0)
	var tm Timer
	var removeRes, sched bool
	count := 0
	tm.Fn = func() {
		count++
		removeRes = w.Remove(&tm)
		sched = tm.Scheduled()
		if count == 1 {
			w.Add(&tm, 50*ms) // re-add in the future
		}
	}
	w.Add(&tm, 10*ms)
	if w.Advance(10*ms) != 1 || removeRes || sched {
		t.Fatalf("Remove(self)=%v Scheduled=%v", removeRes, sched)
	}
	if !tm.Scheduled() || w.Len() != 1 {
		t.Fatal("self re-add lost")
	}
	if w.Advance(49*ms) != 0 {
		t.Fatal("re-added fired early")
	}
	if w.Advance(50*ms) != 1 || count != 2 {
		t.Fatal("re-added did not fire")
	}
}

func TestSelfReAddPastFiresSameAdvance(t *testing.T) {
	w := New(time.Millisecond, 0)
	var tm Timer
	count := 0
	tm.Fn = func() {
		count++
		if count < 3 {
			w.Add(&tm, 10*ms) // at <= now: fires again in this Advance
		}
	}
	w.Add(&tm, 10*ms)
	if n := w.Advance(10 * ms); n != 3 || count != 3 || w.Len() != 0 {
		t.Fatalf("n=%d count=%d len=%d", n, count, w.Len())
	}
}

func TestAddFromFn(t *testing.T) {
	w := New(time.Millisecond, 0)
	var a, past, soon, later Timer
	var order []string
	a.Fn = func() {
		order = append(order, "a")
		w.Add(&past, 5*ms)   // at <= now (now=30ms): this Advance
		w.Add(&soon, 20*ms)  // at <= now: this Advance
		w.Add(&later, 31*ms) // future: later Advance
	}
	past.Fn = func() { order = append(order, "past") }
	soon.Fn = func() { order = append(order, "soon") }
	later.Fn = func() { order = append(order, "later") }
	w.Add(&a, 10*ms)
	if n := w.Advance(30 * ms); n != 3 {
		t.Fatalf("fired %d, want 3 (%v)", n, order)
	}
	if w.Len() != 1 || !later.Scheduled() {
		t.Fatal("later not scheduled")
	}
	if n := w.Advance(31 * ms); n != 1 {
		t.Fatalf("later: fired %d", n)
	}
	want := []string{"a", "past", "soon", "later"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v", order)
		}
	}
}

// TestAddFromFnMidAdvance adds, from a timer firing at an intermediate tick,
// a timer whose expiry lies between that tick and the Advance target.
func TestAddFromFnMidAdvance(t *testing.T) {
	w := New(time.Millisecond, 0)
	var a, b Timer
	var bAt int64
	a.Fn = func() { w.Add(&b, 70000*ms) } // beyond level 0 relative to tick 10
	b.Fn = func() { bAt = 1 }
	w.Add(&a, 10*ms)
	if n := w.Advance(69999 * ms); n != 1 || bAt != 0 {
		t.Fatalf("n=%d", n)
	}
	if n := w.Advance(70000 * ms); n != 1 || bAt != 1 {
		t.Fatalf("b: n=%d", n)
	}
}

func TestPastFiresOnNextAdvance(t *testing.T) {
	w := New(time.Millisecond, 0)
	w.Advance(1000*ms + 400000) // mid-tick
	var tm Timer
	f := 0
	tm.Fn = func() { f++ }
	w.Add(&tm, w.Now()) // tick start, in the past
	if nx, ok := w.Next(); !ok || nx != w.Now() {
		t.Fatalf("Next with due timer = %d,%v", nx, ok)
	}
	if w.Advance(1000*ms+400000) != 1 || f != 1 {
		t.Fatal("past timer did not fire on Advance with unchanged now")
	}
	w.Add(&tm, -5*ms)
	if w.Advance(0) != 1 || f != 2 { // backwards now is clamped
		t.Fatal("past timer did not fire on backwards Advance")
	}
}

func TestResetSemantics(t *testing.T) {
	w := New(time.Millisecond, 0)
	var tm Timer
	f := 0
	tm.Fn = func() { f++ }
	w.Add(&tm, 10*ms)
	w.Add(&tm, 100000*ms) // move to level 2
	w.Add(&tm, 20*ms)
	if w.Len() != 1 {
		t.Fatalf("Len = %d", w.Len())
	}
	if w.Advance(19*ms) != 0 {
		t.Fatal("fired at old time")
	}
	if w.Advance(20*ms) != 1 || f != 1 {
		t.Fatal("did not fire at reset time")
	}
	if w.Remove(&tm) {
		t.Fatal("Remove after fire returned true")
	}
}

func TestLenAndNilFn(t *testing.T) {
	w := New(time.Millisecond, 0)
	timers := make([]Timer, 1000)
	for i := range timers {
		w.Add(&timers[i], int64(i*97%5000+1)*ms)
	}
	if w.Len() != 1000 {
		t.Fatalf("Len = %d", w.Len())
	}
	for i := 0; i < 1000; i += 2 {
		if !w.Remove(&timers[i]) {
			t.Fatal("Remove failed")
		}
		if w.Remove(&timers[i]) {
			t.Fatal("double Remove succeeded")
		}
	}
	if w.Len() != 500 {
		t.Fatalf("Len = %d", w.Len())
	}
	if n := w.Advance(5001 * ms); n != 500 { // nil Fn fires as no-op
		t.Fatalf("fired %d", n)
	}
	if w.Len() != 0 {
		t.Fatalf("Len = %d", w.Len())
	}
	if _, ok := w.Next(); ok {
		t.Fatal("Next on empty wheel returned true")
	}
	var z Timer
	if z.Scheduled() || w.Remove(&z) {
		t.Fatal("zero Timer is scheduled")
	}
}

// TestNextHigherLevelEarlier is the case where a level-1 timer expires before
// a later-added level-0 timer.
func TestNextHigherLevelEarlier(t *testing.T) {
	w := New(time.Millisecond, 0)
	var a, b Timer
	w.Add(&a, 256*ms) // level 1 at tick 0
	w.Advance(100 * ms)
	w.Add(&b, 300*ms) // level 0 (delta 200)
	nx, ok := w.Next()
	if !ok || nx > 256*ms {
		t.Fatalf("Next = %d, want <= %d", nx, 256*ms)
	}
}

// TestNextProperty drives the wheel only through Next and checks that Next
// never overshoots the earliest expiry and that following it makes progress.
func TestNextProperty(t *testing.T) {
	f := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))
		w := New(time.Millisecond, r.Int63n(1<<40))
		timers := make([]Timer, 50)
		ats := make([]int64, len(timers))
		fired := 0
		for i := range timers {
			timers[i].Fn = func() { fired++ }
			var d int64
			if r.Intn(2) == 0 {
				d = r.Int63n(1000 * ms)
			} else {
				d = r.Int63n(1 << 20 * ms)
			}
			ats[i] = w.Now() + d + 1
			w.Add(&timers[i], ats[i])
		}
		for steps := 0; w.Len() > 0; steps++ {
			if steps > 10000 {
				t.Log("no progress")
				return false
			}
			nx, ok := w.Next()
			if !ok {
				return false
			}
			minFire := int64(1<<63 - 1)
			for i := range timers {
				if timers[i].Scheduled() {
					ft := w.base + timers[i].Expires()*w.tick // tick it becomes fireable
					if ft < minFire {
						minFire = ft
					}
				}
			}
			if nx > minFire {
				t.Logf("Next %d > earliest fire time %d", nx, minFire)
				return false
			}
			w.Advance(nx)
		}
		return fired == len(timers)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestZeroAllocs(t *testing.T) {
	w := New(time.Millisecond, 0)
	bg := make([]Timer, 10000)
	for i := range bg {
		w.Add(&bg[i], int64(i+1)*1000*ms)
	}
	var tm Timer
	tm.Fn = func() {}
	i := int64(0)
	if a := testing.AllocsPerRun(1000, func() {
		i++
		w.Add(&tm, w.Now()+(i%100000+1)*ms)
	}); a != 0 {
		t.Fatalf("Add allocates: %v", a)
	}
	if a := testing.AllocsPerRun(1000, func() {
		w.Add(&tm, w.Now()+5*ms)
		w.Remove(&tm)
	}); a != 0 {
		t.Fatalf("Remove allocates: %v", a)
	}
	ts := make([]Timer, 64)
	cnt := 0
	for k := range ts {
		ts[k].Fn = func() { cnt++ }
	}
	if a := testing.AllocsPerRun(1000, func() {
		now := w.Now()
		for k := range ts {
			w.Add(&ts[k], now+int64(k%7+1)*ms+int64(k)*70000*ms%(300*ms))
		}
		w.Advance(now + 400*ms)
	}); a != 0 {
		t.Fatalf("Advance allocates: %v", a)
	}
	if cnt == 0 {
		t.Fatal("no timers fired")
	}

	// The FnT path, as the root package uses it: a package-level function
	// recovers the embedding struct, so firing allocates nothing.
	fts := make([]fntTimer, 64)
	fntFired = 0
	for k := range fts {
		fts[k].t.FnT = fntFire
	}
	if a := testing.AllocsPerRun(1000, func() {
		now := w.Now()
		for k := range fts {
			w.Add(&fts[k].t, now+int64(k%7+1)*ms)
		}
		w.Advance(now + 10*ms)
	}); a != 0 {
		t.Fatalf("Advance with FnT allocates: %v", a)
	}
	if fntFired == 0 {
		t.Fatal("no FnT timers fired")
	}
}

// fntTimer embeds a Timer as its first field, the layout FnT callers rely on
// to convert the *Timer back to their own struct.
type fntTimer struct {
	t     Timer
	id    int
	fired int
}

// fntFired counts fntFire calls.
var fntFired int

// fntFire is a package-level FnT callback: storing it costs no allocation.
func fntFire(t *Timer) {
	o := (*fntTimer)(unsafe.Pointer(t))
	o.fired++
	fntFired++
}

func TestFnT(t *testing.T) {
	w := New(time.Millisecond, 0)
	var a, b, c, d fntTimer
	a.id, b.id, c.id, d.id = 1, 2, 3, 4
	var order []int
	record := func(t *Timer) {
		order = append(order, (*fntTimer)(unsafe.Pointer(t)).id)
	}
	// FnT receives the timer, which converts back to the outer struct.
	a.t.FnT = record
	// Fn takes precedence over FnT when both are set.
	b.t.Fn = func() { order = append(order, -2) }
	b.t.FnT = record
	// nil Fn and nil FnT fire as a no-op.
	// d re-adds itself from FnT, which must work like Fn.
	d.t.FnT = func(t *Timer) {
		record(t)
		if o := (*fntTimer)(unsafe.Pointer(t)); o.fired == 0 {
			o.fired++
			w.Add(t, 3*ms)
		}
	}
	w.Add(&a.t, 1*ms)
	w.Add(&b.t, 1*ms)
	w.Add(&c.t, 1*ms)
	w.Add(&d.t, 2*ms)
	if n := w.Advance(2 * ms); n != 4 {
		t.Fatalf("fired %d, want 4", n)
	}
	if want := []int{1, -2, 4}; !slices.Equal(order, want) {
		t.Fatalf("order %v, want %v", order, want)
	}
	if !d.t.Scheduled() || a.t.Scheduled() || c.t.Scheduled() {
		t.Fatal("scheduled state wrong after FnT")
	}
	if n := w.Advance(3 * ms); n != 1 || !slices.Equal(order, []int{1, -2, 4, 4}) {
		t.Fatalf("re-added FnT timer: fired %d, order %v", n, order)
	}
}

// BenchmarkAddRemove1M inserts and cancels a fresh timer with 1M live timers
// in the wheel (the timers_1m insert+cancel shape).
func BenchmarkAddRemove1M(b *testing.B) {
	w := New(time.Millisecond, 0)
	live := make([]Timer, 1<<20)
	r := rand.New(rand.NewSource(1))
	for i := range live {
		w.Add(&live[i], int64(r.Intn(1<<22)+1)*ms)
	}
	ds := make([]int64, 4096)
	for i := range ds {
		ds[i] = int64(r.Intn(60_000)+1) * ms
	}
	var tm Timer
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		w.Add(&tm, ds[i&4095])
		w.Remove(&tm)
	}
}

// BenchmarkReset1M reschedules randomly chosen live timers among 1M; this is
// dominated by cache misses on the timer and its list neighbours.
func BenchmarkReset1M(b *testing.B) {
	const n = 1 << 20
	w := New(time.Millisecond, 0)
	live := make([]Timer, n)
	r := rand.New(rand.NewSource(1))
	for i := range live {
		w.Add(&live[i], int64(r.Intn(60_000)+1)*ms)
	}
	ds := make([]int64, 4096)
	for i := range ds {
		ds[i] = int64(r.Intn(60_000)+1) * ms
	}
	x := uint32(2463534242) // xorshift32: random timer index without a table
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		w.Add(&live[x&(n-1)], ds[i&4095])
	}
}

// BenchmarkAdvanceFire measures the per-timer cost of firing: timers spread
// over 60 s are all fired by one Advance that walks the wheel through every
// non-empty slot and cascade. ns/op is per timer fired. The 1M case is dominated by cache misses on the timers and their
// closures; the 1K case shows the in-cache cost.
func BenchmarkAdvanceFire(b *testing.B) {
	for _, n := range []int{1 << 10, 1 << 20} {
		b.Run(map[int]string{1 << 10: "1K", 1 << 20: "1M"}[n], func(b *testing.B) {
			benchAdvanceFire(b, n)
		})
	}
}

func benchAdvanceFire(b *testing.B, n int) {
	timers := make([]Timer, n)
	fired := 0
	fn := func() { fired++ }
	for i := range timers {
		timers[i].Fn = fn
	}
	r := rand.New(rand.NewSource(1))
	offs := make([]int64, n)
	for i := range offs {
		offs[i] = int64(r.Intn(60_000)+1) * ms
	}
	b.ReportAllocs()
	b.ResetTimer()
	for done := 0; done < b.N; {
		b.StopTimer()
		w := New(time.Millisecond, 0)
		m := n
		if b.N-done < m {
			m = b.N - done
		}
		for i := 0; i < m; i++ {
			w.Add(&timers[i], offs[i])
		}
		b.StartTimer()
		w.Advance(61_000 * ms)
		done += m
	}
	if fired < b.N {
		b.Fatalf("fired %d < %d", fired, b.N)
	}
}
