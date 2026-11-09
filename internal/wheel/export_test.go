package wheel

// Now returns the start of the wheel's current tick as monotonic ns. Test
// helper.
func (w *Wheel) Now() int64 { return w.curTime() }

// Expires returns the absolute expiry in ticks since the wheel's base time.
// It is meaningful only while t is Scheduled. For a timer added with an at
// that was already in the past it is at most the wheel's current tick. Test
// helper.
func (t *Timer) Expires() int64 { return t.expires }
