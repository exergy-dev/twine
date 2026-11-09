package twine

import "testing"

// Loop.Metrics names its loop (it used to report LoopID 0 for every loop),
// and Engine.Metrics returns one snapshot per loop in loop order whose
// counters reflect real traffic.
func TestMetricsSnapshots(t *testing.T) {
	e := startEngine(t, Loops(3), ReusePort(false))
	for i, l := range e.Loops() {
		if got := l.Metrics().LoopID; got != i {
			t.Errorf("Loops()[%d].Metrics().LoopID = %d", i, got)
		}
	}
	lst := listen(t, e, echoFactory())
	for i := 0; i < 3; i++ {
		echoOnce(t, dialTCP(t, lst.Addr().String()), []byte("metrics"))
	}
	snaps := e.Metrics()
	if len(snaps) != 3 {
		t.Fatalf("Engine.Metrics returned %d snapshots, want 3", len(snaps))
	}
	var accepts, reads, conns, iters uint64
	for i, s := range snaps {
		if s.LoopID != i {
			t.Errorf("snapshot %d has LoopID %d", i, s.LoopID)
		}
		accepts += s.Accepts
		reads += s.ReadBytes
		conns += s.Connections
		iters += s.Iterations
	}
	if accepts != 3 || conns != 3 {
		t.Errorf("Accepts = %d, Connections = %d, want 3 and 3", accepts, conns)
	}
	if reads != 3*uint64(len("metrics")) {
		t.Errorf("ReadBytes = %d, want %d", reads, 3*len("metrics"))
	}
	if iters == 0 {
		t.Error("Iterations = 0")
	}
}
