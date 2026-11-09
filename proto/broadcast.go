package proto

import (
	"sync"

	"github.com/exergy-dev/twine"
)

// Broadcast is a loop-aware fan-out group (design Section 8 rule 4). Members
// are grouped by their Loop; Send copies the message once and schedules one
// CallFromThread per loop that has members, and that task writes the shared
// read-only copy to every member of its loop. All methods are safe from any
// goroutine.
//
// Membership is read on each loop when its task runs, not when Send is
// called, so a member removed before delivery does not receive the message.
// Members should be removed in their ConnectionLost (which runs on their
// loop). A member that closed without being removed is harmless: Write on a
// closed transport is a no-op, and the core's transport handles are
// generation-checked, so a stale handle never reaches a later connection
// that reuses the same internal slot. It does, however, keep its entry
// (and its memory) until removed.
//
// Messages sent from one goroutine are delivered to each member in Send
// order. There is no ordering between messages sent from different
// goroutines.
type Broadcast struct {
	mu     sync.Mutex
	groups map[*twine.Loop]*bgroup
	n      int
}

// bgroup is the membership of one loop. members and index are guarded by
// Broadcast.mu; scratch is touched only on loop.
type bgroup struct {
	b       *Broadcast
	loop    *twine.Loop
	members []twine.Transport
	index   map[twine.Transport]int
	scratch []twine.Transport
}

// NewBroadcast returns an empty group.
func NewBroadcast() *Broadcast {
	return &Broadcast{groups: make(map[*twine.Loop]*bgroup)}
}

// Add adds t to the group. Adding a member twice is a no-op.
func (b *Broadcast) Add(t twine.Transport) {
	l := t.Loop()
	b.mu.Lock()
	defer b.mu.Unlock()
	g := b.groups[l]
	if g == nil {
		g = &bgroup{b: b, loop: l, index: make(map[twine.Transport]int)}
		b.groups[l] = g
	}
	if _, ok := g.index[t]; ok {
		return
	}
	g.index[t] = len(g.members)
	g.members = append(g.members, t)
	b.n++
}

// Remove removes t from the group. Removing a non-member is a no-op.
func (b *Broadcast) Remove(t twine.Transport) {
	b.mu.Lock()
	defer b.mu.Unlock()
	g := b.groups[t.Loop()]
	if g == nil {
		return
	}
	i, ok := g.index[t]
	if !ok {
		return
	}
	last := len(g.members) - 1
	if i != last {
		g.members[i] = g.members[last]
		g.index[g.members[i]] = i
	}
	g.members[last] = nil
	g.members = g.members[:last]
	delete(g.index, t)
	b.n--
}

// Len returns the number of members.
func (b *Broadcast) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n
}

// Send delivers a copy of msg to every member. msg may be reused as soon as
// Send returns: it is copied once into a buffer shared, read-only, by every
// loop's delivery task. Send never blocks on I/O.
func (b *Broadcast) Send(msg []byte) {
	if len(msg) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.n == 0 {
		return
	}
	shared := append([]byte(nil), msg...)
	for _, g := range b.groups {
		if len(g.members) == 0 {
			continue
		}
		g.loop.CallFromThread(func() { g.deliver(shared) })
	}
}

// deliver runs on g.loop. It snapshots the current membership under the
// lock and writes without it, so a Write side effect (a producer callback
// that calls Remove, for example) cannot deadlock.
func (g *bgroup) deliver(msg []byte) {
	g.b.mu.Lock()
	g.scratch = append(g.scratch[:0], g.members...)
	g.b.mu.Unlock()
	for _, t := range g.scratch {
		t.Write(msg)
	}
	clear(g.scratch)
}
