//! Twine internal crate `twine-wheel`. See `rust/docs/INTERNALS.md` for the
//! contract this crate implements. Not part of the public API.
//!
//! A hierarchical timing wheel with 4 levels of 256 slots. Level `L` slots are
//! `256^L` ticks wide, so the horizon is `256^4` ticks; timers further out are
//! parked in the top level and re-placed on cascade until they fit.
//!
//! # Storage
//!
//! Nodes live in an index-linked arena (`Vec<Node<K>>` plus a free list of
//! indices). Slot lists are doubly linked through `u32` indices; the heads and
//! tails of the 1024 wheel slots and of one extra "late" list (timers added
//! with `at` already in the past) are kept in flat arrays with a `NONE`
//! sentinel, and every node records which list it is in so `remove` can fix
//! the head or tail. Per-level occupancy bitmaps let `advance` and `next` jump
//! over empty ticks.
//!
//! Expired timers stay in the arena until [`Wheel::pop_due`] hands them out.
//! The `due` queue holds `(index, ticket)` entries; an entry is valid only
//! while the node is still in the due state with the same ticket, so a timer
//! removed or reset between pops simply leaves a stale entry that is skipped.
//! (This differs from the sketch in the contract, which stores payloads in
//! `due`; keeping payloads in the arena is what makes remove-between-pops O(1).)
//!
//! Time is measured in ticks since the base time. A timer for absolute time
//! `at` has expiry tick `ceil((at - base) / tick)` and fires during the first
//! `advance(now)` whose tick `floor((now - base) / tick)` reaches it: never
//! early, at most one tick late.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::time::Duration;

#[cfg(test)]
#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

const LEVELS: usize = 4;
const SLOT_BITS: u32 = 8;
const SLOTS: usize = 1 << SLOT_BITS;
const SLOT_MASK: u64 = SLOTS as u64 - 1;
/// Horizon in ticks.
const HORIZON: u64 = 1 << (LEVELS as u32 * SLOT_BITS);
/// Clamp for absurd distances into the future, in ns.
const MAX_AHEAD_NS: u64 = 1 << 61;
/// Index of the list of timers that were already expired when added.
const LATE: usize = LEVELS * SLOTS;
/// Number of lists (wheel slots plus the late list).
const LISTS: usize = LATE + 1;
/// Sentinel "no node" index.
const NONE: u32 = u32::MAX;
/// `Node::loc` value for a free node.
const LOC_FREE: u16 = u16::MAX;
/// `Node::loc` value for an expired node waiting in the due queue.
const LOC_DUE: u16 = u16::MAX - 1;

/// Handle to a scheduled timer. `Copy`. Stale handles are harmless.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerId {
    idx: u32,
    gen: u32,
}

struct Node<K> {
    kind: Option<K>,
    next: u32,
    prev: u32,
    gen: u32,
    /// List index (`0..LISTS`), `LOC_DUE` or `LOC_FREE`.
    loc: u16,
    /// Expiry tick since base (valid while scheduled in a list).
    expires: u64,
    /// Absolute expiry as requested by the caller, in ns.
    at_ns: u64,
    /// Due-queue ticket (valid while `loc == LOC_DUE`).
    ticket: u64,
}

/// A hierarchical timing wheel holding timers with payload `K`.
///
/// Not thread-safe. `add`, `remove`, `reset` are O(1) and allocate nothing
/// once the arena has warmed up.
pub struct Wheel<K> {
    heads: [u32; LISTS],
    tails: [u32; LISTS],
    occ: [[u64; SLOTS / 64]; LEVELS],
    nodes: Vec<Node<K>>,
    free: Vec<u32>,
    due: Vec<(u32, u64)>,
    due_head: usize,
    due_live: usize,
    next_ticket: u64,
    tick_ns: u64,
    base_ns: u64,
    cur: u64,
    last_ns: u64,
    len: usize,
}

impl<K> Wheel<K> {
    /// Creates a wheel whose tick 0 is at monotonic nanosecond time `now_ns`.
    /// A zero tick is replaced by 1 ms.
    pub fn new(tick: Duration, now_ns: u64) -> Wheel<K> {
        let tick_ns = u64::try_from(tick.as_nanos()).unwrap_or(u64::MAX);
        let tick_ns = if tick_ns == 0 { 1_000_000 } else { tick_ns };
        Wheel {
            heads: [NONE; LISTS],
            tails: [NONE; LISTS],
            occ: [[0; SLOTS / 64]; LEVELS],
            nodes: Vec::new(),
            free: Vec::new(),
            due: Vec::new(),
            due_head: 0,
            due_live: 0,
            next_ticket: 0,
            tick_ns,
            base_ns: now_ns,
            cur: 0,
            last_ns: now_ns,
            len: 0,
        }
    }

    /// Schedules `kind` to fire when the wheel is advanced to or past `at_ns`
    /// (absolute monotonic ns). A time in the past fires on the next advance. O(1).
    pub fn add(&mut self, at_ns: u64, kind: K) -> TimerId {
        let idx = self.alloc_node(kind);
        self.len += 1;
        self.schedule(idx, at_ns);
        TimerId {
            idx,
            gen: self.nodes[idx as usize].gen,
        }
    }

    /// Unschedules the timer and returns its payload; None if it was not
    /// scheduled (fired, removed, or stale). O(1).
    pub fn remove(&mut self, id: TimerId) -> Option<K> {
        if !self.contains(id) {
            return None;
        }
        let loc = self.nodes[id.idx as usize].loc;
        if loc == LOC_DUE {
            self.due_live -= 1;
        } else {
            self.unlink(id.idx);
        }
        self.len -= 1;
        self.free_node(id.idx)
    }

    /// Reschedules a live timer to `at_ns`, keeping its id and payload. Returns
    /// false if the id is not live. O(1).
    pub fn reset(&mut self, id: TimerId, at_ns: u64) -> bool {
        if !self.contains(id) {
            return false;
        }
        if self.nodes[id.idx as usize].loc == LOC_DUE {
            self.due_live -= 1;
        } else {
            self.unlink(id.idx);
        }
        self.schedule(id.idx, at_ns);
        true
    }

    /// Advances the wheel to `now_ns`, cascading levels and moving every expired
    /// timer to the due list in expiry order (FIFO insertion order within a
    /// tick). Returns how many expired. Does not call user code. A `now_ns`
    /// earlier than a previous call is treated as that previous time.
    ///
    /// Timers already in the due list are kept (the list is appended to).
    pub fn advance(&mut self, now_ns: u64) -> usize {
        let before = self.due.len();
        self.drain_list_to_due(LATE);
        if now_ns > self.last_ns {
            self.last_ns = now_ns;
            let target = (now_ns - self.base_ns) / self.tick_ns;
            while self.cur < target {
                let n = match self.next_event() {
                    Some(n) if n <= target => n,
                    _ => {
                        self.cur = target;
                        break;
                    }
                };
                self.cur = n;
                for level in (1..LEVELS).rev() {
                    if n & ((1u64 << (level as u32 * SLOT_BITS)) - 1) == 0 {
                        let s = ((n >> (level as u32 * SLOT_BITS)) & SLOT_MASK) as usize;
                        self.cascade(level, s);
                    }
                }
                self.drain_list_to_due((n & SLOT_MASK) as usize);
            }
        }
        self.due.len() - before
    }

    /// Pops the next due timer (after `advance`). The caller fires it; it may
    /// add/remove/reset timers between pops.
    pub fn pop_due(&mut self) -> Option<(TimerId, K)> {
        if self.due_live == 0 && !self.due.is_empty() {
            // Only stale entries remain (removed/reset while due): drop them so
            // the queue cannot grow past the node count and reallocate.
            self.due.clear();
            self.due_head = 0;
            return None;
        }
        while self.due_head < self.due.len() {
            let (idx, ticket) = self.due[self.due_head];
            self.due_head += 1;
            if self.due_head == self.due.len() {
                self.due.clear();
                self.due_head = 0;
            }
            let node = &self.nodes[idx as usize];
            if node.loc != LOC_DUE || node.ticket != ticket {
                continue;
            }
            let id = TimerId { idx, gen: node.gen };
            self.due_live -= 1;
            self.len -= 1;
            let kind = self.free_node(idx).expect("scheduled node has payload");
            return Some((id, kind));
        }
        None
    }

    /// A monotonic ns time at or before the earliest scheduled timer's expiry
    /// (the loop uses it as a poll timeout bound); None when nothing is
    /// scheduled. If a timer is already due, returns [`Wheel::now_ns`].
    ///
    /// The bound is the next tick at which a non-empty level-0 slot fires or a
    /// non-empty higher-level slot cascades, found from the occupancy bitmaps
    /// (at most 16 words), so it is never later than the next level-0 wrap
    /// when level 0 is empty.
    pub fn next(&self) -> Option<u64> {
        if self.len == 0 {
            return None;
        }
        if self.due_live > 0 || self.heads[LATE] != NONE {
            return Some(self.now_ns());
        }
        let n = self.next_event()?;
        Some(self.base_ns.saturating_add(n.saturating_mul(self.tick_ns)))
    }

    /// Reports whether id is live (scheduled, not yet popped).
    pub fn contains(&self, id: TimerId) -> bool {
        self.nodes
            .get(id.idx as usize)
            .is_some_and(|n| n.gen == id.gen && n.loc != LOC_FREE)
    }

    /// Absolute expiry of a live timer, as passed to `add` or `reset`.
    pub fn expires(&self, id: TimerId) -> Option<u64> {
        if self.contains(id) {
            Some(self.nodes[id.idx as usize].at_ns)
        } else {
            None
        }
    }

    /// Number of live timers, including expired ones not yet popped.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Reports whether no timer is live.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Start of the wheel's current tick as monotonic ns.
    pub fn now_ns(&self) -> u64 {
        self.base_ns
            .saturating_add(self.cur.saturating_mul(self.tick_ns))
    }

    /// The wheel's tick length.
    pub fn tick(&self) -> Duration {
        Duration::from_nanos(self.tick_ns)
    }

    // ---- arena -----------------------------------------------------------

    fn alloc_node(&mut self, kind: K) -> u32 {
        if let Some(idx) = self.free.pop() {
            let n = &mut self.nodes[idx as usize];
            n.kind = Some(kind);
            return idx;
        }
        let idx = u32::try_from(self.nodes.len()).expect("timer arena exhausted");
        self.nodes.push(Node {
            kind: Some(kind),
            next: NONE,
            prev: NONE,
            gen: 0,
            loc: LOC_FREE,
            expires: 0,
            at_ns: 0,
            ticket: 0,
        });
        // Keep the free list and due queue able to hold every node so that
        // remove and advance never allocate after the arena has warmed up.
        let cap = self.nodes.capacity();
        if self.free.capacity() < cap {
            self.free.reserve_exact(cap - self.free.len());
        }
        if self.due.capacity() < cap {
            self.due.reserve_exact(cap - self.due.len());
        }
        idx
    }

    /// Returns the payload and puts the node on the free list, bumping its
    /// generation so outstanding ids die.
    fn free_node(&mut self, idx: u32) -> Option<K> {
        let n = &mut self.nodes[idx as usize];
        n.loc = LOC_FREE;
        n.gen = n.gen.wrapping_add(1);
        let kind = n.kind.take();
        self.free.push(idx);
        kind
    }

    // ---- lists -----------------------------------------------------------

    fn push_back(&mut self, list: usize, idx: u32) {
        let tail = self.tails[list];
        {
            let n = &mut self.nodes[idx as usize];
            n.prev = tail;
            n.next = NONE;
            n.loc = list as u16;
        }
        if tail == NONE {
            self.heads[list] = idx;
        } else {
            self.nodes[tail as usize].next = idx;
        }
        self.tails[list] = idx;
        if list < LATE {
            let (level, s) = (list >> SLOT_BITS, list & SLOT_MASK as usize);
            self.occ[level][s >> 6] |= 1 << (s & 63);
        }
    }

    fn unlink(&mut self, idx: u32) {
        let (prev, next, list) = {
            let n = &self.nodes[idx as usize];
            (n.prev, n.next, n.loc as usize)
        };
        if prev == NONE {
            self.heads[list] = next;
        } else {
            self.nodes[prev as usize].next = next;
        }
        if next == NONE {
            self.tails[list] = prev;
        } else {
            self.nodes[next as usize].prev = prev;
        }
        if list < LATE && self.heads[list] == NONE {
            let (level, s) = (list >> SLOT_BITS, list & SLOT_MASK as usize);
            self.occ[level][s >> 6] &= !(1 << (s & 63));
        }
    }

    // ---- scheduling ------------------------------------------------------

    /// Places an unlinked live node for absolute time `at_ns`.
    fn schedule(&mut self, idx: u32, at_ns: u64) {
        self.nodes[idx as usize].at_ns = at_ns;
        if at_ns <= self.last_ns {
            self.nodes[idx as usize].expires = self.cur;
            self.push_back(LATE, idx);
            return;
        }
        let d = (at_ns - self.last_ns).min(MAX_AHEAD_NS);
        let off = (self.last_ns - self.base_ns) + d;
        self.nodes[idx as usize].expires = off.div_ceil(self.tick_ns);
        self.place(idx);
    }

    /// Links the node into the slot for its expiry tick relative to `cur`.
    fn place(&mut self, idx: u32) {
        let mut e = self.nodes[idx as usize].expires;
        let delta = e.saturating_sub(self.cur);
        let level = if delta < 1 << SLOT_BITS {
            0
        } else if delta < 1 << (2 * SLOT_BITS) {
            1
        } else if delta < 1 << (3 * SLOT_BITS) {
            2
        } else if delta < HORIZON {
            3
        } else {
            // Beyond the horizon: park in the last top-level slot reachable
            // within it; the cascade re-places the timer.
            e = self.cur + HORIZON - 1;
            3
        };
        let s = ((e >> (level as u32 * SLOT_BITS)) & SLOT_MASK) as usize;
        self.push_back(level * SLOTS + s, idx);
    }

    /// Moves every node of `list` to the due queue, preserving order.
    fn drain_list_to_due(&mut self, list: usize) {
        let mut i = std::mem::replace(&mut self.heads[list], NONE);
        self.tails[list] = NONE;
        if list < LATE {
            let (level, s) = (list >> SLOT_BITS, list & SLOT_MASK as usize);
            self.occ[level][s >> 6] &= !(1 << (s & 63));
        }
        while i != NONE {
            let ticket = self.next_ticket;
            self.next_ticket += 1;
            let n = &mut self.nodes[i as usize];
            n.loc = LOC_DUE;
            n.ticket = ticket;
            let next = n.next;
            self.due.push((i, ticket));
            self.due_live += 1;
            i = next;
        }
    }

    /// Re-places every timer in a higher-level slot relative to `cur`.
    fn cascade(&mut self, level: usize, s: usize) {
        let list = level * SLOTS + s;
        let mut i = std::mem::replace(&mut self.heads[list], NONE);
        self.tails[list] = NONE;
        self.occ[level][s >> 6] &= !(1 << (s & 63));
        while i != NONE {
            let next = self.nodes[i as usize].next;
            self.place(i);
            i = next;
        }
    }

    /// Smallest tick `> cur` at which a non-empty level-0 slot fires or a
    /// non-empty higher-level slot cascades.
    fn next_event(&self) -> Option<u64> {
        let mut best: Option<u64> = None;
        for level in 0..LEVELS {
            let shift = level as u32 * SLOT_BITS;
            let b = self.cur >> shift;
            if let Some(k) = next_set(&self.occ[level], ((b + 1) & SLOT_MASK) as usize) {
                let at = (b + 1 + k as u64) << shift;
                best = Some(best.map_or(at, |x| x.min(at)));
            }
        }
        best
    }
}

/// Circular distance from `from` to the first set bit at or after it in the
/// 256-bit bitmap, or None if the bitmap is empty.
fn next_set(bm: &[u64; SLOTS / 64], from: usize) -> Option<usize> {
    let (wi, bi) = (from >> 6, from & 63);
    let x = bm[wi] >> bi;
    if x != 0 {
        return Some(x.trailing_zeros() as usize);
    }
    for i in 1..=bm.len() {
        let j = (wi + i) & (bm.len() - 1);
        if bm[j] != 0 {
            let pos = (j << 6) + bm[j].trailing_zeros() as usize;
            return Some((pos.wrapping_sub(from)) & SLOT_MASK as usize);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Instant;

    const TICK: u64 = 1000;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn wheel() -> Wheel<u32> {
        Wheel::new(Duration::from_nanos(TICK), 0)
    }

    fn drain(w: &mut Wheel<u32>) -> Vec<u32> {
        let mut v = Vec::new();
        while let Some((_, k)) = w.pop_due() {
            v.push(k);
        }
        v
    }

    #[test]
    fn never_early_at_most_one_tick_late() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut w = wheel();
        // Reference: (expiry tick, seq) -> payload.
        let mut reference: BTreeMap<(u64, u32), u32> = BTreeMap::new();
        let mut ats: Vec<u64> = Vec::new();
        let mut now = 0u64;
        let mut seq = 0u32;
        for _round in 0..4000 {
            for _ in 0..rng.below(8) {
                let at = match rng.below(10) {
                    0 => now.saturating_sub(rng.below(5000)),
                    _ => now + rng.below((1 << 20) * TICK),
                };
                // A time not after the last advance fires on the next advance.
                let e = if at <= now { 0 } else { at.div_ceil(TICK) };
                w.add(at, seq);
                reference.insert((e, seq), seq);
                ats.push(at);
                seq += 1;
            }
            now += match rng.below(4) {
                0 => 0,
                1 => rng.below(2 * TICK),
                2 => rng.below(300 * TICK),
                _ => rng.below(70_000 * TICK),
            };
            let n = w.advance(now);
            let got = drain(&mut w);
            assert_eq!(n, got.len());
            let target = now / TICK;
            let want: Vec<u32> = reference
                .range(..(target + 1, 0))
                .map(|(_, &p)| p)
                .collect();
            for p in &want {
                reference.retain(|_, v| v != p);
            }
            for &k in &got {
                assert!(ats[k as usize] <= now, "fired early");
            }
            let mut a = got.clone();
            let mut b = want;
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "fired set differs at now={now}");
        }
        // Flush the rest.
        now += (1 << 22) * TICK;
        w.advance(now);
        assert_eq!(drain(&mut w).len(), reference.len());
        assert!(w.is_empty());
    }

    #[test]
    fn fires_in_expiry_order_fifo() {
        let mut w = wheel();
        w.add(5 * TICK, 1);
        w.add(3 * TICK, 2);
        w.add(5 * TICK, 3);
        w.add(300 * TICK, 4);
        w.add(300 * TICK, 5);
        w.advance(1000 * TICK);
        assert_eq!(drain(&mut w), vec![2, 1, 3, 4, 5]);
    }

    fn boundary(delta: u64) {
        let mut w = wheel();
        let id = w.add(delta * TICK, 7);
        assert_eq!(w.advance((delta - 1) * TICK), 0, "early at {delta}");
        assert!(w.contains(id));
        assert_eq!(w.advance(delta * TICK), 1, "missed at {delta}");
        assert_eq!(drain(&mut w), vec![7]);
        assert!(w.is_empty());
    }

    #[test]
    fn cascade_boundaries() {
        for d in [
            1u64, 255, 256, 257, 65_535, 65_536, 65_537, 16_777_215, 16_777_216, 16_777_217,
        ] {
            boundary(d);
        }
    }

    #[test]
    fn horizon_clamp() {
        let mut w = wheel();
        let far = (1u64 << 33) + 17;
        w.add(far * TICK, 1);
        assert_eq!(w.advance((far - 1) * TICK), 0);
        assert_eq!(w.advance(far * TICK), 1);
        assert_eq!(drain(&mut w), vec![1]);
        let mut w = wheel();
        w.add(u64::MAX, 2);
        assert_eq!(w.advance(1 << 50), 0);
        assert_eq!(w.len(), 1);
    }

    #[test]
    fn remove_between_pops() {
        let mut w = wheel();
        let a = w.add(10 * TICK, 1);
        let b = w.add(10 * TICK, 2);
        let c = w.add(10 * TICK, 3);
        assert_eq!(w.advance(10 * TICK), 3);
        let (id, k) = w.pop_due().unwrap();
        assert_eq!((id, k), (a, 1));
        assert_eq!(w.remove(c), Some(3));
        assert!(w.contains(b));
        assert_eq!(w.pop_due().map(|x| x.1), Some(2));
        assert_eq!(w.pop_due(), None);
        assert!(w.is_empty());
    }

    #[test]
    fn reset_due_timer_between_pops() {
        let mut w = wheel();
        let a = w.add(10 * TICK, 1);
        let b = w.add(10 * TICK, 2);
        w.advance(10 * TICK);
        assert!(w.reset(b, 50 * TICK));
        assert_eq!(drain(&mut w), vec![1]);
        assert!(!w.contains(a));
        assert!(w.contains(b));
        w.advance(50 * TICK);
        assert_eq!(drain(&mut w), vec![2]);
    }

    #[test]
    fn add_in_past_during_due_phase_fires_next_advance() {
        let mut w = wheel();
        w.add(5 * TICK, 1);
        w.advance(10 * TICK);
        let (_, k) = w.pop_due().unwrap();
        assert_eq!(k, 1);
        let id = w.add(2 * TICK, 99);
        assert_eq!(w.pop_due(), None, "must not fire re-entrantly");
        assert!(w.contains(id));
        assert_eq!(w.next(), Some(w.now_ns()));
        assert_eq!(w.advance(10 * TICK), 1);
        assert_eq!(drain(&mut w), vec![99]);
    }

    #[test]
    fn stale_id_after_remove() {
        let mut w = wheel();
        let id = w.add(5 * TICK, 1);
        assert_eq!(w.remove(id), Some(1));
        assert_eq!(w.remove(id), None);
        assert!(!w.contains(id));
        assert_eq!(w.expires(id), None);
        assert!(!w.reset(id, 9 * TICK));
        w.advance(100 * TICK);
        assert_eq!(w.pop_due(), None);
    }

    #[test]
    fn stale_id_after_fire() {
        let mut w = wheel();
        let id = w.add(5 * TICK, 1);
        w.advance(5 * TICK);
        assert!(w.contains(id));
        let (pid, _) = w.pop_due().unwrap();
        assert_eq!(pid, id);
        assert!(!w.contains(id));
        assert_eq!(w.remove(id), None);
    }

    #[test]
    fn generation_reuse() {
        let mut w = wheel();
        let old = w.add(5 * TICK, 1);
        w.remove(old);
        let new = w.add(7 * TICK, 2);
        assert_eq!(old.idx, new.idx);
        assert_ne!(old.gen, new.gen);
        assert!(!w.contains(old));
        assert_eq!(w.remove(old), None);
        assert!(!w.reset(old, 1));
        assert_eq!(w.expires(new), Some(7 * TICK));
        assert_eq!(w.remove(new), Some(2));
    }

    #[test]
    fn reset_moves_timer() {
        let mut w = wheel();
        let id = w.add(10 * TICK, 1);
        assert!(w.reset(id, 1000 * TICK));
        assert_eq!(w.expires(id), Some(1000 * TICK));
        assert_eq!(w.advance(500 * TICK), 0);
        assert!(w.reset(id, 600 * TICK));
        assert_eq!(w.advance(599 * TICK), 0);
        assert_eq!(w.advance(600 * TICK), 1);
        assert_eq!(w.pop_due(), Some((id, 1)));
    }

    #[test]
    fn next_bounds_earliest_expiry() {
        let mut w = wheel();
        assert_eq!(w.next(), None);
        let mut rng = Rng(12345);
        for round in 0..300 {
            let mut w2 = wheel();
            let base = rng.below(100_000) * TICK;
            w2.advance(base);
            let mut earliest = u64::MAX;
            for i in 0..1 + rng.below(4) {
                let at = base + 1 + rng.below(if round % 3 == 0 { 300 } else { 5_000_000 }) * TICK;
                earliest = earliest.min(at.div_ceil(TICK) * TICK);
                w2.add(at, i as u32);
            }
            let mut fired = 0;
            let mut steps = 0;
            while fired == 0 {
                let n = w2.next().expect("timers scheduled");
                assert!(n <= earliest, "next {n} after earliest {earliest}");
                assert!(n >= w2.now_ns());
                let before = w2.now_ns();
                fired = w2.advance(n.max(before + 1));
                steps += 1;
                assert!(steps < 100);
            }
            drain(&mut w2);
        }
        w.add(100 * TICK, 1);
        assert!(w.next().unwrap() <= 100 * TICK);
    }

    #[test]
    fn second_advance_keeps_undrained_timers() {
        let mut w = wheel();
        w.add(5 * TICK, 1);
        w.add(6 * TICK, 2);
        w.add(9 * TICK, 3);
        assert_eq!(w.advance(5 * TICK), 1);
        assert_eq!(w.advance(9 * TICK), 2);
        assert_eq!(drain(&mut w), vec![1, 2, 3]);
    }

    #[test]
    fn alloc_gates() {
        let mut w = Wheel::<u64>::new(Duration::from_nanos(TICK), 0);
        // Warm-up: grow the arena, free list and due queue.
        let ids: Vec<TimerId> = (0..1000).map(|i| w.add(i * TICK + 1, i)).collect();
        for id in ids {
            w.remove(id);
        }
        w.advance(10);
        let n = twine_alloccount::allocs(|| {
            for i in 0..1000u64 {
                let id = w.add(5000 * TICK + i * 997, i);
                w.remove(id);
            }
        });
        assert_eq!(n, 0, "add+remove allocate");

        for i in 0..1000u64 {
            w.add(20_000 * TICK + i * 10 * TICK, i);
        }
        w.advance(20_000 * TICK);
        w.advance(40_000 * TICK);
        while w.pop_due().is_some() {}
        let mut now = 40_000 * TICK;
        let n = twine_alloccount::allocs(|| {
            for i in 0..500u64 {
                // Reuse nodes; nothing here exceeds warmed capacity.
                w.add(now + 3 * TICK + i, i);
            }
            now += 100_000 * TICK;
            let c = w.advance(now);
            let mut p = 0;
            while w.pop_due().is_some() {
                p += 1;
            }
            assert_eq!(c, p);
        });
        assert_eq!(n, 0, "advance+pop_due allocate");
    }

    #[test]
    #[ignore = "timing gate: run with --release -- --ignored"]
    fn timing_gate_add_remove() {
        const N: usize = 1_000_000;
        let mut w = Wheel::<u64>::new(Duration::from_millis(1), 0);
        let mut rng = Rng(42);
        let ats: Vec<u64> = (0..N).map(|_| 1 + rng.below(1 << 30)).collect();
        let mut ids = Vec::with_capacity(N);
        for &at in &ats {
            ids.push(w.add(at, 0));
        }
        for id in ids.drain(..) {
            w.remove(id);
        }
        let t = Instant::now();
        for &at in &ats {
            ids.push(w.add(at, 0));
        }
        for id in ids.drain(..) {
            w.remove(id);
        }
        let per = t.elapsed().as_nanos() as f64 / N as f64;
        println!("wheel add+remove: {per:.1} ns per pair ({N} timers)");
        assert!(per <= 50.0, "{per} ns > 50 ns");

        // Sanity: 1M-timer advance sweep.
        for &at in &ats {
            w.add(at, 0);
        }
        let t = Instant::now();
        let mut fired = 0;
        let mut now = 0u64;
        while now < (1 << 30) * 1_000_000 + 2_000_000 && fired < N {
            now += 1_000_000 << 10;
            fired += w.advance(now);
            while w.pop_due().is_some() {}
        }
        println!(
            "wheel advance sweep: {fired} fired in {:.1} ms",
            t.elapsed().as_secs_f64() * 1e3
        );
        assert_eq!(fired, N);
    }
}
