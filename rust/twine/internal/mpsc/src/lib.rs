//! Twine internal crate `twine-mpsc`. See `rust/docs/INTERNALS.md` for the
//! contract this crate implements. Not part of the public API.
//!
//! A bounded, lock-free, multi-producer single-consumer queue: Dmitry Vyukov's
//! bounded MPMC queue specialised to one consumer. Every slot carries an atomic
//! sequence number; producers claim a position by CAS on the tail counter; the
//! consumer advances the head counter with a plain store because it is the only
//! writer of it.
//!
//! Slot sequence protocol, for a slot at index `pos & mask` and capacity
//! `cap = mask + 1`:
//!
//! * `seq == pos`: free for the producer that claims position `pos`;
//! * `seq == pos + 1`: filled, ready for the consumer at position `pos`;
//! * after the consumer reads it, `seq` becomes `pos + cap` (free for the next lap).
//!
//! Memory orderings: the producer writes the value, then stores `seq = pos + 1`
//! with `Release`; the consumer loads `seq` with `Acquire`, so the value write
//! happens-before the consumer's read. Symmetrically the consumer reads the
//! value, then stores `seq = pos + cap` with `Release`; a producer's `Acquire`
//! load of that `seq` makes the consumer's read happen-before the producer's
//! next write into the slot. The tail CAS only arbitrates between producers and
//! publishes no data, so it is `Relaxed`. The head is written only by the
//! consumer; its store is `Release` and loads by `len` are `Acquire`, which is
//! all `len` (an approximation) needs.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(test)]
#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

/// Pads (and aligns) a value to its own cache line to avoid false sharing.
#[repr(align(64))]
struct CachePadded<T>(T);

/// One queue slot: a sequence number and storage for one value.
struct Slot<T> {
    seq: AtomicUsize,
    val: UnsafeCell<MaybeUninit<T>>,
}

/// Shared queue state, owned jointly by all producers and the consumer.
struct Inner<T> {
    /// Next position to be claimed by a producer.
    tail: CachePadded<AtomicUsize>,
    /// Next position to be consumed; written only by the consumer.
    head: CachePadded<AtomicUsize>,
    mask: usize,
    slots: Box<[Slot<T>]>,
}

// SAFETY: values of type `T` are moved between threads through the slots, which
// is sound when `T: Send`. The `UnsafeCell` contents are only touched by the
// thread that owns the slot under the sequence protocol (see the crate docs):
// a producer after winning the tail CAS and before publishing `seq = pos + 1`,
// or the single consumer after observing `seq == pos + 1` and before
// publishing `seq = pos + cap`. Everything else in `Inner` is atomic or
// immutable.
unsafe impl<T: Send> Send for Inner<T> {}
// SAFETY: as above; shared access never reads or writes slot storage without
// owning the slot via the sequence protocol, so `&Inner<T>` is safe to share.
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    fn len(&self) -> usize {
        // Head first: tail is monotonic, so the later tail load is >= head.
        let h = self.head.0.load(Ordering::Acquire);
        let t = self.tail.0.load(Ordering::Acquire);
        t.wrapping_sub(h).min(self.capacity())
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut pos = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        while pos != tail {
            let slot = &mut self.slots[pos & self.mask];
            // With `&mut self` no push is in flight, so every position in
            // head..tail is filled.
            debug_assert_eq!(*slot.seq.get_mut(), pos.wrapping_add(1));
            // SAFETY: the slot at a position in head..tail holds an
            // initialised value (seq == pos + 1) that the consumer has not
            // taken; we have exclusive access, and each position is visited
            // once, so the value is dropped exactly once.
            unsafe { slot.val.get_mut().assume_init_drop() };
            pos = pos.wrapping_add(1);
        }
    }
}

/// Creates a queue with capacity rounded up to a power of two (minimum 2).
///
/// # Panics
///
/// Panics if the rounded capacity overflows `usize` or is too large to allocate.
pub fn channel<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let n = capacity
        .max(2)
        .checked_next_power_of_two()
        .expect("twine-mpsc: capacity overflow");
    let slots: Box<[Slot<T>]> = (0..n)
        .map(|i| Slot {
            seq: AtomicUsize::new(i),
            val: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect();
    let inner = Arc::new(Inner {
        tail: CachePadded(AtomicUsize::new(0)),
        head: CachePadded(AtomicUsize::new(0)),
        mask: n - 1,
        slots,
    });
    (
        Producer {
            inner: Arc::clone(&inner),
        },
        Consumer {
            inner,
            _not_sync: PhantomData,
        },
    )
}

/// Producer side of the queue: `Clone + Send + Sync`.
pub struct Producer<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Producer {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Send> Producer<T> {
    /// Enqueues `v`. Returns `Err(v)` when the queue is full. Lock-free, never
    /// allocates.
    pub fn push(&self, v: T) -> Result<(), T> {
        let q = &*self.inner;
        let mut pos = q.tail.0.load(Ordering::Relaxed);
        loop {
            let slot = &q.slots[pos & q.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let dif = seq.wrapping_sub(pos) as isize;
            if dif == 0 {
                match q.tail.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: seq == pos means the slot is free and
                        // the CAS gave this producer exclusive ownership of
                        // position pos; no other thread touches the storage
                        // until seq is published as pos + 1 below.
                        unsafe { (*slot.val.get()).write(v) };
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(cur) => pos = cur,
                }
            } else if dif < 0 {
                // The slot still holds an item from the previous lap: full.
                return Err(v);
            } else {
                // Another producer claimed pos; reload and retry.
                pos = q.tail.0.load(Ordering::Relaxed);
            }
        }
    }

    /// Approximate number of queued items (`tail - head`), clamped to the
    /// capacity. Safe from any thread; counts pushes still in progress.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether [`len`](Self::len) is currently zero (approximate).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The queue's capacity (the rounded value passed to [`channel`]).
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

/// Consumer side of the queue: `Send`, but neither `Sync` nor `Clone`, so there
/// is exactly one consumer at a time by construction.
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
    /// `Cell<()>` is `Send` but `!Sync`, which makes `Consumer` `!Sync`.
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send> Consumer<T> {
    /// Dequeues the oldest item, or `None` when the queue is empty. This
    /// includes the brief window in which the oldest claimed slot is still
    /// being written by its producer (the item becomes visible once that push
    /// returns). The slot is freed, so the queue does not retain the value.
    pub fn pop(&self) -> Option<T> {
        let q = &*self.inner;
        // Only the consumer writes head, so a relaxed read sees its own store.
        let pos = q.head.0.load(Ordering::Relaxed);
        let slot = &q.slots[pos & q.mask];
        let seq = slot.seq.load(Ordering::Acquire);
        if (seq.wrapping_sub(pos.wrapping_add(1)) as isize) < 0 {
            return None;
        }
        // SAFETY: seq == pos + 1 (Acquire) means a producer finished writing
        // this slot and published it with Release, so the value is initialised
        // and visible. `Consumer` is `!Sync` and not `Clone`, so this is the
        // only reader; no producer touches the slot until seq is advanced to
        // pos + cap below, so reading it out by value (leaving it logically
        // uninitialised) is exclusive and happens exactly once.
        let v = unsafe { (*slot.val.get()).assume_init_read() };
        slot.seq
            .store(pos.wrapping_add(q.mask).wrapping_add(1), Ordering::Release);
        q.head.0.store(pos.wrapping_add(1), Ordering::Release);
        Some(v)
    }

    /// Approximate number of queued items (`tail - head`), clamped to the
    /// capacity. Counts pushes still in progress.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether [`len`](Self::len) is currently zero (approximate).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The queue's capacity (the rounded value passed to [`channel`]).
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    // Compile-time check that a type is NOT Sync: the blanket impl and the
    // Sync-only impl would be ambiguous if T were Sync.
    trait AmbiguousIfSync<A> {
        fn probe() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

    #[test]
    fn auto_traits() {
        assert_send::<Producer<u64>>();
        assert_sync::<Producer<u64>>();
        assert_send::<Consumer<u64>>();
        let _ = <Consumer<u64> as AmbiguousIfSync<_>>::probe; // compiles only if !Sync
    }

    #[test]
    fn capacity_rounding() {
        for (req, want) in [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 4),
            (4, 4),
            (5, 8),
            (100, 128),
            (1024, 1024),
        ] {
            let (p, c) = channel::<u8>(req);
            assert_eq!(p.capacity(), want, "req {req}");
            assert_eq!(c.capacity(), want, "req {req}");
        }
    }

    #[test]
    fn empty_pop_none() {
        let (p, c) = channel::<u32>(4);
        assert_eq!(c.pop(), None);
        assert!(p.is_empty() && c.is_empty());
    }

    #[test]
    fn fifo_single_producer_with_wraparound() {
        let (p, c) = channel::<u64>(4);
        let mut next = 0;
        for round in 0..50u64 {
            let n = 1 + round % 4;
            for i in 0..n {
                p.push(round * 10 + i).unwrap();
            }
            assert_eq!(c.len(), n as usize);
            for i in 0..n {
                assert_eq!(c.pop(), Some(round * 10 + i));
                next += 1;
            }
            assert_eq!(c.pop(), None);
        }
        assert_eq!(next, (0..50u64).map(|r| 1 + r % 4).sum::<u64>());
    }

    #[test]
    fn full_returns_value_intact() {
        let (p, c) = channel::<String>(2);
        p.push("a".to_string()).unwrap();
        p.push("b".to_string()).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p.push("c".to_string()), Err("c".to_string()));
        assert_eq!(c.pop().as_deref(), Some("a"));
        p.push("c".to_string()).unwrap();
        assert_eq!(c.pop().as_deref(), Some("b"));
        assert_eq!(c.pop().as_deref(), Some("c"));
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn fifo_per_producer_multi() {
        let (p, c) = channel::<(usize, usize)>(8);
        let per = if cfg!(miri) { 20 } else { 2_000 };
        run_stress(p, c, 4, per);
    }

    /// Drains concurrently, checking per-producer sequence order and that each
    /// item arrives exactly once.
    fn run_stress(
        p: Producer<(usize, usize)>,
        c: Consumer<(usize, usize)>,
        producers: usize,
        per: usize,
    ) {
        let handles: Vec<_> = (0..producers)
            .map(|id| {
                let p = p.clone();
                thread::spawn(move || {
                    for seq in 0..per {
                        let mut item = (id, seq);
                        while let Err(back) = p.push(item) {
                            item = back;
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        drop(p);
        let mut next = vec![0usize; producers];
        let mut got = 0;
        while got < producers * per {
            match c.pop() {
                Some((id, seq)) => {
                    assert_eq!(seq, next[id], "producer {id} out of order or duplicated");
                    next[id] += 1;
                    got += 1;
                }
                None => thread::yield_now(),
            }
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(next.iter().all(|&n| n == per));
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn stress_64_producers() {
        let (p, c) = channel(1024);
        let per = if cfg!(miri) { 2 } else { 10_000 };
        let producers = if cfg!(miri) { 4 } else { 64 };
        run_stress(p, c, producers, per);
    }

    /// Full stress: 64 producers x 1M pushes each. Run with
    /// `cargo test -p twine-mpsc --release -- --ignored`.
    #[test]
    #[ignore = "long stress; run with --release -- --ignored"]
    fn stress_64_producers_1m() {
        let (p, c) = channel(1024);
        run_stress(p, c, 64, 1_000_000);
    }

    #[test]
    fn push_pop_allocate_nothing() {
        let (p, c) = channel::<u64>(64);
        let n = twine_alloccount::allocs_per_run(1000, || {
            p.push(7).unwrap();
            assert_eq!(c.pop(), Some(7));
        });
        assert_eq!(n, 0.0, "push/pop allocate");
        let full = twine_alloccount::allocs(|| {
            for i in 0..64 {
                p.push(i).unwrap();
            }
            assert!(p.push(99).is_err());
            while c.pop().is_some() {}
        });
        assert_eq!(full, 0);
    }

    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn drop_exactly_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (p, c) = channel::<Counted>(8);
        for _ in 0..8 {
            p.push(Counted(drops.clone())).unwrap();
        }
        drop(c.pop()); // consumed item dropped by the caller: 1
        drop(c.pop()); // 2
        for _ in 0..2 {
            p.push(Counted(drops.clone())).unwrap(); // wraps around
        }
        assert!(p.push(Counted(drops.clone())).is_err()); // rejected value dropped: 3
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        drop(p);
        assert_eq!(drops.load(Ordering::SeqCst), 3, "consumer still alive");
        drop(c); // the 8 still-queued items are dropped by Inner
        assert_eq!(drops.load(Ordering::SeqCst), 3 + 8);
        assert_eq!(Arc::strong_count(&drops), 1);
    }
}
