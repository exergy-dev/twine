//! Twine internal crate `twine-slab`. See `rust/docs/INTERNALS.md` for the
//! contract this crate implements. Not part of the public API.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(test)]
#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

/// Slab sizes, ascending.
pub const CLASSES: [usize; 4] = [4 << 10, 16 << 10, 64 << 10, 256 << 10];

/// One slab: a boxed byte slice whose length is its class size.
pub type Slab = Box<[u8]>;

const NUM_CLASSES: usize = CLASSES.len();
const MAX_CLASS: usize = CLASSES[NUM_CLASSES - 1];
/// Preallocated capacity of each class free list.
const INITIAL_FREE_SLOTS: usize = 64;

/// Index of the smallest class >= `n`, or `None` if `n` exceeds every class.
fn class_index(n: usize) -> Option<usize> {
    CLASSES.iter().position(|&c| n <= c)
}

/// Returns the class size for `n` (`0` maps to the smallest class), or `None`
/// if `n` is larger than all classes.
pub fn class_for(n: usize) -> Option<usize> {
    class_index(n).map(|i| CLASSES[i])
}

/// Snapshot of an [`Allocator`]'s counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// Bytes currently handed out.
    pub in_use: usize,
    /// Bytes sitting in free lists.
    pub free: usize,
    /// High-water mark of `in_use + free`, in bytes.
    pub allocated: usize,
    /// Calls to [`Allocator::get`].
    pub gets: u64,
    /// Calls to [`Allocator::put`].
    pub puts: u64,
    /// Gets that had to heap-allocate a new slab.
    pub misses: u64,
}

/// Per-loop size-classed slab allocator. Not thread-safe.
///
/// Taking a slab from a non-empty free list and returning one to a free list
/// with spare capacity never touch the heap.
#[derive(Debug)]
pub struct Allocator {
    free: [Vec<Slab>; NUM_CLASSES],
    in_use: usize,
    free_bytes: usize,
    allocated: usize,
    gets: u64,
    puts: u64,
    misses: u64,
}

impl Default for Allocator {
    fn default() -> Self {
        Self::new()
    }
}

impl Allocator {
    /// Creates an allocator whose free lists each have room for 64 slabs.
    pub fn new() -> Allocator {
        Allocator {
            free: std::array::from_fn(|_| Vec::with_capacity(INITIAL_FREE_SLOTS)),
            in_use: 0,
            free_bytes: 0,
            allocated: 0,
            gets: 0,
            puts: 0,
            misses: 0,
        }
    }

    /// Returns a slab with `len` equal to the smallest class >= `n`. An `n`
    /// larger than the largest class yields a fresh, unpooled slab rounded up
    /// to a multiple of 256 KiB (`put` drops it). Reused slabs are not cleared.
    pub fn get(&mut self, n: usize) -> Slab {
        self.gets += 1;
        let Some(i) = class_index(n) else {
            let size = n
                .div_ceil(MAX_CLASS)
                .checked_mul(MAX_CLASS)
                .expect("twine-slab: size overflow");
            return self.miss(size);
        };
        if let Some(b) = self.free[i].pop() {
            self.free_bytes -= b.len();
            self.in_use += b.len();
            return b;
        }
        self.miss(CLASSES[i])
    }

    fn miss(&mut self, size: usize) -> Slab {
        self.misses += 1;
        self.in_use += size;
        self.allocated = self.allocated.max(self.in_use + self.free_bytes);
        vec![0u8; size].into_boxed_slice()
    }

    /// Returns `b` (identified by its length) to its class free list.
    /// Oversize slabs (a multiple of 256 KiB above the largest class) and
    /// any other non-class sizes are dropped. Allocates only if the free list
    /// already holds more than its preallocated 64 slabs.
    pub fn put(&mut self, b: Slab) {
        self.puts += 1;
        let c = b.len();
        match CLASSES.iter().position(|&s| s == c) {
            Some(i) => {
                self.free[i].push(b);
                // saturating: a foreign slab of a class size was never counted.
                self.in_use = self.in_use.saturating_sub(c);
                self.free_bytes += c;
            }
            None => {
                if c > MAX_CLASS && c % MAX_CLASS == 0 {
                    self.in_use = self.in_use.saturating_sub(c);
                }
            }
        }
    }

    /// Returns a snapshot of the counters.
    pub fn stats(&self) -> Stats {
        Stats {
            in_use: self.in_use,
            free: self.free_bytes,
            allocated: self.allocated,
            gets: self.gets,
            puts: self.puts,
            misses: self.misses,
        }
    }

    /// Releases free slabs above `keep` bytes per class: each class keeps at
    /// most `keep / class_size` free slabs. The free lists keep their capacity.
    pub fn trim(&mut self, keep: usize) {
        for (list, &size) in self.free.iter_mut().zip(CLASSES.iter()) {
            let n = keep / size;
            if list.len() > n {
                self.free_bytes -= (list.len() - n) * size;
                list.truncate(n);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_rounding() {
        assert_eq!(class_for(0), Some(4096));
        assert_eq!(class_for(1), Some(4096));
        assert_eq!(class_for(4096), Some(4096));
        assert_eq!(class_for(4097), Some(16384));
        assert_eq!(class_for(16384), Some(16384));
        assert_eq!(class_for(16385), Some(65536));
        assert_eq!(class_for(65537), Some(262144));
        assert_eq!(class_for(262144), Some(262144));
        assert_eq!(class_for(262145), None);
        let mut a = Allocator::new();
        for (n, want) in [(0, 4096), (5000, 16384), (20000, 65536), (100_000, 262_144)] {
            assert_eq!(a.get(n).len(), want);
        }
    }

    #[test]
    fn oversize_not_pooled() {
        let mut a = Allocator::new();
        let b = a.get(MAX_CLASS + 1);
        assert_eq!(b.len(), 2 * MAX_CLASS);
        let b2 = a.get(3 * MAX_CLASS);
        assert_eq!(b2.len(), 3 * MAX_CLASS);
        let s = a.stats();
        assert_eq!(
            (s.in_use, s.misses, s.allocated),
            (5 * MAX_CLASS, 2, 5 * MAX_CLASS)
        );
        a.put(b);
        a.put(b2);
        let s = a.stats();
        assert_eq!((s.in_use, s.free, s.puts), (0, 0, 2));
        // Not reused: next get misses again.
        let _ = a.get(MAX_CLASS + 1);
        assert_eq!(a.stats().misses, 3);
    }

    #[test]
    fn put_non_class_dropped() {
        let mut a = Allocator::new();
        a.put(vec![0u8; 100].into_boxed_slice());
        a.put(vec![0u8; MAX_CLASS + 1].into_boxed_slice());
        let s = a.stats();
        assert_eq!((s.in_use, s.free, s.puts), (0, 0, 2));
    }

    #[test]
    fn reuse_and_stats() {
        let mut a = Allocator::new();
        let b = a.get(10);
        assert_eq!(a.stats().in_use, 4096);
        a.put(b);
        let s = a.stats();
        assert_eq!((s.in_use, s.free, s.misses), (0, 4096, 1));
        let b = a.get(10);
        let s = a.stats();
        assert_eq!(
            (s.in_use, s.free, s.misses, s.gets, s.puts),
            (4096, 0, 1, 2, 1)
        );
        assert_eq!(s.allocated, 4096);
        drop(b);
    }

    #[test]
    fn trim_keeps_budget() {
        let mut a = Allocator::new();
        let v: Vec<Slab> = (0..4).map(|_| a.get(1)).collect();
        let w: Vec<Slab> = (0..2).map(|_| a.get(MAX_CLASS)).collect();
        v.into_iter().chain(w).for_each(|b| a.put(b));
        assert_eq!(a.stats().free, 4 * 4096 + 2 * MAX_CLASS);
        a.trim(2 * 4096); // keeps 2 small, 0 of the 256K
        assert_eq!(a.stats().free, 2 * 4096);
        a.trim(10 * MAX_CLASS); // nothing to release
        assert_eq!(a.stats().free, 2 * 4096);
        a.trim(0);
        assert_eq!(a.stats().free, 0);
    }

    #[test]
    fn alloc_gates() {
        let mut a = Allocator::new();
        let b = a.get(1);
        a.put(b); // free list non-empty
        let mut slot = None;
        let n = twine_alloccount::allocs(|| slot = Some(a.get(1)));
        assert_eq!(n, 0, "get from free list allocates");
        let b = slot.take().unwrap();
        let n = twine_alloccount::allocs(|| a.put(b));
        assert_eq!(n, 0, "put into free list allocates");
        let per = twine_alloccount::allocs_per_run(100, || {
            let b = a.get(100);
            a.put(b);
        });
        assert_eq!(per, 0.0);
    }

    #[test]
    fn default_works() {
        assert_eq!(Allocator::default().stats().gets, 0);
    }
}
