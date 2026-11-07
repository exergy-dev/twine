//! Twine internal crate `twine-ring`. See `rust/docs/INTERNALS.md` for the
//! contract this crate implements. Not part of the public API.
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::VecDeque;
use std::ffi::c_void;
use twine_slab::{class_for, Allocator, Slab, CLASSES};

#[cfg(test)]
#[global_allocator]
static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;

/// Number of segments the deque has room for from the start.
const INITIAL_SEGMENTS: usize = 4;
const MAX_CLASS: usize = CLASSES[CLASSES.len() - 1];

/// One slab in the chain, used as a circular buffer: readable bytes are
/// `buf[head..head+len]` taken modulo `buf.len()`.
///
/// Writes only go to the last segment and reads only come from the first, so
/// only the first segment can hold wrapped data; later segments are always
/// filled linearly from offset 0.
#[derive(Debug)]
struct Segment {
    buf: Slab,
    head: usize,
    len: usize,
}

impl Segment {
    fn is_full(&self) -> bool {
        self.len == self.buf.len()
    }

    /// Contiguous readable region starting at `head`.
    fn first_region(&self) -> &[u8] {
        let end = (self.head + self.len).min(self.buf.len());
        &self.buf[self.head..end]
    }

    /// Wrapped readable region at the start of the slab (possibly empty).
    fn second_region(&self) -> &[u8] {
        let end = self.head + self.len;
        &self.buf[..end.saturating_sub(self.buf.len())]
    }
}

/// Outbound byte FIFO for one connection, backed by slab segments.
///
/// The ring never grows by reallocating a contiguous buffer. A connection
/// whose pending bytes stay within one 4 KiB slab never touches the allocator
/// after its first write, because the single segment is circular.
#[derive(Debug)]
pub struct Ring {
    segs: VecDeque<Segment>,
    len: usize,
    high_water: usize,
}

impl Default for Ring {
    fn default() -> Self {
        Self::new()
    }
}

impl Ring {
    /// Creates an empty ring holding no slab. Does not allocate: the deque
    /// reserves room for `INITIAL_SEGMENTS` segments on the first write.
    pub fn new() -> Ring {
        Ring {
            segs: VecDeque::new(),
            len: 0,
            high_water: 0,
        }
    }

    /// Number of readable bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no bytes are pending.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Appends `b`, chaining slabs from `a` as needed. Copies `b`; an empty
    /// slice is a no-op. When the last segment is full, the next segment is
    /// the smallest class that fits the bytes still to be written (at most
    /// 256 KiB).
    pub fn write(&mut self, a: &mut Allocator, mut b: &[u8]) {
        if b.is_empty() {
            return;
        }
        if self.segs.capacity() == 0 {
            self.segs.reserve(INITIAL_SEGMENTS);
        }
        self.len += b.len();
        self.high_water = self.high_water.max(self.len);
        while !b.is_empty() {
            if self.segs.back().is_none_or(Segment::is_full) {
                let want = class_for(b.len()).unwrap_or(MAX_CLASS);
                self.segs.push_back(Segment {
                    buf: a.get(want),
                    head: 0,
                    len: 0,
                });
            }
            let Some(s) = self.segs.back_mut() else {
                unreachable!("a tail segment was just ensured");
            };
            let size = s.buf.len();
            let mut w = s.head + s.len;
            let room = if w >= size {
                w -= size;
                s.head - w
            } else {
                size - w
            };
            let c = room.min(b.len());
            s.buf[w..w + c].copy_from_slice(&b[..c]);
            s.len += c;
            b = &b[c..];
        }
    }

    /// Appends each slice in order (one [`write`](Ring::write) per slice).
    pub fn writev(&mut self, a: &mut Allocator, bufs: &[&[u8]]) {
        for b in bufs {
            self.write(a, b);
        }
    }

    /// Fills `dst` with the readable regions in FIFO order and returns how
    /// many entries were filled; stops when `dst` is full. Never allocates. A
    /// wrapped segment contributes two iovecs. The iovecs point into the
    /// ring's slabs and are valid until the next `&mut` call on the ring.
    /// They must only be used for reading (e.g. `writev(2)`).
    pub fn iovecs(&self, dst: &mut [libc::iovec]) -> usize {
        let mut i = 0;
        for s in &self.segs {
            if s.len == 0 {
                continue;
            }
            for region in [s.first_region(), s.second_region()] {
                if region.is_empty() {
                    continue;
                }
                let Some(slot) = dst.get_mut(i) else {
                    return i;
                };
                // The *const -> *mut cast only fits the C struct's field type;
                // nothing ever writes through it.
                slot.iov_base = region.as_ptr() as *mut c_void;
                slot.iov_len = region.len();
                i += 1;
            }
        }
        i
    }

    /// First contiguous readable region (empty when the ring is empty).
    pub fn peek(&self) -> &[u8] {
        match self.segs.front() {
            Some(s) => s.first_region(),
            None => &[],
        }
    }

    /// Drops `n` bytes from the front. Segments that become empty are
    /// returned to `a`, except the last, which is kept (reset to empty) so
    /// the next write needs no allocation.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.len()`.
    pub fn consume(&mut self, a: &mut Allocator, mut n: usize) {
        assert!(n <= self.len, "ring: consume out of range");
        self.len -= n;
        while n > 0 {
            let Some(s) = self.segs.front_mut() else {
                unreachable!("len accounting guarantees a segment");
            };
            if n < s.len {
                s.head = (s.head + n) % s.buf.len();
                s.len -= n;
                return;
            }
            n -= s.len;
            s.len = 0;
            s.head = 0;
            if self.segs.len() == 1 {
                return; // keep the last segment, empty
            }
            if let Some(done) = self.segs.pop_front() {
                a.put(done.buf);
            }
        }
    }

    /// Drops all data and returns every segment to `a`. The deque capacity
    /// and the high-water mark are kept.
    pub fn reset(&mut self, a: &mut Allocator) {
        for s in self.segs.drain(..) {
            a.put(s.buf);
        }
        self.len = 0;
    }

    /// Maximum `len` observed since `new` / [`reset_high_water`](Ring::reset_high_water).
    pub fn high_water(&self) -> usize {
        self.high_water
    }

    /// Sets the high-water mark to the current length.
    pub fn reset_high_water(&mut self) {
        self.high_water = self.len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

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
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn zero_iov() -> libc::iovec {
        libc::iovec {
            iov_base: ptr::null_mut(),
            iov_len: 0,
        }
    }

    /// Copies out all bytes described by `ring.iovecs` with a given dst size.
    fn gather(r: &Ring, cap: usize) -> (Vec<u8>, usize) {
        let mut dst = vec![zero_iov(); cap];
        let n = r.iovecs(&mut dst);
        let mut out = Vec::new();
        for v in &dst[..n] {
            // SAFETY: iovecs points into live slabs owned by `r`, which is
            // borrowed immutably and so unchanged for the duration of the copy.
            out.extend_from_slice(unsafe {
                std::slice::from_raw_parts(v.iov_base as *const u8, v.iov_len)
            });
        }
        (out, n)
    }

    #[test]
    fn property_against_reference() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut a = Allocator::new();
        let mut r = Ring::new();
        let mut model: VecDeque<u8> = VecDeque::new();
        let mut counter = 0u8;
        for step in 0..3000 {
            let roll = rng.below(10);
            if roll < 5 {
                let n = match rng.below(4) {
                    0 => 1 + rng.below(16),
                    1 => 1 + rng.below(5000),
                    2 => 1 + rng.below(70_000),
                    _ => 1 + rng.below(300 * 1024),
                };
                let chunk: Vec<u8> = (0..n)
                    .map(|_| {
                        counter = counter.wrapping_add(1);
                        counter
                    })
                    .collect();
                if step % 7 == 0 {
                    let h = n / 2;
                    r.writev(&mut a, &[&chunk[..h], &chunk[h..]]);
                } else {
                    r.write(&mut a, &chunk);
                }
                model.extend(&chunk);
            } else if roll < 9 {
                let n = if model.is_empty() {
                    0
                } else if rng.below(3) == 0 {
                    model.len()
                } else {
                    rng.below(model.len() + 1)
                };
                r.consume(&mut a, n);
                model.drain(..n);
            } else if rng.below(4) == 0 {
                r.reset(&mut a);
                model.clear();
            }
            assert_eq!(r.len(), model.len());
            assert_eq!(r.is_empty(), model.is_empty());
            let (got, _) = gather(&r, 64);
            // 64 iovecs may truncate for huge backlogs; compare prefix then.
            let want: Vec<u8> = model.iter().copied().collect();
            let (full, n) = gather(&r, 1024);
            assert_eq!(full, want, "step {step} ({n} iovecs)");
            assert!(want.starts_with(&got));
            let p = r.peek();
            assert!(!p.is_empty() || want.is_empty());
            assert_eq!(p, &want[..p.len()]);
        }
        r.reset(&mut a);
        assert_eq!(a.stats().in_use, 0);
    }

    #[test]
    fn wraparound_single_segment() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        let data: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
        r.write(&mut a, &data[..4000]);
        r.consume(&mut a, 3900);
        r.write(&mut a, &data[..300]); // 96 bytes to slab end, 204 wrapped
        assert_eq!(a.stats().gets, 1);
        assert_eq!(r.len(), 400);
        let mut dst = [zero_iov(); 4];
        assert_eq!(r.iovecs(&mut dst), 2);
        assert_eq!((dst[0].iov_len, dst[1].iov_len), (196, 204));
        // dst too small: stops after the first region.
        assert_eq!(r.iovecs(&mut dst[..1]), 1);
        assert_eq!(r.iovecs(&mut []), 0);
        assert_eq!(r.peek().len(), 196);
        let (got, _) = gather(&r, 4);
        let mut want = data[3900..4000].to_vec();
        want.extend_from_slice(&data[..300]);
        assert_eq!(got, want);
        // Consume across the wrap point, then to empty.
        r.consume(&mut a, 250);
        assert_eq!(r.peek(), &want[250..]);
        r.consume(&mut a, 150);
        assert!(r.is_empty());
        assert_eq!(a.stats().gets, 1);
    }

    #[test]
    fn consume_exact_segment_boundary() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.write(&mut a, &[7u8; 4096]);
        r.write(&mut a, &[7u8; 100]); // first slab full -> second 4K slab
        assert_eq!(r.segs.len(), 2);
        r.consume(&mut a, 4096);
        assert_eq!(r.segs.len(), 1);
        assert_eq!(r.len(), 100);
        assert_eq!(a.stats().free, 4096);
        r.consume(&mut a, 100);
        // Last segment kept, reset to empty.
        assert_eq!(r.segs.len(), 1);
        assert!(r.peek().is_empty());
        assert_eq!(r.segs[0].head, 0);
        assert_eq!(a.stats().in_use, 4096);
    }

    #[test]
    fn large_write_picks_big_classes() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.write(&mut a, &vec![1u8; 300 * 1024]);
        let sizes: Vec<usize> = r.segs.iter().map(|s| s.buf.len()).collect();
        assert_eq!(sizes, [256 << 10, 64 << 10]);
        r.write(&mut a, &[2u8; 10]);
        assert_eq!(r.segs.len(), 2);
        let mut r2 = Ring::new();
        r2.write(&mut a, &vec![1u8; 600 * 1024]); // 256K + 256K + 100K -> 256K
        assert_eq!(r2.segs.len(), 3);
    }

    #[test]
    fn write_after_reset_and_capacity_kept() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.write(&mut a, &vec![1u8; 20_000]);
        let cap = r.segs.capacity();
        assert!(cap >= INITIAL_SEGMENTS);
        r.reset(&mut a);
        assert_eq!((r.len(), r.segs.len(), a.stats().in_use), (0, 0, 0));
        assert!(r.peek().is_empty());
        assert!(r.segs.capacity() >= cap);
        r.write(&mut a, b"hello");
        assert_eq!(r.peek(), b"hello");
        r.reset(&mut a);
        r.reset(&mut a); // idempotent
    }

    #[test]
    fn empty_write_is_noop() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.write(&mut a, &[]);
        r.writev(&mut a, &[&[], &[]]);
        assert_eq!(a.stats().gets, 0);
        assert_eq!(r.iovecs(&mut [zero_iov(); 2]), 0);
        assert_eq!(Ring::default().len(), 0);
    }

    #[test]
    fn writev_orders_slices() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.writev(&mut a, &[b"ab", b"", b"cde"]);
        assert_eq!(r.peek(), b"abcde");
    }

    #[test]
    fn iovecs_multi_segment_and_limit() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        for _ in 0..3 {
            r.write(&mut a, &[1u8; 4096]);
        }
        r.write(&mut a, &[2u8; 5]);
        assert_eq!(r.segs.len(), 4);
        let (all, n) = gather(&r, 16);
        assert_eq!((all.len(), n), (4096 * 3 + 5, 4));
        let (part, n) = gather(&r, 2);
        assert_eq!((part.len(), n), (8192, 2));
    }

    #[test]
    fn high_water_tracking() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.write(&mut a, &[0; 100]);
        r.write(&mut a, &[0; 50]);
        r.consume(&mut a, 120);
        assert_eq!((r.len(), r.high_water()), (30, 150));
        r.reset_high_water();
        assert_eq!(r.high_water(), 30);
        r.reset(&mut a);
        assert_eq!(r.high_water(), 30);
    }

    #[test]
    #[should_panic(expected = "consume out of range")]
    fn consume_too_much_panics() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        r.write(&mut a, b"abc");
        r.consume(&mut a, 4);
    }

    #[test]
    fn alloc_gates() {
        let mut a = Allocator::new();
        let mut r = Ring::new();
        assert_eq!(
            twine_alloccount::allocs(Ring::new),
            0,
            "Ring::new allocates"
        );
        // The first write may allocate the deque once (plus the slab).
        r.write(&mut a, &[1u8; 100]);
        let chunk = [9u8; 300];
        let per = twine_alloccount::allocs_per_run(10_000, || {
            r.write(&mut a, &chunk);
            r.consume(&mut a, 300);
        });
        assert_eq!(per, 0.0, "steady-state write+consume allocates");
        // iovecs
        let mut dst = [zero_iov(); 8];
        let n = twine_alloccount::allocs(|| r.iovecs(&mut dst));
        assert_eq!(n, 0, "iovecs allocates");
        // write across segments with warm deque and free list.
        r.reset(&mut a);
        r.write(&mut a, &vec![0u8; 3 * 4096]);
        r.reset(&mut a);
        let big = vec![3u8; 3 * 4096];
        let n = twine_alloccount::allocs(|| r.write(&mut a, &big));
        assert_eq!(n, 0, "write with warm deque and free list allocates");
    }
}
