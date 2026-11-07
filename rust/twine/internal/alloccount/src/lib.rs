//! Counting global allocator for the zero-allocation gates (design Section 7.3 and 12).
//!
//! Each test binary that gates allocations declares
//!
//! ```text
//! #[global_allocator]
//! static ALLOC: twine_alloccount::Counting = twine_alloccount::Counting;
//! ```
//!
//! and then wraps the hot operation in [`allocs`]:
//!
//! ```text
//! let n = twine_alloccount::allocs(|| ring.write(&buf));
//! assert_eq!(n, 0, "write path allocates");
//! ```
//!
//! The counter is per thread (a const-initialised thread local, so the first
//! access itself allocates nothing), which keeps parallel tests from
//! corrupting each other's counts. Only the calling thread's allocations are
//! counted; allocations performed by other threads during the closure are not.
#![deny(missing_docs)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

/// A global allocator that forwards to [`System`] and counts allocations on
/// the current thread.
pub struct Counting;

// SAFETY: every method forwards to System with the same arguments; the
// counter is a plain thread-local Cell touched outside the allocator call.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        System.realloc(ptr, layout, new_size)
    }
}

#[inline]
fn bump() {
    // A thread local may be unavailable while the thread is being torn down;
    // allocations made then are simply not counted.
    let _ = COUNT.try_with(|c| c.set(c.get() + 1));
}

/// Returns the number of allocations (alloc, alloc_zeroed, realloc) made on
/// this thread so far.
pub fn count() -> u64 {
    COUNT.with(|c| c.get())
}

/// Runs `f` and returns how many allocations it performed on this thread.
pub fn allocs<R>(f: impl FnOnce() -> R) -> u64 {
    let before = count();
    let r = f();
    let after = count();
    drop(r);
    after - before
}

/// Runs `f` `n` times after one warm-up call and returns the average number of
/// allocations per call (the equivalent of Go's `testing.AllocsPerRun`).
pub fn allocs_per_run(n: u64, mut f: impl FnMut()) -> f64 {
    f();
    let before = count();
    for _ in 0..n {
        f();
    }
    (count() - before) as f64 / n as f64
}
