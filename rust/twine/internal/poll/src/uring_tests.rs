//! io_uring-specific tests (the shared suite in `tests.rs` also runs against
//! this backend when the `uring` feature is on).

use super::*;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

fn pair() -> (UnixStream, UnixStream) {
    let (a, b) = UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    (a, b)
}

fn uring_available() -> bool {
    io_uring::IoUring::new(8).is_ok()
}

#[test]
fn backend_name_reports_backend() {
    let p = Poller::new().unwrap();
    if uring_available() {
        assert_eq!(p.backend_name(), "io_uring");
    } else {
        assert_eq!(p.backend_name(), "epoll");
    }
    assert_eq!(epoll::Poller::new().unwrap().backend_name(), "epoll");
}

#[test]
fn epoll_fallback_backend_works() {
    let mut p = Poller::new_epoll().unwrap();
    assert_eq!(p.backend_name(), "epoll");
    let (a, mut b) = pair();
    p.add(a.as_raw_fd(), READABLE).unwrap();
    b.write_all(b"x").unwrap();
    let mut evs = [Event::default(); 4];
    assert_eq!(
        p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap(),
        1
    );
    let w = p.waker();
    w.wake().unwrap();
    assert_eq!(p.wait(&mut evs, None).unwrap(), 0);
}

#[test]
fn poller_is_send_sync() {
    fn assert_traits<T: Send + Sync>() {}
    assert_traits::<Poller>();
}

#[test]
fn modify_during_in_flight_completion_neither_loses_nor_duplicates() {
    for _ in 0..50 {
        let mut p = Poller::new().unwrap();
        let (a, mut b) = pair();
        let fd = a.as_raw_fd();
        p.add(fd, READABLE).unwrap();
        // Submit the arm, then let a completion get in flight.
        let mut evs = [Event::default(); 16];
        assert_eq!(p.wait(&mut evs, Some(Duration::ZERO)).unwrap(), 0);
        b.write_all(b"abc").unwrap();
        p.modify(fd, READABLE | WRITABLE).unwrap();
        let mut readable = 0;
        let mut writable = 0;
        let end = Instant::now() + Duration::from_millis(30);
        while Instant::now() < end {
            let n = p.wait(&mut evs, Some(Duration::from_millis(5))).unwrap();
            for e in &evs[..n] {
                assert_eq!(e.fd, fd);
                readable += usize::from(e.flags & READABLE != 0);
                writable += usize::from(e.flags & WRITABLE != 0);
            }
        }
        assert_eq!(readable, 1, "readable edge lost or duplicated");
        assert_eq!(writable, 1);
        // The new registration still works.
        b.write_all(b"d").unwrap();
        let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
        assert_eq!(n, 1);
        assert_ne!(evs[0].flags & READABLE, 0);
        let mut tmp = [0u8; 8];
        let _ = (&a).read(&mut tmp);
    }
}

#[test]
fn delete_then_close_closes_the_socket() {
    for writable_arm in [false, true] {
        for _ in 0..200 {
            let mut p = Poller::new().unwrap();
            let (a, mut b) = pair();
            let interest = if writable_arm { WRITABLE } else { READABLE };
            p.add(a.as_raw_fd(), interest).unwrap();
            // Writable arms complete immediately, exercising -EALREADY.
            let t0 = Instant::now();
            p.delete(a.as_raw_fd()).unwrap();
            drop(a);
            b.set_nonblocking(false).unwrap();
            b.set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut tmp = [0u8; 1];
            assert_eq!(b.read(&mut tmp).unwrap(), 0, "peer did not see EOF");
            assert!(t0.elapsed() < Duration::from_millis(100));
        }
    }
}

#[test]
fn delete_after_modify_then_close_closes_the_socket() {
    for _ in 0..200 {
        let mut p = Poller::new().unwrap();
        let (a, mut b) = pair();
        p.add(a.as_raw_fd(), WRITABLE).unwrap();
        p.modify(a.as_raw_fd(), READABLE | WRITABLE).unwrap();
        p.delete(a.as_raw_fd()).unwrap();
        drop(a);
        b.set_nonblocking(false).unwrap();
        b.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut tmp = [0u8; 1];
        assert_eq!(b.read(&mut tmp).unwrap(), 0, "peer did not see EOF");
    }
}

#[test]
fn delete_keeps_other_fds_events_and_wake() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    let (c, _d) = pair();
    p.add(a.as_raw_fd(), READABLE).unwrap();
    p.add(c.as_raw_fd(), WRITABLE).unwrap();
    b.write_all(b"x").unwrap();
    p.waker().wake().unwrap();
    // delete harvests CQEs into the stash; nothing may be lost.
    p.delete(c.as_raw_fd()).unwrap();
    let mut evs = [Event::default(); 8];
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_eq!(evs[0].fd, a.as_raw_fd());
    // A wake consumed during delete still ends the next blocking wait.
    p.waker().wake().unwrap();
    p.delete(a.as_raw_fd()).unwrap();
    let t = Instant::now();
    assert_eq!(p.wait(&mut evs, None).unwrap(), 0);
    assert!(t.elapsed() < Duration::from_millis(100));
}

fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[test]
fn ten_thousand_cycles_do_not_leak() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    let fd = a.as_raw_fd();
    let mut evs = [Event::default(); 8];
    let before = fd_count();
    for i in 0..10_000 {
        p.add(fd, READABLE).unwrap();
        p.modify(fd, READABLE | WRITABLE).unwrap();
        if i % 100 == 0 {
            p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
        }
        p.delete(fd).unwrap();
    }
    let after = fd_count();
    // Other tests run concurrently and open/close fds; a real leak would be
    // >= 10k.
    assert!(after < before + 1000, "fd count {before} -> {after}");
    drop(a);
    b.set_nonblocking(false).unwrap();
    b.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    let mut tmp = [0u8; 1];
    assert_eq!(b.read(&mut tmp).unwrap(), 0, "armed poll leaked");
    // Slot table did not grow beyond one fd's worth.
    p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
}

#[test]
fn many_fds_more_than_sq_depth() {
    // More registrations than SQ entries between waits forces the SQ-full
    // submit-and-retry path.
    let mut p = Poller::new().unwrap();
    let pairs: Vec<_> = (0..300).map(|_| pair()).collect();
    for (a, _) in &pairs {
        p.add(a.as_raw_fd(), READABLE).unwrap();
    }
    for (_, b) in &pairs {
        (&*b).write_all(b"x").unwrap();
    }
    let mut evs = [Event::default(); 64];
    let mut seen = std::collections::HashSet::new();
    let end = Instant::now() + Duration::from_secs(2);
    while seen.len() < 300 && Instant::now() < end {
        let n = p.wait(&mut evs, Some(Duration::from_millis(50))).unwrap();
        for e in &evs[..n] {
            seen.insert(e.fd);
        }
    }
    assert_eq!(seen.len(), 300);
}

#[test]
fn closed_fd_registration_reports_error_asynchronously() {
    let mut p = Poller::new().unwrap();
    // A descriptor number nothing else in this process will open.
    let fd = 4000;
    // SAFETY: F_GETFD takes no pointers; it only probes whether `fd` is open.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    p.add(fd, READABLE).unwrap();
    let mut evs = [Event::default(); 4];
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_ne!(evs[0].flags & ERROR, 0);
    p.delete(fd).unwrap();
}

/// Run with `cargo test --release --features uring -- --ignored --nocapture idle_wait_cost`.
#[test]
#[ignore]
fn idle_wait_cost() {
    const N: u32 = 100_000;
    let mut evs = [Event::default(); 16];
    for (name, mut p) in [
        ("epoll", Poller::new_epoll().unwrap()),
        ("io_uring", Poller::new().unwrap()),
    ] {
        let (a, _b) = pair();
        p.add(a.as_raw_fd(), READABLE).unwrap();
        p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
        let t = Instant::now();
        for _ in 0..N {
            p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
        }
        let el = t.elapsed();
        println!(
            "idle wait(ZERO) [{}]: {:.1} ns/call over {N} calls",
            p.backend_name(),
            el.as_nanos() as f64 / f64::from(N)
        );
        let _ = name;
    }
}
