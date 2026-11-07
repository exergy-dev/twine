use super::*;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};
use twine_alloccount::allocs;

fn pair() -> (UnixStream, UnixStream) {
    let (a, b) = UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    (a, b)
}

fn buf() -> [Event; 16] {
    [Event::default(); 16]
}

#[test]
fn socketpair_readiness() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    p.add(a.as_raw_fd(), READABLE).unwrap();
    let mut evs = buf();
    assert_eq!(p.wait(&mut evs, Some(Duration::ZERO)).unwrap(), 0);
    b.write_all(b"x").unwrap();
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_eq!(evs[0].fd, a.as_raw_fd());
    assert_ne!(evs[0].flags & READABLE, 0);
    assert_eq!(evs[0].flags & (HANGUP | ERROR), 0);
}

#[test]
fn writable_reported_on_add() {
    let mut p = Poller::new().unwrap();
    let (a, _b) = pair();
    p.add(a.as_raw_fd(), WRITABLE).unwrap();
    let mut evs = buf();
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_eq!(evs[0].flags, WRITABLE);
}

#[test]
fn edge_triggered_second_wait_is_empty() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    p.add(a.as_raw_fd(), READABLE).unwrap();
    b.write_all(b"hello").unwrap();
    let mut evs = buf();
    assert_eq!(
        p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap(),
        1
    );
    // Data is still unread, but no new edge has occurred.
    assert_eq!(p.wait(&mut evs, Some(Duration::ZERO)).unwrap(), 0);
    b.write_all(b"more").unwrap();
    assert_eq!(
        p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap(),
        1
    );
}

#[test]
fn modify_toggles_readable() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    p.add(a.as_raw_fd(), 0).unwrap();
    let mut evs = buf();
    b.write_all(b"x").unwrap();
    // No read interest: nothing reported for the data.
    let n = p.wait(&mut evs, Some(Duration::from_millis(20))).unwrap();
    assert_eq!(n, 0);
    // Enabling READABLE re-evaluates readiness.
    p.modify(a.as_raw_fd(), READABLE).unwrap();
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_ne!(evs[0].flags & READABLE, 0);
    // Drain, disable, write again: silent.
    let mut tmp = [0u8; 8];
    let _ = (&a).read(&mut tmp);
    p.modify(a.as_raw_fd(), 0).unwrap();
    b.write_all(b"y").unwrap();
    assert_eq!(
        p.wait(&mut evs, Some(Duration::from_millis(20))).unwrap(),
        0
    );
}

#[test]
fn modify_unregistered_fails_and_add_twice_fails() {
    let mut p = Poller::new().unwrap();
    let (a, _b) = pair();
    assert!(p.modify(a.as_raw_fd(), READABLE).is_err());
    p.add(a.as_raw_fd(), READABLE).unwrap();
    assert!(p.add(a.as_raw_fd(), READABLE).is_err());
}

#[test]
fn delete_stops_events() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    p.add(a.as_raw_fd(), READABLE).unwrap();
    p.delete(a.as_raw_fd()).unwrap();
    b.write_all(b"x").unwrap();
    let mut evs = buf();
    assert_eq!(
        p.wait(&mut evs, Some(Duration::from_millis(20))).unwrap(),
        0
    );
    assert!(p.delete(a.as_raw_fd()).is_err());
}

#[test]
fn wake_from_another_thread_unblocks_wait() {
    let mut p = Poller::new().unwrap();
    let w = p.waker();
    let t = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        let at = Instant::now();
        w.wake().unwrap();
        at
    });
    let mut evs = buf();
    let n = p.wait(&mut evs, None).unwrap();
    let woke = Instant::now();
    let at = t.join().unwrap();
    assert_eq!(n, 0);
    assert!(woke.duration_since(at) < Duration::from_millis(200));
}

#[test]
fn wake_before_wait_is_not_lost_and_is_drained() {
    let mut p = Poller::new().unwrap();
    p.waker().wake().unwrap();
    let mut evs = buf();
    let start = Instant::now();
    assert_eq!(p.wait(&mut evs, None).unwrap(), 0);
    assert!(start.elapsed() < Duration::from_millis(200));
    // Drained: a further wait times out instead of returning at once.
    let start = Instant::now();
    assert_eq!(
        p.wait(&mut evs, Some(Duration::from_millis(20))).unwrap(),
        0
    );
    assert!(start.elapsed() >= Duration::from_millis(15));
    // And a later wake still works.
    p.waker().wake().unwrap();
    assert_eq!(p.wait(&mut evs, None).unwrap(), 0);
}

#[test]
fn thousand_wakes_coalesce() {
    let mut p = Poller::new().unwrap();
    let w = p.waker();
    for _ in 0..1000 {
        w.wake().unwrap();
    }
    let mut evs = buf();
    assert_eq!(p.wait(&mut evs, None).unwrap(), 0);
    assert_eq!(p.wait(&mut evs, Some(Duration::ZERO)).unwrap(), 0);
}

#[test]
fn waker_outlives_poller() {
    let p = Poller::new().unwrap();
    let w = p.waker();
    let w2 = w.clone();
    drop(p);
    w.wake().unwrap();
    w2.wake().unwrap();
}

#[test]
fn waker_is_send_sync_clone() {
    fn assert_traits<T: Send + Sync + Clone>() {}
    assert_traits::<Waker>();
}

#[test]
fn wait_zero_does_not_allocate() {
    let mut p = Poller::new().unwrap();
    let (a, mut b) = pair();
    p.add(a.as_raw_fd(), READABLE | WRITABLE).unwrap();
    let mut evs = buf();
    // Warm up (consumes the initial writable edge).
    p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
    let n = allocs(|| {
        for _ in 0..100 {
            p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
        }
    });
    assert_eq!(n, 0, "idle wait allocates");
    b.write_all(b"x").unwrap();
    let n = allocs(|| {
        p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
    });
    assert_eq!(n, 0, "wait with events allocates");
}

#[test]
fn wake_does_not_allocate() {
    let mut p = Poller::new().unwrap();
    let w = p.waker();
    let n = allocs(|| {
        for _ in 0..100 {
            w.wake().unwrap();
        }
    });
    assert_eq!(n, 0, "wake allocates");
    let mut evs = buf();
    let n = allocs(|| {
        p.wait(&mut evs, Some(Duration::ZERO)).unwrap();
    });
    assert_eq!(n, 0, "wait draining the wake fd allocates");
}

#[test]
fn hangup_on_peer_close() {
    let mut p = Poller::new().unwrap();
    let (a, b) = pair();
    p.add(a.as_raw_fd(), READABLE).unwrap();
    drop(b);
    let mut evs = buf();
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_ne!(evs[0].flags & HANGUP, 0);
}

#[test]
fn hangup_reported_without_interest() {
    let mut p = Poller::new().unwrap();
    let (a, b) = pair();
    p.add(a.as_raw_fd(), 0).unwrap();
    drop(b);
    let mut evs = buf();
    let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(n, 1);
    assert_ne!(evs[0].flags & HANGUP, 0);
}

#[test]
fn reset_socket_reports_error_and_hangup() {
    use std::net::{TcpListener, TcpStream};
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let c = TcpStream::connect(l.local_addr().unwrap()).unwrap();
    let (mut s, _) = l.accept().unwrap();
    c.set_nonblocking(true).unwrap();
    let mut p = Poller::new().unwrap();
    p.add(c.as_raw_fd(), READABLE).unwrap();
    // Closing a socket that holds unread data makes the kernel send RST:
    // `c` writes bytes the server never reads, then the server closes.
    (&c).write_all(b"unread").unwrap();
    s.write_all(b"data for c").unwrap();
    let mut evs = buf();
    p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
    drop(s);
    let mut flags = 0;
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline && flags & ERROR == 0 {
        let n = p.wait(&mut evs, Some(Duration::from_millis(50))).unwrap();
        for e in &evs[..n] {
            flags |= e.flags;
        }
    }
    assert_ne!(flags & ERROR, 0, "flags {flags:#b}");
    assert_ne!(flags & HANGUP, 0, "flags {flags:#b}");
}

#[test]
fn timeout_rounds_up() {
    assert_eq!(epoll::timeout_ms(None), -1);
    assert_eq!(epoll::timeout_ms(Some(Duration::ZERO)), 0);
    assert_eq!(epoll::timeout_ms(Some(Duration::from_nanos(1))), 1);
    assert_eq!(epoll::timeout_ms(Some(Duration::from_micros(1001))), 2);
    assert_eq!(epoll::timeout_ms(Some(Duration::from_millis(5))), 5);
    assert_eq!(
        epoll::timeout_ms(Some(Duration::from_secs(u64::MAX / 1000))),
        i32::MAX
    );
}

#[test]
fn one_nanosecond_timeout_blocks_about_a_millisecond() {
    let mut p = Poller::new().unwrap();
    let mut evs = buf();
    let start = Instant::now();
    assert_eq!(p.wait(&mut evs, Some(Duration::from_nanos(1))).unwrap(), 0);
    let el = start.elapsed();
    assert!(el >= Duration::from_micros(500), "returned after {el:?}");
    assert!(el < Duration::from_millis(100));
}

#[test]
fn empty_event_slice_is_einval() {
    let mut p = Poller::new().unwrap();
    let e = p.wait(&mut [], Some(Duration::ZERO)).unwrap_err();
    assert_eq!(e.raw_os_error(), Some(libc::EINVAL));
}

#[test]
fn many_events_in_one_wait() {
    let mut p = Poller::new().unwrap();
    let pairs: Vec<_> = (0..20).map(|_| pair()).collect();
    for (a, _) in &pairs {
        p.add(a.as_raw_fd(), READABLE).unwrap();
    }
    for (_, b) in &pairs {
        (&*b).write_all(b"x").unwrap();
    }
    let mut evs = [Event::default(); 8];
    let mut seen = 0;
    while seen < 20 {
        let n = p.wait(&mut evs, Some(Duration::from_millis(100))).unwrap();
        assert!(n > 0 && n <= 8);
        seen += n;
    }
    assert_eq!(seen, 20);
}
