//! Deferred, Resolver, Awaitable, all/any, sleep, defer_to_thread and on_conn.

mod common;

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use common::*;
use twine::{all, any, factory, Deferred, Engine, Error, LoopHandle, Options, Protocol, Transport};

const MS: fn(u64) -> Duration = Duration::from_millis;

struct Rig {
    eng: Arc<Engine>,
    lh: LoopHandle,
    join: Option<JoinHandle<twine::Result<()>>>,
}

fn rig(workers: usize) -> Rig {
    let eng = Arc::new(
        Engine::new(
            Options::default()
                .loops(1)
                .worker_pool(workers)
                .shutdown_timeout(MS(500)),
        )
        .unwrap(),
    );
    let lh = eng.next_loop();
    let e2 = eng.clone();
    let join = Some(std::thread::spawn(move || e2.run()));
    Rig { eng, lh, join }
}

impl Rig {
    fn stop(&mut self) {
        self.eng.stop();
        if let Some(j) = self.join.take() {
            j.join().unwrap().unwrap();
        }
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop();
    }
}

#[test]
fn then_ordering_runs_after_current_callback() {
    let r = rig(2);
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let l2 = log.clone();
    let out =
        r.lh.submit(move |lp| {
            let l = l2.clone();
            let d = Deferred::resolved(lp, 1).then(move |_, v| {
                l.lock().unwrap().push("then");
                v + 1
            });
            l2.lock().unwrap().push("task-end");
            d
        })
        .wait();
    assert_eq!(out.unwrap(), 2);
    assert_eq!(*log.lock().unwrap(), vec!["task-end", "then"]);
}

#[test]
fn resolve_never_runs_callback_inline() {
    let r = rig(2);
    let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let l2 = log.clone();
    let out =
        r.lh.submit(move |lp| {
            let (d, res) = lp.deferred_local::<i32>();
            let l = l2.clone();
            let d = d.then(move |_, v| {
                l.lock().unwrap().push("cb");
                v
            });
            res.resolve(5);
            l2.lock().unwrap().push("after-resolve");
            d
        })
        .wait();
    assert_eq!(out.unwrap(), 5);
    assert_eq!(*log.lock().unwrap(), vec!["after-resolve", "cb"]);
}

#[test]
fn chain_try_then_and_then_catch() {
    let r = rig(2);
    let v =
        r.lh.submit(|lp| {
            Deferred::resolved(lp, 3)
                .try_then(|_, v| Ok(v * 2))
                .and_then(|lp, v| lp.sleep(MS(5)).then(move |_, _| v + 1))
        })
        .wait();
    assert_eq!(v.unwrap(), 7);

    let v =
        r.lh.submit(|lp| {
            Deferred::resolved(lp, 3)
                .try_then(|_, _| Err::<i32, _>(Error::Closed))
                .then(|_, _| unreachable!("skipped on rejection"))
        })
        .wait();
    assert!(matches!(v, Err(Error::Closed)));

    // catch recovers; a value passes through catch untouched.
    let v =
        r.lh.submit(|lp| {
            Deferred::<i32>::rejected(lp, Error::Busy).catch(|_, e| {
                assert!(matches!(e, Error::Busy));
                Ok(9)
            })
        })
        .wait();
    assert_eq!(v.unwrap(), 9);
    let v =
        r.lh.submit(|lp| {
            Deferred::resolved(lp, 4).catch(|_, _| -> twine::Result<i32> { panic!("not called") })
        })
        .wait();
    assert_eq!(v.unwrap(), 4);

    // and_then whose inner Deferred rejects.
    let v =
        r.lh.submit(|lp| {
            Deferred::resolved(lp, 1)
                .and_then(|lp, _| Deferred::<i32>::rejected(lp, Error::IdleTimeout))
        })
        .wait();
    assert!(matches!(v, Err(Error::IdleTimeout)));
}

#[test]
fn finally_observes_both_outcomes() {
    let r = rig(2);
    let (tx, rx) = channel();
    let tx2 = tx.clone();
    r.lh.call(move |lp| {
        Deferred::resolved(lp, 1).finally(move |_, r| tx.send(r.is_ok()).unwrap());
        Deferred::<i32>::rejected(lp, Error::Busy)
            .finally(move |_, r| tx2.send(r.is_err()).unwrap());
    })
    .wait()
    .unwrap();
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap());
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap());
}

struct Tok(Arc<AtomicUsize>);
impl Drop for Tok {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

#[test]
fn value_moved_exactly_once() {
    let r = rig(2);
    let drops = Arc::new(AtomicUsize::new(0));
    let d2 = drops.clone();
    let tok =
        r.lh.submit(move |lp| {
            Deferred::resolved(lp, Tok(d2))
                .then(|_, t| t)
                .try_then(|_, t| Ok(t))
                .and_then(Deferred::resolved)
                .catch(|_, _| unreachable!())
        })
        .wait()
        .unwrap();
    assert_eq!(drops.load(SeqCst), 0, "value must not be dropped en route");
    drop(tok);
    assert_eq!(drops.load(SeqCst), 1);
}

#[test]
fn all_orders_and_rejects() {
    let r = rig(2);
    let v =
        r.lh.submit(|lp| {
            let ds: Vec<_> = [30u64, 1, 15, 5]
                .iter()
                .enumerate()
                .map(|(i, &ms)| lp.sleep(MS(ms)).then(move |_, _| i))
                .collect();
            all(lp, ds)
        })
        .wait();
    assert_eq!(v.unwrap(), vec![0, 1, 2, 3]);

    let v =
        r.lh.submit(|lp| all(lp, Vec::<Deferred<i32>>::new()))
            .wait();
    assert!(v.unwrap().is_empty());

    let v =
        r.lh.submit(|lp| {
            let a = Deferred::resolved(lp, 1);
            let b = Deferred::rejected(lp, Error::Closed);
            let c = Deferred::rejected(lp, Error::Busy);
            all(lp, vec![a, b, c])
        })
        .wait();
    assert!(matches!(v, Err(Error::Closed)), "first rejection wins");
}

#[test]
fn any_first_value_or_last_error() {
    let r = rig(2);
    let v =
        r.lh.submit(|lp| {
            let slow = lp.sleep(MS(80)).then(|_, _| 1);
            let fast = lp.sleep(MS(5)).then(|_, _| 2);
            let bad = Deferred::rejected(lp, Error::Busy);
            any(lp, vec![slow, bad, fast])
        })
        .wait();
    assert_eq!(v.unwrap(), 2);

    let v =
        r.lh.submit(|lp| {
            let a = Deferred::<i32>::rejected(lp, Error::Closed);
            let b = Deferred::<i32>::rejected(lp, Error::LoopStopped);
            let c = Deferred::<i32>::rejected(lp, Error::Busy);
            any(lp, vec![a, b, c])
        })
        .wait();
    assert!(matches!(v, Err(Error::Busy)), "last error: {v:?}");
}

#[test]
fn panicking_then_rejects_and_loop_survives() {
    let r = rig(2);
    let v =
        r.lh.submit(|lp| {
            Deferred::resolved(lp, 1)
                .then(|_, _| -> i32 { panic!("boom") })
                .then(|_, v| v + 1)
        })
        .wait();
    match v {
        Err(Error::Panic(p)) => assert_eq!(p.message, "boom"),
        other => panic!("expected Panic, got {other:?}"),
    }
    assert_eq!(r.lh.call(|_| 5).wait().unwrap(), 5);
}

#[test]
fn resolver_semantics() {
    let r = rig(2);
    // Dropped unsettled -> Abandoned.
    let v =
        r.lh.submit(|lp| {
            let (d, res) = lp.deferred::<i32>();
            drop(res);
            d
        })
        .wait();
    assert!(matches!(v, Err(Error::Abandoned)));
    // Only the last clone's drop abandons.
    let v =
        r.lh.submit(|lp| {
            let (d, res) = lp.deferred::<i32>();
            let r2 = res.clone();
            drop(res);
            std::thread::spawn(move || {
                std::thread::sleep(MS(20));
                r2.resolve(11);
            });
            d
        })
        .wait();
    assert_eq!(v.unwrap(), 11);
    // Resolving twice is a no-op; the first settle wins.
    let v =
        r.lh.submit(|lp| {
            let (d, res) = lp.deferred::<i32>();
            res.resolve(1);
            res.resolve(2);
            res.reject(Error::Closed);
            d
        })
        .wait();
    assert_eq!(v.unwrap(), 1);
    // LocalResolver abandoned.
    let v =
        r.lh.submit(|lp| {
            let (d, res) = lp.deferred_local::<i32>();
            drop(res);
            d
        })
        .wait();
    assert!(matches!(v, Err(Error::Abandoned)));
    // A foreign thread settles a Deferred.
    let v =
        r.lh.submit(|lp| {
            let (d, res) = lp.deferred::<String>();
            std::thread::spawn(move || res.resolve("hi".into()));
            d.then(|_, s| s + "!")
        })
        .wait();
    assert_eq!(v.unwrap(), "hi!");
    // is_done
    let v =
        r.lh.call(|lp| {
            let (d, res) = lp.deferred_local::<i32>();
            let before = d.is_done();
            res.resolve(1);
            let after = d.is_done();
            d.finally(|_, _| {});
            (before, after)
        })
        .wait()
        .unwrap();
    assert_eq!(v, (false, true));
}

#[test]
fn awaitable_wait_wait_timeout_try_take() {
    let r = rig(2);
    let aw = r.lh.submit(|lp| lp.sleep(MS(30)).then(|_, _| 3));
    assert!(aw.try_take().is_none());
    let aw = match aw.wait_timeout(MS(1)) {
        Err(aw) => aw,
        Ok(_) => panic!("should time out"),
    };
    assert_eq!(aw.wait().unwrap(), 3);
    let aw = r.lh.call(|_| 8);
    let got = loop {
        if let Some(v) = aw.try_take() {
            break v;
        }
        std::thread::sleep(MS(1));
    };
    assert_eq!(got.unwrap(), 8);
}

#[test]
fn wait_on_loop_thread_panics_with_await_on_loop() {
    let r = rig(2);
    let ok =
        r.lh.call(|lp| {
            let aw = lp.handle().call(|_| 1);
            match catch_unwind(AssertUnwindSafe(|| aw.wait())) {
                Err(p) => matches!(p.downcast_ref::<Error>(), Some(Error::AwaitOnLoop)),
                Ok(_) => false,
            }
        })
        .wait()
        .unwrap();
    assert!(ok);
}

struct ThreadWaker(std::thread::Thread);
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = Box::pin(f);
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(v) = Pin::new(&mut f).poll(&mut cx) {
            return v;
        }
        std::thread::park_timeout(Duration::from_secs(10));
    }
}

#[test]
fn awaitable_is_a_future() {
    let r = rig(2);
    let aw = r.lh.submit(|lp| lp.sleep(MS(20)).then(|_, _| "done"));
    assert_eq!(block_on(aw).unwrap(), "done");
    let aw = r.lh.call(|_| 77);
    assert_eq!(block_on(async move { aw.await.unwrap() + 1 }), 78);
}

#[test]
fn deferred_awaitable_forwards_outcome() {
    let r = rig(2);
    let aw =
        r.lh.call(|lp| Deferred::<i32>::rejected(lp, Error::Busy).awaitable())
            .wait()
            .unwrap();
    assert!(matches!(aw.wait(), Err(Error::Busy)));
}

#[test]
fn defer_to_thread_from_loop_and_foreign_thread() {
    let r = rig(4);
    let main = std::thread::current().id();
    let v =
        r.lh.submit(move |lp| {
            lp.defer_to_thread(move || {
                let t = std::thread::current();
                assert!(
                    t.name().unwrap().starts_with("twine-worker-"),
                    "{:?}",
                    t.name()
                );
                assert_ne!(t.id(), main);
                Ok(21)
            })
            .then(|_, v| v * 2)
        })
        .wait();
    assert_eq!(v.unwrap(), 42);

    // Err from the job rejects.
    let v =
        r.lh.submit(|lp| lp.defer_to_thread(|| Err::<i32, _>(Error::Busy)))
            .wait();
    assert!(matches!(v, Err(Error::Busy)));

    // Foreign thread: LoopHandle::submit from a plain thread.
    let lh = r.lh.clone();
    let v = std::thread::spawn(move || {
        lh.submit(|lp| lp.defer_to_thread(|| Ok("foreign")))
            .wait()
            .unwrap()
    })
    .join()
    .unwrap();
    assert_eq!(v, "foreign");
}

#[test]
fn defer_to_thread_panic_rejects() {
    let r = rig(2);
    let v =
        r.lh.submit(|lp| lp.defer_to_thread(|| -> twine::Result<i32> { panic!("worker boom") }))
            .wait();
    match v {
        Err(Error::Panic(p)) => assert_eq!(p.message, "worker boom"),
        other => panic!("expected Panic, got {other:?}"),
    }
    // The pool survives.
    let v = r.lh.submit(|lp| lp.defer_to_thread(|| Ok(1))).wait();
    assert_eq!(v.unwrap(), 1);
}

#[test]
fn nested_defer_to_thread_from_a_worker() {
    let r = rig(4);
    let lh = r.lh.clone();
    let v =
        r.lh.submit(move |lp| {
            lp.defer_to_thread(move || {
                let inner = lh.submit(|lp| lp.defer_to_thread(|| Ok(10))).wait()?;
                Ok(inner + 1)
            })
        })
        .wait();
    assert_eq!(v.unwrap(), 11);
}

#[test]
fn thousand_concurrent_defer_to_thread() {
    let r = rig(8);
    let v =
        r.lh.submit(|lp| {
            let ds: Vec<_> = (0..1000usize)
                .map(|i| {
                    lp.defer_to_thread(move || {
                        if i % 10 == 0 {
                            std::thread::sleep(Duration::from_micros(500));
                        }
                        Ok(i * 2)
                    })
                })
                .collect();
            all(lp, ds)
        })
        .wait()
        .unwrap();
    assert_eq!(v.len(), 1000);
    assert!(v.iter().enumerate().all(|(i, x)| *x == i * 2));
}

#[test]
fn pool_shutdown_cancels_queued_jobs_with_engine_stopped() {
    let mut r = rig(1);
    let (gate_tx, gate_rx) = channel::<()>();
    let (started_tx, started_rx) = channel::<()>();
    let (out_tx, out_rx) = channel();
    r.lh.call(move |lp| {
        // Occupies the only worker until released.
        lp.defer_to_thread(move || {
            started_tx.send(()).unwrap();
            let _ = gate_rx.recv_timeout(Duration::from_secs(10));
            Ok(())
        })
        .finally(|_, _| {});
        // Queued behind it; cancelled at engine stop.
        lp.defer_to_thread(|| Ok(1))
            .finally(move |_, r| out_tx.send(r).unwrap());
    })
    .wait()
    .unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    r.stop();
    let got = out_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(got, Err(Error::EngineStopped)), "{got:?}");
    let _ = gate_tx.send(());
}

#[test]
fn calls_after_stop_report_loop_stopped() {
    let mut r = rig(2);
    r.stop();
    assert!(matches!(r.lh.call(|_| 1).wait(), Err(Error::LoopStopped)));
}

#[test]
fn sleep_waits_at_least_the_duration() {
    let r = rig(2);
    let t0 = Instant::now();
    r.lh.submit(|lp| lp.sleep(MS(20))).wait().unwrap();
    assert!(t0.elapsed() >= MS(20), "{:?}", t0.elapsed());
    assert!(t0.elapsed() < Duration::from_secs(2));
}

struct Probe {
    got: Vec<i32>,
    id_slot: Arc<Mutex<Option<twine::ConnId>>>,
    lost: Arc<AtomicUsize>,
}

impl Protocol for Probe {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        *self.id_slot.lock().unwrap() = Some(t.id());
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
    fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<Error>) {
        self.lost.fetch_add(1, SeqCst);
    }
}

#[test]
fn on_conn_delivers_to_protocol_and_drops_if_closed() {
    let id_slot = Arc::new(Mutex::new(None));
    let lost = Arc::new(AtomicUsize::new(0));
    let (s2, l2) = (id_slot.clone(), lost.clone());
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Probe {
                got: vec![],
                id_slot: s2.clone(),
                lost: l2.clone(),
            })
        }),
    );
    let client = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || id_slot
        .lock()
        .unwrap()
        .is_some()));
    let id = id_slot.lock().unwrap().unwrap();
    let lh = srv.eng.next_loop();
    let (tx, rx) = channel();
    lh.call(move |lp| {
        lp.sleep(MS(5))
            .then(|_, _| 42)
            .on_conn::<Probe>(id, move |p, t, r| {
                p.got.push(r.unwrap());
                tx.send((p.got.clone(), t.id())).unwrap();
            });
    })
    .wait()
    .unwrap();
    let (got, tid) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(got, vec![42]);
    assert_eq!(tid, id);

    // Close first: the callback is dropped silently.
    drop(client);
    assert!(wait_until(Duration::from_secs(5), || lost.load(SeqCst) == 1));
    let called = Arc::new(AtomicUsize::new(0));
    let c2 = called.clone();
    lh.call(move |lp| {
        Deferred::resolved(lp, 1).on_conn::<Probe>(id, move |_, _, _| {
            c2.fetch_add(1, SeqCst);
        });
    })
    .wait()
    .unwrap();
    // A later round trip on the loop proves the queued callback has run (or not).
    lh.call(|_| ()).wait().unwrap();
    std::thread::sleep(MS(20));
    assert_eq!(called.load(SeqCst), 0);
    srv.stop();
}
