//! `Loop::with_protocol`, `Loop::conn`, `Loop::local` and `stop()` before `run()`.

mod common;

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, ConnId, Engine, Error, LoopLocal, Options, Protocol, Transport};

#[derive(Default)]
struct Probe {
    results: Mutex<Vec<String>>,
    ids: Mutex<Vec<ConnId>>,
    made: AtomicUsize,
}

struct Prober {
    st: Arc<Probe>,
    tag: u32,
}

impl Protocol for Prober {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        self.st.ids.lock().unwrap().push(t.id());
        self.st.made.fetch_add(1, SeqCst);
    }
    fn data_received(&mut self, t: &mut Transport<'_>, _d: &mut [u8]) {
        let me = t.id();
        let ids = self.st.ids.lock().unwrap().clone();
        let mut out = Vec::new();
        // Re-entering our own protocol is Busy.
        let own = t.loop_mut().with_protocol::<Prober, _>(me, |_, _| ());
        out.push(format!("own:{}", matches!(own, Err(Error::Busy))));
        // A sibling connection on the same loop is reachable synchronously.
        let other = ids.iter().copied().find(|i| *i != me).unwrap();
        let tag = t.loop_mut().with_protocol::<Prober, _>(other, |p, ot| {
            ot.write(b"hi from sibling");
            p.tag
        });
        out.push(format!("other:{tag:?}"));
        // Wrong type: an Io error, not a panic.
        let wrong = t.loop_mut().with_protocol::<Wrong, _>(other, |_, _| ());
        out.push(format!("wrong:{}", matches!(wrong, Err(Error::Io(_)))));
        self.st.results.lock().unwrap().extend(out);
        t.loop_mut().call_soon(|_| {});
    }
}

struct Wrong;
impl Protocol for Wrong {
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

#[test]
fn with_protocol_busy_other_wrong_type_and_closed() {
    let st = Arc::new(Probe::default());
    let s2 = st.clone();
    let tag = Arc::new(AtomicUsize::new(10));
    let mut srv = Server::start(
        1,
        factory(move |_, _| {
            Box::new(Prober {
                st: s2.clone(),
                tag: tag.fetch_add(1, SeqCst) as u32,
            })
        }),
    );
    let mut a = srv.connect();
    let mut b = srv.connect();
    assert!(wait_until(Duration::from_secs(5), || st.made.load(SeqCst) == 2));
    use std::io::Write;
    a.write_all(b"go").unwrap();
    assert_eq!(read_n(&mut b, 15), b"hi from sibling");
    assert!(wait_until(Duration::from_secs(5), || st
        .results
        .lock()
        .unwrap()
        .len()
        == 3));
    let r = st.results.lock().unwrap().clone();
    assert_eq!(r[0], "own:true");
    assert!(r[1].starts_with("other:Ok("), "{}", r[1]);
    assert_eq!(r[2], "wrong:true");
    // After b closes, its id is stale: with_protocol / conn report Closed / None.
    let b_id = st
        .ids
        .lock()
        .unwrap()
        .iter()
        .copied()
        .find(|i| i.loop_id() == 0)
        .unwrap();
    drop(b);
    drop(a);
    assert!(wait_until(Duration::from_secs(5), || srv.eng.metrics()[0]
        .closes
        == 2));
    let (tx, rx) = std::sync::mpsc::channel();
    srv.eng.loop_handles()[0]
        .call_from_thread(move |lp| {
            let closed = matches!(
                lp.with_protocol::<Prober, _>(b_id, |_, _| ()),
                Err(Error::Closed)
            );
            let none = lp.conn(b_id).is_none();
            let _ = tx.send((closed, none));
        })
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        (true, true)
    );
    srv.stop();
}

#[test]
fn loop_local_is_lazy_once_per_loop_and_shared_within_a_loop() {
    let eng = Arc::new(Engine::new(Options::default().loops(2)).unwrap());
    let inits = Arc::new(AtomicUsize::new(0));
    let i2 = inits.clone();
    let key: LoopLocal<Cell<u32>> = eng.loop_local(move || {
        i2.fetch_add(1, SeqCst);
        Cell::new(0)
    });
    assert_eq!(inits.load(SeqCst), 0, "lazy");
    let e2 = eng.clone();
    let j = std::thread::spawn(move || e2.run());
    let (tx, rx) = std::sync::mpsc::channel();
    for lh in eng.loop_handles() {
        let tx = tx.clone();
        lh.call_from_thread(move |lp| {
            let a = lp.local(key);
            a.set(a.get() + 1);
            let b = lp.local(key);
            assert!(std::rc::Rc::ptr_eq(&a, &b));
            let _ = tx.send((lp.id(), b.get()));
        })
        .unwrap();
    }
    let mut got = vec![rx.recv().unwrap(), rx.recv().unwrap()];
    got.sort();
    assert_eq!(got, vec![(0, 1), (1, 1)], "each loop has its own value");
    assert_eq!(inits.load(SeqCst), 2);
    eng.stop();
    j.join().unwrap().unwrap();
}

#[test]
fn call_from_thread_after_stop_before_run_is_loop_stopped() {
    let eng = Engine::new(Options::default().loops(1)).unwrap();
    eng.stop();
    let lh = eng.loop_handles()[0].clone();
    assert!(matches!(
        lh.call_from_thread(|_| {}),
        Err(Error::LoopStopped)
    ));
    assert!(lh.is_stopped());
}
