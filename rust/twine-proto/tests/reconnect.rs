//! `ReconnectingClient` against loopback servers.

mod common;

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use twine::{factory, LoopLocal, Protocol, Transport};
use twine_proto::{
    arg_str, Amp, AmpHandler, AmpReply, Args, ReconnectHandle, ReconnectOptions, ReconnectStatus,
    ReconnectingClient,
};

type Build = twine_proto::BuildFn;
type Slot = RefCell<Option<ReconnectHandle>>;

struct Sum;
impl AmpHandler for Sum {
    fn handle(&mut self, _t: &mut Transport<'_>, cmd: &str, a: Args) -> AmpReply {
        if cmd == "Sum" {
            let x: i64 = arg_str(&a, "a").unwrap().parse().unwrap();
            let y: i64 = arg_str(&a, "b").unwrap().parse().unwrap();
            AmpReply::Answer(twine_proto::args([("total", (x + y).to_string())]))
        } else {
            AmpReply::unhandled(cmd)
        }
    }
}

/// Closes every connection as soon as it is made.
struct Slam;
impl Protocol for Slam {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.lose_connection();
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

fn build_amp() -> Build {
    Rc::new(|_, _| Box::new(Amp::new(Sum)))
}

fn build_nop() -> Build {
    Rc::new(|_, _| Box::new(Slam))
}

fn slot(rig: &Rig) -> LoopLocal<Slot> {
    rig.eng.loop_local(|| RefCell::new(None))
}

/// Starts a client on loop 0. `mk` runs on the loop (build closures and options
/// are `!Send`).
fn start(
    rig: &Rig,
    key: LoopLocal<Slot>,
    addr: SocketAddr,
    mk: impl FnOnce() -> (Build, ReconnectOptions) + Send + 'static,
) -> ReconnectStatus {
    rig.lh(0)
        .call(move |lp| {
            let (b, o) = mk();
            let h = ReconnectingClient::start(lp, addr, b, o);
            let st = h.status();
            *lp.local(key).borrow_mut() = Some(h);
            st
        })
        .wait()
        .unwrap()
}

fn stop(rig: &Rig, key: LoopLocal<Slot>) {
    rig.lh(0)
        .call(move |lp| {
            let h = lp.local(key).borrow().clone().unwrap();
            h.stop_trying(lp);
        })
        .wait()
        .unwrap();
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn fast() -> ReconnectOptions {
    ReconnectOptions::default()
        .initial_delay(ms(30))
        .factor(2.0)
        .jitter(false)
}

#[test]
fn connects_after_server_appears_and_serves_calls() {
    let rig = Rig::start(1);
    let addr = closed_addr();
    let key = slot(&rig);
    let st = start(&rig, key, addr, || (build_amp(), fast()));
    assert!(wait_until(Duration::from_secs(5), || st.attempts() >= 2));
    assert!(!st.connected());
    assert!(st.last_error().is_some());
    let _lst = rig.listen(&addr.to_string(), factory(|_, _| Box::new(Amp::new(Sum))));
    assert!(wait_until(Duration::from_secs(10), || st.connected()));
    assert!(st.attempts() >= 3);
    // The wrapper forwards `visit`, so calls reach the inner Amp.
    let r = rig
        .lh(0)
        .submit(move |lp| {
            let h = lp.local(key).borrow().clone().unwrap();
            let id = h.conn_id().expect("connected");
            Amp::<Sum>::call_on(lp, id, "Sum", twine_proto::args([("a", "20"), ("b", "22")]))
        })
        .wait()
        .unwrap();
    assert_eq!(arg_str(&r, "total"), Some("42"));
    stop(&rig, key);
}

#[test]
fn delays_grow_without_jitter_and_stop_trying_stops() {
    let rig = Rig::start(1);
    let addr = closed_addr();
    let key = slot(&rig);
    let st = start(&rig, key, addr, || (build_nop(), fast()));
    assert!(wait_until(Duration::from_secs(10), || st.attempts() >= 5));
    stop(&rig, key);
    let d = st.delays();
    assert!(d.len() >= 4, "{d:?}");
    assert_eq!(&d[..4], &[ms(30), ms(60), ms(120), ms(240)]);
    assert!(d.windows(2).all(|w| w[1] >= w[0]));
    let n = st.attempts();
    std::thread::sleep(ms(600));
    assert_eq!(st.attempts(), n, "attempts continued after stop_trying");
    assert!(!st.gave_up());
}

#[test]
fn jitter_stays_within_ceiling() {
    let rig = Rig::start(1);
    let addr = closed_addr();
    let key = slot(&rig);
    let st = start(&rig, key, addr, || {
        (
            build_nop(),
            ReconnectOptions::default()
                .initial_delay(ms(2))
                .factor(1.5)
                .max_delay(ms(10))
                .seed(7),
        )
    });
    assert!(wait_until(Duration::from_secs(10), || st.attempts() >= 15));
    stop(&rig, key);
    let d = st.delays();
    assert!(d.iter().all(|x| *x <= ms(10)), "{d:?}");
    assert!(d.iter().any(|x| *x != d[0]), "no jitter: {d:?}");
}

#[test]
fn server_close_triggers_reconnect_and_resets_delay() {
    let rig = Rig::start(1);
    let _lst = rig.listen("127.0.0.1:0", factory(|_, _| Box::new(Slam)));
    let addr = _lst.addr();
    let key = slot(&rig);
    let seq = Arc::new(Mutex::new(Vec::<bool>::new()));
    let s2 = seq.clone();
    let st = start(&rig, key, addr, move || {
        (
            build_nop(),
            fast().on_state_change(move |_, up| s2.lock().unwrap().push(up)),
        )
    });
    assert!(wait_until(Duration::from_secs(10), || {
        seq.lock().unwrap().len() >= 6
    }));
    stop(&rig, key);
    let v = seq.lock().unwrap().clone();
    for (i, up) in v.iter().enumerate() {
        assert_eq!(*up, i % 2 == 0, "sequence {v:?}");
    }
    // Every successful connect resets the backoff, so delays never grow.
    let d = st.delays();
    assert!(d.iter().all(|x| *x == ms(30)), "{d:?}");
    assert!(!st.gave_up());
}

#[test]
fn max_retries_against_closed_port() {
    let rig = Rig::start(1);
    let addr = closed_addr();
    let key = slot(&rig);
    let seq = Arc::new(Mutex::new(Vec::<bool>::new()));
    let s2 = seq.clone();
    let st = start(&rig, key, addr, move || {
        (
            build_nop(),
            fast()
                .initial_delay(ms(5))
                .max_retries(2)
                .on_state_change(move |_, up| s2.lock().unwrap().push(up)),
        )
    });
    assert!(wait_until(Duration::from_secs(10), || st.gave_up()));
    std::thread::sleep(ms(200));
    assert_eq!(st.attempts(), 3, "initial attempt plus two retries");
    assert!(!st.connected());
    assert!(seq.lock().unwrap().is_empty());
    assert!(st.last_error().is_some());
    assert_eq!(st.delays().len(), 2);
}

#[test]
fn state_changes_then_give_up() {
    let rig = Rig::start(1);
    let lst = rig.listen("127.0.0.1:0", factory(|_, _| Box::new(Slam)));
    let addr = lst.addr();
    let key = slot(&rig);
    let seq = Arc::new(Mutex::new(Vec::<bool>::new()));
    let s2 = seq.clone();
    let st = start(&rig, key, addr, move || {
        (
            build_nop(),
            fast()
                .initial_delay(ms(40))
                .max_retries(2)
                .on_state_change(move |_, up| s2.lock().unwrap().push(up)),
        )
    });
    assert!(wait_until(Duration::from_secs(10), || {
        !seq.lock().unwrap().is_empty()
    }));
    lst.close();
    assert!(wait_until(Duration::from_secs(10), || st.gave_up()));
    let v = seq.lock().unwrap().clone();
    assert!(v.len() >= 2 && v.len() % 2 == 0, "{v:?}");
    for (i, up) in v.iter().enumerate() {
        assert_eq!(*up, i % 2 == 0, "sequence {v:?}");
    }
    assert!(!st.connected());
}

/// `gave_up` as seen from a callback on the loop.
fn gave_up_in(lp: &mut twine::Loop, key: LoopLocal<Slot>) -> bool {
    lp.local(key).borrow().as_ref().is_some_and(|h| h.gave_up())
}

#[test]
fn give_up_callback_observes_gave_up_and_connect_failures_are_reported() {
    let rig = Rig::start(1);
    let addr = closed_addr();
    let key = slot(&rig);
    // (what, gave_up as seen by the callback)
    let log = Arc::new(Mutex::new(Vec::<(&'static str, bool)>::new()));
    let (l1, l2) = (log.clone(), log.clone());
    let st = start(&rig, key, addr, move || {
        let o = fast()
            .initial_delay(ms(5))
            .max_retries(1)
            .on_connect_failed(move |lp, e| {
                assert!(!e.to_string().is_empty());
                l1.lock().unwrap().push(("fail", gave_up_in(lp, key)));
            })
            .on_give_up(move |lp| l2.lock().unwrap().push(("give_up", gave_up_in(lp, key))));
        (build_nop(), o)
    });
    assert!(wait_until(Duration::from_secs(10), || st.gave_up()));
    assert!(wait_until(Duration::from_secs(5), || log
        .lock()
        .unwrap()
        .len()
        == 3));
    std::thread::sleep(ms(100));
    // Two failed dials (initial + one retry), then exactly one give-up.
    assert_eq!(
        *log.lock().unwrap(),
        vec![("fail", false), ("fail", false), ("give_up", true)]
    );
}

/// Holds every connection open.
struct Hold;
impl Protocol for Hold {
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

#[test]
fn final_state_change_and_give_up_see_gave_up_on_shutdown() {
    let rig = Rig::start(1);
    // The server never reads and the client floods it, so the graceful close at
    // engine stop cannot finish and the deadline aborts with `Error::Shutdown`.
    let lst = rig.listen("127.0.0.1:0", factory(|_, _| Box::new(Deaf)));
    let addr = lst.addr();
    let key = slot(&rig);
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let (l1, l2) = (log.clone(), log.clone());
    let st = start(&rig, key, addr, move || {
        let o = fast()
            .on_state_change(move |lp, up| {
                l1.lock()
                    .unwrap()
                    .push(format!("state:{up}:{}", gave_up_in(lp, key)));
            })
            .on_give_up(move |lp| {
                l2.lock()
                    .unwrap()
                    .push(format!("give_up:{}", gave_up_in(lp, key)))
            });
        (Rc::new(|_, _| Box::new(Flood)), o)
    });
    assert!(wait_until(Duration::from_secs(10), || st.connected()));
    rig.eng.stop();
    assert!(wait_until(Duration::from_secs(10), || st.gave_up()));
    assert!(wait_until(Duration::from_secs(5), || log
        .lock()
        .unwrap()
        .len()
        >= 3));
    assert_eq!(
        *log.lock().unwrap(),
        vec!["state:true:false", "state:false:true", "give_up:true"]
    );
}

/// Never reads.
struct Deaf;
impl Protocol for Deaf {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.pause_reading();
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

/// Writes more than the socket buffers hold (but less than the 16 MiB cap).
struct Flood;
impl Protocol for Flood {
    fn connection_made(&mut self, t: &mut Transport<'_>) {
        t.write(&vec![0u8; 12 << 20]);
    }
    fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
}

fn build_nop_hold() -> Build {
    Rc::new(|_, _| Box::new(Hold))
}

#[test]
fn stop_trying_during_dial_aborts_the_connection_quietly() {
    let rig = Rig::start(1);
    let closed = Arc::new(Mutex::new(0usize));
    struct Count(Arc<Mutex<usize>>);
    impl Protocol for Count {
        fn data_received(&mut self, _t: &mut Transport<'_>, _d: &mut [u8]) {}
        fn connection_lost(&mut self, _t: &mut Transport<'_>, _r: Option<twine::Error>) {
            *self.0.lock().unwrap() += 1;
        }
    }
    let c2 = closed.clone();
    let lst = rig.listen(
        "127.0.0.1:0",
        factory(move |_, _| Box::new(Count(c2.clone()))),
    );
    let addr = lst.addr();
    let key = slot(&rig);
    let seq = Arc::new(Mutex::new(Vec::<bool>::new()));
    let (s2, made) = (seq.clone(), Arc::new(Mutex::new(0usize)));
    let m2 = made.clone();
    let st = rig
        .lh(0)
        .call(move |lp| {
            let build: Build = Rc::new(move |_, _| {
                *m2.lock().unwrap() += 1;
                Box::new(Hold)
            });
            let o = fast().on_state_change(move |_, up| s2.lock().unwrap().push(up));
            let h = ReconnectingClient::start(lp, addr, build, o);
            // The dial is in flight; stop before it completes.
            h.stop_trying(lp);
            let st = h.status();
            *lp.local(key).borrow_mut() = Some(h);
            st
        })
        .wait()
        .unwrap();
    // The server sees the connection and then its abort.
    assert!(wait_until(Duration::from_secs(10), || *closed
        .lock()
        .unwrap()
        == 1));
    std::thread::sleep(ms(150));
    assert!(seq.lock().unwrap().is_empty(), "{:?}", seq.lock().unwrap());
    assert!(!st.connected());
    assert!(!st.gave_up());
    assert_eq!(st.attempts(), 1, "no retry after stop_trying");
    let conn = rig
        .lh(0)
        .call(move |lp| lp.local(key).borrow().as_ref().unwrap().conn_id())
        .wait()
        .unwrap();
    assert!(conn.is_none());
}

#[test]
fn disconnect_aborts_and_the_client_reconnects() {
    let rig = Rig::start(1);
    let lst = rig.listen("127.0.0.1:0", factory(|_, _| Box::new(Hold)));
    let addr = lst.addr();
    let key = slot(&rig);
    let seq = Arc::new(Mutex::new(Vec::<bool>::new()));
    let s2 = seq.clone();
    let st = start(&rig, key, addr, move || {
        (
            build_nop_hold(),
            fast().on_state_change(move |_, up| s2.lock().unwrap().push(up)),
        )
    });
    assert!(wait_until(Duration::from_secs(10), || st.connected()));
    rig.lh(0)
        .call(move |lp| {
            let h = lp.local(key).borrow().clone().unwrap();
            h.disconnect(lp);
            h.disconnect(lp); // idempotent
        })
        .wait()
        .unwrap();
    assert!(wait_until(Duration::from_secs(10), || {
        seq.lock().unwrap().len() >= 3
    }));
    assert_eq!(seq.lock().unwrap()[..3], [true, false, true]);
    // With stop_trying first, disconnect stays down.
    rig.lh(0)
        .call(move |lp| {
            let h = lp.local(key).borrow().clone().unwrap();
            h.stop_trying(lp);
            h.disconnect(lp);
        })
        .wait()
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || !st.connected()));
    std::thread::sleep(ms(200));
    assert!(!st.connected());
    assert!(!st.gave_up());
}
