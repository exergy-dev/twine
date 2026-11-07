//! [`Broadcast`]: loop-aware fan-out to a set of connections.
//!
//! Members are grouped by the loop that owns them. [`Broadcast::send`] issues
//! **one** `LoopHandle::call_from_thread` per loop, carrying a shared
//! `Arc<[u8]>` and that loop's member ids; on the loop each member is written
//! with `lp.conn(id)` and `Transport::write` (no per-member allocation or copy of
//! the message by this module).
//!
//! # Bookkeeping choice
//!
//! The member table is a mutex-guarded map owned by the `Broadcast` (shared
//! through an `Arc`), rather than per-loop `LoopLocal` shards. That keeps `add`,
//! `remove` and `len` callable from any thread. Each loop's shard holds a member
//! vector plus a `HashMap<ConnId, usize>` index, so `add` and `remove` are O(1)
//! (swap-remove); the `Arc<[ConnId]>` snapshot used by `send` is rebuilt lazily
//! after membership changes, so a send that follows no change allocates one task
//! box per loop and nothing per member.
//!
//! Members whose connection is gone are dropped lazily: when the loop task finds
//! `lp.conn(id)` is `None` it removes that id from the table (taking the mutex,
//! briefly, on the loop thread). If a loop has stopped, all of its members are
//! removed. Call [`Broadcast::remove`] from `connection_lost` for eager removal.
//!
//! # One `Broadcast` per engine
//!
//! Shards are keyed by [`ConnId::loop_id`], which is an index *within one
//! engine*; two engines in one process both have a loop 0. `twine` offers no
//! way to compare `LoopHandle`s for identity, so a `Broadcast` must only hold
//! connections of a single engine. `add` enforces this where it can: when it
//! runs on a loop thread (the normal case, `connection_made`) and a shard with
//! the same loop index belongs to a different, still running loop, the
//! connection is **not** added and a warning is logged. A shard whose loop has
//! stopped is replaced. An `add` from a non-loop thread cannot be checked.
//!
//! # Ordering and delivery
//!
//! [`Broadcast::send`] is always asynchronous, even for a member on the calling
//! thread's own loop: the write happens in a later loop task, so it is **not**
//! ordered relative to a direct `Transport::write` on the same connection made
//! by the caller (a `write` after `send` may reach the peer first). Messages
//! sent from one thread arrive at each member in `send` order.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use twine::{ConnId, LoopHandle, TransportHandle};

struct Shard {
    lh: LoopHandle,
    members: Vec<ConnId>,
    /// `members[index[id]] == id`.
    index: HashMap<ConnId, usize>,
    snapshot: Option<Arc<[ConnId]>>,
}

impl Shard {
    /// Removes `id` (swap-remove); true if it was a member.
    fn remove(&mut self, id: ConnId) -> bool {
        let Some(i) = self.index.remove(&id) else {
            return false;
        };
        self.members.swap_remove(i);
        if let Some(moved) = self.members.get(i) {
            self.index.insert(*moved, i);
        }
        self.snapshot = None;
        true
    }
}

#[derive(Default)]
struct Table {
    shards: HashMap<usize, Shard>,
    len: usize,
}

struct Inner {
    table: Mutex<Table>,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn remove_ids<'a>(&self, loop_id: usize, ids: impl IntoIterator<Item = &'a ConnId>) {
        let mut t = self.lock();
        let Some(sh) = t.shards.get_mut(&loop_id) else {
            return;
        };
        let removed = ids.into_iter().filter(|id| sh.remove(**id)).count();
        if sh.members.is_empty() {
            t.shards.remove(&loop_id);
        }
        t.len -= removed;
    }
}

/// A shared set of connections that can be written to as one. `Send + Sync` and
/// `Clone` (clones share the same set); wrap it in an `Arc` or clone it into each
/// protocol.
#[derive(Clone)]
pub struct Broadcast {
    inner: Arc<Inner>,
}

impl Default for Broadcast {
    fn default() -> Self {
        Broadcast::new()
    }
}

impl Broadcast {
    /// An empty set.
    pub fn new() -> Broadcast {
        Broadcast {
            inner: Arc::new(Inner {
                table: Mutex::new(Table::default()),
            }),
        }
    }

    /// Adds a connection (idempotent). May be called from any thread, typically
    /// from `connection_made` with `t.handle()`.
    ///
    /// All members must belong to one engine (see the module docs). Called on a
    /// loop thread, a connection from a second, running engine is refused with
    /// a logged warning.
    pub fn add(&self, h: TransportHandle) {
        let id = h.id();
        let lid = id.loop_id();
        let mut t = self.inner.lock();
        if let Some(sh) = t.shards.get(&lid) {
            // `in_loop` is only true for the loop running this thread, which is
            // the one way to tell two same-index loops of different engines apart.
            if h.loop_handle().in_loop() && !sh.lh.in_loop() {
                if !sh.lh.is_stopped() {
                    log::warn!(
                        "twine-proto: Broadcast::add: loop {lid} of a second engine; \
                         one Broadcast per engine, connection not added"
                    );
                    return;
                }
                // The old engine's loop is gone: drop its members.
                let old = t.shards.remove(&lid).map_or(0, |s| s.members.len());
                t.len -= old;
            }
        }
        let sh = t.shards.entry(lid).or_insert_with(|| Shard {
            lh: h.loop_handle().clone(),
            members: Vec::new(),
            index: HashMap::new(),
            snapshot: None,
        });
        if sh.index.contains_key(&id) {
            return;
        }
        sh.index.insert(id, sh.members.len());
        sh.members.push(id);
        sh.snapshot = None;
        t.len += 1;
    }

    /// Removes a connection (no-op if absent).
    pub fn remove(&self, h: &TransportHandle) {
        self.remove_id(h.id());
    }

    /// Removes a connection by id (no-op if absent).
    pub fn remove_id(&self, id: ConnId) {
        self.inner.remove_ids(id.loop_id(), &[id]);
    }

    /// The number of members, including ones that closed but have not yet been
    /// pruned by a `send`.
    pub fn len(&self) -> usize {
        self.inner.lock().len
    }

    /// True when there are no members.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Writes `msg` to every member. Callable from any thread, including loop
    /// threads. Delivery is asynchronous, even for members on the caller's own
    /// loop, so the write is not ordered relative to a direct `Transport::write`
    /// (see the module docs); per-member ordering follows the order of `send`
    /// calls made from one thread.
    ///
    /// `msg` is copied once into a shared `Arc<[u8]>` (`&[u8]` and `Vec<u8>` each
    /// cost exactly one copy; pass an `Arc<[u8]>` to share an existing buffer
    /// with no copy). Every member is then written from that one buffer.
    pub fn send(&self, msg: impl Into<Arc<[u8]>>) {
        self.fan_out(msg.into(), None);
    }

    /// Like [`send`](Self::send), skipping the member `except` (the sender).
    pub fn send_from(&self, msg: impl Into<Arc<[u8]>>, except: ConnId) {
        self.fan_out(msg.into(), Some(except));
    }

    fn fan_out(&self, msg: Arc<[u8]>, except: Option<ConnId>) {
        let work: Vec<(usize, LoopHandle, Arc<[ConnId]>)> = {
            let mut t = self.inner.lock();
            t.shards
                .iter_mut()
                .map(|(lid, sh)| {
                    let snap = sh
                        .snapshot
                        .get_or_insert_with(|| Arc::from(sh.members.as_slice()))
                        .clone();
                    (*lid, sh.lh.clone(), snap)
                })
                .collect()
        };
        for (lid, lh, ids) in work {
            let inner = self.inner.clone();
            let msg = msg.clone();
            let ids2 = ids.clone();
            let r = lh.call_from_thread(move |lp| {
                let mut dead: HashSet<ConnId> = HashSet::new();
                for &id in ids2.iter() {
                    if Some(id) == except {
                        // Still a live member unless it is gone.
                        if lp.conn(id).is_none() {
                            dead.insert(id);
                        }
                        continue;
                    }
                    match lp.conn(id) {
                        Some(mut t) => t.write(&msg),
                        None => {
                            dead.insert(id);
                        }
                    }
                }
                if !dead.is_empty() {
                    inner.remove_ids(lid, dead.iter());
                }
            });
            if r.is_err() {
                // The loop has exited: none of its connections survive.
                self.inner.remove_ids(lid, ids.iter());
            }
        }
    }
}
