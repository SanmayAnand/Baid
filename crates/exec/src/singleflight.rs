//! Single-flight: concurrent loads of the same key share one backend call.
//!
//! The first caller for a key (the *leader*) creates the load future and drives it; callers
//! that arrive while it is in flight (*followers*) await the same future. The entry leaves
//! the map when the load completes, success or error.
//!
//! # Cancellation
//!
//! A load is driven by whoever awaits it, so it keeps going while *anyone* is still waiting:
//! if the leader's request is cancelled, a follower carries it on. Each flight counts its
//! waiters, and when the last one goes away before completion the entry is removed and the
//! load future is dropped, cancelling the backend call. Without that, an abandoned future
//! would sit in the map and the next caller would resume a call whose deadline had passed.

use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use baid_core::Key;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};

struct Flight<T: Clone> {
    fut: Shared<BoxFuture<'static, T>>,
    waiters: Arc<AtomicUsize>,
    gen: u64,
}

/// Whether this caller started the load or joined one already in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Created and first drove the load.
    Leader,
    /// Joined a load another caller started.
    Follower,
}

/// The in-flight loads, by key.
pub struct SingleFlight<T: Clone> {
    map: DashMap<Key, Flight<T>>,
    next_gen: AtomicU64,
}

impl<T: Clone + Send + Sync + 'static> Default for SingleFlight<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Decrements the waiter count on drop; the last waiter of an unfinished flight removes it.
struct Waiter<'a, T: Clone> {
    map: &'a DashMap<Key, Flight<T>>,
    key: &'a Key,
    gen: u64,
    waiters: Arc<AtomicUsize>,
    finished: bool,
}

impl<T: Clone> Drop for Waiter<'_, T> {
    fn drop(&mut self) {
        let gen = self.gen;
        if self.finished {
            // Done: whoever finishes first removes the entry; later calls are no-ops, and a
            // newer flight for the same key (another generation) is left alone.
            self.map.remove_if(self.key, |_, f| f.gen == gen);
        } else if self.waiters.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Last waiter gave up. Re-check under the shard lock: a caller may have joined
            // between the decrement and here.
            self.map.remove_if(self.key, |_, f| {
                f.gen == gen && f.waiters.load(Ordering::Acquire) == 0
            });
        }
    }
}

impl<T: Clone + Send + Sync + 'static> SingleFlight<T> {
    /// No loads in flight.
    pub fn new() -> Self {
        SingleFlight {
            map: DashMap::new(),
            next_gen: AtomicU64::new(0),
        }
    }

    /// Runs `make()` for `key`, or joins the run already in flight. `make` is called only by
    /// the leader.
    pub async fn run<F, Fut>(&self, key: &Key, make: F) -> (T, Role)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T> + Send + 'static,
    {
        let (fut, waiters, gen, role) = match self.map.entry(key.clone()) {
            Entry::Occupied(e) => {
                let f = e.get();
                f.waiters.fetch_add(1, Ordering::AcqRel);
                (f.fut.clone(), f.waiters.clone(), f.gen, Role::Follower)
            }
            Entry::Vacant(v) => {
                let gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
                let fut = make().boxed().shared();
                let waiters = Arc::new(AtomicUsize::new(1));
                v.insert(Flight {
                    fut: fut.clone(),
                    waiters: waiters.clone(),
                    gen,
                });
                (fut, waiters, gen, Role::Leader)
            }
        }; // the shard lock is released here, before any await
        let mut waiter = Waiter {
            map: &self.map,
            key,
            gen,
            waiters,
            finished: false,
        };
        let out = fut.await;
        waiter.finished = true;
        drop(waiter);
        (out, role)
    }

    /// Loads currently in flight.
    pub fn in_flight(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use baid_core::{AdapterId, BackendQuery, EntityId, TenantId};
    use tokio::sync::Notify;

    use super::*;

    fn key(s: &str) -> Key {
        Key::derive(
            &TenantId::new("t").unwrap(),
            &AdapterId::new("a").unwrap(),
            &BackendQuery::Entity {
                id: EntityId::new(s).unwrap(),
            },
        )
    }

    /// A load that waits for `gate`, counts how often it was created, and records whether
    /// it was dropped before finishing.
    fn load(
        calls: &Arc<AtomicUsize>,
        gate: &Arc<Notify>,
        dropped: &Arc<AtomicBool>,
        value: u32,
    ) -> impl Future<Output = Result<u32, String>> + Send + 'static {
        calls.fetch_add(1, Ordering::SeqCst);
        let (gate, dropped) = (gate.clone(), dropped.clone());
        async move {
            struct OnDrop(Arc<AtomicBool>, bool);
            impl Drop for OnDrop {
                fn drop(&mut self) {
                    if !self.1 {
                        self.0.store(true, Ordering::SeqCst);
                    }
                }
            }
            let mut guard = OnDrop(dropped, false);
            gate.notified().await;
            guard.1 = true;
            Ok(value)
        }
    }

    struct Rig {
        sf: Arc<SingleFlight<Result<u32, String>>>,
        calls: Arc<AtomicUsize>,
        gate: Arc<Notify>,
        dropped: Arc<AtomicBool>,
    }

    fn rig() -> Rig {
        Rig {
            sf: Arc::new(SingleFlight::new()),
            calls: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Notify::new()),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }

    impl Rig {
        fn spawn(
            &self,
            k: &str,
            value: u32,
        ) -> tokio::task::JoinHandle<(Result<u32, String>, Role)> {
            let (sf, calls, gate, dropped, k) = (
                self.sf.clone(),
                self.calls.clone(),
                self.gate.clone(),
                self.dropped.clone(),
                key(k),
            );
            // spawn-ok: test task, joined or aborted before the test ends
            tokio::spawn(async move { sf.run(&k, || load(&calls, &gate, &dropped, value)).await })
        }
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_load() {
        let r = rig();
        let handles: Vec<_> = (0..10).map(|i| r.spawn("k", i)).collect();
        settle().await;
        assert_eq!(r.sf.in_flight(), 1);
        r.gate.notify_waiters();
        let mut leaders = 0;
        for h in handles {
            let (v, role) = h.await.unwrap();
            assert_eq!(v, Ok(0), "everyone gets the leader's result");
            leaders += usize::from(role == Role::Leader);
        }
        assert_eq!(r.calls.load(Ordering::SeqCst), 1, "one backend call");
        assert_eq!(leaders, 1);
        assert_eq!(r.sf.in_flight(), 0, "entry removed on completion");
    }

    #[tokio::test]
    async fn different_keys_load_separately() {
        let r = rig();
        let a = r.spawn("a", 1);
        let b = r.spawn("b", 2);
        settle().await;
        assert_eq!(r.sf.in_flight(), 2);
        r.gate.notify_waiters();
        assert_eq!(a.await.unwrap().0, Ok(1));
        assert_eq!(b.await.unwrap().0, Ok(2));
        assert_eq!(r.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn errors_reach_every_waiter_and_are_not_cached() {
        let sf: SingleFlight<Result<u32, String>> = SingleFlight::new();
        let k = key("k");
        let (v, _) = sf
            .run(&k, || async { Err::<u32, _>("down".to_owned()) })
            .await;
        assert_eq!(v, Err("down".into()));
        assert_eq!(sf.in_flight(), 0);
        let (v, role) = sf.run(&k, || async { Ok(7) }).await;
        assert_eq!(
            (v, role),
            (Ok(7), Role::Leader),
            "a failure is not remembered"
        );
    }

    #[tokio::test]
    async fn a_follower_carries_on_when_the_leader_is_cancelled() {
        let r = rig();
        let leader = r.spawn("k", 1);
        settle().await;
        let follower = r.spawn("k", 2);
        settle().await;
        leader.abort();
        settle().await;
        assert_eq!(r.sf.in_flight(), 1, "still wanted by the follower");
        assert!(!r.dropped.load(Ordering::SeqCst), "load not cancelled");
        r.gate.notify_waiters();
        assert_eq!(follower.await.unwrap(), (Ok(1), Role::Follower));
        assert_eq!(r.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_abandoned_load_is_cancelled_and_forgotten() {
        let r = rig();
        let a = r.spawn("k", 1);
        let b = r.spawn("k", 2);
        settle().await;
        a.abort();
        b.abort();
        settle().await;
        assert_eq!(r.sf.in_flight(), 0, "no waiters: entry removed");
        assert!(
            r.dropped.load(Ordering::SeqCst),
            "and the load itself dropped"
        );
        // The next caller starts afresh instead of resuming a dead future.
        let c = r.spawn("k", 3);
        settle().await;
        r.gate.notify_waiters();
        assert_eq!(c.await.unwrap(), (Ok(3), Role::Leader));
        assert_eq!(r.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn many_rounds_never_leak_entries() {
        let sf: Arc<SingleFlight<u32>> = Arc::new(SingleFlight::new());
        for round in 0..200u32 {
            let k = key(&format!("k{}", round % 5));
            let futs: Vec<_> = (0..8)
                .map(|_| {
                    let (sf, k) = (sf.clone(), k.clone());
                    async move {
                        tokio::time::timeout(
                            Duration::from_millis(u64::from(round % 3)),
                            sf.run(&k, || async move {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                                round
                            }),
                        )
                        .await
                    }
                })
                .collect();
            futures::future::join_all(futs).await;
        }
        assert_eq!(sf.in_flight(), 0);
    }
}
