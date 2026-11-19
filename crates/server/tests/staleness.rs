//! The staleness property (Code architecture section 6.2).
//!
//! Random traces interleave writes and reads across sessions against the real engine (RAM
//! cache, write log, single-flight, invalidator). A checker then asserts:
//!
//! 1. **No stale read after an acknowledged write.** A read filtered on entity `E` that
//!    *started* after a write to `E` was acknowledged returns that write.
//! 2. **Read-your-writes.** Any read by the writing session (filtered or not) that started
//!    after its own write was acknowledged returns that write.
//!
//! Unfiltered reads by *other* sessions are not held to rule 1: a write can add a result
//! to a search they cached, which is visible within the TTL (G14, documented).
//!
//! Race windows are widened on purpose: the backend reads its data at once and replies
//! after an injected delay (so a load can carry pre-write data), and writes are delayed
//! before they apply. "Before" and "after" come from one shared counter that both
//! acknowledgements and read starts increment, so the ordering is exact.
//!
//! The main property runs on a single-threaded runtime with a paused clock: every case is
//! deterministic and proptest can shrink a failure to a minimal trace. A smaller variant
//! runs on real threads and real time for genuine parallelism.

#![allow(clippy::unwrap_used)] // test file

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use adapters::Registry;
use async_trait::async_trait;
use baid_core::{
    AdapterId, Backend, BackendError, BackendQuery, BackendResult, Capabilities, Config, EntityId,
    Fact, Filters, Health, MemoryBackend, RecallRequest, SessionId, TenantId, TokenBudget,
    WriteAck,
};
use cache::{RamStore, SystemClock, TierConfig};
use pack::TruncatingPacker;
use proptest::prelude::*;
use server::auth::Tenant;
use server::engine::Engine;
use session::Sessions;

/// Entity names: whole words, so a search for one never matches another.
const ENTITIES: [&str; 3] = ["alpha", "bravo", "charlie"];

/// A memory backend that reads at once and replies late, with delays taken in order from a
/// generated list (so a paused-clock run is reproducible).
struct Delayed {
    inner: MemoryBackend,
    delays: Mutex<VecDeque<u8>>,
}

impl Delayed {
    fn next_delay(&self) -> Duration {
        let ms = self.delays.lock().unwrap().pop_front().unwrap_or(0);
        Duration::from_millis(u64::from(ms))
    }
}

#[async_trait]
impl Backend for Delayed {
    fn id(&self) -> AdapterId {
        self.inner.id()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn query_batch(
        &self,
        t: &TenantId,
        qs: &[BackendQuery],
    ) -> Result<Vec<BackendResult>, BackendError> {
        let answer = self.inner.query_now(t, qs); // data as of now ...
        tokio::time::sleep(self.next_delay()).await; // ... delivered later
        Ok(answer)
    }
    async fn write(&self, t: &TenantId, f: &Fact) -> Result<WriteAck, BackendError> {
        tokio::time::sleep(self.next_delay()).await;
        Ok(self.inner.write_now(t, f))
    }
    async fn health(&self) -> Health {
        Health::Up
    }
}

#[derive(Clone, Debug)]
enum Act {
    Write { entity: usize },
    Read { entity: usize, filtered: bool },
}

#[derive(Clone, Debug)]
struct Step {
    act: Act,
    /// Think time before this step, in ms.
    pause_ms: u8,
}

#[derive(Clone, Debug)]
struct Trace {
    sessions: Vec<Vec<Step>>,
    backend_delays: Vec<u8>,
}

fn step() -> impl Strategy<Value = Step> {
    let act = prop_oneof![
        (0..ENTITIES.len()).prop_map(|entity| Act::Write { entity }),
        (0..ENTITIES.len(), any::<bool>())
            .prop_map(|(entity, filtered)| Act::Read { entity, filtered }),
    ];
    (act, 0u8..4).prop_map(|(act, pause_ms)| Step { act, pause_ms })
}

fn trace() -> impl Strategy<Value = Trace> {
    (
        prop::collection::vec(prop::collection::vec(step(), 1..9), 2..5),
        prop::collection::vec(0u8..6, 0..80),
    )
        .prop_map(|(sessions, backend_delays)| Trace {
            sessions,
            backend_delays,
        })
}

#[derive(Debug)]
struct WriteRec {
    session: usize,
    entity: usize,
    value: u64,
    acked_at: u64,
}

#[derive(Debug)]
struct ReadRec {
    session: usize,
    entity: usize,
    filtered: bool,
    started_at: u64,
    values: HashSet<u64>,
}

fn tenant() -> Tenant {
    let config: Config = toml::from_str(
        "[[tenants]]\nid = \"acme\"\napi_key_env = \"K\"\nadapter = \"memory\"\nadapter_url = \"memory://\"\nbackend_timeout = \"30s\"\n",
    )
    .unwrap();
    Tenant {
        id: config.tenants[0].id.clone(),
        config: Arc::new(config.tenants[0].clone()),
    }
}

/// Runs the trace: sessions concurrently, each one's steps in order.
async fn run(trace: &Trace) -> (Vec<WriteRec>, Vec<ReadRec>) {
    let t = tenant();
    let store = RamStore::new(Arc::new(SystemClock));
    store.add_tenant(t.id.clone(), TierConfig::with_capacity(16 << 20));
    let backend = Arc::new(Delayed {
        inner: MemoryBackend::new(),
        delays: Mutex::new(trace.backend_delays.iter().copied().collect()),
    });
    let mut reg = Registry::new();
    reg.insert(t.id.clone(), backend);
    let engine = Arc::new(Engine::new(
        Arc::new(store),
        reg,
        Arc::new(TruncatingPacker),
        Arc::new(Sessions::new()),
    ));
    let clock = Arc::new(AtomicU64::new(0));
    let next_value = Arc::new(AtomicU64::new(1));

    let sessions = trace.sessions.iter().enumerate().map(|(si, steps)| {
        let (engine, clock, next_value, t) =
            (engine.clone(), clock.clone(), next_value.clone(), t.clone());
        async move {
            let sid = SessionId::new(format!("s{si}")).unwrap();
            let (mut writes, mut reads) = (Vec::new(), Vec::new());
            for st in steps {
                tokio::time::sleep(Duration::from_millis(u64::from(st.pause_ms))).await;
                match st.act {
                    Act::Write { entity } => {
                        let value = next_value.fetch_add(1, Ordering::SeqCst);
                        let fact = Fact {
                            text: format!("{} v{value}", ENTITIES[entity]),
                            entities: vec![EntityId::new(ENTITIES[entity]).unwrap()],
                            labels: vec![],
                            as_of: None,
                            idempotency_key: None,
                        };
                        engine.remember(&t, &sid, fact).await.unwrap();
                        let acked_at = clock.fetch_add(1, Ordering::SeqCst) + 1;
                        writes.push(WriteRec {
                            session: si,
                            entity,
                            value,
                            acked_at,
                        });
                    }
                    Act::Read { entity, filtered } => {
                        let started_at = clock.fetch_add(1, Ordering::SeqCst) + 1;
                        let filters = if filtered {
                            Filters {
                                entity_ids: vec![EntityId::new(ENTITIES[entity]).unwrap()],
                                ..Filters::default()
                            }
                        } else {
                            Filters::default()
                        };
                        let req = RecallRequest {
                            session: sid.clone(),
                            query: ENTITIES[entity].into(),
                            budget: TokenBudget::default(),
                            filters,
                            max_staleness: None,
                        };
                        let r = engine.recall(&t, req).await.unwrap();
                        let values = r
                            .packed
                            .items
                            .iter()
                            .map(|i| {
                                let (name, v) = i.text.split_once(" v").unwrap();
                                assert_eq!(
                                    name, ENTITIES[entity],
                                    "a read returned another entity's fact"
                                );
                                v.parse::<u64>().unwrap()
                            })
                            .collect();
                        reads.push(ReadRec {
                            session: si,
                            entity,
                            filtered,
                            started_at,
                            values,
                        });
                    }
                }
            }
            (writes, reads)
        }
    });
    let results = futures::future::join_all(sessions).await;
    let (mut writes, mut reads) = (Vec::new(), Vec::new());
    for (w, r) in results {
        writes.extend(w);
        reads.extend(r);
    }
    (writes, reads)
}

/// Every violation of the two rules, as readable messages.
fn violations(writes: &[WriteRec], reads: &[ReadRec]) -> Vec<String> {
    let mut out = Vec::new();
    for r in reads {
        for w in writes
            .iter()
            .filter(|w| w.entity == r.entity && w.acked_at < r.started_at)
        {
            if r.values.contains(&w.value) {
                continue;
            }
            if r.filtered {
                out.push(format!(
                    "rule 1: s{} read {} (filtered) at t={} without v{}, acked by s{} at t={}",
                    r.session, ENTITIES[r.entity], r.started_at, w.value, w.session, w.acked_at
                ));
            } else if w.session == r.session {
                out.push(format!(
                    "rule 2: s{} read {} at t={} without its own v{}, acked at t={}",
                    r.session, ENTITIES[r.entity], r.started_at, w.value, w.acked_at
                ));
            }
        }
    }
    out
}

/// Cases per property: `BAID_PROPTEST_CASES` if set (the nightly run uses a large value),
/// otherwise `default`.
fn cases(default: u32) -> u32 {
    std::env::var("BAID_PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn check(trace: &Trace, rt: tokio::runtime::Runtime) -> Result<(), TestCaseError> {
    let (writes, reads) = rt.block_on(run(trace));
    let v = violations(&writes, &reads);
    prop_assert!(v.is_empty(), "{} violation(s):\n{}", v.len(), v.join("\n"));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(256), ..ProptestConfig::default() })]

    /// Single thread, paused clock: deterministic, so failures shrink to a minimal trace.
    #[test]
    fn no_stale_reads_deterministic(trace in trace()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        check(&trace, rt)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(32), ..ProptestConfig::default() })]

    /// Four worker threads, real time: genuine parallelism, at a higher cost per case.
    #[test]
    fn no_stale_reads_parallel(trace in trace()) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .build()
            .unwrap();
        check(&trace, rt)?;
    }
}

/// The checker itself must catch a stale read, or the properties above prove nothing.
#[test]
fn the_checker_flags_stale_reads() {
    let writes = [WriteRec {
        session: 0,
        entity: 0,
        value: 7,
        acked_at: 1,
    }];
    let stale_filtered = ReadRec {
        session: 1,
        entity: 0,
        filtered: true,
        started_at: 2,
        values: HashSet::new(),
    };
    let stale_own = ReadRec {
        session: 0,
        entity: 0,
        filtered: false,
        started_at: 2,
        values: HashSet::new(),
    };
    let concurrent = ReadRec {
        session: 1,
        entity: 0,
        filtered: true,
        started_at: 1,
        values: HashSet::new(),
    };
    let other_unfiltered = ReadRec {
        session: 1,
        entity: 0,
        filtered: false,
        started_at: 2,
        values: HashSet::new(),
    };
    assert_eq!(violations(&writes, &[stale_filtered]).len(), 1);
    assert_eq!(violations(&writes, &[stale_own]).len(), 1);
    assert!(
        violations(&writes, &[concurrent]).is_empty(),
        "read started before the ack"
    );
    assert!(
        violations(&writes, &[other_unfiltered]).is_empty(),
        "G14 exception"
    );
}
