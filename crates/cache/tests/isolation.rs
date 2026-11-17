//! The isolation property (Code architecture section 6.1): no sequence of puts and gets
//! ever returns one tenant's items to another tenant.

#![allow(clippy::unwrap_used)] // test file

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use baid_core::{
    AdapterId, BackendQuery, CacheEntry, CacheStore, Filters, Key, LoadInfo, MemoryItem, NodeId,
    TenantId,
};
use cache::{RamStore, SystemClock, TierConfig};
use futures::executor::block_on;
use proptest::prelude::*;

const TENANTS: [&str; 3] = ["acme", "globex", "initech"];
/// Few distinct queries, so tenants often ask the very same question.
const QUERIES: [&str; 4] = [
    "status of renewal",
    "who owns it",
    "Status of renewal?",
    "open bugs",
];

#[derive(Clone, Debug)]
enum Op {
    Put {
        tenant: usize,
        query: usize,
        items: Vec<u8>,
    },
    Get {
        tenant: usize,
        query: usize,
    },
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (
            0..TENANTS.len(),
            0..QUERIES.len(),
            prop::collection::vec(any::<u8>(), 0..5)
        )
            .prop_map(|(tenant, query, items)| Op::Put {
                tenant,
                query,
                items
            }),
        (0..TENANTS.len(), 0..QUERIES.len()).prop_map(|(tenant, query)| Op::Get { tenant, query }),
    ]
}

fn adapter() -> AdapterId {
    AdapterId::new("bolt").unwrap()
}

fn key(tenant: &TenantId, query: usize) -> Key {
    let q = BackendQuery::Search {
        text: QUERIES[query].into(),
        filters: Filters::default(),
        limit: 20,
    };
    Key::derive(tenant, &adapter(), &q)
}

/// Item ids carry their owner, so a leak is visible in the result itself.
fn items(tenant: &TenantId, ids: &[u8]) -> Vec<MemoryItem> {
    ids.iter()
        .map(|n| MemoryItem {
            id: NodeId::new(format!("{tenant}/{n}")).unwrap(),
            text: Arc::from(format!("item {n}")),
            score: 1.0,
            tokens: 0,
            entities: Default::default(),
            as_of: None,
            source: adapter(),
        })
        .collect()
}

fn ids(items: &[MemoryItem]) -> Vec<String> {
    items.iter().map(|i| i.id.to_string()).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn no_read_crosses_tenants(ops in prop::collection::vec(any_op(), 1..300)) {
        let store = RamStore::new(Arc::new(SystemClock));
        let tenants: Vec<TenantId> = TENANTS.iter().map(|t| TenantId::new(t).unwrap()).collect();
        for t in &tenants {
            store.add_tenant(t.clone(), TierConfig::with_capacity(64 << 20));
        }
        let mut model: HashMap<Key, Vec<String>> = HashMap::new();

        for op in ops {
            match op {
                Op::Put { tenant, query, items: raw } => {
                    let t = &tenants[tenant];
                    let k = key(t, query);
                    let its = items(t, &raw);
                    let load = LoadInfo {
                        query_entities: &[],
                        loaded_at: store.write_seq(t),
                        source: adapter(),
                        ttl: Duration::from_secs(600),
                        speculative: false,
                    };
                    let entry = CacheEntry::new(its.clone(), load, Instant::now());
                    block_on(store.put(k.clone(), entry));
                    model.insert(k, ids(&its));
                }
                Op::Get { tenant, query } => {
                    let t = &tenants[tenant];
                    let k = key(t, query);
                    if let Some(e) = block_on(store.get(&k)) {
                        prop_assert_eq!(Some(&ids(&e.items)), model.get(&k));
                        let prefix = format!("{t}/");
                        prop_assert!(e.items.iter().all(|i| i.id.as_str().starts_with(&prefix)));
                    }
                }
            }
        }
    }
}
