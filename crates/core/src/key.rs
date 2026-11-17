//! Cache keys.
//!
//! A [`Key`] can only be built by [`Key::derive`], which takes a tenant, so a key without a
//! tenant cannot exist (tenant isolation by the type system). The string form is
//!
//! ```text
//! baid:v1:{tenant}:{kind}:{hex(blake3(...))}
//! ```
//!
//! where the hash covers the tenant, the adapter, [`SCHEMA_VERSION`] and the normalised query.
//! The tenant is hashed too, not only prefixed, so a store that keys on the hash alone (the
//! NVMe tier, for example) still cannot mix tenants up.

use std::fmt;

use time::OffsetDateTime;
use unicode_normalization::UnicodeNormalization;

use crate::types::{AdapterId, BackendQuery, EntityId, Filters, KeyKind, TenantId};

/// Version of the key derivation. Bump it whenever hashing or normalisation changes, so
/// entries written by an older binary are simply never found again.
pub const SCHEMA_VERSION: u32 = 1;

/// Prefix of every key string.
pub const KEY_PREFIX: &str = "baid:v1";

/// blake3 key-derivation context; keeps our hashes apart from any other blake3 use.
const HASH_CONTEXT: &str = "baid 2026-11 cache key";

/// A cache key: tenant, kind and a 32-byte hash of everything else.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    tenant: TenantId,
    kind: KeyKind,
    hash: [u8; 32],
}

impl Key {
    /// Derives the key for `query`, run by `adapter`, on behalf of `tenant`.
    ///
    /// Queries that differ only in case, Unicode form, whitespace, trailing sentence
    /// punctuation, or the order or duplication of filter values get the same key.
    pub fn derive(tenant: &TenantId, adapter: &AdapterId, query: &BackendQuery) -> Self {
        let mut h = blake3::Hasher::new_derive_key(HASH_CONTEXT);
        h.update(&SCHEMA_VERSION.to_le_bytes());
        put_str(&mut h, tenant.as_str());
        put_str(&mut h, adapter.as_str());
        hash_query(&mut h, query);
        Key {
            tenant: tenant.clone(),
            kind: query.kind(),
            hash: *h.finalize().as_bytes(),
        }
    }

    /// The tenant that owns this key.
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// What kind of result the key holds.
    pub fn kind(&self) -> KeyKind {
        self.kind
    }

    /// The 32-byte hash.
    pub fn hash(&self) -> &[u8; 32] {
        &self.hash
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{KEY_PREFIX}:{}:{}:{}",
            self.tenant,
            self.kind.code(),
            blake3::Hash::from(self.hash).to_hex()
        )
    }
}

/// Normalises query text for keying: NFKC, lowercase, whitespace collapsed to single
/// spaces, and sentence punctuation (`. , ; : ! ? ¿ ¡`) trimmed from both ends.
///
/// Punctuation *inside* the text is kept on purpose: trimming it would make `c++` and `c`
/// share a key, and a wrong answer is worse than a miss.
pub fn normalise(text: &str) -> String {
    let lowered = text.nfkc().collect::<String>().to_lowercase();
    let collapsed = lowered.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed
        .trim_matches(|c: char| {
            c.is_whitespace() || matches!(c, '.' | ',' | ';' | ':' | '!' | '?' | '¿' | '¡')
        })
        .to_owned()
}

// Every field is written with a type tag or a length prefix, so two different queries can
// never produce the same byte stream (no "ab"+"c" == "a"+"bc" collisions).

fn put_str(h: &mut blake3::Hasher, s: &str) {
    h.update(&(s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

fn put_entities(h: &mut blake3::Hasher, ids: &[EntityId]) {
    let mut sorted: Vec<&str> = ids.iter().map(EntityId::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    h.update(&(sorted.len() as u64).to_le_bytes());
    for id in sorted {
        put_str(h, id);
    }
}

fn put_time(h: &mut blake3::Hasher, t: Option<OffsetDateTime>) {
    match t {
        None => {
            h.update(&[0]);
        }
        Some(t) => {
            h.update(&[1]);
            h.update(&t.unix_timestamp_nanos().to_le_bytes());
        }
    }
}

fn put_filters(h: &mut blake3::Hasher, f: &Filters) {
    put_entities(h, &f.entity_ids);
    put_time(h, f.since);
    let mut labels: Vec<&str> = f.labels.iter().map(String::as_str).collect();
    labels.sort_unstable();
    labels.dedup();
    h.update(&(labels.len() as u64).to_le_bytes());
    for l in labels {
        put_str(h, l);
    }
}

fn hash_query(h: &mut blake3::Hasher, q: &BackendQuery) {
    match q {
        BackendQuery::Search {
            text,
            filters,
            limit,
        } => {
            h.update(b"q");
            put_str(h, &normalise(text));
            put_filters(h, filters);
            h.update(&limit.to_le_bytes());
        }
        BackendQuery::Neighbourhood {
            entity,
            hops,
            limit,
        } => {
            h.update(b"n");
            put_str(h, entity.as_str());
            h.update(&[*hops]);
            h.update(&limit.to_le_bytes());
        }
        BackendQuery::Entity { id } => {
            h.update(b"e");
            put_str(h, id.as_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn tenant(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }
    fn adapter() -> AdapterId {
        AdapterId::new("bolt").unwrap()
    }
    fn ent(s: &str) -> EntityId {
        EntityId::new(s).unwrap()
    }
    fn search(text: &str, entities: &[&str], labels: &[&str]) -> BackendQuery {
        BackendQuery::Search {
            text: text.into(),
            filters: Filters {
                entity_ids: entities.iter().map(|e| ent(e)).collect(),
                since: None,
                labels: labels.iter().map(|l| l.to_string()).collect(),
            },
            limit: 20,
        }
    }

    #[test]
    fn normalise_examples() {
        assert_eq!(
            normalise("  Status of   ACME renewal? "),
            "status of acme renewal"
        );
        assert_eq!(normalise("Ｓｔａｔｕｓ"), "status", "NFKC folds full-width");
        assert_eq!(normalise("what is c++?"), "what is c++");
        assert_eq!(normalise("¿qué pasa?"), "qué pasa");
        assert_eq!(normalise("e.g. acme"), "e.g. acme");
        assert_eq!(normalise("?!"), "");
    }

    #[test]
    fn equivalent_queries_share_a_key() {
        let t = tenant("acme");
        let a = Key::derive(
            &t,
            &adapter(),
            &search("Status of Acme renewal?", &["b", "a", "a"], &["x", "y"]),
        );
        let b = Key::derive(
            &t,
            &adapter(),
            &search("status   of acme renewal", &["a", "b"], &["y", "x"]),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn every_input_changes_the_key() {
        let t = tenant("acme");
        let base = Key::derive(&t, &adapter(), &search("q", &[], &[]));
        let other_tenant = Key::derive(&tenant("globex"), &adapter(), &search("q", &[], &[]));
        let other_adapter = Key::derive(&t, &AdapterId::new("s3").unwrap(), &search("q", &[], &[]));
        let other_text = Key::derive(&t, &adapter(), &search("r", &[], &[]));
        let other_filter = Key::derive(&t, &adapter(), &search("q", &["a"], &[]));
        let other_label = Key::derive(&t, &adapter(), &search("q", &[], &["a"]));
        for k in [
            other_tenant,
            other_adapter,
            other_text,
            other_filter,
            other_label,
        ] {
            assert_ne!(base, k);
            assert_ne!(base.hash(), k.hash());
        }
    }

    #[test]
    fn entity_vs_label_with_same_text_differ() {
        let t = tenant("acme");
        let a = Key::derive(&t, &adapter(), &search("q", &["x"], &[]));
        let b = Key::derive(&t, &adapter(), &search("q", &[], &["x"]));
        assert_ne!(a, b);
    }

    #[test]
    fn kind_comes_from_the_query() {
        let t = tenant("acme");
        let n = Key::derive(
            &t,
            &adapter(),
            &BackendQuery::Neighbourhood {
                entity: ent("acme"),
                hops: 1,
                limit: 20,
            },
        );
        let e = Key::derive(&t, &adapter(), &BackendQuery::Entity { id: ent("acme") });
        assert_eq!(n.kind(), KeyKind::Neighbourhood);
        assert_eq!(e.kind(), KeyKind::Entity);
        assert_ne!(n.hash(), e.hash());
    }

    #[test]
    fn display_format() {
        let k = Key::derive(&tenant("acme"), &adapter(), &search("q", &[], &[]));
        let s = k.to_string();
        assert!(s.starts_with("baid:v1:acme:q:"), "{s}");
        assert_eq!(s.len(), "baid:v1:acme:q:".len() + 64);
    }

    proptest! {
        #[test]
        fn normalise_is_idempotent(s in "\\PC{0,64}") {
            let once = normalise(&s);
            prop_assert_eq!(normalise(&once), once);
        }

        #[test]
        fn tenants_never_share_a_key(
            a in "[a-z0-9_-]{1,16}",
            b in "[a-z0-9_-]{1,16}",
            q in "\\PC{0,32}",
        ) {
            prop_assume!(a != b);
            let qa = search(&q, &[], &[]);
            let ka = Key::derive(&tenant(&a), &adapter(), &qa);
            let kb = Key::derive(&tenant(&b), &adapter(), &qa);
            prop_assert_ne!(ka.hash(), kb.hash());
            prop_assert_ne!(ka, kb);
        }

        #[test]
        fn case_and_spacing_do_not_change_the_key(q in "[a-zA-Z ]{1,32}") {
            let t = tenant("acme");
            let k1 = Key::derive(&t, &adapter(), &search(&q, &[], &[]));
            let k2 = Key::derive(&t, &adapter(), &search(&format!("  {}  ", q.to_uppercase()), &[], &[]));
            prop_assert_eq!(k1, k2);
        }
    }
}
