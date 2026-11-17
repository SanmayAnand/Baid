//! The write log: the whole stale-data defence.
//!
//! Each tenant has a write sequence number. Each `(tenant, entity)` remembers the sequence
//! number of the last acknowledged write that touched it. A cached result loaded at
//! sequence `s` is valid only while none of its entities has a stamp greater than `s`.
//!
//! Why this is safe: a writer takes its sequence number only *after* the backend has
//! acknowledged the write. So a load whose snapshot is at least that number started after
//! the acknowledgement and sees the write, and a load with a smaller snapshot is refused
//! as soon as the entity is stamped.

use baid_core::{EntityId, TenantId, WriteSeq};
use dashmap::DashMap;

/// Per-tenant write sequence numbers and per-entity last-write stamps.
#[derive(Debug, Default)]
pub struct WriteLog {
    seqs: DashMap<TenantId, WriteSeq>,
    last_write: DashMap<(TenantId, EntityId), WriteSeq>,
}

impl WriteLog {
    /// An empty log: every tenant at sequence 0, no entity ever written.
    pub fn new() -> Self {
        Self::default()
    }

    /// The tenant's current sequence number.
    pub fn seq(&self, tenant: &TenantId) -> WriteSeq {
        self.seqs.get(tenant).map_or(0, |s| *s)
    }

    /// Sequence number of the last write to `entity` (0 if never written).
    pub fn last_write(&self, tenant: &TenantId, entity: &EntityId) -> WriteSeq {
        self.last_write
            .get(&(tenant.clone(), entity.clone()))
            .map_or(0, |s| *s)
    }

    /// Records an acknowledged write: takes the tenant's next sequence number and stamps
    /// each distinct entity with it. Returns the number.
    pub fn mark(&self, tenant: &TenantId, entities: &[EntityId]) -> WriteSeq {
        let seq = {
            let mut s = self.seqs.entry(tenant.clone()).or_insert(0);
            *s += 1;
            *s
        };
        let mut distinct: Vec<&EntityId> = entities.iter().collect();
        distinct.sort_unstable();
        distinct.dedup();
        for e in distinct {
            self.last_write
                .entry((tenant.clone(), e.clone()))
                .and_modify(|v| *v = (*v).max(seq))
                .or_insert(seq);
        }
        seq
    }

    /// True if none of `entities` was written after `loaded_at`.
    pub fn is_current(
        &self,
        tenant: &TenantId,
        entities: &[EntityId],
        loaded_at: WriteSeq,
    ) -> bool {
        entities
            .iter()
            .all(|e| self.last_write(tenant, e) <= loaded_at)
    }

    /// Number of entities ever written.
    pub fn len(&self) -> usize {
        self.last_write.len()
    }

    /// True if nothing was ever written.
    pub fn is_empty(&self) -> bool {
        self.last_write.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }
    fn e(s: &str) -> EntityId {
        EntityId::new(s).unwrap()
    }

    #[test]
    fn sequence_advances_once_per_write() {
        let w = WriteLog::new();
        assert_eq!(w.seq(&t("a")), 0);
        assert_eq!(w.mark(&t("a"), &[e("x"), e("x"), e("y")]), 1);
        assert_eq!(w.mark(&t("a"), &[e("x")]), 2);
        assert_eq!(w.last_write(&t("a"), &e("x")), 2);
        assert_eq!(w.last_write(&t("a"), &e("y")), 1);
        assert_eq!(w.last_write(&t("a"), &e("z")), 0);
    }

    #[test]
    fn tenants_are_independent() {
        let w = WriteLog::new();
        w.mark(&t("a"), &[e("x")]);
        assert_eq!(w.seq(&t("b")), 0);
        assert_eq!(w.last_write(&t("b"), &e("x")), 0);
    }

    #[test]
    fn currency() {
        let w = WriteLog::new();
        let loaded_at = w.seq(&t("a"));
        assert!(w.is_current(&t("a"), &[e("x"), e("y")], loaded_at));
        w.mark(&t("a"), &[e("y")]);
        assert!(!w.is_current(&t("a"), &[e("x"), e("y")], loaded_at));
        assert!(w.is_current(&t("a"), &[e("x")], loaded_at), "x untouched");
        assert!(
            w.is_current(&t("a"), &[e("y")], w.seq(&t("a"))),
            "loaded after the write"
        );
        assert!(w.is_current(&t("a"), &[], 0));
    }
}
