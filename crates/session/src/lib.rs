//! Session tracking.
//!
//! So far: the last write each session made (for read-your-writes) and when it was last
//! seen (for idle expiry). The recall window and think-time estimate come with plan T22.

use std::time::{Duration, Instant};

use baid_core::{SessionId, TenantId, WriteSeq};
use dashmap::DashMap;

#[derive(Clone, Copy, Debug)]
struct State {
    last_write: WriteSeq,
    last_seen: Instant,
}

/// All live sessions, keyed by tenant and session id.
#[derive(Debug, Default)]
pub struct Sessions {
    map: DashMap<(TenantId, SessionId), State>,
}

impl Sessions {
    /// No sessions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records activity; creates the session if new.
    pub fn touch(&self, tenant: &TenantId, session: &SessionId, now: Instant) {
        self.map
            .entry((tenant.clone(), session.clone()))
            .and_modify(|s| s.last_seen = now)
            .or_insert(State {
                last_write: 0,
                last_seen: now,
            });
    }

    /// Write sequence number of the session's most recent write (0 if none). A cached
    /// entry loaded before this must not be served to this session.
    pub fn last_write(&self, tenant: &TenantId, session: &SessionId) -> WriteSeq {
        self.map
            .get(&(tenant.clone(), session.clone()))
            .map_or(0, |s| s.last_write)
    }

    /// Records that the session made a write that took sequence number `seq`.
    pub fn note_write(&self, tenant: &TenantId, session: &SessionId, seq: WriteSeq, now: Instant) {
        self.map
            .entry((tenant.clone(), session.clone()))
            .and_modify(|s| {
                s.last_write = s.last_write.max(seq);
                s.last_seen = now;
            })
            .or_insert(State {
                last_write: seq,
                last_seen: now,
            });
    }

    /// Ends a session. Returns true if it existed.
    pub fn end(&self, tenant: &TenantId, session: &SessionId) -> bool {
        self.map
            .remove(&(tenant.clone(), session.clone()))
            .is_some()
    }

    /// Drops sessions idle for longer than `idle`. Returns how many went.
    pub fn expire_idle(&self, now: Instant, idle: Duration) -> usize {
        let before = self.map.len();
        self.map
            .retain(|_, s| now.saturating_duration_since(s.last_seen) <= idle);
        before.saturating_sub(self.map.len())
    }

    /// Number of live sessions.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if there are no sessions.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }
    fn s(x: &str) -> SessionId {
        SessionId::new(x).unwrap()
    }

    #[test]
    fn last_write_only_moves_forward() {
        let ss = Sessions::new();
        let now = Instant::now();
        assert_eq!(ss.last_write(&t("a"), &s("1")), 0);
        ss.note_write(&t("a"), &s("1"), 7, now);
        ss.note_write(&t("a"), &s("1"), 5, now);
        assert_eq!(ss.last_write(&t("a"), &s("1")), 7);
    }

    #[test]
    fn sessions_are_per_tenant() {
        let ss = Sessions::new();
        ss.note_write(&t("a"), &s("1"), 3, Instant::now());
        assert_eq!(ss.last_write(&t("b"), &s("1")), 0);
    }

    #[test]
    fn end_and_expire() {
        let ss = Sessions::new();
        let t0 = Instant::now();
        ss.touch(&t("a"), &s("old"), t0);
        ss.touch(&t("a"), &s("new"), t0 + Duration::from_secs(50));
        let gone = ss.expire_idle(t0 + Duration::from_secs(70), Duration::from_secs(60));
        assert_eq!(gone, 1);
        assert_eq!(ss.len(), 1);
        assert!(ss.end(&t("a"), &s("new")));
        assert!(!ss.end(&t("a"), &s("new")));
    }
}
