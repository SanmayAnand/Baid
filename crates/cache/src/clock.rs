//! A clock the cache reads expiry against, so tests can move time by hand.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Source of "now".
pub trait Clock: Send + Sync + 'static {
    /// The current instant.
    fn now(&self) -> Instant;
}

/// The real monotonic clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A clock that only moves when told to. For tests.
#[derive(Debug)]
pub struct ManualClock {
    base: Instant,
    offset_ns: AtomicU64,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ManualClock {
    /// A clock frozen at the current instant.
    pub fn new() -> Self {
        ManualClock {
            base: Instant::now(),
            offset_ns: AtomicU64::new(0),
        }
    }

    /// Moves the clock forward by `d`.
    pub fn advance(&self, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.offset_ns.fetch_add(ns, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_nanos(self.offset_ns.load(Ordering::SeqCst))
    }
}
