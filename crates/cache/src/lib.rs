//! Cache tiers for BAiD: one foyer RAM tier per tenant (NVMe tier later), the entity
//! write log, and the guarded put and get that keep writes from being undone by slow
//! reads.
//!
//! The entry point is [`RamStore`], which implements [`baid_core::CacheStore`].

mod clock;
mod store;
mod tier;
mod versions;

pub use clock::{Clock, ManualClock, SystemClock};
pub use store::RamStore;
pub use tier::{TierConfig, TierStats};
pub use versions::WriteLog;
