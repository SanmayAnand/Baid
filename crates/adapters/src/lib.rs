//! Backend adapters and the registry that maps each tenant to its backend.
//!
//! Built so far: the `memory` adapter (re-exported from `baid-core`). Bolt (Neo4j) and S3
//! come next (plan T11, T12), then Zep and Mem0.

mod registry;

pub use baid_core::MemoryBackend;
pub use registry::{BuildError, Registry};
