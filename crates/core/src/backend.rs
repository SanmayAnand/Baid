//! Types exchanged with memory backends through the [`crate::Backend`] trait.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::RequestError;
use crate::item::MemoryItem;
use crate::types::{EntityId, NodeId};

/// What one backend query returned.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BackendResult {
    /// The items, best first.
    pub items: Vec<MemoryItem>,
}

/// A durable fact to store, sent by `remember`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fact {
    /// The fact in plain text.
    pub text: String,
    /// Entities the fact is about. Writes invalidate cache entries that mention them.
    #[serde(default)]
    pub entities: Vec<EntityId>,
    /// Free-form labels.
    #[serde(default)]
    pub labels: Vec<String>,
    /// When the fact became true, if known.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub as_of: Option<OffsetDateTime>,
    /// Client-chosen key; a retried write with the same key is applied once.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

impl Fact {
    /// Longest accepted fact text, in bytes.
    pub const MAX_TEXT_BYTES: usize = 32 * 1024;
    /// Most entities a fact may name.
    pub const MAX_ENTITIES: usize = 64;

    /// Checks the parts of a fact that types alone cannot.
    pub fn validate(&self) -> Result<(), RequestError> {
        if self.text.trim().is_empty() {
            return Err(RequestError::EmptyText);
        }
        if self.text.len() > Self::MAX_TEXT_BYTES {
            return Err(RequestError::TextTooLong(self.text.len()));
        }
        if self.entities.len() > Self::MAX_ENTITIES {
            return Err(RequestError::TooManyEntities(self.entities.len()));
        }
        Ok(())
    }
}

/// A backend's acknowledgement of a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteAck {
    /// The id of the stored item.
    pub node: NodeId,
    /// Every entity the backend linked the fact to. It may find more than the client
    /// named, so invalidation uses the union of these and [`Fact::entities`].
    pub entities: Vec<EntityId>,
    /// The new version of the item, if the backend tracks versions.
    pub version: Option<u64>,
}

/// What a backend can do. The core never special-cases a backend; it reads these flags
/// (ADR-007).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capabilities {
    /// Many queries in one round trip (Bolt `UNWIND`). False for REST search APIs.
    pub native_batch: bool,
    /// Largest batch `query_batch` accepts.
    pub max_batch: usize,
    /// Can answer [`crate::BackendQuery::Neighbourhood`].
    pub neighbourhood: bool,
    /// Publishes a change feed of writes made outside BAiD.
    pub change_feed: bool,
    /// Returns a version per node.
    pub versions: bool,
}

impl Default for Capabilities {
    /// The most conservative backend: no batching, no extras.
    fn default() -> Self {
        Capabilities {
            native_batch: false,
            max_batch: 1,
            neighbourhood: false,
            change_feed: false,
            versions: false,
        }
    }
}

/// Backend health, reported on `/readyz`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Health {
    /// Serving normally.
    Up,
    /// Serving, but slowly or partially.
    Degraded {
        /// Why.
        reason: String,
    },
    /// Not serving.
    Down {
        /// Why.
        reason: String,
    },
}

impl Health {
    /// True unless the backend is down.
    pub fn is_serving(&self) -> bool {
        !matches!(self, Health::Down { .. })
    }
}
