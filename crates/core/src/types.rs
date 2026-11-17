//! Identifiers, token budgets and backend queries.
//!
//! Every identifier is a cheap-to-clone `Arc<str>` newtype that is validated when it is built,
//! including when it is deserialised, so an invalid id cannot exist at runtime.

use std::borrow::Borrow;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{IdError, TokenBudgetError};

/// Characters allowed in ids that appear inside cache keys, metric labels and config.
fn check_slug(kind: &'static str, s: &str) -> Result<(), IdError> {
    check_len(kind, s, 64)?;
    if s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Ok(())
    } else {
        Err(IdError {
            kind,
            reason: "only ASCII letters, digits, '-' and '_' are allowed",
        })
    }
}

/// Free-form ids supplied by clients or backends: any text without control characters.
fn check_text(kind: &'static str, s: &str, max: usize) -> Result<(), IdError> {
    check_len(kind, s, max)?;
    if s.chars().any(char::is_control) {
        return Err(IdError {
            kind,
            reason: "control characters are not allowed",
        });
    }
    Ok(())
}

fn check_len(kind: &'static str, s: &str, max: usize) -> Result<(), IdError> {
    if s.is_empty() {
        return Err(IdError {
            kind,
            reason: "must not be empty",
        });
    }
    if s.len() > max {
        return Err(IdError {
            kind,
            reason: "too long",
        });
    }
    Ok(())
}

fn check_tenant(kind: &'static str, s: &str) -> Result<(), IdError> {
    check_slug(kind, s)
}
fn check_adapter(kind: &'static str, s: &str) -> Result<(), IdError> {
    check_slug(kind, s)
}
fn check_session(kind: &'static str, s: &str) -> Result<(), IdError> {
    check_text(kind, s, 128)
}
fn check_entity(kind: &'static str, s: &str) -> Result<(), IdError> {
    check_text(kind, s, 512)
}

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident, $check:path) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Arc<str>);

        impl $name {
            /// Builds the id, rejecting values that break its format rules.
            pub fn new(s: impl AsRef<str>) -> Result<Self, IdError> {
                let s = s.as_ref();
                $check(stringify!($name), s)?;
                Ok(Self(Arc::from(s)))
            }

            /// The id as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// The id's UTF-8 bytes.
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;
            fn try_from(s: String) -> Result<Self, IdError> {
                Self::new(s)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdError;
            fn try_from(s: &str) -> Result<Self, IdError> {
                Self::new(s)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.0.as_ref().to_owned()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(
    /// A tenant. Comes only from the API key (ADR-008). 1–64 chars of `[A-Za-z0-9_-]`,
    /// because it is the first segment of every cache key and a metric label.
    TenantId,
    check_tenant
);
id_type!(
    /// An agent session, chosen by the client. 1–128 bytes, no control characters.
    SessionId,
    check_session
);
id_type!(
    /// A domain entity such as a customer or project. 1–512 bytes, no control characters.
    EntityId,
    check_entity
);
id_type!(
    /// One stored memory item (graph node, object, record) in a backend.
    /// 1–512 bytes, no control characters.
    NodeId,
    check_entity
);
id_type!(
    /// A configured backend adapter, e.g. `bolt` or `s3`. Same format as [`TenantId`].
    AdapterId,
    check_adapter
);

/// How many tokens of packed context the agent wants back. Always in `64..=32_000`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct TokenBudget(u32);

impl TokenBudget {
    /// Smallest accepted budget.
    pub const MIN: u32 = 64;
    /// Largest accepted budget.
    pub const MAX: u32 = 32_000;
    /// Budget used when the client does not send one.
    pub const DEFAULT: TokenBudget = TokenBudget(2_000);

    /// Builds a budget, rejecting values outside `MIN..=MAX`.
    pub fn new(tokens: u32) -> Result<Self, TokenBudgetError> {
        if tokens < Self::MIN {
            Err(TokenBudgetError::TooSmall(tokens))
        } else if tokens > Self::MAX {
            Err(TokenBudgetError::TooLarge(tokens))
        } else {
            Ok(Self(tokens))
        }
    }

    /// The number of tokens.
    pub fn get(self) -> u32 {
        self.0
    }

    /// The bucket this budget falls in. Budgets in the same bucket share cache entries.
    pub fn bucket(self) -> BudgetBucket {
        match self.0 {
            0..=512 => BudgetBucket::Up512,
            513..=1_024 => BudgetBucket::Up1k,
            1_025..=2_048 => BudgetBucket::Up2k,
            2_049..=4_096 => BudgetBucket::Up4k,
            _ => BudgetBucket::Over4k,
        }
    }
}

impl Default for TokenBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<u32> for TokenBudget {
    type Error = TokenBudgetError;
    fn try_from(v: u32) -> Result<Self, TokenBudgetError> {
        Self::new(v)
    }
}

impl From<TokenBudget> for u32 {
    fn from(b: TokenBudget) -> u32 {
        b.0
    }
}

/// Coarse budget classes (512 / 1k / 2k / 4k / 8k+), so near budgets share a cache entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BudgetBucket {
    /// Up to 512 tokens.
    Up512,
    /// 513 to 1,024 tokens.
    Up1k,
    /// 1,025 to 2,048 tokens.
    Up2k,
    /// 2,049 to 4,096 tokens.
    Up4k,
    /// More than 4,096 tokens.
    Over4k,
}

impl BudgetBucket {
    /// How many items to ask the backend for. The packer trims to the exact budget later,
    /// so this only has to be generous enough to fill the bucket's largest budget.
    pub fn item_limit(self) -> u16 {
        match self {
            BudgetBucket::Up512 => 10,
            BudgetBucket::Up1k => 20,
            BudgetBucket::Up2k => 40,
            BudgetBucket::Up4k => 80,
            BudgetBucket::Over4k => 200,
        }
    }
}

/// Optional narrowing of a search.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filters {
    /// Only items that mention at least one of these entities.
    #[serde(default)]
    pub entity_ids: Vec<EntityId>,
    /// Only items recorded at or after this time.
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub since: Option<OffsetDateTime>,
    /// Only items carrying at least one of these labels.
    #[serde(default)]
    pub labels: Vec<String>,
}

impl Filters {
    /// True when no filter is set.
    pub fn is_empty(&self) -> bool {
        self.entity_ids.is_empty() && self.since.is_none() && self.labels.is_empty()
    }
}

/// What kind of result a cache key holds. Shown as one letter in the key string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyKind {
    /// A query result (`q`).
    Query,
    /// An entity's neighbourhood (`n`); answers many questions about that entity.
    Neighbourhood,
    /// A single entity (`e`).
    Entity,
}

impl KeyKind {
    /// The one-letter code used in the key string.
    pub fn code(self) -> char {
        match self {
            KeyKind::Query => 'q',
            KeyKind::Neighbourhood => 'n',
            KeyKind::Entity => 'e',
        }
    }
}

/// One request to a memory backend. Adapters translate it into Cypher, S3 reads or REST.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendQuery {
    /// Free-text search.
    Search {
        /// The query text as the agent wrote it.
        text: String,
        /// Optional filters.
        #[serde(default)]
        filters: Filters,
        /// Maximum items to return.
        limit: u16,
    },
    /// The items within `hops` of an entity.
    Neighbourhood {
        /// The centre entity.
        entity: EntityId,
        /// Graph distance, usually 1.
        hops: u8,
        /// Maximum items to return.
        limit: u16,
    },
    /// One entity.
    Entity {
        /// The entity.
        id: EntityId,
    },
}

impl BackendQuery {
    /// The key kind that results of this query are stored under.
    pub fn kind(&self) -> KeyKind {
        match self {
            BackendQuery::Search { .. } => KeyKind::Query,
            BackendQuery::Neighbourhood { .. } => KeyKind::Neighbourhood,
            BackendQuery::Entity { .. } => KeyKind::Entity,
        }
    }

    /// Entities named by the query itself. Their versions are snapshotted before a load,
    /// and a write to any of them invalidates the result.
    pub fn entities(&self) -> &[EntityId] {
        match self {
            BackendQuery::Search { filters, .. } => &filters.entity_ids,
            BackendQuery::Neighbourhood { entity, .. } => std::slice::from_ref(entity),
            BackendQuery::Entity { id } => std::slice::from_ref(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_rejects_separator_and_empty() {
        assert!(TenantId::new("acme").is_ok());
        assert!(TenantId::new("acme-eu_2").is_ok());
        assert!(TenantId::new("").is_err());
        assert!(
            TenantId::new("a:b").is_err(),
            "':' would break the key format"
        );
        assert!(TenantId::new("a b").is_err());
        assert!(TenantId::new("x".repeat(65)).is_err());
    }

    #[test]
    fn free_text_ids_reject_control_chars() {
        assert!(EntityId::new("Acme Corp / EU").is_ok());
        assert!(EntityId::new("acme\n").is_err());
        assert!(SessionId::new("s".repeat(129)).is_err());
    }

    #[test]
    fn ids_validate_on_deserialise() {
        let ok: Result<TenantId, _> = serde_json::from_str("\"acme\"");
        assert!(ok.is_ok());
        let bad: Result<TenantId, _> = serde_json::from_str("\"a:b\"");
        assert!(bad.is_err());
    }

    #[test]
    fn budget_range_and_buckets() {
        assert_eq!(TokenBudget::new(63), Err(TokenBudgetError::TooSmall(63)));
        assert_eq!(
            TokenBudget::new(32_001),
            Err(TokenBudgetError::TooLarge(32_001))
        );
        let b = |n| TokenBudget::new(n).map(TokenBudget::bucket);
        assert_eq!(b(64), Ok(BudgetBucket::Up512));
        assert_eq!(b(512), Ok(BudgetBucket::Up512));
        assert_eq!(b(513), Ok(BudgetBucket::Up1k));
        assert_eq!(b(2_048), Ok(BudgetBucket::Up2k));
        assert_eq!(b(4_097), Ok(BudgetBucket::Over4k));
        assert_eq!(b(32_000), Ok(BudgetBucket::Over4k));
    }

    #[test]
    fn query_kind_and_entities() {
        let e = EntityId::new("acme").unwrap();
        let q = BackendQuery::Neighbourhood {
            entity: e.clone(),
            hops: 1,
            limit: 20,
        };
        assert_eq!(q.kind(), KeyKind::Neighbourhood);
        assert_eq!(q.entities(), std::slice::from_ref(&e));
    }
}
