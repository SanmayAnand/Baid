//! API key → tenant (ADR-008: the tenant comes only from the key, never the request body).
//!
//! Keys are held as blake3 hashes and looked up by hash, so lookup time does not depend on
//! how much of a guessed key matches a real one, and the raw keys are not kept in memory
//! after start-up.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use baid_core::{TenantConfig, TenantId};

use crate::config::Secret;
use crate::http::error::ApiError;
use crate::state::AppState;

/// The configured API keys.
pub struct ApiKeys {
    by_hash: HashMap<[u8; 32], TenantId>,
}

fn hash(key: &str) -> [u8; 32] {
    *blake3::hash(key.as_bytes()).as_bytes()
}

impl ApiKeys {
    /// Builds the table from `(key, tenant)` pairs.
    pub fn new(keys: impl IntoIterator<Item = (Secret, TenantId)>) -> Self {
        ApiKeys {
            by_hash: keys
                .into_iter()
                .map(|(k, t)| (hash(k.expose()), t))
                .collect(),
        }
    }

    /// The tenant a key belongs to, if any.
    pub fn tenant_for(&self, key: &str) -> Option<&TenantId> {
        self.by_hash.get(&hash(key))
    }

    /// Number of keys.
    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    /// True if there are no keys.
    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty()
    }
}

impl fmt::Debug for ApiKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiKeys({} keys)", self.by_hash.len())
    }
}

/// The caller's tenant, established from `Authorization: Bearer <key>`. Handlers take it as
/// an argument; a request without a valid key never reaches them.
#[derive(Clone, Debug)]
pub struct Tenant {
    /// The tenant id.
    pub id: TenantId,
    /// Its configuration.
    pub config: Arc<TenantConfig>,
}

/// Pulls the key out of an `Authorization` header value: `Bearer <key>`, scheme
/// case-insensitive.
fn bearer(value: &str) -> Option<&str> {
    let (scheme, key) = value.trim().split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| key.trim())
}

impl FromRequestParts<AppState> for Tenant {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let key = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(bearer)
            .ok_or(ApiError::Unauthorized)?;
        let id = state
            .secrets
            .api_keys
            .tenant_for(key)
            .ok_or(ApiError::Unauthorized)?;
        let config = state.tenant_config(id).ok_or(ApiError::Unauthorized)?;
        Ok(Tenant {
            id: id.clone(),
            config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_parsing() {
        assert_eq!(bearer("Bearer abc"), Some("abc"));
        assert_eq!(bearer("bearer   abc "), Some("abc"));
        assert_eq!(bearer("Basic abc"), None);
        assert_eq!(bearer("abc"), None);
    }

    #[test]
    fn lookup_by_key() {
        let a = TenantId::new("a").unwrap();
        let keys = ApiKeys::new([(Secret::new("key-a-0123456789"), a.clone())]);
        assert_eq!(keys.tenant_for("key-a-0123456789"), Some(&a));
        assert_eq!(keys.tenant_for("key-a-012345678"), None);
        assert_eq!(format!("{keys:?}"), "ApiKeys(1 keys)");
    }
}
