//! Context packing: fit recalled items into the agent's token budget.
//!
//! So far: [`TruncatingPacker`], which keeps backend order. Ranking, near-duplicate
//! removal and exact tiktoken counts come with plan T29.

use std::collections::HashSet;

use baid_core::{MemoryItem, PackedContext, Packer, TokenBudget};

/// Rough token count when the item has none: about four bytes of English per token.
pub fn estimate_tokens(text: &str) -> u32 {
    u32::try_from(text.len().div_ceil(4))
        .unwrap_or(u32::MAX)
        .max(1)
}

/// Keeps items in the order given, drops repeated ids, and skips any item that would push
/// the total past the budget (smaller later items may still fit).
#[derive(Clone, Copy, Debug, Default)]
pub struct TruncatingPacker;

impl Packer for TruncatingPacker {
    fn pack(&self, items: &[MemoryItem], budget: TokenBudget) -> PackedContext {
        let limit = budget.get();
        let mut seen = HashSet::new();
        let mut out = PackedContext::default();
        for item in items {
            if !seen.insert(&item.id) {
                out.dropped += 1;
                continue;
            }
            let tokens = if item.tokens > 0 {
                item.tokens
            } else {
                estimate_tokens(&item.text)
            };
            if out.tokens.saturating_add(tokens) > limit {
                out.dropped += 1;
                continue;
            }
            out.tokens += tokens;
            let mut kept = item.clone();
            kept.tokens = tokens;
            out.items.push(kept);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use baid_core::{AdapterId, NodeId};

    use super::*;

    fn item(id: &str, bytes: usize) -> MemoryItem {
        MemoryItem {
            id: NodeId::new(id).unwrap(),
            text: Arc::from("x".repeat(bytes)),
            score: 1.0,
            tokens: 0,
            entities: Default::default(),
            as_of: None,
            source: AdapterId::new("memory").unwrap(),
        }
    }

    #[test]
    fn keeps_order_dedupes_and_fits_budget() {
        let items = [
            item("a", 200),
            item("b", 400),
            item("a", 200),
            item("c", 40),
        ];
        let p = TruncatingPacker.pack(&items, TokenBudget::new(64).unwrap());
        let ids: Vec<&str> = p.items.iter().map(|i| i.id.as_str()).collect();
        // a = 50 tokens, b = 100 (does not fit), duplicate a, c = 10.
        assert_eq!(ids, ["a", "c"]);
        assert_eq!(p.tokens, 60);
        assert_eq!(p.dropped, 2);
        assert!(p.items.iter().all(|i| i.tokens > 0));
    }

    #[test]
    fn stable_for_the_same_input() {
        let items: Vec<_> = (0..50).map(|i| item(&format!("n{i}"), 10 * i)).collect();
        let b = TokenBudget::new(2000).unwrap();
        assert_eq!(
            TruncatingPacker.pack(&items, b),
            TruncatingPacker.pack(&items, b)
        );
    }
}
