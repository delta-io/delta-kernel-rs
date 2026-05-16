//! [`KdfStateToken`] — semantic identifier that joins a KDF node with its
//! extracted state.
//!
//! Tokens are stamped at plan-build time when a [`SinkType::ConsumeByKdf`]
//! sink is constructed. Each token carries the KDF's `kdf_id` and a
//! monotonic `serial` that disambiguates multiple instances of the same KDF
//! within one plan. `Display` emits `kdf_id#serial` (e.g.
//! `consumer.metadata_protocol#42`) — grep-friendly in logs and errors.
//!
//! [`SinkType::ConsumeByKdf`]: crate::plans::ir::nodes::SinkType::ConsumeByKdf

use std::sync::atomic::{AtomicU64, Ordering};

/// Identifier joining a plan-tree KDF node to its extracted state.
///
/// Fields:
/// - `kdf_id` — owned copy of [`crate::plans::kdf::Kdf::kdf_id`]'s return.
/// - `serial` — monotonic counter; disambiguates multiple instances of the same KDF type within one
///   plan (rare but real — e.g., two `MetadataProtocol` consumers over disjoint sub-unions).
///
/// Equality / hashing consider both fields. `Display` formats as
/// `kdf_id#serial`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KdfStateToken {
    pub kdf_id: String,
    pub serial: u64,
}

impl KdfStateToken {
    /// Mint a fresh token for a KDF with the given `kdf_id`.
    ///
    /// Serials come from a process-wide atomic counter — unique across all
    /// tokens in a process.
    pub fn new(kdf_id: &'static str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self {
            kdf_id: kdf_id.to_string(),
            serial: COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl std::fmt::Display for KdfStateToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.kdf_id, self.serial)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_tokens_differ_in_serial() {
        let a = KdfStateToken::new("consumer.test");
        let b = KdfStateToken::new("consumer.test");
        assert_eq!(a.kdf_id, b.kdf_id);
        assert_ne!(a.serial, b.serial);
    }

    #[test]
    fn display_is_kdf_id_hash_serial() {
        let t = KdfStateToken {
            kdf_id: "consumer.checkpoint_hint".to_string(),
            serial: 42,
        };
        assert_eq!(t.to_string(), "consumer.checkpoint_hint#42");
    }
}
