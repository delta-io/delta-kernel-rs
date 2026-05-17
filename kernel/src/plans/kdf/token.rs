//! [`KdfStateToken`] — semantic identifier that joins a KDF node with its
//! extracted state.
//!
//! Tokens are stamped at plan-build time when a [`SinkType::Consume`]
//! sink is constructed. Each token carries the KDF type and a UUID string id.
//! `Display` emits `<kdf_type>#<id>`.
//!
//! [`SinkType::Consume`]: crate::plans::ir::nodes::SinkType::Consume

use uuid::Uuid;

/// Identifier joining a plan-tree KDF node to its extracted state.
///
/// Equality / hashing consider both fields. `Display` formats as
/// `<kdf_type>#<id>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KdfType {
    Consumer(String),
}

impl std::fmt::Display for KdfType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Consumer(name) => write!(f, "{name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KdfStateToken {
    pub kdf_type: KdfType,
    pub id: String,
}

impl KdfStateToken {
    /// Mint a fresh token for a consumer KDF with a UUID id.
    pub fn new(kdf_id: &'static str) -> Self {
        Self {
            kdf_type: KdfType::Consumer(kdf_id.to_string()),
            id: Uuid::new_v4().to_string(),
        }
    }
}

impl std::fmt::Display for KdfStateToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.kdf_type, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_tokens_differ_in_id() {
        let a = KdfStateToken::new("consumer.test");
        let b = KdfStateToken::new("consumer.test");
        assert_eq!(a.kdf_type, b.kdf_type);
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn display_is_kdf_type_hash_id() {
        let t = KdfStateToken {
            kdf_type: KdfType::Consumer("consumer.checkpoint_hint".to_string()),
            id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
        };
        assert_eq!(
            t.to_string(),
            "consumer.checkpoint_hint#123e4567-e89b-12d3-a456-426614174000"
        );
    }
}
