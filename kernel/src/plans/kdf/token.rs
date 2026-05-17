//! [`KdfStateToken`] — semantic identifier that joins a KDF node with its
//! extracted state.
//!
//! Tokens are stamped at plan-build time when a [`SinkType::Consume`]
//! sink is constructed. Each token carries the KDF id and a UUID string id.
//! `Display` emits `<kdf_id>#<id>`.
//!
//! [`SinkType::Consume`]: crate::plans::ir::nodes::SinkType::Consume

use uuid::Uuid;
use strum::Display as StrumDisplay;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum ConsumerKdfId {
    CheckpointHint,
    MetadataProtocol,
    SidecarCollector,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KdfStateToken {
    pub kdf_id: ConsumerKdfId,
    pub id: String,
}

impl KdfStateToken {
    /// Mint a fresh token for a consumer KDF with a UUID id.
    pub fn new(kdf_id: ConsumerKdfId) -> Self {
        Self {
            kdf_id,
            id: Uuid::new_v4().to_string(),
        }
    }
}

impl std::fmt::Display for KdfStateToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.kdf_id, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_tokens_differ_in_id() {
        let a = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        let b = KdfStateToken::new(ConsumerKdfId::CheckpointHint);
        assert_eq!(a.kdf_id, b.kdf_id);
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn display_is_kdf_id_hash_id() {
        let t = KdfStateToken {
            kdf_id: ConsumerKdfId::CheckpointHint,
            id: "123e4567-e89b-12d3-a456-426614174000".to_string(),
        };
        assert_eq!(
            t.to_string(),
            "checkpoint_hint#123e4567-e89b-12d3-a456-426614174000"
        );
    }
}
