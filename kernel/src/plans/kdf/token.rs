//! [`KdfStateToken`] — semantic identifier that joins a KDF node with its
//! extracted state.
//!
//! Tokens are stamped at plan-build time when a [`SinkType::Consume`]
//! sink is constructed. Each token carries the KDF id and a UUID string id.
//! `Display` emits `<kdf_id>#<id>`.
//!
//! [`SinkType::Consume`]: crate::plans::ir::nodes::SinkType::Consume

use strum::Display as StrumDisplay;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum ConsumerKdfId {
    CheckpointHint,
    MetadataProtocol,
    SidecarCollector,
}

/// Identity for a KDF entry in [`PhaseState`].
///
/// Stamped at plan-build time when a [`SinkType::Consume`] sink is
/// constructed. The fresh UUID `id` ensures stale handles from a prior
/// plan can't be confused with current ones — a token from a dead plan
/// won't match any live `PhaseState` entry, so old handles can't be
/// used incorrectly to read from or write into the current plan's state.
///
/// [`PhaseState`]: crate::plans::state_machines::framework::phase_state::PhaseState
/// [`SinkType::Consume`]: crate::plans::ir::nodes::SinkType::Consume
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
