//! [`KernelConsumerToken`] — semantic identifier that joins a kernel-consumer node with its
//! extracted state.
//!
//! Tokens are stamped at plan-build time when a [`EngineRequest::Consume`]
//! step is constructed. Each token carries the consumer kind and a UUID string id.
//! `Display` emits `<kind>#<id>`.
//!
//! [`EngineRequest::Consume`]: crate::plans::state_machines::framework::step::EngineRequest::Consume

use strum::Display as StrumDisplay;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum KernelConsumerKind {
    CheckpointHint,
    MetadataProtocol,
    SidecarCollector,
}

/// Identity for a kernel-consumer entry on a finished handle.
///
/// Stamped at plan-build time when a [`EngineRequest::Consume`] step is constructed. The fresh
/// UUID `id` ensures stale handles from a prior plan can't be confused with current
/// ones -- a [`FinishedHandle`] arriving with a token from a dead plan fails the
/// [`Extractor`] sanity check at decode time.
///
/// [`EngineRequest::Consume`]: crate::plans::state_machines::framework::step::EngineRequest::Consume
/// [`FinishedHandle`]: super::handle::FinishedHandle
/// [`Extractor`]: super::typed::Extractor
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KernelConsumerToken {
    pub kind: KernelConsumerKind,
    pub id: String,
}

impl KernelConsumerToken {
    /// Mint a fresh token for a kernel consumer with a UUID id.
    pub fn new(kind: KernelConsumerKind) -> Self {
        Self {
            kind,
            id: Uuid::new_v4().to_string(),
        }
    }
}

impl std::fmt::Display for KernelConsumerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.kind, self.id)
    }
}
