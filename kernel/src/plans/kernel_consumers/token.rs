//! [`KernelConsumerToken`] — semantic identifier that joins a kernel-consumer node with its
//! extracted state.
//!
//! Tokens are stamped at plan-build time when a [`SinkType::Consume`]
//! sink is constructed. Each token carries the consumer kind and a UUID string id.
//! `Display` emits `<kind>#<id>`.
//!
//! [`SinkType::Consume`]: crate::plans::ir::nodes::SinkType::Consume

use strum::Display as StrumDisplay;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StrumDisplay)]
#[strum(serialize_all = "snake_case")]
pub enum KernelConsumerKind {
    CheckpointHint,
    MetadataProtocol,
    SidecarCollector,
}

/// Identity for a kernel-consumer entry in [`StepResult`].
///
/// Stamped at plan-build time when a [`SinkType::Consume`] sink is
/// constructed. The fresh UUID `id` ensures stale handles from a prior
/// plan can't be confused with current ones — a token from a dead plan
/// won't match any live `StepResult` entry, so old handles can't be
/// used incorrectly to read from or write into the current plan's state.
///
/// [`StepResult`]: crate::plans::operations::framework::step_result::StepResult
/// [`SinkType::Consume`]: crate::plans::ir::nodes::SinkType::Consume
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
