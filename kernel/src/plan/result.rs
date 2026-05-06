//! Defines the [`PlanResult`] type returned by plan execution.

use crate::{DeltaResult, EngineData};

/// The result of executing a [`DeclarativePlanNode`](super::DeclarativePlanNode).
///
/// Each variant describes a different shape of output. Currently the only variant is `Data`,
/// which carries a stream of columnar [`EngineData`] batches.
pub enum PlanResult {
    /// A stream of columnar data batches produced by the plan.
    Data(Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>>>),
}
