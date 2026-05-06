//! Defines the [`PlanExecutor`] trait for executing declarative plans.

use std::sync::Arc;

use crate::plan::{DeclarativePlanNode, PlanResult};
use crate::{AsAny, DeltaResult};

/// Executes [`DeclarativePlanNode`]s, returning results as [`PlanResult`].
///
/// Implementations are responsible for interpreting each plan variant and performing the
/// underlying I/O and computation. The returned [`PlanResult`] carries columnar data batches
/// as [`EngineData`](crate::EngineData).
pub trait PlanExecutor: AsAny {
    /// Execute the given declarative plan node and return the result.
    fn execute_plan(&self, plan: DeclarativePlanNode) -> DeltaResult<PlanResult>;
}

/// A type-erased, shared reference to a [`PlanExecutor`].
pub type PlanExecutorRef = Arc<dyn PlanExecutor>;
