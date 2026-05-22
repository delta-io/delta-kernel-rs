//! This module defines the concept of a PlanExecutor and its associated input + output types.
//!
//! This module is opt-in behind the `declarative-plans` feature flag.
mod ir;
mod query_builder;

use bytes::Bytes;
pub use ir::{IoOperation, Plan, QueryPlan, QueryPlanNode};
pub use query_builder::QueryPlanBuilder;

use crate::{AsAny, DeltaResult, DeltaResultIterator, EngineData, FileMeta};

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, relational plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and return the result.
    fn execute_plan(&self, plan: Plan) -> DeltaResult<PlanResult>;
}

/// The result of executing a [`Plan`].
///
/// Each variant describes a different shape of output that a plan can possibly produce.
pub enum PlanResult {
    /// A stream of columnar data batches (as [`EngineData`]) produced by the plan.
    Data(DeltaResultIterator<Box<dyn EngineData>>),
    /// A stream of file metadata entries.
    FileMeta(DeltaResultIterator<FileMeta>),
    /// A stream of raw byte buffers.
    Bytes(DeltaResultIterator<Bytes>),
    /// Represents the successful completion of a plan, but with no return value.
    Unit,
}
