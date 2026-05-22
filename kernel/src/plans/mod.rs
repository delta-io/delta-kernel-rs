//! This module defines the concept of a PlanExecutor and its associated input + output types.
//!
//! This module is opt-in behind the `declarative-plans` feature flag.
mod ir;
mod query_builder;

use bytes::Bytes;
pub use ir::{IOOperation, Plan, QueryPlan, QueryPlanNode, ScanFileFormat};
pub use query_builder::QueryPlanBuilder;

use crate::{AsAny, DeltaResult, DeltaResultIterator, EngineData, FileMeta};

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, SQL-like plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and return the result.
    fn execute_plan(&self, plan: Plan) -> DeltaResult<PlanResult>;
}

/// The result of executing a [`Plan`].
///
/// Each variant describes a different shape of output that a plan can possibly produce.
pub enum PlanResult {
    /// A single data batch (as an EngineData) produced by the plan.
    BoundedData(Box<dyn EngineData>),
    /// An unbounded iterator of data batches (as EngineData) produced by the plan.
    UnboundedData(DeltaResultIterator<Box<dyn EngineData>>),
    /// A single file's metadata.
    FileMeta(FileMeta),
    /// An unbounded iterator of file metadata entries.
    FileMetaIter(DeltaResultIterator<FileMeta>),
    /// An unbounded iterator of raw byte buffers.
    BytesIter(DeltaResultIterator<Bytes>),
    /// Represents the successful completion of a plan, but with no return value.
    Unit,
}
