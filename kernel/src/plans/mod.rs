//! Declarative plans: the kernel describes data work as a relational plan; the engine executes it.
//!
//! Kernel does no I/O or data processing itself. When an operation needs data work, kernel builds a
//! plan and hands it to the engine's [`PlanExecutor`], which compiles it into the engine's own
//! representation (a Spark or DataFusion logical plan, an iterator pipeline) and runs it. The
//! engine therefore applies its own optimizer, parallelism, and async I/O to all of kernel's data
//! work, not just leaf scans.
//!
//! # What a plan is
//!
//! A [`Plan`](ir::plan::Plan) is a DAG of relational operators ([`Operator`](ir::nodes::Operator)):
//! sources, transforms, and set combinators. Most map one-to-one onto a SQL operator, so a plan
//! reads like a query. The live-add metadata plan built in `scan::scan_plan`, for example, is
//! roughly:
//!
//! ```sql
//! -- commits: keep the newest action per file, then keep only the live adds
//! SELECT add FROM (
//!     SELECT max_by(action, version) AS add FROM commits GROUP BY file_key
//! ) WHERE add IS NOT NULL
//! UNION ALL
//! -- checkpoint adds that no newer commit superseded
//! SELECT c.add FROM checkpoint c
//! LEFT ANTI JOIN commit_keys k ON c.file_key = k.file_key
//! ```
//!
//! # Expression boundary
//!
//! [`Expression`](crate::expressions::Expression) and
//! [`Predicate`](crate::expressions::Predicate) are broad source types. A connector creates a
//! predicate such as `price / 2 > 50` and passes it to a scan; Kernel also creates expressions for
//! imperative [`EvaluationHandler`](crate::EvaluationHandler) work. These trees contain syntax and
//! literal types, but column nodes do not carry their resolved type or effective nullability.
//!
//! A mandatory plan carries a smaller contract. [`PlanBuilder::project`] and
//! [`PlanBuilder::filter`] resolve broad inputs against the current schema into
//! [`PlanExpression`](ir::expression::PlanExpression) and
//! [`PlanPredicate`](ir::expression::PlanPredicate). Resolution proves column paths, exact types,
//! effective nested nullability, Boolean filters, and project assignment. It rejects arithmetic,
//! casts, arrays, `IN`, opaque callbacks, and unknown nodes because replay does not require them.
//!
//! ```text
//! connector or Kernel Expression/Predicate
//!                 |
//!                 v
//!       Kernel resolution + validation
//!                 |
//!                 v
//!       PlanExpression/PlanPredicate
//!                 |
//!                 v
//!             PlanExecutor
//! ```
//!
//! Data skipping is intentionally separate. A [`DataSkippingSite`](ir::nodes::DataSkippingSite)
//! is an optional identity operation containing the broad source predicate, resolved statistics
//! locations, protected-row rules, and possibly Kernel's portable keep predicate. An executor may
//! apply that predicate, derive a richer conservative native filter, or keep every row. Thus an
//! engine can analyze `price / 2 > 50` without making division mandatory for metadata replay.
//!
//! # Writing an executor
//!
//! An executor implements [`PlanExecutor::execute_op`], dispatching on the [`Operation`] it
//! receives and returning the matching [`PlanResult`] variant:
//!
//! - [`Operation::IoOperation`] is a single I/O request; each [`IoOperation`] variant documents the
//!   [`PlanResult`] it must return.
//! - [`Operation::QueryPlan`] is a [`Plan`](ir::plan::Plan), returning [`PlanResult::Data`]. Either
//!   evaluate [`Plan::nodes`](ir::plan::Plan::nodes) in slice order, which is topologically sorted
//!   so a node's inputs are already evaluated, or compile the DAG into the engine's own plan.
//!
//! Every mandatory operator, expression, and predicate a plan contains must be handled. A
//! [`DataSkippingSite`](ir::nodes::DataSkippingSite) may be treated as identity, because it affects
//! performance rather than correctness. The sync engine's `SyncPlanExecutor` is a reference
//! implementation.
//!
//! # Consuming terminal results
//!
//! [`PlanExecutor::execute_op`] and [`PlanResult::Data`] provide the generic result contract for
//! operations whose output is consumed by kernel.
//!
//! Some kernel APIs instead return a [`Plan`](ir::plan::Plan) whose terminal rows belong to the
//! connector. The connector may execute the plan through its ordinary query engine and keep the
//! result in the engine's native representation instead of adapting it through [`EngineData`] only
//! to pass it back to itself. [`Scan::declarative_metadata_scan_plan`] is one example: the
//! connector may consume the live `add` rows from the returned plan itself.
//!
//! Connectors should use this native path when they own the result and adapting it through
//! [`EngineData`] adds no semantic value. The native path must preserve the plan's relational
//! semantics and declared output schema. Schema validation, streaming behavior, error propagation,
//! and cancellation must work the same way as they do on the generic path.
//!
//! The generic [`PlanExecutor`] path remains required for operations whose results kernel consumes.
//!
//! [`Scan::declarative_metadata_scan_plan`]: crate::scan::Scan::declarative_metadata_scan_plan
//!
//! # Where to look
//!
//! - [`PlanBuilder`] builds plans through a fluent, schema-validating API, each method documenting
//!   its operator with a runnable example.
//! - [`ir::nodes`] is the operator catalog: each [`Operator`](ir::nodes::Operator) variant's
//!   payload struct carries its semantics, invariants, and worked examples.
//! - [`ir::expression`] defines the closed, resolved expressions and predicates executors match.
//! - [`crate::expressions`] defines the broader public source language accepted at scan and
//!   imperative-evaluation boundaries.
//!
//! This module is opt-in behind the `declarative-plans` feature flag.
mod builder;
pub mod ir;
pub mod proto;

pub use builder::PlanBuilder;
use bytes::Bytes;
pub use ir::{IoOperation, Operation};

use crate::{
    AsAny, DeltaResult, DeltaResultIteratorStatic, EngineData, Error, FileMeta, ParquetFooter,
};

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, relational plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and returns the result.
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult>;

    /// Reads a parquet file's footer (schema and, in future, row-group stats) via a
    /// [`IoOperation::ParquetFooter`] op.
    fn read_parquet_footer(&self, file: FileMeta) -> DeltaResult<ParquetFooter> {
        self.execute_op(Operation::IoOperation(IoOperation::parquet_footer(file)))?
            .into_parquet_footer()
    }
}

/// The result of executing an [`Operation`].
///
/// Each variant describes a different shape of output that a plan can possibly produce.
pub enum PlanResult {
    /// A stream of columnar data batches (as [`EngineData`]) produced by the plan.
    Data(DeltaResultIteratorStatic<Box<dyn EngineData>>),
    /// A stream of file metadata entries.
    FileMeta(DeltaResultIteratorStatic<FileMeta>),
    /// A stream of raw byte buffers.
    Bytes(DeltaResultIteratorStatic<Bytes>),
    /// Metadata extracted from a Parquet file footer.
    ParquetFooter(ParquetFooter),
    /// Represents the successful completion of a plan, but with no return value.
    Unit,
}

impl PlanResult {
    /// Consumes the PlanResult and extracts the inner iterator of EngineData (assuming that it is a
    /// PlanResult::Data variant). Returns an error if the PlanResult is not the expected variant.
    pub fn into_data(self) -> DeltaResult<DeltaResultIteratorStatic<Box<dyn EngineData>>> {
        match self {
            Self::Data(iter) => Ok(iter),
            other => Err(other.type_mismatch("Data")),
        }
    }

    /// Consumes the PlanResult and extracts the inner iterator of FileMeta (assuming that it is a
    /// PlanResult::FileMeta variant). Returns an error if the PlanResult is not the expected
    /// variant.
    pub fn into_file_meta(self) -> DeltaResult<DeltaResultIteratorStatic<FileMeta>> {
        match self {
            Self::FileMeta(iter) => Ok(iter),
            other => Err(other.type_mismatch("FileMeta")),
        }
    }

    /// Consumes the PlanResult and extracts the inner iterator of Bytes (assuming that it is a
    /// PlanResult::Bytes variant). Returns an error if the PlanResult is not a PlanResult::Bytes
    /// variant.
    pub fn into_bytes(self) -> DeltaResult<DeltaResultIteratorStatic<Bytes>> {
        match self {
            Self::Bytes(iter) => Ok(iter),
            other => Err(other.type_mismatch("Bytes")),
        }
    }

    /// Consumes the PlanResult and extracts the inner [`ParquetFooter`] (assuming that it is a
    /// PlanResult::ParquetFooter variant). Returns an error if the PlanResult is not the expected
    /// variant.
    pub fn into_parquet_footer(self) -> DeltaResult<ParquetFooter> {
        match self {
            Self::ParquetFooter(footer) => Ok(footer),
            other => Err(other.type_mismatch("ParquetFooter")),
        }
    }

    /// Consumes the PlanResult, verifying that it is a PlanResult::Unit variant. Returns an error
    /// if the PlanResult is not the expected variant.
    pub fn into_unit(self) -> DeltaResult<()> {
        match self {
            Self::Unit => Ok(()),
            other => Err(other.type_mismatch("Unit")),
        }
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Self::Data(_) => "Data",
            Self::FileMeta(_) => "FileMeta",
            Self::Bytes(_) => "Bytes",
            Self::ParquetFooter(_) => "ParquetFooter",
            Self::Unit => "Unit",
        }
    }

    /// Build an [`Error::PlanResultTypeMismatch`] reporting `self`'s variant as the actual one.
    fn type_mismatch(&self, expected: &'static str) -> Error {
        Error::plan_result_type_mismatch(expected, self.variant_name())
    }
}
