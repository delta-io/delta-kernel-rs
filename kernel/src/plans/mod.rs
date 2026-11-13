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
//! A [`Plan`] is a DAG of relational operators ([`Operator`](ir::nodes::Operator)):
//! sources, transforms, writes, and set combinators. Most map one-to-one onto a SQL operator, so a
//! plan reads like a query. The live-add metadata plan built in `scan::scan_plan`, for example, is
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
//! # Writing an executor
//!
//! An executor implements [`PlanExecutor::execute_op`], dispatching on the [`Operation`] it
//! receives and returning the matching [`PlanResult`] variant:
//!
//! - [`Operation::IoOperation`] is a single I/O request; each [`IoOperation`] variant documents the
//!   [`PlanResult`] it must return.
//! - [`Operation::QueryPlan`] is a [`Plan`], returning [`PlanResult::Data`]. Either evaluate
//!   [`Plan::nodes`](ir::plan::Plan::nodes) in slice order, which is topologically sorted so a
//!   node's inputs are already evaluated, or compile the DAG into the engine's own plan.
//!
//! Every operator, expression, and predicate a plan contains must be handled; returning an error
//! for an unsupported one is fine, and kernel surfaces it to the caller. The sync engine's
//! `SyncPlanExecutor` is a complete reference implementation.
//!
//! # Consuming terminal results
//!
//! [`PlanExecutor::execute_op`] and [`PlanResult::Data`] provide the generic result contract for
//! operations whose output is consumed by kernel.
//!
//! Some kernel APIs instead return a [`Plan`] whose terminal rows belong to the
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
//! Data results can also be materialized into Rust structs using the companion derives:
//!
//! ```rust
//! use delta_kernel::delta_kernel_derive::{ToSchema, TryFromPlanResult, TryFromStructData};
//! # use delta_kernel::plans::{Plan, PlanExecutor};
//! # use delta_kernel::KernelResult;
//!
//! #[derive(ToSchema, TryFromStructData, TryFromPlanResult)]
//! struct Row {
//!     id: i64,
//! }
//!
//! # fn singleton(executor: &dyn PlanExecutor, plan: Plan) -> KernelResult<()> {
//! let row: Row = executor.execute_plan(plan)?.try_into()?;
//! # Ok(())
//! # }
//! # fn multiple(executor: &dyn PlanExecutor, plan: Plan) -> KernelResult<()> {
//! let rows: Vec<Row> = executor.execute_plan(plan)?.try_into()?;
//! # Ok(())
//! # }
//! ```
//!
//! Both conversions use metadata cached by `TryFromPlanResult`, materialize every batch, and
//! propagate late errors. A singleton requires exactly one total row. Columns match by name;
//! unrequested source fields are ignored. Use [`PlanResult::into_values`] to obtain literal rows
//! with the same cached schema.
//!
//! # Where to look
//!
//! - [`PlanBuilder`] builds plans through a fluent, schema-validating API, each method documenting
//!   its operator with a runnable example.
//! - [`ir::nodes`] is the operator catalog: each [`Operator`](ir::nodes::Operator) variant's
//!   payload struct carries its semantics, invariants, and worked examples.
//! - [`crate::expressions`] defines the expressions and predicates operators evaluate, including
//!   the type and null semantics an executor must match.
//!
//! This module is opt-in behind the `declarative-plans` feature flag.
mod builder;
pub mod ir;
pub mod proto;
mod values;

pub use builder::PlanBuilder;
use bytes::Bytes;
pub use ir::nodes::{RelationId, RelationRef};
pub use ir::plan::Plan;
pub use ir::{IoOperation, Operation};

use crate::error::add_scalar_path_context;
use crate::expressions::StructData;
use crate::plans::ir::nodes::Values;
use crate::schema::{ColumnNamesAndTypes, SchemaRef, ToSchema};
use crate::{
    AsAny, EngineData, FileMeta, KernelError, KernelResult, KernelResultIterator, ParquetFooter,
};

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, relational plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and returns the result.
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult>;

    /// Executes a query plan through [`Self::execute_op`].
    fn execute_plan(&self, plan: Plan) -> KernelResult<PlanResult> {
        self.execute_op(Operation::QueryPlan(plan))
    }

    /// Reads a parquet file's footer (schema and, in future, row-group stats) via a
    /// [`IoOperation::ParquetFooter`] op.
    fn read_parquet_footer(&self, file: FileMeta) -> KernelResult<ParquetFooter> {
        self.execute_op(Operation::IoOperation(IoOperation::parquet_footer(file)))?
            .into_parquet_footer()
    }
}

/// A [`PlanExecutor`] that can retain relations within its own lifetime.
///
/// The connector creates and owns the scoped executor.
///
/// Implementations should release every relation retained through
/// [`execute_and_retain`](Self::execute_and_retain) when this executor is dropped. A
/// [`RelationRef`] may outlive the executor, but reading it after the executor is dropped must
/// return an error. Reading a ref through a scoped executor other than the one that produced it
/// is also an error.
pub trait ScopedPlanExecutor: PlanExecutor {
    /// Executes `plan` and retains its terminal relation in the engine, returning a handle that
    /// can be used as a [`RelationSource`](ir::nodes::Operator::RelationSource) in another plan.
    /// `name` is a debug-only identifier that an executor may embed in the resulting
    /// [`RelationId`] to identify its source. The handle is valid until this scoped executor is
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the plan cannot be executed or its result cannot be retained.
    fn execute_and_retain(&self, name: &str, plan: Plan) -> KernelResult<RelationRef>;
}

/// The result of executing an [`Operation`].
///
/// Each variant describes a different shape of output that a plan can possibly produce.
pub enum PlanResult {
    /// A stream of columnar data batches (as [`EngineData`]) produced by the plan.
    Data(Box<dyn KernelResultIterator<Box<dyn EngineData>>>),
    /// A stream of file metadata entries.
    FileMeta(Box<dyn KernelResultIterator<FileMeta>>),
    /// A stream of raw byte buffers.
    Bytes(Box<dyn KernelResultIterator<Bytes>>),
    /// Metadata extracted from a Parquet file footer.
    ParquetFooter(ParquetFooter),
    /// Represents the successful completion of a plan, but with no return value.
    Unit,
}

/// Static row metadata generated by `TryFromPlanResult`.
#[doc(hidden)]
pub trait PlanResultRow: ToSchema + TryFrom<StructData, Error = KernelError> {
    /// Returns the cached row schema and matching leaf visitor columns, in schema traversal order.
    fn schema_and_columns() -> (&'static SchemaRef, &'static ColumnNamesAndTypes);
}

impl PlanResult {
    /// Materializes all data batches using the row type's cached schema and visitor metadata.
    ///
    /// Supports numeric primitives, booleans, strings, binary, dates, and string containers
    /// with non-null elements or values. String list sources must contain no null elements:
    /// the existing typed list getter does not expose element validity. Map null values and
    /// duplicate keys return an error. Nested types, decimals, and timestamps are unsupported,
    /// including for empty streams. Other result variants retain the [`Self::into_data`] error.
    pub fn into_values<T: PlanResultRow>(self) -> KernelResult<Values> {
        values::from_batches::<T>(self.into_data()?)
    }

    /// Converts exactly one total row, propagating batch and materialization errors first.
    ///
    /// The complete stream is materialized before checking cardinality. Row types derive
    /// `ToSchema`, `TryFromStructData`, and `TryFromPlanResult`.
    pub fn into_single<T: PlanResultRow>(self) -> KernelResult<T> {
        let Values { schema, rows } = self.into_values::<T>()?;
        let [row]: [_; 1] = rows.try_into().map_err(|rows: Vec<_>| {
            KernelError::scalar_conversion("exactly one row", format!("{} rows", rows.len()))
        })?;
        T::try_from(StructData::from_values_unchecked(
            schema.as_ref().clone(),
            row,
        ))
        .map_err(|error| add_scalar_path_context(error, "[0]"))
    }

    /// Consumes the PlanResult and extracts the inner iterator of EngineData (assuming that it is a
    /// PlanResult::Data variant). Returns an error if the PlanResult is not the expected variant.
    pub fn into_data(self) -> KernelResult<Box<dyn KernelResultIterator<Box<dyn EngineData>>>> {
        match self {
            Self::Data(iter) => Ok(iter),
            other => Err(other.type_mismatch("Data")),
        }
    }

    /// Consumes the PlanResult and extracts the inner iterator of FileMeta (assuming that it is a
    /// PlanResult::FileMeta variant). Returns an error if the PlanResult is not the expected
    /// variant.
    pub fn into_file_meta(self) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
        match self {
            Self::FileMeta(iter) => Ok(iter),
            other => Err(other.type_mismatch("FileMeta")),
        }
    }

    /// Consumes the PlanResult and extracts the inner iterator of Bytes (assuming that it is a
    /// PlanResult::Bytes variant). Returns an error if the PlanResult is not a PlanResult::Bytes
    /// variant.
    pub fn into_bytes(self) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
        match self {
            Self::Bytes(iter) => Ok(iter),
            other => Err(other.type_mismatch("Bytes")),
        }
    }

    /// Consumes the PlanResult and extracts the inner [`ParquetFooter`] (assuming that it is a
    /// PlanResult::ParquetFooter variant). Returns an error if the PlanResult is not the expected
    /// variant.
    pub fn into_parquet_footer(self) -> KernelResult<ParquetFooter> {
        match self {
            Self::ParquetFooter(footer) => Ok(footer),
            other => Err(other.type_mismatch("ParquetFooter")),
        }
    }

    /// Consumes the PlanResult, verifying that it is a PlanResult::Unit variant. Returns an error
    /// if the PlanResult is not the expected variant.
    pub fn into_unit(self) -> KernelResult<()> {
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

    /// Build a [`KernelError::PlanResultTypeMismatch`] reporting `self`'s variant as the actual
    /// one.
    fn type_mismatch(&self, expected: &'static str) -> KernelError {
        KernelError::plan_result_type_mismatch(expected, self.variant_name())
    }
}

/// Materializes all batches using metadata generated by `TryFromPlanResult`, then reuses the
/// existing [`Values`] to [`Vec<T>`] conversion.
impl<T: PlanResultRow> TryFrom<PlanResult> for Vec<T> {
    type Error = KernelError;

    fn try_from(value: PlanResult) -> KernelResult<Self> {
        value.into_values::<T>()?.try_into()
    }
}
