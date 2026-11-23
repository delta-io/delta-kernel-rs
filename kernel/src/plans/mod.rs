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
//! Data results can be collected into Rust types that implement `ToSchema` and
//! `TryFrom<StructData, Error = KernelError>`, either manually or with these derives:
//!
//! ```rust
//! use delta_kernel::delta_kernel_derive::{ToSchema, TryFromStructData};
//! # use delta_kernel::plans::{Plan, PlanExecutor, PlanExecutorExt};
//! # use delta_kernel::KernelResult;
//!
//! #[derive(ToSchema, TryFromStructData)]
//! struct Row {
//!     id: i64,
//! }
//!
//! # fn one_row(executor: &dyn PlanExecutor, plan: Plan) -> KernelResult<()> {
//! let row: Row = executor.execute_plan_into(plan)?;
//! # Ok(())
//! # }
//! # fn multiple(executor: &dyn PlanExecutor, plan: Plan) -> KernelResult<()> {
//! let rows: Vec<Row> = executor.execute_plan_into(plan)?;
//! # Ok(())
//! # }
//! # fn optional(executor: &dyn PlanExecutor, plan: Plan) -> KernelResult<()> {
//! let row: Option<Row> = executor.execute_plan_into(plan)?;
//! # Ok(())
//! # }
//! ```
//!
//! These conversions decode batches in order and stop at the first error. Converting to a struct
//! requires exactly one total row; `Option<T>` permits zero or one. Both decode at most two rows.
//! The plan output schema must exactly match the requested schema, including field nullability
//! and metadata. Callers must project out unwanted fields.
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

use std::sync::LazyLock;

use url::Url;

mod builder;
pub mod ir;
pub mod proto;
mod values;

pub use builder::PlanBuilder;
use bytes::Bytes;
pub use ir::nodes::{RelationId, RelationRef};
pub use ir::plan::Plan;
pub use ir::{IoOperation, Operation};

use crate::engine_data::TypedGetData as _;
use crate::expressions::{ColumnName, TryFromStructRows, column_name};
use crate::schema::{DataType, ToSchema};
use crate::{
    AsAny, EngineData, FileMeta, GetData, KernelError, KernelResult, KernelResultIterator,
    ParquetFooter, RowVisitor as _,
};

/// Execution capabilities declared by a [`PlanExecutor`].
///
/// Kernel uses this declaration to select between declarative and imperative execution. It does
/// not restrict handler I/O or explicitly requested plan execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanExecutorCapabilities {
    /// Supports I/O operations and general relational plans.
    Full,
    /// Supports I/O operations and file-scan plans.
    ///
    /// This includes single-node JSON and Parquet scans and empty [`ir::nodes::Values`] plans
    /// produced for empty reads.
    IoOnly,
}

/// Provides the ability to execute declarative plans to the Delta Kernel.
///
/// This gives the kernel the ability to execute data-intensive operations by constructing a
/// declarative, relational plan algebra, without prescribing *how* to do it.
pub trait PlanExecutor: AsAny {
    /// Executes the given declarative plan and returns the result.
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult>;

    /// Returns the executor's declared capabilities.
    ///
    /// Defaults to [`PlanExecutorCapabilities::Full`]. The returned value must remain constant
    /// throughout the executor's lifetime.
    fn capabilities(&self) -> PlanExecutorCapabilities {
        PlanExecutorCapabilities::Full
    }

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

/// Executes plans as typed Rust values for kernel consumers.
///
/// Use this extension with concrete executors or [`dyn PlanExecutor`](PlanExecutor) to decode
/// terminal rows without writing visitors or getters. The output type supplies the schema,
/// decoder, and row count requirements. The plan schema must match the requested type.
pub trait PlanExecutorExt: PlanExecutor {
    /// Executes a plan for a kernel consumer and decodes its terminal rows into the requested type.
    ///
    /// A row requires exactly one result; an optional row allows at most one; vectors accept any
    /// count. Batches are decoded in order, stopping at the first error. A row or optional row
    /// decodes at most two rows; later batches may remain unread. The plan's schema must equal the
    /// requested schema, including field nullability and metadata.
    ///
    /// Supports numbers, booleans, strings, binary, dates, string lists, string maps, and nested
    /// structs. A null struct masks its children, including their required fields.
    /// List elements and map values must be non-nullable in the schema, even for empty streams.
    /// String-list source schemas must declare non-null elements; batches are also checked.
    /// Required nulls and null string-list elements return errors. Null map entries are dropped;
    /// duplicate-key results are unspecified. Other collections, decimals, and timestamps are
    /// unsupported, including for empty streams.
    fn execute_plan_into<T: TryFromStructRows>(&self, plan: Plan) -> KernelResult<T> {
        let schema = <T::Row as ToSchema>::schema_ref();
        for field in schema.fields() {
            values::ensure_supported(field)?;
        }
        if &plan.schema != schema {
            return Err(KernelError::invalid_struct_data(format!(
                "Plan output schema does not match requested schema.\nExpected:\n{schema}\nActual:\n{}",
                plan.schema
            )));
        }
        let columns = <T::Row as ToSchema>::leaf_columns();
        T::collect_into(values::BatchRows {
            batches: self.execute_plan(plan)?.into_data()?,
            schema,
            columns,
        })
    }
}

impl<E: PlanExecutor + ?Sized> PlanExecutorExt for E {}

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

impl PlanResult {
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

/// Decodes a file write's single metadata row, consuming the full stream.
pub(crate) fn file_meta_from_batches(
    batches: impl KernelResultIterator<Box<dyn EngineData>>,
) -> KernelResult<FileMeta> {
    let mut visitor = FileMetaVisitor::default();
    for batch in batches {
        let batch = batch?;
        if !batch.is_empty() {
            visitor.visit_rows_of(batch.as_ref())?;
        }
    }
    visitor
        .file_meta
        .ok_or_else(|| KernelError::generic("Write returned no file metadata"))
}

#[derive(Default)]
struct FileMetaVisitor {
    file_meta: Option<FileMeta>,
}

impl crate::RowVisitor for FileMetaVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 3]> = LazyLock::new(|| {
            [
                column_name!("location"),
                column_name!("size"),
                column_name!("last_modified"),
            ]
        });
        static TYPES: [DataType; 3] = [DataType::STRING, DataType::LONG, DataType::LONG];
        (&*NAMES, &TYPES)
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..row_count {
            if self.file_meta.is_some() {
                return Err(KernelError::internal_error(
                    "Write returned multiple file metadata rows",
                ));
            }
            let location: String = getters[0].get(row, "location")?;
            let location = Url::parse(&location).map_err(|err| {
                KernelError::internal_error(format!("Invalid written file URL: {err}"))
            })?;
            let size: i64 = getters[1].get(row, "size")?;
            let size = u64::try_from(size)
                .map_err(|_| KernelError::internal_error("Write returned negative file size"))?;
            let last_modified: i64 = getters[2].get(row, "last_modified")?;
            self.file_meta = Some(FileMeta::new(location, last_modified, size));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::Engine;
    use crate::engine::sync::SyncEngine;
    use crate::plans::ir::nodes::FILE_META_SCHEMA;

    #[rstest]
    #[case::valid("memory:///file", 12, vec![0, 1, 0], true)]
    #[case::empty_file("memory:///file", 0, vec![1], true)]
    #[case::empty("memory:///file", 12, vec![], false)]
    #[case::multiple("memory:///file", 12, vec![2], false)]
    #[case::multiple_batches("memory:///file", 12, vec![1, 1], false)]
    #[case::negative_size("memory:///file", -1, vec![1], false)]
    #[case::invalid_url("invalid URL", 12, vec![1], false)]
    fn written_file_metadata(
        #[case] path: &str,
        #[case] size: i64,
        #[case] rows_per_batch: Vec<usize>,
        #[case] valid: bool,
        #[values(false, true)] late_error: bool,
    ) -> KernelResult<()> {
        let evaluation = SyncEngine::new().evaluation_handler();
        let values = vec![path.into(), size.into(), 7i64.into()];
        let batches = rows_per_batch.into_iter().map(move |rows| {
            evaluation.create_many(FILE_META_SCHEMA.clone(), vec![values.clone(); rows])
        });
        let result = file_meta_from_batches(
            batches.chain(late_error.then(|| Err(KernelError::generic("late result error")))),
        );
        if valid && !late_error {
            assert_eq!(result?, FileMeta::new(Url::parse(path)?, 7, size as u64));
        } else {
            assert!(result.is_err());
        }
        Ok(())
    }

    #[rstest]
    #[case::unit(PlanResult::Unit)]
    #[case::listing(PlanResult::FileMeta(Box::new(std::iter::empty())))]
    fn written_file_metadata_rejects_other_results(#[case] result: PlanResult) {
        assert!(matches!(
            result.into_data().and_then(file_meta_from_batches),
            Err(KernelError::PlanResultTypeMismatch {
                expected: "Data",
                ..
            })
        ));
    }
}
