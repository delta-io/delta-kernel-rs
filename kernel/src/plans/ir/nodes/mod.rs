//! Individual plan node types.
//!
//! Each struct here is a single node kind in the declarative plan tree. The
//! recursive tree is assembled in [`super::declarative::DeclarativePlanNode`].
//!
//! This module ships nodes the prototype's read path exercises plus sink IR in the
//! [`sinks`] submodule; [`WriteSink`] / [`PartitionedWriteSink`] are IR-only until an engine lowers
//! them.

use std::sync::Arc;

use crate::expressions::{ColumnName, Expression, Scalar};
use crate::schema::{SchemaRef, StructField, StructType};
use crate::{DeltaResult, Error, FileMeta};

// ============================================================================
// Leaf nodes
// ============================================================================

/// File formats readable by a [`ScanNode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Alias for [`FileType`] used by format-parametric scan APIs when the readable format is chosen at
/// runtime (for example checkpoint JSON vs Parquet).
pub type FileFormat = FileType;

/// Read columnar data from a fixed file list.
///
/// # Schema contract
///
/// The `schema` field specifies the desired output structure with the
/// nullability semantics of [`crate::ParquetHandler::read_parquet_files`]:
/// missing nullable columns become NULL; missing non-nullable columns error;
/// present columns may be cast to the requested type; column order follows
/// `schema`.
///
/// # Row index column
///
/// When `row_index_column` is `Some(name)`, each output row is augmented with
/// a synthetic `LONG` column containing its 0-indexed position within the
/// originating file. The name must not collide with any column in `schema`.
///
/// # Predicate
///
/// `predicate` is a pushdown hint: the engine MAY apply it during the read,
/// but MUST NOT over-filter. Any residual filtering happens via
/// [`FilterNode`].
#[derive(Debug, Clone)]
pub struct ScanNode {
    pub file_type: FileType,
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
    pub row_index_column: Option<String>,
    pub predicate: Option<Arc<Expression>>,
}

impl ScanNode {
    /// Create a new scan node with no row index column and no predicate.
    pub fn new(file_type: FileType, files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self {
            file_type,
            files,
            schema,
            row_index_column: None,
            predicate: None,
        }
    }

    /// Output schema of the scan, including a row-index metadata column when
    /// [`Self::row_index_column`] is set. Engine compilers should use this
    /// instead of reconstructing the schema-plus-row-index shape themselves.
    pub fn effective_output_schema(&self) -> DeltaResult<SchemaRef> {
        let Some(name) = &self.row_index_column else {
            return Ok(self.schema.clone());
        };
        let mut fields: Vec<StructField> = self.schema.fields().cloned().collect();
        fields.push(StructField::create_metadata_column(
            name.clone(),
            crate::schema::MetadataColumnSpec::RowIndex,
        ));
        StructType::try_new(fields)
            .map(std::sync::Arc::new)
            .map_err(|e| {
                Error::generic(format!("scan output schema with row index is invalid: {e}"))
            })
    }
}

/// List files from a storage prefix via [`crate::StorageHandler::list_from`].
#[derive(Debug, Clone)]
pub struct FileListingNode {
    /// Directory URL or file path to start listing from.
    pub path: url::Url,
}

/// Kernel-provided constant rows (no I/O), matching SQL `VALUES`-style rows.
///
/// Emits `rows.len()` rows, each carrying one [`Scalar`] per top-level field
/// in `schema`.
///
/// # Invariants
///
/// Enforced by [`ValuesNode::try_new`] / [`ValuesNode::try_new_row`]:
/// - `rows.len() <= LITERAL_NODE_MAX_ROWS`
/// - every row has `values.len() == schema.fields().count()`
/// - every row has `values.len() <= LITERAL_NODE_MAX_COLS`
/// - estimated total payload `<= LITERAL_NODE_MAX_ESTIMATED_BYTES`
///
/// The layout mirrors [`crate::EvaluationHandler::create_many`], which is
/// what the executor uses to materialize a values node into `EngineData`.
#[derive(Debug, Clone)]
pub struct ValuesNode {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

/// Maximum number of rows in a [`ValuesNode`].
pub const LITERAL_NODE_MAX_ROWS: usize = 1024;

/// Maximum number of top-level scalars per row.
pub const LITERAL_NODE_MAX_COLS: usize = 100;

/// Maximum estimated payload bytes across all rows.
pub const LITERAL_NODE_MAX_ESTIMATED_BYTES: usize = 10 * 1024;

impl ValuesNode {
    /// Construct and validate a multi-row literal.
    pub fn try_new(schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> DeltaResult<Self> {
        Self::validate(&schema, &rows)?;
        Ok(Self { schema, rows })
    }

    /// Construct and validate a single-row literal.
    pub fn try_new_row(schema: SchemaRef, values: Vec<Scalar>) -> DeltaResult<Self> {
        Self::try_new(schema, vec![values])
    }

    fn validate(schema: &SchemaRef, rows: &[Vec<Scalar>]) -> DeltaResult<()> {
        if rows.len() > LITERAL_NODE_MAX_ROWS {
            return Err(Error::generic(format!(
                "ValuesNode exceeds max rows: {} > {}",
                rows.len(),
                LITERAL_NODE_MAX_ROWS,
            )));
        }
        let expected_cols = schema.fields().count();
        let mut total_bytes: usize = 0;
        for (i, row) in rows.iter().enumerate() {
            if row.len() != expected_cols {
                return Err(Error::generic(format!(
                    "ValuesNode row {i} has {} scalars, schema expects {}",
                    row.len(),
                    expected_cols,
                )));
            }
            if row.len() > LITERAL_NODE_MAX_COLS {
                return Err(Error::generic(format!(
                    "ValuesNode row {i} exceeds max cols: {} > {}",
                    row.len(),
                    LITERAL_NODE_MAX_COLS,
                )));
            }
            for scalar in row {
                total_bytes = total_bytes.saturating_add(estimate_scalar_bytes(scalar));
            }
        }
        if total_bytes > LITERAL_NODE_MAX_ESTIMATED_BYTES {
            return Err(Error::generic(format!(
                "ValuesNode exceeds max payload: {total_bytes} > {LITERAL_NODE_MAX_ESTIMATED_BYTES}",
            )));
        }
        Ok(())
    }
}

/// Coarse upper bound on the heap footprint of a scalar. Conservative — used
/// only to gate [`ValuesNode`] payload size.
fn estimate_scalar_bytes(s: &Scalar) -> usize {
    use Scalar::*;
    match s {
        Null(_) | Boolean(_) | Byte(_) | Short(_) => 1,
        Integer(_) | Float(_) => 4,
        Long(_) | Double(_) | Date(_) | Timestamp(_) | TimestampNtz(_) => 8,
        Decimal(_) => 16,
        String(s) => s.len(),
        Binary(b) => b.len(),
        Struct(_) | Array(_) | Map(_) => 64, // rough; nested literals are rare here
    }
}

// ============================================================================
// Unary transforms
// ============================================================================

/// Filter rows where `predicate` evaluates `true`. `NULL` predicate values
/// drop the row (SQL semantics).
#[derive(Debug, Clone)]
pub struct FilterNode {
    pub predicate: Arc<Expression>,
}

/// Project input rows through a list of expressions into `output_schema`.
///
/// `columns.len()` must equal `output_schema.fields().count()`, and each
/// expression must be evaluable against the child's schema and assignable to
/// its matching output field.
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub columns: Vec<Arc<Expression>>,
    pub output_schema: SchemaRef,
}

// ============================================================================
// N-ary
// ============================================================================

/// Concatenate N child streams.
///
/// When `ordered` is `true`, children are consumed in declaration order. When
/// `false`, the engine may interleave or reorder freely.
///
/// All children must have compatible schemas; the first child's schema is
/// the canonical output. An empty [`UnionNode`] yields the empty-struct schema.
#[derive(Debug, Clone)]
pub struct UnionNode {
    pub ordered: bool,
}

// Binary — equi-join
// ============================================================================

/// SQL join semantics. Selects what gets emitted, independent of how (see
/// [`JoinHint`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Emit `(left, right)` pairs whose join keys match.
    Inner,
    /// Emit each left row whose key does NOT match any right row.
    LeftAnti,
}

/// Strategy hint for the executor. Today only [`JoinHint::Hash`] is consumed;
/// future hints (broadcast, sort-merge) ride on this enum without a plan-IR
/// migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinHint {
    /// Drain the *build* subtree into an in-memory lookup, then stream probe.
    /// `JoinNode.build_keys` indexes the lookup; `JoinNode.probe_keys`
    /// queries it.
    Hash,
}

/// Equi-join with composite-key support, in build/probe framing.
///
/// Binary tree node: the two child subtrees produce the build and probe sides
/// respectively. [`JoinHint`] picks the execution strategy; [`JoinType`]
/// picks the SQL semantics.
///
/// # Output schema
///
/// `LeftAnti` mirrors the probe child's schema unchanged. Other variants are
/// reserved (see [`JoinType`]).
///
/// # SQL anti-join null semantics (`LeftAnti` variant)
///
/// - **Build side**: rows with null in any key column are skipped (not inserted into the lookup).
/// - **Probe side**: rows with null in any key column always pass — a null tuple can't equal
///   anything, so it can't be in the build set.
///
/// # Invariants (validated by the executor at run time)
///
/// - `build_keys` and `probe_keys` are both non-empty.
/// - `build_keys.len() == probe_keys.len()`.
#[derive(Debug, Clone)]
pub struct JoinNode {
    /// Key expressions evaluated against build batches; their tuples are
    /// inserted into the lookup.
    pub build_keys: Vec<Arc<Expression>>,
    /// Key expressions evaluated against probe batches; their tuples are
    /// looked up against the build set.
    pub probe_keys: Vec<Arc<Expression>>,
    /// SQL join semantics — what to emit.
    pub join_type: JoinType,
    /// Execution strategy hint — how to compute the join.
    pub hint: JoinHint,
}

// ============================================================================

// ============================================================================
// Ordering (used by KDF phases in later PRs; kept here so the IR is stable)
// ============================================================================

/// A single-column sort specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderingSpec {
    pub column: ColumnName,
    pub descending: bool,
    pub nulls_first: bool,
}

impl OrderingSpec {
    pub fn asc(column: ColumnName) -> Self {
        Self {
            column,
            descending: false,
            nulls_first: false,
        }
    }

    pub fn desc(column: ColumnName) -> Self {
        Self {
            column,
            descending: true,
            nulls_first: false,
        }
    }
}

// ============================================================================
// Window
// ============================================================================

/// One window function applied within a [`WindowNode`]. Only `row_number()` is
/// supported; the function emits one `LONG NOT NULL` column named `output_col`.
#[derive(Debug, Clone)]
pub struct WindowFunction {
    pub output_col: String,
}

/// Window functions over `partition_by` + `order_by`.
///
/// Output schema = `Schema(child) ++ (one LONG NOT NULL column per function)`.
///
/// Producers must supply a non-empty `order_by` so `row_number()` ranking is deterministic
/// (for example `ORDER BY version DESC` for newest-wins dedup). [`WindowNode::try_new`] and
/// [`DeclarativePlanNode::window`](crate::plans::ir::DeclarativePlanNode::window) enforce this at
/// construction time; the DataFusion executor also rejects empty `order_by` when compiling plans.
#[derive(Debug, Clone)]
pub struct WindowNode {
    pub functions: Vec<WindowFunction>,
    pub partition_by: Vec<Arc<Expression>>,
    pub order_by: Vec<OrderingSpec>,
}

impl WindowNode {
    /// Construct a [`WindowNode`] after validating IR invariants.
    pub fn try_new(
        functions: Vec<WindowFunction>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<OrderingSpec>,
    ) -> DeltaResult<Self> {
        if order_by.is_empty() {
            return Err(Error::generic(
                "WindowNode requires non-empty order_by; explicit ORDER BY columns are required \
                 for deterministic row_number semantics",
            ));
        }
        Ok(Self {
            functions,
            partition_by,
            order_by,
        })
    }
}

mod sinks;

pub use sinks::{
    ConsumeByKdfSink, DvRef, LoadSink, LoadSpec, RelationHandle, ScanFileColumns, SinkNode,
    SinkType,
};
