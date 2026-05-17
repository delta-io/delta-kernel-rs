//! Node payload types used by the declarative plan IR.
//!
//! The recursive tree shape lives in [`super::declarative::DeclarativePlanNode`].

use std::sync::Arc;

use crate::expressions::{ColumnName, Expression, Scalar};
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, FileMeta};

// ============================================================================
// Leaf nodes
// ============================================================================

/// File formats supported by [`ScanNode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Alias used by APIs that choose file format at runtime.
pub type FileFormat = FileType;

/// Read rows from an explicit file list.
///
/// `schema` defines requested output columns. `predicate` is a pushdown hint:
/// engines may apply it during read but must preserve correctness.
#[derive(Debug, Clone)]
pub struct ScanNode {
    pub file_type: FileType,
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
    pub predicate: Option<Arc<Expression>>,
}

impl ScanNode {
    /// Create a scan node without a pushdown predicate.
    pub fn new(file_type: FileType, files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self {
            file_type,
            files,
            schema,
            predicate: None,
        }
    }

    /// Attach/replace a pushdown predicate hint.
    pub fn with_predicate(mut self, predicate: Arc<Expression>) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// List files from a storage prefix.
#[derive(Debug, Clone)]
pub struct FileListingNode {
    /// Directory URL or path to list from.
    pub path: url::Url,
}

/// Constant rows (no I/O), similar to SQL `VALUES`.
///
/// Each row must match schema arity.
#[derive(Debug, Clone)]
pub struct ValuesNode {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

impl ValuesNode {
    /// Construct and validate multi-row literals.
    pub fn try_new(schema: SchemaRef, rows: Vec<Vec<Scalar>>) -> DeltaResult<Self> {
        let expected_cols = schema.fields().count();
        for (i, row) in rows.iter().enumerate() {
            if row.len() != expected_cols {
                return Err(Error::generic(format!(
                    "ValuesNode row {i} has {} scalars, schema expects {}",
                    row.len(),
                    expected_cols,
                )));
            }
        }

        Ok(Self { schema, rows })
    }
}

// ============================================================================
// Unary transforms
// ============================================================================

/// Keep rows where `predicate` is `true` (SQL null semantics).
#[derive(Debug, Clone)]
pub struct FilterNode {
    pub predicate: Arc<Expression>,
}

/// Project rows through expressions into `output_schema`.
///
/// `columns.len()` must equal output field count.
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub columns: Vec<Arc<Expression>>,
    pub output_schema: SchemaRef,
}

// ============================================================================
// N-ary
// ============================================================================

/// Concatenate child streams.
///
/// `ordered=true` preserves child order; `ordered=false` allows reordering.
#[derive(Debug, Clone)]
pub struct UnionNode {
    pub ordered: bool,
}

// Binary — equi-join
// ============================================================================

/// SQL join semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Emit `(left, right)` pairs whose join keys match.
    Inner,
    /// Emit each right row whose key does not match any left row.
    LeftAnti,
}

/// Equi-join with composite keys.
///
/// `left_keys` map to left child rows; `right_keys` map to right child rows.
/// For [`JoinType::LeftAnti`], output schema mirrors the right child.
#[derive(Debug, Clone)]
pub struct JoinNode {
    /// Key expressions evaluated on left input batches.
    pub left_keys: Vec<Arc<Expression>>,
    /// Key expressions evaluated on right input batches.
    pub right_keys: Vec<Arc<Expression>>,
    /// Join output semantics.
    pub join_type: JoinType,
    /// Output schema of this join.
    pub output_schema: SchemaRef,
}

// ============================================================================

// ============================================================================
// Ordering (used by window ORDER BY)
// ============================================================================

/// Single-column sort specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderingSpec {
    pub column: ColumnName,
    pub descending: bool,
}

impl OrderingSpec {
    /// Ascending order.
    pub fn asc(column: ColumnName) -> Self {
        Self {
            column,
            descending: false,
        }
    }

    /// Descending order.
    pub fn desc(column: ColumnName) -> Self {
        Self {
            column,
            descending: true,
        }
    }
}

// ============================================================================
// Window
// ============================================================================

/// One window function in a [`WindowNode`].
///
/// Only `row_number()` is supported.
#[derive(Debug, Clone)]
pub struct WindowFunction {
    pub output_col: String,
}

/// Window functions over `partition_by` + `order_by`.
///
/// Output schema is child schema plus one LONG column per function.
#[derive(Debug, Clone)]
pub struct WindowNode {
    pub functions: Vec<WindowFunction>,
    pub partition_by: Vec<Arc<Expression>>,
    pub order_by: Vec<OrderingSpec>,
}

impl WindowNode {
    /// Construct a [`WindowNode`] and validate invariants.
    pub fn try_new(
        functions: Vec<WindowFunction>,
        partition_by: Vec<Arc<Expression>>,
        order_by: Vec<OrderingSpec>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            functions,
            partition_by,
            order_by,
        })
    }
}

mod sinks;

pub use sinks::{ConsumeSink, DvRef, LoadSink, RelationHandle, ScanFileColumns, SinkType};
