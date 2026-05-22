//! NodeKind payload types referenced by the SSA IR ([`super::plan`]) and the engine compile path.

use std::sync::Arc;

use crate::expressions::{Expression, Scalar};
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

/// Alias for APIs that pick file format at runtime.
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
// Project node payload (consumed by both SSA `NodeKind::Project` lowering and the
// engine's `compile_project_node` helper)
// ============================================================================

/// Project rows through expressions into `output_schema`.
///
/// `columns.len()` must equal output field count.
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub columns: Vec<Arc<Expression>>,
    pub output_schema: SchemaRef,
}

mod sinks;

pub(crate) use sinks::default_scan_file_columns;
pub use sinks::{ConsumeSink, DvRef, LoadSink, RelationHandle, ScanFileColumns};
