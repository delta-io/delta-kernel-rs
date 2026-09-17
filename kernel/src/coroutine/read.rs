//! File and data-read requests.
//!
//! JSON and Parquet reads share the paged request protocol while preserving format-specific cursor
//! types. Connectors must return batches in file and row order using the requested physical schema.

use super::{PagedOperation, PlanOperation};
use crate::engine_data::EngineData;
use crate::schema::SchemaRef;
use crate::{FileMeta, PredicateRef};

/// Paginated JSON read operation.
pub struct ReadJsonFiles {
    /// Files to read in order; batches preserve file and row order and never span files.
    pub files: Vec<FileMeta>,
    /// Exact schema required for every returned batch, including field order and physical names.
    pub physical_schema: SchemaRef,
    /// Optional conservative push-down. Connectors may ignore it; if applied, omit data only when
    /// it cannot be true. Returned data need not satisfy it.
    pub predicate: Option<PredicateRef>,
}

impl ReadJsonFiles {
    pub fn new(
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self {
            files,
            physical_schema,
            predicate,
        }
    }
}

impl PagedOperation for ReadJsonFiles {
    type Page = Vec<Box<dyn EngineData>>;
}

/// Paginated Parquet read operation.
pub struct ReadParquetFiles {
    /// Files to read in order; batches preserve file and row order and never span files.
    pub files: Vec<FileMeta>,
    /// Exact schema required for every returned batch, including field order and physical names.
    pub physical_schema: SchemaRef,
    /// Optional conservative push-down. Connectors may ignore it; if applied, omit data only when
    /// it cannot be true. Returned data need not satisfy it.
    pub predicate: Option<PredicateRef>,
}

impl ReadParquetFiles {
    pub fn new(
        files: Vec<FileMeta>,
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> Self {
        Self {
            files,
            physical_schema,
            predicate,
        }
    }
}

impl PagedOperation for ReadParquetFiles {
    type Page = Vec<Box<dyn EngineData>>;
}

/// Paginated declarative-plan execution.
impl PagedOperation for PlanOperation {
    type Page = Vec<Box<dyn EngineData>>;
}
