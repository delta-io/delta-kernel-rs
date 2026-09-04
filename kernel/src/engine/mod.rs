//! Engine infrastructure shared by `Engine` implementations.
//!
//! The default Arrow/Tokio engine lives in the separate `delta_kernel_default_engine` crate.
//! `SyncEngine` is included only in test builds.

#[cfg(feature = "arrow-expression")]
use delta_kernel_derive::internal_api;

#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_reader::ArrowReaderOptions;
#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_writer::ArrowWriterOptions;

/// Returns the standard [`ArrowReaderOptions`] for all default engine parquet reads.
///
/// Skipping the embedded Arrow IPC schema avoids dependence on Arrow-specific metadata and
/// ensures that type resolution is driven by the kernel schema rather than the file's schema.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn reader_options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_skip_arrow_metadata(true)
}

/// Returns the standard [`ArrowWriterOptions`] for all kernel parquet writes.
///
/// Omitting the Arrow IPC schema from the file metadata keeps Delta files interoperable with
/// non-Arrow readers and avoids encoding Arrow-specific type information.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn writer_options() -> ArrowWriterOptions {
    ArrowWriterOptions::new().with_skip_arrow_metadata(true)
}

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

/// Invalid Arrow input or reader state encountered while evaluating engine operations.
#[derive(Debug, thiserror::Error)]
pub enum ArrowEngineError {
    /// A requested row is outside the array.
    #[error("Row index {index} out of bounds for {field} of length {length}")]
    RowIndexOutOfBounds {
        /// Name of the field or operation being read.
        field: String,
        /// Requested logical row index.
        index: usize,
        /// Number of available logical rows.
        length: usize,
    },
    /// A row group was requested more than once.
    #[error("Found duplicate row group ordinal {ordinal}")]
    DuplicateRowGroupOrdinal {
        /// Repeated ordinal.
        ordinal: usize,
    },
    /// A requested row group does not exist.
    #[error("Row group ordinal {ordinal} is out of bounds for {count} row groups")]
    RowGroupOutOfBounds {
        /// Requested ordinal.
        ordinal: usize,
        /// Available row groups.
        count: usize,
    },
    /// A nested reader projection returned an invalid number of children.
    #[error("{container} projection expected {expected} reorder indices, got {actual}")]
    ProjectionChildCount {
        /// Container whose children are projected.
        container: &'static str,
        /// Required child count.
        expected: usize,
        /// Observed child count.
        actual: usize,
    },
    /// A JSON input row does not contain exactly one complete JSON object.
    #[error("Malformed JSON: Multiple, partial, or 0 JSON objects on row {row}")]
    InvalidJsonRow {
        /// One-based input row number.
        row: usize,
    },
    /// JSON decoding produced a different number of rows than requested.
    #[error("Unexpected number of rows decoded. Got {actual}, expected {expected}")]
    JsonRowCount {
        /// Number of input rows.
        expected: usize,
        /// Number of decoded rows.
        actual: usize,
    },
    /// JSON decoding yielded no batch for nonempty input.
    #[error("Malformed JSON: no batch decoded for {expected} input rows")]
    MissingJsonBatch {
        /// Number of input rows.
        expected: usize,
    },
}

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(all(feature = "arrow-expression", feature = "internal-api"))]
pub mod arrow_utils;
#[cfg(all(feature = "arrow-expression", not(feature = "internal-api")))]
pub(crate) mod arrow_utils;
#[cfg(all(feature = "internal-api", feature = "arrow-expression"))]
pub use self::arrow_utils::{parse_json, to_json_bytes};

// The plan executor support modules read Arrow data (`arrow_utils`, `arrow_data`), so they
// require the Arrow engine base in addition to the declarative-plans IR.
#[cfg(all(feature = "declarative-plans", feature = "default-engine-base"))]
pub mod plans;

#[cfg(test)]
pub(crate) mod sync;

#[cfg(test)]
pub(crate) mod test_delegating;

#[cfg(feature = "default-engine-base")]
pub mod arrow_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod arrow_get_data;

#[cfg(all(feature = "default-engine-base", feature = "internal-api"))]
pub mod ensure_data_types;
#[cfg(all(feature = "default-engine-base", not(feature = "internal-api")))]
pub(crate) mod ensure_data_types;
#[cfg(feature = "default-engine-base")]
// module is always pub; trait inside is gated by #[internal_api]
pub mod parquet_row_group_skipping;
#[cfg(all(test, feature = "default-engine-base"))]
pub(crate) mod test_utils;
