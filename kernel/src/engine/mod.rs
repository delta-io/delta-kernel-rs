//! Engine infrastructure shared by `Engine` implementations.
//!
//! The default Arrow/Tokio engine lives in the separate `delta_kernel_default_engine` crate.
//! `SyncEngine` is included only in test builds.

mod cancellable_engine;
pub(crate) use cancellable_engine::CancellableEngine;
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

/// Returns the [`ArrowWriterOptions`] for writing batches of `arrow_schema` as a data file with
/// `physical_kernel_schema`: the [`writer_options`], with each `file` column written as a group
/// annotated with the Parquet `FILE` logical type whose sub-fields carry the field ids the column
/// reserves. Fails if `physical_kernel_schema` has a `file` column and the `arrow-60` feature is
/// not enabled, if a `file` column is missing from `arrow_schema`, or if its sub-field ids
/// overflow.
#[cfg(feature = "arrow-expression")]
#[internal_api]
pub(crate) fn data_file_writer_options(
    arrow_schema: &crate::arrow::datatypes::Schema,
    physical_kernel_schema: &crate::schema::StructType,
) -> crate::KernelResult<ArrowWriterOptions> {
    parquet_file_type::with_file_columns(writer_options(), arrow_schema, physical_kernel_schema)
}

#[cfg(feature = "arrow-expression")]
mod parquet_file_type;

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(all(feature = "default-engine-base", feature = "internal-api"))]
pub mod arrow_utils;
#[cfg(all(feature = "default-engine-base", not(feature = "internal-api")))]
pub(crate) mod arrow_utils;
#[cfg(all(feature = "default-engine-base", feature = "internal-api"))]
pub use self::arrow_utils::{parse_json, to_json_bytes};

// The plan executor support modules read Arrow data (`arrow_utils`, `arrow_data`), so they
// require the Arrow engine base in addition to the declarative-plans IR.
#[cfg(all(feature = "declarative-plans", feature = "default-engine-base"))]
pub mod plans;

#[cfg(any(
    test,
    all(
        feature = "test-utils",
        feature = "declarative-plans",
        feature = "default-engine-base"
    )
))]
pub mod sync;

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
