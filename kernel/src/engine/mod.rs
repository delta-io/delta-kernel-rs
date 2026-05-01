//! Provides an engine implementation that implements the required traits. The engine can optionally
//! be built into the kernel by setting the `default-engine-rustls` or `default-engine-native-tls`
//! feature flag. See the related module for more information.

#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_reader::ArrowReaderOptions;
#[cfg(feature = "arrow-expression")]
use crate::parquet::arrow::arrow_writer::ArrowWriterOptions;

/// Returns the standard [`ArrowReaderOptions`] for all default engine parquet reads.
///
/// Skipping the embedded Arrow IPC schema avoids dependence on Arrow-specific metadata and
/// ensures that type resolution is driven by the kernel schema rather than the file's schema.
#[cfg(feature = "arrow-expression")]
pub(crate) fn reader_options() -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_skip_arrow_metadata(true)
}

/// Returns the standard [`ArrowWriterOptions`] for all kernel parquet writes.
///
/// Omitting the Arrow IPC schema from the file metadata keeps Delta files interoperable with
/// non-Arrow readers and avoids encoding Arrow-specific type information.
#[cfg(feature = "arrow-expression")]
pub(crate) fn writer_options() -> ArrowWriterOptions {
    ArrowWriterOptions::new().with_skip_arrow_metadata(true)
}

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(all(feature = "arrow-expression", feature = "internal-api"))]
pub mod arrow_utils;
#[cfg(all(feature = "arrow-expression", not(feature = "internal-api")))]
pub(crate) mod arrow_utils;
#[cfg(feature = "internal-api")]
pub use self::arrow_utils::{parse_json, to_json_bytes};

#[cfg(feature = "arrow-expression")]
mod parse_json_geo;

#[cfg(feature = "default-engine-base")]
pub mod default;

#[cfg(test)]
pub(crate) mod sync;

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

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod cross_engine_tests;
