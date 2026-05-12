//! Engine infrastructure shared by [`Engine`] implementations.
//!
//! The default Arrow/Tokio engine lives in the separate `delta_kernel_default_engine` crate.
//! [`SyncEngine`] is included only in test builds.

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

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(all(feature = "arrow-expression", feature = "internal-api"))]
pub mod arrow_utils;
#[cfg(all(feature = "arrow-expression", not(feature = "internal-api")))]
pub(crate) mod arrow_utils;
#[cfg(feature = "internal-api")]
pub use self::arrow_utils::{parse_json, to_json_bytes};

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

// The handler contract helpers in this module are test fixtures: they unwrap freely and panic
// on failure (as any assert! does). The kernel's deny-list of `unwrap_used` / `expect_used` /
// `panic` is intended for production code and is already re-allowed under `cfg(test)`; we
// extend the same allowance when `internal-api` makes these helpers callable from external
// test crates (e.g. `delta_kernel_default_engine`).
#[cfg(any(test, feature = "internal-api"))]
#[cfg_attr(
    feature = "internal-api",
    allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
pub mod tests;
