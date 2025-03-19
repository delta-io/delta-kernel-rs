//! Provides engine implementation that implement the required traits. These engines can optionally
//! be built into the kernel by setting the `default-engine` or `sync-engine` feature flags. See the
//! related modules for more information.

#[cfg(feature = "arrow-conversion")]
pub(crate) mod arrow_conversion;

#[cfg(all(
    feature = "arrow-expression",
    any(feature = "default-engine-base", feature = "sync-engine")
))]
pub mod arrow_expression;
#[cfg(feature = "arrow-expression")]
pub(crate) mod arrow_utils;

#[cfg(feature = "default-engine-base")]
pub mod default;

#[cfg(feature = "sync-engine")]
pub mod sync;

#[cfg(any(feature = "default-engine-base", feature = "sync-engine"))]
pub mod arrow_data;
#[cfg(any(feature = "default-engine-base", feature = "sync-engine"))]
pub(crate) mod arrow_get_data;
#[cfg(any(feature = "default-engine-base", feature = "sync-engine"))]
pub(crate) mod ensure_data_types;
#[cfg(any(feature = "default-engine-base", feature = "sync-engine"))]
pub mod parquet_row_group_skipping;
