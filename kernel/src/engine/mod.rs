//! Provides an engine implementation that implements the required traits. The engine can optionally
//! be built into the kernel by setting the `default-engine` feature flag. See the related module
//! for more information.

#[cfg(feature = "arrow-conversion")]
pub mod arrow_conversion;

#[cfg(all(feature = "arrow-expression", feature = "default-engine-base"))]
pub mod arrow_expression;
#[cfg(feature = "arrow-expression")]
pub(crate) mod arrow_utils;
#[cfg(feature = "internal-api")]
pub use self::arrow_utils::{parse_json, to_json_bytes};

#[cfg(feature = "default-engine-base")]
pub mod default;

#[cfg(test)]
pub(crate) mod sync;

#[cfg(feature = "default-engine-base")]
pub mod arrow_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod arrow_get_data;
#[cfg(feature = "default-engine-base")]
pub(crate) mod ensure_data_types;
#[cfg(feature = "default-engine-base")]
pub mod parquet_row_group_skipping;

#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod cross_engine_tests;
