//! Provides engine implementation that implement the required traits. These engines can optionally
//! be built into the kernel by setting the `default-engine` or `sync-engine` feature flags. See the
//! related modules for more information.

#[cfg(feature = "arrow-conversion")]
pub(crate) mod arrow_conversion;

#[cfg(all(
    feature = "arrow-expression",
    any(feature = "default-engine", feature = "sync-engine")
))]
pub mod arrow_expression;

#[cfg(feature = "default-engine")]
pub mod default;

#[cfg(feature = "sync-engine")]
pub mod sync;

macro_rules! declare_modules {
    ( $(($vis:vis, $module:ident)),*) => {
        $(
            $vis mod $module;
        )*
    };
}

#[cfg(any(feature = "default-engine", feature = "sync-engine"))]
declare_modules!(
    (pub, arrow_data),
    (pub, parquet_row_group_skipping),
    (pub, parquet_stats_skipping),
    (pub(crate), arrow_get_data),
    (pub(crate), arrow_utils),
    (pub(crate), ensure_data_types)
);
