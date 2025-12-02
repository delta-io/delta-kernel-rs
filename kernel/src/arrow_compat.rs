//! This module re-exports the different versions of arrow, parquet, and object_store we support.

#[cfg(feature = "arrow-57")]
mod arrow_compat_shims {
    pub use arrow_57 as arrow;
    pub use parquet_57 as parquet;
}

#[cfg(all(feature = "arrow-56", not(feature = "arrow-57"),))]
mod arrow_compat_shims {
    pub use arrow_56 as arrow;
    pub use parquet_56 as parquet;
}

// if nothing is enabled but we need arrow because of some other feature flag, throw compile-time
// error
#[cfg(all(
    feature = "need-arrow",
    not(feature = "arrow-56"),
    not(feature = "arrow-57")
))]
compile_error!("Requested a feature that needs arrow without enabling arrow. Please enable the `arrow-56`, or `arrow-57` feature");

#[cfg(any(feature = "arrow-56", feature = "arrow-57"))]
pub use arrow_compat_shims::*;
