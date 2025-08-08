//! This module re-exports the different versions of arrow, parquet, and object_store we support.

#[cfg(feature = "arrow-56")]
mod arrow_compat_shims {
    pub use arrow_56 as arrow;
    pub use object_store_55 as object_store;
    pub use parquet_56 as parquet;
}

#[cfg(all(feature = "arrow-55", not(feature = "arrow-56")))]
mod arrow_compat_shims {
    pub use arrow_55 as arrow;
    pub use object_store_55 as object_store;
    pub use parquet_55 as parquet;
}

// if nothing is enabled but we need arrow because of some other feature flag, throw compile-time
// error
#[cfg(all(
    feature = "need-arrow",
    not(feature = "arrow-55"),
    not(feature = "arrow-56")
))]
compile_error!("Requested a feature that needs arrow without enabling arrow. Please enable the `arrow-55` or `arrow-56` feature");

#[cfg(any(feature = "arrow-55", feature = "arrow-56"))]
pub use arrow_compat_shims::*;
