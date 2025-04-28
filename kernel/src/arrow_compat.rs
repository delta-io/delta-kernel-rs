//! This module re-exports the different versions of arrow, parquet, and object_store we support.

#[cfg(feature = "arrow-55")]
mod arrow_compat_shims {
    pub use arrow_55 as arrow;
    pub use object_store_55 as object_store;
    pub use parquet_55 as parquet;
}

#[cfg(all(feature = "arrow-54", not(feature = "arrow-55")))]
mod arrow_compat_shims {
    pub use arrow_54 as arrow;
    pub use object_store_54 as object_store;
    pub use parquet_54 as parquet;
}

// if nothing is enabled but we need arrow because of some other feature flag, throw compile-time
// error
#[cfg(all(
    feature = "need-arrow",
    not(feature = "arrow-54"),
    not(feature = "arrow-55")
))]
compile_error!("Requested a feature that needs arrow without enabling arrow. Please enable the `arrow-54` or `arrow-55` feature");

#[cfg(any(feature = "arrow-54", feature = "arrow-55"))]
pub use arrow_compat_shims::*;
