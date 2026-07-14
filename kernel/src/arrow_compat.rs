//! This module re-exports the different versions of arrow, parquet, and object_store we support.

#[cfg(feature = "arrow-59")]
mod arrow_compat_shims {
    pub use arrow_59 as arrow;
    pub use parquet_59 as parquet;

    pub mod object_store {
        pub use object_store_13::*;
    }
}

#[cfg(all(feature = "arrow-58", not(feature = "arrow-59")))]
mod arrow_compat_shims {
    pub use arrow_58 as arrow;
    pub use parquet_58 as parquet;

    pub mod object_store {
        pub use object_store_13::*;
    }
}

// if nothing is enabled but we need arrow because of some other feature flag, throw compile-time
// error
#[cfg(all(
    feature = "need-arrow",
    not(feature = "arrow-58"),
    not(feature = "arrow-59")
))]
compile_error!(
    "Requested a feature that needs arrow without enabling arrow. Please enable the `arrow-58` or `arrow-59` feature"
);

#[cfg(any(feature = "arrow-58", feature = "arrow-59"))]
#[doc(hidden)]
pub use arrow_compat_shims::*;
