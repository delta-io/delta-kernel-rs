//! This module re-exports the different versions of arrow, parquet, and object_store we support.

#[cfg(feature = "arrow-60")]
mod arrow_compat_shims {
    pub use arrow_60 as arrow;
    pub use parquet_60 as parquet;

    pub mod object_store {
        pub use object_store_14::*;

        #[doc(hidden)]
        pub mod delta_kernel_compat {
            use std::ops::Range;

            use super::{Attributes, GetResult, GetResultPayload, ObjectMeta, PutResult};

            /// Creates a put result without an entity tag or object version.
            pub fn empty_put_result() -> PutResult {
                PutResult {
                    e_tag: None,
                    version: None,
                    extensions: Default::default(),
                }
            }

            /// Creates a get result from its payload, metadata, range, and attributes.
            pub fn get_result(
                payload: GetResultPayload,
                meta: ObjectMeta,
                range: Range<u64>,
                attributes: Attributes,
            ) -> GetResult {
                GetResult {
                    payload,
                    meta,
                    range,
                    attributes,
                    extensions: Default::default(),
                }
            }
        }
    }
}

#[cfg(all(feature = "arrow-59", not(feature = "arrow-60")))]
mod arrow_compat_shims {
    pub use arrow_59 as arrow;
    pub use parquet_59 as parquet;

    pub mod object_store {
        pub use object_store_13::*;

        #[doc(hidden)]
        pub mod delta_kernel_compat {
            use std::ops::Range;

            use super::{Attributes, GetResult, GetResultPayload, ObjectMeta, PutResult};

            /// Creates a put result without an entity tag or object version.
            pub fn empty_put_result() -> PutResult {
                PutResult {
                    e_tag: None,
                    version: None,
                }
            }

            /// Creates a get result from its payload, metadata, range, and attributes.
            pub fn get_result(
                payload: GetResultPayload,
                meta: ObjectMeta,
                range: Range<u64>,
                attributes: Attributes,
            ) -> GetResult {
                GetResult {
                    payload,
                    meta,
                    range,
                    attributes,
                }
            }
        }
    }
}

// if nothing is enabled but we need arrow because of some other feature flag, throw compile-time
// error
#[cfg(all(
    feature = "need-arrow",
    not(feature = "arrow-59"),
    not(feature = "arrow-60")
))]
compile_error!(
    "Requested a feature that needs arrow without enabling arrow. Please enable the `arrow-59` or `arrow-60` feature"
);

#[cfg(any(feature = "arrow-59", feature = "arrow-60"))]
#[doc(hidden)]
pub use arrow_compat_shims::*;
