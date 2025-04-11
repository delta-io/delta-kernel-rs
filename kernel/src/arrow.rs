//! This module exists to help re-export the version of arrow used by default-engine and other
//! parts of kernel that need arrow

#[cfg(all(feature = "arrow_55"))]
pub use arrow_55::*;

// if nothing is enabled but we need arrow because of some other feature flag, default to lowest
// supported version
#[cfg(all(feature = "need_arrow", not(feature = "arrow_55")))]
compile_error!("Requested a feature that needs arrow without enabling arrow. Please enable the `arrow_53` or `arrow_54` feature");
