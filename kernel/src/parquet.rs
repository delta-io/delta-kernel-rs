//! This module exists to help re-export the version of arrow used by default-engine and other
//! parts of kernel that need arrow

#[cfg(feature = "arrow_53")]
pub use parquet_53::*;

#[cfg(all(feature = "arrow_54", not(feature = "arrow_53")))]
pub use parquet_54::*;

// if nothing is enabled but we need arrow because of some other feature flag, default to lowest
// supported version
#[cfg(all(
    feature = "need_arrow",
    not(feature = "arrow_53"),
    not(feature = "arrow_54")
))]
pub use parquet_53::*;
