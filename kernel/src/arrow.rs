//! This module exists to help re-export the version of arrow used by default-engine and other
//! parts of kernel that need arrow

#[cfg(feature = "arrow_53")]
pub use arrow_53::*;

#[cfg(all(feature = "arrow_54", not(feature = "arrow_53")))]
pub use arrow_54::*;
