//! This module exists to help re-export the version of arrow used by default-engine and other
//! parts of kernel that need arrow

#[cfg(all(feature = "arrow_53", feature = "arrow_54"))]
compile_error!("Multiple versions of the arrow cannot be used at the same time!");

#[cfg(feature = "arrow_53")]
pub use parquet_53::*;

#[cfg(feature = "arrow_54")]
pub use parquet_54::*;
