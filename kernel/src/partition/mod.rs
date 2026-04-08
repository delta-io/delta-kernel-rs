//! Partition utilities for Delta table writes.
//!
//! This module contains three submodules that handle different aspects of partition
//! processing. The same partition value flows through two independent transformations:
//!
//! ```text
//! Scalar::String("US/East")
//!         |
//!         |  (1) serialization: serialize for the Delta log
//!         v
//!   Some("US/East")           <- stored in AddFile.partitionValues as-is
//!         |
//!         |  (2) hive: encode for the file system path
//!         v
//!   "US%2FEast"               <- used in directory name: region=US%2FEast/
//! ```
//!
//! ## Submodules
//!
//! - [`hive`]: Hive-style partition path percent-encoding (`escape_partition_value`,
//!   `build_partition_path`). Public convenience utilities for custom engines.
//! - [`serialization`]: Converts typed `Scalar` values to protocol-compliant strings for
//!   the `partitionValues` map in Add actions. Public for custom engines.
//! - `validation`: Validates partition keys and value types. Internal to the write path.

pub mod hive;
pub mod serialization;
pub(crate) mod validation;

// Re-export commonly used items from hive for convenience.
pub use hive::{build_partition_path, escape_partition_value, HIVE_DEFAULT_PARTITION};
