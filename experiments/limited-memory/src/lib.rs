//! Fallible, allocator-backed storage for Kernel metadata operations.
//!
//! Every payload allocated by these containers shares a [`MemoryBudget`]. Retained capacity stays
//! charged until its allocation is freed, even after the caller drops its budget handle. Existing
//! Kernel inputs, engine memory, allocator bookkeeping, and the budget's fixed control block are
//! outside the limit. This crate is an experiment, not a total snapshot or process memory bound.
//!
//! ```
//! use delta_kernel_limited_memory_poc::{LimitedBytes, MemoryBudget};
//!
//! let budget = MemoryBudget::new(8);
//! let mut buffer = LimitedBytes::try_copy(&budget, b"12345678")?;
//! assert!(buffer.try_push(b'9').is_err());
//! assert_eq!(&buffer[..], b"12345678");
//! drop(buffer);
//! assert_eq!(budget.usage().reserved, 0);
//! # Ok::<(), delta_kernel_limited_memory_poc::MemoryError>(())
//! ```

mod containers;
mod json;
mod lifecycle;
mod schema;

pub use delta_kernel::memory::{
    FileKeySet, LimitedAllocator, LimitedBytes, LimitedString, LimitedVec, MemoryBudget,
    MemoryError, MemoryResult, MemoryUsage, try_to_json,
};
pub use json::{LimitedJson, try_parse_json};
pub use lifecycle::{ActionBuffer, LogFile, LogTail};
pub use schema::LimitedSchema;
