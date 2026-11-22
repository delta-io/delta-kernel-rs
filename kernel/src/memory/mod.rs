//! Experimental fallible storage for driver metadata. Requires Rust 1.100.
//!
//! Install [`MemoryBudget::install_global`] before any Kernel work to share one cap across ordinary
//! snapshot, scan, transaction, and FFI calls. Derived objects, returned wire buffers, and lazy
//! iterators retain their allocator until deallocation, including across threads and after the
//! originating call returns. Callers do not enter thread-local scopes.
//!
//! Explicit builder `with_allocator` methods optionally impose an additional local cap.
//! [`MemoryBudget::with`] is a construction adapter for standalone serde calls.
//! This does not intercept global allocations or bound engine-owned Arrow, Parquet, or parser
//! scratch storage.
//!
//! ```no_run
//! # use delta_kernel::{Engine, Snapshot, memory::MemoryBudget};
//! # fn example(engine: &dyn Engine) -> delta_kernel::Result<()> {
//! let budget = MemoryBudget::install_global(64 * 1024 * 1024)
//!     .map_err(delta_kernel::KernelError::from)?;
//! let snapshot = Snapshot::builder_for("file:///table/").build(engine)?;
//! let scan = snapshot.scan_builder().build()?;
//! for batch in scan.scan_metadata(engine)? {
//!     let _batch = batch?;
//! }
//! assert!(budget.usage().peak <= budget.usage().limit);
//! # Ok(())
//! # }
//! ```

mod allocator;
mod containers;
mod json;
mod keys;
pub(crate) mod ordered_map;
#[cfg(feature = "default-engine-base")]
mod output;
mod owned;
mod shared_string;
#[cfg(test)]
mod tests;

pub(crate) use allocator::{AllocatedIterator, AllocationScope};
pub use allocator::{LimitedAllocator, MemoryBudget, MemoryUsage};
pub use containers::{
    LimitedBytes, LimitedIntoIter, LimitedString, LimitedVec, RetainedVec, try_to_json,
};
pub use json::{LimitedJson, try_parse_json};
pub use keys::FileKeySet;
pub use ordered_map::OrderedMap;
#[cfg(feature = "default-engine-base")]
pub(crate) use output::JsonOutput;
pub use owned::Owned;
pub use shared_string::SharedString;

/// A failure that can be propagated without allocating an error message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryError {
    /// Global policy was already sealed by configuration or allocator construction.
    AlreadyConfigured,
    /// The budget or backing allocator could not satisfy a container allocation.
    AllocationFailed {
        /// The requested-byte cap shared by the operation's containers.
        limit: usize,
    },
    /// A requested container size cannot be represented.
    CapacityOverflow,
    /// The serializer rejected its input independently of the output allocation budget.
    Serialization,
    /// Input is not valid JSON under the parser's supported nesting limit.
    InvalidJson,
    /// A schema exceeds the experiment's recursion-depth limit.
    SchemaTooDeep,
    /// A top-level schema projection contains an invalid field index.
    InvalidProjection,
    /// A log-tail entry does not follow the previous entry's version.
    InvalidLogOrder,
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyConfigured => f.write_str("global memory policy is already configured"),
            Self::AllocationFailed { limit } => {
                write!(f, "allocation failed under a {limit}-byte metadata budget")
            }
            Self::CapacityOverflow => f.write_str("container capacity overflow"),
            Self::Serialization => f.write_str("metadata serialization failed"),
            Self::InvalidJson => f.write_str("invalid metadata JSON"),
            Self::SchemaTooDeep => f.write_str("schema nesting limit exceeded"),
            Self::InvalidProjection => f.write_str("invalid schema projection"),
            Self::InvalidLogOrder => f.write_str("log tail is not in consecutive version order"),
        }
    }
}

impl std::error::Error for MemoryError {}

/// Result of a fallible metadata-storage operation.
pub type MemoryResult<T> = Result<T, MemoryError>;

impl From<MemoryError> for crate::KernelError {
    fn from(error: MemoryError) -> Self {
        Self::Memory(error)
    }
}

thread_local! {
    static FAILURE: std::cell::Cell<Option<MemoryError>> = const { std::cell::Cell::new(None) };
}
pub(crate) fn record_failure(error: MemoryError) {
    FAILURE.with(|failure| failure.set(Some(error)));
}
pub(crate) fn take_failure() -> Option<MemoryError> {
    FAILURE.with(|failure| failure.take())
}

pub(crate) fn decode_json<T: serde::de::DeserializeOwned>(input: &[u8]) -> crate::KernelResult<T> {
    take_failure();
    serde_json::from_slice(input).map_err(|error| match take_failure() {
        Some(failure) => failure.into(),
        None => crate::KernelError::MalformedJson(error),
    })
}
