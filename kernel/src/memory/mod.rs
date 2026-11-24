//! Shared allocation tracking for allocation-heavy Kernel metadata using stable `GlobalAlloc`.
//!
//! Install [`KernelAllocator`] as the global allocator to track standard containers, dependency
//! allocations, and Rust-owned Arrow buffers. Its requested-byte limit is disabled by default.
//! Set or clear it through [`KERNEL_ALLOCATOR`] or the FFI `kernel_memory_set_limit` entry point.
//! Usage follows backing allocations until they are freed, across calls and threads.
//!
//! Fallible allocation APIs return errors on rejection. Infallible paths, including dependency
//! allocations, can still invoke Rust's allocation-error handler. The limit covers requested Rust
//! allocations through this hook, rather than RSS, direct libc allocations, or memory mappings.
//!
//! ```no_run
//! # use delta_kernel::{Engine, Snapshot, memory::{KERNEL_ALLOCATOR, KernelAllocator}};
//! #[global_allocator]
//! static ALLOCATOR: KernelAllocator = KernelAllocator::new();
//! # fn example(engine: &dyn Engine) -> delta_kernel::Result<()> {
//! KERNEL_ALLOCATOR.set_limit(Some(64 * 1024 * 1024))
//!     .map_err(delta_kernel::KernelError::from)?;
//! let snapshot = Snapshot::builder_for("file:///table/").build(engine)?;
//! let scan = snapshot.scan_builder().build()?;
//! for batch in scan.scan_metadata(engine)? {
//!     let _batch = batch?;
//! }
//! assert!(KERNEL_ALLOCATOR.usage().reserved <= 64 * 1024 * 1024);
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

pub use allocator::{KERNEL_ALLOCATOR, KernelAllocator, MemoryUsage};
pub use containers::{IntoIter, MetadataBytes, MetadataString, MetadataVec, try_to_json};
pub use json::{JsonValue, try_parse_json};
pub use keys::FileKeySet;
pub use ordered_map::OrderedMap;
#[cfg(feature = "default-engine-base")]
pub(crate) use output::JsonOutput;
pub use owned::Owned;
pub use shared_string::SharedString;

/// A failure that can be propagated without allocating an error message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryError {
    /// The requested limit is below current tracked usage.
    LimitBelowUsage,
    /// The shared allocator could not satisfy a container allocation.
    AllocationFailed {
        /// The configured cap, or `usize::MAX` when enforcement is disabled.
        limit: usize,
    },
    /// A requested container size cannot be represented.
    CapacityOverflow,
    /// The serializer rejected its input independently of the output allocation.
    Serialization,
    /// Input is not valid JSON under the parser's supported nesting limit.
    InvalidJson,
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitBelowUsage => f.write_str("memory limit is below current tracked usage"),
            Self::AllocationFailed { limit } => {
                write!(f, "allocation failed under a {limit}-byte metadata budget")
            }
            Self::CapacityOverflow => f.write_str("container capacity overflow"),
            Self::Serialization => f.write_str("metadata serialization failed"),
            Self::InvalidJson => f.write_str("invalid metadata JSON"),
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

// Fixed canonical schemas have infallible constructors. Complete their initialization before any
// cap can be published; query-sized schema construction uses the fallible storage paths.
fn initialize_schemas() {
    static INITIALIZED: std::sync::Once = std::sync::Once::new();
    INITIALIZED.call_once(|| {
        crate::actions::initialize_schemas();
        crate::scan::initialize_schemas();
        crate::transaction::initialize_schemas();
        crate::checkpoint::initialize_schemas();
        #[cfg(feature = "declarative-plans")]
        std::sync::LazyLock::force(&crate::plans::ir::nodes::FILE_META_SCHEMA);
        std::sync::LazyLock::force(&crate::schema::FILE_FIELDS);
        std::sync::LazyLock::force(&crate::schema::VARIANT_FIELDS);
    });
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
