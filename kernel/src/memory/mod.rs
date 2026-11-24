//! Fallible metadata storage with an optional explicit allocator budget.
//!
//! Normal builds use standard containers on stable Rust 1.95. The `memory-budget` feature requires
//! stable Rust 1.100 and selects one shared `std::alloc::Allocator` for participating storage.
//! FFI enables that feature. No process-global limiting allocator or nightly compiler is required.
//! Charges follow backing storage across calls, shared owners, threads, and FFI release functions.
//!
//! Budget rejection is recoverable at fallible allocation sites. Ordinary dependency allocations
//! remain outside this budget; infallible operations on allocator-backed storage can still invoke
//! the allocation-error handler. The limit is not a bound on RSS or all native memory.
//!
//! ```no_run
//! # #[cfg(feature = "memory-budget")]
//! # fn example(engine: &dyn delta_kernel::Engine) -> delta_kernel::Result<()> {
//! use delta_kernel::{Snapshot, memory::KERNEL_ALLOCATOR};
//! KERNEL_ALLOCATOR.set_limit(Some(64 * 1024 * 1024))
//!     .map_err(delta_kernel::KernelError::from)?;
//! let snapshot = Snapshot::builder_for("file:///table/").build(engine)?;
//! let _scan = snapshot.scan_builder().build()?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "memory-budget")]
// Cargo's package MSRV does not describe this opt-in backend's Rust 1.100 requirement.
#[allow(clippy::incompatible_msrv)]
mod allocator;
mod containers;
mod json;
mod keys;
pub(crate) mod ordered_map;
#[cfg(feature = "default-engine-base")]
mod output;
mod owned;
mod shared_string;
#[cfg_attr(feature = "memory-budget", allow(clippy::incompatible_msrv))]
mod storage;
#[cfg(all(test, feature = "memory-budget"))]
#[allow(clippy::incompatible_msrv)]
mod tests;

#[cfg(feature = "memory-budget")]
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
#[cfg(feature = "memory-budget")]
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

fn allocation_error() -> MemoryError {
    #[cfg(feature = "memory-budget")]
    return KERNEL_ALLOCATOR.allocation_error();
    #[cfg(not(feature = "memory-budget"))]
    MemoryError::AllocationFailed { limit: usize::MAX }
}
