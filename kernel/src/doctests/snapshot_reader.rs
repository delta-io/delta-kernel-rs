//! Compile-time visibility checks for snapshot metadata reads.

/// Metadata reads require a reader, including when no cancellation token is needed.
///
/// ```compile_fail,E0624
/// # use delta_kernel::{Engine, Snapshot};
/// # fn read(snapshot: &Snapshot, engine: &dyn Engine) {
/// snapshot.get_domain_metadata("my-domain", engine);
/// # }
/// ```
#[cfg(doctest)]
pub struct DomainMetadataRequiresReader;

/// Snapshot helpers remain private when `internal-api` exposes reader getters.
///
/// ```compile_fail,E0624
/// # use delta_kernel::{Engine, Snapshot};
/// # fn read(snapshot: &Snapshot, engine: &dyn Engine) {
/// snapshot.get_physical_clustering_columns(engine);
/// # }
/// ```
#[cfg(doctest)]
pub struct PhysicalClusteringColumnsRequiresReader;

/// The cancellation engine adapter is internal to Kernel.
///
/// ```compile_fail,E0603
/// use delta_kernel::engine::CancellableEngine;
/// ```
#[cfg(doctest)]
pub struct CancellationEngineIsPrivate;
