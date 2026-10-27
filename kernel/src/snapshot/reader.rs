//! Operation-scoped access to snapshot metadata that may require storage reads.

use std::collections::HashSet;

use delta_kernel_derive::internal_api;

use super::Snapshot;
use crate::actions::DomainMetadata;
use crate::clustering::ClusteringColumnInfo;
use crate::engine::CancellableEngine;
use crate::expressions::ColumnName;
use crate::log_segment::DomainMetadataMap;
use crate::{CancellationTokenRef, Engine, KernelResult};

/// Reads snapshot metadata with an optional operation-scoped cancellation token.
///
/// Use [`Snapshot::reader`] for domain metadata, clustering columns, application transactions,
/// row-tracking metadata, and commit timestamps. These getters can read storage when the snapshot
/// does not already contain the required metadata. In-memory getters such as [`Snapshot::schema`]
/// and [`Snapshot::version`] remain on the snapshot.
///
/// Getters run synchronously and may block on I/O. By default, they use the supplied engine's
/// normal cancellation and timeout behavior. [`Self::with_cancellation_token`] adds cooperative
/// cancellation to handler-backed reads; it does not make the getters asynchronous. Cached
/// results can be returned without checking the token, and interrupting in-flight I/O depends
/// on the engine. Creating or configuring a reader does not issue I/O.
///
/// The reader borrows its snapshot and retains no engine. The snapshot retains neither the reader
/// nor its token, so independent operations can use different readers on the same snapshot.
///
/// ```no_run
/// # use delta_kernel::{CancellationTokenRef, Engine, KernelResult, Snapshot};
/// # fn read_metadata(
/// #     snapshot: &Snapshot,
/// #     engine: &dyn Engine,
/// #     token: CancellationTokenRef,
/// # ) -> KernelResult<()> {
/// // Read synchronously with the engine's normal behavior.
/// let domain = snapshot.reader().get_domain_metadata("my-domain", engine)?;
/// // Share an operation's cancellation scope across related metadata reads.
/// let reader = snapshot.reader().with_cancellation_token(token);
/// let version = reader.get_app_id_version("my-app", engine)?;
/// let timestamp = reader.get_timestamp(engine)?;
/// # Ok(())
/// # }
/// ```
///
/// Metadata reads require a reader, including when no cancellation token is needed:
///
/// ```compile_fail,E0624
/// # use delta_kernel::{Engine, Snapshot};
/// # fn read(snapshot: &Snapshot, engine: &dyn Engine) {
/// snapshot.get_domain_metadata("my-domain", engine);
/// # }
/// ```
///
/// The same restriction applies to getters exposed by `internal-api`:
///
/// ```compile_fail,E0624
/// # use delta_kernel::{Engine, Snapshot};
/// # fn read(snapshot: &Snapshot, engine: &dyn Engine) {
/// snapshot.get_physical_clustering_columns(engine);
/// # }
/// ```
///
/// The engine adapter is internal to Kernel:
///
/// ```compile_fail,E0603
/// use delta_kernel::engine::CancellableEngine;
/// ```
pub struct SnapshotReader<'a> {
    /// Immutable table state shared independently of this read scope.
    snapshot: &'a Snapshot,
    /// Optional token bound to each getter's handler-backed reads.
    cancellation_token: Option<CancellationTokenRef>,
}

#[cfg(test)]
mod tests;

impl<'a> SnapshotReader<'a> {
    /// Sets the cancellation token for this reader's handler-backed reads, or clears it with
    /// `None`.
    ///
    /// Reads return [`crate::KernelError::Cancelled`] when the engine observes cancellation.
    /// The original token is forwarded unchanged, and getters remain synchronous. This does not
    /// affect snapshot construction, scans, or other readers created from the same snapshot.
    pub fn with_cancellation_token(
        mut self,
        token: impl Into<Option<CancellationTokenRef>>,
    ) -> Self {
        self.cancellation_token = token.into();
        self
    }

    /// Returns the latest non-expired transaction version for `application_id`, or `None`.
    ///
    /// Uses `engine` to replay the log when CRC state cannot answer the query. Returns read,
    /// retention-property, or cancellation errors. Reports transaction-load success or failure.
    pub fn get_app_id_version(
        &self,
        application_id: &str,
        engine: &dyn Engine,
    ) -> KernelResult<Option<i64>> {
        self.read(engine, |snapshot, engine| {
            snapshot.get_app_id_version(application_id, engine)
        })
    }

    /// Returns the latest configuration for the user-controlled `domain`, or `None` if absent.
    ///
    /// Uses `engine` to replay the log when needed. Returns an error for system-controlled
    /// `delta.*` domains, read failures, or cancellation.
    pub fn get_domain_metadata(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> KernelResult<Option<String>> {
        self.read(engine, |snapshot, engine| {
            snapshot.get_domain_metadata(domain, engine)
        })
    }

    /// Returns the row-tracking high-water mark, or `None` if no active row-tracking domain exists.
    ///
    /// Uses `engine` to load domain metadata when needed. Returns read, malformed-configuration,
    /// or cancellation errors. [`crate::ROW_TRACKING_INITIAL_HIGH_WATER_MARK`] indicates that no
    /// row IDs have been assigned yet.
    pub fn get_row_tracking_high_water_mark(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<Option<i64>> {
        self.read(engine, Snapshot::get_row_tracking_high_water_mark)
    }

    /// Returns this version's timestamp in milliseconds since the Unix epoch.
    ///
    /// Uses the In-Commit Timestamp (ICT) when enabled, reading commit JSON through `engine` if
    /// needed; otherwise returns the retained commit-file modification time. Returns missing
    /// commit, invalid ICT configuration, read, or cancellation errors.
    pub fn get_timestamp(&self, engine: &dyn Engine) -> KernelResult<i64> {
        self.read(engine, Snapshot::get_timestamp)
    }

    /// Returns clustering descriptors in domain order, or `None` if clustering is absent.
    ///
    /// Each descriptor contains the physical name, logical name, and type. Uses `engine` to load
    /// domain metadata when needed. Returns malformed-metadata, column-resolution, read, or
    /// cancellation errors. An empty vector means the clustering domain names no columns.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn get_clustering_column_infos(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<Option<Vec<ClusteringColumnInfo>>> {
        self.read(engine, Snapshot::get_clustering_column_infos)
    }

    /// Returns physical clustering column names, or `None` if clustering is absent.
    ///
    /// Uses `engine` to load domain metadata when needed and preserves domain order and column
    /// mapping. Returns malformed-metadata, read, or cancellation errors. An empty vector means
    /// the clustering domain names no columns.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn get_physical_clustering_columns(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<Option<Vec<ColumnName>>> {
        self.read(engine, Snapshot::get_physical_clustering_columns)
    }

    /// Returns current metadata for `domains`, or all domains when the filter is `None`.
    ///
    /// Includes system-controlled domains. Uses `engine` to replay the log when CRC state cannot
    /// answer the query. Returns read or cancellation errors and reports domain-load metrics.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn get_domain_metadatas_internal(
        &self,
        engine: &dyn Engine,
        domains: Option<&HashSet<&str>>,
    ) -> KernelResult<DomainMetadataMap> {
        self.read(engine, |snapshot, engine| {
            snapshot.get_domain_metadatas_internal(engine, domains)
        })
    }

    /// Returns the latest configuration for a user or system `domain`, or `None` if absent.
    ///
    /// Unlike [`Self::get_domain_metadata`], accepts `delta.*` domains. Uses `engine` to replay
    /// the log when needed and returns read or cancellation errors.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn get_domain_metadata_internal(
        &self,
        domain: &str,
        engine: &dyn Engine,
    ) -> KernelResult<Option<String>> {
        self.read(engine, |snapshot, engine| {
            snapshot.get_domain_metadata_internal(domain, engine)
        })
    }

    /// Returns all active user-controlled domain metadata, excluding `delta.*` domains.
    ///
    /// Uses `engine` to replay the log when needed and returns read or cancellation errors.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn get_all_domain_metadata(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<Vec<DomainMetadata>> {
        self.read(engine, Snapshot::get_all_domain_metadata)
    }

    /// Returns this version's In-Commit Timestamp (ICT), or `None` when ICT is disabled.
    ///
    /// Uses `engine` to read the commit when no at-version CRC is available. Returns missing
    /// commit, invalid or missing ICT, read, or cancellation errors.
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[internal_api]
    pub(crate) fn get_in_commit_timestamp(&self, engine: &dyn Engine) -> KernelResult<Option<i64>> {
        self.read(engine, Snapshot::get_in_commit_timestamp)
    }

    /// Creates an unscoped metadata reader without issuing I/O.
    pub(super) fn new(snapshot: &'a Snapshot) -> Self {
        Self {
            snapshot,
            cancellation_token: None,
        }
    }

    /// Binds this reader's token for one getter while leaving the shared snapshot unchanged.
    fn read<T>(
        &self,
        engine: &dyn Engine,
        operation: impl FnOnce(&Snapshot, &dyn Engine) -> KernelResult<T>,
    ) -> KernelResult<T> {
        let scoped_engine = self
            .cancellation_token
            .as_ref()
            .map(|token| CancellableEngine::new(engine, token.clone()));
        let engine = scoped_engine
            .as_ref()
            .map_or(engine, |engine| engine as &dyn Engine);
        operation(self.snapshot, engine)
    }
}
