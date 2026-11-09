//! Builder for creating [`Snapshot`] instances.

use std::sync::Arc;

use delta_kernel_derive::internal_api;
use tracing::{info, instrument};

use crate::cancellation::CancellationTokenRef;
use crate::engine::CancellableEngine;
use crate::log_path::LogPath;
use crate::log_segment::{LogSegment, validate_catalog_managed_log_tail};
use crate::log_segment_files::CheckpointHandling;
use crate::metrics::events::SNAPSHOT_COMPLETED_SPAN;
use crate::metrics::{MetricId, SnapshotLoadMetricContext, SnapshotLoadType};
use crate::path::DELTA_LOG_DIR_WITH_SLASH;
use crate::snapshot::{SnapshotHint, SnapshotRef};
use crate::utils::{PhantomType, is_absent_local_path, require, try_parse_uri};
use crate::{DeltaError, Engine, Error, KernelError, KernelResult, Result, Snapshot, Version};

/// Marker for builders that load a snapshot from a table root.
#[doc(hidden)]
pub struct FromTableRoot;

/// Marker for builders that incrementally update an existing snapshot.
#[doc(hidden)]
pub struct FromSnapshot;

/// Builder for creating [`Snapshot`] instances.
///
/// # Example
///
/// ```no_run
/// # use delta_kernel::{Snapshot, Engine};
/// # fn example(engine: &dyn Engine) -> delta_kernel::Result<()> {
/// let table_root = "file:///path/to/table/";
///
/// // Build a snapshot
/// let snapshot = Snapshot::builder_for(table_root)
///     .at_version(5) // Optional: specify a time-travel version (default is latest version)
///     .build(engine)?;
///
/// # Ok(())
/// # }
/// ```
pub struct SnapshotBuilder<Mode = FromTableRoot> {
    schema_recursion_limit: usize,
    table_root: Option<String>,
    existing_snapshot: Option<SnapshotRef>,
    version: Option<Version>,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
    incremental_replay: IncrementalReplay,
    checkpoint_handling: CheckpointHandling,
    snapshot_hint: Option<Box<SnapshotHint>>,
    /// Kernel-minted id correlating this build's metric events with its child events.
    operation_id: MetricId,
    /// Opaque, caller-supplied id recorded on this build's metric events. Not interpreted by
    /// kernel; set via [`with_correlation_id`](Self::with_correlation_id).
    correlation_id: Option<Arc<str>>,
    /// Optional cooperative cancellation token for this build's handler-backed reads.
    cancellation_token: Option<CancellationTokenRef>,
    // Carries the zero-sized typestate that limits mode-specific methods at compile time.
    mode: PhantomType<Mode>,
}

/// Builder for incrementally updating an existing [`Snapshot`].
pub type IncrementalSnapshotBuilder = SnapshotBuilder<FromSnapshot>;

// Hand-written because `CancellationToken` is not `Debug`: the token is projected to a bool. Ends
// with `finish_non_exhaustive` so a future field is not silently dropped from the output.
impl<Mode> std::fmt::Debug for SnapshotBuilder<Mode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotBuilder")
            .field("schema_recursion_limit", &self.schema_recursion_limit)
            .field("table_root", &self.table_root)
            .field("existing_snapshot", &self.existing_snapshot)
            .field("version", &self.version)
            .field("log_tail", &self.log_tail)
            .field("max_catalog_version", &self.max_catalog_version)
            .field("incremental_replay", &self.incremental_replay)
            .field("checkpoint_handling", &self.checkpoint_handling)
            .field("snapshot_hint", &self.snapshot_hint)
            .field("operation_id", &self.operation_id)
            .field("correlation_id", &self.correlation_id)
            .field("cancellable", &self.cancellation_token.is_some())
            .finish_non_exhaustive()
    }
}

/// Controls whether kernel replays commits to advance a stale base CRC (the existing snapshot's
/// in-memory CRC, or an on-disk CRC) to the target snapshot version on load. A CRC already at the
/// target version is always used regardless of this setting; this only bounds the cost of
/// advancing a *stale* CRC.
///
/// A resolved CRC gives the snapshot precomputed file statistics (file count and sizes, useful
/// for query optimization and for writers producing a post-commit CRC) along with domain metadata
/// and set transactions (useful for writers), all without extra log replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IncrementalReplay {
    /// Never advance a stale CRC; fall back to normal log replay. `UpToCommits(0)` is equivalent.
    #[default]
    Disabled,
    /// Advance only when the CRC is within `n` commits of the target version, i.e.
    /// `target_version - crc_version <= n`.
    UpToCommits(u64),
    /// Advance regardless of how stale the CRC is.
    Unlimited,
}

impl IncrementalReplay {
    pub(super) fn is_disabled(self) -> bool {
        self == Self::Disabled || self == Self::UpToCommits(0)
    }

    /// Whether the configured budget permits advancing a CRC at `crc_version` to `target_version`.
    /// Errors if `crc_version` is ahead of `target_version`, which violates a caller invariant.
    ///
    /// Example: 95.crc with commits 96.json through 100.json is 5 commits, so `UpToCommits(5)`
    /// advances and `UpToCommits(4)` does not; `Unlimited` always advances.
    pub(crate) fn should_advance(
        self,
        crc_version: Version,
        target_version: Version,
    ) -> KernelResult<bool> {
        let distance = target_version.checked_sub(crc_version).ok_or_else(|| {
            KernelError::internal_error(format!(
                "CRC version {crc_version} is ahead of target version {target_version}"
            ))
        })?;
        Ok(match self {
            IncrementalReplay::Disabled => false,
            IncrementalReplay::UpToCommits(n) => distance <= n,
            IncrementalReplay::Unlimited => true,
        })
    }
}

impl SnapshotBuilder<FromTableRoot> {
    // ============================================================================
    // Constructors
    // ============================================================================

    pub(crate) fn new_for(table_root: impl AsRef<str>) -> Self {
        Self {
            schema_recursion_limit: crate::actions::DEFAULT_SCHEMA_RECURSION_LIMIT,
            table_root: Some(table_root.as_ref().to_string()),
            existing_snapshot: None,
            version: None,
            log_tail: Vec::new(),
            max_catalog_version: None,
            incremental_replay: IncrementalReplay::default(),
            checkpoint_handling: CheckpointHandling::Adopt,
            snapshot_hint: None,
            operation_id: MetricId::new(),
            correlation_id: None,
            cancellation_token: None,
            mode: PhantomType::default(),
        }
    }

    /// Sets the JSON recursion limit used to decode this table's schema. The default is 128.
    ///
    /// A non-default limit bounds all JSON objects and arrays in `metaData.schemaString`,
    /// including field metadata and unknown fields. Decoding fails when nesting reaches the
    /// limit. This counts JSON containers, not columns: each nested struct adds three containers.
    /// Derived snapshots retain this setting, including incremental loads and metadata updates.
    ///
    /// The supported range is 1 through 1000; [`build`](Self::build) rejects other values.
    /// The upper bound limits recursion in schema operations beyond JSON decoding.
    pub fn with_schema_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.schema_recursion_limit = recursion_limit;
        self
    }

    /// Supply complete snapshot state that Kernel can validate and construct without engine I/O.
    ///
    /// The hint conflicts with [`with_log_tail`](Self::with_log_tail) and non-disabled incremental
    /// CRC replay. An explicit [`at_version`](Self::at_version) must equal the hint version. When
    /// no explicit version is set, a supplied maximum catalog version must also equal the hint
    /// version. The caller must canonicalize supplied log paths into the same URL form as the table
    /// root. Kernel preserves the paths, requires them to be beneath this builder's table log root,
    /// and validates structural consistency without reading the supplied files. The caller must
    /// ensure the protocol and metadata came from those files and any staged commits were ratified
    /// by the managing catalog.
    ///
    /// # Errors
    ///
    /// [`build`](Self::build) returns [`Error::Kernel`](crate::Error::Kernel) wrapping
    /// [`KernelError::SnapshotHint`] for hint conflicts and hinted log-segment validation failures.
    /// Catalog-version, table-root URI, protocol, and metadata failures retain their Kernel error
    /// variants, so a local table root that does not exist is
    /// [`KernelError::InvalidTableLocation`].
    #[allow(dead_code)]
    #[internal_api]
    pub(crate) fn with_snapshot_hint(mut self, hint: impl Into<Box<SnapshotHint>>) -> Self {
        self.snapshot_hint = Some(hint.into());
        self
    }
}

impl SnapshotBuilder<FromSnapshot> {
    pub(crate) fn new_from(existing_snapshot: SnapshotRef) -> Self {
        Self {
            schema_recursion_limit: existing_snapshot.table_configuration.schema_recursion_limit,
            table_root: None,
            existing_snapshot: Some(existing_snapshot),
            version: None,
            log_tail: Vec::new(),
            max_catalog_version: None,
            incremental_replay: IncrementalReplay::default(),
            checkpoint_handling: CheckpointHandling::Adopt,
            snapshot_hint: None,
            operation_id: MetricId::new(),
            correlation_id: None,
            cancellation_token: None,
            mode: PhantomType::default(),
        }
    }

    /// Skip adopting new checkpoints while updating the input snapshot.
    ///
    /// The resulting snapshot retains newly listed commits instead of adopting a newer checkpoint
    /// as its replay base. For example, when updating snapshot N to N+5, a checkpoint at N+3 is
    /// recognized but not adopted, and commits N+1 through N+5 remain in the log segment.
    ///
    /// The update fails if the new commits are not contiguous.
    ///
    /// This option increases memory use and may increase full-scan log replay. It is available
    /// only for builders created by [`Snapshot::builder_from`].
    pub fn skip_new_checkpoints(mut self) -> Self {
        self.checkpoint_handling = CheckpointHandling::Ignore;
        self
    }
}

impl<Mode> SnapshotBuilder<Mode> {
    // ============================================================================
    // Chainable configuration
    // ============================================================================

    /// Sets the target version of the [`Snapshot`].
    ///
    /// Without a snapshot hint, omitting this uses the supplied
    /// [`max_catalog_version`](Self::with_max_catalog_version), or discovers the latest version
    /// using the filesystem and any supplied [`log tail`](Self::with_log_tail) if no bound is set.
    /// With a hint, omitting this uses the hint version and its supplied freshness.
    ///
    /// An explicit target does not require a catalog bound solely because that version is
    /// catalog-managed. An explicit target must not exceed a supplied catalog bound.
    pub fn at_version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the log tail to use when building the snapshot. This allows catalogs or external
    /// systems to provide an up-to-date log tail when used to build a snapshot.
    ///
    /// A nonempty tail must be an ascending, contiguous sequence of commits without duplicates.
    /// For replay, it replaces filesystem commit files from its first version onward, including
    /// filesystem commits newer than the tail.
    ///
    /// Staged commits supplied in the tail must have been ratified by the managing catalog and
    /// require [`with_max_catalog_version`] even with an explicit target.
    ///
    /// See [`with_max_catalog_version`] for constraints on the last tail version when a bound is
    /// supplied.
    ///
    /// [`with_max_catalog_version`]: Self::with_max_catalog_version
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }

    /// Sets the maximum version ratified by the catalog as the snapshot's version bound.
    ///
    /// Without [`at_version`], the snapshot is built at this version even if newer filesystem
    /// commits exist. An explicit target must not exceed the bound. These rules apply whether or
    /// not the resulting snapshot is catalog-managed.
    ///
    /// Supply the catalog's bound when loading its latest ratified version, including a downgrade.
    /// An empty catalog tail does not remove this requirement. Kernel rejects a latest version
    /// request without a bound if the resulting snapshot is catalog-managed.
    ///
    /// # Log tail requirements
    ///
    /// With a bound and no explicit target, the last log-tail entry, if any, must match the bound.
    /// With an explicit target, the last entry, if any, must be at least the target version.
    /// See [`with_log_tail`](Self::with_log_tail) for staged-commit input requirements.
    ///
    /// [`at_version`]: Self::at_version
    pub fn with_max_catalog_version(mut self, max_catalog_version: Version) -> Self {
        self.max_catalog_version = Some(max_catalog_version);
        self
    }

    /// Supply a [`CancellationToken`] for snapshot builds that list or read the log.
    ///
    /// Kernel forwards the token (if any) to cancellation-aware [`Engine`] listing and read
    /// operations, including CRC and imperative protocol/metadata replay. Declarative plan
    /// execution does not use this token. The build fails with [`Error::Kernel`] wrapping
    /// [`KernelError::Cancelled`] if cancellation is observed during these reads. Snapshot-hint
    /// builds perform no listing or reads, so the token has no effect on them. By default (or
    /// when passing `None`), the supplied engine's cancellation behavior is unchanged.
    ///
    /// This token applies only to the build and is not retained by the snapshot. Builds run
    /// synchronously and may block on I/O, including when a cancellation token is supplied.
    /// Configure [`Snapshot::reader`] with a token for subsequent metadata reads.
    ///
    /// [`CancellationToken`]: crate::CancellationToken
    /// [`Error::Kernel`]: crate::Error::Kernel
    /// [`KernelError::Cancelled`]: crate::KernelError::Cancelled
    pub fn with_cancellation_token(
        mut self,
        token: impl Into<Option<CancellationTokenRef>>,
    ) -> Self {
        self.cancellation_token = token.into();
        self
    }

    /// Bound how many commits kernel will replay to advance a stale CRC to the target version.
    /// See [`IncrementalReplay`]. Defaults to [`IncrementalReplay::Disabled`].
    ///
    /// Writers should set this to [`IncrementalReplay::Unlimited`] for faster writes, as should
    /// readers that always want table-level file statistics for query optimization.
    ///
    /// Applies to both fresh and incremental builds.
    pub fn with_incremental_crc_replay(mut self, mode: IncrementalReplay) -> Self {
        self.incremental_replay = mode;
        self
    }

    /// Attach an opaque, caller-supplied correlation id for joining this build's metric events to
    /// the caller's own request or operation id. An empty id is treated as unset. When unset,
    /// behavior is unchanged.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.correlation_id = Some(correlation_id.into()).filter(|id| !id.is_empty());
        self
    }

    // ============================================================================
    // Terminal: build the Snapshot
    // ============================================================================

    /// Create a new [`Snapshot`]. This returns a [`SnapshotRef`] (`Arc<Snapshot>`), perhaps
    /// returning a reference to an existing snapshot if the request to build a new snapshot
    /// matches the version of an existing snapshot.
    ///
    /// Reports metrics: [`MetricEvent::SnapshotBuildSuccess`] or
    /// [`MetricEvent::SnapshotBuildFailure`]. Events include `load_type`; hinted builds use
    /// `snapshot_hint` and emit no child log-load events because they perform no engine I/O.
    ///
    /// # Parameters
    ///
    /// - `engine`: Implementation of [`Engine`] apis.
    ///
    /// # Errors
    ///
    /// A [`Snapshot::builder_for`] build without a snapshot hint returns [`Error::Delta`] with the
    /// [`DeltaPathDoesNotExist`] condition when it proves that the table does not exist:
    /// - the table root is a local path that does not exist, or
    /// - neither [`at_version`](Self::at_version) nor
    ///   [`with_max_catalog_version`](Self::with_max_catalog_version) is set and the Delta log has
    ///   no commit, checkpoint, or valid `_last_checkpoint` file.
    ///
    /// The [`KernelError`] that detected the missing table is the error's
    /// [`source`](std::error::Error::source). When a target version is set, a build that finds no
    /// log files returns a Kernel error, such as [`KernelError::EmptyLog`], instead: an existing
    /// table whose earlier history was removed also has no log files up to that version.
    ///
    /// [`DeltaPathDoesNotExist`]: crate::DeltaErrorCondition::DeltaPathDoesNotExist
    /// [`MetricEvent::SnapshotBuildSuccess`]: crate::metrics::MetricEvent::SnapshotBuildSuccess
    /// [`MetricEvent::SnapshotBuildFailure`]: crate::metrics::MetricEvent::SnapshotBuildFailure
    // `is_catalog_managed` is the requested load mode, not the confirmed protocol
    // (see `IS_CATALOG_MANAGED_FIELD`).
    #[instrument(
        name = SNAPSHOT_COMPLETED_SPAN,
        skip_all,
        fields(path = %self.table_path(), report, enable_call_frame, version = tracing::field::Empty, operation_id = %self.operation_id, is_catalog_managed = self.max_catalog_version.is_some(), correlation_id = self.correlation_id.as_deref().unwrap_or(""), load_type = self.load_type().as_ref()),
        err
    )]
    pub fn build(self, engine: &dyn Engine) -> Result<SnapshotRef> {
        // Fold the context into the message string rather than passing structured fields: this
        // `info!` fires inside the `snap.build` metrics span, where any field the
        // `SnapshotBuildSuccess` event doesn't recognize would trip a spurious "Invalid field"
        // warning from the metrics layer.
        info!(
            "building snapshot: target={}, from_version={:?}, log_tail_len={}, \
             max_catalog_version={:?}",
            self.target_version_str(),
            self.existing_snapshot.as_ref().map(|s| s.version()),
            self.log_tail.len(),
            self.max_catalog_version
        );

        let load_type = self.load_type();

        // Destructure self so fields can be moved independently
        let Self {
            schema_recursion_limit,
            table_root,
            existing_snapshot,
            version,
            log_tail,
            max_catalog_version,
            incremental_replay,
            checkpoint_handling,
            snapshot_hint,
            operation_id,
            correlation_id,
            cancellation_token,
            mode: _,
        } = self;

        require!(
            (1..=crate::actions::MAX_SCHEMA_RECURSION_LIMIT).contains(&schema_recursion_limit),
            KernelError::generic(format!(
                "Schema JSON recursion limit must be between 1 and {}",
                crate::actions::MAX_SCHEMA_RECURSION_LIMIT
            ))
            .into()
        );

        let metric_context = SnapshotLoadMetricContext {
            operation_id,
            is_catalog_managed: max_catalog_version.is_some(),
            correlation_id,
            load_type,
        };

        let snapshot = if let Some(snapshot_hint) = snapshot_hint {
            snapshot_hint.build(
                table_root,
                version,
                log_tail,
                max_catalog_version,
                incremental_replay,
                schema_recursion_limit,
            )?
        } else {
            let scoped_engine =
                cancellation_token.map(|token| CancellableEngine::new(engine, token));
            let engine = scoped_engine
                .as_ref()
                .map_or(engine, |engine| engine as &dyn Engine);
            let log_tail: Vec<_> = log_tail.into_iter().map(Into::into).collect();

            // Pre-build validations for catalog-managed tables
            Self::validate_catalog_managed_build_inputs(version, max_catalog_version, &log_tail)?;

            // Use time-travel version if set, otherwise fall back to max_catalog_version. Passing
            // this as the version to LogSegment::for_snapshot does NOT skip the _last_checkpoint
            // hint -- the hint is still used when its version <= effective_version.
            let effective_version = version.or(max_catalog_version);

            // A snapshot is latest when no explicit time-travel version is requested, or when the
            // requested version is exactly the max_catalog_version.
            let built_as_latest = version.is_none() || version == max_catalog_version;

            if let Some(table_root) = table_root {
                let table_url = try_parse_uri(&table_root)
                    .map_err(|error| classify_table_root_error(&table_root, error))?;
                let log_segment = LogSegment::for_snapshot(
                    engine.storage_handler().as_ref(),
                    table_url
                        .join(DELTA_LOG_DIR_WITH_SLASH)
                        .map_err(KernelError::from)?,
                    log_tail,
                    effective_version,
                    metric_context.clone(),
                )
                .map_err(|error| {
                    classify_log_segment_error(&table_root, effective_version, error)
                })?;
                Snapshot::try_new_from_log_segment(
                    table_url,
                    log_segment,
                    engine,
                    metric_context,
                    incremental_replay,
                    built_as_latest,
                    schema_recursion_limit,
                )
                .map(Into::into)?
            } else {
                let Some(existing_snapshot) = existing_snapshot else {
                    return Err(KernelError::internal_error(
                        "SnapshotBuilder should have either table_root or existing_snapshot",
                    )
                    .into());
                };
                Snapshot::try_new_from(
                    existing_snapshot,
                    log_tail,
                    engine,
                    effective_version,
                    metric_context,
                    incremental_replay,
                    checkpoint_handling,
                    built_as_latest,
                )?
            }
        };

        // Post-build validations for catalog-managed tables
        Self::validate_catalog_managed_build_result(&snapshot, version, max_catalog_version)?;
        tracing::Span::current().record("version", snapshot.version());
        Ok(snapshot)
    }

    // ============================================================================
    // Helpers
    // ============================================================================

    // ===== Catalog-managed Validations =====

    /// Pre-build validations for catalog-managed table invariants.
    fn validate_catalog_managed_build_inputs(
        version: Option<Version>,
        max_catalog_version: Option<Version>,
        log_tail: &[crate::path::ParsedLogPath],
    ) -> KernelResult<()> {
        validate_catalog_managed_log_tail(version, max_catalog_version, log_tail)
    }

    /// Require a catalog bound when a latest version request resolves to a catalog-managed
    /// snapshot.
    fn validate_catalog_managed_build_result(
        snapshot: &SnapshotRef,
        requested_version: Option<Version>,
        max_catalog_version: Option<Version>,
    ) -> KernelResult<()> {
        let is_catalog_managed = snapshot.table_configuration().is_catalog_managed();

        require!(
            requested_version.is_some() || !is_catalog_managed || max_catalog_version.is_some(),
            KernelError::MaxCatalogVersion(
                "Max catalog version is required when loading the latest catalog-managed snapshot. \
                 Use with_max_catalog_version()."
                    .to_string()
            )
        );
        // A non-catalog-managed protocol cannot distinguish a downgrade from an ordinary filesystem
        // commit. Trust the caller's catalog bound without reading another protocol.
        // TODO(#3852): Enforce publication before filesystem writes in Transaction::commit.
        Ok(())
    }

    // ===== Instrumentation Helpers =====

    fn table_path(&self) -> &str {
        self.table_root
            .as_deref()
            .or_else(|| {
                self.existing_snapshot
                    .as_ref()
                    .map(|s| s.table_root().as_str())
            })
            .unwrap_or("unknown")
    }

    /// A build from a table root is a fresh, full log listing; a build from an existing snapshot
    /// reuses that snapshot's log root and lists only the commits above it (`table_root` is None).
    fn load_type(&self) -> SnapshotLoadType {
        if self.snapshot_hint.is_some() {
            SnapshotLoadType::SnapshotHint
        } else if self.table_root.is_some() {
            SnapshotLoadType::Full
        } else {
            SnapshotLoadType::Incremental
        }
    }

    fn target_version_str(&self) -> String {
        if let Some(mcv) = self.max_catalog_version {
            return match self.version {
                Some(v) => format!("{v} (max_catalog_version={mcv})"),
                None => format!("{mcv} (max_catalog_version)"),
            };
        }

        self.version
            .map(|v| v.to_string())
            .unwrap_or_else(|| "LATEST".into())
    }
}

/// Classifies a [`try_parse_uri`] failure for the table root of a snapshot build.
///
/// A local root that does not exist becomes `DELTA_PATH_DOES_NOT_EXIST`. Any other failure, such
/// as a root that is not a directory, stays a Kernel error.
fn classify_table_root_error(table_root: &str, error: KernelError) -> Error {
    match error {
        KernelError::InvalidTableLocation(_) if is_absent_local_path(table_root) => {
            DeltaError::path_does_not_exist(table_root, error).into()
        }
        error => error.into(),
    }
}

/// Classifies a [`LogSegment::for_snapshot`] failure for a snapshot build at `target_version`.
///
/// Without a target version, [`KernelError::EmptyLog`] means that listing from version 0 found no
/// commit or checkpoint (a usable `_last_checkpoint` fails with `InvalidCheckpoint` instead), so
/// the table root has no Delta log and the error becomes `DELTA_PATH_DOES_NOT_EXIST`. With a
/// target version, every failure stays a Kernel error; see [`SnapshotBuilder::build`].
fn classify_log_segment_error(
    table_root: &str,
    target_version: Option<Version>,
    error: KernelError,
) -> Error {
    match error {
        KernelError::EmptyLog if target_version.is_none() => {
            DeltaError::path_does_not_exist(table_root, error).into()
        }
        error => error.into(),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::error::Error as _;
    use std::mem::discriminant;
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use serde_json::json;
    use test_utils::{TestAction, actions_to_string, add_commit};
    use url::Url;

    use super::*;
    use crate::DeltaErrorCondition;
    use crate::engine::sync::SyncEngine;
    use crate::metrics::{LogSegmentLoadType, MetricEvent};
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
    use crate::schema::{ArrayType, DataType, MapType, StructField, StructType};
    use crate::unit_test_utils::{CapturingReporter, install_thread_local_metrics_reporter};
    use crate::utils::FoldWithOption as _;

    pub(in crate::snapshot) fn setup_test() -> (Arc<SyncEngine>, Arc<DynObjectStore>, String) {
        let table_root = String::from("memory:///");
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
        (engine, store, table_root)
    }

    /// Returns the snapshot's active file paths in sorted order.
    pub(in crate::snapshot) fn scan_paths(
        snapshot: SnapshotRef,
        engine: &dyn Engine,
    ) -> KernelResult<Vec<String>> {
        let scan = snapshot.scan_builder().build()?;
        let mut paths = Vec::new();
        for batch in scan.scan_metadata(engine)? {
            paths = batch?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
        }
        paths.sort();
        Ok(paths)
    }

    pub(in crate::snapshot) async fn create_table(
        store: &Arc<DynObjectStore>,
        table_root: &str,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        add_commit(
            table_root,
            store.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        add_commit(
            table_root,
            store.as_ref(),
            1,
            format!(
                "{}\n{}",
                json!({"commitInfo": {"operation": "WRITE"}}),
                actions_to_string(vec![TestAction::Add("part-00000-test.parquet".into())])
            ),
        )
        .await?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_builder() -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();
        create_table(&store, &table_root).await?;

        let snapshot = SnapshotBuilder::new_for(table_root.clone()).build(engine)?;
        assert_eq!(snapshot.version(), 1);

        let snapshot = SnapshotBuilder::new_for(table_root.clone())
            .at_version(0)
            .build(engine)?;
        assert_eq!(snapshot.version(), 0);

        Ok(())
    }

    #[rstest::rstest]
    #[case::formerly_rejected(42, "struct")]
    #[case::struct_boundary(331, "struct")]
    #[case::array_boundary(996, "array")]
    #[case::map_boundary(996, "map")]
    #[test_log::test(tokio::test)]
    async fn snapshot_schema_recursion_limit_is_inherited(
        #[case] depth: usize,
        #[case] container: &str,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let data_type = (0..depth).fold(DataType::INTEGER, |nested, level| match container {
            "struct" => {
                StructType::try_new([StructField::nullable(format!("level_{level}"), nested)])
                    .unwrap()
                    .into()
            }
            "array" => ArrayType::new(nested, true).into(),
            "map" => MapType::new(DataType::STRING, nested, true).into(),
            _ => panic!("unknown schema container"),
        });
        let schema = StructType::try_new([StructField::nullable("nested", data_type)])?;
        let mut metadata = json!({"metaData": {
            "id": "nested-schema",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": serde_json::to_string(&schema)?,
            "partitionColumns": [],
            "configuration": {}
        }});
        let protocol = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
        add_commit(
            &table_root,
            store.as_ref(),
            0,
            format!("{protocol}\n{metadata}"),
        )
        .await?;
        assert!(
            Snapshot::builder_for(&table_root)
                .build(engine.as_ref())
                .is_err()
        );
        let initial = Snapshot::builder_for(&table_root)
            .with_schema_recursion_limit(1000)
            .build(engine.as_ref())?;
        assert_eq!(initial.schema().as_ref(), &schema);
        let scan = initial.clone().scan_builder().build()?;
        assert_eq!(scan.logical_schema().as_ref(), &schema);

        metadata["metaData"]["name"] = json!("updated-metadata");
        add_commit(&table_root, store.as_ref(), 1, metadata.to_string()).await?;
        let updated = Snapshot::builder_from(initial).build(engine.as_ref())?;
        assert_eq!(updated.version(), 1);
        assert_eq!(
            updated.table_configuration.metadata().name(),
            Some("updated-metadata")
        );
        assert_eq!(updated.schema().as_ref(), &schema);
        let rebuilt = Snapshot::builder_for(&table_root)
            .with_schema_recursion_limit(1000)
            .with_snapshot_hint(updated.to_snapshot_hint()?)
            .build(engine.as_ref())?;
        assert_eq!(rebuilt.schema().as_ref(), &schema);
        assert_eq!(rebuilt.table_configuration.schema_recursion_limit, 1000);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_with_unsupported_type() -> KernelResult<()> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();

        // Create a table with an unsupported type in the schema
        let protocol = json!({
            "minReaderVersion": 1,
            "minWriterVersion": 2,
        });

        let metadata = json!({
            "id": "test-table-id",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"interval_col\",\"type\":\"interval year to second\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        });

        let commit0 = [
            json!({
                "protocol": protocol
            }),
            json!({
                "metaData": metadata
            }),
        ];

        let commit0_data = commit0
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");

        let path = Path::from("_delta_log/00000000000000000000.json");
        store.put(&path, commit0_data.into()).await?;

        // Try to build a snapshot and expect a clear error message
        let result = SnapshotBuilder::new_for(table_root.clone()).build(engine);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Unsupported Delta table type: 'interval year to second'"),
            "Expected clear error message about unsupported type, got: {err_msg}"
        );

        Ok(())
    }

    pub(in crate::snapshot) fn measuring_reporter()
    -> (Arc<CapturingReporter>, tracing::subscriber::DefaultGuard) {
        let reporter = Arc::new(CapturingReporter::default());
        let guard = install_thread_local_metrics_reporter(reporter.clone());
        (reporter, guard)
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_failed_emits_metric_on_error() -> KernelResult<()> {
        let (engine, store, table_root) = setup_test();

        // Unsupported schema type forces a build failure
        let protocol = json!({"minReaderVersion": 1, "minWriterVersion": 2});
        let metadata = json!({
            "id": "test-table-id",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"interval year to second","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        });
        let commit0_data = [json!({"protocol": protocol}), json!({"metaData": metadata})]
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");
        store
            .put(
                &Path::from("_delta_log/00000000000000000000.json"),
                commit0_data.into(),
            )
            .await?;

        let (reporter, _guard) = measuring_reporter();
        let result = SnapshotBuilder::new_for(table_root).build(engine.as_ref());
        assert!(result.is_err());

        let events = reporter.events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotBuildFailure(_))),
            "expected SnapshotBuildFailure event on build failure"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotBuildSuccess(_))),
            "should not emit SnapshotBuildSuccess on failure"
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn log_segment_load_failure_emits_metric_on_empty_log() -> KernelResult<()> {
        let (engine, _store, table_root) = setup_test();
        let (reporter, _guard) = measuring_reporter();

        let error = SnapshotBuilder::new_for(&table_root)
            .build(engine.as_ref())
            .unwrap_err();
        assert_path_does_not_exist(&error, &table_root, &KernelError::EmptyLog);

        let events = reporter.events();
        let failure = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::LogSegmentLoadFailure(f) => Some(f),
                _ => None,
            })
            .expect("expected LogSegmentLoadFailure when the log has no commits");
        assert_eq!(failure.load_type, LogSegmentLoadType::Full);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotBuildFailure(_))),
            "a Delta error must still report SnapshotBuildFailure"
        );
        Ok(())
    }

    /// Asserts that `error` reports `DELTA_PATH_DOES_NOT_EXIST` for `table_root`, with a Kernel
    /// source of the same variant as `expected_source`.
    fn assert_path_does_not_exist(error: &Error, table_root: &str, expected_source: &KernelError) {
        let Error::Delta(delta) = error else {
            panic!("expected DELTA_PATH_DOES_NOT_EXIST, got {error:?}");
        };
        assert_eq!(
            delta.condition(),
            DeltaErrorCondition::DeltaPathDoesNotExist
        );
        assert_eq!(delta.sql_state(), Some("42K03"));
        let parameters = delta
            .parameters()
            .iter()
            .map(|parameter| (parameter.name(), parameter.value()))
            .collect_vec();
        assert_eq!(parameters, [("path", table_root)]);
        assert_eq!(
            delta.message(),
            format!("{table_root} doesn't exist, or is not a Delta table.")
        );
        let source = delta
            .source()
            .and_then(|source| source.downcast_ref::<KernelError>())
            .expect("the Kernel error that detected the missing table is the source");
        assert_eq!(discriminant(source), discriminant(expected_source));
    }

    fn absent_local_path(dir: &std::path::Path) -> String {
        dir.join("absent").to_str().unwrap().to_string()
    }

    fn absent_file_url(dir: &std::path::Path) -> String {
        Url::from_directory_path(dir.join("absent"))
            .unwrap()
            .to_string()
    }

    fn empty_local_directory(dir: &std::path::Path) -> String {
        dir.to_str().unwrap().to_string()
    }

    // `_last_checkpoint` that fails to parse is ignored, so the build lists the whole log.
    fn unparseable_last_checkpoint(dir: &std::path::Path) -> String {
        let log_dir = dir.join("_delta_log");
        std::fs::create_dir(&log_dir).unwrap();
        std::fs::write(log_dir.join("_last_checkpoint"), "not json").unwrap();
        dir.to_str().unwrap().to_string()
    }

    #[rstest::rstest]
    #[case::absent_local_path(absent_local_path, None, KernelError::invalid_table_location(""))]
    #[case::absent_local_path_at_version(
        absent_local_path,
        Some(0),
        KernelError::invalid_table_location("")
    )]
    #[case::absent_file_url(absent_file_url, None, KernelError::invalid_table_location(""))]
    #[case::empty_local_directory(empty_local_directory, None, KernelError::EmptyLog)]
    #[case::unparseable_last_checkpoint(unparseable_last_checkpoint, None, KernelError::EmptyLog)]
    fn build_reports_missing_table_as_path_does_not_exist(
        #[case] table_root: fn(&std::path::Path) -> String,
        #[case] version: Option<Version>,
        #[case] expected_source: KernelError,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let table_root = table_root(dir.path());
        let error = SnapshotBuilder::new_for(&table_root)
            .fold_with(version, SnapshotBuilder::at_version)
            .build(&SyncEngine::new())
            .unwrap_err();
        assert_path_does_not_exist(&error, &table_root, &expected_source);
    }

    #[rstest::rstest]
    #[case::at_version(Some(0), None)]
    #[case::max_catalog_version(None, Some(0))]
    #[tokio::test]
    async fn build_keeps_empty_log_error_when_target_version_is_set(
        #[case] version: Option<Version>,
        #[case] max_catalog_version: Option<Version>,
    ) {
        let (engine, _store, table_root) = setup_test();
        let error = SnapshotBuilder::new_for(table_root)
            .fold_with(version, SnapshotBuilder::at_version)
            .fold_with(
                max_catalog_version,
                SnapshotBuilder::with_max_catalog_version,
            )
            .build(engine.as_ref())
            .unwrap_err();
        assert!(
            matches!(error, Error::Kernel(KernelError::EmptyLog)),
            "{error:?}"
        );
    }

    // A valid `_last_checkpoint` shows that a Delta log exists even though no checkpoint or
    // commit remains.
    #[tokio::test]
    async fn build_keeps_kernel_error_for_last_checkpoint_without_checkpoint() -> Result<()> {
        let (engine, store, table_root) = setup_test();
        store
            .put(
                &Path::from("_delta_log/_last_checkpoint"),
                r#"{"version":1,"size":1}"#.into(),
            )
            .await
            .map_err(KernelError::from)?;
        let error = SnapshotBuilder::new_for(table_root)
            .build(engine.as_ref())
            .unwrap_err();
        assert!(
            matches!(error, Error::Kernel(KernelError::InvalidCheckpoint(_))),
            "{error:?}"
        );
        Ok(())
    }

    // On Unix, `try_exists` fails for these roots instead of reporting absence: a file root is
    // resolved with a trailing slash (ENOTDIR), and a self-referential symlink loops (ELOOP).
    #[cfg(unix)]
    #[rstest::rstest]
    #[case::regular_file(|dir: &std::path::Path| {
        let file = dir.join("file");
        std::fs::write(&file, "").unwrap();
        file
    })]
    #[case::self_referential_symlink(|dir: &std::path::Path| {
        let link = dir.join("loop");
        std::os::unix::fs::symlink(&link, &link).unwrap();
        link
    })]
    fn build_keeps_kernel_error_for_local_root_that_is_not_proven_absent(
        #[case] create_root: fn(&std::path::Path) -> std::path::PathBuf,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let table_root = create_root(dir.path());
        let error = SnapshotBuilder::new_for(table_root.to_str().unwrap())
            .build(&SyncEngine::new())
            .unwrap_err();
        assert!(
            matches!(error, Error::Kernel(KernelError::InvalidTableLocation(_))),
            "{error:?}"
        );
    }

    #[test_log::test(tokio::test)]
    async fn protocol_metadata_load_failure_emits_metric_when_actions_absent()
    -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        // A commit with no protocol/metadata: the segment lists fine, then the read fails.
        add_commit(
            &table_root,
            store.as_ref(),
            0,
            actions_to_string(vec![TestAction::Add("part-00000-test.parquet".into())]),
        )
        .await?;
        let (reporter, _guard) = measuring_reporter();

        assert!(
            SnapshotBuilder::new_for(table_root)
                .build(engine.as_ref())
                .is_err()
        );

        let events = reporter.events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, MetricEvent::ProtocolMetadataLoadFailure(_))),
            "expected ProtocolMetadataLoadFailure when protocol/metadata are absent"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MetricEvent::ProtocolMetadataLoadSuccess(_))),
            "must not emit ProtocolMetadataLoadSuccess when the load fails"
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_update_from_existing_emits_metric()
    -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;

        // Build v0 snapshot before installing the reporter so only the update is measured
        let snap_v0 = SnapshotBuilder::new_for(table_root)
            .at_version(0)
            .build(engine.as_ref())?;
        assert_eq!(snap_v0.version(), 0);

        let (reporter, _guard) = measuring_reporter();

        let snap_v1 = SnapshotBuilder::new_from(snap_v0).build(engine.as_ref())?;
        assert_eq!(snap_v1.version(), 1);

        let events = reporter.events();
        let (version, duration) = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::SnapshotBuildSuccess(s) => Some((s.version, s.duration)),
                _ => None,
            })
            .expect("expected SnapshotBuildSuccess event");
        assert_eq!(version, 1, "version should match the updated snapshot");
        assert!(duration > Duration::ZERO, "duration should be non-zero");
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_update_to_earlier_version_emits_failed_metric()
    -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;

        // Build v1 snapshot before installing the reporter
        let snap_v1 = SnapshotBuilder::new_for(table_root).build(engine.as_ref())?;
        assert_eq!(snap_v1.version(), 1);

        let (reporter, _guard) = measuring_reporter();

        let result = SnapshotBuilder::new_from(snap_v1)
            .at_version(0)
            .build(engine.as_ref());
        assert!(
            result.is_err(),
            "updating to an earlier version should fail"
        );

        let events = reporter.events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotBuildFailure(_))),
            "expected SnapshotBuildFailure when version update goes backwards"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotBuildSuccess(_))),
            "should not emit SnapshotBuildSuccess when version update fails"
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_completed_duration_exceeds_log_segment_load_duration()
    -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;

        let (reporter, _guard) = measuring_reporter();
        let _snap = SnapshotBuilder::new_for(table_root).build(engine.as_ref())?;

        let events = reporter.events();
        let snap_duration = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::SnapshotBuildSuccess(s) => Some(s.duration),
                _ => None,
            })
            .expect("expected SnapshotBuildSuccess event");
        let segment_duration = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::LogSegmentLoadSuccess(s) => Some(s.duration),
                _ => None,
            })
            .expect("expected LogSegmentLoadSuccess event");

        assert!(
            snap_duration > Duration::ZERO,
            "duration should be non-zero"
        );
        assert!(
            snap_duration >= segment_duration,
            "SnapshotBuildSuccess.duration ({snap_duration:?}) should be >= LogSegmentLoadSuccess.duration ({segment_duration:?})"
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::with_id(Some("req-abc-123"))]
    #[case::without_id(None)]
    #[test_log::test(tokio::test)]
    async fn snapshot_build_and_child_events_carry_correlation_id(
        #[case] correlation_id: Option<&str>,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;

        let (reporter, _guard) = measuring_reporter();
        let _ = SnapshotBuilder::new_for(table_root)
            .fold_with(correlation_id, SnapshotBuilder::with_correlation_id)
            .build(engine.as_ref())?;

        // The build event and its snapshot-load child events must all carry the id, since they
        // ride the same SnapshotLoadMetricContext.
        let events = reporter.events();
        let id_of = |pick: fn(&MetricEvent) -> Option<&Option<Arc<str>>>| {
            events
                .iter()
                .find_map(pick)
                .expect("expected event")
                .as_deref()
                .map(str::to_string)
        };
        let build_id = id_of(|e| match e {
            MetricEvent::SnapshotBuildSuccess(s) => Some(&s.correlation_id),
            _ => None,
        });
        let segment_id = id_of(|e| match e {
            MetricEvent::LogSegmentLoadSuccess(s) => Some(&s.correlation_id),
            _ => None,
        });
        let metadata_id = id_of(|e| match e {
            MetricEvent::ProtocolMetadataLoadSuccess(s) => Some(&s.correlation_id),
            _ => None,
        });
        let expected = correlation_id.map(str::to_string);
        assert_eq!(build_id, expected);
        assert_eq!(segment_id, expected, "log-segment child must carry the id");
        assert_eq!(
            metadata_id, expected,
            "protocol/metadata child must carry the id"
        );
        Ok(())
    }

    pub(in crate::snapshot) mod catalog_managed_tests {
        use rstest::rstest;
        use test_utils::{
            TestAction, actions_to_string, actions_to_string_catalog_managed, add_commit,
            add_staged_commit,
        };

        use super::*;
        use crate::FileMeta;
        use crate::actions::Protocol;
        use crate::log_path::LogPath;
        use crate::metrics::MeteredDeltaEngine;
        use crate::path::ParsedLogPath;
        use crate::snapshot::ChecksumWriteResult;
        use crate::table_features::TableFeature;
        use crate::utils::try_parse_uri;

        pub(in crate::snapshot) fn create_log_path(table_root: &str, commit_path: Path) -> LogPath {
            let table_url = try_parse_uri(table_root).expect("Failed to parse table root");
            let commit_url = table_url.join(commit_path.as_ref()).unwrap();
            let file_meta = FileMeta {
                location: commit_url,
                last_modified: 123,
                size: 100,
            };
            LogPath::try_new(file_meta).expect("Failed to create LogPath")
        }

        /// Creates an in-memory engine, store, and table root with an initial catalog-managed
        /// commit at version 0 (protocol + metadata).
        pub(in crate::snapshot) async fn setup_catalog_managed_test()
        -> (Arc<SyncEngine>, Arc<DynObjectStore>, String) {
            let (engine, store, table_root) = setup_test();
            let actions = vec![TestAction::Metadata];
            add_commit(
                &table_root,
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await
            .expect("Failed to write initial catalog-managed commit");
            (engine, store, table_root)
        }

        #[rstest]
        #[case::catalog_commit(false, false)]
        #[case::unpublished_downgrade(true, false)]
        #[case::published_downgrade(true, true)]
        #[test_log::test(tokio::test)]
        async fn test_staged_commits_without_max_catalog_version_errors(
            #[case] downgrade: bool,
            #[case] published: bool,
            #[values(None, Some(0), Some(1))] requested_version: Option<Version>,
            #[values(false, true)] incremental: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let base = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(0)
                .build(engine.as_ref())?;
            let actions = if downgrade {
                json!({"protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?})
                .to_string()
            } else {
                "{}".into()
            };
            let path1 = add_staged_commit(&table_root, store.as_ref(), 1, actions.clone()).await?;
            if published {
                add_commit(&table_root, store.as_ref(), 1, actions).await?;
            }

            let log_tail = vec![create_log_path(&table_root, path1)];

            let result = if incremental {
                SnapshotBuilder::new_from(base)
                    .with_log_tail(log_tail)
                    .fold_with(requested_version, SnapshotBuilder::at_version)
                    .build(engine.as_ref())
            } else {
                SnapshotBuilder::new_for(table_root)
                    .with_log_tail(log_tail)
                    .fold_with(requested_version, SnapshotBuilder::at_version)
                    .build(engine.as_ref())
            };

            assert!(matches!(
                result,
                Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
            ));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_version_exceeds_max_catalog_version_errors() -> KernelResult<()> {
            let (engine, _store, table_root) = setup_catalog_managed_test().await;

            let result = SnapshotBuilder::new_for(table_root)
                .at_version(5)
                .with_max_catalog_version(3)
                .build(engine.as_ref());

            assert!(matches!(
                result,
                Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
            ));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_log_tail_last_version_mismatch_errors()
        -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 1, actions_to_string(actions)).await?;
            let actions = vec![TestAction::Add("file_2.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 2, actions_to_string(actions)).await?;

            let log_tail = vec![
                create_log_path(&table_root, test_utils::delta_path_for_version(1, "json")),
                create_log_path(&table_root, test_utils::delta_path_for_version(2, "json")),
            ];

            // log_tail ends at v2, max_catalog_version=3, no time-travel -> error
            let result = SnapshotBuilder::new_for(table_root)
                .with_log_tail(log_tail)
                .with_max_catalog_version(3)
                .build(engine.as_ref());

            assert!(matches!(
                result,
                Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
            ));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_catalog_managed_table_without_max_catalog_version_errors() -> KernelResult<()>
        {
            let (engine, _store, table_root) = setup_catalog_managed_test().await;

            let result = SnapshotBuilder::new_for(table_root).build(engine.as_ref());

            assert!(matches!(
                result,
                Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
            ));

            Ok(())
        }

        #[rstest]
        #[case::always_filesystem([false, false, false, false], 3)]
        #[case::always_catalog_managed([true, true, true, true], 3)]
        #[case::upgrade([false, false, true, true], 3)]
        #[case::downgrade([true, true, false, false], 2)]
        #[case::upgrade_then_downgrade([false, true, false, false], 2)]
        #[case::downgrade_then_upgrade([true, false, true, true], 3)]
        #[test_log::test(tokio::test)]
        async fn test_catalog_bound_depends_on_request_not_historical_protocol(
            #[case] catalog_managed_at_version: [bool; 4],
            #[case] catalog_version: Version,
            #[values(None, Some(0), Some(1), Some(2), Some(3))] requested_version: Option<Version>,
            #[values(false, true)] with_catalog_bound: bool,
            #[values(None, Some(0), Some(1))] base_version: Option<Version>,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: published history that can cross catalog-managed boundaries.
            let (engine, store, table_root) = setup_test();
            let engine = Arc::new(MeteredDeltaEngine::new(engine));
            let catalog_protocol = Protocol::try_new_modern(
                [TableFeature::CatalogManaged],
                [
                    TableFeature::CatalogManaged,
                    TableFeature::InCommitTimestamp,
                ],
            )?;
            let filesystem_protocol = Protocol::try_new_modern(
                std::iter::empty::<TableFeature>(),
                [TableFeature::InCommitTimestamp],
            )?;
            let initial_actions = actions_to_string_catalog_managed(vec![TestAction::Metadata])
                .lines()
                .map(|line| -> KernelResult<_, serde_json::Error> {
                    let mut action: serde_json::Value = serde_json::from_str(line)?;
                    if action.get("protocol").is_some() && !catalog_managed_at_version[0] {
                        action["protocol"] = json!(filesystem_protocol);
                    }
                    Ok(action.to_string())
                })
                .collect::<KernelResult<Vec<_>, _>>()?
                .join("\n");
            add_commit(&table_root, store.as_ref(), 0, initial_actions).await?;
            let base = SnapshotBuilder::new_for(&table_root)
                .at_version(0)
                .build(engine.as_ref())?;
            assert_eq!(
                base.table_configuration().is_catalog_managed(),
                catalog_managed_at_version[0]
            );
            for version in 1..=3_usize {
                let mut actions =
                    actions_to_string(vec![TestAction::Add(format!("file_{version}.parquet"))]);
                if catalog_managed_at_version[version] != catalog_managed_at_version[version - 1] {
                    let protocol = if catalog_managed_at_version[version] {
                        &catalog_protocol
                    } else {
                        &filesystem_protocol
                    };
                    actions.push_str(&format!("\n{}", json!({"protocol": protocol})));
                }
                add_commit(&table_root, store.as_ref(), version as Version, actions).await?;
            }
            let base = if let Some(version) = base_version {
                SnapshotBuilder::new_from(base)
                    .at_version(version)
                    .build(engine.as_ref())?
            } else {
                base
            };
            let expected_version = requested_version.unwrap_or(if with_catalog_bound {
                catalog_version
            } else {
                3
            });
            let max_catalog_version = with_catalog_bound.then_some(catalog_version);
            // When: build the requested version from the root or an older snapshot.
            let (reporter, _guard) = measuring_reporter();
            let result = if base_version.is_some() {
                SnapshotBuilder::new_from(base)
                    .fold_with(requested_version, SnapshotBuilder::at_version)
                    .fold_with(
                        max_catalog_version,
                        SnapshotBuilder::with_max_catalog_version,
                    )
                    .build(engine.as_ref())
            } else {
                SnapshotBuilder::new_for(&table_root)
                    .fold_with(requested_version, SnapshotBuilder::at_version)
                    .fold_with(
                        max_catalog_version,
                        SnapshotBuilder::with_max_catalog_version,
                    )
                    .build(engine.as_ref())
            };
            // Then: validate the request, reconstructed state, hint round trip, and replay I/O.
            if (with_catalog_bound && expected_version > catalog_version)
                || (!with_catalog_bound
                    && requested_version.is_none()
                    && catalog_managed_at_version[3])
            {
                assert!(matches!(
                    result,
                    Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
                ));
                return Ok(());
            }
            if base_version.is_some_and(|version| version > expected_version) {
                assert!(
                    matches!(result, Err(Error::Kernel(KernelError::Generic(message)))
                    if message.contains("older than snapshot hint version"))
                );
                return Ok(());
            }
            let snapshot = result?;
            assert_eq!(snapshot.version(), expected_version);
            assert_eq!(
                snapshot.table_configuration().is_catalog_managed(),
                catalog_managed_at_version[expected_version as usize]
            );
            let hinted = SnapshotBuilder::new_for(&table_root)
                .with_snapshot_hint(snapshot.to_snapshot_hint()?)
                .fold_with(requested_version, SnapshotBuilder::at_version)
                .fold_with(
                    max_catalog_version,
                    SnapshotBuilder::with_max_catalog_version,
                )
                .build(engine.as_ref())?;
            assert_eq!(hinted.version(), expected_version);
            assert_eq!(hinted.table_configuration(), snapshot.table_configuration());
            assert_eq!(hinted.log_segment.listed, snapshot.log_segment.listed);
            for count in [
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::StorageListCompleted(_)))
                    .count(),
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::JsonReadCompleted(_)))
                    .count(),
            ] {
                assert_eq!(count, usize::from(base_version != Some(expected_version)));
            }
            assert_eq!(
                scan_paths(snapshot, engine.as_ref())?,
                (1..=expected_version)
                    .map(|version| format!("file_{version}.parquet"))
                    .collect_vec()
            );
            Ok(())
        }

        #[rstest]
        #[case::unpublished_tail(false, true)]
        #[case::published_tail(true, true)]
        #[case::published_without_tail(true, false)]
        #[case::unavailable_downgrade(false, false)]
        #[test_log::test(tokio::test(flavor = "multi_thread"))]
        async fn test_staged_catalog_downgrade_does_not_add_replay_io(
            #[case] published: bool,
            #[case] supply_tail: bool,
            #[values("catalogManaged", "catalogOwned-preview")] catalog_feature: &str,
            #[values(false, true)] incremental: bool,
            #[values(false, true)] checkpointed: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: a ratified downgrade with the selected publication and checkpoint state.
            let (engine, store, table_root) = setup_test();
            let engine = Arc::new(MeteredDeltaEngine::new(engine));
            let initial_actions = actions_to_string_catalog_managed(vec![
                TestAction::Metadata,
                TestAction::Add("retained.parquet".into()),
                TestAction::Add("removed.parquet".into()),
            ])
            .replace("catalogManaged", catalog_feature);
            add_commit(&table_root, store.as_ref(), 0, initial_actions).await?;
            let base = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(0)
                .build(engine.as_ref())?;
            let base = if checkpointed {
                base.checkpoint(engine.as_ref(), None)?.1
            } else {
                base
            };
            let downgrade = json!({
                "protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?
            })
            .to_string();
            let downgrade = format!(
                "{downgrade}\n{}",
                actions_to_string(vec![
                    TestAction::Remove("removed.parquet".into()),
                    TestAction::Add("downgrade.parquet".into()),
                ])
            );
            let staged_downgrade =
                add_staged_commit(&table_root, store.as_ref(), 1, downgrade.clone()).await?;
            let expected_commit = create_log_path(
                &table_root,
                if supply_tail {
                    staged_downgrade
                } else {
                    test_utils::delta_path_for_version(1, "json")
                },
            );
            let log_tail = if supply_tail {
                vec![expected_commit.clone()]
            } else {
                vec![]
            };
            let expected_commit: ParsedLogPath = expected_commit.into();
            if published {
                add_commit(&table_root, store.as_ref(), 1, downgrade).await?;
            }

            // When: load the downgrade through catalog discovery.
            let (reporter, _guard) = measuring_reporter();
            let result = if incremental {
                SnapshotBuilder::new_from(base)
                    .with_log_tail(log_tail)
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())
            } else {
                SnapshotBuilder::new_for(&table_root)
                    .with_log_tail(log_tail)
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())
            };
            // Then: require available history and preserve normal replay I/O and tail precedence.
            if !published && !supply_tail {
                assert!(matches!(
                    result,
                    Err(Error::Kernel(KernelError::MissingVersion(1)))
                ));
                return Ok(());
            }
            let snapshot = result?;
            assert_eq!(snapshot.version(), 1);
            assert!(!snapshot.table_configuration().is_catalog_managed());
            let selected = snapshot
                .log_segment
                .listed
                .ascending_commit_files
                .last()
                .unwrap();
            if supply_tail {
                assert_eq!(selected, &expected_commit);
            } else {
                assert_eq!(selected.version, expected_commit.version);
                assert_eq!(
                    selected.location.location,
                    expected_commit.location.location
                );
                assert_eq!(selected.file_type, expected_commit.file_type);
            }
            assert_eq!(
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::JsonReadCompleted(_)))
                    .count(),
                1
            );
            assert_eq!(
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::ParquetReadCompleted(_)))
                    .count(),
                usize::from(checkpointed && !incremental)
            );
            assert_eq!(
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::StorageListCompleted(_)))
                    .count(),
                1
            );
            let hinted = SnapshotBuilder::new_for(&table_root)
                .with_snapshot_hint(snapshot.to_snapshot_hint()?)
                .build(engine.as_ref())?;
            for snapshot in [snapshot, hinted] {
                assert_eq!(
                    scan_paths(snapshot, engine.as_ref())?,
                    ["downgrade.parquet", "retained.parquet"]
                );
            }
            Ok(())
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        enum DowngradeRefresh {
            Published,
            EarlierCheckpoint,
            TerminalCheckpoint,
            FilesystemSuccessor,
            Upgrade,
        }

        #[rstest]
        #[case::published(DowngradeRefresh::Published)]
        #[case::new_checkpoint_before_downgrade(DowngradeRefresh::EarlierCheckpoint)]
        #[case::new_checkpoint_at_downgrade(DowngradeRefresh::TerminalCheckpoint)]
        #[case::new_filesystem_commit(DowngradeRefresh::FilesystemSuccessor)]
        #[case::new_upgrade_requires_catalog_bound(DowngradeRefresh::Upgrade)]
        #[test_log::test(tokio::test(flavor = "multi_thread"))]
        async fn test_cached_downgrade_refresh_after_publication(
            #[case] refresh: DowngradeRefresh,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: a cached snapshot of a ratified downgrade before publication.
            let downgrade_version = 2;
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let engine = Arc::new(MeteredDeltaEngine::new(engine));
            for version in 1..downgrade_version {
                add_commit(&table_root, store.as_ref(), version, "{}".into()).await?;
            }
            let downgrade = format!(
                "{}\n{}",
                json!({"commitInfo": {"inCommitTimestamp": 1587968586155i64}}),
                json!({"protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?}),
            );
            let staged = add_staged_commit(
                &table_root,
                store.as_ref(),
                downgrade_version,
                downgrade.clone(),
            )
            .await?;
            let source = Snapshot::builder_for(&table_root)
                .with_log_tail(vec![create_log_path(&table_root, staged)])
                .with_max_catalog_version(downgrade_version)
                .build(engine.as_ref())?;
            let source_files = source.log_segment().listed.clone();
            let missing = Snapshot::builder_from(source.clone())
                .at_version(downgrade_version + 1)
                .build(engine.as_ref());
            assert!(matches!(
                missing,
                Err(Error::Kernel(KernelError::MissingVersion(_)))
            ));

            // When: publish the downgrade and discover any later filesystem changes.
            add_commit(&table_root, store.as_ref(), downgrade_version, downgrade).await?;
            match refresh {
                DowngradeRefresh::EarlierCheckpoint | DowngradeRefresh::TerminalCheckpoint => {
                    let version = if refresh == DowngradeRefresh::EarlierCheckpoint {
                        downgrade_version - 1
                    } else {
                        downgrade_version
                    };
                    Snapshot::builder_for(&table_root)
                        .at_version(version)
                        .with_max_catalog_version(version)
                        .build(engine.as_ref())?
                        .checkpoint(engine.as_ref(), None)?;
                }
                DowngradeRefresh::FilesystemSuccessor | DowngradeRefresh::Upgrade => {
                    let actions = if refresh == DowngradeRefresh::Upgrade {
                        json!({"protocol": Protocol::try_new_modern(
                            [TableFeature::CatalogManaged],
                            [TableFeature::CatalogManaged, TableFeature::InCommitTimestamp],
                        )?})
                        .to_string()
                    } else {
                        "{}".into()
                    };
                    add_commit(&table_root, store.as_ref(), downgrade_version + 1, actions).await?;
                }
                DowngradeRefresh::Published => {}
            }

            let (reporter, _guard) = measuring_reporter();
            let result = Snapshot::builder_from(source.clone()).build(engine.as_ref());
            // Then: preserve the source snapshot and require a catalog bound after re-upgrade.
            assert_eq!(source.log_segment().listed, source_files);
            match refresh {
                DowngradeRefresh::Upgrade => {
                    assert!(matches!(
                        result,
                        Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
                    ));
                }
                _ => {
                    let updated = result?;
                    assert_eq!(
                        updated.version(),
                        downgrade_version
                            + u64::from(refresh == DowngradeRefresh::FilesystemSuccessor)
                    );
                    assert!(!updated.table_configuration().is_catalog_managed());
                    if refresh == DowngradeRefresh::Published {
                        assert!(Arc::ptr_eq(&updated, &source));
                        assert!(!reporter.events().iter().any(|event| matches!(
                            event,
                            MetricEvent::StorageReadCompleted(_)
                                | MetricEvent::JsonReadCompleted(_)
                                | MetricEvent::ParquetReadCompleted(_)
                        )));
                    }
                    if matches!(
                        refresh,
                        DowngradeRefresh::EarlierCheckpoint | DowngradeRefresh::TerminalCheckpoint
                    ) {
                        assert_eq!(
                            updated.log_segment().checkpoint_version,
                            Some(
                                downgrade_version
                                    - u64::from(refresh == DowngradeRefresh::EarlierCheckpoint)
                            )
                        );
                    }
                    let rebuilt = Snapshot::builder_for(&table_root)
                        .with_snapshot_hint(updated.to_snapshot_hint()?)
                        .build(engine.as_ref())?;
                    assert_eq!(rebuilt.log_segment().listed, updated.log_segment().listed);
                }
            }
            assert_eq!(
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::StorageListCompleted(_)))
                    .count(),
                1
            );
            Ok(())
        }

        #[rstest]
        #[case::single_retained(2, None)]
        #[case::two_retained(3, None)]
        #[case::published_prefix(3, Some(2))]
        #[test_log::test(tokio::test(flavor = "multi_thread"))]
        async fn test_incremental_catalog_tail_matches_full_replay(
            #[case] source_version: Version,
            #[case] publish_through: Option<Version>,
            #[values(false, true)] explicit_version: bool,
            #[values(false, true)] restore_hint: bool,
            #[values(false, true)] new_checkpoint: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: cached catalog history and a complete ratified tail through the new target.
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            add_commit(
                &table_root,
                store.as_ref(),
                1,
                actions_to_string(vec![TestAction::Add("file_1.parquet".into())]),
            )
            .await?;
            let target_version = source_version + 1;
            let commit_actions = |version| {
                actions_to_string(vec![TestAction::Add(format!("file_{version}.parquet"))])
            };
            let mut tail = Vec::new();
            for version in 2..=target_version {
                let staged = add_staged_commit(
                    &table_root,
                    store.as_ref(),
                    version,
                    commit_actions(version),
                )
                .await?;
                tail.push(create_log_path(&table_root, staged));
            }
            let source = Snapshot::builder_for(&table_root)
                .with_log_tail(tail[..tail.len() - 1].to_vec())
                .with_max_catalog_version(source_version)
                .build(engine.as_ref())?;
            let source = if restore_hint {
                Snapshot::builder_for(&table_root)
                    .at_version(source_version)
                    .with_snapshot_hint(source.to_snapshot_hint()?)
                    .build(engine.as_ref())?
            } else {
                source
            };
            if let Some(publish_through) = publish_through {
                for version in 2..=publish_through {
                    add_commit(
                        &table_root,
                        store.as_ref(),
                        version,
                        commit_actions(version),
                    )
                    .await?;
                }
            }
            if new_checkpoint {
                Snapshot::builder_for(&table_root)
                    .at_version(1)
                    .build(engine.as_ref())?
                    .checkpoint(engine.as_ref(), None)?;
            }
            // When: refresh from the snapshot using the catalog tail, including cached versions.
            let updated = Snapshot::builder_from(source)
                .with_log_tail(tail.clone())
                .with_max_catalog_version(target_version)
                .fold_with(
                    explicit_version.then_some(target_version),
                    SnapshotBuilder::at_version,
                )
                .build(engine.as_ref())?;
            let fresh = Snapshot::builder_for(&table_root)
                .with_log_tail(tail)
                .with_max_catalog_version(target_version)
                .build(engine.as_ref())?;
            // Then: full, incremental, and hinted reads agree on the log and scanned files.
            assert_eq!(updated.version(), target_version);
            assert_eq!(updated.table_configuration(), fresh.table_configuration());
            assert_eq!(updated.log_segment().listed, fresh.log_segment().listed);
            assert_eq!(
                updated.log_segment().listed.max_published_version,
                Some(publish_through.unwrap_or(1))
            );
            let hinted = Snapshot::builder_for(&table_root)
                .at_version(target_version)
                .with_snapshot_hint(updated.to_snapshot_hint()?)
                .build(engine.as_ref())?;
            for snapshot in [updated, fresh, hinted] {
                assert_eq!(
                    scan_paths(snapshot, engine.as_ref())?,
                    (1..=target_version)
                        .map(|version| format!("file_{version}.parquet"))
                        .collect_vec()
                );
            }
            Ok(())
        }

        #[rstest]
        #[case::missing_tail(None, false)]
        #[case::published_tail(Some(false), false)]
        #[case::staged_tail(Some(true), false)]
        #[case::changed_staged_metadata(Some(true), true)]
        #[test_log::test(tokio::test)]
        async fn test_catalog_downgrade_reuses_snapshot_without_terminal_file_equality(
            #[case] supplied_staged: Option<bool>,
            #[case] changed_metadata: bool,
            #[values(false, true)] staged_snapshot: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: a published downgrade cached using either its staged or published path.
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let engine = Arc::new(MeteredDeltaEngine::new(engine));
            let downgrade = json!({
                "protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?
            })
            .to_string();
            let staged =
                add_staged_commit(&table_root, store.as_ref(), 1, downgrade.clone()).await?;
            add_commit(&table_root, store.as_ref(), 1, downgrade.clone()).await?;
            let staged = create_log_path(&table_root, staged);
            let snapshot = if staged_snapshot {
                SnapshotBuilder::new_for(&table_root)
                    .with_log_tail(vec![staged.clone()])
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())
            } else {
                SnapshotBuilder::new_for(&table_root)
                    .at_version(1)
                    .build(engine.as_ref())
            }?;
            let log_tail = match supplied_staged {
                None => vec![],
                Some(false) => vec![create_log_path(
                    &table_root,
                    test_utils::delta_path_for_version(1, "json"),
                )],
                Some(true) if changed_metadata => {
                    let mut parsed: ParsedLogPath = staged.into();
                    parsed.location.last_modified += 1;
                    parsed.location.size += 1;
                    vec![LogPath::try_new(parsed.location)?]
                }
                Some(true) => vec![staged],
            };
            // When: request the same catalog version with independently reported file metadata.
            let (reporter, _guard) = measuring_reporter();
            let result = SnapshotBuilder::new_from(snapshot.clone())
                .with_log_tail(log_tail)
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            // Then: reuse the immutable snapshot without rereading the commit.
            assert!(Arc::ptr_eq(&snapshot, &result));
            assert!(!reporter.events().iter().any(|event| matches!(
                event,
                MetricEvent::StorageListCompleted(_)
                    | MetricEvent::StorageReadCompleted(_)
                    | MetricEvent::JsonReadCompleted(_)
                    | MetricEvent::ParquetReadCompleted(_)
            )));
            Ok(())
        }

        #[rstest]
        #[test_log::test(tokio::test)]
        async fn test_staged_catalog_downgrade_time_travel_respects_catalog_bound(
            #[values(0, 1, 2)] max_catalog_version: Version,
            #[values(false, true)] published: bool,
            #[values(false, true)] incremental: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let base = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(0)
                .build(engine.as_ref())?;
            let downgrade = json!({
                "protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?
            })
            .to_string();
            let staged =
                add_staged_commit(&table_root, store.as_ref(), 1, downgrade.clone()).await?;
            if published {
                add_commit(&table_root, store.as_ref(), 1, downgrade).await?;
            }
            let tail = vec![create_log_path(&table_root, staged)];
            let result = if incremental {
                SnapshotBuilder::new_from(base)
                    .at_version(1)
                    .with_log_tail(tail)
                    .with_max_catalog_version(max_catalog_version)
                    .build(engine.as_ref())
            } else {
                SnapshotBuilder::new_for(&table_root)
                    .at_version(1)
                    .with_log_tail(tail)
                    .with_max_catalog_version(max_catalog_version)
                    .build(engine.as_ref())
            };
            if max_catalog_version >= 1 {
                assert_eq!(result?.version(), 1);
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
                ));
            }
            Ok(())
        }

        #[rstest]
        #[test_log::test(tokio::test)]
        async fn test_catalog_downgrade_stays_bounded_after_publication_without_io_on_reuse(
            #[values(false, true)] with_checksum: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: a cached ratified downgrade whose publication may advance independently.
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let engine = Arc::new(MeteredDeltaEngine::new(engine));
            let downgrade = format!(
                "{}\n{}",
                json!({
                    "commitInfo": {
                        "timestamp": 1587968586155i64,
                        "inCommitTimestamp": 1587968586155i64,
                        "txnId": "test-downgrade",
                    }
                }),
                json!({
                    "protocol": Protocol::try_new_modern(
                        std::iter::empty::<TableFeature>(),
                        [TableFeature::InCommitTimestamp],
                    )?
                }),
            );
            let staged =
                add_staged_commit(&table_root, store.as_ref(), 1, downgrade.clone()).await?;
            let log_tail = vec![create_log_path(&table_root, staged)];
            let snapshot = SnapshotBuilder::new_for(&table_root)
                .with_log_tail(log_tail.clone())
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            let snapshot = if with_checksum {
                let (result, snapshot) = snapshot.write_checksum(engine.as_ref())?;
                assert_eq!(result, ChecksumWriteResult::Written);
                snapshot
            } else {
                snapshot
            };
            let hint = snapshot.to_snapshot_hint()?;
            // When: repeat the catalog-bounded request before and after filesystem advancement.
            for published in [false, true] {
                if published {
                    add_commit(&table_root, store.as_ref(), 1, downgrade.clone()).await?;
                    add_commit(
                        &table_root,
                        store.as_ref(),
                        2,
                        actions_to_string(vec![TestAction::Add("after-downgrade.parquet".into())]),
                    )
                    .await?;
                }
                let (reporter, _guard) = measuring_reporter();
                let reused = SnapshotBuilder::new_from(snapshot.clone())
                    .with_log_tail(log_tail.clone())
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())?;
                // Then: cached reuse and hint reconstruction stay at the bound without I/O.
                assert!(Arc::ptr_eq(&snapshot, &reused));
                assert_eq!(reused.version(), 1);
                let without_tail = SnapshotBuilder::new_from(snapshot.clone())
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())?;
                assert!(Arc::ptr_eq(&snapshot, &without_tail));
                let hinted = SnapshotBuilder::new_for(&table_root)
                    .with_snapshot_hint(hint.clone())
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())?;
                assert_eq!(hinted.version(), 1);
                assert!(!hinted.table_configuration().is_catalog_managed());
                assert_eq!(hinted.log_segment.listed, snapshot.log_segment.listed);
                assert_eq!(hinted.crc_at_version(), snapshot.crc_at_version());
                assert!(!reporter.events().iter().any(|event| matches!(
                    event,
                    MetricEvent::StorageListCompleted(_)
                        | MetricEvent::StorageReadCompleted(_)
                        | MetricEvent::JsonReadCompleted(_)
                        | MetricEvent::ParquetReadCompleted(_)
                )));
                let fresh = SnapshotBuilder::new_for(&table_root)
                    .with_log_tail(log_tail.clone())
                    .with_max_catalog_version(1)
                    .build(engine.as_ref())?;
                assert_eq!(fresh.version(), 1);
                assert!(!fresh.table_configuration().is_catalog_managed());
                assert_eq!(
                    reporter
                        .events()
                        .iter()
                        .filter(|event| matches!(event, MetricEvent::JsonReadCompleted(_)))
                        .count(),
                    usize::from(!with_checksum)
                );
                assert_eq!(
                    reporter
                        .events()
                        .iter()
                        .filter(|event| matches!(event, MetricEvent::StorageListCompleted(_)))
                        .count(),
                    1
                );
            }
            let published_only = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            assert_eq!(published_only.version(), 1);
            assert!(!published_only.table_configuration().is_catalog_managed());
            let hinted = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(1)
                .with_snapshot_hint(published_only.to_snapshot_hint()?)
                .build(engine.as_ref())?;
            assert_eq!(hinted.version(), 1);
            // Without the old bound, discovery can advance to the published filesystem successor.
            let latest = SnapshotBuilder::new_from(snapshot).build(engine.as_ref())?;
            assert_eq!(latest.version(), 2);
            assert!(!latest.table_configuration().is_catalog_managed());
            Ok(())
        }

        #[rstest]
        #[case::catalog_managed("catalogManaged")]
        #[case::catalog_owned("catalogOwned-preview")]
        #[test_log::test(tokio::test)]
        async fn test_catalog_downgrade_and_reupgrade_follow_catalog_bound(
            #[case] catalog_feature: &str,
            #[values(false, true)] incremental: bool,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            // Given: a table that repeatedly downgrades and re-upgrades.
            let (engine, store, table_root) = setup_test();
            add_commit(
                &table_root,
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(vec![TestAction::Metadata])
                    .replace("catalogManaged", catalog_feature),
            )
            .await?;
            let mut snapshot = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(0)
                .build(engine.as_ref())?;
            let downgrade = json!({
                "protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?
            })
            .to_string();
            let upgrade = json!({
                "protocol": Protocol::try_new_modern(
                    [catalog_feature],
                    [catalog_feature, "inCommitTimestamp"],
                )?
            })
            .to_string();

            // When: read each boundary with its catalog bound.
            for downgrade_version in [1, 3] {
                let staged = add_staged_commit(
                    &table_root,
                    store.as_ref(),
                    downgrade_version,
                    downgrade.clone(),
                )
                .await?;
                let log_tail = vec![create_log_path(&table_root, staged)];
                add_commit(
                    &table_root,
                    store.as_ref(),
                    downgrade_version,
                    downgrade.clone(),
                )
                .await?;
                snapshot = if incremental {
                    SnapshotBuilder::new_from(snapshot)
                        .with_log_tail(log_tail.clone())
                        .with_max_catalog_version(downgrade_version)
                        .build(engine.as_ref())
                } else {
                    SnapshotBuilder::new_for(&table_root)
                        .with_log_tail(log_tail.clone())
                        .with_max_catalog_version(downgrade_version)
                        .build(engine.as_ref())
                }?;
                assert_eq!(snapshot.version(), downgrade_version);
                assert!(!snapshot.table_configuration().is_catalog_managed());

                let upgrade_version = downgrade_version + 1;
                add_commit(
                    &table_root,
                    store.as_ref(),
                    upgrade_version,
                    upgrade.clone(),
                )
                .await?;
                let bounded = if incremental {
                    SnapshotBuilder::new_from(snapshot.clone())
                        .with_log_tail(log_tail)
                        .with_max_catalog_version(downgrade_version)
                        .build(engine.as_ref())
                } else {
                    SnapshotBuilder::new_for(&table_root)
                        .with_log_tail(log_tail)
                        .with_max_catalog_version(downgrade_version)
                        .build(engine.as_ref())
                }?;
                assert_eq!(bounded.version(), downgrade_version);
                assert!(!bounded.table_configuration().is_catalog_managed());

                snapshot = if incremental {
                    SnapshotBuilder::new_from(snapshot)
                        .with_max_catalog_version(upgrade_version)
                        .build(engine.as_ref())
                } else {
                    SnapshotBuilder::new_for(&table_root)
                        .with_max_catalog_version(upgrade_version)
                        .build(engine.as_ref())
                }?;
                assert_eq!(snapshot.version(), upgrade_version);
                assert!(snapshot.table_configuration().is_catalog_managed());
                // Then: a latest version request after re-upgrade still requires a bound.
                let without_catalog_bound =
                    SnapshotBuilder::new_from(snapshot.clone()).build(engine.as_ref());
                assert!(matches!(
                    without_catalog_bound,
                    Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
                ));
            }
            Ok(())
        }

        #[rstest]
        #[case::missing_staged_commit(None)]
        #[case::malformed_protocol(Some(
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#
        ))]
        #[test_log::test(tokio::test)]
        async fn test_catalog_downgrade_reuses_loaded_state_but_fresh_replay_propagates_errors(
            #[case] replacement_commit: Option<&str>,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let downgrade = json!({
                "protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?
            })
            .to_string();
            let staged = add_staged_commit(&table_root, store.as_ref(), 1, downgrade).await?;
            let log_tail = vec![create_log_path(&table_root, staged.clone())];
            let snapshot = SnapshotBuilder::new_for(&table_root)
                .with_log_tail(log_tail.clone())
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            if let Some(replacement_commit) = replacement_commit {
                store
                    .put(&staged, replacement_commit.to_string().into())
                    .await?;
            } else {
                store.delete(&staged).await?;
            }
            let reused = SnapshotBuilder::new_from(snapshot.clone())
                .with_log_tail(log_tail.clone())
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            assert!(Arc::ptr_eq(&snapshot, &reused));
            let error = SnapshotBuilder::new_for(&table_root)
                .with_log_tail(log_tail)
                .with_max_catalog_version(1)
                .build(engine.as_ref())
                .unwrap_err();
            if replacement_commit.is_some() {
                assert!(
                    matches!(error, Error::Kernel(KernelError::InvalidProtocol(_))),
                    "{error}"
                );
            } else {
                assert!(
                    matches!(error, Error::Kernel(KernelError::FileNotFound(_))),
                    "{error}"
                );
            }
            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_log_tail_last_version_less_than_time_travel_version_errors()
        -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 1, actions_to_string(actions)).await?;

            let log_tail = vec![create_log_path(
                &table_root,
                test_utils::delta_path_for_version(1, "json"),
            )];

            // Time travel to v2, but log tail only goes up to v1
            let result = SnapshotBuilder::new_for(table_root)
                .at_version(2)
                .with_log_tail(log_tail)
                .with_max_catalog_version(3)
                .build(engine.as_ref());

            assert!(matches!(
                result,
                Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
            ));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_max_catalog_version_as_effective_version()
        -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 1, actions_to_string(actions)).await?;
            let actions = vec![TestAction::Add("file_2.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 2, actions_to_string(actions)).await?;

            // max_catalog_version=1, no time-travel -> snapshot at v1
            let snapshot = SnapshotBuilder::new_for(table_root)
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            assert_eq!(snapshot.version(), 1);

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_time_travel_with_max_catalog_version()
        -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 1, actions_to_string(actions)).await?;

            // at_version(0) + max_catalog_version=1 -> snapshot at v0
            let snapshot = SnapshotBuilder::new_for(table_root)
                .at_version(0)
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            assert_eq!(snapshot.version(), 0);

            Ok(())
        }

        #[rstest]
        #[test_log::test(tokio::test)]
        async fn test_cached_snapshot_validates_request_and_catalog_bound(
            #[values(false, true)] catalog_managed: bool,
            #[values(None, Some(1))] requested_version: Option<Version>,
            #[values(None, Some(0), Some(1))] max_catalog_version: Option<Version>,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_test();
            let engine = MeteredDeltaEngine::new(engine);
            let metadata = if catalog_managed {
                actions_to_string_catalog_managed(vec![TestAction::Metadata])
            } else {
                actions_to_string(vec![TestAction::Metadata])
            };
            add_commit(&table_root, store.as_ref(), 0, metadata).await?;
            add_commit(&table_root, store.as_ref(), 1, "{}".into()).await?;
            let initial = SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(1)
                .build(&engine)?;
            let (reporter, _guard) = measuring_reporter();
            let result = SnapshotBuilder::new_from(initial.clone())
                .fold_with(requested_version, SnapshotBuilder::at_version)
                .fold_with(
                    max_catalog_version,
                    SnapshotBuilder::with_max_catalog_version,
                )
                .build(&engine);
            match (requested_version, max_catalog_version, catalog_managed) {
                (None, Some(0), _) => assert!(
                    matches!(result, Err(Error::Kernel(KernelError::Generic(message)))
                    if message.contains("older than snapshot hint version"))
                ),
                (Some(_), Some(0), _) | (None, None, true) => {
                    assert!(matches!(
                        result,
                        Err(Error::Kernel(KernelError::MaxCatalogVersion(_)))
                    ));
                }
                _ => assert!(Arc::ptr_eq(&initial, &result?)),
            }
            assert_eq!(
                reporter
                    .events()
                    .iter()
                    .filter(|event| matches!(event, MetricEvent::StorageListCompleted(_)))
                    .count(),
                usize::from(requested_version.is_none() && max_catalog_version.is_none())
            );
            assert!(!reporter.events().iter().any(|event| matches!(
                event,
                MetricEvent::StorageReadCompleted(_)
                    | MetricEvent::JsonReadCompleted(_)
                    | MetricEvent::ParquetReadCompleted(_)
            )));
            Ok(())
        }

        #[rstest::rstest]
        #[case::gap(vec![1, 3], vec![1, 3], 3)]
        #[case::duplicates(vec![1], vec![1, 1], 1)]
        #[case::unsorted(vec![1, 2], vec![2, 1], 2)]
        #[case::overflow(vec![], vec![Version::MAX, 0], 0)]
        #[test_log::test(tokio::test)]
        async fn test_non_contiguous_log_tail_errors(
            #[case] commit_versions: Vec<u64>,
            #[case] log_tail_versions: Vec<u64>,
            #[case] mcv: u64,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            for v in &commit_versions {
                let actions = vec![TestAction::Add(format!("file_{v}.parquet"))];
                add_commit(&table_root, store.as_ref(), *v, actions_to_string(actions)).await?;
            }

            let log_tail: Vec<_> = log_tail_versions
                .iter()
                .map(|v| {
                    create_log_path(&table_root, test_utils::delta_path_for_version(*v, "json"))
                })
                .collect();

            let result = SnapshotBuilder::new_for(table_root)
                .with_log_tail(log_tail)
                .with_max_catalog_version(mcv)
                .build(engine.as_ref());

            assert!(matches!(
                result,
                Err(Error::Kernel(
                    KernelError::LogTailVersionsNotContiguous { .. }
                ))
            ));

            Ok(())
        }

        #[rstest::rstest]
        #[case::crc("crc")]
        #[case::classic_checkpoint("checkpoint.parquet")]
        #[case::uuid_checkpoint("checkpoint.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json")]
        #[case::multipart_checkpoint("checkpoint.0000000001.0000000002.parquet")]
        #[case::compaction("00000000000000000002.compacted.json")]
        #[test_log::test(tokio::test)]
        async fn test_non_commit_log_tail_errors(
            #[case] suffix: &str,
        ) -> KernelResult<(), Box<dyn std::error::Error>> {
            let (engine, _store, table_root) = setup_catalog_managed_test().await;
            let log_tail = vec![create_log_path(
                &table_root,
                test_utils::delta_path_for_version(1, suffix),
            )];

            let result = SnapshotBuilder::new_for(table_root)
                .with_log_tail(log_tail)
                .with_max_catalog_version(1)
                .build(engine.as_ref());

            assert!(matches!(
                result,
                Err(Error::Kernel(KernelError::InvalidLogPath(_)))
            ));

            Ok(())
        }
    }
}
