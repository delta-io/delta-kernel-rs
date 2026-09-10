//! Builder for creating [`Snapshot`] instances.

use std::marker::PhantomData;
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use tracing::{info, instrument};

use crate::actions::{Metadata, Protocol};
use crate::cancellation::CancellationTokenRef;
use crate::crc::Crc;
use crate::error::SnapshotHintError;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::metrics::events::SNAPSHOT_COMPLETED_SPAN;
use crate::metrics::{MetricId, SnapshotLoadMetricContext, SnapshotLoadType};
use crate::path::LogPathFileType;
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::utils::{require, try_parse_uri};
use crate::{DeltaResult, Engine, Error, Snapshot, Version};

/// Marker for builders that load a snapshot from a table root.
#[doc(hidden)]
pub struct FromTableRoot;

/// Marker for builders that incrementally update an existing snapshot.
#[doc(hidden)]
pub struct FromSnapshot;

/// The connector-provided freshness of the version carried by a [`SnapshotHint`].
///
/// Kernel trusts this value: [`Latest`](Self::Latest) makes
/// [`Snapshot::is_built_as_latest`] true, while [`Unverified`](Self::Unverified) makes it false.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
#[internal_api]
pub(crate) enum SnapshotHintFreshness {
    /// The connector supplied the version without establishing that it was the latest version.
    Unverified,
    /// The connector established that the supplied version was the latest version.
    Latest,
}

/// Complete state for constructing a [`Snapshot`] without engine I/O.
///
/// Kernel validates the log segment, protocol, metadata, and optional CRC before constructing the
/// snapshot. It records the connector-provided freshness without validating it.
#[derive(Debug, Clone)]
#[internal_api]
pub(crate) struct SnapshotHint {
    /// The target table version to construct.
    ///
    /// Historical commits and checkpoints in [`Self::log_segment_files`] may describe earlier
    /// versions.
    pub version: Version,
    /// The complete set of log files required by the snapshot.
    ///
    /// `latest_crc_file` identifies the latest CRC present in storage and may be older than
    /// `version`.
    pub log_segment_files: LogSegmentFiles,
    /// The table protocol at `version`.
    pub protocol: Protocol,
    /// The table metadata at `version`.
    pub metadata: Metadata,
    /// The optional `_last_checkpoint` contents associated with the log segment.
    pub last_checkpoint_hint: Option<LastCheckpointHint>,
    /// The optional CRC state resolved at `version`.
    ///
    /// This may have been advanced from an older `latest_crc_file`.
    pub crc: Option<Arc<Crc>>,
    /// Whether the connector established that `version` was latest.
    pub freshness: SnapshotHintFreshness,
}

/// Builder for creating [`Snapshot`] instances.
///
/// # Example
///
/// ```no_run
/// # use delta_kernel::{Snapshot, Engine};
/// # use url::Url;
/// # fn example(engine: &dyn Engine) -> delta_kernel::DeltaResult<()> {
/// let table_root = Url::parse("file:///path/to/table")?;
///
/// // Build a snapshot
/// let snapshot = Snapshot::builder_for(table_root.clone())
///     .at_version(5) // Optional: specify a time-travel version (default is latest version)
///     .build(engine)?;
///
/// # Ok(())
/// # }
/// ```
pub struct SnapshotBuilder<Mode = FromTableRoot> {
    table_root: Option<String>,
    existing_snapshot: Option<SnapshotRef>,
    version: Option<Version>,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
    incremental_replay: IncrementalReplay,
    checkpoint_handling: CheckpointHandling,
    snapshot_hint: Option<SnapshotHint>,
    /// Kernel-minted id correlating this build's metric events with its child events.
    operation_id: MetricId,
    /// Opaque, caller-supplied id recorded on this build's metric events. Not interpreted by
    /// kernel; set via [`with_correlation_id`](Self::with_correlation_id).
    correlation_id: Option<Arc<str>>,
    /// Optional cooperative cancellation token supplied via
    /// [`with_cancellation_token`](Self::with_cancellation_token). `None` means the build is not
    /// cancellable.
    cancellation_token: Option<CancellationTokenRef>,
    // Carries the zero-sized typestate that limits mode-specific methods at compile time.
    mode: PhantomData<Mode>,
}

/// Builder for incrementally updating an existing [`Snapshot`].
pub type IncrementalSnapshotBuilder = SnapshotBuilder<FromSnapshot>;

// Hand-written because `CancellationToken` is not `Debug`: the token is projected to a bool. Ends
// with `finish_non_exhaustive` so a future field is not silently dropped from the output.
impl<Mode> std::fmt::Debug for SnapshotBuilder<Mode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotBuilder")
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
    fn is_disabled(self) -> bool {
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
    ) -> DeltaResult<bool> {
        let distance = target_version.checked_sub(crc_version).ok_or_else(|| {
            Error::internal_error(format!(
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
            mode: PhantomData,
        }
    }

    /// Supply complete snapshot state that Kernel can validate and construct without engine I/O.
    ///
    /// The hint conflicts with [`with_log_tail`](Self::with_log_tail) and non-disabled incremental
    /// CRC replay. An explicit [`at_version`](Self::at_version) must equal the hint version. When
    /// no explicit version is set, a supplied maximum catalog version must also equal the hint
    /// version. Kernel validates structural consistency without reading the supplied files. The
    /// caller must ensure every path belongs to this builder's table root, the protocol and
    /// metadata came from those files, and `max_published_version` accurately describes the
    /// published commit prefix.
    ///
    /// # Errors
    ///
    /// [`build`](Self::build) returns [`Error::SnapshotHint`] for hint conflicts and hinted
    /// log-segment validation failures. Catalog-version, table-root URI, protocol, and metadata
    /// failures retain their normal error variants.
    #[allow(dead_code)]
    #[internal_api]
    pub(crate) fn with_snapshot_hint(mut self, hint: SnapshotHint) -> Self {
        self.snapshot_hint = Some(hint);
        self
    }
}

impl SnapshotBuilder<FromSnapshot> {
    pub(crate) fn new_from(existing_snapshot: SnapshotRef) -> Self {
        Self {
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
            mode: PhantomData,
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
    /// Without a snapshot hint, omitting this targets the latest table version. With a hint,
    /// omitting this uses the hint version and its supplied freshness.
    pub fn at_version(mut self, version: Version) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the log tail to use when building the snapshot. This allows catalogs or external
    /// systems to provide an up-to-date log tail when used to build a snapshot.
    ///
    /// Note that the log tail must be a contiguous sequence of commits from M..=N where N is the
    /// target version of the snapshot and 0 <= M <= N.
    ///
    /// See [`with_max_catalog_version`] for additional constraints when loading catalog-managed
    /// tables.
    ///
    /// [`with_max_catalog_version`]: Self::with_max_catalog_version
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }

    /// Set the maximum catalog-ratified version. When set, the snapshot will not load versions
    /// beyond this limit, even if later commits exist on the filesystem. This ensures the catalog
    /// remains the source of truth for catalog-managed tables.
    ///
    /// When no explicit time-travel version is set via [`at_version`], `max_catalog_version` is
    /// used as the effective target version. When time-travelling to an explicit version,
    /// `max_catalog_version` must still be set for catalog-managed tables -- the requested version
    /// must not exceed it.
    ///
    /// # Log tail requirements
    ///
    /// When `max_catalog_version` is set and no time-travel version is specified, the last entry in
    /// the log tail must match `max_catalog_version` exactly. When time-travelling, the last log
    /// tail entry must be >= the requested version.
    ///
    /// [`at_version`]: Self::at_version
    pub fn with_max_catalog_version(mut self, max_catalog_version: Version) -> Self {
        self.max_catalog_version = Some(max_catalog_version);
        self
    }

    /// Supply a [`CancellationToken`] for snapshot builds that list or read the log.
    ///
    /// Kernel polls the token while consuming a log listing. A cancellation-aware [`Engine`]
    /// returns from [`build`](Self::build) when either the token is cancelled or the listing and
    /// log-read work completes, whichever happens first. Snapshot-hint builds perform no listing or
    /// reads, so the token has no effect on them. On cancellation, `build` returns
    /// [`Error::Cancelled`] rather than a snapshot built from a partial listing. With no token the
    /// build is not cancellable.
    ///
    /// [`CancellationToken`]: crate::CancellationToken
    /// [`Error::Cancelled`]: crate::Error::Cancelled
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
    /// [`MetricEvent::SnapshotBuildFailure`].
    ///
    /// # Parameters
    ///
    /// - `engine`: Implementation of [`Engine`] apis.
    ///
    /// [`MetricEvent::SnapshotBuildSuccess`]: crate::metrics::MetricEvent::SnapshotBuildSuccess
    /// [`MetricEvent::SnapshotBuildFailure`]: crate::metrics::MetricEvent::SnapshotBuildFailure
    // `is_catalog_managed` is the requested load mode, not the confirmed protocol
    // (see `IS_CATALOG_MANAGED_FIELD`).
    #[instrument(
        name = SNAPSHOT_COMPLETED_SPAN,
        skip_all,
        fields(path = %self.table_path(), report, version = tracing::field::Empty, operation_id = %self.operation_id, is_catalog_managed = self.max_catalog_version.is_some(), correlation_id = self.correlation_id.as_deref().unwrap_or(""), load_type = self.load_type().as_ref()),
        err
    )]
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<SnapshotRef> {
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

        let metric_context = SnapshotLoadMetricContext {
            operation_id,
            is_catalog_managed: max_catalog_version.is_some(),
            correlation_id,
            load_type,
        };

        let snapshot = if let Some(snapshot_hint) = snapshot_hint {
            Self::build_from_snapshot_hint(
                table_root,
                version,
                log_tail,
                max_catalog_version,
                incremental_replay,
                snapshot_hint,
            )?
        } else {
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
                let table_url = try_parse_uri(table_root)?;
                let log_segment = LogSegment::for_snapshot(
                    engine.storage_handler().as_ref(),
                    table_url.join("_delta_log/")?,
                    log_tail,
                    effective_version,
                    metric_context.clone(),
                    cancellation_token.as_ref(),
                )?;
                Snapshot::try_new_from_log_segment(
                    table_url,
                    log_segment,
                    engine,
                    metric_context,
                    incremental_replay,
                    built_as_latest,
                )
                .map(Into::into)?
            } else {
                let Some(existing_snapshot) = existing_snapshot else {
                    return Err(Error::internal_error(
                        "SnapshotBuilder should have either table_root or existing_snapshot",
                    ));
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
                    cancellation_token.as_ref(),
                )?
            }
        };

        // Post-build validations for catalog-managed tables
        Self::validate_catalog_managed_build_result(&snapshot, max_catalog_version)?;
        tracing::Span::current().record("version", snapshot.version());
        Ok(snapshot)
    }

    // ============================================================================
    // Helpers
    // ============================================================================

    fn build_from_snapshot_hint(
        table_root: Option<String>,
        requested_version: Option<Version>,
        log_tail: Vec<LogPath>,
        max_catalog_version: Option<Version>,
        incremental_replay: IncrementalReplay,
        snapshot_hint: SnapshotHint,
    ) -> DeltaResult<SnapshotRef> {
        require!(log_tail.is_empty(), SnapshotHintError::LogTail.into());
        require!(
            incremental_replay.is_disabled(),
            SnapshotHintError::IncrementalReplay.into()
        );
        if let Some(version) = requested_version {
            require!(
                version == snapshot_hint.version,
                SnapshotHintError::VersionMismatch {
                    requested: version,
                    hint: snapshot_hint.version,
                }
                .into()
            );
        } else if let Some(max_catalog_version) = max_catalog_version {
            require!(
                max_catalog_version == snapshot_hint.version,
                SnapshotHintError::MaxCatalogVersionMismatch {
                    max_catalog_version,
                    hint: snapshot_hint.version,
                }
                .into()
            );
        }

        let table_root = table_root.ok_or_else(|| {
            Error::internal_error("SnapshotBuilder with a snapshot hint must have a table root")
        })?;
        let table_url = try_parse_uri(table_root)?;
        let log_root = table_url.join("_delta_log/")?;
        let SnapshotHint {
            version,
            log_segment_files,
            protocol,
            metadata,
            last_checkpoint_hint,
            crc,
            freshness,
        } = snapshot_hint;
        if freshness == SnapshotHintFreshness::Latest {
            require!(
                max_catalog_version.is_none_or(|max| max <= version),
                SnapshotHintError::LatestVersionConflict {
                    hint: version,
                    max_catalog_version: max_catalog_version.unwrap_or(version),
                }
                .into()
            );
        }
        let has_staged_commits = log_segment_files
            .ascending_commit_files
            .iter()
            .chain(log_segment_files.latest_commit_file.iter())
            .any(|path| path.file_type == LogPathFileType::StagedCommit);
        let latest_commit_version = log_segment_files
            .latest_commit_file
            .as_ref()
            .or_else(|| log_segment_files.ascending_commit_files.last())
            .map(|path| path.version);
        Self::validate_catalog_managed_versions(
            requested_version,
            max_catalog_version,
            has_staged_commits,
            latest_commit_version,
        )?;

        require!(
            log_segment_files.ascending_compaction_files.is_empty(),
            SnapshotHintError::LogCompaction.into()
        );

        // Hinted locations are connector-resolved storage URLs. Kernel cannot determine root
        // membership through lexical URL comparison because equivalent locations may use
        // filesystem aliases or connector-specific URI forms. The connector must ensure that all
        // hinted locations belong to this table. Kernel validates only path self-consistency and
        // log-segment semantics.
        let log_segment = LogSegment::try_new(
            log_segment_files,
            log_root,
            Some(version),
            last_checkpoint_hint,
        )
        .map_err(|source| SnapshotHintError::LogSegment {
            source: Box::new(source),
        })?;
        // Regular construction derives this field from storage listing. A hint supplies it, so
        // only the hint path needs to validate it explicitly.
        require!(
            log_segment
                .listed
                .max_published_version
                .is_none_or(|published_version| published_version <= version),
            SnapshotHintError::MaxPublishedVersion { hint: version }.into()
        );
        require!(
            log_segment.checkpoint_version.is_some()
                || log_segment
                    .listed
                    .ascending_commit_files
                    .first()
                    .is_some_and(|path| path.version == 0),
            SnapshotHintError::MissingHistoryAnchor.into()
        );
        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url, version)?;
        if let Some(crc) = crc.as_ref() {
            require!(
                crc.version == version,
                SnapshotHintError::CrcVersion {
                    crc: crc.version,
                    hint: version,
                }
                .into()
            );
            require!(
                crc.protocol == *table_configuration.protocol(),
                SnapshotHintError::CrcProtocol.into()
            );
            require!(
                crc.metadata == *table_configuration.metadata(),
                SnapshotHintError::CrcMetadata.into()
            );
        }

        Snapshot::new_with_crc(
            log_segment,
            table_configuration,
            crc,
            freshness == SnapshotHintFreshness::Latest,
            false, /* skipped_new_checkpoints */
        )
        .map(Into::into)
    }

    // ===== Catalog-managed Validations =====

    /// Pre-build validations for catalog-managed table invariants.
    fn validate_catalog_managed_build_inputs(
        version: Option<Version>,
        max_catalog_version: Option<Version>,
        log_tail: &[crate::path::ParsedLogPath],
    ) -> DeltaResult<()> {
        // Log tail must be sorted ascending and contiguous (no gaps or duplicates)
        for pair in log_tail.windows(2) {
            require!(
                pair[0].version.checked_add(1) == Some(pair[1].version),
                Error::LogTailVersionsNotContiguous {
                    first_version: pair[0].version,
                    second_version: pair[1].version,
                }
            );
        }

        // TODO: If inline commits (or any other catalog commits) are ever supported, change this
        // method to check if there are any catalog commits.
        let has_catalog_commits = log_tail
            .iter()
            .any(|p| p.file_type == LogPathFileType::StagedCommit);

        Self::validate_catalog_managed_versions(
            version,
            max_catalog_version,
            has_catalog_commits,
            log_tail.last().map(|path| path.version),
        )
    }

    fn validate_catalog_managed_versions(
        version: Option<Version>,
        max_catalog_version: Option<Version>,
        has_staged_commits: bool,
        latest_commit_version: Option<Version>,
    ) -> DeltaResult<()> {
        Self::validate_catalog_version_bounds(version, max_catalog_version)?;
        Self::require_max_catalog_version_for_staged_commits(
            has_staged_commits,
            max_catalog_version,
        )?;

        // Log tail end version validation when max_catalog_version is set
        if let (Some(max_cv), Some(latest_commit_version)) =
            (max_catalog_version, latest_commit_version)
        {
            if let Some(ver) = version {
                // With time-travel: last log_tail entry must be >= requested version
                require!(
                    latest_commit_version >= ver,
                    Error::MaxCatalogVersion(format!(
                        "Log tail version {} is less than requested version {ver} for max catalog \
                         version {max_cv}",
                        latest_commit_version
                    ))
                );
            } else {
                // Without time-travel: last log_tail entry must == max_catalog_version
                require!(
                    latest_commit_version == max_cv,
                    Error::MaxCatalogVersion(format!(
                        "Log tail version {} does not match max catalog version {max_cv}",
                        latest_commit_version
                    ))
                );
            }
        }

        Ok(())
    }

    fn validate_catalog_version_bounds(
        version: Option<Version>,
        max_catalog_version: Option<Version>,
    ) -> DeltaResult<()> {
        if let (Some(version), Some(max_catalog_version)) = (version, max_catalog_version) {
            require!(
                version <= max_catalog_version,
                Error::MaxCatalogVersion(format!(
                    "Requested version {version} exceeds max catalog version {max_catalog_version}"
                ))
            );
        }
        Ok(())
    }

    fn require_max_catalog_version_for_staged_commits(
        has_staged_commits: bool,
        max_catalog_version: Option<Version>,
    ) -> DeltaResult<()> {
        require!(
            !has_staged_commits || max_catalog_version.is_some(),
            Error::MaxCatalogVersion(
                "Max catalog version is required when providing staged commits. \
                 Use with_max_catalog_version()."
                    .to_string()
            )
        );
        Ok(())
    }

    /// Post-build validation: catalog-managed tables must have max_catalog_version, and
    /// non-catalog-managed tables must not.
    fn validate_catalog_managed_build_result(
        snapshot: &SnapshotRef,
        max_catalog_version: Option<Version>,
    ) -> DeltaResult<()> {
        let is_catalog_managed = snapshot.table_configuration().is_catalog_managed();

        require!(
            !is_catalog_managed || max_catalog_version.is_some(),
            Error::MaxCatalogVersion(
                "Max catalog version is required when loading a catalog-managed table. \
                 Use with_max_catalog_version()."
                    .to_string()
            )
        );
        if let Some(max_catalog_version) = max_catalog_version {
            require!(
                is_catalog_managed,
                Error::MaxCatalogVersion(format!(
                    "Max catalog version {max_catalog_version} must not be set for a \
                     non-catalog-managed table"
                ))
            );
        }

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

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use serde_json::json;
    use test_utils::{
        actions_to_string, actions_to_string_catalog_managed, add_commit,
        assert_result_error_with_message, TestAction,
    };

    use super::*;
    use crate::engine::sync::SyncEngine;
    use crate::metrics::{LogSegmentLoadType, MetricEvent};
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
    use crate::schema::schema_ref;
    use crate::unit_test_utils::{
        create_log_path, install_thread_local_metrics_reporter, CapturingReporter,
        TestCancellationToken,
    };
    use crate::utils::FoldWithOption as _;

    fn setup_test() -> (Arc<SyncEngine>, Arc<DynObjectStore>, String) {
        let table_root = String::from("memory:///");
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
        (engine, store, table_root)
    }

    async fn create_table(
        store: &Arc<DynObjectStore>,
        table_root: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

    fn hint_from_snapshot(
        snapshot: &SnapshotRef,
        freshness: SnapshotHintFreshness,
    ) -> SnapshotHint {
        SnapshotHint {
            version: snapshot.version(),
            log_segment_files: snapshot.log_segment().listed.clone(),
            protocol: snapshot.table_configuration().protocol().clone(),
            metadata: snapshot.table_configuration().metadata().clone(),
            last_checkpoint_hint: snapshot.log_segment().last_checkpoint_metadata.clone(),
            crc: snapshot.crc_at_version().cloned(),
            freshness,
        }
    }

    async fn snapshot_and_hint(
        freshness: SnapshotHintFreshness,
    ) -> Result<(Arc<SyncEngine>, String, SnapshotRef, SnapshotHint), Box<dyn std::error::Error>>
    {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;
        let snapshot = SnapshotBuilder::new_for(&table_root).build(engine.as_ref())?;
        let _ = snapshot.write_checksum(engine.as_ref())?;
        let snapshot = SnapshotBuilder::new_for(&table_root).build(engine.as_ref())?;
        let hint = hint_from_snapshot(&snapshot, freshness);
        Ok((engine, table_root, snapshot, hint))
    }

    fn assert_hint_error(
        builder: SnapshotBuilder<FromTableRoot>,
        hint: SnapshotHint,
        engine: &dyn Engine,
        expected: &str,
    ) {
        let error = builder.with_snapshot_hint(hint).build(engine).unwrap_err();
        assert!(matches!(&error, Error::SnapshotHint(_)));
        assert!(
            error.to_string().contains(expected),
            "expected error to contain {expected:?}, got {error}"
        );
    }

    fn assert_log_segment_hint_error(result: DeltaResult<SnapshotRef>, expected_source: &str) {
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid snapshot hint: supplied log files do not form a valid log segment"
        );
        let Error::SnapshotHint(source) = error else {
            panic!("expected SnapshotHint")
        };
        let source = source
            .source()
            .expect("LogSegment error must preserve its source");
        assert!(source.to_string().contains(expected_source));
    }

    #[rstest::rstest]
    #[case::latest(SnapshotHintFreshness::Latest, true)]
    #[case::unverified(SnapshotHintFreshness::Unverified, false)]
    #[test_log::test(tokio::test)]
    async fn complete_snapshot_hint_matches_storage_snapshot_without_child_loads(
        #[case] freshness: SnapshotHintFreshness,
        #[case] expected_built_as_latest: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, storage_snapshot, hint) = snapshot_and_hint(freshness).await?;
        assert!(storage_snapshot.crc_at_version().is_some());

        let token: CancellationTokenRef = Arc::new(TestCancellationToken::cancelled());
        let (reporter, _guard) = measuring_reporter();
        let hinted_snapshot = SnapshotBuilder::new_for(&table_root)
            .with_snapshot_hint(hint)
            .with_correlation_id("hint-request")
            .with_cancellation_token(token)
            .build(engine.as_ref())?;

        assert_eq!(hinted_snapshot.version(), storage_snapshot.version());
        assert_eq!(hinted_snapshot.schema(), storage_snapshot.schema());
        assert_eq!(
            hinted_snapshot.table_configuration().protocol(),
            storage_snapshot.table_configuration().protocol()
        );
        assert_eq!(
            hinted_snapshot.table_configuration().metadata(),
            storage_snapshot.table_configuration().metadata()
        );
        assert_eq!(
            hinted_snapshot.log_segment(),
            storage_snapshot.log_segment()
        );
        assert_eq!(
            hinted_snapshot.crc_at_version(),
            storage_snapshot.crc_at_version()
        );
        assert_eq!(hinted_snapshot.table_root(), storage_snapshot.table_root());
        assert_eq!(
            hinted_snapshot.is_built_as_latest(),
            expected_built_as_latest
        );

        let events = reporter.events();
        assert_eq!(
            events.len(),
            1,
            "hint build must emit only its completion event"
        );
        let MetricEvent::SnapshotBuildSuccess(success) = &events[0] else {
            panic!("expected SnapshotBuildSuccess");
        };
        assert_eq!(success.load_type, SnapshotLoadType::SnapshotHint);
        assert_eq!(success.correlation_id.as_deref(), Some("hint-request"));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn complete_snapshot_hint_without_crc_succeeds() -> Result<(), Box<dyn std::error::Error>>
    {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.crc = None;

        let hinted = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())?;
        assert!(hinted.crc_at_version().is_none());
        Ok(())
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn complete_snapshot_hint_preserves_checkpoint_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;
        let snapshot = SnapshotBuilder::new_for(&table_root).build(engine.as_ref())?;
        let _ = snapshot.checkpoint(engine.as_ref(), None)?;
        let storage_snapshot = SnapshotBuilder::new_for(&table_root).build(engine.as_ref())?;
        assert!(!storage_snapshot
            .log_segment()
            .listed
            .checkpoint_parts
            .is_empty());
        assert!(storage_snapshot
            .log_segment()
            .last_checkpoint_metadata
            .is_some());

        let mut hint = hint_from_snapshot(&storage_snapshot, SnapshotHintFreshness::Unverified);
        hint.log_segment_files.ascending_commit_files.clear();
        hint.log_segment_files.latest_commit_file = None;
        let hinted_snapshot = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())?;
        assert_eq!(
            hinted_snapshot.log_segment().checkpoint_version,
            storage_snapshot.log_segment().checkpoint_version
        );
        assert_eq!(
            hinted_snapshot.log_segment().listed.checkpoint_parts,
            storage_snapshot.log_segment().listed.checkpoint_parts
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::replay_and_latest(true)]
    #[case::latest_only(false)]
    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_staged_commit_without_catalog_version(
        #[case] include_replay_commit: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        let staged = create_log_path(concat!(
            "memory:///_delta_log/_staged_commits/00000000000000000000.",
            "11111111-1111-1111-1111-111111111111.json"
        ));
        hint.version = 0;
        hint.crc = None;
        hint.last_checkpoint_hint = None;
        let (ascending_commit_files, checkpoint_parts) = if include_replay_commit {
            (vec![staged.clone()], vec![])
        } else {
            (
                vec![],
                vec![create_log_path(
                    "memory:///_delta_log/00000000000000000000.checkpoint.parquet",
                )],
            )
        };
        hint.log_segment_files = LogSegmentFiles {
            ascending_commit_files,
            checkpoint_parts,
            latest_commit_file: Some(staged),
            ..Default::default()
        };

        assert_result_error_with_message(
            SnapshotBuilder::new_for(table_root)
                .with_snapshot_hint(hint)
                .build(engine.as_ref()),
            "Max catalog version is required when providing staged commits",
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_published_version_after_hint_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.log_segment_files.max_published_version = Some(Version::MAX);

        let err = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())
            .unwrap_err();
        assert!(matches!(err, Error::SnapshotHint(_)));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_malformed_log_segment_and_incompatible_table_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;

        let mut gapped = hint.clone();
        gapped.log_segment_files.ascending_commit_files[1] =
            create_log_path("memory:///_delta_log/00000000000000000003.json");
        gapped.log_segment_files.latest_commit_file = Some(create_log_path(
            "memory:///_delta_log/00000000000000000003.json",
        ));
        assert_log_segment_hint_error(
            SnapshotBuilder::new_for(&table_root)
                .with_snapshot_hint(gapped)
                .build(engine.as_ref()),
            "Table version 1 is missing",
        );

        let mut inconsistent_path = hint.clone();
        inconsistent_path.log_segment_files.ascending_commit_files[0].version = 1;
        assert_log_segment_hint_error(
            SnapshotBuilder::new_for(&table_root)
                .with_snapshot_hint(inconsistent_path)
                .build(engine.as_ref()),
            "Parsed log path fields do not match its location",
        );

        let mut compacted = hint.clone();
        compacted
            .log_segment_files
            .ascending_compaction_files
            .push(create_log_path(
                "memory:///_delta_log/00000000000000000000.00000000000000000001.compacted.json",
            ));
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root),
            compacted,
            engine.as_ref(),
            "log compaction files are not supported",
        );

        let mut incompatible = hint;
        incompatible.metadata = incompatible
            .metadata
            .with_schema(schema_ref! { nullable "ts": TIMESTAMP_NTZ })?;
        assert_result_error_with_message(
            SnapshotBuilder::new_for(&table_root)
                .with_snapshot_hint(incompatible)
                .build(engine.as_ref()),
            "does not have the required 'timestampNtz' feature",
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_checkpoint_free_history_not_starting_at_version_zero(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.crc = None;
        hint.log_segment_files.ascending_commit_files.remove(0);

        assert_hint_error(
            SnapshotBuilder::new_for(table_root),
            hint,
            engine.as_ref(),
            "snapshot history does not start at version 0",
        );
        Ok(())
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn snapshot_hint_internal_log_invariant_preserves_source(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;
        let snapshot = SnapshotBuilder::new_for(&table_root).build(engine.as_ref())?;
        let _ = snapshot.checkpoint(engine.as_ref(), None)?;
        let snapshot = SnapshotBuilder::new_for(&table_root).build(engine.as_ref())?;
        let mut hint = hint_from_snapshot(&snapshot, SnapshotHintFreshness::Unverified);
        hint.log_segment_files.latest_commit_file = Some(create_log_path(
            "memory:///_delta_log/00000000000000000000.json",
        ));

        let err = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())
            .unwrap_err();
        let Error::SnapshotHint(source) = err else {
            panic!("expected SnapshotHint")
        };
        let source = source
            .source()
            .expect("LogSegment error must preserve its source");
        assert!(source
            .to_string()
            .contains("latest_commit_file version 0 does not match end_version 1"));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_mismatched_crc_state() -> Result<(), Box<dyn std::error::Error>>
    {
        let (engine, table_root, _snapshot, hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        let matching_crc = hint.crc.as_ref().unwrap().as_ref().clone();

        let mut wrong_version = hint.clone();
        wrong_version.crc = Some(Arc::new(Crc {
            version: hint.version - 1,
            ..matching_crc.clone()
        }));
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root),
            wrong_version,
            engine.as_ref(),
            "does not match snapshot hint version",
        );

        let mut wrong_protocol = hint.clone();
        wrong_protocol.crc = Some(Arc::new(Crc {
            protocol: Protocol::try_new_legacy(1, 1)?,
            ..matching_crc.clone()
        }));
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root),
            wrong_protocol,
            engine.as_ref(),
            "CRC protocol does not match",
        );

        let mut wrong_metadata = hint;
        wrong_metadata.crc = Some(Arc::new(Crc {
            metadata: Metadata::default(),
            ..matching_crc
        }));
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root),
            wrong_metadata,
            engine.as_ref(),
            "CRC metadata does not match",
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_conflicting_builder_options_and_reports_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, snapshot, hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        let log_path = LogPath::try_new(
            snapshot
                .log_segment()
                .listed
                .latest_commit_file
                .as_ref()
                .unwrap()
                .location
                .clone(),
        )?;

        assert_hint_error(
            SnapshotBuilder::new_for(&table_root).at_version(hint.version - 1),
            hint.clone(),
            engine.as_ref(),
            "does not match snapshot hint version",
        );
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root).with_log_tail(vec![log_path]),
            hint.clone(),
            engine.as_ref(),
            "cannot be combined with a log tail",
        );
        let zero_budget = SnapshotBuilder::new_for(&table_root)
            .with_incremental_crc_replay(IncrementalReplay::UpToCommits(0))
            .with_snapshot_hint(hint.clone())
            .build(engine.as_ref())?;
        assert_eq!(zero_budget.version(), hint.version);
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root)
                .with_incremental_crc_replay(IncrementalReplay::UpToCommits(1)),
            hint.clone(),
            engine.as_ref(),
            "cannot be combined with incremental CRC replay",
        );
        assert_result_error_with_message(
            SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(hint.version)
                .with_snapshot_hint(hint.clone())
                .build(engine.as_ref()),
            "must not be set for a non-catalog-managed table",
        );

        let (reporter, _guard) = measuring_reporter();
        let result = SnapshotBuilder::new_for(table_root)
            .with_max_catalog_version(hint.version + 1)
            .with_snapshot_hint(hint)
            .build(engine.as_ref());
        assert!(matches!(&result, Err(Error::SnapshotHint(_))));
        assert_result_error_with_message(result, "does not match snapshot hint version");
        let events = reporter.events();
        assert_eq!(events.len(), 1);
        let MetricEvent::SnapshotBuildFailure(failure) = &events[0] else {
            panic!("expected SnapshotBuildFailure");
        };
        assert_eq!(failure.load_type, SnapshotLoadType::SnapshotHint);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_version_must_match_log_segment_end_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.version -= 1;
        hint.crc = None;

        assert_log_segment_hint_error(
            SnapshotBuilder::new_for(table_root)
                .with_snapshot_hint(hint)
                .build(engine.as_ref()),
            "newer than requested end version",
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn time_travel_snapshot_hint_accepts_later_max_catalog_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        add_commit(
            &table_root,
            store.as_ref(),
            0,
            actions_to_string_catalog_managed(vec![TestAction::Metadata]),
        )
        .await?;
        add_commit(
            &table_root,
            store.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("part-00000-test.parquet".into())]),
        )
        .await?;
        let snapshot = SnapshotBuilder::new_for(&table_root)
            .with_max_catalog_version(1)
            .build(engine.as_ref())?;
        let hint = hint_from_snapshot(&snapshot, SnapshotHintFreshness::Unverified);

        assert_result_error_with_message(
            SnapshotBuilder::new_for(&table_root)
                .with_snapshot_hint(hint.clone())
                .build(engine.as_ref()),
            "Max catalog version is required",
        );
        assert_hint_error(
            SnapshotBuilder::new_for(&table_root).with_max_catalog_version(2),
            hint.clone(),
            engine.as_ref(),
            "Max catalog version 2 does not match snapshot hint version 1",
        );
        let latest = SnapshotBuilder::new_for(&table_root)
            .with_max_catalog_version(1)
            .with_snapshot_hint(hint.clone())
            .build(engine.as_ref())?;
        assert_eq!(latest.version(), 1);
        assert!(!latest.is_built_as_latest());

        let mut lower_bound_hint = hint.clone();
        lower_bound_hint.freshness = SnapshotHintFreshness::Latest;
        let result = SnapshotBuilder::new_for(&table_root)
            .at_version(1)
            .with_max_catalog_version(0)
            .with_snapshot_hint(lower_bound_hint)
            .build(engine.as_ref());
        assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

        let hinted = SnapshotBuilder::new_for(&table_root)
            .at_version(1)
            .with_max_catalog_version(2)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())?;
        assert_eq!(hinted.version(), 1);
        assert!(!hinted.is_built_as_latest());

        let mut contradictory_hint = hint_from_snapshot(&snapshot, SnapshotHintFreshness::Latest);
        contradictory_hint.crc = None;
        assert_hint_error(
            SnapshotBuilder::new_for("memory:///")
                .at_version(1)
                .with_max_catalog_version(2),
            contradictory_hint,
            engine.as_ref(),
            "marked latest but max catalog version is 2",
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
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

    #[test_log::test(tokio::test)]
    async fn test_snapshot_with_unsupported_type() -> Result<(), Box<dyn std::error::Error>> {
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

    fn measuring_reporter() -> (Arc<CapturingReporter>, tracing::subscriber::DefaultGuard) {
        let reporter = Arc::new(CapturingReporter::default());
        let guard = install_thread_local_metrics_reporter(reporter.clone());
        (reporter, guard)
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_failed_emits_metric_on_error() -> Result<(), Box<dyn std::error::Error>> {
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
    async fn log_segment_load_failure_emits_metric_on_empty_log(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _store, table_root) = setup_test();
        let (reporter, _guard) = measuring_reporter();

        assert!(SnapshotBuilder::new_for(table_root)
            .build(engine.as_ref())
            .is_err());

        let events = reporter.events();
        let failure = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::LogSegmentLoadFailure(f) => Some(f),
                _ => None,
            })
            .expect("expected LogSegmentLoadFailure when the log has no commits");
        assert_eq!(failure.load_type, LogSegmentLoadType::Full);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn protocol_metadata_load_failure_emits_metric_when_actions_absent(
    ) -> Result<(), Box<dyn std::error::Error>> {
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

        assert!(SnapshotBuilder::new_for(table_root)
            .build(engine.as_ref())
            .is_err());

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
    async fn snapshot_update_from_existing_emits_metric() -> Result<(), Box<dyn std::error::Error>>
    {
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
    async fn snapshot_update_to_earlier_version_emits_failed_metric(
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    async fn snapshot_completed_duration_exceeds_log_segment_load_duration(
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<(), Box<dyn std::error::Error>> {
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

    mod catalog_managed_tests {
        use test_utils::{
            actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit,
            TestAction,
        };

        use super::*;
        use crate::log_path::LogPath;
        use crate::utils::try_parse_uri;
        use crate::FileMeta;

        fn create_log_path(table_root: &str, commit_path: Path) -> LogPath {
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
        async fn setup_catalog_managed_test() -> (Arc<SyncEngine>, Arc<DynObjectStore>, String) {
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

        #[test_log::test(tokio::test)]
        async fn test_staged_commits_without_max_catalog_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let path1 =
                add_staged_commit(&table_root, store.as_ref(), 1, String::from("{}")).await?;

            let log_tail = vec![create_log_path(&table_root, path1)];

            let result = SnapshotBuilder::new_for(table_root)
                .with_log_tail(log_tail)
                .build(engine.as_ref());

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn snapshot_hint_accepts_staged_commit_with_catalog_version(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let actions = actions_to_string(vec![TestAction::Add("file_1.parquet".to_string())]);
            let staged_path = add_staged_commit(&table_root, store.as_ref(), 1, actions).await?;
            let source = SnapshotBuilder::new_for(&table_root)
                .with_log_tail(vec![create_log_path(&table_root, staged_path)])
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            let hint = hint_from_snapshot(&source, SnapshotHintFreshness::Latest);

            let hinted = SnapshotBuilder::new_for(table_root)
                .with_max_catalog_version(1)
                .with_snapshot_hint(hint)
                .build(engine.as_ref())?;

            assert_eq!(hinted.version(), 1);
            assert!(hinted.is_built_as_latest());
            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_version_exceeds_max_catalog_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, _store, table_root) = setup_catalog_managed_test().await;

            let result = SnapshotBuilder::new_for(table_root)
                .at_version(5)
                .with_max_catalog_version(3)
                .build(engine.as_ref());

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_log_tail_last_version_mismatch_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
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

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_catalog_managed_table_without_max_catalog_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, _store, table_root) = setup_catalog_managed_test().await;

            let result = SnapshotBuilder::new_for(table_root).build(engine.as_ref());

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_non_catalog_managed_table_with_max_catalog_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_test();

            let actions = vec![TestAction::Metadata];
            add_commit(&table_root, store.as_ref(), 0, actions_to_string(actions)).await?;

            let result = SnapshotBuilder::new_for(table_root)
                .with_max_catalog_version(0)
                .build(engine.as_ref());

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_log_tail_last_version_less_than_time_travel_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
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

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_max_catalog_version_as_effective_version(
        ) -> Result<(), Box<dyn std::error::Error>> {
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
        async fn test_time_travel_with_max_catalog_version(
        ) -> Result<(), Box<dyn std::error::Error>> {
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

        #[test_log::test(tokio::test)]
        async fn test_builder_from_catalog_managed_without_mcv_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_catalog_managed_test().await;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(&table_root, store.as_ref(), 1, actions_to_string(actions)).await?;

            let initial = SnapshotBuilder::new_for(table_root)
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;

            // Incremental update without mcv should fail
            let result = SnapshotBuilder::new_from(initial).build(engine.as_ref());

            assert!(matches!(result, Err(Error::MaxCatalogVersion(_))));

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
        ) -> Result<(), Box<dyn std::error::Error>> {
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
                Err(Error::LogTailVersionsNotContiguous { .. })
            ));

            Ok(())
        }
    }
}
