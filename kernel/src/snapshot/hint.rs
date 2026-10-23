//! Connector-provided state for constructing snapshots without engine I/O.

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::actions::{Metadata, Protocol};
use crate::crc::Crc;
use crate::error::SnapshotHintError;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_path::LogPath;
use crate::log_segment::{validate_catalog_managed_versions, LogSegment};
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::{LogPathFileType, ParsedLogPath, DELTA_LOG_DIR_WITH_SLASH};
use crate::snapshot::{IncrementalReplay, SnapshotRef};
use crate::table_configuration::TableConfiguration;
use crate::utils::{require, try_parse_uri};
use crate::{KernelError, KernelResult, Snapshot, Version};

/// The freshness claim for the version carried by a [`SnapshotHint`].
///
/// Kernel trusts this value: [`Latest`](Self::Latest) makes
/// [`Snapshot::is_built_as_latest`] true, while [`Unverified`](Self::Unverified) makes it false.
/// Connector inputs supply this claim; exports preserve the snapshot's build-time claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
#[internal_api]
pub(crate) enum SnapshotHintFreshness {
    /// The hinted version was not established as latest.
    Unverified,
    /// The hinted version was established as latest when the claim was made.
    Latest,
}

/// Publication state supplied when constructing a [`SnapshotHint`].
///
/// Publication is separate from ratification: staged commits can belong to a snapshot without
/// having been published. Neither the snapshot version nor its freshness establishes publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
#[internal_api]
pub(crate) enum PublicationWatermark {
    /// Infer the highest published commit version from the supplied log paths.
    ///
    /// Staged paths do not count. If no published commit path is supplied, the watermark is
    /// absent.
    InferFromLogPaths,
    /// Record that no published commits were observed, without inferring from paths.
    NoPublishedCommits,
    /// Preserve the highest observed published version independently of the supplied paths.
    ///
    /// Retained paths may omit published commits or still name their staged locations.
    PublishedThrough(Version),
}

/// Complete state for constructing a [`Snapshot`] without engine I/O.
///
/// Kernel validates the log-segment shape, table configuration, and optional CRC consistency
/// before constructing the snapshot. It does not replay the log to verify the supplied state's
/// provenance, and records the connector-provided freshness without validating it.
/// The caller must ensure that retained staged commits were ratified. Their publication state does
/// not require a new catalog bound. Without an explicit target, catalog-managed snapshot state
/// still requires a bound; explicit targets must respect any supplied bound.
#[derive(Debug, Clone)]
#[internal_api]
pub(crate) struct SnapshotHint {
    /// The target table version to construct.
    ///
    /// Historical commits and checkpoints in [`Self::log_segment_files`] may describe earlier
    /// versions.
    version: Version,
    /// The complete set of log files required by the snapshot.
    ///
    /// `latest_crc_file` identifies the latest CRC present in storage and may be older than
    /// `version`.
    log_segment_files: LogSegmentFiles,
    /// The table protocol at `version`.
    protocol: Protocol,
    /// The table metadata at `version`.
    metadata: Metadata,
    /// The optional `_last_checkpoint` contents associated with the log segment.
    last_checkpoint_hint: Option<LastCheckpointHint>,
    /// The optional CRC state resolved at `version`.
    ///
    /// This may have been advanced from an older `latest_crc_file`.
    crc: Option<Arc<Crc>>,
    /// Whether `version` was established as latest when the hint was captured.
    freshness: SnapshotHintFreshness,
}

impl SnapshotHint {
    /// Creates a hint for `table_root` at `version` from connector-provided log paths and table
    /// state.
    ///
    /// Every path is validated beneath the table's `_delta_log` root before sorting and grouping
    /// using the same checkpoint-selection logic as storage listing. The connector must
    /// canonicalize every path into the same URL form as `table_root`; Kernel preserves the
    /// supplied locations. Snapshot construction performs the remaining consistency and
    /// table-configuration validation.
    ///
    /// Local table roots, including `file:` URLs, are resolved and canonicalized through the
    /// local filesystem and must refer to an existing directory.
    ///
    /// `publication_watermark` selects path-based inference or an explicit observation that may
    /// no longer be represented by a retained path.
    ///
    /// # Errors
    ///
    /// Returns an error if `table_root` is invalid,
    /// [`SnapshotHintError::LogPathOutsideRoot`] if any path is outside `table_root`'s `_delta_log`
    /// root, or
    /// [`SnapshotHintError::LogCompaction`] if any path is a compacted commit. Returns an error
    /// when the supplied paths cannot be grouped into a valid log-segment file set.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        table_root: impl AsRef<str>,
        version: Version,
        publication_watermark: PublicationWatermark,
        log_paths: Vec<LogPath>,
        protocol: Protocol,
        metadata: Metadata,
        last_checkpoint_hint: Option<LastCheckpointHint>,
        crc: Option<Arc<Crc>>,
        freshness: SnapshotHintFreshness,
    ) -> KernelResult<Self> {
        let log_root = try_parse_uri(table_root)?.join(DELTA_LOG_DIR_WITH_SLASH)?;
        let mut parsed_paths: Vec<ParsedLogPath> = log_paths.into_iter().map(Into::into).collect();
        require!(
            !parsed_paths
                .iter()
                .any(|path| matches!(path.file_type, LogPathFileType::CompactedCommit { .. })),
            SnapshotHintError::LogCompaction.into()
        );
        validate_snapshot_hint_paths(parsed_paths.iter(), &log_root)?;
        parsed_paths
            .sort_unstable_by(|a, b| (a.version, &a.filename).cmp(&(b.version, &b.filename)));
        let parsed_paths = parsed_paths.into_iter().map(Ok);
        let mut log_segment_files = LogSegmentFiles::build_log_segment_files(
            parsed_paths,
            Vec::new(),
            0,
            None,
            CheckpointHandling::Adopt,
        )?;
        match publication_watermark {
            PublicationWatermark::InferFromLogPaths => {}
            PublicationWatermark::NoPublishedCommits => {
                log_segment_files.max_published_version = None;
            }
            PublicationWatermark::PublishedThrough(version) => {
                log_segment_files.max_published_version = Some(version);
            }
        }
        Ok(Self {
            version,
            log_segment_files,
            protocol,
            metadata,
            last_checkpoint_hint,
            crc,
            freshness,
        })
    }

    /// Returns the version described by this hint.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn version(&self) -> Version {
        self.version
    }

    /// Returns the retained log files, including the observed publication watermark.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn log_segment_files(&self) -> &LogSegmentFiles {
        &self.log_segment_files
    }

    /// Returns the resolved publication observation, never a request for inference.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn publication_watermark(&self) -> PublicationWatermark {
        match self.log_segment_files.max_published_version {
            Some(version) => PublicationWatermark::PublishedThrough(version),
            None => PublicationWatermark::NoPublishedCommits,
        }
    }

    /// Returns the protocol at the hinted version.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    /// Returns the metadata at the hinted version.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Returns the retained checkpoint hint, if present.
    ///
    /// Hints exported by [`Snapshot::to_snapshot_hint`] retain only a matching checkpoint hint.
    /// Connector-provided inputs are checked when the snapshot is built.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn last_checkpoint_hint(&self) -> Option<&LastCheckpointHint> {
        self.last_checkpoint_hint.as_ref()
    }

    /// Returns the retained CRC state, if present.
    ///
    /// Hints exported by [`Snapshot::to_snapshot_hint`] retain only CRC state at the snapshot
    /// version. Connector-provided inputs are checked when the snapshot is built.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn crc(&self) -> Option<&Crc> {
        self.crc.as_deref()
    }

    /// Returns the freshness established when the hint was captured.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    pub(crate) fn freshness(&self) -> SnapshotHintFreshness {
        self.freshness
    }

    pub(crate) fn from_snapshot(snapshot: &Snapshot) -> KernelResult<Self> {
        let segment = snapshot.log_segment();
        require!(
            segment.listed.ascending_compaction_files.is_empty(),
            SnapshotHintError::LogCompaction.into()
        );
        Ok(Self {
            version: snapshot.version(),
            log_segment_files: segment.listed.clone(),
            protocol: snapshot.table_configuration().protocol().clone(),
            metadata: snapshot.table_configuration().metadata().clone(),
            last_checkpoint_hint: segment.checkpoint_hint().cloned(),
            crc: snapshot.crc_at_version().cloned(),
            freshness: if snapshot.is_built_as_latest() {
                SnapshotHintFreshness::Latest
            } else {
                SnapshotHintFreshness::Unverified
            },
        })
    }

    /// Constructs a snapshot from this hint and the supplied builder settings.
    ///
    /// Returns an error for conflicting settings, invalid table roots, or inconsistent hint state.
    pub(super) fn build(
        self: Box<Self>,
        table_root: Option<String>,
        requested_version: Option<Version>,
        log_tail: Vec<LogPath>,
        max_catalog_version: Option<Version>,
        incremental_replay: IncrementalReplay,
    ) -> KernelResult<SnapshotRef> {
        require!(log_tail.is_empty(), SnapshotHintError::LogTail.into());
        require!(
            incremental_replay.is_disabled(),
            SnapshotHintError::IncrementalReplay.into()
        );
        if let Some(version) = requested_version {
            require!(
                version == self.version,
                SnapshotHintError::VersionMismatch {
                    requested: version,
                    hint: self.version,
                }
                .into()
            );
        } else if let Some(max_catalog_version) = max_catalog_version {
            require!(
                max_catalog_version == self.version,
                SnapshotHintError::MaxCatalogVersionMismatch {
                    max_catalog_version,
                    hint: self.version,
                }
                .into()
            );
        }

        let table_root = table_root.ok_or_else(|| {
            KernelError::internal_error(
                "SnapshotBuilder with a snapshot hint must have a table root",
            )
        })?;
        let table_url = try_parse_uri(table_root)?;
        let log_root = table_url.join(DELTA_LOG_DIR_WITH_SLASH)?;
        let SnapshotHint {
            version,
            log_segment_files,
            protocol,
            metadata,
            last_checkpoint_hint,
            crc,
            freshness,
        } = *self;
        if requested_version.is_none() && freshness == SnapshotHintFreshness::Latest {
            require!(
                max_catalog_version.is_none_or(|max| max <= version),
                SnapshotHintError::LatestVersionConflict {
                    hint: version,
                    max_catalog_version: max_catalog_version.unwrap_or(version),
                }
                .into()
            );
        }
        let latest_commit_version = log_segment_files
            .latest_commit_file
            .as_ref()
            .or_else(|| log_segment_files.ascending_commit_files.last())
            .map(|path| path.version);
        validate_catalog_managed_versions(
            requested_version,
            max_catalog_version,
            false, // Complete hints already attest ratification of their retained staged commits.
            latest_commit_version,
        )?;

        require!(
            log_segment_files.ascending_compaction_files.is_empty(),
            SnapshotHintError::LogCompaction.into()
        );

        // Construction validates every path against one root, and grouping preserves locations.
        // One retained path suffices to reject a hint attached to a different table's builder.
        // LogSegment::try_new below rejects empty replay history.
        validate_snapshot_hint_paths(log_segment_files.iter_all_paths().take(1), &log_root)?;
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
}

/// Validates that hinted log locations are beneath the given log root.
fn validate_snapshot_hint_paths<'a>(
    mut paths: impl Iterator<Item = &'a ParsedLogPath>,
    log_root: &url::Url,
) -> KernelResult<()> {
    let log_root = log_root.as_str();
    if let Some(path) = paths.find(|path| !path.location.location.as_str().starts_with(log_root)) {
        return Err(SnapshotHintError::LogPathOutsideRoot {
            path: path.location.location.to_string(),
            log_root: log_root.to_string(),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::sync::Arc;

    use itertools::Itertools;
    use test_utils::{
        actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit,
        assert_result_error_with_message, TestAction,
    };

    use super::*;
    use crate::cancellation::CancellationTokenRef;
    use crate::engine::sync::SyncEngine;
    use crate::metrics::{MeteredDeltaEngine, MetricEvent, SnapshotLoadType};
    use crate::schema::schema_ref;
    use crate::snapshot::builder::tests::catalog_managed_tests::{
        create_log_path as create_catalog_log_path, setup_catalog_managed_test,
    };
    use crate::snapshot::builder::tests::{create_table, measuring_reporter, setup_test};
    use crate::snapshot::{FromTableRoot, SnapshotBuilder};
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        create_log_path, create_log_path_with_size, TestCancellationToken,
    };
    use crate::Engine;

    fn hint_from_snapshot(
        snapshot: &SnapshotRef,
        freshness: SnapshotHintFreshness,
    ) -> SnapshotHint {
        let mut hint = snapshot.to_snapshot_hint().unwrap();
        hint.freshness = freshness;
        hint
    }

    async fn snapshot_and_hint(
        freshness: SnapshotHintFreshness,
    ) -> KernelResult<
        (Arc<SyncEngine>, String, SnapshotRef, SnapshotHint),
        Box<dyn std::error::Error>,
    > {
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
        assert!(matches!(&error, KernelError::SnapshotHint(_)));
        assert!(
            error.to_string().contains(expected),
            "expected error to contain {expected:?}, got {error}"
        );
    }

    fn assert_log_segment_hint_error(result: KernelResult<SnapshotRef>, expected_source: &str) {
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid snapshot hint: supplied log files do not form a valid log segment"
        );
        let KernelError::SnapshotHint(source) = error else {
            panic!("expected SnapshotHint")
        };
        let source = source
            .source()
            .expect("LogSegment error must preserve its source");
        assert!(source.to_string().contains(expected_source));
    }

    #[rstest::rstest]
    #[case::latest(None, SnapshotHintFreshness::Latest)]
    #[case::historical(Some(0), SnapshotHintFreshness::Unverified)]
    #[test_log::test(tokio::test)]
    async fn snapshot_to_hint_preserves_build_time_freshness(
        #[case] version: Option<Version>,
        #[case] expected: SnapshotHintFreshness,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;
        let mut builder = Snapshot::builder_for(&table_root);
        if let Some(version) = version {
            builder = builder.at_version(version);
        }
        let snapshot = builder.build(engine.as_ref())?;
        let hint = snapshot.to_snapshot_hint()?;
        assert_eq!(hint.freshness, expected);
        assert_eq!(hint.log_segment_files, snapshot.log_segment().listed);
        assert!(hint.crc.is_none());
        Ok(())
    }

    #[rstest::rstest]
    #[case::matching(1, true)]
    #[case::stale(0, false)]
    #[test_log::test(tokio::test)]
    async fn snapshot_to_hint_retains_only_matching_checkpoint_hint(
        #[case] hint_version: Version,
        #[case] expected_present: bool,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, table_root, snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.log_segment_files.checkpoint_parts = vec![create_log_path(&format!(
            "{table_root}_delta_log/00000000000000000001.checkpoint.parquet"
        ))];
        hint.last_checkpoint_hint = Some(LastCheckpointHint {
            version: hint_version,
            ..Default::default()
        });
        let snapshot = Snapshot::new_with_crc(
            LogSegment::try_new(
                hint.log_segment_files,
                snapshot.log_segment().log_root.clone(),
                Some(snapshot.version()),
                hint.last_checkpoint_hint,
            )?,
            snapshot.table_configuration().clone(),
            snapshot.crc_at_version().cloned(),
            false,
            false,
        )?;
        let exported = snapshot.to_snapshot_hint()?;
        assert_eq!(exported.last_checkpoint_hint.is_some(), expected_present);
        assert_eq!(exported.crc, snapshot.crc_at_version().cloned());
        let rebuilt = Snapshot::builder_for(table_root)
            .with_snapshot_hint(exported)
            .build(engine.as_ref())?;
        assert_eq!(rebuilt.version(), snapshot.version());
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_to_hint_rejects_log_compaction(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (_engine, table_root, snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.log_segment_files
            .ascending_compaction_files
            .push(create_log_path(&format!(
                "{table_root}_delta_log/00000000000000000000.\
                 00000000000000000001.compacted.json"
            )));
        let snapshot = Snapshot::new_with_crc(
            LogSegment::try_new(
                hint.log_segment_files,
                snapshot.log_segment().log_root.clone(),
                Some(snapshot.version()),
                None,
            )?,
            snapshot.table_configuration().clone(),
            None,
            false,
            false,
        )?;
        assert!(matches!(
            snapshot.to_snapshot_hint(),
            Err(KernelError::SnapshotHint(source))
                if matches!(*source, SnapshotHintError::LogCompaction)
        ));
        Ok(())
    }

    #[rstest::rstest]
    #[case::infer_published(PublicationWatermark::InferFromLogPaths, false, Some(1))]
    #[case::infer_staged_only(PublicationWatermark::InferFromLogPaths, true, None)]
    #[case::explicit_absence(PublicationWatermark::NoPublishedCommits, false, None)]
    #[case::explicit_version(PublicationWatermark::PublishedThrough(0), true, Some(0))]
    fn snapshot_hint_resolves_publication_watermark(
        #[case] watermark: PublicationWatermark,
        #[case] staged: bool,
        #[case] expected: Option<Version>,
    ) {
        let path = if staged {
            "memory:///target/_delta_log/_staged_commits/\
             00000000000000000001.3a0d65cd-4a56-49a8-937b-95f9e3ee90e5.json"
        } else {
            "memory:///target/_delta_log/00000000000000000001.json"
        };
        let hint = SnapshotHint::try_new(
            "memory:///target/",
            1,
            watermark,
            vec![LogPath::try_new(create_log_path(path).location).unwrap()],
            Protocol::default(),
            Metadata::default(),
            None,
            None,
            SnapshotHintFreshness::Unverified,
        )
        .unwrap();

        assert_eq!(hint.log_segment_files().max_published_version, expected);
        assert_eq!(
            hint.publication_watermark(),
            expected.map_or(
                PublicationWatermark::NoPublishedCommits,
                PublicationWatermark::PublishedThrough,
            )
        );
    }

    #[rstest::rstest]
    fn snapshot_hint_sorts_caller_supplied_log_paths(
        #[values("memory:///target/", "memory:///target")] table_root: &str,
    ) {
        let log_paths = [
            "memory:///target/_delta_log/00000000000000000001.json",
            "memory:///target/_delta_log/00000000000000000000.json",
        ]
        .into_iter()
        .map(|path| LogPath::try_new(create_log_path(path).location))
        .collect::<KernelResult<Vec<_>>>()
        .unwrap();
        let hint = SnapshotHint::try_new(
            table_root,
            1,
            PublicationWatermark::InferFromLogPaths,
            log_paths,
            Protocol::default(),
            Metadata::default(),
            None,
            None,
            SnapshotHintFreshness::Unverified,
        )
        .unwrap();

        assert_eq!(
            hint.log_segment_files
                .ascending_commit_files
                .iter()
                .map(|path| path.version)
                .collect_vec(),
            vec![0, 1]
        );
    }

    #[rstest::rstest]
    #[case::invalid_local_root("file:///invalid%00table/", "Invalid table location")]
    #[case::non_hierarchical_root(
        "mailto:table@example.com",
        "relative URL with a cannot-be-a-base base"
    )]
    fn snapshot_hint_rejects_invalid_table_roots(
        #[case] table_root: &str,
        #[case] expected_error: &str,
    ) {
        let result = SnapshotHint::try_new(
            table_root,
            0,
            PublicationWatermark::InferFromLogPaths,
            Vec::new(),
            Protocol::default(),
            Metadata::default(),
            None,
            None,
            SnapshotHintFreshness::Unverified,
        );
        assert_result_error_with_message(result, expected_error);
    }

    #[rstest::rstest]
    #[case::published_commit("00000000000000000008.json", Some(8))]
    #[case::staged_commit(
        "_staged_commits/00000000000000000008.11111111-1111-1111-1111-111111111111.json",
        None
    )]
    #[case::checkpoint("00000000000000000008.checkpoint.parquet", None)]
    #[case::incomplete_checkpoint(
        "00000000000000000008.checkpoint.0000000001.0000000002.parquet",
        None
    )]
    #[case::crc("00000000000000000008.crc", None)]
    fn snapshot_hint_validates_paths_discarded_by_checkpoint_selection(
        #[case] filename: &str,
        #[case] expected_published_version: Option<Version>,
        #[values(false, true)] foreign_table: bool,
        #[values(false, true)] reverse_paths: bool,
    ) -> KernelResult<()> {
        const TABLE_ROOT: &str = "memory:///target/";
        const LOG_ROOT: &str = "memory:///target/_delta_log/";

        let supplied_root = if foreign_table {
            "memory:///other/_delta_log/"
        } else {
            LOG_ROOT
        };
        let supplied = format!("{supplied_root}{filename}");
        let mut log_paths = [
            supplied.as_str(),
            "memory:///target/_delta_log/00000000000000000010.checkpoint.parquet",
        ]
        .into_iter()
        .map(|path| LogPath::try_new(create_log_path_with_size(path, 1).location))
        .collect::<KernelResult<Vec<_>>>()?;
        if reverse_paths {
            log_paths.reverse();
        }
        let (engine, _store, _table_root) = setup_test();
        let result = SnapshotHint::try_new(
            TABLE_ROOT,
            10,
            PublicationWatermark::InferFromLogPaths,
            log_paths,
            Protocol::try_new_legacy(1, 2)?,
            Metadata::default().with_schema(schema_ref! { nullable "id": INTEGER })?,
            None,
            None,
            SnapshotHintFreshness::Unverified,
        )
        .and_then(|hint| {
            SnapshotBuilder::new_for(TABLE_ROOT)
                .with_snapshot_hint(hint)
                .build(engine.as_ref())
        });

        if foreign_table {
            assert!(matches!(
                result.unwrap_err(),
                KernelError::SnapshotHint(source)
                    if matches!(
                        &*source,
                        SnapshotHintError::LogPathOutsideRoot { path, log_root }
                            if path == &supplied && log_root == LOG_ROOT
                    )
            ));
        } else {
            let snapshot = result?;
            let segment = snapshot.log_segment();
            assert_eq!(segment.checkpoint_version, Some(10));
            assert!(segment.listed.ascending_commit_files.is_empty());
            assert!(segment.listed.latest_commit_file.is_none());
            assert!(segment.listed.latest_crc_file.is_none());
            assert_eq!(
                segment.listed.max_published_version,
                expected_published_version
            );
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::commit_only(0, &["00000000000000000000.json"])]
    #[case::checkpoint_only(10, &["00000000000000000010.checkpoint.parquet"])]
    #[case::checkpoint_and_commit(
        11,
        &[
            "00000000000000000010.checkpoint.parquet",
            "00000000000000000011.json",
        ]
    )]
    fn snapshot_hint_build_validates_the_hint_table_root(
        #[case] version: Version,
        #[case] filenames: &[&str],
        #[values("memory:///target/", "memory:///other/")] builder_table_root: &str,
    ) {
        const TABLE_ROOT: &str = "memory:///target/";
        let log_paths = filenames
            .iter()
            .map(|filename| {
                let supplied = format!("{TABLE_ROOT}_delta_log/{filename}");
                LogPath::try_new(create_log_path_with_size(&supplied, 1).location).unwrap()
            })
            .collect_vec();
        let hint = SnapshotHint::try_new(
            TABLE_ROOT,
            version,
            PublicationWatermark::InferFromLogPaths,
            log_paths,
            Protocol::try_new_legacy(1, 2).unwrap(),
            Metadata::default()
                .with_schema(schema_ref! { nullable "id": INTEGER })
                .unwrap(),
            None,
            None,
            SnapshotHintFreshness::Unverified,
        )
        .unwrap();
        let first_retained_path = hint
            .log_segment_files
            .iter_all_paths()
            .next()
            .unwrap()
            .location
            .location
            .to_string();
        let (engine, _store, _table_root) = setup_test();
        let result = SnapshotBuilder::new_for(builder_table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref());
        if builder_table_root == TABLE_ROOT {
            let snapshot = result.unwrap();
            assert_eq!(snapshot.version(), version);
            assert_eq!(snapshot.table_root().as_str(), TABLE_ROOT);
        } else {
            assert!(matches!(
                result.unwrap_err(),
                KernelError::SnapshotHint(source)
                    if matches!(
                        &*source,
                        SnapshotHintError::LogPathOutsideRoot { path, log_root }
                            if path == &first_retained_path
                                && log_root == "memory:///other/_delta_log/"
                    )
            ));
        }
    }

    #[rstest::rstest]
    fn snapshot_hint_build_rejects_empty_replay_history(
        #[values("memory:///target/", "memory:///other/")] builder_table_root: &str,
    ) {
        let hint = SnapshotHint::try_new(
            "memory:///target/",
            0,
            PublicationWatermark::InferFromLogPaths,
            Vec::new(),
            Protocol::default(),
            Metadata::default(),
            None,
            None,
            SnapshotHintFreshness::Unverified,
        )
        .unwrap();
        let (engine, _store, _table_root) = setup_test();
        assert_log_segment_hint_error(
            SnapshotBuilder::new_for(builder_table_root)
                .with_snapshot_hint(hint)
                .build(engine.as_ref()),
            "No table version found",
        );
    }

    #[derive(Clone, Copy)]
    enum SnapshotHintPathField {
        AscendingCommit,
        AscendingCompaction,
        CheckpointPart,
        LatestCrc,
        LatestCommit,
    }

    #[rstest::rstest]
    #[case::commit(
        SnapshotHintPathField::AscendingCommit,
        "memory:///target/_delta_log/00000000000000000001.json",
        true
    )]
    #[case::checkpoint(
        SnapshotHintPathField::CheckpointPart,
        "memory:///target/_delta_log/00000000000000000001.checkpoint.parquet",
        true
    )]
    #[case::crc(
        SnapshotHintPathField::LatestCrc,
        "memory:///target/_delta_log/00000000000000000001.crc",
        true
    )]
    #[case::staged_commit(
        SnapshotHintPathField::LatestCommit,
        concat!(
            "memory:///target/_delta_log/_staged_commits/",
            "00000000000000000001.11111111-1111-1111-1111-111111111111.json"
        ),
        true
    )]
    #[case::different_table(
        SnapshotHintPathField::AscendingCommit,
        "memory:///other/_delta_log/00000000000000000001.json",
        false
    )]
    #[case::different_scheme(
        SnapshotHintPathField::AscendingCommit,
        "s3a://target/_delta_log/00000000000000000001.json",
        false
    )]
    #[case::root_prefix_collision(
        SnapshotHintPathField::AscendingCommit,
        "memory:///target/_delta_log_suffix/00000000000000000001.json",
        false
    )]
    #[case::compaction_outside_root(
        SnapshotHintPathField::AscendingCompaction,
        concat!(
            "memory:///other/_delta_log/",
            "00000000000000000001.00000000000000000002.compacted.json"
        ),
        false
    )]
    #[case::checkpoint_outside_root(
        SnapshotHintPathField::CheckpointPart,
        "memory:///other/_delta_log/00000000000000000001.checkpoint.parquet",
        false
    )]
    #[case::crc_outside_root(
        SnapshotHintPathField::LatestCrc,
        "memory:///other/_delta_log/00000000000000000001.crc",
        false
    )]
    #[case::latest_commit_outside_root(
        SnapshotHintPathField::LatestCommit,
        "memory:///other/_delta_log/00000000000000000001.json",
        false
    )]
    fn snapshot_hint_paths_must_be_beneath_the_builder_log_root(
        #[case] field: SnapshotHintPathField,
        #[case] supplied: &str,
        #[case] expected_valid: bool,
    ) {
        const LOG_ROOT: &str = "memory:///target/_delta_log/";

        let path = create_log_path(supplied);
        let mut files = LogSegmentFiles::default();
        match field {
            SnapshotHintPathField::AscendingCommit => files.ascending_commit_files.push(path),
            SnapshotHintPathField::AscendingCompaction => {
                files.ascending_compaction_files.push(path)
            }
            SnapshotHintPathField::CheckpointPart => files.checkpoint_parts.push(path),
            SnapshotHintPathField::LatestCrc => files.latest_crc_file = Some(path),
            SnapshotHintPathField::LatestCommit => files.latest_commit_file = Some(path),
        }

        let result = validate_snapshot_hint_paths(
            files.iter_all_paths(),
            &url::Url::parse(LOG_ROOT).unwrap(),
        );
        if expected_valid {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert!(matches!(
                error,
                KernelError::SnapshotHint(source)
                    if matches!(
                        &*source,
                        SnapshotHintError::LogPathOutsideRoot { path, log_root }
                            if path == supplied && log_root == LOG_ROOT
                    )
            ));
        }
        assert_eq!(
            files
                .iter_all_paths()
                .next()
                .unwrap()
                .location
                .location
                .as_str(),
            supplied
        );
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_build_rejects_a_log_path_outside_the_table_log_root(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        let path = hint
            .log_segment_files
            .ascending_commit_files
            .first_mut()
            .unwrap();
        let supplied = format!("memory:///other/_delta_log/{}", path.filename);
        path.location.location = url::Url::parse(&supplied)?;
        let expected_log_root = format!("{table_root}_delta_log/");

        let result = SnapshotBuilder::new_for(&table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref());

        let error = result.unwrap_err();
        assert!(matches!(
            error,
            KernelError::SnapshotHint(source)
                if matches!(
                    &*source,
                    SnapshotHintError::LogPathOutsideRoot { path, log_root }
                        if path == &supplied && log_root == &expected_log_root
                )
        ));
        Ok(())
    }

    #[rstest::rstest]
    #[case::latest(SnapshotHintFreshness::Latest, true)]
    #[case::unverified(SnapshotHintFreshness::Unverified, false)]
    #[test_log::test(tokio::test)]
    async fn complete_snapshot_hint_matches_storage_snapshot_without_child_loads(
        #[case] freshness: SnapshotHintFreshness,
        #[case] expected_built_as_latest: bool,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
    async fn complete_snapshot_hint_without_crc_succeeds(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.crc = None;

        let hinted = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())?;
        assert!(hinted.crc_at_version().is_none());
        Ok(())
    }

    #[rstest::rstest]
    #[case::supported(
        Protocol::try_new_modern(["deletionVectors"], ["deletionVectors"]).unwrap(),
        None,
    )]
    #[case::future_reader_version(
        Protocol::try_new_legacy(4, 2).unwrap(),
        Some("Unsupported minimum reader version 4"),
    )]
    #[case::unknown_reader_feature(
        Protocol::try_new_modern(["futureFeature"], ["futureFeature"]).unwrap(),
        Some("Feature 'futureFeature' is not supported"),
    )]
    #[case::missing_feature_requirement(
        Protocol::try_new_modern(["catalogManaged"], ["catalogManaged"]).unwrap(),
        Some("Feature 'catalogManaged' requires 'inCommitTimestamp' to be enabled"),
    )]
    #[test_log::test(tokio::test)]
    async fn snapshot_hint_validates_reader_protocol(
        #[case] protocol: Protocol,
        #[case] expected_error: Option<&str>,
        #[values(false, true)] with_crc: bool,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, table_root, snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.protocol = protocol.clone();
        if with_crc {
            Arc::make_mut(hint.crc.as_mut().unwrap()).protocol = protocol;
        } else {
            hint.crc = None;
        }

        let result = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref());
        if let Some(expected_error) = expected_error {
            assert_result_error_with_message(result, expected_error);
        } else {
            assert_eq!(result?.version(), snapshot.version());
        }
        Ok(())
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn complete_snapshot_hint_preserves_checkpoint_state(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
    async fn snapshot_hint_trusts_retained_staged_ratification_without_catalog_version(
        #[case] include_replay_commit: bool,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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

        let snapshot = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint.clone())
            .build(engine.as_ref())?;
        assert_eq!(snapshot.version(), 0);
        assert_eq!(snapshot.log_segment().listed, hint.log_segment_files);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_published_version_after_hint_version(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, table_root, _snapshot, mut hint) =
            snapshot_and_hint(SnapshotHintFreshness::Unverified).await?;
        hint.log_segment_files.max_published_version = Some(Version::MAX);

        let err = SnapshotBuilder::new_for(table_root)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())
            .unwrap_err();
        assert!(matches!(err, KernelError::SnapshotHint(_)));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_hint_rejects_malformed_log_segment_and_incompatible_table_state(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
        let KernelError::SnapshotHint(source) = err else {
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
    async fn snapshot_hint_rejects_mismatched_crc_state(
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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

        let mut wrong_metadata = hint.clone();
        wrong_metadata.crc = Some(Arc::new(Crc {
            metadata: Metadata::default(),
            ..matching_crc.clone()
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
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
        let bounded = SnapshotBuilder::new_for(&table_root)
            .with_max_catalog_version(hint.version)
            .with_snapshot_hint(hint.clone())
            .build(engine.as_ref())?;
        assert_eq!(bounded.version(), hint.version);

        let (reporter, _guard) = measuring_reporter();
        let result = SnapshotBuilder::new_for(table_root)
            .with_max_catalog_version(hint.version + 1)
            .with_snapshot_hint(hint)
            .build(engine.as_ref());
        assert!(matches!(&result, Err(KernelError::SnapshotHint(_))));
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
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
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
        assert!(matches!(result, Err(KernelError::MaxCatalogVersion(_))));

        let hinted = SnapshotBuilder::new_for(&table_root)
            .at_version(1)
            .with_max_catalog_version(2)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())?;
        assert_eq!(hinted.version(), 1);
        assert!(!hinted.is_built_as_latest());

        let originally_latest = SnapshotBuilder::new_for(&table_root)
            .at_version(1)
            .with_max_catalog_version(2)
            .with_snapshot_hint(snapshot.to_snapshot_hint()?)
            .build(engine.as_ref())?;
        assert_eq!(originally_latest.version(), 1);
        assert!(originally_latest.is_built_as_latest());
        Ok(())
    }

    #[rstest::rstest]
    #[case::before_downgrade(5)]
    #[case::at_downgrade(4)]
    #[case::after_downgrade(3)]
    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn snapshot_hint_round_trip_after_incremental_downgrade(
        #[case] downgrade_version: Version,
        #[values(None, Some(2), Some(5))] checkpoint_version: Option<Version>,
        #[values(false, true)] skip_new_checkpoints: bool,
        #[values(IncrementalReplay::Disabled, IncrementalReplay::Unlimited)]
        replay: IncrementalReplay,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_catalog_managed_test().await;
        let engine = Arc::new(MeteredDeltaEngine::new(engine));
        let downgrade_protocol = Protocol::try_new_modern(
            std::iter::empty::<TableFeature>(),
            [TableFeature::InCommitTimestamp],
        )?;
        let commit_actions = |version: Version| {
            let timestamp = 1_587_968_586_154_u64 + version;
            let mut actions = vec![
                serde_json::json!({"commitInfo": {
                    "timestamp": timestamp,
                    "inCommitTimestamp": timestamp,
                    "txnId": format!("test-txn-{version}"),
                    "operation": "WRITE",
                    "operationParameters": {"mode": "Append"},
                    "isBlindAppend": true,
                }})
                .to_string(),
                actions_to_string(vec![TestAction::Add(format!("file_{version}.parquet"))]),
            ];
            if version == downgrade_version {
                actions.push(serde_json::json!({"protocol": downgrade_protocol}).to_string());
            }
            actions.join("\n")
        };
        let staged_version = downgrade_version.min(4);
        for version in 1..staged_version {
            add_commit(
                &table_root,
                store.as_ref(),
                version,
                commit_actions(version),
            )
            .await?;
        }
        if checkpoint_version == Some(2) {
            Snapshot::builder_for(&table_root)
                .at_version(2)
                .with_max_catalog_version(2)
                .build(engine.as_ref())?
                .checkpoint(engine.as_ref(), None)?;
        }
        let staged_path = add_staged_commit(
            &table_root,
            store.as_ref(),
            staged_version,
            commit_actions(staged_version),
        )
        .await?;
        let staged = create_catalog_log_path(&table_root, staged_path);
        let mut cached = Snapshot::builder_for(&table_root)
            .with_log_tail(vec![staged])
            .with_max_catalog_version(staged_version)
            .build(engine.as_ref())?;
        for version in staged_version..=4 {
            add_commit(
                &table_root,
                store.as_ref(),
                version,
                commit_actions(version),
            )
            .await?;
        }
        if cached.version() < 4 {
            cached = Snapshot::builder_from(cached)
                .at_version(4)
                .build(engine.as_ref())?;
        }
        let cached = cached.write_checksum(engine.as_ref())?.1;
        assert_eq!(cached.version(), 4);
        assert_eq!(
            cached.table_configuration().is_catalog_managed(),
            downgrade_version > 4
        );
        let cached_files = cached.log_segment().listed.clone();
        assert!(cached_files.ascending_commit_files.iter().any(|path| {
            path.version == staged_version && path.file_type == LogPathFileType::StagedCommit
        }));

        for version in 5..=6 {
            add_commit(
                &table_root,
                store.as_ref(),
                version,
                commit_actions(version),
            )
            .await?;
        }
        if checkpoint_version == Some(5) {
            Snapshot::builder_for(&table_root)
                .at_version(5)
                .build(engine.as_ref())?
                .checkpoint(engine.as_ref(), None)?;
        }
        let builder = Snapshot::builder_from(cached.clone())
            .at_version(6)
            .with_incremental_crc_replay(replay);
        let builder = if skip_new_checkpoints {
            builder.skip_new_checkpoints()
        } else {
            builder
        };
        let updated = builder.build(engine.as_ref())?;
        assert_eq!(updated.version(), 6);
        assert!(!updated.table_configuration().is_catalog_managed());
        assert_eq!(cached.log_segment().listed, cached_files);
        let retains_staged = checkpoint_version != Some(5) || skip_new_checkpoints;
        if retains_staged {
            assert_eq!(
                updated.crc_at_version().is_some(),
                replay == IncrementalReplay::Unlimited
            );
        }
        assert_eq!(
            updated
                .log_segment()
                .listed
                .ascending_commit_files
                .iter()
                .any(|path| {
                    path.version == staged_version
                        && path.file_type == LogPathFileType::StagedCommit
                }),
            retains_staged,
        );

        let hinted = {
            let (reporter, _guard) = measuring_reporter();
            let hint = updated.to_snapshot_hint()?;
            assert_eq!(
                hint.publication_watermark(),
                PublicationWatermark::PublishedThrough(6)
            );
            if retains_staged {
                for watermark in [None, Some(staged_version - 1)] {
                    let mut unpublished_hint = hint.clone();
                    unpublished_hint.log_segment_files.max_published_version = watermark;
                    let retained = Snapshot::builder_for(&table_root)
                        .with_snapshot_hint(unpublished_hint)
                        .build(engine.as_ref())?;
                    assert_eq!(retained.version(), 6);
                    assert_eq!(
                        retained.log_segment().listed.max_published_version,
                        watermark
                    );
                }
            }
            let hinted = Snapshot::builder_for(&table_root)
                .with_snapshot_hint(hint.clone())
                .build(engine.as_ref())?;
            let bounded = Snapshot::builder_for(&table_root)
                .with_max_catalog_version(6)
                .with_snapshot_hint(hint)
                .build(engine.as_ref())?;
            assert_eq!(bounded.version(), 6);
            assert!(!reporter.events().iter().any(|event| matches!(
                event,
                MetricEvent::StorageListCompleted(_)
                    | MetricEvent::StorageReadCompleted(_)
                    | MetricEvent::JsonReadCompleted(_)
                    | MetricEvent::ParquetReadCompleted(_)
            )));
            hinted
        };
        assert_eq!(hinted.version(), updated.version());
        assert_eq!(hinted.log_segment().listed, updated.log_segment().listed);
        assert_eq!(hinted.crc_at_version(), updated.crc_at_version());
        let scan = hinted.clone().scan_builder().build()?;
        let mut paths = Vec::new();
        for batch in scan.scan_metadata(engine.as_ref())? {
            paths = batch?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
        }
        paths.sort();
        assert_eq!(
            paths,
            (1..=6)
                .map(|version| format!("file_{version}.parquet"))
                .collect_vec()
        );

        add_commit(&table_root, store.as_ref(), 7, commit_actions(7)).await?;
        let advanced = Snapshot::builder_from(hinted)
            .at_version(7)
            .with_incremental_crc_replay(replay)
            .build(engine.as_ref())?;
        let rebuilt = Snapshot::builder_for(&table_root)
            .with_snapshot_hint(advanced.to_snapshot_hint()?)
            .build(engine.as_ref())?;
        assert_eq!(rebuilt.version(), 7);
        assert!(!rebuilt.table_configuration().is_catalog_managed());
        assert_eq!(rebuilt.log_segment().listed, advanced.log_segment().listed);
        Ok(())
    }

    #[rstest::rstest]
    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn snapshot_hint_and_cached_snapshot_reuse_ratified_staged_commits_without_io(
        #[values(false, true)] downgrade: bool,
        #[values(false, true)] published: bool,
        #[values(false, true)] checkpointed: bool,
    ) -> KernelResult<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_catalog_managed_test().await;
        let engine = Arc::new(MeteredDeltaEngine::new(engine));
        if checkpointed {
            SnapshotBuilder::new_for(&table_root)
                .with_max_catalog_version(0)
                .build(engine.as_ref())?
                .checkpoint(engine.as_ref(), None)?;
        }
        let actions = if downgrade {
            serde_json::json!({
                "protocol": Protocol::try_new_modern(
                    std::iter::empty::<TableFeature>(),
                    [TableFeature::InCommitTimestamp],
                )?
            })
            .to_string()
        } else {
            actions_to_string(vec![TestAction::Add("file_1.parquet".to_string())])
        };
        let staged_path =
            add_staged_commit(&table_root, store.as_ref(), 1, actions.clone()).await?;
        if published {
            add_commit(&table_root, store.as_ref(), 1, actions.clone()).await?;
        }
        let source = SnapshotBuilder::new_for(&table_root)
            .with_log_tail(vec![create_catalog_log_path(&table_root, staged_path)])
            .with_max_catalog_version(1)
            .build(engine.as_ref())?;
        let (reporter, _guard) = measuring_reporter();
        let hint = source.to_snapshot_hint()?;
        assert_eq!(hint.log_segment_files, source.log_segment().listed);

        let without_catalog_version = SnapshotBuilder::new_for(&table_root)
            .with_snapshot_hint(hint.clone())
            .build(engine.as_ref());
        if downgrade {
            assert_eq!(
                without_catalog_version?.log_segment().listed,
                source.log_segment().listed
            );
        } else {
            assert_result_error_with_message(
                without_catalog_version,
                "Max catalog version is required",
            );
        }
        for max_catalog_version in [0, 2] {
            assert_hint_error(
                SnapshotBuilder::new_for(&table_root)
                    .with_max_catalog_version(max_catalog_version),
                hint.clone(),
                engine.as_ref(),
                &format!(
                    "Max catalog version {max_catalog_version} does not match snapshot hint version 1"
                ),
            );
        }
        for max_catalog_version in [None, Some(1)] {
            let mut builder = SnapshotBuilder::new_for(&table_root)
                .at_version(1)
                .with_snapshot_hint(hint.clone());
            let mut cached_builder = SnapshotBuilder::new_from(source.clone()).at_version(1);
            if let Some(bound) = max_catalog_version {
                builder = builder.with_max_catalog_version(bound);
                cached_builder = cached_builder.with_max_catalog_version(bound);
            }
            let historical = builder.build(engine.as_ref())?;
            assert_eq!(historical.log_segment().listed, source.log_segment().listed);
            let cached = cached_builder.build(engine.as_ref())?;
            assert!(Arc::ptr_eq(&cached, &source));
        }
        if !published {
            add_commit(&table_root, store.as_ref(), 1, actions).await?;
        }
        let upgrade = serde_json::json!({
            "protocol": Protocol::try_new_modern(
                [TableFeature::CatalogManaged],
                [TableFeature::CatalogManaged, TableFeature::InCommitTimestamp],
            )?
        })
        .to_string();
        add_commit(&table_root, store.as_ref(), 2, upgrade).await?;
        let historical = SnapshotBuilder::new_for(&table_root)
            .at_version(1)
            .with_max_catalog_version(2)
            .with_snapshot_hint(hint.clone())
            .build(engine.as_ref())?;
        assert_eq!(historical.log_segment().listed, source.log_segment().listed);
        assert!(historical.is_built_as_latest());
        let hinted = SnapshotBuilder::new_for(table_root)
            .with_max_catalog_version(1)
            .with_snapshot_hint(hint)
            .build(engine.as_ref())?;

        assert_eq!(hinted.version(), 1);
        assert!(hinted.is_built_as_latest());
        assert_eq!(
            hinted.table_configuration().is_catalog_managed(),
            !downgrade
        );
        assert_eq!(hinted.log_segment().listed, source.log_segment().listed);
        assert!(!reporter.events().iter().any(|event| matches!(
            event,
            MetricEvent::StorageListCompleted(_)
                | MetricEvent::StorageReadCompleted(_)
                | MetricEvent::JsonReadCompleted(_)
                | MetricEvent::ParquetReadCompleted(_)
        )));
        Ok(())
    }
}
