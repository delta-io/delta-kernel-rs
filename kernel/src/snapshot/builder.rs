//! Builder for creating [`Snapshot`] instances.

use tracing::{info, instrument};

use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::metrics::MetricId;
use crate::path::LogPathFileType;
use crate::snapshot::SnapshotRef;
use crate::utils::{require, try_parse_uri};
use crate::{DeltaResult, Engine, Error, Snapshot, Version};

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
//
// Note the SnapshotBuilder must have either a table_root or an existing_snapshot (but not both).
// We enforce this in the constructors. We could improve this in the future with different
// types/add type state.
#[derive(Debug)]
pub struct SnapshotBuilder {
    table_root: Option<String>,
    existing_snapshot: Option<SnapshotRef>,
    version: Option<Version>,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
}

impl SnapshotBuilder {
    pub(crate) fn new_for(table_root: impl AsRef<str>) -> Self {
        Self {
            table_root: Some(table_root.as_ref().to_string()),
            existing_snapshot: None,
            version: None,
            log_tail: Vec::new(),
            max_catalog_version: None,
        }
    }

    pub(crate) fn new_from(existing_snapshot: SnapshotRef) -> Self {
        Self {
            table_root: None,
            existing_snapshot: Some(existing_snapshot),
            version: None,
            log_tail: Vec::new(),
            max_catalog_version: None,
        }
    }

    /// Set the target version of the [`Snapshot`]. When omitted, the Snapshot is created at the
    /// latest version of the table.
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

    /// Create a new [`Snapshot`]. This returns a [`SnapshotRef`] (`Arc<Snapshot>`), perhaps
    /// returning a reference to an existing snapshot if the request to build a new snapshot
    /// matches the version of an existing snapshot.
    ///
    /// Reports metrics: [`MetricEvent::SnapshotCompleted`] or [`MetricEvent::SnapshotFailed`].
    ///
    /// # Parameters
    ///
    /// - `engine`: Implementation of [`Engine`] apis.
    ///
    /// [`MetricEvent::SnapshotCompleted`]: crate::metrics::MetricEvent::SnapshotCompleted
    /// [`MetricEvent::SnapshotFailed`]: crate::metrics::MetricEvent::SnapshotFailed
    // Span name must match `SNAP_BUILD_SPAN` in `metrics::reporter`.
    #[instrument(
        name = "snap.build",
        skip_all,
        fields(path = %self.table_path(), report, version = tracing::field::Empty, operation_id = tracing::field::Empty),
        err
    )]
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<SnapshotRef> {
        info!(
            target = self.target_version_str(),
            from_version = ?self.existing_snapshot.as_ref().map(|s| s.version()),
            log_tail_len = self.log_tail.len(),
            max_catalog_version = ?self.max_catalog_version,
            "building snapshot"
        );

        // Destructure self so fields can be moved independently
        let Self {
            table_root,
            existing_snapshot,
            version,
            log_tail,
            max_catalog_version,
        } = self;

        let log_tail: Vec<_> = log_tail.into_iter().map(Into::into).collect();
        let operation_id = MetricId::new();
        tracing::Span::current().record("operation_id", tracing::field::display(operation_id));

        // Pre-build validations for catalog-managed tables
        Self::validate_catalog_managed_build_inputs(version, max_catalog_version, &log_tail)?;

        // Use time-travel version if set, otherwise fall back to max_catalog_version. Passing this
        // as the version to LogSegment::for_snapshot does NOT skip the _last_checkpoint hint --
        // the hint is still used when its version <= effective_version.
        let effective_version = version.or(max_catalog_version);

        let result = if let Some(table_root) = table_root {
            try_parse_uri(table_root).and_then(|table_url| {
                let log_segment = LogSegment::for_snapshot(
                    engine.storage_handler().as_ref(),
                    table_url.join("_delta_log/")?,
                    log_tail,
                    effective_version,
                    operation_id,
                )?;
                Snapshot::try_new_from_log_segment(
                    table_url,
                    log_segment,
                    engine,
                    operation_id,
                    None,
                )
                .map(Into::into)
            })
        } else {
            existing_snapshot
                .ok_or_else(|| {
                    Error::internal_error(
                        "SnapshotBuilder should have either table_root or existing_snapshot",
                    )
                })
                .and_then(|existing_snapshot| {
                    Snapshot::try_new_from(
                        existing_snapshot,
                        log_tail,
                        engine,
                        effective_version,
                        operation_id,
                    )
                })
        };

        // Post-build validations for catalog-managed tables
        let result = result.and_then(|snapshot| {
            Self::validate_catalog_managed_build_result(&snapshot, max_catalog_version)?;
            Ok(snapshot)
        });
        if let Ok(ref snapshot) = result {
            tracing::Span::current().record("version", snapshot.version());
        }
        result
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
                pair[0].version + 1 == pair[1].version,
                Error::generic(format!(
                    "log_tail must be sorted and contiguous, but found versions {} and {}",
                    pair[0].version, pair[1].version
                ))
            );
        }

        // TODO: If inline commits (or any other catalog commits) are ever supported, change this
        // method to check if there are any catalog commits.
        let has_catalog_commits = log_tail
            .iter()
            .any(|p| p.file_type == LogPathFileType::StagedCommit);

        // Staged commits require max_catalog_version
        require!(
            !has_catalog_commits || max_catalog_version.is_some(),
            Error::generic(
                "Staged commits in log_tail require max_catalog_version to be set. \
                 Use with_max_catalog_version() when providing staged commits."
            )
        );

        // Time-travel version must not exceed max_catalog_version
        if let (Some(ver), Some(max_cv)) = (version, max_catalog_version) {
            require!(
                ver <= max_cv,
                Error::generic(format!(
                    "Time-travel version {ver} exceeds max_catalog_version {max_cv}"
                ))
            );
        }

        // Log tail end version validation when max_catalog_version is set
        if let (Some(max_cv), Some(last)) = (max_catalog_version, log_tail.last()) {
            if let Some(ver) = version {
                // With time-travel: last log_tail entry must be >= requested version
                require!(
                    last.version >= ver,
                    Error::generic(format!(
                        "Log tail last version {} is less than requested version {ver}",
                        last.version
                    ))
                );
            } else {
                // Without time-travel: last log_tail entry must == max_catalog_version
                require!(
                    last.version == max_cv,
                    Error::generic(format!(
                        "Log tail last version {} does not match max_catalog_version {max_cv}",
                        last.version
                    ))
                );
            }
        }

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
            Error::generic(
                "Catalog-managed table requires max_catalog_version to be set. \
                 Use with_max_catalog_version() when loading a catalog-managed table."
            )
        );
        require!(
            is_catalog_managed || max_catalog_version.is_none(),
            Error::generic("max_catalog_version must not be set for non-catalog-managed tables.")
        );

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
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use serde_json::json;
    use test_utils::{actions_to_string, add_commit, TestAction};
    use tracing_subscriber::util::SubscriberInitExt as _;

    use super::*;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::default::{DefaultEngine, DefaultEngineBuilder};
    use crate::metrics::{MetricEvent, WithMetricsReporterLayer as _};
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::{DynObjectStore, ObjectStoreExt as _};
    use crate::utils::test_utils::CapturingReporter;

    fn setup_test() -> (
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<DynObjectStore>,
        String,
    ) {
        let table_root = String::from("memory:///");
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
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
            actions_to_string(vec![TestAction::Add("part-00000-test.parquet".into())]),
        )
        .await?;
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
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"interval_col\",\"type\":\"interval second\",\"nullable\":true,\"metadata\":{}}]}",
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
            err_msg.contains("Unsupported Delta table type: 'interval second'"),
            "Expected clear error message about unsupported type, got: {err_msg}"
        );

        Ok(())
    }

    fn measuring_reporter() -> (Arc<CapturingReporter>, tracing::subscriber::DefaultGuard) {
        // Install a real global default before the per-test thread-local subscriber. See
        // `test_utils::ensure_metrics_compatible_global_subscriber` for the rationale.
        test_utils::ensure_metrics_compatible_global_subscriber();
        let reporter = Arc::new(CapturingReporter::default());
        let guard = tracing_subscriber::registry()
            .with_metrics_reporter_layer(reporter.clone())
            .set_default();
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
            "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"interval second","nullable":true,"metadata":{}}]}"#,
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
                .any(|e| matches!(e, MetricEvent::SnapshotFailed { .. })),
            "expected SnapshotFailed event on build failure"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotCompleted { .. })),
            "should not emit SnapshotCompleted on failure"
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
        let (version, total_duration) = events
            .iter()
            .find_map(|e| {
                if let MetricEvent::SnapshotCompleted {
                    version,
                    total_duration,
                    ..
                } = e
                {
                    Some((*version, *total_duration))
                } else {
                    None
                }
            })
            .expect("expected SnapshotCompleted event");
        assert_eq!(version, 1, "version should match the updated snapshot");
        assert!(
            total_duration > Duration::ZERO,
            "total_duration should be non-zero"
        );
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
                .any(|e| matches!(e, MetricEvent::SnapshotFailed { .. })),
            "expected SnapshotFailed when version update goes backwards"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MetricEvent::SnapshotCompleted { .. })),
            "should not emit SnapshotCompleted when version update fails"
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_completed_total_duration_exceeds_log_segment_load_duration(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        create_table(&store, &table_root).await?;

        let (reporter, _guard) = measuring_reporter();
        let _snap = SnapshotBuilder::new_for(table_root).build(engine.as_ref())?;

        let events = reporter.events();
        let total_duration = events
            .iter()
            .find_map(|e| {
                if let MetricEvent::SnapshotCompleted { total_duration, .. } = e {
                    Some(*total_duration)
                } else {
                    None
                }
            })
            .expect("expected SnapshotCompleted event");
        let segment_duration = events
            .iter()
            .find_map(|e| {
                if let MetricEvent::LogSegmentLoaded { duration, .. } = e {
                    Some(*duration)
                } else {
                    None
                }
            })
            .expect("expected LogSegmentLoaded event");

        assert!(
            total_duration > Duration::ZERO,
            "total_duration should be non-zero"
        );
        assert!(
            total_duration >= segment_duration,
            "SnapshotCompleted.total_duration ({total_duration:?}) should be >= LogSegmentLoaded.duration ({segment_duration:?})"
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
        async fn setup_catalog_managed_test() -> (
            Arc<DefaultEngine<TokioBackgroundExecutor>>,
            Arc<DynObjectStore>,
            String,
        ) {
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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Staged commits in log_tail require max_catalog_version"));

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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Time-travel version 5 exceeds max_catalog_version 3"));

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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Log tail last version 2 does not match max_catalog_version 3"));

            Ok(())
        }

        #[test_log::test(tokio::test)]
        async fn test_catalog_managed_table_without_max_catalog_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, _store, table_root) = setup_catalog_managed_test().await;

            let result = SnapshotBuilder::new_for(table_root).build(engine.as_ref());

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Catalog-managed table requires max_catalog_version"));

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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("max_catalog_version must not be set for non-catalog-managed tables"));

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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Log tail last version 1 is less than requested version 2"));

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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Catalog-managed table requires max_catalog_version"));

            Ok(())
        }

        #[rstest::rstest]
        #[case::gap(vec![1, 3], vec![1, 3], 3)]
        #[case::duplicates(vec![1], vec![1, 1], 1)]
        #[case::unsorted(vec![1, 2], vec![2, 1], 2)]
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

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("log_tail must be sorted and contiguous"));

            Ok(())
        }
    }
}
