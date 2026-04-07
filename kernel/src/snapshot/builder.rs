//! Builder for creating [`Snapshot`] instances.
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, instrument};

use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::metrics::{MetricEvent, MetricId, MetricsReporter};
use crate::snapshot::SnapshotRef;
use crate::utils::try_parse_uri;
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
}

impl SnapshotBuilder {
    pub(crate) fn new_for(table_root: impl AsRef<str>) -> Self {
        Self {
            table_root: Some(table_root.as_ref().to_string()),
            existing_snapshot: None,
            version: None,
            log_tail: Vec::new(),
        }
    }

    pub(crate) fn new_from(existing_snapshot: SnapshotRef) -> Self {
        Self {
            table_root: None,
            existing_snapshot: Some(existing_snapshot),
            version: None,
            log_tail: Vec::new(),
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
    /// latest version of the table and 0 <= M <= N.
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>) -> Self {
        self.log_tail = log_tail;
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
    #[instrument(
        name = "snap.build",
        skip_all,
        fields(path = %self.table_path()),
        err
    )]
    pub fn build(self, engine: &dyn Engine) -> DeltaResult<SnapshotRef> {
        info!(
            target = self.target_version_str(),
            from_version = ?self.existing_snapshot.as_ref().map(|s| s.version()),
            log_tail_len = self.log_tail.len(),
            "building snapshot"
        );

        let log_tail = self.log_tail.into_iter().map(Into::into).collect();
        let operation_id = MetricId::new();
        let reporter = engine.get_metrics_reporter();
        let start = Instant::now();

        let result = if let Some(table_root) = self.table_root {
            try_parse_uri(table_root).and_then(|table_url| {
                let log_segment = LogSegment::for_snapshot(
                    engine.storage_handler().as_ref(),
                    table_url.join("_delta_log/")?,
                    log_tail,
                    self.version,
                    reporter.as_ref(),
                    Some(operation_id),
                )?;
                Snapshot::try_new_from_log_segment_impl(
                    table_url,
                    log_segment,
                    engine,
                    operation_id,
                )
                .map(Into::into)
            })
        } else {
            self.existing_snapshot
                .ok_or_else(|| {
                    Error::internal_error(
                        "SnapshotBuilder should have either table_root or existing_snapshot",
                    )
                })
                .and_then(|existing_snapshot| {
                    Snapshot::try_new_from_impl(
                        existing_snapshot,
                        log_tail,
                        engine,
                        self.version,
                        operation_id,
                    )
                })
        };

        Self::report_snapshot_build_result(result, start, operation_id, reporter.as_ref())
    }

    /// Emit [`MetricEvent::SnapshotCompleted`] or [`MetricEvent::SnapshotFailed`] based on the
    /// result, measuring total duration from `start`.
    fn report_snapshot_build_result(
        result: DeltaResult<SnapshotRef>,
        start: Instant,
        operation_id: MetricId,
        reporter: Option<&Arc<dyn MetricsReporter>>,
    ) -> DeltaResult<SnapshotRef> {
        let snapshot_duration = start.elapsed();
        match &result {
            Ok(snapshot) => {
                reporter.inspect(|r| {
                    r.report(MetricEvent::SnapshotCompleted {
                        operation_id,
                        version: snapshot.version(),
                        total_duration: snapshot_duration,
                    });
                });
            }
            Err(_) => {
                reporter.inspect(|r| {
                    r.report(MetricEvent::SnapshotFailed {
                        operation_id,
                        duration: snapshot_duration,
                    });
                });
            }
        }
        result
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
        self.version
            .map(|v| v.to_string())
            .unwrap_or_else(|| "LATEST".into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::default::{
        executor::tokio::TokioBackgroundExecutor, DefaultEngine, DefaultEngineBuilder,
    };
    use crate::metrics::MetricEvent;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::{DynObjectStore, ObjectStore as _};
    use crate::utils::test_utils::CapturingReporter;
    use itertools::Itertools;
    use serde_json::json;

    use super::*;

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

    // TODO (#1990): update this function to properly store the table at table_root
    async fn create_table(store: &Arc<DynObjectStore>, _table_root: String) -> DeltaResult<()> {
        let protocol = json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged"],
            "writerFeatures": ["catalogManaged"],
        });

        let metadata = json!({
            "id": "test-table-id",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        });

        // Create commit 0 with protocol and metadata
        let commit0 = [
            json!({
                "protocol": protocol
            }),
            json!({
                "metaData": metadata
            }),
        ];

        // Write commit 0
        let commit0_data = commit0
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");

        let path = Path::from(format!("_delta_log/{:020}.json", 0).as_str());
        store.put(&path, commit0_data.into()).await?;

        // Create commit 1 with a single addFile action
        let commit1 = [json!({
            "add": {
                "path": "part-00000-test.parquet",
                "partitionValues": {},
                "size": 1024,
                "modificationTime": 1587968586000i64,
                "dataChange": true,
                "stats": null,
                "tags": null
            }
        })];

        // Write commit 1
        let commit1_data = commit1
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");

        let path = Path::from(format!("_delta_log/{:020}.json", 1).as_str());
        store.put(&path, commit1_data.into()).await?;

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();
        create_table(&store, table_root.clone()).await?;

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

    fn setup_test_with_reporter() -> (
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<DynObjectStore>,
        String,
        Arc<CapturingReporter>,
    ) {
        let table_root = String::from("memory:///");
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        let reporter = Arc::new(CapturingReporter::default());
        let engine = Arc::new(
            DefaultEngineBuilder::new(store.clone())
                .with_metrics_reporter(reporter.clone())
                .build(),
        );
        (engine, store, table_root, reporter)
    }

    fn assert_has_event(reporter: &CapturingReporter, pred: fn(&MetricEvent) -> bool, msg: &str) {
        let events = reporter.events();
        assert!(events.iter().any(pred), "{msg}");
    }

    fn assert_no_event(reporter: &CapturingReporter, pred: fn(&MetricEvent) -> bool, msg: &str) {
        let events = reporter.events();
        assert!(!events.iter().any(pred), "{msg}");
    }

    fn is_snapshot_completed(e: &MetricEvent) -> bool {
        matches!(e, MetricEvent::SnapshotCompleted { .. })
    }

    fn is_snapshot_failed(e: &MetricEvent) -> bool {
        matches!(e, MetricEvent::SnapshotFailed { .. })
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_failed_emits_metric_on_error() {
        let (engine, store, table_root, reporter) = setup_test_with_reporter();

        // Write a commit with an unsupported schema type to force a build failure
        let commit0_data = [
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({"metaData": {
                "id": "test-table-id",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"interval second\",\"nullable\":true,\"metadata\":{}}]}",
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1587968585495i64
            }}),
        ]
        .iter()
        .map(ToString::to_string)
        .collect_vec()
        .join("\n");

        let path = Path::from("_delta_log/00000000000000000000.json");
        store.put(&path, commit0_data.into()).await.unwrap();

        let result = SnapshotBuilder::new_for(table_root).build(engine.as_ref());
        assert!(result.is_err());

        assert_has_event(
            &reporter,
            is_snapshot_failed,
            "expected a SnapshotFailed event",
        );
        assert_no_event(
            &reporter,
            is_snapshot_completed,
            "should not emit SnapshotCompleted on failure",
        );
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_update_from_existing_emits_metric() {
        let (engine, store, table_root, reporter) = setup_test_with_reporter();
        create_table(&store, table_root.clone()).await.unwrap();

        // Build an initial snapshot at version 0
        let base = SnapshotBuilder::new_for(table_root)
            .at_version(0)
            .build(engine.as_ref())
            .unwrap();

        // Clear events from the initial build
        reporter.clear();

        // Incrementally update to the latest version via the else branch
        let updated = SnapshotBuilder::new_from(base)
            .build(engine.as_ref())
            .unwrap();
        assert_eq!(updated.version(), 1);

        let events = reporter.events();
        let snapshot_completed = events.iter().find_map(|e| match e {
            MetricEvent::SnapshotCompleted {
                version,
                total_duration,
                ..
            } => Some((*version, *total_duration)),
            _ => None,
        });

        let (version, duration) = snapshot_completed.expect("expected SnapshotCompleted event");
        assert_eq!(version, 1);
        assert!(
            !duration.is_zero(),
            "SnapshotCompleted.total_duration should be non-zero"
        );
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_update_to_earlier_version_emits_failed_metric() {
        let (engine, store, table_root, reporter) = setup_test_with_reporter();
        create_table(&store, table_root.clone()).await.unwrap();

        // Build a snapshot at version 1
        let base = SnapshotBuilder::new_for(table_root)
            .build(engine.as_ref())
            .unwrap();
        assert_eq!(base.version(), 1);

        // Clear events from the initial build
        reporter.clear();

        // Attempt to update to version 0 (earlier than base version 1)
        let result = SnapshotBuilder::new_from(base)
            .at_version(0)
            .build(engine.as_ref());
        assert!(result.is_err());

        assert_has_event(
            &reporter,
            is_snapshot_failed,
            "expected a SnapshotFailed event when updating to an earlier version",
        );
        assert_no_event(
            &reporter,
            is_snapshot_completed,
            "should not emit SnapshotCompleted on failure",
        );
    }

    #[test_log::test(tokio::test)]
    async fn snapshot_completed_duration_includes_log_segment_loading() {
        let (engine, store, table_root, reporter) = setup_test_with_reporter();
        create_table(&store, table_root.clone()).await.unwrap();

        let _snapshot = SnapshotBuilder::new_for(table_root)
            .build(engine.as_ref())
            .unwrap();

        assert_has_event(
            &reporter,
            is_snapshot_completed,
            "expected a SnapshotCompleted event",
        );

        let events = reporter.events();

        let log_segment_duration = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::LogSegmentLoaded { duration, .. } => Some(*duration),
                _ => None,
            })
            .expect("expected LogSegmentLoaded event");
        let snapshot_duration = events
            .iter()
            .find_map(|e| match e {
                MetricEvent::SnapshotCompleted { total_duration, .. } => Some(*total_duration),
                _ => None,
            })
            .expect("expected SnapshotCompleted event");

        assert!(
            snapshot_duration >= log_segment_duration,
            "SnapshotCompleted.total_duration ({snapshot_duration:?}) should be >= \
             LogSegmentLoaded.duration ({log_segment_duration:?})"
        );

        let snapshot_completed_count = events.iter().filter(|e| is_snapshot_completed(e)).count();
        assert_eq!(
            snapshot_completed_count, 1,
            "expected exactly one SnapshotCompleted event"
        );
    }
}
