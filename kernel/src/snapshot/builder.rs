//! Builder for creating [`Snapshot`] instances.
use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::metrics::MetricId;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::snapshot::SnapshotRef;
use crate::{DeltaResult, Engine, Error, Snapshot, Version};

use tracing::{info, instrument};
use url::Url;

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
    table_root: Option<Url>,
    existing_snapshot: Option<SnapshotRef>,
    version: Option<Version>,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
}

impl SnapshotBuilder {
    pub(crate) fn new_for(table_root: Url) -> Self {
        Self {
            table_root: Some(table_root),
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
    /// latest version of the table and 0 <= M <= N.
    #[cfg(feature = "catalog-managed")]
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }

    /// Set the max catalog version. This is the latest version ratified by the catalog for
    /// catalog-managed tables. Snapshot construction will never load versions beyond this.
    #[cfg(feature = "catalog-managed")]
    pub fn with_max_catalog_version(mut self, version: Version) -> Self {
        self.max_catalog_version = Some(version);
        self
    }

    /// Create a new [`Snapshot`]. This returns a [`SnapshotRef`] (`Arc<Snapshot>`), perhaps
    /// returning a reference to an existing snapshot if the request to build a new snapshot
    /// matches the version of an existing snapshot.
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

        let log_tail: Vec<_> = self.log_tail.into_iter().map(Into::into).collect();

        // Pre-build validations for max_catalog_version
        Self::validate_pre_build(&log_tail, self.version, self.max_catalog_version)?;

        // For ccv2 "latest" queries (no time-travel), max_catalog_version becomes the target
        // version. This reuses the existing time-travel version constraint machinery in
        // LogSegment::for_snapshot.
        let effective_version = self.version.or(self.max_catalog_version);

        let operation_id = MetricId::new();
        let reporter = engine.get_metrics_reporter();

        let snapshot = if let Some(table_root) = self.table_root {
            let log_segment = LogSegment::for_snapshot(
                engine.storage_handler().as_ref(),
                table_root.join("_delta_log/")?,
                log_tail,
                effective_version,
                reporter.as_ref(),
                Some(operation_id),
            )?;

            Snapshot::try_new_from_log_segment(table_root, log_segment, engine, Some(operation_id))?
                .into()
        } else {
            let existing_snapshot = self.existing_snapshot.ok_or_else(|| {
                Error::internal_error(
                    "SnapshotBuilder should have either table_root or existing_snapshot",
                )
            })?;

            Snapshot::try_new_from(
                existing_snapshot,
                log_tail,
                engine,
                effective_version,
                Some(operation_id),
            )?
        };

        // Post-build validation: check ccv2 <-> max_catalog_version relationship
        #[cfg(feature = "catalog-managed")]
        validate_catalog_managed_version(&snapshot, self.max_catalog_version)?;

        Ok(snapshot)
    }

    // ===== Instrumentation Helpers =====

    fn table_path(&self) -> &str {
        self.table_root
            .as_ref()
            .map(|u| u.as_str())
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

    // ===== Pre-build Validations =====

    fn validate_pre_build(
        log_tail: &[ParsedLogPath],
        version: Option<Version>,
        max_catalog_version: Option<Version>,
    ) -> DeltaResult<()> {
        // 1. Catalog commits (staged commits) require max_catalog_version
        let has_staged_commits = log_tail
            .iter()
            .any(|f| f.file_type == LogPathFileType::StagedCommit);
        if has_staged_commits && max_catalog_version.is_none() {
            return Err(Error::generic(
                "Catalog commits (staged commits) were provided in the log tail, \
                 but max_catalog_version was not set. \
                 max_catalog_version is required when catalog commits are present.",
            ));
        }

        // 2. Time travel version must not exceed max_catalog_version
        if let (Some(v), Some(mcv)) = (version, max_catalog_version) {
            if v > mcv {
                return Err(Error::generic(format!(
                    "Cannot time-travel to version {v} which is past the max catalog version {mcv}"
                )));
            }
        }

        // 3. Log tail end version validation (only when max_catalog_version is set and log_tail
        //    is non-empty)
        if let Some(mcv) = max_catalog_version {
            if let Some(last) = log_tail.last() {
                let last_version = last.version;
                if let Some(v) = version {
                    // Time-travel: log tail must include the requested version
                    if last_version < v {
                        return Err(Error::generic(
                            "Provided catalog commits must include the requested \
                             version for time-travel queries",
                        ));
                    }
                } else {
                    // Latest query: log tail must end at max_catalog_version
                    if last_version != mcv {
                        return Err(Error::generic(
                            "Provided catalog commits must end with the max catalog version",
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(feature = "catalog-managed")]
fn validate_catalog_managed_version(
    snapshot: &Snapshot,
    max_catalog_version: Option<Version>,
) -> DeltaResult<()> {
    let is_catalog_managed = snapshot.table_configuration().is_catalog_managed();
    if is_catalog_managed {
        if max_catalog_version.is_none() {
            return Err(Error::generic(
                "Must provide max_catalog_version for catalog-managed tables",
            ));
        }
    } else if max_catalog_version.is_some() {
        return Err(Error::generic(
            "Must not provide max_catalog_version for non-catalog-managed tables",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::default::{
        executor::tokio::TokioBackgroundExecutor, DefaultEngine, DefaultEngineBuilder,
    };
    use crate::path::ParsedLogPath;
    use crate::utils::test_utils::assert_result_error_with_message;

    use itertools::Itertools;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use serde_json::json;

    use super::*;

    fn setup_test() -> (
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<dyn ObjectStore>,
        Url,
    ) {
        let table_root = Url::parse("memory:///test_table").unwrap();
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
        (engine, store, table_root)
    }

    /// Write a commit at the given version with the given JSON actions.
    async fn write_commit(
        store: &Arc<dyn ObjectStore>,
        version: u64,
        actions: &[serde_json::Value],
    ) -> DeltaResult<()> {
        let data = actions
            .iter()
            .map(ToString::to_string)
            .collect_vec()
            .join("\n");
        let path =
            object_store::path::Path::from(format!("_delta_log/{:020}.json", version).as_str());
        store.put(&path, data.into()).await?;
        Ok(())
    }

    async fn create_non_ccv2_table(store: &Arc<dyn ObjectStore>) -> DeltaResult<()> {
        let protocol = json!({
            "minReaderVersion": 1,
            "minWriterVersion": 2,
        });
        let metadata = json!({
            "id": "test-table-id",
            "format": { "provider": "parquet", "options": {} },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        });
        write_commit(
            store,
            0,
            &[json!({"protocol": protocol}), json!({"metaData": metadata})],
        )
        .await?;

        let add_action = json!({
            "add": {
                "path": "part-00000-test.parquet",
                "partitionValues": {},
                "size": 1024,
                "modificationTime": 1587968586000i64,
                "dataChange": true,
                "stats": null,
                "tags": null
            }
        });
        write_commit(store, 1, &[add_action]).await?;
        Ok(())
    }

    fn make_staged_commit(table_root: &Url, version: Version) -> ParsedLogPath {
        ParsedLogPath::create_parsed_staged_commit(table_root, version)
    }

    fn make_published_commit(table_root: &Url, version: Version) -> ParsedLogPath {
        ParsedLogPath::create_parsed_published_commit(table_root, version)
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();
        create_non_ccv2_table(&store).await?;

        let snapshot = SnapshotBuilder::new_for(table_root.clone()).build(engine)?;
        assert_eq!(snapshot.version(), 1);

        let snapshot = SnapshotBuilder::new_for(table_root.clone())
            .at_version(0)
            .build(engine)?;
        assert_eq!(snapshot.version(), 0);

        Ok(())
    }

    // ===== Pre-build validation tests (unit tests, no engine needed) =====

    #[test]
    fn test_catalog_commits_without_max_version() {
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = vec![make_staged_commit(&table_root, 1)];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, None, None);
        assert_result_error_with_message(
            result,
            "max_catalog_version is required when catalog commits are present",
        );
    }

    #[test]
    fn test_log_tail_without_catalog_commits_no_max_version() {
        // Published commits without max_catalog_version should succeed (non-ccv2 case)
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = vec![make_published_commit(&table_root, 1)];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_time_travel_past_max_catalog_version() {
        let log_tail = [];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, Some(10), Some(5));
        assert_result_error_with_message(
            result,
            "Cannot time-travel to version 10 which is past the max catalog version 5",
        );
    }

    #[test]
    fn test_time_travel_at_max_catalog_version() {
        let log_tail = [];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, Some(5), Some(5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_log_tail_ends_wrong_version() {
        // No time-travel: log tail last version must == max_catalog_version
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = vec![make_published_commit(&table_root, 3)];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, None, Some(5));
        assert_result_error_with_message(
            result,
            "Provided catalog commits must end with the max catalog version",
        );
    }

    #[test]
    fn test_log_tail_missing_time_travel_version() {
        // Time-travel: log tail last version must >= requested version
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = vec![make_published_commit(&table_root, 3)];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, Some(5), Some(5));
        assert_result_error_with_message(
            result,
            "Provided catalog commits must include the requested version for time-travel queries",
        );
    }

    #[test]
    fn test_valid_latest_query_with_max_catalog_version() {
        // No time-travel + log_tail ending at max_catalog_version: should pass
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = vec![make_published_commit(&table_root, 5)];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, None, Some(5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_time_travel_within_range() {
        // Time-travel version <= max_catalog_version, log_tail covers the version
        let table_root = Url::parse("memory:///").unwrap();
        let log_tail = vec![
            make_published_commit(&table_root, 3),
            make_published_commit(&table_root, 4),
            make_published_commit(&table_root, 5),
        ];
        let result = SnapshotBuilder::validate_pre_build(&log_tail, Some(4), Some(5));
        assert!(result.is_ok());
    }
}
