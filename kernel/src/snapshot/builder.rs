//! Builder for creating [`Snapshot`] instances.
use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::metrics::MetricId;
use crate::snapshot::SnapshotRef;
use crate::utils::try_parse_uri;
use crate::{DeltaResult, Engine, Error, Snapshot, Version};

use tracing::{info, instrument};

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
    /// latest version of the table and 0 <= M <= N.
    #[cfg(feature = "catalog-managed")]
    pub fn with_log_tail(mut self, log_tail: Vec<LogPath>) -> Self {
        self.log_tail = log_tail;
        self
    }

    /// Set the maximum catalog-ratified version. When set, the snapshot will not load versions
    /// beyond this limit, even if later commits exist on the filesystem. This ensures the
    /// catalog remains the source of truth for catalog-managed tables.
    ///
    /// When no explicit time-travel version is set via [`at_version`], the `max_catalog_version`
    /// is used as the effective target version.
    ///
    /// [`at_version`]: Self::at_version
    #[cfg(feature = "catalog-managed")]
    pub fn with_max_catalog_version(mut self, max_catalog_version: Version) -> Self {
        self.max_catalog_version = Some(max_catalog_version);
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
        let reporter = engine.get_metrics_reporter();

        // Pre-build validations for catalog-managed tables
        #[cfg(feature = "catalog-managed")]
        Self::validate_catalog_version_static(version, max_catalog_version, &log_tail)?;

        // Compute effective version: use time-travel version, or fall back to max_catalog_version
        let effective_version = version.or(max_catalog_version);

        let snapshot = if let Some(table_root) = table_root {
            let table_url = try_parse_uri(table_root)?;
            let log_segment = LogSegment::for_snapshot(
                engine.storage_handler().as_ref(),
                table_url.join("_delta_log/")?,
                log_tail,
                effective_version,
                reporter.as_ref(),
                Some(operation_id),
            )?;

            Snapshot::try_new_from_log_segment(table_url, log_segment, engine, Some(operation_id))?
                .into()
        } else {
            let existing_snapshot = existing_snapshot.ok_or_else(|| {
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

        // Post-build validations for catalog-managed tables
        #[cfg(feature = "catalog-managed")]
        Self::validate_catalog_managed_consistency(&snapshot, max_catalog_version)?;

        Ok(snapshot)
    }

    // ===== Catalog-managed Validations =====

    /// Pre-build validations for catalog-managed table invariants.
    #[cfg(feature = "catalog-managed")]
    fn validate_catalog_version_static(
        version: Option<Version>,
        max_catalog_version: Option<Version>,
        log_tail: &[crate::path::ParsedLogPath],
    ) -> DeltaResult<()> {
        use crate::path::LogPathFileType;
        use crate::utils::require;

        // TODO: If inline commits (or any other catalog commits) are
        // ever supported, change this method to check if there are any
        // catalog commits
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
        if let Some(max_cv) = max_catalog_version {
            if let Some(last) = log_tail.last() {
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
        }

        Ok(())
    }

    /// Post-build validation: catalog-managed tables must have max_catalog_version, and
    /// non-catalog-managed tables must not.
    #[cfg(feature = "catalog-managed")]
    fn validate_catalog_managed_consistency(
        snapshot: &SnapshotRef,
        max_catalog_version: Option<Version>,
    ) -> DeltaResult<()> {
        use crate::utils::require;

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
        let version_str = self
            .version
            .map(|v| v.to_string())
            .unwrap_or_else(|| "LATEST".into());

        #[cfg(feature = "catalog-managed")]
        if let Some(mcv) = self.max_catalog_version {
            return format!("{version_str} (max_catalog_version={mcv})");
        }

        version_str
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::default::{
        executor::tokio::TokioBackgroundExecutor, DefaultEngine, DefaultEngineBuilder,
    };

    use itertools::Itertools;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use serde_json::json;

    use super::*;

    fn setup_test() -> (
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<dyn ObjectStore>,
        String,
    ) {
        let table_root = String::from("memory:///");
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
        (engine, store, table_root)
    }

    // TODO (#1990): update this function to properly store the table at table_root
    async fn create_table(store: &Arc<dyn ObjectStore>, _table_root: String) -> DeltaResult<()> {
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

        let path = object_store::path::Path::from(format!("_delta_log/{:020}.json", 0).as_str());
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

        let path = object_store::path::Path::from(format!("_delta_log/{:020}.json", 1).as_str());
        store.put(&path, commit1_data.into()).await?;

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        let engine = engine.as_ref();
        create_table(&store, table_root.clone()).await?;

        let snapshot = SnapshotBuilder::new_for(table_root.clone())
            .with_max_catalog_version(1)
            .build(engine)?;
        assert_eq!(snapshot.version(), 1);

        let snapshot = SnapshotBuilder::new_for(table_root.clone())
            .at_version(0)
            .with_max_catalog_version(1)
            .build(engine)?;
        assert_eq!(snapshot.version(), 0);

        Ok(())
    }

    #[cfg(feature = "catalog-managed")]
    mod catalog_managed_tests {
        use super::*;

        use test_utils::{
            actions_to_string, actions_to_string_catalog_managed, add_commit, add_staged_commit,
            TestAction,
        };

        use crate::log_path::LogPath;
        use crate::utils::try_parse_uri;
        use crate::FileMeta;

        fn create_log_path(table_root: &str, commit_path: object_store::path::Path) -> LogPath {
            let table_url = try_parse_uri(table_root).expect("Failed to parse table root");
            let commit_url = table_url.join(commit_path.as_ref()).unwrap();
            let file_meta = FileMeta {
                location: commit_url,
                last_modified: 123,
                size: 100,
            };
            LogPath::try_new(file_meta).expect("Failed to create LogPath")
        }

        #[test_log::test(tokio::test)]
        async fn test_staged_commits_without_max_catalog_version_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_test();

            let actions = vec![TestAction::Metadata];
            add_commit(
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await?;
            let path1 = add_staged_commit(store.as_ref(), 1, String::from("{}")).await?;

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
            let (engine, store, table_root) = setup_test();

            let actions = vec![TestAction::Metadata];
            add_commit(
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await?;

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
            let (engine, store, table_root) = setup_test();

            let actions = vec![TestAction::Metadata];
            add_commit(
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await?;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(store.as_ref(), 1, actions_to_string(actions)).await?;
            let actions = vec![TestAction::Add("file_2.parquet".to_string())];
            add_commit(store.as_ref(), 2, actions_to_string(actions)).await?;

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
            let (engine, store, table_root) = setup_test();

            let actions = vec![TestAction::Metadata];
            add_commit(
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await?;

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
            add_commit(store.as_ref(), 0, actions_to_string(actions)).await?;

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
        async fn test_max_catalog_version_as_effective_version(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (engine, store, table_root) = setup_test();

            // Create catalog-managed table with commits 0, 1, 2
            let actions = vec![TestAction::Metadata];
            add_commit(
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await?;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(store.as_ref(), 1, actions_to_string(actions)).await?;
            let actions = vec![TestAction::Add("file_2.parquet".to_string())];
            add_commit(store.as_ref(), 2, actions_to_string(actions)).await?;

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
            let (engine, store, table_root) = setup_test();

            // Create catalog-managed table with commits 0, 1
            let actions = vec![TestAction::Metadata];
            add_commit(
                store.as_ref(),
                0,
                actions_to_string_catalog_managed(actions),
            )
            .await?;
            let actions = vec![TestAction::Add("file_1.parquet".to_string())];
            add_commit(store.as_ref(), 1, actions_to_string(actions)).await?;

            // at_version(0) + max_catalog_version=1 -> snapshot at v0
            let snapshot = SnapshotBuilder::new_for(table_root)
                .at_version(0)
                .with_max_catalog_version(1)
                .build(engine.as_ref())?;
            assert_eq!(snapshot.version(), 0);

            Ok(())
        }
    }
}
