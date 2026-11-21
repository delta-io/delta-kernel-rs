//! Declarative plans for data cleanup (VACUUM): which files under the table root may be deleted.
//!
//! A file may be deleted when no table version inside the deleted-file retention window can read
//! it and it is older than that window. The plan lists files under the table root, subtracts every
//! file the window can read, and keeps only files last modified before the retention cutoff. The
//! age check protects recently written files; it assumes an uncommitted transaction does not keep
//! its files longer than the retention period. Kernel builds the plan; the connector executes it
//! and deletes the returned files.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use super::{Snapshot, SnapshotRef};
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::actions::{ADD_NAME, CDC_NAME, REMOVE_NAME};
use crate::expressions::{Expression as Expr, Predicate, UnaryExpressionOp, col, column_name, lit};
use crate::history_manager::error::LogHistoryError;
use crate::history_manager::search::Bound;
use crate::history_manager::{HistoryCommitType, get_earliest_commit, timestamp_to_version};
use crate::log_segment::LogSegment;
use crate::plans::ir::plan::Plan;
use crate::schema::{SchemaRef, ToSchema, lazy_schema_ref};
use crate::table_features::{Operation, TableFeature};
use crate::utils::require;
use crate::{
    Engine, Error, KernelError, KernelResult, PlanBuilder, Result, Version, version_as_i64,
};

/// Output column containing each file's fully qualified location.
pub const DATA_CLEANUP_LOCATION: &str = "location";
/// Output column containing each file's table-relative, URI-encoded path.
pub const DATA_CLEANUP_RELATIVE_PATH: &str = "relative_path";
/// Output column containing each file's last modification time in milliseconds since the Unix
/// epoch.
pub const DATA_CLEANUP_LAST_MODIFIED: &str = "last_modified";

/// The Delta default for `delta.deletedFileRetentionDuration`.
const DEFAULT_DELETED_FILE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Kernel owns every file under the log directory; data cleanup never returns one.
const DELTA_LOG_PREFIX: &str = "_delta_log/";
/// The smallest string greater than every string starting with [`DELTA_LOG_PREFIX`]: `'0'` is the
/// byte after `'/'`.
const DELTA_LOG_PREFIX_END: &str = "_delta_log0";

const PATH: &str = "path";
const VERSION: &str = "version";
const DELETION_VECTOR: &str = "deletionVector";

/// One row per file a table version can read.
static REACHABLE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null PATH: STRING,
};

/// The commit fields the Remove and CDC arms read, narrower than the full action schemas.
static COMMIT_READ_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable REMOVE_NAME: {
        not_null PATH: STRING,
        nullable DELETION_VECTOR: (DeletionVectorDescriptor::to_schema()),
    },
    nullable CDC_NAME: {
        not_null PATH: STRING,
    },
    nullable VERSION: LONG,
};

/// Inputs to [`Snapshot::eligible_data_files_to_be_deleted`] that are not part of the table state.
#[derive(Debug, Clone)]
pub struct DataCleanupOptions {
    now_ms: i64,
    deleted_file_retention: Option<Duration>,
}

impl DataCleanupOptions {
    /// Options for a cleanup running at `now_ms` (milliseconds since the Unix epoch). Kernel has no
    /// clock, so the caller supplies the time retention is measured from.
    pub fn new(now_ms: i64) -> Self {
        Self {
            now_ms,
            deleted_file_retention: None,
        }
    }

    /// Overrides `delta.deletedFileRetentionDuration`. A retention shorter than the table's lets
    /// cleanup delete files that time travel inside the table's configured window still reads.
    pub fn with_deleted_file_retention(mut self, retention: Duration) -> Self {
        self.deleted_file_retention = Some(retention);
        self
    }
}

impl Snapshot {
    /// Builds a plan that lists table files data cleanup may delete.
    ///
    /// The plan recursively lists files under the table root. A listed file is eligible when all of
    /// these hold:
    ///
    /// - It was last modified before the retention cutoff: `now - retention`, where retention is
    ///   [`DataCleanupOptions::with_deleted_file_retention`], else
    ///   `delta.deletedFileRetentionDuration`, else seven days. This protects recently written
    ///   files; it assumes uncommitted transactions do not keep files longer than the retention
    ///   period.
    /// - No table version a reader at or after the cutoff can see reads it: no data file, deletion
    ///   vector file, or change data file of those versions has its location or relative path.
    /// - It is not under `_delta_log/`.
    ///
    /// The output follows [`crate::plans::ir::nodes::LIST_FILES_SCHEMA`]. Kernel builds the plan;
    /// the connector executes it and deletes the returned files.
    ///
    /// # Errors
    ///
    /// Returns an error if kernel cannot write to the table, if the table supports
    /// `adaptiveMetadata`, or if a commit the window reads is missing from the log. Kernel never
    /// narrows the window to the commits it finds.
    #[tracing::instrument(
        name = "snapshot.eligible_data_files_to_be_deleted",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub fn eligible_data_files_to_be_deleted(
        self: &SnapshotRef,
        engine: &dyn Engine,
        options: &DataCleanupOptions,
    ) -> Result<Plan> {
        let config = self.table_configuration();
        config.ensure_operation_supported(Operation::Write)?;
        // Live adds of adaptive-metadata tables live in the content tree, which the declarative
        // scan does not read. An incomplete reachable set would let cleanup delete live files.
        require!(
            !config.is_feature_supported(&TableFeature::AdaptiveMetadataPreview),
            Error::Kernel(KernelError::unsupported(
                "eligible_data_files_to_be_deleted does not support adaptiveMetadata tables"
            ))
        );
        let retention = options
            .deleted_file_retention
            .or(self.table_properties().deleted_file_retention_duration)
            .unwrap_or(DEFAULT_DELETED_FILE_RETENTION);
        let retention_ms = i64::try_from(retention.as_millis()).map_err(|_| {
            KernelError::generic(format!("deleted file retention {retention:?} is too large"))
        })?;
        let cutoff_ms = options.now_ms.saturating_sub(retention_ms);
        let window = self.retention_window(engine, cutoff_ms)?;
        let reachable = self.reachable_data_files(engine, window)?;
        let table_root = self
            .log_segment()
            .log_root
            .join("../")
            .map_err(KernelError::from)?;

        let in_log = Predicate::and(
            col!(DATA_CLEANUP_RELATIVE_PATH).ge(lit(DELTA_LOG_PREFIX)),
            col!(DATA_CLEANUP_RELATIVE_PATH).lt(lit(DELTA_LOG_PREFIX_END)),
        );
        // A reachable path is relative when the action names a file under the table root and
        // absolute otherwise, so a candidate is reachable if either of its forms matches.
        Ok(PlanBuilder::list_files(table_root)?
            .filter(Predicate::and(
                col!(DATA_CLEANUP_LAST_MODIFIED).lt(lit(cutoff_ms)),
                Predicate::not(in_log),
            ))?
            .anti_join(
                reachable.clone(),
                [column_name!(DATA_CLEANUP_LOCATION)],
                [column_name!(PATH)],
            )?
            .anti_join(
                reachable,
                [column_name!(DATA_CLEANUP_RELATIVE_PATH)],
                [column_name!(PATH)],
            )?
            .build()?)
    }

    /// Builds the relation of every file some version in `window` can read: data files, deletion
    /// vector files, and change data files, each as its action records the path (relative to the
    /// table root, or absolute).
    ///
    /// A file a version in the window reads is either still live at this snapshot or removed by a
    /// later commit in the window, so the relation unions this snapshot's live adds with the files
    /// of the window's Remove actions and the change data files its commits wrote. Removes come
    /// from commits, not checkpoint tombstones, whose retention a writer may have cut shorter than
    /// the window. A path may appear more than once.
    pub(crate) fn reachable_data_files(
        self: &SnapshotRef,
        engine: &dyn Engine,
        window: RetentionWindow,
    ) -> Result<PlanBuilder> {
        let log_segment = self.log_segment();
        // Catalog-managed tables hold their ratified, unpublished commits in the snapshot's tail.
        let staged_tail = log_segment.listed.staged_commits().cloned().collect();
        let commits = LogSegment::for_table_changes_with_log_tail(
            engine.storage_handler().as_ref(),
            log_segment.log_root.clone(),
            window.first_commit,
            self.version(),
            staged_tail,
        )?;
        let commit_actions = PlanBuilder::scan_json(
            LogSegment::version_tagged_scan_files(&commits.listed.ascending_commit_files)?,
            &[VERSION],
            COMMIT_READ_SCHEMA.clone(),
        )?;
        let removes = commit_actions.clone().filter(Predicate::and(
            col!(REMOVE_NAME, PATH).is_not_null(),
            col!(VERSION).ge(lit(version_as_i64(window.first_remove)?)),
        ))?;
        let cdc = commit_actions.filter(col!(CDC_NAME, PATH).is_not_null())?;

        let mut arms = vec![
            action_files(removes.clone(), REMOVE_NAME)?,
            deletion_vector_files(removes, REMOVE_NAME)?,
            action_files(cdc, CDC_NAME)?,
        ];
        let live_adds = Arc::clone(self)
            .scan_builder()
            .build()?
            .declarative_metadata_scan_plan_builder(engine)?;
        if let Some(adds) = live_adds {
            arms.push(action_files(adds.clone(), ADD_NAME)?);
            arms.push(deletion_vector_files(adds, ADD_NAME)?);
        }
        Ok(PlanBuilder::union_all(arms)?)
    }

    /// The commits whose actions name every file a reader at or after `cutoff_ms` can see.
    fn retention_window(&self, engine: &dyn Engine, cutoff_ms: i64) -> Result<RetentionWindow> {
        match timestamp_to_version(
            self,
            engine,
            cutoff_ms,
            Bound::GreatestLower,
            HistoryCommitType::Published,
        ) {
            // A reader at the cutoff sees `commit.version`, which still reads the change data its
            // commit wrote and every file later commits remove.
            Ok(commit) => Ok(RetentionWindow {
                first_commit: commit.version,
                first_remove: commit.version + 1,
            }),
            // Every retained commit is newer than the cutoff. A version before the earliest commit
            // can still be readable from a checkpoint, and the files it reads that later versions
            // dropped are removed by commits from the earliest one on.
            Err(LogHistoryError::TimestampOutOfRange { .. }) => {
                let earliest = get_earliest_commit(
                    engine,
                    &self.log_segment().log_root,
                    None,
                    HistoryCommitType::Published,
                )?;
                Ok(RetentionWindow {
                    first_commit: earliest,
                    first_remove: earliest,
                })
            }
            Err(err) => Err(KernelError::from(err).into()),
        }
    }
}

/// The commits [`Snapshot::reachable_data_files`] reads: Remove actions from `first_remove` and
/// change data files from `first_commit`, both through the snapshot version.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RetentionWindow {
    first_commit: Version,
    first_remove: Version,
}

/// Projects the file named by each row's `action` struct.
fn action_files(plan: PlanBuilder, action: &str) -> KernelResult<PlanBuilder> {
    plan.project(
        Expr::struct_from([Expr::column([action, PATH])]),
        REACHABLE_SCHEMA.clone(),
    )
}

/// Projects the file storing each row's `action` deletion vector. Rows without a DV, or with an
/// inline DV that no file stores, are dropped.
fn deletion_vector_files(plan: PlanBuilder, action: &str) -> KernelResult<PlanBuilder> {
    plan.filter(
        Expr::column([action, DELETION_VECTOR, "storageType"])
            .ne(lit(DeletionVectorStorageType::Inline.to_string())),
    )?
    .project(
        Expr::struct_from([Expr::unary(
            UnaryExpressionOp::DeletionVectorPath,
            Expr::column([action, DELETION_VECTOR]),
        )]),
        REACHABLE_SCHEMA.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use rstest::rstest;
    use test_utils::add_commit;
    use url::Url;

    use super::*;
    use crate::Snapshot;
    use crate::arrow::array::AsArray as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::engine::sync::plan::SyncPlanExecutor;
    use crate::object_store::DynObjectStore;
    use crate::object_store::memory::InMemory;
    use crate::plans::{Operation, PlanExecutor as _};
    use crate::unit_test_utils::assert_result_error_with_message;

    const TABLE_ROOT: &str = "memory:///";
    const COMMIT_TIMESTAMP_BASE_MS: i64 = 4_000_000_000_000;
    const DV_FILE: &str = "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";
    const CDC_FILE: &str = "_change_data/cdc-2.parquet";
    const ABSOLUTE_FILE: &str = "memory:///e.parquet";
    const UUID_DV: Option<(&str, &str)> = Some(("u", "ab^-aqEH.-t@S}K{vb[*k^"));
    const INLINE_DV: Option<(&str, &str)> =
        Some(("i", "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L"));

    fn file_action(action: &str, path: &str, dv: Option<(&str, &str)>) -> String {
        let dv = dv.map_or_else(String::new, |(storage_type, path_or_inline_dv)| {
            format!(
                r#","deletionVector":{{"storageType":"{storage_type}","pathOrInlineDv":"{path_or_inline_dv}","offset":1,"sizeInBytes":36,"cardinality":2}}"#
            )
        });
        format!(
            r#"{{"{action}":{{"path":"{path}","partitionValues":{{}},"size":10,"modificationTime":1,"deletionTimestamp":1,"dataChange":true{dv}}}}}"#
        )
    }

    fn commit_info(timestamp: i64) -> String {
        format!(
            r#"{{"commitInfo":{{"inCommitTimestamp":{timestamp},"timestamp":{timestamp},"operation":"WRITE"}}}}"#
        )
    }

    /// In-commit timestamps place commit `v` at `COMMIT_TIMESTAMP_BASE_MS + (v + 1) * 1000`, with
    /// deleted-file retention one second. v0 adds `a`; v1 adds `b` with a UUID DV; v2 removes `a`,
    /// adds `c`, and writes a CDC file; v3 removes `b`, adds `d` with an inline DV, and adds `e` by
    /// absolute path. With `checkpoint`, v2 is checkpointed, so windows starting before it read
    /// pre-checkpoint commits.
    async fn table(checkpoint: bool) -> (Arc<DynObjectStore>, Arc<dyn Engine>, SnapshotRef) {
        let metadata = [
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors","inCommitTimestamp"]}}"#,
            r#"{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.enableInCommitTimestamps":"true","delta.deletedFileRetentionDuration":"interval 1 second"},"createdTime":1}}"#,
        ]
        .join("\n");
        let cdc = format!(
            r#"{{"cdc":{{"path":"{CDC_FILE}","partitionValues":{{}},"size":7,"dataChange":false}}}}"#
        );
        let commits = [
            vec![metadata, file_action("add", "a.parquet", None)],
            vec![file_action("add", "b.parquet", UUID_DV)],
            vec![
                file_action("remove", "a.parquet", None),
                file_action("add", "c.parquet", None),
                cdc,
            ],
            vec![
                file_action("remove", "b.parquet", UUID_DV),
                file_action("add", "d.parquet", INLINE_DV),
                file_action("add", ABSOLUTE_FILE, None),
            ],
        ];
        let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
        for (version, actions) in commits.into_iter().enumerate() {
            let timestamp = COMMIT_TIMESTAMP_BASE_MS + (version as i64 + 1) * 1000;
            let body = std::iter::once(commit_info(timestamp))
                .chain(actions)
                .collect::<Vec<_>>()
                .join("\n");
            add_commit(TABLE_ROOT, store.as_ref(), version as u64, body)
                .await
                .unwrap();
        }
        let engine: Arc<dyn Engine> = Arc::new(SyncEngine::new_with_store(Arc::clone(&store)));
        for path in [
            "a.parquet",
            "b.parquet",
            "c.parquet",
            "d.parquet",
            "e.parquet",
            DV_FILE,
            CDC_FILE,
            "orphan.parquet",
            "part=a%20b/orphan.parquet",
        ] {
            let url = Url::parse(&format!("{TABLE_ROOT}{path}")).unwrap();
            engine
                .storage_handler()
                .put(&url, Bytes::new(), true)
                .unwrap();
        }
        if checkpoint {
            Snapshot::builder_for(TABLE_ROOT)
                .at_version(2)
                .build(engine.as_ref())
                .unwrap()
                .checkpoint(engine.as_ref(), None)
                .unwrap();
        }
        let snapshot = Snapshot::builder_for(TABLE_ROOT)
            .build(engine.as_ref())
            .unwrap();
        (store, engine, snapshot)
    }

    /// Plans and executes data cleanup, returning the eligible relative paths.
    fn eligible(
        store: &Arc<DynObjectStore>,
        engine: &dyn Engine,
        snapshot: &SnapshotRef,
        options: &DataCleanupOptions,
    ) -> Result<BTreeSet<String>> {
        let executor = SyncPlanExecutor::new(Some(Arc::clone(store)));
        let plan = snapshot.eligible_data_files_to_be_deleted(engine, options)?;
        let mut paths = BTreeSet::new();
        for batch in executor
            .execute_op(Operation::QueryPlan(plan))?
            .into_data()?
        {
            let batch = batch?.try_into_record_batch()?;
            let column = batch
                .column_by_name(DATA_CLEANUP_RELATIVE_PATH)
                .unwrap()
                .as_string::<i32>();
            paths.extend(column.iter().flatten().map(str::to_string));
        }
        Ok(paths)
    }

    #[rstest]
    // Cutoff 2500: a reader sees v1, so v2's CDC file and every later remove stay protected.
    #[case::window_starts_at_v1(
        COMMIT_TIMESTAMP_BASE_MS + 3_500,
        None,
        vec!["orphan.parquet", "part=a%20b/orphan.parquet"],
    )]
    // Cutoff 3500: v2 is the oldest readable version, so `a` (removed by v2) can go, but the CDC
    // file v2 wrote cannot.
    #[case::window_starts_at_v2(
        COMMIT_TIMESTAMP_BASE_MS + 4_500,
        None,
        vec!["a.parquet", "orphan.parquet", "part=a%20b/orphan.parquet"],
    )]
    // Cutoff 9000: only v3 is readable, so every removed or CDC file is eligible.
    #[case::window_at_latest(
        COMMIT_TIMESTAMP_BASE_MS + 10_000,
        None,
        vec![
            "a.parquet",
            "b.parquet",
            CDC_FILE,
            DV_FILE,
            "orphan.parquet",
            "part=a%20b/orphan.parquet",
        ],
    )]
    // Cutoff 500 precedes every commit, so every commit's files stay protected.
    #[case::cutoff_before_first_commit(
        COMMIT_TIMESTAMP_BASE_MS + 1_500,
        None,
        vec!["orphan.parquet", "part=a%20b/orphan.parquet"],
    )]
    // A longer retention override moves the cutoff from 9000 back to 2500.
    #[case::retention_override(
        COMMIT_TIMESTAMP_BASE_MS + 10_000,
        Some(Duration::from_millis(7_500)),
        vec!["orphan.parquet", "part=a%20b/orphan.parquet"],
    )]
    #[tokio::test]
    async fn eligible_files_exclude_files_the_retention_window_reads(
        #[case] now_ms: i64,
        #[case] retention: Option<Duration>,
        #[case] expected: Vec<&str>,
        #[values(false, true)] checkpoint: bool,
    ) {
        let (store, engine, snapshot) = table(checkpoint).await;
        let mut options = DataCleanupOptions::new(now_ms);
        if let Some(retention) = retention {
            options = options.with_deleted_file_retention(retention);
        }
        let expected = expected.into_iter().map(str::to_string).collect();
        assert_eq!(
            eligible(&store, engine.as_ref(), &snapshot, &options).unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn eligible_files_fail_when_a_window_commit_is_missing() {
        let (store, engine, snapshot) = table(true).await;
        let commit = url::Url::parse(&format!("{TABLE_ROOT}_delta_log/00000000000000000001.json"));
        engine.storage_handler().delete(&commit.unwrap()).unwrap();
        let result = eligible(
            &store,
            engine.as_ref(),
            &snapshot,
            &DataCleanupOptions::new(COMMIT_TIMESTAMP_BASE_MS + 3_500),
        );
        assert_result_error_with_message(result, "Table version 1 is missing");
    }

    #[tokio::test]
    async fn eligible_files_exclude_recently_written_files() {
        let (store, engine, snapshot) = table(false).await;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let recent = Url::parse(&format!("{TABLE_ROOT}recent.parquet")).unwrap();
        engine
            .storage_handler()
            .put(&recent, Bytes::new(), true)
            .unwrap();

        let eligible = eligible(
            &store,
            engine.as_ref(),
            &snapshot,
            &DataCleanupOptions::new(now_ms).with_deleted_file_retention(Duration::ZERO),
        )
        .unwrap();
        assert!(!eligible.contains("recent.parquet"));
    }
}
