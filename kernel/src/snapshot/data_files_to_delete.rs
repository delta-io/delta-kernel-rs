//! Snapshot plans for VACUUM candidate discovery and file reachability.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use delta_kernel_derive::internal_api;
use url::Url;

use super::{Snapshot, SnapshotRef};
use crate::actions::{ADD_FIELD, ADD_NAME, CDC_FIELD, CDC_NAME, REMOVE_FIELD, REMOVE_NAME};
use crate::commit_range::CommitRange;
use crate::expressions::{Expression as Expr, Predicate, UnaryExpressionOp, column_name, lit};
use crate::history_manager::error::LogHistoryError;
use crate::history_manager::search::Bound;
use crate::history_manager::{HistoryCommitType, timestamp_to_version};
use crate::plans::PlanBuilder;
use crate::plans::ir::nodes::{Agg, ScanFile};
use crate::plans::ir::plan::Plan;
use crate::scan::StatsOptions;
use crate::schema::{DataType, SchemaRef, StructField, StructType, lazy_schema_ref};
use crate::table_features::Operation as TableOperation;
use crate::{Engine, KernelError, KernelResult, LogPath, Result, Scalar, Version, version_as_i64};

const DEFAULT_DELETED_FILE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const PATH: &str = "path";
const LOCATION: &str = "location";
const RELATIVE_PATH: &str = "relative_path";
const LAST_MODIFIED: &str = "last_modified";
const VERSION: &str = "version";
const DELETION_VECTOR: &str = "deletionVector";
const DELTA_LOG_PREFIX: &str = "_delta_log/";
const DELTA_LOG_PREFIX_END: &str = "_delta_log0";

static PATH_INFO_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable LOCATION: STRING,
    nullable RELATIVE_PATH: STRING,
};

static REACHABLE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null LOCATION: STRING,
    nullable RELATIVE_PATH: STRING,
};

static CANDIDATE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null LOCATION: STRING,
    nullable RELATIVE_PATH: STRING,
};

static DATA_FILES_TO_DELETE_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null PATH: STRING,
};

/// Candidate discovery mode for [`Snapshot::get_data_files_to_delete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DataFilesToDeleteMode {
    /// Recursively list the table root and retain old, unprotected files.
    Full,
    /// Derive candidates from log commits after `anchor_version` and no newer than the retention
    /// cutoff. `None` means that no LITE anchor has been recorded yet.
    Lite { anchor_version: Option<Version> },
}

/// Inputs to [`Snapshot::get_data_files_to_delete`] that are not part of the table snapshot.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DataFilesToDeleteOptions {
    now_ms: i64,
    deleted_file_retention: Option<Duration>,
    mode: DataFilesToDeleteMode,
}

impl DataFilesToDeleteOptions {
    /// Options for a cleanup plan using the caller's clock and candidate source.
    pub fn new(now_ms: i64, mode: DataFilesToDeleteMode) -> Self {
        Self {
            now_ms,
            deleted_file_retention: None,
            mode,
        }
    }

    /// Override `delta.deletedFileRetentionDuration` for this cleanup plan.
    pub fn with_deleted_file_retention(mut self, retention: Duration) -> Self {
        self.deleted_file_retention = Some(retention);
        self
    }
}

impl Snapshot {
    /// Builds a plan of absolute file URLs that Delta-level VACUUM may delete.
    ///
    /// FULL lists the table root and protects recent or reachable files. LITE derives candidates
    /// from Remove, deletion-vector, and CDC actions after the supplied anchor version. The caller
    /// supplies the clock; Kernel never deletes files or tracks live transactions. FULL's mtime
    /// cutoff protects uncommitted files under the assumption that transactions do not outlive the
    /// retention period. LITE may return absolute paths outside the table root; it does not
    /// classify shallow-clone references. A present plan has one non-null STRING `path` column;
    /// `None` denotes an empty LITE candidate range. Connectors must apply their own hidden-path,
    /// table-kind, clone, and other policy filters before deleting the returned paths.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine cannot plan the required metadata scans, if the table has an
    /// unsupported writer feature, if adaptive metadata prevents a complete reachable-file set, or
    /// if required log history is unavailable.
    #[tracing::instrument(
        name = "snapshot.get_data_files_to_delete",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub fn get_data_files_to_delete(
        self: &SnapshotRef,
        engine: &dyn Engine,
        options: &DataFilesToDeleteOptions,
    ) -> Result<Option<Plan>> {
        ensure_cleanup_supported(self)?;
        let retention = options
            .deleted_file_retention
            .or(self.table_properties().deleted_file_retention_duration)
            .unwrap_or(DEFAULT_DELETED_FILE_RETENTION);
        let retention_ms = i64::try_from(retention.as_millis()).map_err(|_| {
            KernelError::generic(format!("deleted file retention {retention:?} is too large"))
        })?;
        let cutoff_ms = options.now_ms.saturating_sub(retention_ms);
        let cut_version = resolve_cut_version(self, engine, cutoff_ms)?;
        let candidates = match options.mode {
            DataFilesToDeleteMode::Full => full_candidates(self.table_root().clone(), cutoff_ms)?,
            DataFilesToDeleteMode::Lite { anchor_version } => {
                lite_candidates(self, engine, anchor_version, cut_version)?
            }
        };
        let reachable = self.get_reachable_data_files(engine, cut_version)?;
        let table_relative = candidates
            .clone()
            .filter(Expr::column([RELATIVE_PATH]).is_not_null())?
            .anti_join(
                reachable.clone(),
                [column_name!(LOCATION)],
                [column_name!(LOCATION)],
            )?
            .anti_join(
                reachable.clone(),
                [column_name!(RELATIVE_PATH)],
                [column_name!(RELATIVE_PATH)],
            )?;
        let external = candidates
            .filter(Expr::column([RELATIVE_PATH]).is_null())?
            .anti_join(
                reachable,
                [column_name!(LOCATION)],
                [column_name!(LOCATION)],
            )?;
        let eligible = PlanBuilder::union_all([table_relative, external])?;
        let distinct = eligible.aggregate_by([column_name!(LOCATION)], |aggs| {
            aggs.aggregate_as(Agg::count_star(), "candidate_count")
        })?;
        let output = distinct.project(
            Expr::struct_from([Expr::column([LOCATION])]),
            DATA_FILES_TO_DELETE_SCHEMA.clone(),
        )?;
        Ok(output.build_opt()?)
    }

    /// Builds this snapshot's reachable-file paths as a composable plan relation.
    ///
    /// Paths are absolute URLs. This internal API accepts one snapshot at a time and has no
    /// shallow-clone semantics; connectors can combine its result with other protected-path sets.
    #[internal_api]
    #[cfg_attr(not(feature = "internal-api"), allow(dead_code))]
    #[tracing::instrument(
        name = "snapshot.get_reachable_data_files",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(crate) fn get_reachable_data_files(
        self: &SnapshotRef,
        engine: &dyn Engine,
        cut_version: Option<Version>,
    ) -> Result<PlanBuilder> {
        ensure_cleanup_supported(self)?;
        if let Some(version) = cut_version.filter(|version| *version > self.version()) {
            return Err(KernelError::generic(format!(
                "retention cut version {version} is newer than snapshot version {}",
                self.version()
            ))
            .into());
        }
        let table_root = self.table_root();
        let scan = Arc::clone(self)
            .scan_builder()
            .with_stats(StatsOptions::none())
            .build()?;
        let live_paths = match scan.declarative_metadata_scan_plan_builder(engine)? {
            Some(live_adds) => PlanBuilder::union_all([
                file_paths_from_action(live_adds.clone(), table_root, ADD_NAME, PATH)?,
                deletion_vector_paths_from_action(live_adds, table_root, ADD_NAME)?,
            ])?,
            None => empty_reachable_plan()?,
        };

        let first_commit = cut_version.unwrap_or(0);
        let commit_actions = commit_actions_plan(self, engine, first_commit, self.version())?;
        let add_dvs = filter_commit_actions_from_version(commit_actions.clone(), cut_version)?
            .filter(Expr::column([ADD_NAME, DELETION_VECTOR]).is_not_null())?;
        let cdc_actions = filter_commit_actions_from_version(commit_actions.clone(), cut_version)?;
        let remove_actions = match cut_version {
            Some(cut_version) => match cut_version.checked_add(1) {
                Some(first_remove) => {
                    filter_commit_actions_from_version(commit_actions, Some(first_remove))?
                }
                None => empty_commit_actions_plan()?,
            },
            None => commit_actions,
        };

        PlanBuilder::union_all([
            live_paths,
            deletion_vector_paths_from_action(add_dvs, table_root, ADD_NAME)?,
            file_paths_from_action(remove_actions.clone(), table_root, REMOVE_NAME, PATH)?,
            deletion_vector_paths_from_action(remove_actions, table_root, REMOVE_NAME)?,
            file_paths_from_action(cdc_actions, table_root, CDC_NAME, PATH)?,
        ])
        .map_err(Into::into)
    }
}

fn ensure_cleanup_supported(snapshot: &SnapshotRef) -> Result<()> {
    snapshot
        .table_configuration()
        .ensure_operation_supported(TableOperation::Write)?;
    if snapshot.has_adaptive_metadata() {
        return Err(KernelError::unsupported(
            "data-file cleanup plans do not support adaptive-metadata tables",
        )
        .into());
    }
    Ok(())
}

fn resolve_cut_version(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    cutoff_ms: i64,
) -> Result<Option<Version>> {
    match timestamp_to_version(
        snapshot,
        engine,
        cutoff_ms,
        Bound::GreatestLower,
        HistoryCommitType::Published,
    ) {
        Ok(commit) => Ok(Some(commit.version)),
        Err(LogHistoryError::TimestampOutOfRange { .. }) => Ok(None),
        Err(error) => Err(KernelError::LogHistory(Box::new(error)).into()),
    }
}

fn full_candidates(base_url: Url, cutoff_ms: i64) -> KernelResult<PlanBuilder> {
    let relative_path = Expr::column([RELATIVE_PATH]);
    let in_delta_log = Predicate::and(
        relative_path.clone().ge(lit(DELTA_LOG_PREFIX)),
        relative_path.lt(lit(DELTA_LOG_PREFIX_END)),
    );
    PlanBuilder::list_files(base_url)?
        .filter(Predicate::and(
            Expr::column([LAST_MODIFIED]).lt(lit(cutoff_ms)),
            Predicate::not(in_delta_log),
        ))?
        .project(
            Expr::struct_from([Expr::column([LOCATION]), Expr::column([RELATIVE_PATH])]),
            CANDIDATE_SCHEMA.clone(),
        )
}

fn lite_candidates(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    anchor_version: Option<Version>,
    cut_version: Option<Version>,
) -> Result<PlanBuilder> {
    let Some(cut_version) = cut_version else {
        return empty_candidate_plan().map_err(Into::into);
    };
    let start_version = match anchor_version {
        Some(anchor) if anchor >= cut_version => return empty_candidate_plan().map_err(Into::into),
        Some(anchor) => match anchor.checked_add(1) {
            Some(version) => version,
            None => return empty_candidate_plan().map_err(Into::into),
        },
        None => 0,
    };
    if start_version > cut_version {
        return empty_candidate_plan().map_err(Into::into);
    }

    let commits = commit_actions_plan(snapshot, engine, start_version, cut_version)?;
    let root = snapshot.table_root();
    let remove_paths = file_paths_from_action(commits.clone(), root, REMOVE_NAME, PATH)?;
    let remove_dvs = deletion_vector_paths_from_action(commits.clone(), root, REMOVE_NAME)?;
    let cdc_paths = file_paths_from_action(commits, root, CDC_NAME, PATH)?;
    let relative_path = Expr::column([RELATIVE_PATH]);
    let in_delta_log = Predicate::and(
        relative_path.clone().ge(lit(DELTA_LOG_PREFIX)),
        relative_path.clone().lt(lit(DELTA_LOG_PREFIX_END)),
    );
    let not_in_delta_log = Predicate::or(
        relative_path.clone().is_null(),
        Predicate::not(in_delta_log),
    );
    let candidates = path_info_candidates(PlanBuilder::union_all([
        remove_paths,
        remove_dvs,
        cdc_paths,
    ])?)?
    .filter(not_in_delta_log)?;
    deduplicate_candidates(candidates).map_err(Into::into)
}

fn empty_candidate_plan() -> KernelResult<PlanBuilder> {
    PlanBuilder::values(CANDIDATE_SCHEMA.clone(), vec![])
}

fn project_action_field(field: &StructField, names: &[&str]) -> KernelResult<StructField> {
    let DataType::Struct(schema) = field.data_type() else {
        return Err(KernelError::schema(format!(
            "{} action field must contain a struct",
            field.name()
        )));
    };
    Ok(StructField::nullable(
        field.name(),
        schema.project_as_struct(names)?,
    ))
}

fn commit_read_schema() -> KernelResult<SchemaRef> {
    Ok(Arc::new(StructType::try_new([
        project_action_field(&ADD_FIELD, &[PATH, DELETION_VECTOR])?,
        project_action_field(&REMOVE_FIELD, &[PATH, DELETION_VECTOR])?,
        project_action_field(&CDC_FIELD, &[PATH])?,
        StructField::not_null(VERSION, DataType::LONG),
    ])?))
}

fn commit_actions_plan(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    start_version: Version,
    end_version: Version,
) -> Result<PlanBuilder> {
    let schema = commit_read_schema()?;
    if start_version > end_version {
        return Ok(PlanBuilder::values(schema, vec![])?);
    }

    let mut builder = CommitRange::builder_for(snapshot.table_root().as_str(), start_version)
        .with_end_version(end_version);
    if snapshot.table_configuration().is_catalog_managed() {
        let log_tail = snapshot
            .log_segment()
            .listed
            .staged_commits()
            .filter(|path| path.version <= snapshot.version())
            .map(|path| LogPath::try_new(path.location.clone()))
            .collect::<KernelResult<Vec<_>>>()?;
        builder = builder.with_log_tail(log_tail, snapshot.version());
    }
    let commit_range = builder.build(engine)?;
    let files = commit_range
        .commit_files()?
        .map(|path| {
            let path = path?;
            Ok(ScanFile {
                meta: path.location.clone(),
                file_constants: vec![Scalar::Long(path.version_as_i64()?)],
            })
        })
        .collect::<KernelResult<Vec<_>>>()?;
    Ok(PlanBuilder::scan_json(files, &[VERSION], schema)?)
}

fn filter_commit_actions_from_version(
    plan: PlanBuilder,
    first_version_inclusive: Option<Version>,
) -> Result<PlanBuilder> {
    let Some(version) = first_version_inclusive else {
        return Ok(plan);
    };
    let version = version_as_i64(version)?;
    Ok(plan.filter(Expr::column([VERSION]).ge(lit(version)))?)
}

fn empty_commit_actions_plan() -> Result<PlanBuilder> {
    Ok(PlanBuilder::values(commit_read_schema()?, vec![])?)
}

fn empty_reachable_plan() -> Result<PlanBuilder> {
    Ok(PlanBuilder::values(REACHABLE_SCHEMA.clone(), vec![])?)
}

fn deduplicate_candidates(plan: PlanBuilder) -> KernelResult<PlanBuilder> {
    plan.aggregate_by(
        [column_name!(LOCATION), column_name!(RELATIVE_PATH)],
        |aggregates| aggregates.aggregate_as(Agg::count_star(), "candidate_count"),
    )?
    .project(
        Expr::struct_from([Expr::column([LOCATION]), Expr::column([RELATIVE_PATH])]),
        CANDIDATE_SCHEMA.clone(),
    )
}

fn file_paths_from_action(
    plan: PlanBuilder,
    table_root: &Url,
    action: &str,
    path_field: &str,
) -> KernelResult<PlanBuilder> {
    let path = Expr::column([action, path_field]);
    let path_info = resolve_file_path_expression(table_root, path.clone());
    plan.filter(path.is_not_null())?
        .project(path_info, PATH_INFO_SCHEMA.clone())?
        .filter(Expr::column([LOCATION]).is_not_null())?
        .project(
            Expr::struct_from([Expr::column([LOCATION]), Expr::column([RELATIVE_PATH])]),
            REACHABLE_SCHEMA.clone(),
        )
}

fn deletion_vector_paths_from_action(
    plan: PlanBuilder,
    table_root: &Url,
    action: &str,
) -> KernelResult<PlanBuilder> {
    let descriptor = Expr::column([action, DELETION_VECTOR]);
    let path_info = deletion_vector_path_expression(table_root, descriptor.clone());
    plan.filter(descriptor.is_not_null())?
        .project(path_info, PATH_INFO_SCHEMA.clone())?
        .filter(Expr::column([LOCATION]).is_not_null())?
        .project(
            Expr::struct_from([Expr::column([LOCATION]), Expr::column([RELATIVE_PATH])]),
            REACHABLE_SCHEMA.clone(),
        )
}

fn resolve_file_path_expression(table_root: &Url, path: Expr) -> Expr {
    Expr::unary(
        UnaryExpressionOp::ResolveFilePath,
        Expr::struct_from([lit(table_root.as_str()), path]),
    )
}

fn deletion_vector_path_expression(table_root: &Url, descriptor: Expr) -> Expr {
    Expr::unary(
        UnaryExpressionOp::DeletionVectorPath,
        Expr::struct_from([lit(table_root.as_str()), descriptor]),
    )
}

fn path_info_candidates(plan: PlanBuilder) -> KernelResult<PlanBuilder> {
    plan.filter(Expr::column([LOCATION]).is_not_null())?
        .project(
            Expr::struct_from([Expr::column([LOCATION]), Expr::column([RELATIVE_PATH])]),
            CANDIDATE_SCHEMA.clone(),
        )
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use test_utils::table_builder::{FeatureSet, LogState, TestTableBuilder};
    use test_utils::{CATALOG_MANAGED_METADATA, add_commit, add_staged_commit};

    use super::*;
    use crate::arrow::array::{Array as _, StringArray};
    use crate::engine::sync::SyncEngine;
    use crate::plans::ir::nodes::Operator;
    use crate::unit_test_utils::plans::execute_plan;

    fn execute_paths(engine: &dyn Engine, plan: Option<Plan>) -> KernelResult<Vec<String>> {
        let Some(plan) = plan else {
            return Ok(Vec::new());
        };
        let mut paths = Vec::new();
        for batch in execute_plan(engine, plan)? {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| KernelError::schema("cleanup plan output must be a string path"))?;
            for row in 0..values.len() {
                if !values.is_null(row) {
                    paths.push(values.value(row).to_owned());
                }
            }
        }
        Ok(paths)
    }

    fn future_now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_millis() as i64
            + 60_000
    }

    #[test]
    fn full_candidates_obey_mtime_cutoff_and_protect_live_files() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_data(1, 1)
            .build()
            .expect("test table build should succeed");
        let engine = SyncEngine::new_with_store(table.store().clone());
        let orphan =
            Url::parse("memory:///orphan%20file.parquet").expect("orphan URL should be valid");
        engine
            .storage_handler()
            .put(&orphan, Bytes::from_static(b"orphan"), false)?;
        let orphan_mtime = engine.storage_handler().head(&orphan)?.last_modified;
        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let retention = Duration::ZERO;

        let at_cutoff = DataFilesToDeleteOptions::new(orphan_mtime, DataFilesToDeleteMode::Full)
            .with_deleted_file_retention(retention);
        assert!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &at_cutoff)?,
            )?
            .is_empty()
        );

        let after_cutoff = DataFilesToDeleteOptions::new(
            orphan_mtime.saturating_add(1),
            DataFilesToDeleteMode::Full,
        )
        .with_deleted_file_retention(retention);
        assert_eq!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &after_cutoff)?,
            )?,
            vec![orphan.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn lite_candidates_use_remove_actions_after_anchor() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .build()
            .expect("test table build should succeed");
        let store = table.store().clone();
        let commit = serde_json::json!({
            "remove": {
                "path": "old%20file.parquet",
                "partitionValues": {},
                "size": 1,
                "modificationTime": 0,
                "dataChange": true
            }
        })
        .to_string();
        add_commit(table.table_root(), store.as_ref(), 1, commit.to_owned())
            .await
            .expect("remove commit should be written");
        let engine = SyncEngine::new_with_store(store);
        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let now_ms = future_now_ms();
        let options = DataFilesToDeleteOptions::new(
            now_ms,
            DataFilesToDeleteMode::Lite {
                anchor_version: None,
            },
        )
        .with_deleted_file_retention(Duration::ZERO);

        assert_eq!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &options)?,
            )?,
            vec!["memory:///old%20file.parquet".to_owned()]
        );

        let anchored = DataFilesToDeleteOptions::new(
            now_ms,
            DataFilesToDeleteMode::Lite {
                anchor_version: Some(1),
            },
        )
        .with_deleted_file_retention(Duration::ZERO);
        assert!(
            snapshot
                .get_data_files_to_delete(&engine, &anchored)?
                .is_none()
        );
        let future_anchor = DataFilesToDeleteOptions::new(
            now_ms,
            DataFilesToDeleteMode::Lite {
                anchor_version: Some(2),
            },
        )
        .with_deleted_file_retention(Duration::ZERO);
        assert!(
            snapshot
                .get_data_files_to_delete(&engine, &future_anchor)?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn full_protects_window_remove_dvs_and_cdc() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .with_features(
                FeatureSet::new()
                    .deletion_vectors()
                    .change_data_feed()
                    .ict(),
            )
            .build()
            .expect("test table build should succeed");
        let store = table.store().clone();
        let engine = SyncEngine::new_with_store(store.clone());
        let root = Url::parse(table.table_root()).expect("table root should be valid");
        let dv_add_path = "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";
        let dv_live_path = "ef/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";
        let dv_remove_path = "cd/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";
        for path in [
            "data-2.parquet",
            "live-dv-data.parquet",
            dv_add_path,
            dv_live_path,
            dv_remove_path,
            "cdc-3.parquet",
            "orphan.parquet",
        ] {
            let url = root.join(path).expect("test file URL should be valid");
            engine
                .storage_handler()
                .put(&url, Bytes::from_static(b"test file"), false)?;
        }

        let now_ms = future_now_ms();
        let add_dv = serde_json::json!({
            "storageType": "u",
            "pathOrInlineDv": "ab^-aqEH.-t@S}K{vb[*k^",
            "offset": 0,
            "sizeInBytes": 4,
            "cardinality": 1
        });
        let live_dv = serde_json::json!({
            "storageType": "u",
            "pathOrInlineDv": "ef^-aqEH.-t@S}K{vb[*k^",
            "offset": 0,
            "sizeInBytes": 4,
            "cardinality": 1
        });
        let remove_dv = serde_json::json!({
            "storageType": "u",
            "pathOrInlineDv": "cd^-aqEH.-t@S}K{vb[*k^",
            "offset": 0,
            "sizeInBytes": 4,
            "cardinality": 1
        });
        let add_actions = [
            serde_json::json!({
                "commitInfo": {
                    "timestamp": now_ms.saturating_add(3_600_000),
                    "inCommitTimestamp": now_ms.saturating_add(3_600_000)
                }
            })
            .to_string(),
            serde_json::json!({
                "add": {
                    "path": "data-2.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true,
                    "deletionVector": add_dv
                }
            })
            .to_string(),
            serde_json::json!({
                "add": {
                    "path": "live-dv-data.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true,
                    "deletionVector": live_dv
                }
            })
            .to_string(),
        ]
        .join("\n");
        add_commit(table.table_root(), store.as_ref(), 2, add_actions)
            .await
            .expect("add-with-DV commit should be written");
        let remove_commit = [
            serde_json::json!({
                "commitInfo": {
                    "timestamp": now_ms.saturating_add(7_200_000),
                    "inCommitTimestamp": now_ms.saturating_add(7_200_000)
                }
            })
            .to_string(),
            serde_json::json!({
                "remove": {
                    "path": "data-2.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true,
                    "deletionTimestamp": now_ms,
                    "extendedFileMetadata": true,
                    "deletionVector": remove_dv
                }
            })
            .to_string(),
            serde_json::json!({"cdc": {"path": "cdc-3.parquet"}}).to_string(),
        ]
        .join("\n");
        add_commit(table.table_root(), store.as_ref(), 3, remove_commit)
            .await
            .expect("remove-and-CDC commit should be written");

        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let options = DataFilesToDeleteOptions::new(now_ms, DataFilesToDeleteMode::Full)
            .with_deleted_file_retention(Duration::ZERO);
        assert_eq!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &options)?,
            )?,
            vec!["memory:///orphan.parquet".to_owned()]
        );
        Ok(())
    }

    #[test]
    fn full_protects_live_adds_replayed_from_v2_sidecars() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .with_log_state(
                LogState::with_latest_version(2)
                    .with_checkpoint_at([1])
                    .with_sidecars_if_enabled(Some(1)),
            )
            .with_features(FeatureSet::new().v2_checkpoint())
            .build()
            .expect("V2 sidecar table build should succeed");
        let engine = SyncEngine::new_with_store(table.store().clone());
        let orphan = Url::parse("memory:///orphan.parquet").expect("orphan URL should be valid");
        engine
            .storage_handler()
            .put(&orphan, Bytes::from_static(b"orphan"), false)?;
        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let options = DataFilesToDeleteOptions::new(future_now_ms(), DataFilesToDeleteMode::Full)
            .with_deleted_file_retention(Duration::ZERO);

        assert_eq!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &options)?,
            )?,
            vec![orphan.to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn full_protects_readded_paths_but_not_removes_at_the_cut() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .with_log_state(LogState::with_latest_version(1))
            .build()
            .expect("test table build should succeed");
        let store = table.store().clone();
        let engine = SyncEngine::new_with_store(store.clone());
        let root = Url::parse(table.table_root()).expect("table root should be valid");
        for path in ["reuse.parquet", "removed-old.parquet", "orphan.parquet"] {
            let url = root.join(path).expect("test file URL should be valid");
            engine
                .storage_handler()
                .put(&url, Bytes::from_static(b"test file"), false)?;
        }

        let remove_commit = [
            serde_json::json!({
                "remove": {
                    "path": "reuse.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
            serde_json::json!({
                "remove": {
                    "path": "removed-old.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
        ]
        .join("\n");
        add_commit(table.table_root(), store.as_ref(), 2, remove_commit)
            .await
            .expect("remove commit should be written");
        add_commit(
            table.table_root(),
            store.as_ref(),
            3,
            serde_json::json!({"commitInfo": {"timestamp": 0}}).to_string(),
        )
        .await
        .expect("empty commit-info version should be written");
        add_commit(
            table.table_root(),
            store.as_ref(),
            4,
            serde_json::json!({
                "add": {
                    "path": "reuse.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
        )
        .await
        .expect("re-add commit should be written");

        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let options = DataFilesToDeleteOptions::new(future_now_ms(), DataFilesToDeleteMode::Full)
            .with_deleted_file_retention(Duration::ZERO);
        let mut actual = execute_paths(
            &engine,
            snapshot.get_data_files_to_delete(&engine, &options)?,
        )?;
        actual.sort();
        let mut expected = vec![
            "memory:///orphan.parquet".to_owned(),
            "memory:///removed-old.parquet".to_owned(),
        ];
        expected.sort();
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn lite_candidates_include_remove_dv_and_cdc_paths() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .with_features(
                FeatureSet::new()
                    .deletion_vectors()
                    .change_data_feed()
                    .ict(),
            )
            .build()
            .expect("test table build should succeed");
        let store = table.store().clone();
        let engine = SyncEngine::new_with_store(store.clone());
        let now_ms = future_now_ms();
        let version_one_timestamp = now_ms.saturating_sub(10_000);
        let version_two_timestamp = now_ms.saturating_sub(5_000);
        let remove_dv = serde_json::json!({
            "storageType": "u",
            "pathOrInlineDv": "ab^-aqEH.-t@S}K{vb[*k^",
            "offset": 0,
            "sizeInBytes": 4,
            "cardinality": 1
        });
        let first_commit = [
            serde_json::json!({
                "commitInfo": {
                    "timestamp": version_one_timestamp,
                    "inCommitTimestamp": version_one_timestamp
                }
            })
            .to_string(),
            serde_json::json!({
                "remove": {
                    "path": "old%20file.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true,
                    "deletionVector": remove_dv
                }
            })
            .to_string(),
            serde_json::json!({
                "remove": {
                    "path": "s3://external/data.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
            serde_json::json!({
                "remove": {
                    "path": "s3://external/live.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
            serde_json::json!({"cdc": {"path": "old-cdc.parquet"}}).to_string(),
        ]
        .join("\n");
        add_commit(table.table_root(), store.as_ref(), 1, first_commit)
            .await
            .expect("first LITE commit should be written");
        let second_commit = [
            serde_json::json!({
                "commitInfo": {
                    "timestamp": version_two_timestamp,
                    "inCommitTimestamp": version_two_timestamp
                }
            })
            .to_string(),
            serde_json::json!({
                "add": {
                    "path": "s3://external/live.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
        ]
        .join("\n");
        add_commit(table.table_root(), store.as_ref(), 2, second_commit)
            .await
            .expect("second LITE commit should be written");

        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let options = DataFilesToDeleteOptions::new(
            now_ms,
            DataFilesToDeleteMode::Lite {
                anchor_version: None,
            },
        )
        .with_deleted_file_retention(Duration::ZERO);
        let plan = snapshot
            .get_data_files_to_delete(&engine, &options)?
            .expect("LITE has a nonempty commit range");
        assert!(
            !plan
                .nodes
                .iter()
                .any(|node| matches!(&node.op, Operator::ListFiles(_)))
        );
        let mut paths = execute_paths(&engine, Some(plan))?;
        paths.sort();
        let mut expected = vec![
            "memory:///ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_owned(),
            "memory:///old%20file.parquet".to_owned(),
            "memory:///old-cdc.parquet".to_owned(),
            "s3://external/data.parquet".to_owned(),
        ];
        expected.sort();
        assert_eq!(paths, expected);

        let after_version_one = DataFilesToDeleteOptions::new(
            now_ms,
            DataFilesToDeleteMode::Lite {
                anchor_version: Some(1),
            },
        )
        .with_deleted_file_retention(Duration::ZERO);
        assert!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &after_version_one)?,
            )?
            .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn lite_includes_catalog_ratified_staged_commit_paths() -> crate::Result<()> {
        let table_root = Url::parse("memory:///").expect("table root should be valid");
        let store: Arc<crate::object_store::DynObjectStore> =
            Arc::new(crate::object_store::memory::InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        add_commit(
            table_root.as_str(),
            store.as_ref(),
            0,
            CATALOG_MANAGED_METADATA.to_owned(),
        )
        .await
        .expect("catalog-managed create commit should be written");

        let now_ms = future_now_ms();
        let staged_commit = [
            serde_json::json!({
                "commitInfo": {
                    "timestamp": now_ms.saturating_sub(1_000),
                    "inCommitTimestamp": now_ms.saturating_sub(1_000),
                    "txnId": "test-txn-1"
                }
            })
            .to_string(),
            serde_json::json!({
                "remove": {
                    "path": "staged-removed.parquet",
                    "partitionValues": {},
                    "size": 1,
                    "modificationTime": 0,
                    "dataChange": true
                }
            })
            .to_string(),
        ]
        .join("\n");
        let staged_path = add_staged_commit(table_root.as_str(), store.as_ref(), 1, staged_commit)
            .await
            .expect("ratified staged commit should be written");
        let staged_url = table_root
            .join(staged_path.as_ref())
            .expect("staged commit URL should be valid");
        let staged_log_path = LogPath::try_new(engine.storage_handler().head(&staged_url)?)?;
        let snapshot = Snapshot::builder_for(table_root.as_str())
            .with_log_tail(vec![staged_log_path])
            .with_max_catalog_version(1)
            .build(&engine)?;
        let options = DataFilesToDeleteOptions::new(
            now_ms,
            DataFilesToDeleteMode::Lite {
                anchor_version: None,
            },
        )
        .with_deleted_file_retention(Duration::ZERO);

        assert_eq!(
            execute_paths(
                &engine,
                snapshot.get_data_files_to_delete(&engine, &options)?,
            )?,
            vec!["memory:///staged-removed.parquet".to_owned()]
        );
        Ok(())
    }

    #[test]
    fn lite_fails_closed_when_anchor_history_is_missing() -> crate::Result<()> {
        let table = TestTableBuilder::new()
            .with_log_state(
                LogState::with_latest_version(2)
                    .with_checkpoint_at([1])
                    .with_cleanup_commits_before(1),
            )
            .build()
            .expect("test table build should succeed");
        let engine = SyncEngine::new_with_store(table.store().clone());
        let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
        let options = DataFilesToDeleteOptions::new(
            future_now_ms(),
            DataFilesToDeleteMode::Lite {
                anchor_version: None,
            },
        )
        .with_deleted_file_retention(Duration::ZERO);

        assert!(
            snapshot
                .get_data_files_to_delete(&engine, &options)
                .is_err()
        );
        Ok(())
    }
}
