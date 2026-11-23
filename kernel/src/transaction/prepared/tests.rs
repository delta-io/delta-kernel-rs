use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rstest::rstest;
use url::Url;

use super::*;
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::arrow::array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use crate::checkpoint::CheckpointSpec;
use crate::committer::{
    CommitMetadata, Committer, FileSystemCommitter, PublishMetadata, WinningCommits,
    write_commit_json_declarative,
};
use crate::crc::{Crc, DomainMetadataState};
use crate::engine::arrow_conversion::TryFromArrow;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::metrics::{CommitFailureReason, MetricEvent, TableType};
use crate::object_store::memory::InMemory;
use crate::plans::PlanExecutor as _;
use crate::scan::StatsOptions;
use crate::schema::{StructField, ToSchema};
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::unit_test_utils::{
    CapturingReporter, add_files_with_partition_values, create_valid_add_file_batch,
    install_thread_local_metrics_reporter, nullable_add_file, nullable_add_files,
    set_field_as_null,
};
use crate::{FileMeta, FilteredEngineData, KernelResultIterator, Result, Snapshot};

struct ObservedCommitter {
    history_reads: Arc<AtomicUsize>,
    commit_calls: Arc<AtomicUsize>,
    history_failures: AtomicUsize,
    reject_first_attempt: AtomicBool,
    lose_success_response: Option<Arc<Mutex<Option<ObservedSubmission>>>>,
    write_successor: bool,
    reported_version: Option<Version>,
}

struct ObservedSubmission {
    actions: Vec<RelationRef>,
    version: Version,
    timestamp: i64,
}

impl Committer for ObservedCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        let path = metadata.published_commit_path()?;
        match engine.json_handler().write_json_file(&path, actions, false) {
            Ok(size) => Ok(CommitResponse::Committed {
                file_meta: FileMeta::new(path, metadata.in_commit_timestamp(), size),
            }),
            Err(KernelError::FileAlreadyExists(_)) => Ok(CommitResponse::Conflict {
                version: metadata.version(),
            }),
            Err(error) => Err(error),
        }
    }
    fn commit_declarative(
        &self,
        executor: &dyn ScopedPlanExecutor,
        actions: Box<dyn Iterator<Item = RelationRef> + '_>,
        metadata: CommitMetadata,
    ) -> KernelResult<CommitResponse> {
        self.commit_calls.fetch_add(1, Ordering::Relaxed);
        if self.reject_first_attempt.swap(false, Ordering::Relaxed) {
            return Ok(CommitResponse::Retryable {
                error: KernelError::IOError(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "Commit temporarily rejected before writing",
                )),
            });
        }
        let successor_path = metadata
            .table_root()
            .join(&format!("_delta_log/{:020}.json", metadata.version() + 1))?;
        let actions: Vec<_> = actions.collect();
        if let Some(observed) = &self.lose_success_response {
            *observed.lock().unwrap() = Some(ObservedSubmission {
                actions: actions.clone(),
                version: metadata.version(),
                timestamp: metadata.in_commit_timestamp(),
            });
        }
        let result = match write_commit_json_declarative(
            executor,
            Box::new(actions.into_iter()),
            metadata.published_commit_path()?,
        ) {
            Ok(mut file_meta) => {
                if let Some(version) = self.reported_version {
                    file_meta.location = metadata
                        .table_root()
                        .join(&format!("_delta_log/{version:020}.json"))?;
                }
                CommitResponse::Committed { file_meta }
            }
            Err(KernelError::FileAlreadyExists(_)) => CommitResponse::Conflict {
                version: metadata.version(),
            },
            Err(error) => return Err(error),
        };
        if self.write_successor && matches!(result, CommitResponse::Committed { .. }) {
            executor
                .execute_op(Operation::IoOperation(
                    crate::plans::IoOperation::write_bytes(
                        successor_path,
                        bytes::Bytes::from_static(b"{\"commitInfo\":{\"timestamp\":10}}\n"),
                        false,
                    ),
                ))?
                .into_unit()?;
        }
        if self.lose_success_response.is_some()
            && matches!(result, CommitResponse::Committed { .. })
        {
            return Err(KernelError::IOError(std::io::Error::other(
                "Response lost after the write succeeded",
            )));
        }
        Ok(result)
    }
    fn get_winning_commits(
        &self,
        engine: &dyn Engine,
        table_root: &Url,
        conflict_version: Version,
    ) -> KernelResult<WinningCommits> {
        self.history_reads.fetch_add(1, Ordering::Relaxed);
        if self
            .history_failures
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(KernelError::IOError(std::io::Error::other(
                "History listing unavailable",
            )));
        }
        FileSystemCommitter::new().get_winning_commits(engine, table_root, conflict_version)
    }
    fn is_catalog_committer(&self) -> bool {
        false
    }
    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> KernelResult<()> {
        FileSystemCommitter::new().publish(engine, metadata)
    }
}

fn table() -> KernelResult<(Arc<InMemory>, SyncEngine, SnapshotRef)> {
    table_with_partitioning(None)
}

fn table_with_partitioning(
    column_mapping_mode: Option<&str>,
) -> KernelResult<(Arc<InMemory>, SyncEngine, SnapshotRef)> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let mut builder = create_table(
        "memory:///table/",
        if column_mapping_mode.is_some() {
            schema_ref! { nullable "id": INTEGER, nullable "p1": STRING, nullable "p2": STRING }
        } else {
            schema_ref! { nullable "id": INTEGER }
        },
        "test",
        true,
    );
    if let Some(mode) = column_mapping_mode {
        builder = builder
            .with_data_layout(DataLayout::partitioned(["p1", "p2"]))
            .with_table_properties([("delta.columnMapping.mode", mode)]);
    }
    let snapshot = builder
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    Ok((store, engine, snapshot))
}

#[rstest]
#[case::unpartitioned(None)]
#[case::partitioned(Some("none"))]
#[case::partitioned_name_mapping(Some("name"))]
#[case::partitioned_id_mapping(Some("id"))]
fn prepare_append_commit_produces_the_exact_post_commit_snapshot(
    #[case] column_mapping_mode: Option<&str>,
    #[values(StatsOptions::json_only(), StatsOptions::all_struct())] stats: StatsOptions,
) -> Result<()> {
    let (store, engine, snapshot) = table_with_partitioning(column_mapping_mode)?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let mut txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let partition_columns: Vec<_> = snapshot
        .table_configuration()
        .physical_partition_columns()
        .collect();
    let batch = if let [p1, p2] = partition_columns.as_slice() {
        add_files_with_partition_values(
            &["dummy", "nulls"],
            &[
                &[(p2, Some("2026")), (p1, Some("US"))],
                &[(p1, None), (p2, Some(""))],
            ],
        )
    } else {
        create_valid_add_file_batch(false)
    };
    let schema = Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
    let input = executor.create_ref("adds", schema, batch)?;
    txn.add_files_ref(input);
    let prepared = txn.prepare_declarative(&executor)?;
    assert_eq!(prepared.target_version(), 1);
    assert_eq!(prepared.read_snapshot().unwrap().version(), 0);
    let result = prepared.commit(&engine, &executor)?;
    assert!(result.get_prepared_transaction().is_none());
    assert!(result.get_winning_commits(&engine)?.is_none());
    let committed = result.unwrap_committed();
    assert_eq!(committed.commit_version(), 1);
    assert_eq!(committed.post_commit_snapshot().unwrap().version(), 1);
    let loaded = crate::Snapshot::builder_for("memory:///table/").build(&engine)?;
    assert_eq!(loaded.version(), 1);
    assert_eq!(
        loaded.table_configuration().metadata().id(),
        snapshot.table_configuration().metadata().id()
    );
    let scan = loaded.clone().scan_builder().with_stats(stats).build()?;
    let metadata = scan
        .scan_metadata(&engine)?
        .collect::<KernelResult<Vec<_>>>()?;
    let mut files = Vec::new();
    for batch in &metadata {
        files = batch.visit_scan_files(files, |files, file| {
            files.push((file.path, file.partition_values))
        })?;
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(files[0].0, "dummy");
    if let [p1, p2] = partition_columns.as_slice() {
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].1.get(p1).unwrap(), "US");
        assert_eq!(files[0].1.get(p2).unwrap(), "2026");
        assert_eq!(files[1].0, "nulls");
    } else {
        assert_eq!(files.len(), 1);
        assert!(files[0].1.is_empty());
    }
    let mut remove = loaded
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    for batch in metadata {
        let (data, selection) = batch.scan_files.into_parts();
        let batch = data.try_into_record_batch()?;
        let batch = crate::arrow::compute::filter_record_batch(
            &batch,
            &crate::arrow::array::BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| selection.get(row).copied().unwrap_or(true))
                    .collect::<Vec<_>>(),
            ),
        )
        .map_err(KernelError::from)?;
        let schema =
            Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
        remove.remove_files_ref(executor.create_ref("removes", schema, batch)?);
    }
    let removed = remove
        .prepare_declarative(&executor)?
        .commit(&engine, &executor)?
        .unwrap_committed();
    let snapshot = removed.post_commit_snapshot().unwrap();
    assert_eq!(snapshot.version(), 2);
    let scan = snapshot.clone().scan_builder().build()?;
    let mut paths = Vec::new();
    for batch in scan.scan_metadata(&engine)? {
        paths = batch?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
    }
    assert!(paths.is_empty());
    Ok(())
}

#[test]
fn declarative_create_preserves_typed_actions_and_application_transaction() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let txn = create_table(
        "memory:///created/",
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .with_table_properties([
        ("delta.feature.domainMetadata", "supported"),
        ("delta.enableInCommitTimestamps", "true"),
    ])
    .with_domain_metadata("example", "{}")
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .with_transaction_id("app".to_string(), 7);
    let prepared = txn.prepare_declarative(&executor)?;
    assert_eq!(prepared.target_version(), 0);
    assert_eq!(prepared.prepared_domain_metadata_actions.len(), 1);
    let committed = prepared.commit(&engine, &executor)?.unwrap_committed();
    let snapshot = committed.post_commit_snapshot().unwrap();
    assert_eq!(snapshot.version(), 0);
    assert_eq!(
        snapshot.reader().get_app_id_version("app", &engine)?,
        Some(7)
    );
    let loaded = crate::Snapshot::builder_for("memory:///created/").build(&engine)?;
    assert_eq!(loaded.schema(), snapshot.schema());
    assert_eq!(
        loaded.reader().get_domain_metadata("example", &engine)?,
        Some("{}".to_string())
    );
    assert_eq!(
        snapshot.reader().get_domain_metadata("example", &engine)?,
        Some("{}".to_string())
    );
    assert!(loaded.reader().get_in_commit_timestamp(&engine)?.unwrap() > 0);
    assert_eq!(loaded.reader().get_app_id_version("app", &engine)?, Some(7));
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn prepare_reads_domain_removals_through_the_scope(
    #[values(
        None,
        Some(CheckpointSpec::V1),
        Some(CheckpointSpec::V2(crate::checkpoint::V2CheckpointConfig::NoSidecar))
    )]
    checkpoint: Option<CheckpointSpec>,
    #[values(
        "none",
        "stale_complete",
        "stale_partial",
        "current_complete",
        "current_partial"
    )]
    cache: &str,
) -> Result<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let root = "memory:///domain-removals/";
    let mut builder = create_table(root, schema_ref! { nullable "id": INTEGER }, "test", true)
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .with_domain_metadata("active", "old")
        .with_domain_metadata("gone", "before-removal");
    if matches!(checkpoint, Some(CheckpointSpec::V2(_))) {
        builder = builder.with_table_properties([("delta.feature.v2Checkpoint", "supported")]);
    }
    let initial = builder
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    if checkpoint.is_some() {
        initial.checkpoint(&engine, checkpoint.as_ref())?;
    }
    initial
        .update_table(UpdateTableOperation::Write, "update-domains", false)
        .with_domain_metadata("active", "new")
        .with_domain_metadata_removed("gone")
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
    let loaded = Snapshot::builder_for(root).build(&engine)?;
    let crc = if cache == "none" {
        None
    } else {
        let stale = cache.starts_with("stale");
        let domains = if stale {
            vec![
                DomainMetadata::new("active".into(), "old".into()),
                DomainMetadata::new("gone".into(), "before-removal".into()),
            ]
        } else {
            vec![DomainMetadata::new("active".into(), "new".into())]
        };
        Some(Arc::new(Crc {
            version: if stale { 0 } else { loaded.version() },
            domain_metadata_state: if cache.ends_with("complete") {
                DomainMetadataState::try_complete(domains)?
            } else {
                DomainMetadataState::try_partial(domains)?
            },
            ..Default::default()
        }))
    };
    let snapshot = Arc::new(Snapshot::new_with_crc(
        loaded.log_segment().clone(),
        loaded.table_configuration().clone(),
        crc,
        false,
        false,
    )?);
    // An authoritative current cache must work without access to any table files.
    let read_store = if cache == "current_complete" {
        Arc::new(InMemory::new())
    } else {
        store
    };
    let executor = SyncPlanExecutor::new(Some(read_store)).get_scoped();
    let txn = snapshot
        .update_table(UpdateTableOperation::Write, "remove-domains", false)
        .with_execution_mode::<Declarative>()
        .with_domain_metadata_removed("active")
        .with_domain_metadata_removed("gone")
        .with_domain_metadata_removed("missing")
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let prepared = txn.prepare_declarative(&executor)?;
    assert_eq!(
        prepared.prepared_domain_metadata_actions,
        vec![DomainMetadata::remove("active".into(), "new".into())]
    );
    Ok(())
}

#[rstest]
#[case::same(8, true)]
#[case::higher(9, true)]
#[case::lower(7, false)]
fn prepare_checks_row_tracking_high_water_mark_through_the_scope(
    #[case] provided: i64,
    #[case] succeeds: bool,
) -> Result<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let root = "memory:///row-tracking-metadata/";
    let initial = create_table(root, schema_ref! { nullable "id": INTEGER }, "test", true)
        .with_table_properties([("delta.enableRowTracking", "true")])
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    initial
        .update_table(
            UpdateTableOperation::Write,
            "advance-high-water-mark",
            false,
        )
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .with_row_tracking_high_water_mark(8)?
        .commit(&engine)?
        .unwrap_committed();
    let snapshot = Snapshot::builder_for(root).build(&engine)?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", false)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .with_row_tracking_high_water_mark(provided)?;
    let result = txn.prepare_declarative(&executor);
    if succeeds {
        let prepared = result?;
        assert_eq!(prepared.prepared_domain_metadata_actions.len(), 1);
        assert_eq!(
            crate::row_tracking::parse_row_tracking_high_water_mark(
                prepared.prepared_domain_metadata_actions[0].configuration(),
            )?,
            provided
        );
    } else {
        test_utils::assert_result_error_with_message(result, "cannot be less than");
    }
    Ok(())
}

#[rstest]
#[case::row_tracking_enablement_absent("row tracking", None)]
#[case::row_tracking_disabled("row tracking", Some("false"))]
#[case::row_tracking_enabled("row tracking", Some("true"))]
#[case::clustered("clustered file writes", None)]
fn unsupported_file_writes_fail_before_submission(
    #[case] feature: &str,
    #[case] row_tracking_enablement: Option<&str>,
) -> Result<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let root = "memory:///unsupported-writes/";
    let mut builder = create_table(root, schema_ref! { nullable "id": INTEGER }, "test", true);
    if feature == "row tracking" {
        builder = builder.with_table_properties([
            ("delta.feature.rowTracking", "supported"),
            ("delta.feature.domainMetadata", "supported"),
        ]);
        if let Some(enabled) = row_tracking_enablement {
            builder = builder.with_table_properties([("delta.enableRowTracking", enabled)]);
        }
    } else {
        builder = builder.with_data_layout(DataLayout::clustered(["id"]));
    }
    let snapshot = builder
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    if feature == "row tracking" {
        assert!(
            snapshot
                .table_configuration()
                .is_feature_supported(&TableFeature::RowTracking)
        );
        assert_eq!(
            snapshot
                .table_configuration()
                .is_feature_enabled(&TableFeature::RowTracking),
            row_tracking_enablement == Some("true")
        );
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(
            &engine,
            Box::new(ObservedCommitter {
                history_reads: Arc::new(AtomicUsize::new(0)),
                commit_calls: calls.clone(),
                history_failures: AtomicUsize::new(0),
                reject_first_attempt: AtomicBool::new(false),
                lose_success_response: None,
                write_successor: false,
                reported_version: None,
            }),
        )?;
    let batch = create_valid_add_file_batch(false);
    let stats = batch
        .column_by_name("stats")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let mut columns = stats.columns().to_vec();
    let num_records = stats
        .fields()
        .iter()
        .position(|field| field.name() == "numRecords")
        .unwrap();
    columns[num_records] = Arc::new(Int64Array::from(vec![1i64]));
    let batch = test_utils::replace_column(
        &batch,
        "stats",
        Arc::new(StructArray::new(stats.fields().clone(), columns, None)),
    );
    let schema = Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
    txn.add_files_ref(executor.create_ref("adds", schema, batch)?);
    let error = txn
        .prepare_declarative(&executor)
        .and_then(|prepared| prepared.commit(&engine, &executor))
        .unwrap_err();
    assert!(matches!(&error, KernelError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains(feature), "{error}");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(Snapshot::builder_for(root).build(&engine)?.version(), 0);
    Ok(())
}

#[cfg(feature = "adaptive-metadata-in-dev")]
#[test]
fn adaptive_metadata_preparation_is_unsupported_without_file_inputs() -> Result<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let txn = create_table(
        "memory:///adaptive-create/",
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .with_table_properties([("delta.enableIcebergNativeV4", "true")])
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    assert!(txn.state.is_adaptive_metadata_enabled());

    let error = txn.prepare_declarative(&executor).unwrap_err();
    assert!(matches!(&error, KernelError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("adaptive metadata"), "{error}");
    Ok(())
}

#[rstest]
fn connector_commit_info_is_rejected_without_writing(
    #[values(false, true)] create: bool,
) -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let calls = Arc::new(AtomicUsize::new(0));
    let committer = Box::new(ObservedCommitter {
        history_reads: Arc::new(AtomicUsize::new(0)),
        commit_calls: calls.clone(),
        history_failures: AtomicUsize::new(0),
        reject_first_attempt: AtomicBool::new(false),
        lose_success_response: None,
        write_successor: false,
        reported_version: None,
    });
    let schema = schema_ref! { nullable "sourceJobId": STRING };
    let commit_info = Box::new(ArrowEngineData::new(
        RecordBatch::try_from_iter([(
            "sourceJobId",
            Arc::new(StringArray::from(vec!["connector-job"])) as ArrayRef,
        )])
        .map_err(KernelError::from)?,
    ));
    let result = if create {
        create_table(
            "memory:///created/",
            schema_ref! { nullable "id": INTEGER },
            "test",
            true,
        )
        .with_commit_info(commit_info, schema)
        .with_execution_mode::<Declarative>()
        .build(&engine, committer)
        .and_then(|txn| txn.prepare_declarative(&executor))
        .and_then(|prepared| prepared.commit(&engine, &executor))
        .map(|_| ())
    } else {
        snapshot
            .update_table(UpdateTableOperation::Write, "test", true)
            .with_commit_info(commit_info, schema)
            .with_execution_mode::<Declarative>()
            .build(&engine, committer)
            .and_then(|txn| txn.prepare_declarative(&executor))
            .and_then(|prepared| prepared.commit(&engine, &executor))
            .map(|_| ())
    };
    let error = result.unwrap_err();
    assert!(matches!(&error, KernelError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("CommitInfo"), "{error}");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        0
    );
    assert!(
        Snapshot::builder_for("memory:///created/")
            .build(&engine)
            .is_err()
    );
    Ok(())
}

#[rstest]
fn append_only_rejects_only_nonempty_data_changing_removals(
    #[values(false, true)] append_only: bool,
    #[values(false, true)] data_change: bool,
    #[values(false, true)] empty: bool,
) -> Result<()> {
    let store = Arc::new(InMemory::new());
    let engine = SyncEngine::new_with_store(store.clone());
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let root = "memory:///append-only/";
    let mut initial = create_table(root, schema_ref! { nullable "id": INTEGER }, "test", true)
        .with_table_properties([(
            "delta.appendOnly",
            if append_only { "true" } else { "false" },
        )])
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    initial.add_files(Box::new(ArrowEngineData::new(create_valid_add_file_batch(
        false,
    ))));
    let snapshot = initial.commit(&engine)?.unwrap_post_commit_snapshot();
    let scan = snapshot.clone().scan_builder().build()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", data_change)
        .with_execution_mode::<Declarative>()
        .build(
            &engine,
            Box::new(ObservedCommitter {
                history_reads: Arc::new(AtomicUsize::new(0)),
                commit_calls: calls.clone(),
                history_failures: AtomicUsize::new(0),
                reject_first_attempt: AtomicBool::new(false),
                lose_success_response: None,
                write_successor: false,
                reported_version: None,
            }),
        )?;
    let mut removal_count = 0;
    for metadata in scan.scan_metadata(&engine)? {
        let (data, selection) = metadata?.scan_files.into_parts();
        let batch = data.try_into_record_batch()?;
        let selected = BooleanArray::from(
            (0..batch.num_rows())
                .map(|row| !empty && selection.get(row).copied().unwrap_or(true))
                .collect::<Vec<_>>(),
        );
        let batch = crate::arrow::compute::filter_record_batch(&batch, &selected)
            .map_err(KernelError::from)?;
        removal_count += batch.num_rows();
        let schema =
            Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
        txn.remove_files_ref(executor.create_ref("removes", schema, batch)?);
    }
    assert_eq!(removal_count, usize::from(!empty));
    let blocked = append_only && data_change && !empty;
    let result = txn
        .prepare_declarative(&executor)
        .and_then(|prepared| prepared.commit(&engine, &executor));
    if blocked {
        let error = result.unwrap_err();
        assert!(
            matches!(&error, KernelError::InvalidTransactionState(_)),
            "{error}"
        );
        assert!(error.to_string().contains("Append-only"), "{error}");
    } else {
        let committed = result?.unwrap_committed();
        assert_eq!(committed.commit_version(), 1);
        assert_eq!(committed.post_commit_snapshot().unwrap().version(), 1);
    }
    assert_eq!(calls.load(Ordering::Relaxed), usize::from(!blocked));
    let loaded = Snapshot::builder_for(root).build(&engine)?;
    assert_eq!(loaded.version(), u64::from(!blocked));
    let mut paths = Vec::new();
    for metadata in loaded.scan_builder().build()?.scan_metadata(&engine)? {
        paths = metadata?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
    }
    assert_eq!(
        paths,
        if empty || blocked {
            vec!["dummy".to_string()]
        } else {
            vec![]
        }
    );
    Ok(())
}

#[rstest]
fn staged_deletion_vector_inputs_are_unsupported_even_without_matching_files(
    #[values(false, true)] empty: bool,
) -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(
            &engine,
            Box::new(ObservedCommitter {
                history_reads: Arc::new(AtomicUsize::new(0)),
                commit_calls: calls.clone(),
                history_failures: AtomicUsize::new(0),
                reject_first_attempt: AtomicBool::new(false),
                lose_success_response: None,
                write_successor: false,
                reported_version: None,
            }),
        )?;
    let dv = DeletionVectorDescriptor::try_new(
        DeletionVectorStorageType::PersistedAbsolute,
        "file:///unused-dv.bin",
        Some(0),
        4,
        1,
    )?;
    let schema = schema_ref! {
        not_null "path": STRING,
        not_null "deletionVector": (DeletionVectorDescriptor::to_schema()),
    };
    let dvs = executor.execute_and_retain(
        "dvs",
        PlanBuilder::values(
            schema,
            if empty {
                vec![]
            } else {
                vec![vec!["dummy".into(), dv.into()]]
            },
        )?
        .build()?,
    )?;
    let files = executor.execute_and_retain(
        "files",
        PlanBuilder::values(crate::scan::scan_row_schema(), vec![])?.build()?,
    )?;
    txn.update_deletion_vectors_ref(dvs, files);
    let error = txn
        .prepare_declarative(&executor)
        .and_then(|prepared| prepared.commit(&engine, &executor))
        .unwrap_err();
    assert!(matches!(&error, KernelError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("DV updates"), "{error}");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        0
    );
    Ok(())
}

#[rstest]
#[case::committed(None)]
#[case::conflict(Some(CommitFailureReason::Conflict))]
#[case::retryable(Some(CommitFailureReason::RetryableIo))]
#[case::unknown(Some(CommitFailureReason::Unknown))]
fn prepared_submissions_report_outcomes_and_finalize_removal_timestamps(
    #[case] expected_failure: Option<CommitFailureReason>,
) -> Result<()> {
    let tempdir = tempfile::tempdir().map_err(KernelError::from)?;
    let root = Url::from_directory_path(tempdir.path()).unwrap();
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::new(None).get_scoped();
    let mut initial = create_table(
        root.as_str(),
        schema_ref! { nullable "id": INTEGER },
        "test",
        true,
    )
    .with_table_properties([("delta.feature.domainMetadata", "supported")])
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    initial.add_files(Box::new(ArrowEngineData::new(nullable_add_files(&[
        "removed", "kept",
    ]))));
    let snapshot = initial.commit(&engine)?.unwrap_post_commit_snapshot();
    if expected_failure == Some(CommitFailureReason::Conflict) {
        snapshot
            .clone()
            .update_table(UpdateTableOperation::Write, "winner", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_committed();
    }
    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let mut txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .add_column(StructField::nullable("extra", DataType::INTEGER))
        .with_domain_metadata("example", "updated")
        .with_transaction_id("app", 7)
        .with_correlation_id("prepared-submission")
        .with_execution_mode::<Declarative>()
        .build(
            &engine,
            Box::new(ObservedCommitter {
                history_reads: reads.clone(),
                commit_calls: calls.clone(),
                history_failures: AtomicUsize::new(0),
                reject_first_attempt: AtomicBool::new(
                    expected_failure == Some(CommitFailureReason::RetryableIo),
                ),
                lose_success_response: (expected_failure == Some(CommitFailureReason::Unknown))
                    .then(|| Arc::new(Mutex::new(None))),
                write_successor: false,
                reported_version: None,
            }),
        )?;
    let batch = nullable_add_files(&["added", "also-added"]);
    let schema = Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
    txn.add_files_ref(executor.create_ref("adds", schema, batch)?);
    let scan = snapshot.scan_builder().build()?;
    let mut selected_removals = 0;
    for metadata in scan.scan_metadata(&engine)? {
        let (data, selection) = metadata?.scan_files.into_parts();
        let batch = data.try_into_record_batch()?;
        let paths = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let selected = BooleanArray::from(
            (0..batch.num_rows())
                .map(|row| {
                    selection.get(row).copied().unwrap_or(true) && paths.value(row) == "removed"
                })
                .collect::<Vec<_>>(),
        );
        let batch = crate::arrow::compute::filter_record_batch(&batch, &selected)
            .map_err(KernelError::from)?;
        selected_removals += batch.num_rows();
        let schema =
            Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
        txn.remove_files_ref(executor.create_ref("removes", schema, batch)?);
    }
    assert_eq!(selected_removals, 1);
    let operation_id = txn.state.operation_id;
    txn.state.commit_timestamp = 1;
    let prepared = txn.prepare_declarative(&executor)?;
    assert_eq!(prepared.state.operation_id, operation_id);
    let mut submission_start = current_time_ms()?;
    let result = prepared.commit(&engine, &executor)?;
    assert!(match expected_failure {
        None => matches!(&result, PreparedTransactionCommitResult::Committed(_)),
        Some(CommitFailureReason::Conflict) =>
            matches!(&result, PreparedTransactionCommitResult::Conflicted { .. }),
        Some(CommitFailureReason::RetryableIo) =>
            matches!(&result, PreparedTransactionCommitResult::Retryable { .. }),
        Some(CommitFailureReason::Unknown) =>
            matches!(&result, PreparedTransactionCommitResult::Unknown { .. }),
        Some(reason) => panic!("Unexpected failure reason: {reason:?}"),
    });
    let commit_events: Vec<_> = reporter
        .events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                MetricEvent::TransactionCommitSuccess(_) | MetricEvent::TransactionCommitFailure(_)
            )
        })
        .collect();
    assert_eq!(commit_events.len(), 1);
    if let Some(reason) = expected_failure {
        let MetricEvent::TransactionCommitFailure(failure) = &commit_events[0] else {
            panic!("Unconfirmed submission emitted a success metric");
        };
        assert_eq!(failure.reason, reason);
        assert_eq!(failure.operation_id, operation_id);
        assert_eq!(
            failure.correlation_id.as_deref(),
            Some("prepared-submission")
        );
        assert_eq!(failure.table_type, TableType::PathBased);
        assert_eq!(
            result
                .get_prepared_transaction()
                .unwrap()
                .state
                .operation_id,
            operation_id
        );
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    if expected_failure == Some(CommitFailureReason::Conflict) {
        assert_eq!(
            Snapshot::builder_for(root.as_str())
                .build(&engine)?
                .version(),
            1
        );
        return Ok(());
    }
    if expected_failure == Some(CommitFailureReason::Unknown) {
        let pending = result.get_prepared_transaction().unwrap();
        for relation in pending.unresolved_attempt().unwrap().actions() {
            for data in executor
                .execute_op(Operation::QueryPlan(
                    PlanBuilder::relation_source(relation.clone()).build()?,
                ))?
                .into_data()?
            {
                let batch = data?.try_into_record_batch()?;
                for row in 0..batch.num_rows() {
                    assert_eq!(
                        batch
                            .columns()
                            .iter()
                            .filter(|column| !column.is_null(row))
                            .count(),
                        1,
                        "Each submitted row must contain exactly one non-null action"
                    );
                }
            }
        }
        assert_eq!(
            Snapshot::builder_for(root.as_str())
                .build(&engine)?
                .version(),
            1
        );
        return Ok(());
    }
    let committed = if expected_failure == Some(CommitFailureReason::RetryableIo) {
        assert_eq!(
            Snapshot::builder_for(root.as_str())
                .build(&engine)?
                .version(),
            0
        );
        let pending = result.into_prepared_transaction().unwrap();
        assert_eq!(pending.target_version(), 1);
        submission_start = current_time_ms()?;
        pending.commit(&engine, &executor)?.unwrap_committed()
    } else {
        result.unwrap_committed()
    };
    let submission_end = current_time_ms()?;
    assert_eq!(committed.commit_version(), 1);
    let successes: Vec<_> = reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::TransactionCommitSuccess(success) => Some(success),
            _ => None,
        })
        .collect();
    assert_eq!(successes.len(), 1);
    let success = &successes[0];
    assert_eq!(success.operation_id, operation_id);
    assert_eq!(
        success.correlation_id.as_deref(),
        Some("prepared-submission")
    );
    assert_eq!(success.table_type, TableType::PathBased);
    assert_eq!(success.commit_version, 1);
    assert_eq!(success.num_add_files, 2);
    assert_eq!(success.num_remove_files, 1);
    assert_eq!(success.add_files_bytes, 2);
    assert_eq!(success.remove_files_bytes, 1);
    assert_eq!(success.num_dv_updates, 0);
    assert!(success.data_change);
    assert!(!success.is_blind_append);
    assert_eq!(success.operation.as_deref(), Some("WRITE"));
    assert_eq!(
        calls.load(Ordering::Relaxed),
        if expected_failure.is_some() { 2 } else { 1 }
    );
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    let removals =
        test_utils::read_actions_from_commit(&root, committed.commit_version(), "remove")
            .map_err(KernelError::generic_err)?;
    assert_eq!(removals.len(), 1);
    let timestamp = removals[0]["deletionTimestamp"].as_i64().unwrap();
    assert!(
        (submission_start..=submission_end).contains(&timestamp),
        "{timestamp}"
    );
    let loaded = Snapshot::builder_for(root.as_str()).build(&engine)?;
    assert_eq!(loaded.version(), 1);
    assert!(loaded.schema().contains("extra"));
    assert_eq!(loaded.reader().get_app_id_version("app", &engine)?, Some(7));
    assert_eq!(
        loaded.reader().get_domain_metadata("example", &engine)?,
        Some("updated".to_string())
    );
    let mut paths = Vec::new();
    for metadata in loaded.scan_builder().build()?.scan_metadata(&engine)? {
        paths = metadata?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
    }
    paths.sort();
    assert_eq!(paths, ["added", "also-added", "kept"]);
    Ok(())
}

#[rstest]
#[case::history_available(0)]
#[case::history_read_fails_once(1)]
fn conflict_discovery_fetches_on_each_call_and_returns_independently_owned_history(
    #[case] history_failures: usize,
    #[values(false, true)] discard_pending: bool,
) -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let committer = ObservedCommitter {
        history_reads: reads.clone(),
        commit_calls: calls.clone(),
        history_failures: AtomicUsize::new(history_failures),
        reject_first_attempt: AtomicBool::new(false),
        lose_success_response: None,
        write_successor: false,
        reported_version: None,
    };
    let txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(committer))?;
    let prepared = txn.prepare_declarative(&executor)?;
    let actions = prepared.get_delta_actions().clone();
    let mut latest = snapshot;
    for _ in 0..3 {
        latest = latest
            .update_table(UpdateTableOperation::Write, "winner", true)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_post_commit_snapshot();
    }
    let mut result = prepared.commit(&engine, &executor)?;
    assert!(matches!(
        &result,
        PreparedTransactionCommitResult::Conflicted {
            conflict_version: 1,
            ..
        }
    ));
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        result.get_prepared_transaction().unwrap().target_version(),
        1
    );
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    if history_failures > 0 {
        let error = result.get_winning_commits(&engine).unwrap_err();
        assert!(error.to_string().contains("History listing unavailable"));
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let pending = result.get_prepared_transaction().unwrap();
        assert_eq!(pending.target_version(), 1);
        assert_eq!(pending.get_delta_actions().id(), actions.id());
        assert!(pending.unresolved_attempt().is_none());
        assert_eq!(reads.load(Ordering::Relaxed), 1);
    }
    let winners = result.get_winning_commits(&engine)?.unwrap();
    assert_eq!(
        winners
            .iter()
            .map(|commit| commit.version)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let paths = winners
        .iter()
        .map(|commit| commit.log_path.clone())
        .collect::<Vec<_>>();
    assert_eq!(reads.load(Ordering::Relaxed), history_failures + 1);
    latest
        .update_table(UpdateTableOperation::Write, "later-winner", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
    let refreshed = result.get_winning_commits(&engine)?.unwrap();
    assert_eq!(
        refreshed
            .iter()
            .map(|commit| commit.version)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(reads.load(Ordering::Relaxed), history_failures + 2);
    let mut pending = result.take_prepared_transaction().unwrap();
    assert!(result.get_prepared_transaction().is_none());
    assert!(result.take_prepared_transaction().is_none());
    if discard_pending {
        drop(pending);
    } else {
        assert!(matches!(
            pending.rebase(&winners),
            Err(KernelError::Unsupported(_))
        ));
        assert_eq!(pending.target_version(), 1);
        assert_eq!(pending.read_snapshot().unwrap().version(), 0);
        assert_eq!(pending.get_delta_actions().id(), actions.id());
    }
    assert!(matches!(
        result.get_winning_commits(&engine),
        Err(KernelError::InvalidTransactionState(_))
    ));
    assert_eq!(
        winners
            .iter()
            .map(|commit| commit.log_path.clone())
            .collect::<Vec<_>>(),
        paths
    );
    assert_eq!(winners.len(), 3);
    assert_eq!(reads.load(Ordering::Relaxed), history_failures + 2);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[rstest]
fn conflict_discovery_requires_pending_transaction(
    #[values(false, true)] fail_before_extraction: bool,
) -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let prepared = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(
            &engine,
            Box::new(ObservedCommitter {
                history_reads: reads.clone(),
                commit_calls: calls.clone(),
                history_failures: AtomicUsize::new(1),
                reject_first_attempt: AtomicBool::new(false),
                lose_success_response: None,
                write_successor: false,
                reported_version: None,
            }),
        )?
        .prepare_declarative(&executor)?;
    snapshot
        .update_table(UpdateTableOperation::Write, "winner", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
    let mut result = prepared.commit(&engine, &executor)?;
    if fail_before_extraction {
        assert!(result.get_winning_commits(&engine).is_err());
    }
    drop(result.take_prepared_transaction().unwrap());
    assert!(matches!(
        result.get_winning_commits(&engine),
        Err(KernelError::InvalidTransactionState(_))
    ));
    assert_eq!(
        reads.load(Ordering::Relaxed),
        usize::from(fail_before_extraction)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn retryable_rejection_preserves_actions_and_retries_the_same_version_without_history() -> Result<()>
{
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let committer = ObservedCommitter {
        history_reads: reads.clone(),
        commit_calls: calls.clone(),
        history_failures: AtomicUsize::new(0),
        reject_first_attempt: AtomicBool::new(true),
        lose_success_response: None,
        write_successor: false,
        reported_version: None,
    };
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(committer))?;
    txn.add_files_ref(executor.create_ref(
        "adds",
        txn.add_files_schema().clone(),
        create_valid_add_file_batch(false),
    )?);
    let prepared = txn.prepare_declarative(&executor)?;
    let actions = prepared.get_delta_actions().clone();
    let result = prepared.commit(&engine, &executor)?;
    assert!(matches!(
        &result,
        PreparedTransactionCommitResult::Retryable { error: KernelError::IOError(error), .. }
            if error.kind() == std::io::ErrorKind::WouldBlock
    ));
    assert!(!result.is_committed());
    assert!(result.get_winning_commits(&engine)?.is_none());
    let pending = result.get_prepared_transaction().unwrap();
    assert_eq!(pending.target_version(), 1);
    assert_eq!(pending.read_snapshot().unwrap().version(), 0);
    assert_eq!(pending.get_delta_actions().id(), actions.id());
    assert!(pending.unresolved_attempt().is_none());
    assert_eq!(
        crate::Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        0
    );
    let success = result
        .into_prepared_transaction()
        .unwrap()
        .commit(&engine, &executor)?;
    assert!(success.get_winning_commits(&engine)?.is_none());
    let committed = success.unwrap_committed();
    assert_eq!(committed.commit_version(), 1);
    assert_eq!(committed.post_commit_snapshot().unwrap().version(), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn uncertain_success_preserves_exact_attempt_without_discovery_or_blind_resubmission() -> Result<()>
{
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let submitted = Arc::new(Mutex::new(None));
    let committer = ObservedCommitter {
        history_reads: reads.clone(),
        commit_calls: calls.clone(),
        history_failures: AtomicUsize::new(0),
        reject_first_attempt: AtomicBool::new(false),
        lose_success_response: Some(submitted.clone()),
        write_successor: false,
        reported_version: None,
    };
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(committer))?;
    txn.add_files_ref(executor.create_ref(
        "adds",
        txn.add_files_schema().clone(),
        create_valid_add_file_batch(false),
    )?);
    let prepared = txn.prepare_declarative(&executor)?;
    let result = prepared.commit(&engine, &executor)?;
    assert!(matches!(
        &result,
        PreparedTransactionCommitResult::Unknown { .. }
    ));
    assert!(result.get_winning_commits(&engine)?.is_none());
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    let pending = result.into_prepared_transaction().unwrap();
    let attempt = pending.unresolved_attempt().unwrap();
    let submitted = submitted.lock().unwrap().take().unwrap();
    let signature = |relations: &[RelationRef]| {
        relations
            .iter()
            .map(|relation| (relation.id().to_string(), relation.schema().clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(attempt.version(), 1);
    assert_eq!(attempt.version(), submitted.version);
    assert_eq!(attempt.actions().len(), 2);
    assert_eq!(signature(attempt.actions()), signature(&submitted.actions));
    assert_eq!(attempt.timestamp(), submitted.timestamp);
    assert_eq!(attempt.in_commit_timestamp(), None);
    assert_eq!(
        crate::Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        1
    );
    assert!(pending.commit(&engine, &executor).is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[rstest]
#[case::missing_key("missing")]
#[case::wrong_key_same_count("wrong")]
#[case::extra_key("extra")]
#[case::duplicate_key_missing_another("duplicate_missing")]
#[case::duplicate_key_with_all_expected_keys("duplicate_extra")]
#[case::wrong_case("case")]
#[case::null_map("null")]
#[case::logical_keys_in_mapped_table("logical")]
fn partition_key_validation_fails_before_writing(
    #[case] invalid: &str,
    #[values("none", "name", "id")] column_mapping_mode: &str,
    #[values(false, true)] skip_dedup_validation: bool,
) -> Result<()> {
    let (store, engine, snapshot) = table_with_partitioning(Some(column_mapping_mode))?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let keys: Vec<_> = snapshot
        .table_configuration()
        .physical_partition_columns()
        .collect();
    let wrong_case = keys[0].to_uppercase();
    let entries = match invalid {
        "missing" => vec![(keys[0].as_str(), Some("US"))],
        "wrong" => vec![(keys[0].as_str(), Some("US")), ("unexpected", None)],
        "extra" => vec![
            (keys[0].as_str(), Some("US")),
            (keys[1].as_str(), None),
            ("unexpected", None),
        ],
        "duplicate_missing" => vec![(keys[0].as_str(), None), (keys[0].as_str(), None)],
        "duplicate_extra" => vec![
            (keys[0].as_str(), None),
            (keys[1].as_str(), None),
            (keys[0].as_str(), None),
        ],
        "case" => vec![(wrong_case.as_str(), None), (keys[1].as_str(), None)],
        "logical" if column_mapping_mode != "none" => vec![("p1", None), ("p2", None)],
        "logical" => vec![("P1", None), ("P2", None)],
        "null" => vec![],
        _ => unreachable!(),
    };
    let batch = add_files_with_partition_values(&["a"], &[&entries]);
    let batch = if invalid == "null" {
        set_field_as_null(&batch, "partitionValues", 0)
    } else {
        batch
    };
    let schema = Arc::new(StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?);
    let mut builder = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>();
    if skip_dedup_validation {
        builder = builder.without_dedup_validation();
    }
    let mut txn = builder.build(&engine, Box::new(FileSystemCommitter::new()))?;
    txn.add_files_ref(executor.create_ref("adds", schema, batch)?);
    let error = txn
        .prepare_declarative(&executor)
        .and_then(|prepared| prepared.commit(&engine, &executor))
        .unwrap_err();
    let expected = if invalid == "null" {
        "partitionValues"
    } else {
        "Invalid AddFile metadata"
    };
    assert!(error.to_string().contains(expected), "{error}");
    assert_eq!(
        crate::Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        0
    );
    Ok(())
}

#[rstest]
#[case::duplicate_paths("duplicate")]
#[case::negative_size("negative")]
#[case::empty_path("empty")]
#[case::null_size("null_size")]
#[case::wrong_partition_keys("partition")]
fn file_action_validation_fails_before_submitting(#[case] invalid: &str) -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let calls = Arc::new(AtomicUsize::new(0));
    let committer = ObservedCommitter {
        history_reads: Arc::new(AtomicUsize::new(0)),
        commit_calls: calls.clone(),
        history_failures: AtomicUsize::new(0),
        reject_first_attempt: AtomicBool::new(false),
        lose_success_response: None,
        write_successor: false,
        reported_version: None,
    };
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(committer))?;
    let batch = match invalid {
        "empty" => nullable_add_file(""),
        "null_size" => set_field_as_null(&nullable_add_file("a"), "size", 0),
        "partition" => add_files_with_partition_values(&["a"], &[&[("unexpected", Some("1"))]]),
        "negative" => test_utils::replace_column(
            &nullable_add_file("a"),
            "size",
            Arc::new(crate::arrow::array::Int64Array::from(vec![-1i64])),
        ),
        _ => nullable_add_file("a"),
    };
    let schema = StructType::try_from_arrow(batch.schema()).map_err(KernelError::from)?;
    let input = executor.create_ref("input", Arc::new(schema), batch)?;
    txn.add_files_ref(input.clone());
    if invalid == "duplicate" {
        txn.add_files_ref(input);
    }
    let result = txn
        .prepare_declarative(&executor)
        .and_then(|prepared| prepared.commit(&engine, &executor));
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        crate::Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        0
    );
    Ok(())
}

#[rstest]
fn imperative_conflict_discovers_history_lazily_and_retries_failed_reads(
    #[values(0, 1)] failures: usize,
) -> Result<()> {
    let (_store, engine, snapshot) = table()?;
    let reads = Arc::new(AtomicUsize::new(0));
    let txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .build(
            &engine,
            Box::new(ObservedCommitter {
                history_reads: reads.clone(),
                commit_calls: Arc::new(AtomicUsize::new(0)),
                history_failures: AtomicUsize::new(failures),
                reject_first_attempt: AtomicBool::new(false),
                lose_success_response: None,
                write_successor: false,
                reported_version: None,
            }),
        )?;
    let winner = snapshot
        .update_table(UpdateTableOperation::Write, "winner", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_post_commit_snapshot();
    let super::super::CommitResult::Conflicted(result) = txn.commit(&engine)? else {
        panic!("Expected version conflict");
    };
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    if failures > 0 {
        let error = result.get_winning_commits(&engine).unwrap_err();
        assert!(error.to_string().contains("History listing unavailable"));
        assert_eq!(reads.load(Ordering::Relaxed), 1);
    }
    let history = result.get_winning_commits(&engine)?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].version, 1);
    winner
        .update_table(UpdateTableOperation::Write, "later", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
    let refreshed = result.get_winning_commits(&engine)?;
    assert_eq!(refreshed.len(), 2);
    assert_eq!(history.len(), 1);
    assert_eq!(reads.load(Ordering::Relaxed), failures + 2);
    Ok(())
}

#[rstest]
#[case::accepted_version(None)]
#[case::invalid_reported_version(Some(2))]
fn post_commit_snapshot_stays_at_committed_version_or_propagates_construction_error(
    #[case] reported_version: Option<Version>,
) -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let committer = ObservedCommitter {
        history_reads: Arc::new(AtomicUsize::new(0)),
        commit_calls: Arc::new(AtomicUsize::new(0)),
        history_failures: AtomicUsize::new(0),
        reject_first_attempt: AtomicBool::new(false),
        lose_success_response: None,
        write_successor: true,
        reported_version,
    };
    let prepared = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(committer))?
        .prepare_declarative(&executor)?;
    let result = prepared.commit(&engine, &executor);
    if reported_version.is_some() {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Log file version (2)"));
        assert!(error.to_string().contains("Snapshot version (0) + 1"));
    } else {
        let committed = result?.unwrap_committed();
        assert_eq!(committed.commit_version(), 1);
        assert_eq!(committed.post_commit_snapshot().unwrap().version(), 1);
    }
    assert_eq!(
        crate::Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        2
    );
    Ok(())
}

#[test]
fn preparation_moves_shared_context_and_preserves_original_input_relations() -> Result<()> {
    let (store, engine, snapshot) = table()?;
    let executor = SyncPlanExecutor::new(Some(store)).get_scoped();
    let mut txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "shared-context-test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let input = executor.create_ref(
        "adds",
        txn.add_files_schema().clone(),
        create_valid_add_file_batch(false),
    )?;
    txn.add_files_ref(input.clone());
    let operation_id = txn.state.operation_id;
    let prepared = txn.prepare_declarative(&executor)?;
    assert_eq!(prepared.state.operation_id, operation_id);
    assert_eq!(prepared.state.engine_info, "shared-context-test");
    assert!(Arc::ptr_eq(prepared.read_snapshot().unwrap(), &snapshot));
    assert_eq!(prepared.original_inputs.add_files.len(), 1);
    assert_eq!(prepared.original_inputs.add_files[0].id(), input.id());
    assert_eq!(prepared.target_version(), 1);
    prepared.commit(&engine, &executor)?.unwrap_committed();
    Ok(())
}
