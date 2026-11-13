use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rstest::rstest;
use url::Url;

use super::*;
use crate::committer::{
    CommitMetadata, Committer, FileSystemCommitter, PublishMetadata, write_commit_json_declarative,
};
use crate::engine::arrow_conversion::TryFromArrow;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::object_store::memory::InMemory;
use crate::plans::PlanExecutor;
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::unit_test_utils::{
    add_files_with_partition_values, create_valid_add_file_batch, nullable_add_file,
    set_field_as_null,
};
use crate::{FilteredEngineData, KernelResultIterator};

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
                commits: self.load_winning_commits(
                    engine,
                    metadata.table_root(),
                    metadata.version(),
                ),
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
                commits: self.load_winning_commits_declarative(
                    executor,
                    metadata.table_root(),
                    metadata.version(),
                ),
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
    fn is_catalog_committer(&self) -> bool {
        false
    }
    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> KernelResult<()> {
        FileSystemCommitter::new().publish(engine, metadata)
    }
}

impl ObservedCommitter {
    fn load_winning_commits(
        &self,
        engine: &dyn Engine,
        root: &Url,
        version: Version,
    ) -> KernelResult<CommitRange> {
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
        CommitRange::builder_for(root, version).build(engine)
    }
    fn load_winning_commits_declarative(
        &self,
        executor: &dyn PlanExecutor,
        root: &Url,
        version: Version,
    ) -> KernelResult<CommitRange> {
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
        CommitRange::builder_for(root, version).build_declarative(executor)
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
) -> KernelResult<()> {
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
    let schema = Arc::new(StructType::try_from_arrow(batch.schema())?);
    let input = executor.create_ref("adds", schema, batch)?;
    txn.add_files_ref(input);
    let prepared = txn.prepare(&engine, &executor)?;
    assert_eq!(prepared.target_version(), 1);
    assert_eq!(prepared.read_snapshot().unwrap().version(), 0);
    let result = prepared.commit(&engine, &executor)?;
    assert!(result.get_prepared_transaction().is_none());
    assert!(result.get_winning_commits()?.is_none());
    let committed = result.unwrap_committed();
    assert_eq!(committed.commit_version(), 1);
    assert_eq!(committed.post_commit_snapshot().unwrap().version(), 1);
    let loaded = crate::Snapshot::builder_for("memory:///table/").build(&engine)?;
    assert_eq!(loaded.version(), 1);
    assert_eq!(
        loaded.table_configuration().metadata().id(),
        snapshot.table_configuration().metadata().id()
    );
    let scan = loaded.clone().scan_builder().build()?;
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
        )?;
        remove.remove_files_ref(executor.create_ref(
            "removes",
            crate::scan::scan_row_schema(),
            batch,
        )?);
    }
    let removed = remove
        .prepare(&engine, &executor)?
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
fn declarative_create_preserves_typed_actions_and_application_transaction() -> KernelResult<()> {
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
    let prepared = txn.prepare(&engine, &executor)?;
    assert_eq!(prepared.target_version(), 0);
    assert!(prepared.non_file_actions.protocol.is_some());
    assert!(prepared.non_file_actions.metadata.is_some());
    assert_eq!(prepared.non_file_actions.set_transactions.len(), 1);
    assert_eq!(prepared.non_file_actions.domain_metadata.len(), 1);
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
#[case::history_available(0)]
#[case::eager_history_read_fails(1)]
fn conflict_getters_preserve_saved_history_or_error_without_refetching(
    #[case] history_failures: usize,
    #[values(false, true)] discard_pending: bool,
) -> KernelResult<()> {
    let (store, engine, snapshot) = table()?;
    let history_executor = SyncPlanExecutor::new(Some(store));
    let executor = history_executor.get_scoped();
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let committer = ObservedCommitter {
        history_reads: reads.clone(),
        commit_calls: calls,
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
    let prepared = txn.prepare(&engine, &executor)?;
    let actions = prepared.get_delta_actions().clone();
    let mut latest = snapshot.clone();
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
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        result.winning_commits_error().is_some(),
        history_failures > 0
    );
    let mut pending = result.take_prepared_transaction().unwrap();
    assert!(result.get_prepared_transaction().is_none());
    assert!(result.take_prepared_transaction().is_none());
    if history_failures > 0 {
        for _ in 0..2 {
            assert!(
                result
                    .get_winning_commits()
                    .unwrap_err()
                    .to_string()
                    .contains("History listing unavailable")
            );
            assert!(result.winning_commits_error().is_some());
        }
        assert_eq!(pending.get_delta_actions().id(), actions.id());
        drop(pending);
        assert!(result.get_winning_commits().is_err());
        let fresh = crate::Snapshot::builder_for("memory:///table/").build(&engine)?;
        let committed = fresh
            .update_table(UpdateTableOperation::Write, "restart", true)
            .with_execution_mode::<Declarative>()
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .prepare(&engine, &executor)?
            .commit(&engine, &executor)?
            .unwrap_committed();
        assert_eq!(committed.commit_version(), 4);
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        return Ok(());
    }
    let winners = result.get_winning_commits()?.unwrap().clone();
    assert_eq!(winners.start_version(), 1);
    assert_eq!(winners.end_version(), 3);
    assert!(result.winning_commits_error().is_none());
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
    latest
        .update_table(UpdateTableOperation::Write, "later-winner", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
    assert!(Arc::ptr_eq(
        result.get_winning_commits()?.unwrap(),
        &winners
    ));
    assert_eq!(winners.end_version(), 3);
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn retryable_rejection_preserves_actions_and_retries_the_same_version_without_history()
-> KernelResult<()> {
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
    let prepared = txn.prepare(&engine, &executor)?;
    let actions = prepared.get_delta_actions().clone();
    let result = prepared.commit(&engine, &executor)?;
    assert!(matches!(
        &result,
        PreparedTransactionCommitResult::Retryable { error: KernelError::IOError(error), .. }
            if error.kind() == std::io::ErrorKind::WouldBlock
    ));
    assert!(!result.is_committed());
    assert!(result.get_winning_commits()?.is_none());
    let pending = result.get_prepared_transaction().unwrap();
    assert_eq!(pending.target_version(), 1);
    assert_eq!(pending.read_snapshot().unwrap().version(), 0);
    assert_eq!(pending.get_delta_actions().id(), actions.id());
    assert!(pending.submitted_attempt().is_none());
    assert_eq!(
        crate::Snapshot::builder_for("memory:///table/")
            .build(&engine)?
            .version(),
        0
    );
    let committed = result
        .into_prepared_transaction()
        .unwrap()
        .commit(&engine, &executor)?
        .unwrap_committed();
    assert_eq!(committed.commit_version(), 1);
    assert_eq!(committed.post_commit_snapshot().unwrap().version(), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn uncertain_success_preserves_exact_attempt_without_discovery_or_blind_resubmission()
-> KernelResult<()> {
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
    let prepared = txn.prepare(&engine, &executor)?;
    let result = prepared.commit(&engine, &executor)?;
    assert!(matches!(
        &result,
        PreparedTransactionCommitResult::Unknown { .. }
    ));
    assert!(result.get_winning_commits()?.is_none());
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    let pending = result.into_prepared_transaction().unwrap();
    let attempt = pending.submitted_attempt().unwrap();
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
) -> KernelResult<()> {
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
    let schema = Arc::new(StructType::try_from_arrow(batch.schema())?);
    let mut builder = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>();
    if skip_dedup_validation {
        builder = builder.without_dedup_validation();
    }
    let mut txn = builder.build(&engine, Box::new(FileSystemCommitter::new()))?;
    txn.add_files_ref(executor.create_ref("adds", schema, batch)?);
    let error = txn
        .prepare(&engine, &executor)
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
fn file_action_validation_fails_before_submitting(#[case] invalid: &str) -> KernelResult<()> {
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
    let schema = StructType::try_from_arrow(batch.schema())?;
    let input = executor.create_ref("input", Arc::new(schema), batch)?;
    txn.add_files_ref(input.clone());
    if invalid == "duplicate" {
        txn.add_files_ref(input);
    }
    let result = txn
        .prepare(&engine, &executor)
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
fn imperative_conflict_saves_history_or_error_without_refetching(
    #[values(0, 1)] failures: usize,
) -> KernelResult<()> {
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
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    assert_eq!(result.winning_commits_error().is_some(), failures > 0);
    if failures > 0 {
        assert!(result.get_winning_commits().is_err());
        assert!(result.get_winning_commits().is_err());
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        return Ok(());
    }
    let history = result.get_winning_commits()?.clone();
    assert_eq!(history.start_version(), 1);
    assert_eq!(history.end_version(), 1);
    winner
        .update_table(UpdateTableOperation::Write, "later", true)
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .commit(&engine)?
        .unwrap_committed();
    assert!(Arc::ptr_eq(&history, result.get_winning_commits()?));
    assert!(result.winning_commits_error().is_none());
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    Ok(())
}

#[rstest]
#[case::accepted_version(None)]
#[case::invalid_reported_version(Some(2))]
fn post_commit_snapshot_stays_at_committed_version_or_propagates_construction_error(
    #[case] reported_version: Option<Version>,
) -> KernelResult<()> {
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
        .prepare(&engine, &executor)?;
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
