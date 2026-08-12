//! Metrics tests for `Transaction::commit`.
//!
//! Each test builds a table, attaches a metrics reporter, runs a commit, and asserts the
//! commit metrics a real connector would observe.

use std::sync::{Arc, Mutex};

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::metrics::{MetricEvent, MetricsReporter, TableType, TransactionCommitSuccess};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    assert_result_error_with_message, begin_transaction, create_add_files_metadata, insert_data,
    insert_data_with, install_thread_local_metrics_reporter, test_table_setup_mt,
};
use url::Url;

use super::table_type::{create_simple_table, make_committer};
use super::{measuring_engine, simple_schema};
use crate::common::write_utils::{
    create_dv_table_with_files, get_scan_files, sequential_dv_descriptors,
};

/// Reporter that keeps the last `TransactionCommitSuccess` for field-level assertions.
#[derive(Debug, Default)]
struct LastCommitSuccess(Mutex<Option<TransactionCommitSuccess>>);

impl MetricsReporter for LastCommitSuccess {
    fn report(&self, event: MetricEvent) {
        if let MetricEvent::TransactionCommitSuccess(success) = event {
            *self.0.lock().unwrap() = Some(success);
        }
    }
}

impl LastCommitSuccess {
    /// Returns the last captured commit-success event, panicking if none was recorded.
    fn take_success(&self) -> TransactionCommitSuccess {
        self.0
            .lock()
            .unwrap()
            .clone()
            .expect("commit success event")
    }
}

fn setup_empty_table() -> DeltaResult<(tempfile::TempDir, Url)> {
    let (temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    create_table(&table_path, simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?
        .unwrap_committed();
    Ok((temp_dir, table_url))
}

#[rstest]
#[case::write_append("WRITE", true, false)]
#[case::blind_append("WRITE", true, true)]
#[case::optimize_no_data_change("OPTIMIZE", false, false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_append_emits_success_metrics(
    #[case] operation: &str,
    #[case] data_change: bool,
    #[case] is_blind_append: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_url) = setup_empty_table()?;
    let reporter = Arc::new(LastCommitSuccess::default());
    let engine = Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    insert_data_with(
        snap,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        Box::new(FileSystemCommitter::new()),
        operation,
        data_change,
        is_blind_append,
    )
    .await?
    .unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.table_type, TableType::PathBased);
    assert_eq!(success.commit_version, 1);
    assert_eq!(success.num_add_files, 1);
    assert!(success.add_files_bytes > 0);
    assert_eq!(success.num_remove_files, 0);
    assert_eq!(success.remove_files_bytes, 0);
    assert_eq!(success.operation.as_deref(), Some(operation));
    assert_eq!(success.data_change, data_change);
    assert_eq!(success.is_blind_append, is_blind_append);
    assert!(success.total_duration >= success.prepare_duration + success.committer_duration);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_reports_added_file_count_not_batch_count() -> DeltaResult<()> {
    // num_add_files counts added FILES, not add_files() batches: two batches of two files each
    // must report 4. A regression to add_files_metadata.len() (a batch count) would report 2.
    let (_temp_dir, table_url) = setup_empty_table()?;
    let reporter = Arc::new(LastCommitSuccess::default());
    let engine = Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let mut txn = begin_transaction(snap, engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_data_change(true);
    let add_files_schema = txn.add_files_schema();
    // Two separate add_files() calls -> two batches, four files total.
    let batches = vec![
        vec![
            ("f0.parquet", 1024, 1_000_000, Some(3)),
            ("f1.parquet", 1124, 1_000_001, Some(3)),
        ],
        vec![
            ("f2.parquet", 1224, 1_000_002, Some(3)),
            ("f3.parquet", 1324, 1_000_003, Some(3)),
        ],
    ];
    for batch in batches {
        let metadata = create_add_files_metadata(add_files_schema, batch)
            .map_err(|e| delta_kernel::Error::generic(e.to_string()))?;
        txn.add_files(metadata);
    }
    txn.commit(engine.as_ref())?.unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.num_add_files, 4);
    Ok(())
}

/// Sets the correlation id on the `Transaction` returned by `build()` and checks it reaches the
/// commit metric event. The two tests below instead set it on the builder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_success_carries_correlation_id() -> DeltaResult<()> {
    let (_temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let reporter = Arc::new(LastCommitSuccess::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    create_table(&table_path, simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .with_correlation_id("commit-req-1")
        .commit(setup_engine.as_ref())?
        .unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.commit_version, 0);
    assert_eq!(success.correlation_id.as_deref(), Some("commit-req-1"));
    Ok(())
}

/// A correlation id set on the create-table *builder* (rather than on the `Transaction` it
/// produces) reaches the commit metric event, and an empty id is treated as unset. This is the
/// builder-level setter added in issue #2833.
#[rstest]
#[case::with_id(Some("create-req-1"), Some("create-req-1"))]
#[case::without_id(None, None)]
#[case::empty_id_is_unset(Some(""), None)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_table_builder_carries_correlation_id(
    #[case] correlation_id: Option<&str>,
    #[case] expected: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let reporter = Arc::new(LastCommitSuccess::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let mut builder = create_table(&table_path, simple_schema(), "Test/1.0");
    if let Some(id) = correlation_id {
        builder = builder.with_correlation_id(id);
    }
    builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.commit_version, 0);
    assert_eq!(success.correlation_id.as_deref(), expected);
    Ok(())
}

/// A correlation id set on the alter-table *builder* reaches the commit metric event. The id is
/// set before any schema operation (in the `Ready` state), so this also verifies it survives the
/// builder's `Ready -> Modifying` transition. An empty id is treated as unset. Builder-level
/// setter added in issue #2833.
#[rstest]
#[case::with_id(Some("alter-req-1"), Some("alter-req-1"))]
#[case::without_id(None, None)]
#[case::empty_id_is_unset(Some(""), None)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_table_builder_carries_correlation_id(
    #[case] correlation_id: Option<&str>,
    #[case] expected: Option<&str>,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    create_table(&table_path, simple_schema(), "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Install the reporter after the create commit so the captured event is the alter commit.
    let reporter = Arc::new(LastCommitSuccess::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    // Set the id in the `Ready` state (before `add_column`) to exercise the carry-through.
    let mut builder = snapshot.alter_table();
    if let Some(id) = correlation_id {
        builder = builder.with_correlation_id(id);
    }
    builder
        .add_column(StructField::nullable("extra", DataType::STRING))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.commit_version, 1);
    assert_eq!(success.correlation_id.as_deref(), expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_conflict_emits_conflict_metric() -> DeltaResult<()> {
    // GIVEN a table at v0 (00.json) and a snapshot pinned to it.
    let (_temp_dir, table_url) = setup_empty_table()?;
    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let engine = Arc::new(engine);
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    reporter.reset();
    // WHEN a first append commits against that snapshot, advancing the table to v1 (01.json).
    insert_data(
        snap.clone(),
        &engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?
    .unwrap_committed();
    // AND a second append reuses the SAME v0 snapshot, so it also targets v1 (already written).
    let result = insert_data(snap, &engine, vec![Arc::new(Int32Array::from(vec![2]))]).await?;

    // THEN the second commit conflicts and emits exactly one conflict metric.
    assert!(matches!(result, CommitResult::ConflictedTransaction(_)));
    assert_eq!(reporter.transaction_commits.get(), 1);
    assert_eq!(reporter.commit_conflicts.get(), 1);
    assert_eq!(reporter.commit_errors.get(), 0);
    Ok(())
}

#[rstest]
#[case::path_based(false)]
#[case::catalog_managed(true)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_success_carries_table_type(#[case] catalog_managed: bool) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    create_simple_table(engine.as_ref(), &table_path, catalog_managed)?;

    let reporter = Arc::new(LastCommitSuccess::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let builder = Snapshot::builder_for(table_url);
    let builder = if catalog_managed {
        builder.with_max_catalog_version(0)
    } else {
        builder
    };
    let snapshot = builder.build(engine.as_ref())?;
    insert_data_with(
        snapshot,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
        make_committer(catalog_managed),
        "WRITE",
        /* data_change */ true,
        /* is_blind_append */ false,
    )
    .await?
    .unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(
        success.table_type,
        TableType::from_catalog_managed(catalog_managed)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_dv_update_reports_updated_file_count_not_batch_count(
) -> Result<(), Box<dyn std::error::Error>> {
    // Three files get a new deletion vector in a single update_deletion_vectors call.
    // num_dv_updates must be the count of updated FILES (3), not the number of scan-metadata
    // batches they arrive in -- a single batch carrying all three would otherwise report 1.
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ])?);
    let file_names = &["file0.parquet", "file1.parquet", "file2.parquet"];
    let (_store, engine, table_url, file_paths) =
        create_dv_table_with_files("dv_metrics_table", schema, file_names).await?;

    let reporter = Arc::new(LastCommitSuccess::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    let mut paths_with_unmatched = file_paths.clone();
    paths_with_unmatched.push("missing.parquet".to_string());
    let mut scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;
    assert_result_error_with_message(
        txn.update_deletion_vectors(
            sequential_dv_descriptors(&paths_with_unmatched),
            scan_files.drain(..).map(Ok),
        ),
        "Number of matched DV files does not match number of new DV descriptors: 3 != 4",
    );

    // A failed call with an unmatched path must not contribute its three matched files.
    let mut scan_files = get_scan_files(snapshot, engine.as_ref())?;
    let dv_map = sequential_dv_descriptors(&file_paths);
    txn.update_deletion_vectors(dv_map, scan_files.drain(..).map(Ok))?;
    txn.commit(engine.as_ref())?.unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.num_dv_updates, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_dv_update_accumulates_file_count_across_calls(
) -> Result<(), Box<dyn std::error::Error>> {
    // num_dv_updates must accumulate (`+=`) across multiple update_deletion_vectors calls on one
    // transaction: two calls updating one file each must report 2, not the last call's 1.
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("value", DataType::STRING),
    ])?);
    let file_names = &["file0.parquet", "file1.parquet"];
    let (_store, engine, table_url, file_paths) =
        create_dv_table_with_files("dv_metrics_multi_call_table", schema, file_names).await?;

    let reporter = Arc::new(LastCommitSuccess::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?
        .with_operation("UPDATE".to_string())
        .with_data_change(true);

    // Each call re-derives scan files from the same snapshot and updates exactly one file, so
    // matched_dv_files is 1 per call and the accumulator must reach 2.
    let all_descriptors = sequential_dv_descriptors(&file_paths);
    for path in &file_paths {
        let mut scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;
        let dv_map = std::iter::once((path.clone(), all_descriptors[path].clone())).collect();
        txn.update_deletion_vectors(dv_map, scan_files.drain(..).map(Ok))?;
    }
    txn.commit(engine.as_ref())?.unwrap_committed();

    let success = reporter.take_success();
    assert_eq!(success.num_dv_updates, 2);
    Ok(())
}
