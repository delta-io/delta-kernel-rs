//! Metrics tests for `Transaction::commit`.
//!
//! Each test builds a table, attaches a metrics reporter, runs a commit, and asserts the
//! commit metrics a real connector would observe.

use std::sync::{Arc, Mutex};

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::metrics::{MetricEvent, MetricsReporter, TransactionCommitSuccess};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{insert_data, install_thread_local_metrics_reporter, test_table_setup_mt};
use url::Url;

use super::{measuring_engine, simple_schema};

#[derive(Debug, Default)]
struct LastCommitSuccess(Mutex<Option<TransactionCommitSuccess>>);

impl MetricsReporter for LastCommitSuccess {
    fn report(&self, event: MetricEvent) {
        if let MetricEvent::TransactionCommitSuccess(success) = event {
            *self.0.lock().unwrap() = Some(success);
        }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_append_emits_success_metrics() -> DeltaResult<()> {
    let (_temp_dir, table_url) = setup_empty_table()?;
    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let engine = Arc::new(engine);
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    reporter.reset();
    let committed = insert_data(
        snap,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?
    .unwrap_committed();

    assert_eq!(committed.commit_version(), 1);
    assert_eq!(reporter.transaction_commits.get(), 1);
    assert_eq!(reporter.commit_add_files.get(), 1);
    assert!(reporter.commit_add_bytes.get() > 0);
    assert_eq!(reporter.commit_remove_files.get(), 0);
    assert_eq!(reporter.commit_remove_bytes.get(), 0);
    assert_eq!(reporter.commit_conflicts.get(), 0);
    assert_eq!(reporter.commit_errors.get(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_conflict_emits_conflict_metric() -> DeltaResult<()> {
    let (_temp_dir, table_url) = setup_empty_table()?;
    let (engine, reporter, _guard) = measuring_engine(Arc::new(LocalFileSystem::new()));
    let engine = Arc::new(engine);
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    reporter.reset();
    insert_data(
        snap.clone(),
        &engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?
    .unwrap_committed();
    // Both transactions are built from the same base snapshot, so the second targets a version
    // the first already wrote, forcing a conflict.
    let result = insert_data(snap, &engine, vec![Arc::new(Int32Array::from(vec![2]))]).await?;

    assert!(matches!(result, CommitResult::ConflictedTransaction(_)));
    assert_eq!(reporter.transaction_commits.get(), 1);
    assert_eq!(reporter.commit_conflicts.get(), 1);
    assert_eq!(reporter.commit_errors.get(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_success_event_carries_operation_and_flags() -> DeltaResult<()> {
    let (_temp_dir, table_url) = setup_empty_table()?;
    let reporter = Arc::new(LastCommitSuccess::default());
    let engine = Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    let snap = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    insert_data(
        snap,
        &engine,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .await?
    .unwrap_committed();

    let success = reporter
        .0
        .lock()
        .unwrap()
        .clone()
        .expect("commit success event");
    assert_eq!(success.operation.as_deref(), Some("WRITE"));
    assert!(success.data_change);
    assert!(!success.is_blind_append);
    assert_eq!(success.num_add_files, 1);
    Ok(())
}
