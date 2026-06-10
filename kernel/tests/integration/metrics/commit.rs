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
use rstest::rstest;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::{
    insert_data, insert_data_with, install_thread_local_metrics_reporter, test_table_setup_mt,
};
use url::Url;

use super::{measuring_engine, simple_schema};

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
        operation,
        data_change,
        is_blind_append,
    )
    .await?
    .unwrap_committed();

    let success = reporter
        .0
        .lock()
        .unwrap()
        .clone()
        .expect("commit success event");
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
