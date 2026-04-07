//! Integration tests that verify [`CountingReporter`] metrics across different table
//! states and API call patterns.
//!
//! All tests use only public kernel APIs (`create_table`, `Transaction`, `write_parquet`,
//! `log_compaction_writer`, `scan.execute()`) to ensure that the metrics surface is
//! verified through the same code paths a real engine connector would exercise.
//!
//! Tests are organized by concern:
//! - [`snapshot_load`]: snapshot-loading scenarios (delta-only, checkpoint, compaction, CRC,
//!   and on-demand API calls like `get_domain_metadata`)
//! - [`scan`]: scan execution scenarios (`scan.execute()` parquet data-file reads)
//! - [`incremental_snapshot`]: incremental update scenarios via `Snapshot::builder_from`

use std::sync::Arc;

use delta_kernel::arrow::array::Int32Array;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::{DefaultEngine, DefaultEngineBuilder};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Snapshot};
use test_utils::{insert_data, test_table_setup_mt, CountingReporter};
use url::Url;

mod incremental_snapshot;
mod scan;
mod snapshot_load;

/// Build a `DefaultEngine` + `CountingReporter` backed by `store`, for use in the
/// *measurement* phase (building a snapshot and asserting metric counters).
fn measuring_engine(
    store: Arc<dyn delta_kernel::object_store::ObjectStore>,
) -> (
    DefaultEngine<delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor>,
    Arc<CountingReporter>,
) {
    let reporter = Arc::new(CountingReporter::default());
    let engine = DefaultEngineBuilder::new(store)
        .with_metrics_reporter(reporter.clone())
        .build();
    (engine, reporter)
}

/// Build a minimal single-column INTEGER schema for test tables.
fn simple_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![StructField::nullable("id", DataType::INTEGER)])
            .expect("valid schema"),
    )
}

/// Create an in-memory table with `num_inserts` commits after the initial create-table
/// commit. Returns `(table_url, setup_engine, store)`. Table is at version `num_inserts`.
async fn setup_in_memory_table(
    num_inserts: usize,
) -> DeltaResult<(
    Url,
    Arc<DefaultEngine<delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor>>,
    Arc<InMemory>,
)> {
    let store = Arc::new(InMemory::new());
    let table_url = Url::parse("memory:///").unwrap();
    let engine = Arc::new(DefaultEngineBuilder::new(store.clone() as Arc<_>).build());

    let _ = create_table("memory:///", simple_schema(), "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let mut snap = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    for val in 1..=num_inserts {
        let committed = insert_data(
            snap.clone(),
            &engine,
            vec![Arc::new(Int32Array::from(vec![val as i32]))],
        )
        .await?
        .unwrap_committed();
        snap = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot")
            .clone();
    }

    Ok((table_url, engine, store))
}

/// Insert `count` rows (starting from `start_val`) into an existing table, using
/// `post_commit_snapshot` to chain snapshots instead of rebuilding from scratch.
async fn insert_rows(
    table_url: &Url,
    engine: &Arc<
        DefaultEngine<delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor>,
    >,
    start_val: i32,
    count: i32,
) -> DeltaResult<()> {
    let mut snap = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    for val in start_val..(start_val + count) {
        let committed = insert_data(snap, engine, vec![Arc::new(Int32Array::from(vec![val]))])
            .await?
            .unwrap_committed();
        snap = committed
            .post_commit_snapshot()
            .expect("post-commit snapshot")
            .clone();
    }
    Ok(())
}

/// Create a table at v0, insert one row, and write a v1 parquet checkpoint.
///
/// Returns `(table_url, setup_engine, _temp_dir)` where `_temp_dir` must be kept
/// alive for the duration of the test to prevent early cleanup. Callers can add
/// further commits via `insert_data` or directly build the measuring snapshot.
///
/// Uses `TokioMultiThreadExecutor` because `snapshot.checkpoint()` issues nested
/// `block_on` calls; `TokioBackgroundExecutor` deadlocks in that pattern.
async fn setup_table_with_v1_checkpoint() -> DeltaResult<(
    Url,
    Arc<DefaultEngine<TokioMultiThreadExecutor>>,
    tempfile::TempDir,
)> {
    let (temp_dir, table_path, setup_engine) = test_table_setup_mt()?;
    let table_url = delta_kernel::try_parse_uri(&table_path)?;

    let _ = create_table(&table_path, simple_schema(), "Test/1.0")
        .build(setup_engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(setup_engine.as_ref())?;

    let snap0 = Snapshot::builder_for(table_url.clone()).build(setup_engine.as_ref())?;
    let committed = insert_data(
        snap0,
        &setup_engine,
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .await?
    .unwrap_committed();
    committed
        .post_commit_snapshot()
        .expect("post-commit snapshot")
        .clone()
        .checkpoint(setup_engine.as_ref())?;

    Ok((table_url, setup_engine, temp_dir))
}
