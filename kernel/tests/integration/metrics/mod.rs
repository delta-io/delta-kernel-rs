//! Integration tests that verify [`CountingReporter`] metrics across different table
//! states and API call patterns.
//!
//! All tests use only public kernel APIs (`create_table`, `Transaction`, `write_parquet`,
//! `log_compaction_writer`, `scan.execute()`) to ensure that the metrics surface is
//! verified through the same code paths a real engine connector would exercise.
//!
//! Tests are organized by concern:
//! - [`snapshot_load`]: snapshot-loading scenarios (delta-only, checkpoint, compaction, CRC, and
//!   on-demand API calls like `get_domain_metadata`)
//! - [`scan`]: scan execution scenarios (`scan.execute()` parquet data-file reads)
//! - [`commit`]: `Transaction::commit()` success / conflict metrics
//!
//! Where possible, tests use [`TestTableBuilder`] for table setup and install the measuring
//! reporter after the history has been written.

use std::sync::Arc;

use delta_kernel::Snapshot;
use delta_kernel::arrow::array::Int32Array;
use delta_kernel::schema::{SchemaRef, schema_ref};
use test_utils::delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::table_builder::{LogState, TestTableBuilder};
use test_utils::{CountingReporter, insert_data, install_thread_local_metrics_reporter};
use url::Url;

mod checkpoint;
mod commit;
mod parallel_scan;
mod scan;
mod snapshot_load;
mod table_type;

/// Build a `DefaultEngine` + `CountingReporter` backed by `store`, for use in the
/// *measurement* phase (building a snapshot and asserting metric counters).
///
/// Returns the engine, the reporter, and a tracing `DefaultGuard` that must be kept alive
/// for the duration of the test. When the guard is dropped the subscriber is uninstalled.
fn measuring_engine(
    store: Arc<dyn delta_kernel::object_store::ObjectStore>,
) -> (
    DefaultEngine<
        test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor,
    >,
    Arc<CountingReporter>,
    tracing::subscriber::DefaultGuard,
) {
    let reporter = Arc::new(CountingReporter::default());
    let engine = DefaultEngineBuilder::new(store).build();
    let guard = install_thread_local_metrics_reporter(reporter.clone());
    (engine, reporter, guard)
}

/// Build a minimal single-column INTEGER schema for test tables.
fn simple_schema() -> SchemaRef {
    schema_ref! { nullable "id": INTEGER }
}

/// Insert `count` rows (starting from `start_val`) into an existing table, using
/// `post_commit_snapshot` to chain snapshots instead of rebuilding from scratch.
async fn insert_rows(
    table_url: &Url,
    engine: &Arc<
        DefaultEngine<
            test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor,
        >,
    >,
    start_val: i32,
    count: i32,
) -> delta_kernel::Result<()> {
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
