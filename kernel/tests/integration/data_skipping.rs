//! Integration tests for past-cap data-skipping behavior.
//!
//! Covers a table with `delta.dataSkippingNumIndexedCols=N` plus a predicate referencing a
//! past-cap column. The rewritten predicate must drop refs to columns missing from the
//! unified stats schema (and from the checkpoint parquet projection); otherwise scan setup
//! fails with "Schema error: No such field". The tests exercise three code paths that all
//! share that rewrite:
//!
//! - `Scan::scan_metadata` (in-memory `DataSkippingFilter`)
//! - `Scan::parallel_scan_metadata` (worker rebuilds `DataSkippingFilter` from `InternalScanState`)
//! - `Scan::build_actions_meta_predicate` (`as_checkpoint_skipping_predicate` pushed down to the
//!   checkpoint parquet row-group filter)

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{Int64Array, RecordBatch};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::expressions::{column_expr, Expression as Expr, Predicate as Pred, PredicateRef};
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Snapshot, SnapshotRef};
use rstest::rstest;
use test_utils::{create_table_and_load_snapshot, test_table_setup_mt, write_batch_to_table};
use url::Url;

use crate::common::write_utils::set_table_properties;

type TestEngine = DefaultEngine<TokioMultiThreadExecutor>;

/// Flat schema with `c0..c4`. `delta.dataSkippingNumIndexedCols=2` keeps stats on `c0` and
/// `c1`; `c2..c4` are past-cap.
fn capped_schema() -> SchemaRef {
    Arc::new(
        StructType::try_new((0..5).map(|i| StructField::nullable(format!("c{i}"), DataType::LONG)))
            .unwrap(),
    )
}

/// Builds one `RecordBatch` of `rows` rows. `c0` ranges over `c0_start..c0_start+rows`, with
/// `c1 = c0 + 100` so `c1` ranges share the same width but at higher values. `c2..c4` are
/// filled with constant `c0_start` so they have no useful min/max even if they were indexed.
fn long_batch(schema: &SchemaRef, c0_start: i64, rows: i64) -> RecordBatch {
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().unwrap();
    let c0 = Int64Array::from_iter_values(c0_start..c0_start + rows);
    let c1 = Int64Array::from_iter_values(c0_start + 100..c0_start + 100 + rows);
    let cn = Int64Array::from_iter_values(std::iter::repeat_n(c0_start, rows as usize));
    RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(c0),
            Arc::new(c1),
            Arc::new(cn.clone()),
            Arc::new(cn.clone()),
            Arc::new(cn),
        ],
    )
    .unwrap()
}

/// Creates a fresh table with `dataSkippingNumIndexedCols=2`, writes three add-files with
/// disjoint `c0`/`c1` ranges, then checkpoints. Returns `(temp_dir, table_path, engine)`.
/// The `temp_dir` keeps the on-disk table alive for the duration of the test.
async fn build_capped_table_with_checkpoint(
) -> Result<(tempfile::TempDir, String, Arc<TestEngine>), Box<dyn std::error::Error>> {
    let schema = capped_schema();
    let (tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;
    // `create_table_and_load_snapshot` rejects `delta.dataSkippingNumIndexedCols` at CREATE
    // TABLE time, so backfill it via a v0-rewrite metadata commit.
    let snapshot =
        create_table_and_load_snapshot(&table_path, schema.clone(), engine.as_ref(), &[])?;
    let mut snapshot = set_table_properties(
        &table_path,
        &table_url,
        engine.as_ref(),
        snapshot.version(),
        &[("delta.dataSkippingNumIndexedCols", "2")],
    )?;

    // file A: c0 in [10, 19], c1 in [110, 119]
    // file B: c0 in [50, 59], c1 in [150, 159]
    // file C: c0 in [100, 109], c1 in [200, 209]
    for c0_start in [10, 50, 100] {
        snapshot = write_batch_to_table(
            &snapshot,
            engine.as_ref(),
            long_batch(&schema, c0_start, 10),
            HashMap::new(),
        )
        .await?;
    }

    snapshot.checkpoint(engine.as_ref(), None)?;
    Ok((tmp_dir, table_path, engine))
}

/// Counts the files surviving data skipping. Exercises the sequential
/// `Scan::scan_metadata` path when `use_parallel` is `false`, or the two-phase
/// `Scan::parallel_scan_metadata` + `ParallelScanMetadata` path when `true`. The parallel
/// branch dispatches one file per `ParallelScanMetadata` to maximize coverage of the worker
/// rebuild path.
fn count_selected_files(
    snapshot: SnapshotRef,
    engine: Arc<TestEngine>,
    predicate: PredicateRef,
    use_parallel: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    let scan = snapshot.scan_builder().with_predicate(predicate).build()?;

    fn push_path(paths: &mut Vec<String>, scan_file: delta_kernel::scan::state::ScanFile) {
        paths.push(scan_file.path);
    }

    let mut paths: Vec<String> = Vec::new();

    if use_parallel {
        let mut sequential = scan.parallel_scan_metadata(engine.clone())?;
        for sm in sequential.by_ref() {
            paths = sm?.visit_scan_files(paths, push_path)?;
        }
        if let AfterSequentialScanMetadata::Parallel { state, files } = sequential.finish()? {
            let state = Arc::from(state);
            for file in files {
                let parallel =
                    ParallelScanMetadata::try_new(engine.clone(), Arc::clone(&state), vec![file])?;
                for sm in parallel {
                    paths = sm?.visit_scan_files(paths, push_path)?;
                }
            }
        }
    } else {
        for sm in scan.scan_metadata(engine.as_ref())? {
            paths = sm?.visit_scan_files(paths, push_path)?;
        }
    }

    Ok(paths.len())
}

/// Loads a fresh snapshot off the same on-disk table and returns the surviving file count
/// for `predicate` along both the sequential and parallel scan-metadata paths.
fn surviving_files(
    table_path: &str,
    engine: Arc<TestEngine>,
    predicate: PredicateRef,
    use_parallel: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    let url = delta_kernel::try_parse_uri(table_path)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    count_selected_files(snapshot, engine, predicate, use_parallel)
}

// === Predicate matrix ===
//
// Table layout (3 files, `dataSkippingNumIndexedCols=2`):
//   file A: c0 in [10, 19],  c1 in [110, 119]
//   file B: c0 in [50, 59],  c1 in [150, 159]
//   file C: c0 in [100, 109], c1 in [200, 209]
//   c2/c3/c4: past-cap, no stats.

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_in_cap_prunes(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = build_capped_table_with_checkpoint().await?;
    let pred = Arc::new(Pred::gt(column_expr!("c0"), Expr::literal(60i64)));
    assert_eq!(surviving_files(&table_path, engine, pred, use_parallel)?, 1);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_in_cap_keeps_all(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = build_capped_table_with_checkpoint().await?;
    let pred = Arc::new(Pred::gt(column_expr!("c0"), Expr::literal(0i64)));
    assert_eq!(surviving_files(&table_path, engine, pred, use_parallel)?, 3);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_past_cap_keeps_all(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = build_capped_table_with_checkpoint().await?;
    let pred = Arc::new(Pred::gt(column_expr!("c4"), Expr::literal(1_000_000i64)));
    assert_eq!(surviving_files(&table_path, engine, pred, use_parallel)?, 3);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_and_in_cap_prunes(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = build_capped_table_with_checkpoint().await?;
    // c0 > 60 prunes files A and B; c4 > 50 is past-cap and folds to NULL.
    // AND(false, NULL) = false keeps the prune; AND(true, NULL) = NULL keeps file C.
    let pred = Arc::new(Pred::and(
        Pred::gt(column_expr!("c0"), Expr::literal(60i64)),
        Pred::gt(column_expr!("c4"), Expr::literal(50i64)),
    ));
    assert_eq!(surviving_files(&table_path, engine, pred, use_parallel)?, 1);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_or_keeps_all(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = build_capped_table_with_checkpoint().await?;
    // c0 > 1000 would prune all 3 by max; c4 > 50 is past-cap and folds to NULL.
    // OR(false, NULL) = NULL keeps every file.
    let pred = Arc::new(Pred::or(
        Pred::gt(column_expr!("c0"), Expr::literal(1000i64)),
        Pred::gt(column_expr!("c4"), Expr::literal(50i64)),
    ));
    assert_eq!(surviving_files(&table_path, engine, pred, use_parallel)?, 3);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boundary_c1_prunes_all(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = build_capped_table_with_checkpoint().await?;
    // c1 max across files is 209 < 250 so the in-cap arm rules out everything.
    // c2 > 50 is past-cap and folds to NULL; AND(false, NULL) = false everywhere.
    let pred = Arc::new(Pred::and(
        Pred::gt(column_expr!("c1"), Expr::literal(250i64)),
        Pred::gt(column_expr!("c2"), Expr::literal(50i64)),
    ));
    assert_eq!(surviving_files(&table_path, engine, pred, use_parallel)?, 0);
    Ok(())
}
