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
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::expressions::{
    column_expr, Expression as Expr, Predicate as Pred, PredicateRef, Scalar,
};
use delta_kernel::metrics::{MetricEvent, ScanType};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{Error, Snapshot, SnapshotRef};
use rstest::rstest;
use serde::Serialize;
use serde_json::{json, Value};
use test_utils::delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    add_commit, create_table_and_load_snapshot, install_thread_local_metrics_reporter,
    test_table_setup_mt, write_batch_to_table, CapturingReporter,
};
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

/// Returns the paths of files surviving data skipping. Exercises the sequential
/// `Scan::scan_metadata` path when `use_parallel` is `false`, or the two-phase
/// `Scan::parallel_scan_metadata` + `ParallelScanMetadata` path when `true`. The parallel
/// branch dispatches one file per `ParallelScanMetadata` to maximize coverage of the worker
/// rebuild path.
fn selected_paths(
    snapshot: SnapshotRef,
    engine: Arc<TestEngine>,
    predicate: PredicateRef,
    use_parallel: bool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
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
            state.log_metrics();
        }
    } else {
        for sm in scan.scan_metadata(engine.as_ref())? {
            paths = sm?.visit_scan_files(paths, push_path)?;
        }
    }

    Ok(paths)
}

/// Loads a fresh snapshot off the same on-disk table and returns the surviving file count
/// for `predicate` along both the sequential and parallel scan-metadata paths.
fn surviving_files(
    table_path: &str,
    engine: Arc<TestEngine>,
    predicate: PredicateRef,
    use_parallel: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(surviving_paths(table_path, engine, predicate, use_parallel)?.len())
}

/// Like [`surviving_files`] but returns the surviving file paths, sorted so a test can make an
/// order-independent exact-set assertion (`scan_metadata`, and especially the parallel path, does
/// not guarantee a stable file emission order).
///
/// Test-only: this never runs in the production scan path. The sort is trivial here because the
/// test tables yield only a handful of files, but it is O(n log n) in the surviving-file count and
/// so would not belong on a hot path over a large result set.
fn surviving_paths(
    table_path: &str,
    engine: Arc<TestEngine>,
    predicate: PredicateRef,
    use_parallel: bool,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let url = delta_kernel::try_parse_uri(table_path)?;
    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;
    let mut paths = selected_paths(snapshot, engine, predicate, use_parallel)?;
    paths.sort();
    Ok(paths)
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

// === Per-cell NULL on malformed stats ===
//
// A Delta producer can emit an Add whose `stats` JSON contains an extended-year ISO 8601
// timestamp like `+48690-07-02T22:50:38.211Z`. Arrow's `TimestampParser` rejects such
// strings. With per-cell NULL only the bad cell becomes NULL, so other files in the same
// batch keep their stats and the predicate prunes them correctly.

fn timestamp_stats_schema() -> SchemaRef {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("EventTime", DataType::TIMESTAMP),
            StructField::nullable("UserId", DataType::LONG),
        ])
        .unwrap(),
    )
}

/// Builds a stringified Delta `stats` JSON given EventTime/UserId min/max bounds.
fn stats_json(
    num_records: i64,
    event_time_min: &str,
    event_time_max: &str,
    user_id_min: i64,
    user_id_max: i64,
) -> String {
    format!(
        r#"{{"numRecords":{num_records},"minValues":{{"EventTime":"{event_time_min}","UserId":{user_id_min}}},"maxValues":{{"EventTime":"{event_time_max}","UserId":{user_id_max}}},"nullCount":{{"EventTime":0,"UserId":0}},"tightBounds":true}}"#
    )
}

/// Builds a Delta commit body containing a `commitInfo` plus one Add per `(path, stats)`.
fn commit_with_adds(version: u64, adds: &[(&str, String)]) -> String {
    let mut lines = vec![format!(
        r#"{{"commitInfo":{{"timestamp":1700000000000,"operation":"WRITE","version":{version}}}}}"#
    )];
    for (path, stats) in adds {
        let stats_escaped = serde_json::Value::String(stats.clone()).to_string();
        lines.push(format!(
            r#"{{"add":{{"path":"{path}","size":1024,"modificationTime":1700000000000,"dataChange":true,"partitionValues":{{}},"stats":{stats_escaped}}}}}"#
        ));
    }
    lines.join("\n")
}

/// Builds a Delta commit body containing a `commitInfo` plus a single `remove` action.
fn commit_with_remove(version: u64, path: &str, deletion_timestamp: i64) -> String {
    let commit_info = format!(
        r#"{{"commitInfo":{{"timestamp":{deletion_timestamp},"operation":"DELETE","version":{version}}}}}"#
    );
    let remove = format!(
        r#"{{"remove":{{"path":"{path}","deletionTimestamp":{deletion_timestamp},"dataChange":true,"extendedFileMetadata":false}}}}"#
    );
    format!("{commit_info}\n{remove}")
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_year_timestamp_stats_dont_collapse_skipping(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let _ = create_table_and_load_snapshot(
        &table_path,
        timestamp_stats_schema(),
        engine.as_ref(),
        &[],
    )?;

    // Inject Adds via raw JSON commits using a separate `LocalFileSystem` instance pointing
    // at the same on-disk root the kernel-managed engine uses. The kernel only reads commit
    // files during scan_metadata, so a fake Add (no backing parquet) is fine for this test.
    let store: Arc<delta_kernel::object_store::DynObjectStore> = Arc::new(LocalFileSystem::new());
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;
    let table_url_string = table_url.to_string();

    // Co-locate the malformed `file_B` with a valid `file_D` (Sep 2024, out of predicate
    // range) in the same commit. If the parse error all-nulled the batch, file_D would
    // survive too; with per-cell NULL its stats are preserved and the predicate prunes it.
    add_commit(
        &table_url_string,
        store.as_ref(),
        1,
        commit_with_adds(
            1,
            &[(
                "file_A.parquet",
                stats_json(
                    10,
                    "2024-01-15T00:00:00.000Z",
                    "2024-01-31T00:00:00.000Z",
                    1,
                    100,
                ),
            )],
        ),
    )
    .await?;
    add_commit(
        &table_url_string,
        store.as_ref(),
        2,
        commit_with_adds(
            2,
            &[
                (
                    "file_B.parquet",
                    stats_json(
                        20,
                        "+48690-07-02T22:50:38.211Z",
                        "+48690-07-02T22:50:38.211Z",
                        5,
                        200,
                    ),
                ),
                (
                    "file_D.parquet",
                    stats_json(
                        40,
                        "2024-09-01T00:00:00.000Z",
                        "2024-09-30T00:00:00.000Z",
                        7,
                        70,
                    ),
                ),
            ],
        ),
    )
    .await?;
    add_commit(
        &table_url_string,
        store.as_ref(),
        3,
        commit_with_adds(
            3,
            &[(
                "file_C.parquet",
                stats_json(
                    30,
                    "2025-01-01T00:00:00.000Z",
                    "2025-12-31T00:00:00.000Z",
                    10,
                    300,
                ),
            )],
        ),
    )
    .await?;

    // Predicate: EventTime < 2024-06-01T00:00:00Z AND UserId > 0.
    // 2024-06-01T00:00:00 UTC = 1_717_200_000 seconds since epoch = 1_717_200_000_000_000 us.
    let june_first_2024_us: i64 = 1_717_200_000_000_000;
    let predicate = Arc::new(Pred::and(
        Pred::lt(
            column_expr!("EventTime"),
            Expr::literal(Scalar::Timestamp(june_first_2024_us)),
        ),
        Pred::gt(column_expr!("UserId"), Expr::literal(0i64)),
    ));

    // Expected survivors:
    //   file_A: EventTime min Jan 15 < Jun 1, UserId max 100 > 0, kept.
    //   file_B: EventTime min/max NULL from safe-cast, conservatively kept; UserId max 200 > 0.
    //   file_C: EventTime min Jan 2025 > Jun 2024, pruned.
    //   file_D: EventTime min Sep 2024 > Jun 2024, pruned. Would survive if file_B's bad
    //           stats had collapsed the whole v2 batch to NULL stats.
    assert_eq!(
        surviving_files(&table_path, engine, predicate, use_parallel)?,
        2
    );
    Ok(())
}

/// Round-trip the safe-cast through checkpoint materialization + post-checkpoint JSON +
/// Remove honoring.
///
/// Sequence:
///   v0: CREATE TABLE with `delta.checkpoint.writeStatsAsStruct = true`.
///   v1: One commit containing file_A (valid Jan 2024, would-survive),
///       file_B (malformed extended-year EventTime), and
///       file_E (valid Jan-Dec 2025, would-be-pruned by the predicate).
///       Co-locating them forces the checkpoint-write `parse_json` call to see all three
///       in one batch, which is where the per-cell NULL property matters.
///   checkpoint at v1: `build_stats_parsed_expr` evaluates
///       `COALESCE(stats_parsed, ParseJson(stats))`; safe-cast preserves file_A and file_E
///       stats and NULLs only file_B's `EventTime` min/max.
///   v2: file_C added via JSON (post-checkpoint), exercises
///       `scan/log_replay.rs` `parse_json` on a normal JSON commit.
///   v3: Remove for file_C, exercises Remove reconciliation against a post-checkpoint Add.
///
/// Predicate: `EventTime < 2024-06-01 AND UserId > 0`.
///
/// Expected survivors (driven by both sources):
///   file_A: kept via checkpoint `stats_parsed` (Jan 2024 satisfies predicate).
///   file_B: kept via checkpoint `stats_parsed` (NULL EventTime, conservative).
///   file_E: pruned via checkpoint `stats_parsed` (Jan 2025+). Would survive if file_B's
///           bad stats had blanked the whole v1 batch during checkpoint write.
///   file_C: would-survive predicate via JSON `parse_json`, but the v3 Remove suppresses
///           it during log replay.
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_year_timestamp_round_trip_via_checkpoint_and_remove(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;
    let table_url_string = table_url.to_string();
    let store: Arc<delta_kernel::object_store::DynObjectStore> = Arc::new(LocalFileSystem::new());

    // v0: create table with writeStatsAsStruct enabled so the checkpoint materializes
    // stats_parsed (the column where the safe-cast result lands).
    let _v0 = create_table_and_load_snapshot(
        &table_path,
        timestamp_stats_schema(),
        engine.as_ref(),
        &[("delta.checkpoint.writeStatsAsStruct", "true")],
    )?;

    // v1: file_A + file_B + file_E in one commit. The shared batch is what makes the
    // per-cell NULL property observable during checkpoint write.
    add_commit(
        &table_url_string,
        store.as_ref(),
        1,
        commit_with_adds(
            1,
            &[
                (
                    "file_A.parquet",
                    stats_json(
                        10,
                        "2024-01-15T00:00:00.000Z",
                        "2024-01-31T00:00:00.000Z",
                        1,
                        100,
                    ),
                ),
                (
                    "file_B.parquet",
                    stats_json(
                        20,
                        "+48690-07-02T22:50:38.211Z",
                        "+48690-07-02T22:50:38.211Z",
                        5,
                        200,
                    ),
                ),
                (
                    "file_E.parquet",
                    stats_json(
                        50,
                        "2025-01-01T00:00:00.000Z",
                        "2025-12-31T00:00:00.000Z",
                        10,
                        300,
                    ),
                ),
            ],
        ),
    )
    .await?;

    // Checkpoint at v1: build_stats_parsed_expr COALESCE evaluates parse_json on the v1
    // batch and materializes stats_parsed with file_B's EventTime safe-cast to NULL.
    let snapshot_v1 = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    snapshot_v1.checkpoint(engine.as_ref(), None)?;

    // v2: file_C via JSON commit, post-checkpoint. Reads still see this as JSON stats and
    // run parse_json at scan time.
    add_commit(
        &table_url_string,
        store.as_ref(),
        2,
        commit_with_adds(
            2,
            &[(
                "file_C.parquet",
                stats_json(
                    15,
                    "2024-02-01T00:00:00.000Z",
                    "2024-02-29T00:00:00.000Z",
                    50,
                    80,
                ),
            )],
        ),
    )
    .await?;

    // v3: Remove file_C. Log replay must reconcile the v2 Add with this Remove so that
    // file_C does not appear in the scan output, regardless of predicate.
    add_commit(
        &table_url_string,
        store.as_ref(),
        3,
        commit_with_remove(3, "file_C.parquet", 1_700_000_001_000),
    )
    .await?;

    let june_first_2024_us: i64 = 1_717_200_000_000_000;
    let predicate = Arc::new(Pred::and(
        Pred::lt(
            column_expr!("EventTime"),
            Expr::literal(Scalar::Timestamp(june_first_2024_us)),
        ),
        Pred::gt(column_expr!("UserId"), Expr::literal(0i64)),
    ));

    assert_eq!(
        surviving_files(&table_path, engine, predicate, use_parallel)?,
        2
    );
    Ok(())
}

/// Replace table may change partition and data column types. A scan after replacement should
/// remain compatible with the old addFiles.
#[rstest]
#[case::partition(
    Arc::new(Pred::eq(column_expr!("part"), Expr::literal(1i64))),
    &["new-1.parquet"],
    1,
    101
)]
#[case::stats(
    Arc::new(Pred::eq(column_expr!("value"), Expr::literal(20i64))),
    &["new-2.parquet"],
    1,
    102
)]
#[case::partition_and_stats(
    Arc::new(Pred::and(
        Pred::eq(column_expr!("part"), Expr::literal(3i64)),
        Pred::eq(column_expr!("value"), Expr::literal(30i64)),
    )),
    &["new-3.parquet"],
    1,
    103
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_with_replace_table_schema_change(
    #[case] predicate: PredicateRef,
    #[case] expected_paths: &[&str],
    #[case] expected_active_files: u64,
    #[case] expected_active_bytes: u64,
    #[values(false, true)] use_parallel: bool,
    #[values(false, true)] checkpoint_old_add: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;

    // Commit 1 creates the table with a string partition column.
    let old_schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("part", DataType::STRING),
        StructField::nullable("value", DataType::STRING),
    ])?);
    create_table(&table_path, old_schema, "Test/1.0")
        .with_data_layout(DataLayout::partitioned(["part"]))
        .with_table_properties([("delta.feature.v2Checkpoint", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let store: Arc<delta_kernel::object_store::DynObjectStore> = Arc::new(LocalFileSystem::new());
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;

    // Commit 2 writes addFiles using `old_schema`.
    let old_paths = ["old-1.parquet", "old-2.parquet", "old-3.parquet"];
    let old_adds = [
        json!({
            "commitInfo": {
                "timestamp": 1700000000000i64,
                "operation": "WRITE",
                "version": 1,
            }
        }),
        add_file_action(
            old_paths[0],
            201,                           /* size */
            "not-an-integer-partition-1",  /* partition_value */
            "not-an-integer-data-value-1", /* data_value */
        )?,
        add_file_action(
            old_paths[1],
            202,                           /* size */
            "not-an-integer-partition-2",  /* partition_value */
            "not-an-integer-data-value-2", /* data_value */
        )?,
        add_file_action(
            old_paths[2],
            203,                           /* size */
            "not-an-integer-partition-3",  /* partition_value */
            "not-an-integer-data-value-3", /* data_value */
        )?,
    ];
    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        old_adds.map(|action| action.to_string()).join("\n"),
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    if checkpoint_old_add {
        let checkpoint_spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        });
        snapshot.checkpoint(engine.as_ref(), Some(&checkpoint_spec))?;
    }

    // Commit 3 replaces the table metadata and removes the addFiles.
    let replacement_schema = StructType::try_new(vec![
        StructField::nullable("part", DataType::LONG),
        StructField::nullable("value", DataType::LONG),
    ])?;
    let replacement = [
        json!({
            "commitInfo": {
                "timestamp": 1700000001000i64,
                "operation": "CREATE OR REPLACE TABLE",
                "version": 2,
            }
        }),
        json!({
            "metaData": {
                "id": "replacement-table",
                "format": { "provider": "parquet", "options": {} },
                "schemaString": serde_json::to_string(&replacement_schema)?,
                "partitionColumns": ["part"],
                "configuration": {},
                "createdTime": 1700000001000i64,
            }
        }),
        remove_file_action(old_paths[0])?,
        remove_file_action(old_paths[1])?,
        remove_file_action(old_paths[2])?,
    ];
    add_commit(
        table_url.as_str(),
        store.as_ref(),
        2,
        replacement.map(|action| action.to_string()).join("\n"),
    )
    .await?;

    // Commit 4 writes addFiles with distinct partition values and statistics.
    let new_adds = [
        json!({
            "commitInfo": {
                "timestamp": 1700000002000i64,
                "operation": "WRITE",
                "version": 3,
            }
        }),
        add_file_action(
            "new-1.parquet",
            101, /* size */
            "1", /* partition_value */
            10,  /* data_value */
        )?,
        add_file_action(
            "new-2.parquet",
            102, /* size */
            "2", /* partition_value */
            20,  /* data_value */
        )?,
        add_file_action(
            "new-3.parquet",
            103, /* size */
            "3", /* partition_value */
            30,  /* data_value */
        )?,
    ];
    add_commit(
        table_url.as_str(),
        store.as_ref(),
        3,
        new_adds.map(|action| action.to_string()).join("\n"),
    )
    .await?;

    let reporter = Arc::new(CapturingReporter::default());
    let _guard = install_thread_local_metrics_reporter(reporter.clone());
    assert_eq!(
        surviving_paths(&table_path, engine.clone(), predicate.clone(), use_parallel)?,
        expected_paths
            .iter()
            .map(|path| (*path).to_string())
            .collect::<Vec<_>>()
    );
    let scan_events = reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::ScanMetadataCompleted(event) => Some(event),
            _ => None,
        })
        .collect::<Vec<_>>();
    if use_parallel && checkpoint_old_add {
        assert!(
            scan_events
                .iter()
                .any(|event| event.scan_type == ScanType::ParallelPhase),
            "the V2 sidecar should be processed by the parallel phase"
        );
    }
    assert_eq!(
        scan_events
            .iter()
            .map(|e| e.num_add_files_seen)
            .sum::<u64>(),
        4
    );
    assert_eq!(
        scan_events
            .iter()
            .map(|e| e.num_active_add_files)
            .sum::<u64>(),
        expected_active_files
    );
    assert_eq!(
        scan_events
            .iter()
            .map(|e| e.active_add_files_bytes)
            .sum::<u64>(),
        expected_active_bytes
    );
    assert_eq!(
        scan_events
            .iter()
            .map(|e| e.num_remove_files_seen)
            .sum::<u64>(),
        3
    );
    assert_eq!(
        scan_events
            .iter()
            .map(|e| e.num_predicate_filtered)
            .sum::<u64>(),
        2
    );
    // Negative test: a live addFile with incompatible schema should fail the scan.
    let active_incompatible_add = [
        json!({
            "commitInfo": {
                "timestamp": 1700000003000i64,
                "operation": "WRITE",
                "version": 4,
            }
        }),
        add_file_action(
            "active-incompatible.parquet",
            301,                               /* size */
            "not-an-integer-active-partition", /* partition_value */
            "not-an-integer-active-value",     /* data_value */
        )?,
    ];
    add_commit(
        table_url.as_str(),
        store.as_ref(),
        4,
        active_incompatible_add
            .map(|action| action.to_string())
            .join("\n"),
    )
    .await?;

    if use_parallel {
        // Put the active incompatible Add in a sidecar so the parallel processor rejects it.
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        let checkpoint_spec = CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        });
        snapshot.checkpoint(engine.as_ref(), Some(&checkpoint_spec))?;
    }
    let error = surviving_paths(&table_path, engine, predicate, use_parallel)
        .expect_err("an active incompatible add file should fail the scan");
    assert!(matches!(
        error.downcast_ref::<Error>(),
        Some(Error::ParseError(_, _))
    ));
    Ok(())
}

// === RFC 3339 offset partition values (#2733) ===
//
// A foreign writer can emit a timestamp partition value with a non-UTC RFC 3339 offset. The
// offset must be honored and the value normalized to UTC, e.g. `2024-06-15T14:30:00+05:00`
// denotes 09:30 UTC, not 14:30 UTC.

/// Builds a Delta commit body containing a `commitInfo` plus one stats-less Add per
/// `(path, ts_partition_value)`.
fn commit_with_ts_partitioned_adds(version: u64, adds: &[(&str, &str)]) -> String {
    let mut lines = vec![format!(
        r#"{{"commitInfo":{{"timestamp":1700000000000,"operation":"WRITE","version":{version}}}}}"#
    )];
    for (path, ts) in adds {
        lines.push(format!(
            r#"{{"add":{{"path":"{path}","size":1024,"modificationTime":1700000000000,"dataChange":true,"partitionValues":{{"ts":"{ts}"}}}}}}"#
        ));
    }
    lines.join("\n")
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_pruning_honors_rfc3339_offset_partition_values(
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("ts", DataType::TIMESTAMP),
        StructField::nullable("v", DataType::LONG),
    ])?);
    create_table(&table_path, schema, "Test/1.0")
        .with_data_layout(DataLayout::partitioned(["ts"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // v1: adds in the style of an offset-emitting foreign writer. Pruning never opens data
    // files, so fake paths are fine. The two partition values denote *different* instants:
    // file_A is 2024-06-15T09:30:00Z once its +05:00 offset is honored, file_B is 14:30:00Z.
    let store: Arc<delta_kernel::object_store::DynObjectStore> = Arc::new(LocalFileSystem::new());
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;
    add_commit(
        table_url.as_str(),
        store.as_ref(),
        1,
        commit_with_ts_partitioned_adds(
            1,
            &[
                ("file_A.parquet", "2024-06-15T14:30:00+05:00"),
                ("file_B.parquet", "2024-06-15T14:30:00Z"),
            ],
        ),
    )
    .await?;

    let nine_thirty_utc_us: i64 = 1_718_443_800_000_000; // 2024-06-15T09:30:00Z
    let fourteen_thirty_utc_us: i64 = 1_718_461_800_000_000; // 2024-06-15T14:30:00Z

    // ts == 09:30Z must keep only file_A (its +05:00 value normalized to 09:30 UTC).
    let predicate = Arc::new(Pred::eq(
        column_expr!("ts"),
        Expr::literal(Scalar::Timestamp(nine_thirty_utc_us)),
    ));
    assert_eq!(
        surviving_files(&table_path, engine.clone(), predicate, use_parallel)?,
        1
    );

    // ts == 14:30Z must keep only file_B.
    let predicate = Arc::new(Pred::eq(
        column_expr!("ts"),
        Expr::literal(Scalar::Timestamp(fourteen_thirty_utc_us)),
    ));
    assert_eq!(
        surviving_files(&table_path, engine, predicate, use_parallel)?,
        1
    );
    Ok(())
}

// === All-null file pruning across log-replay sources ===
//
// A file whose queried column is entirely NULL can never satisfy a null-intolerant comparison
// (`=`, `!=`, `<`, `>`, `<=`, `>=`), so data skipping should prune it. `DataSkippingFilter`
// builds its skipping predicate via `eval_sql_where`, which rewrites the `col IS NOT NULL`
// guard to `nullCount != numRecords` -- FALSE for an all-null file, forcing the prune. That
// single guarded predicate is applied to every transformed action batch, so pruning must hold
// no matter how the all-null file's Add is sourced or how its stats are stored: a JSON commit
// (`log_transform` parses `add.stats`), a checkpoint with native `stats_parsed`
// (`writeStatsAsStruct=true`), or a checkpoint with JSON stats (the protocol default, parsed
// from `add.stats` at read time). One exception is captured at the assertion: the parallel scan
// path does not yet read `stats_parsed`, so it cannot skip an all-null file sourced from a
// struct-only checkpoint.
#[derive(Clone, Copy, Debug)]
enum AllNullSource {
    /// All-null file added via a JSON commit, with no checkpoint (the pure delta-log path).
    CommitOnly,
    /// All-null file materialized into the checkpoint as a native `stats_parsed` struct
    /// (`delta.checkpoint.writeStatsAsStruct=true`).
    CheckpointStructStats,
    /// All-null file materialized into the checkpoint with JSON stats (the protocol default;
    /// kernel parses `add.stats` at read time).
    CheckpointJsonStats,
    /// One all-null file in the checkpoint and another in a post-checkpoint JSON commit.
    Both,
}

/// Builds a stringified Delta `stats` JSON for the single `value: long` column.
/// `null_count == num_records` with `bounds == None` mimics an all-null file (Delta writers
/// omit min/max for an all-null column); pass `Some((min, max))` for a normal file.
fn value_stats_json(num_records: i64, null_count: i64, bounds: Option<(i64, i64)>) -> String {
    let min_max = match bounds {
        Some((min, max)) => {
            format!(r#""minValues":{{"value":{min}}},"maxValues":{{"value":{max}}},"#)
        }
        None => r#""minValues":{},"maxValues":{},"#.to_string(),
    };
    format!(
        r#"{{"numRecords":{num_records},{min_max}"nullCount":{{"value":{null_count}}},"tightBounds":true}}"#
    )
}

#[rstest]
// Two representative operators: `=` exercises the direct null-intolerant arm of
// `eval_sql_where` and `!=` the NOT-wrapped arm. The not-all-null guard is operator-agnostic, and
// the unit test `test_all_null_pruning_all_comparison_ops` covers all six operators at the rewrite
// level, so the source/parallel matrix here does not repeat every operator.
#[case::eq(Pred::eq(column_expr!("value"), Expr::literal(5i64)))]
#[case::ne(Pred::ne(column_expr!("value"), Expr::literal(5i64)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_null_files_pruned_regardless_of_source(
    #[case] predicate: Pred,
    #[values(
        AllNullSource::CommitOnly,
        AllNullSource::CheckpointStructStats,
        AllNullSource::CheckpointJsonStats,
        AllNullSource::Both
    )]
    source: AllNullSource,
    #[values(false, true)] use_parallel: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp_dir, table_path, engine) = test_table_setup_mt()?;
    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "value",
        DataType::LONG,
    )])?);
    // For the struct-stats cases, also disable `writeStatsAsJson` so the checkpoint carries ONLY
    // `stats_parsed`. That forces the scan to read the pre-parsed struct (no JSON fallback),
    // genuinely validating that path. The JSON-stats case keeps the defaults
    // (`writeStatsAsJson=true`, `writeStatsAsStruct=false`), so its checkpoint carries only JSON
    // stats. Either way the all-null file's `nullCount` survives into the checkpoint.
    let table_properties: &[(&str, &str)] = match source {
        AllNullSource::CheckpointStructStats | AllNullSource::Both => &[
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.checkpoint.writeStatsAsJson", "false"),
        ],
        AllNullSource::CommitOnly | AllNullSource::CheckpointJsonStats => &[],
    };
    let _ = create_table_and_load_snapshot(&table_path, schema, engine.as_ref(), table_properties)?;
    let store: Arc<delta_kernel::object_store::DynObjectStore> = Arc::new(LocalFileSystem::new());
    let table_url = Url::from_directory_path(&table_path)
        .map_err(|_| "table_path should be a valid file URL")?;
    let table_url_string = table_url.to_string();

    let use_checkpoint = matches!(
        source,
        AllNullSource::CheckpointStructStats
            | AllNullSource::CheckpointJsonStats
            | AllNullSource::Both
    );
    let add_post_checkpoint_commit =
        matches!(source, AllNullSource::CommitOnly | AllNullSource::Both);

    // v1: a matching file (`keep.parquet`, value in [1, 10]) is always present. For
    // checkpoint-sourced scenarios, co-locate an all-null file so it lands in the checkpoint.
    let mut v1_adds = vec![("keep.parquet", value_stats_json(10, 0, Some((1, 10))))];
    if use_checkpoint {
        v1_adds.push(("allnull_ckpt.parquet", value_stats_json(10, 10, None)));
    }
    add_commit(
        &table_url_string,
        store.as_ref(),
        1,
        commit_with_adds(1, &v1_adds),
    )
    .await?;

    // Checkpoint so the all-null file (and the kept file) are sourced from the checkpoint parquet.
    if use_checkpoint {
        let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
        snapshot.checkpoint(engine.as_ref(), None)?;
    }

    // v2: post-checkpoint JSON commit adding an all-null file (the delta-log source).
    if add_post_checkpoint_commit {
        add_commit(
            &table_url_string,
            store.as_ref(),
            2,
            commit_with_adds(
                2,
                &[("allnull_commit.parquet", value_stats_json(10, 10, None))],
            ),
        )
        .await?;
    }

    // `5` can never match an all-null file, but it sits inside the kept file's `[1, 10]` range, so
    // `keep.parquet` always survives; every all-null file should otherwise be pruned.
    let survivors = surviving_paths(&table_path, engine, Arc::new(predicate), use_parallel)?;
    let mut expected = vec!["keep.parquet".to_string()];
    // TODO(#2832): remove this branch once the parallel scan path reads `stats_parsed`.
    // `Scan::parallel_scan_metadata` sets `has_stats_parsed = false` and re-parses JSON
    // `add.stats`, so over a struct-only checkpoint (JSON stats disabled) it has no stats to read
    // and conservatively keeps the checkpoint-sourced all-null file -- a missed optimization,
    // never wrong data; the sequential path reads `stats_parsed` and prunes it. When the parallel
    // checkpoint reader gains `stats_parsed` support, `allnull_ckpt.parquet` will be pruned here
    // too. The commit-sourced all-null file in `Both` carries JSON stats, so it is pruned on both
    // paths.
    let struct_only_checkpoint = matches!(
        source,
        AllNullSource::CheckpointStructStats | AllNullSource::Both
    );
    if use_parallel && struct_only_checkpoint {
        expected.push("allnull_ckpt.parquet".to_string());
    }
    expected.sort();
    assert_eq!(
        survivors, expected,
        "unexpected survivors for source {source:?} (use_parallel={use_parallel})"
    );
    Ok(())
}

#[derive(Serialize)]
struct AddActionFixture {
    add: AddFileFixture,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AddFileFixture {
    path: String,
    size: i64,
    modification_time: i64,
    data_change: bool,
    partition_values: HashMap<String, String>,
    stats: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsFixture<T> {
    num_records: i64,
    min_values: ValueFixture<T>,
    max_values: ValueFixture<T>,
    null_count: ValueFixture<i64>,
}

#[derive(Clone, Serialize)]
struct ValueFixture<T> {
    value: T,
}

fn add_file_action<T: Clone + Serialize>(
    path: &str,
    size: i64,
    partition_value: &str,
    data_value: T,
) -> serde_json::Result<Value> {
    let stats = serde_json::to_string(&StatsFixture {
        num_records: 1,
        min_values: ValueFixture {
            value: data_value.clone(),
        },
        max_values: ValueFixture { value: data_value },
        null_count: ValueFixture { value: 0 },
    })?;
    serde_json::to_value(AddActionFixture {
        add: AddFileFixture {
            path: path.to_string(),
            size,
            modification_time: 1700000000000,
            data_change: true,
            partition_values: HashMap::from([("part".to_string(), partition_value.to_string())]),
            stats,
        },
    })
}

#[derive(Serialize)]
struct RemoveActionFixture {
    remove: RemoveFileFixture,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemoveFileFixture {
    path: String,
    deletion_timestamp: i64,
    data_change: bool,
    extended_file_metadata: bool,
}

fn remove_file_action(path: &str) -> serde_json::Result<Value> {
    serde_json::to_value(RemoveActionFixture {
        remove: RemoveFileFixture {
            path: path.to_string(),
            deletion_timestamp: 1700000001000,
            data_change: true,
            extended_file_metadata: false,
        },
    })
}
