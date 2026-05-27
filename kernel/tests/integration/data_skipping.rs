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
use delta_kernel::expressions::{
    column_expr, Expression as Expr, Predicate as Pred, PredicateRef, Scalar,
};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::{AfterSequentialScanMetadata, ParallelScanMetadata};
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Snapshot, SnapshotRef};
use rstest::rstest;
use test_utils::{
    add_commit, create_table_and_load_snapshot, test_table_setup_mt, write_batch_to_table,
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
    let table_url_string = format!("file://{}/", std::path::Path::new(&table_path).display());

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
