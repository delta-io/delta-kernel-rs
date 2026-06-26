use std::sync::Arc;

use delta_kernel::arrow::array::{Array, Int32Array, RecordBatch};
use delta_kernel::expressions::{lit, Expression, PredicateRef};
use delta_kernel::schema::MetadataColumnSpec;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use rstest_reuse::apply;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{
    all_features_cm_id, all_features_cm_name, checkpoint_at_end, checkpoint_at_end_crc_at_end,
    checkpoint_at_end_no_hint, checkpoint_at_end_no_hint_post_cleanup,
    checkpoint_at_end_post_cleanup, checkpoint_mid, checkpoint_mid_crc_above_mid_post_cleanup,
    checkpoint_mid_crc_at_end_post_cleanup, checkpoint_mid_crc_at_mid_post_cleanup,
    checkpoint_mid_no_hint, checkpoint_mid_no_hint_post_cleanup, checkpoint_mid_post_cleanup,
    clustered, commits_only, crc_at_end, crc_at_mid, no_checkpoint_stats, no_features,
    num_indexed_cols_all, num_indexed_cols_narrow, num_indexed_cols_zero, partitioned,
    stats_columns_empty, stats_columns_reordered, test_table, two_checkpoints_stale_hint,
    two_checkpoints_stale_hint_post_cleanup, unpartitioned, version_at_mid,
    version_at_timestamp_max, version_incremental_from_mid_to_latest,
    version_incremental_from_mid_to_pre_latest, version_latest, with_json_stats, with_struct_stats,
    DataLayoutConfig, FeatureSet, LogState, TableConfig, TestTableBuilder, VersionTarget,
    NULL_RATE_EVERY_NTH,
};
use test_utils::{assert_row_ids_unique, build_snapshot, default_sweep, read_scan};

/// `TestTableBuilder`'s default is one file per data commit with this many rows, so a
/// snapshot at version `v` has exactly `v * ROWS_PER_COMMIT` total rows. File count
/// per commit isn't asserted because partitioned layouts may emit multiple files.
const ROWS_PER_COMMIT: usize = 10;

/// Data-skipping predicate threshold. The generator fills the skipping column with
/// `version * 1000 + row`, so `>= 2000` keeps versions >= 2; version 1's max (well under
/// 2000) makes its file prunable by min/max skipping.
const SKIP_THRESHOLD: i32 = 2000;

/// Counts non-null rows of the `INTEGER` column `col` whose value is `>= threshold`.
fn count_at_least(batches: &[RecordBatch], col: &str, threshold: i32) -> usize {
    batches
        .iter()
        .map(|batch| {
            let idx = batch
                .schema()
                .index_of(col)
                .expect("skipping column present in scan output");
            let array = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("INTEGER column reads back as Int32Array");
            (0..array.len())
                .filter(|&i| array.is_valid(i) && array.value(i) >= threshold)
                .count()
        })
        .sum()
}

/// `default_sweep` is the canonical `{LogState x FeatureSet x (DataLayoutConfig,
/// TableConfig) x VersionTarget}` cross-product defined in `test_utils`. Data layout and
/// stats config share one bundled axis (see `layout_config_values`) rather than being
/// crossed, to keep the case count within budget. Each combination expands into its own
/// test runner case. Invoking `test_table` here also exercises the write path that produces
/// each table state.
#[apply(default_sweep)]
fn test_cross_product_read_write(
    log_state: LogState,
    feature_set: FeatureSet,
    layout_config: (DataLayoutConfig, TableConfig),
    version_target: VersionTarget,
) -> DeltaResult<()> {
    let (data_layout, table_config) = layout_config;
    let skip_col = data_layout.skipping_column();
    let table = test_table(log_state.clone(), feature_set, data_layout, table_config);
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
    let snap = build_snapshot!(version_target, table.table_root(), engine.as_ref());

    let expected_version = match &version_target {
        VersionTarget::Latest | VersionTarget::IncrementalToLatest { .. } => {
            log_state.latest_version()
        }
        // `i64::MAX` is the only timestamp in the sweep; it always resolves to latest.
        VersionTarget::AtTimestamp(ts) if *ts == i64::MAX => log_state.latest_version(),
        VersionTarget::AtVersion(v) => *v,
        VersionTarget::IncrementalFrom { to, .. } => *to,
        VersionTarget::AtTimestamp(ts) => {
            panic!("sweep only uses AtTimestamp(i64::MAX), got {ts}")
        }
    };
    assert_eq!(snap.version(), expected_version);

    let scan = snap.clone().scan_builder().build()?;
    let batches = read_scan(&scan, engine.clone())?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, expected_version as usize * ROWS_PER_COMMIT);

    // Min/max data-skipping predicate on the integer column. Data skipping is file-level, not
    // row-level, so the scan may return extra rows from files it kept; residual-filtering the
    // result here asserts skipping is *sound* (no row satisfying the predicate is dropped)
    // uniformly across stats configs. Matching rows live only in versions >= 2, whose files
    // are never validly skippable, so the matching count is fixed regardless of whether
    // version 1 was pruned.
    let predicate = Arc::new(Expression::column([skip_col]).ge(lit(SKIP_THRESHOLD)));
    let filtered_scan = snap
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;
    let filtered_batches = read_scan(&filtered_scan, engine.clone())?;
    let nulls_per_file = (0..ROWS_PER_COMMIT)
        .filter(|i| i % NULL_RATE_EVERY_NTH == 0)
        .count();
    let matching_versions = expected_version.saturating_sub(1) as usize;
    assert_eq!(
        count_at_least(&filtered_batches, skip_col, SKIP_THRESHOLD),
        matching_versions * (ROWS_PER_COMMIT - nulls_per_file),
        "data skipping dropped rows that satisfy the predicate",
    );

    if snap.table_properties().enable_row_tracking == Some(true) {
        let scan_schema = Arc::new(
            snap.schema()
                .add_metadata_column("row_id", MetadataColumnSpec::RowId)?,
        );
        let scan = snap.scan_builder().with_schema(scan_schema).build()?;
        let batches = read_scan(&scan, engine)?;
        assert_row_ids_unique(&batches);
    }

    Ok(())
}

/// Counts the files surviving data skipping for `predicate`. Skipping is file-level, so this
/// reflects effectiveness (how many files the predicate pruned), unlike the sweep's
/// row-level soundness check.
fn count_scan_files(
    snap: &Arc<Snapshot>,
    engine: &dyn Engine,
    predicate: PredicateRef,
) -> DeltaResult<usize> {
    let scan = snap
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;
    let mut files = 0usize;
    for scan_metadata in scan.scan_metadata(engine)? {
        files = scan_metadata?.visit_scan_files(files, |count, _| *count += 1)?;
    }
    Ok(files)
}

/// Effectiveness companion to the sweep's soundness assertion: with stats on the predicate
/// column, a `>= SKIP_THRESHOLD` predicate must actually prune the version-1 file via min/max
/// data skipping (whereas the sweep only proves skipping never drops a matching row).
#[test]
fn test_min_max_skipping_prunes_files() -> DeltaResult<()> {
    // `num_indexed_cols_all` writes stats for every leaf column, so `int_col` is indexed.
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_table_config(num_indexed_cols_all())
        .build()?;
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
    let snap = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;

    // v1 = [1000, 1009], v2 = [2000, 2009], v3 = [3000, 3009]. `int_col >= 2000` excludes v1.
    let predicate = Arc::new(Expression::column(["int_col"]).ge(lit(SKIP_THRESHOLD)));
    let files = count_scan_files(&snap, engine.as_ref(), predicate)?;
    assert_eq!(
        files, 2,
        "version-1 file should be pruned by min/max skipping"
    );

    Ok(())
}

/// Partition pruning effectiveness: a predicate on a partition column prunes files whose
/// partition value can't match, using the Add action's partition values rather than min/max
/// stats. Independent of `numIndexedCols`, since partition values are always present.
#[test]
fn test_partition_pruning_skips_files() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(3))
        .with_data_layout(partitioned())
        .build()?;
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
    let snap = Snapshot::builder_for(table.table_root()).build(engine.as_ref())?;

    // The `part_long` partition value is `version * 1_000_000` (v1=1M, v2=2M, v3=3M), so
    // `part_long >= 2_000_000` prunes version 1's partition.
    let predicate = Arc::new(Expression::column(["part_long"]).ge(lit(2_000_000i64)));
    let files = count_scan_files(&snap, engine.as_ref(), predicate)?;
    assert_eq!(files, 2, "version-1 partition should be pruned");

    Ok(())
}
