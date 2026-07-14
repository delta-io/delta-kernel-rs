use std::sync::Arc;

use delta_kernel::schema::MetadataColumnSpec;
use delta_kernel::snapshot::ChecksumWriteResult;
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
    DataLayoutConfig, FeatureSet, LogState, TableConfig, VersionTarget,
};
use test_utils::{assert_row_ids_unique, build_snapshot, default_sweep, read_scan};

/// `TestTableBuilder`'s default is one file per data commit with this many rows, so a
/// snapshot at version `v` has exactly `v * ROWS_PER_COMMIT` total rows. File count
/// per commit isn't asserted because partitioned layouts may emit multiple files.
const ROWS_PER_COMMIT: usize = 10;

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

    if snap.table_properties().enable_row_tracking == Some(true) {
        let scan_schema = Arc::new(
            snap.schema()
                .add_metadata_column("row_id", MetadataColumnSpec::RowId)?,
        );
        let scan = snap
            .clone()
            .scan_builder()
            .with_schema(scan_schema)
            .build()?;
        let batches = read_scan(&scan, engine.clone())?;
        assert_row_ids_unique(&batches);
    }

    write_and_assert_checksum(&snap, &log_state, expected_version, engine.as_ref())?;

    Ok(())
}

/// Write a CRC for `snap` and assert the exact outcome. A CRC can be written for virtually any
/// table state; the only cases that block it are a missing remove `size` or a non-incremental
/// operation (e.g. ANALYZE STATS) in the replayed commits, which force `Indeterminate` file
/// stats. The sweep exercises neither, so the write succeeds unless a CRC already exists at this
/// version, which is exactly known from the sweep.
fn write_and_assert_checksum(
    snap: &delta_kernel::snapshot::SnapshotRef,
    log_state: &LogState,
    version: u64,
    engine: &dyn Engine,
) -> DeltaResult<()> {
    let expected_result = if log_state.crcs_at().contains(&version) {
        ChecksumWriteResult::AlreadyExists
    } else {
        ChecksumWriteResult::Written
    };
    let (result, _) = snap.write_checksum(engine)?;
    assert_eq!(result, expected_result);
    Ok(())
}
