use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use rstest_reuse::apply;
use test_utils::table_builder::{
    all_features_cm_id, all_features_cm_name, checkpoint_at_end, checkpoint_at_end_crc_at_end,
    checkpoint_at_end_crc_at_end_post_cleanup, checkpoint_at_end_crc_at_mid_post_cleanup,
    checkpoint_at_end_no_hint, checkpoint_at_end_no_hint_post_cleanup,
    checkpoint_at_end_post_cleanup, checkpoint_json_stats, checkpoint_mid,
    checkpoint_mid_crc_at_mid_post_cleanup, checkpoint_mid_no_hint,
    checkpoint_mid_no_hint_post_cleanup, checkpoint_mid_post_cleanup, checkpoint_struct_stats,
    clustered, commits_only, crc_at_end, crc_at_mid, no_checkpoint_stats, no_features, partitioned,
    test_table, two_checkpoints_stale_hint, two_checkpoints_stale_hint_post_cleanup, unpartitioned,
    version_at_mid, version_incremental_to_latest, version_latest, DataLayoutConfig, FeatureSet,
    LogState, TableConfig, VersionTarget,
};
use test_utils::{build_snapshot, default_sweep, read_scan};

/// `TestTableBuilder`'s default is one file per data commit with this many rows, so a
/// snapshot at version `v` has exactly `v * ROWS_PER_COMMIT` total rows. File count
/// per commit isn't asserted because partitioned layouts may emit multiple files.
const ROWS_PER_COMMIT: usize = 10;

/// `default_sweep` is the canonical `{LogState x FeatureSet x DataLayoutConfig x
/// TableConfig x VersionTarget}` cross-product defined in `test_utils`. Each
/// combination expands into its own test runner case. Invoking `test_table` here also
/// exercises the write path that produces each table state.
#[apply(default_sweep)]
fn test_cross_product_read_write(
    log_state: LogState,
    feature_set: FeatureSet,
    data_layout: DataLayoutConfig,
    table_config: TableConfig,
    version_target: VersionTarget,
) -> DeltaResult<()> {
    let table = test_table(log_state.clone(), feature_set, data_layout, table_config);
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
    let snap = build_snapshot!(version_target, table.table_root(), engine.as_ref());

    let expected_version = match &version_target {
        VersionTarget::Latest | VersionTarget::IncrementalToLatest { .. } => {
            log_state.latest_version()
        }
        VersionTarget::AtVersion(v) => *v,
    };
    assert_eq!(snap.version(), expected_version);

    let scan = snap.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, expected_version as usize * ROWS_PER_COMMIT);

    Ok(())
}
