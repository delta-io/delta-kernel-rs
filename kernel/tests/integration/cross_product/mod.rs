use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use rstest_reuse::apply;
use test_utils::table_builder::{
    all_features_cm_id, all_features_cm_name, checkpoint_at_end, checkpoint_at_end_no_hint,
    checkpoint_mid, checkpoint_mid_no_hint, clustered, commits_only, json_stats, no_features,
    no_stats, partitioned, post_cleanup, struct_stats, test_table, two_checkpoints_stale_hint,
    unpartitioned, version_at_mid, version_incremental_to_latest, version_latest, DataLayoutConfig,
    FeatureSet, LogState, VersionTarget,
};
use test_utils::{build_snapshot, default_sweep, read_scan};

/// `TestTableBuilder`'s default is one file per data commit with this many rows, so a
/// snapshot at version `v` has exactly `v * ROWS_PER_COMMIT` total rows. File count
/// per commit isn't asserted because partitioned layouts may emit multiple files.
const ROWS_PER_COMMIT: usize = 10;

/// `default_sweep` is the canonical `{LogState x FeatureSet x DataLayoutConfig x
/// VersionTarget}` cross-product defined in `test_utils`. Each combination expands
/// into its own test runner case. Invoking `test_table` here also exercises the
/// write path that produces each table state.
#[apply(default_sweep)]
fn test_cross_product_read_write(
    log_state: LogState,
    feature_set: FeatureSet,
    data_layout: DataLayoutConfig,
    version_target: VersionTarget,
) -> DeltaResult<()> {
    let table = test_table(log_state.clone(), feature_set.clone(), data_layout.clone());
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(table.store().clone()).build());
    let snap = build_snapshot!(version_target, table.table_root(), engine.as_ref());

    let expected_version = match &version_target {
        VersionTarget::Latest => log_state.latest_version(),
        VersionTarget::AtVersion(v) => *v,
        VersionTarget::IncrementalToLatest { .. } => log_state.latest_version(),
    };
    assert_eq!(
        snap.version(),
        expected_version,
        "snapshot version mismatch for {log_state} + {feature_set} + {data_layout} + {version_target}",
    );

    let scan = snap.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let expected_rows = expected_version as usize * ROWS_PER_COMMIT;
    assert_eq!(
        rows, expected_rows,
        "row count mismatch for {log_state} + {feature_set} + {data_layout} + {version_target}",
    );

    Ok(())
}
