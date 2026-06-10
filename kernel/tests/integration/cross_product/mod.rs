use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use rstest_reuse::apply;
use test_utils::delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::table_builder::{
    all_features_cm_id, all_features_cm_name, checkpoint_at_end, checkpoint_at_end_crc_at_end,
    checkpoint_at_end_no_hint, checkpoint_at_end_no_hint_post_cleanup,
    checkpoint_at_end_post_cleanup, checkpoint_json_stats, checkpoint_mid,
    checkpoint_mid_crc_above_mid_post_cleanup, checkpoint_mid_crc_at_end_post_cleanup,
    checkpoint_mid_crc_at_mid_post_cleanup, checkpoint_mid_no_hint,
    checkpoint_mid_no_hint_post_cleanup, checkpoint_mid_post_cleanup, checkpoint_struct_stats,
    clustered, commits_only, crc_at_end, crc_at_mid, no_checkpoint_stats, no_features, partitioned,
    test_table, two_checkpoints_stale_hint, two_checkpoints_stale_hint_post_cleanup, unpartitioned,
    version_at_mid, version_at_timestamp_max, version_incremental_from_mid_to_latest,
    version_incremental_from_mid_to_pre_latest, version_latest, DataLayoutConfig, FeatureSet,
    LogState, TableConfig, VersionTarget,
};
use test_utils::{build_snapshot, default_sweep, read_scan, test_table_setup};

use crate::common::write_utils::get_simple_int_schema;

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
        // `i64::MAX` is the only timestamp in the sweep; it always resolves to latest.
        VersionTarget::AtTimestamp(ts) if *ts == i64::MAX => log_state.latest_version(),
        VersionTarget::AtVersion(v) => *v,
        VersionTarget::IncrementalFrom { to, .. } => *to,
        VersionTarget::AtTimestamp(ts) => {
            panic!("sweep only uses AtTimestamp(i64::MAX), got {ts}")
        }
    };
    assert_eq!(snap.version(), expected_version);

    let scan = snap.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, expected_version as usize * ROWS_PER_COMMIT);

    Ok(())
}

/// Verifies that [`VersionTarget::AtTimestamp`] wires through `build_snapshot!` to the
/// version `latest_version_as_of` resolves the timestamp to. Resolution correctness for
/// the many timestamp/commit-layout combinations is covered by the `history_manager` unit
/// tests; this only checks the `TestTableBuilder` integration end-to-end. The default
/// sweep's `version_at_timestamp_max()` row can't reach an intermediate version because
/// `InMemory` collapses successive `put` timestamps to a single millisecond, so this
/// writes on the local filesystem and sets each commit's modification time explicitly.
#[test]
fn test_at_timestamp_resolves_to_intermediate_version() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: CreateTable
    let schema = get_simple_int_schema();
    let mut snap = create_table(&table_path, schema, "AtTimestampTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    // v1..=4: noop commits (each writes a metaData-free, add-free commit JSON).
    for _ in 1..=4 {
        snap = test_utils::begin_transaction(snap.clone(), engine.as_ref())?
            .with_engine_info("AtTimestampTest")
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
    }

    // Set each commit's mtime to a distinct, monotonic value (in ms).
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let log_dir = table_url.to_file_path().unwrap().join("_delta_log");
    for v in 0..=4u64 {
        let file_path = log_dir.join(format!("{v:020}.json"));
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis((v + 1) * 1000);
        file.set_modified(time).unwrap();
    }

    // 2500ms lands strictly between v1 (2000) and v2 (3000), so it resolves to v1.
    let target = VersionTarget::AtTimestamp(2500);
    let snap_at_ts = build_snapshot!(target, &table_path, engine.as_ref());
    assert_eq!(snap_at_ts.version(), 1);

    // i64::MAX resolves to latest, mirroring the default sweep's smoke row.
    let target_max = VersionTarget::AtTimestamp(i64::MAX);
    let snap_max = build_snapshot!(target_max, &table_path, engine.as_ref());
    assert_eq!(snap_max.version(), 4);

    Ok(())
}
