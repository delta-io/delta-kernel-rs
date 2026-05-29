use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::history_manager::latest_version_as_of;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
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
    version_at_mid, version_at_timestamp_max, version_incremental_from_mid_to_pre_latest,
    version_incremental_to_latest, version_latest, DataLayoutConfig, FeatureSet, LogState,
    TableConfig, VersionTarget,
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
        VersionTarget::Latest
        | VersionTarget::IncrementalToLatest { .. }
        | VersionTarget::AtTimestamp(_) => log_state.latest_version(),
        VersionTarget::AtVersion(v) => *v,
        VersionTarget::IncrementalFrom { to, .. } => *to,
    };
    assert_eq!(snap.version(), expected_version);

    let scan = snap.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, expected_version as usize * ROWS_PER_COMMIT);

    Ok(())
}

/// Companion to the `version_at_timestamp_max()` row in the default sweep, which is a
/// smoke test (`i64::MAX` trivially resolves to latest). This test exercises
/// [`VersionTarget::AtTimestamp`] resolving to an intermediate version by writing the
/// table on local filesystem and assigning each commit file an explicit, distinct
/// modification time via [`std::fs::File::set_modified`]. The `InMemory` store backing
/// the default sweep collapses successive `put` timestamps to a single millisecond, so
/// this case is unreachable there.
#[test]
fn test_at_timestamp_resolves_to_intermediate_version() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    // v0: CreateTable
    let schema = get_simple_int_schema();
    let create_result = create_table(&table_path, schema, "AtTimestampTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;
    let mut snap = match create_result {
        CommitResult::CommittedTransaction(c) => c.post_commit_snapshot().unwrap().clone(),
        _ => panic!("create_table should commit"),
    };

    // v1..=4: noop commits (each writes a metaData-free, add-free commit JSON).
    for _ in 1..=4 {
        let txn = test_utils::begin_transaction(snap.clone(), engine.as_ref())?
            .with_engine_info("AtTimestampTest");
        snap = match txn.commit(engine.as_ref())? {
            CommitResult::CommittedTransaction(c) => c.post_commit_snapshot().unwrap().clone(),
            _ => panic!("commit should succeed"),
        };
    }

    // Pin each commit's mtime to a distinct, monotonic value (in ms).
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let log_dir = table_url.to_file_path().unwrap().join("_delta_log");
    let commit_ts_ms: Vec<i64> = (0..=4).map(|v| (v as i64 + 1) * 1000).collect();
    for (v, ts) in commit_ts_ms.iter().enumerate() {
        let file_path = log_dir.join(format!("{:020}.json", v));
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(*ts as u64);
        file.set_modified(time).unwrap();
    }

    // 2500ms lands strictly between v1 (2000) and v2 (3000): resolves to v1.
    let latest_snap = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let v = latest_version_as_of(&latest_snap, engine.as_ref(), 2500)?;
    assert_eq!(
        v, 1,
        "ts=2500 should resolve to v1 (latest with ts <= 2500)"
    );

    // build_snapshot! end-to-end: at_timestamp(2500) should produce a snapshot at v1.
    let target = VersionTarget::AtTimestamp(2500);
    let snap_at_ts = build_snapshot!(target, &table_path, engine.as_ref());
    assert_eq!(snap_at_ts.version(), 1);

    // Boundary checks: i64::MAX -> latest; before-all -> error.
    let target_max = VersionTarget::AtTimestamp(i64::MAX);
    let snap_max = build_snapshot!(target_max, &table_path, engine.as_ref());
    assert_eq!(snap_max.version(), 4);

    let before_all = latest_version_as_of(&latest_snap, engine.as_ref(), 0);
    assert!(
        before_all.is_err(),
        "ts before earliest commit should error, got {before_all:?}"
    );

    Ok(())
}
