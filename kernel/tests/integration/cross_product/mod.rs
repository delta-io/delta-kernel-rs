//! Default cross-product sweep (v1).
//!
//! Drives a single read-and-scan assertion across the canonical
//! `(LogState x FeatureSet x VersionTarget)` matrix via the [`default_sweep`]
//! rstest_reuse template. Each combination expands to its own test runner case so
//! failures are isolated and report per-case. Future predicate, scan-metadata, and
//! round-trip-checkpoint sweeps reuse the same template; updating it on either
//! side flows through automatically.
//!
//! v1 is read-only; other assertion types land as the framework grows (issue
//! #2526).

use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use rstest::rstest;
use rstest_reuse::apply;
use test_utils::table_builder::{
    test_table, FeatureSet, LogState, VersionTarget, DEFAULT_SWEEP_MID_VERSION,
};
use test_utils::{build_snapshot, default_sweep, read_scan};

/// Each data commit (versions `1..=latest`) writes [`TestTableBuilder`]'s
/// default 1-file-of-10-rows, so the row count at any snapshot version `v` is
/// exactly `v * ROWS_PER_COMMIT`.
const ROWS_PER_COMMIT: usize = 10;

#[apply(default_sweep)]
fn read_cross_product(
    log_state: LogState,
    feature_set: FeatureSet,
    version_target: VersionTarget,
) -> DeltaResult<()> {
    let table = test_table(log_state.clone(), feature_set.clone());
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
        "snapshot version mismatch for {log_state} + {feature_set} + {version_target}",
    );

    let scan = snap.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let expected_rows = expected_version as usize * ROWS_PER_COMMIT;
    assert_eq!(
        rows, expected_rows,
        "row count mismatch for {log_state} + {feature_set} + {version_target}",
    );

    Ok(())
}
