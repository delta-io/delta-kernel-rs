//! In-Commit Timestamp (ICT) integration tests for the CreateTable API.
//!
//! Tests that creating a table with `delta.enableInCommitTimestamps=true` automatically adds the
//! `inCommitTimestamp` feature to the protocol (writer-only) and that the snapshot exposes a
//! valid `inCommitTimestamp` value.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine};
use test_utils::test_table_setup;

/// Asserts the ICT protocol and enablement state of a snapshot, returning the ICT value.
fn assert_ict_state(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    expect_supported: bool,
    expect_enabled: bool,
    test_start_ms: i64,
) -> DeltaResult<Option<i64>> {
    let table_config = snapshot.table_configuration();
    assert_eq!(
        table_config.is_feature_supported(&TableFeature::InCommitTimestamp),
        expect_supported,
    );
    if expect_supported {
        let protocol = table_config.protocol();
        assert!(
            protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION,
            "Reader version should be at least {TABLE_FEATURES_MIN_READER_VERSION}"
        );
        assert!(
            protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION,
            "Writer version should be at least {TABLE_FEATURES_MIN_WRITER_VERSION}"
        );
        assert!(
            protocol
                .writer_features()
                .is_some_and(|f| f.contains(&TableFeature::InCommitTimestamp)),
            "inCommitTimestamp should be in writer features"
        );
        assert!(
            !protocol
                .reader_features()
                .is_some_and(|f| f.contains(&TableFeature::InCommitTimestamp)),
            "inCommitTimestamp should NOT be in reader features"
        );
    }

    let ict = snapshot.get_in_commit_timestamp(engine)?;
    if expect_enabled {
        let ts = ict.expect("ICT should be present when enabled");
        assert!(
            ts >= test_start_ms,
            "inCommitTimestamp {ts} should be >= test start time {test_start_ms}"
        );
    } else {
        assert!(ict.is_none(), "ICT should be None when not enabled");
    }
    Ok(ict)
}

#[rstest::rstest]
#[case::ict_enabled(&[("delta.enableInCommitTimestamps", "true")], true, true)]
#[case::no_ict(&[], false, false)]
#[case::feature_signal_only(&[("delta.feature.inCommitTimestamp", "supported")], true, false)]
fn test_create_table_ict(
    #[case] properties: &[(&str, &str)],
    #[case] expect_ict_feature_supported: bool,
    #[case] expect_ict_enabled: bool,
) -> DeltaResult<()> {
    let test_start_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let committed = create_table(&table_path, super::simple_schema()?, "Test/1.0")
        .with_table_properties(properties.iter().copied())
        .build(engine.as_ref(), Arc::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Verify via post-commit snapshot (reads ICT from in-memory CRC delta)
    let post_snapshot = committed
        .post_commit_snapshot()
        .expect("should have snapshot");
    let post_ict = assert_ict_state(
        post_snapshot,
        engine.as_ref(),
        expect_ict_feature_supported,
        expect_ict_enabled,
        test_start_ms,
    )?;

    // Verify via fresh snapshot loaded from disk (reads ICT from commit JSON)
    let disk_snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let disk_ict = assert_ict_state(
        &disk_snapshot,
        engine.as_ref(),
        expect_ict_feature_supported,
        expect_ict_enabled,
        test_start_ms,
    )?;

    assert_eq!(
        post_ict, disk_ict,
        "post-commit and disk ICT values should match"
    );

    Ok(())
}
