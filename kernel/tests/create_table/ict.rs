//! In-Commit Timestamp (ICT) integration tests for the CreateTable API.
//!
//! Tests that creating a table with `delta.enableInCommitTimestamps=true` automatically adds the
//! `inCommitTimestamp` feature to the protocol (writer-only) and that the snapshot exposes a
//! valid `inCommitTimestamp` value.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::{
    TableFeature, TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION,
};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use test_utils::test_table_setup;

fn assert_ict_protocol(snapshot: &Snapshot, expect_supported: bool) {
    let table_config = snapshot.table_configuration();
    assert_eq!(
        table_config.is_feature_supported(&TableFeature::InCommitTimestamp),
        expect_supported,
    );
    if !expect_supported {
        return;
    }
    let protocol = table_config.protocol();
    assert!(
        protocol.min_reader_version() >= TABLE_FEATURES_MIN_READER_VERSION,
        "Reader version should be at least {}",
        TABLE_FEATURES_MIN_READER_VERSION
    );
    assert!(
        protocol.min_writer_version() >= TABLE_FEATURES_MIN_WRITER_VERSION,
        "Writer version should be at least {}",
        TABLE_FEATURES_MIN_WRITER_VERSION
    );
    // ICT is writer-only: it should be in writer features but not reader features
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

#[rstest::rstest]
#[case::ict_enabled(&[("delta.enableInCommitTimestamps", "true")], true, true)]
#[case::no_ict(&[], false, false)]
#[case::feature_signal_only(&[("delta.feature.inCommitTimestamp", "supported")], true, false)]
fn test_create_table_ict(
    #[case] properties: &[(&str, &str)],
    #[case] expect_ict_feature_supported: bool,
    #[case] expect_ict_enabled: bool,
) -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let committed = create_table(&table_path, super::simple_schema()?, "Test/1.0")
        .with_table_properties(properties.iter().copied())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_committed();

    // Verify via post-commit snapshot (reads ICT from in-memory CRC delta)
    let post_commit_snapshot = committed
        .post_commit_snapshot()
        .expect("should have snapshot");
    assert_ict_protocol(post_commit_snapshot, expect_ict_feature_supported);

    let post_commit_ict = post_commit_snapshot.get_in_commit_timestamp(engine.as_ref())?;

    // Verify via fresh snapshot loaded from disk (reads ICT from commit JSON)
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let disk_snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_ict_protocol(&disk_snapshot, expect_ict_feature_supported);

    let disk_ict = disk_snapshot.get_in_commit_timestamp(engine.as_ref())?;

    if expect_ict_enabled {
        let post_ts = post_commit_ict.expect("post-commit ICT should be present when enabled");
        let disk_ts = disk_ict.expect("disk ICT should be present when enabled");
        assert!(
            post_ts > 1_577_836_800_000,
            "inCommitTimestamp {post_ts} should be a recent wall-clock timestamp"
        );
        assert_eq!(
            post_ts, disk_ts,
            "post-commit and disk ICT values should match"
        );
    } else {
        assert!(
            post_commit_ict.is_none(),
            "post-commit ICT should be None when not enabled"
        );
        assert!(
            disk_ict.is_none(),
            "disk ICT should be None when not enabled"
        );
    }

    Ok(())
}
