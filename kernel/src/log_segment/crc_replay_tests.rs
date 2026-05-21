//! Tests for `LogSegment::build_crc_from_stale` (incremental CRC catch-up via reverse log
//! replay). Uses the fluent builder from `crc_tests` to write log files and construct a
//! `LogSegment` directly (no `Snapshot` involvement).

use super::crc_tests::{
    metadata_a, metadata_b, metadata_ict, protocol_v2, protocol_v2_dv, protocol_v2_ict, CrcReadTest,
};
use crate::crc::Crc;
use crate::Error;

// === Protocol / metadata ===

#[tokio::test]
async fn test_incrementally_build_crc_newest_protocol_and_metadata_win() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_p_m(1, protocol_v2_dv(), metadata_b())
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    assert_eq!(crc.protocol, protocol_v2_dv());
    assert_eq!(crc.metadata, metadata_b());
}

#[tokio::test]
async fn test_incrementally_build_crc_preserves_base_protocol_when_segment_has_none() {
    // Segment has no protocol action in (0, latest]; result keeps the base's protocol.
    let base = Crc {
        protocol: protocol_v2(),
        metadata: metadata_a(),
        ..Default::default()
    };
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta(1)
        .build()
        .await
        .incrementally_build_crc(&base, None)
        .unwrap();
    assert_eq!(crc.protocol, protocol_v2());
    assert_eq!(crc.metadata, metadata_a());
}

// === ICT ===

#[tokio::test]
async fn test_incrementally_build_crc_ict_captured_from_newest_commit_only() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2_ict(), metadata_ict())
        .delta_with_ict(1, 1000)
        .delta_with_ict(2, 2000)
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    assert_eq!(crc.in_commit_timestamp_opt, Some(2000));
}

#[tokio::test]
async fn test_incrementally_build_crc_ict_none_when_newest_commit_has_no_commit_info() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2_ict(), metadata_ict())
        .delta_with_ict(1, 1000)
        .delta_with_txn(2, "app", 1, None)
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    assert_eq!(crc.in_commit_timestamp_opt, None);
}

// === Domain metadata ===

#[tokio::test]
async fn test_incrementally_build_crc_dm_newest_wins() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_dm(1, "d", "old", false)
        .delta_with_dm(2, "d", "new", false)
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    let map = crc.domain_metadata_state.expect_partial();
    assert_eq!(map["d"].configuration(), "new");
    assert!(!map["d"].is_removed());
}

#[tokio::test]
async fn test_incrementally_build_crc_dm_tombstone_removes_entry_from_base() {
    use crate::actions::DomainMetadata;
    use crate::crc::DomainMetadataState;

    // Base has "d" in its DM map; the tombstone in the segment must remove it.
    let base = Crc {
        domain_metadata_state: DomainMetadataState::Complete(
            [(
                "d".to_string(),
                DomainMetadata::new("d".to_string(), "stale".to_string()),
            )]
            .into(),
        ),
        ..Default::default()
    };
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_dm(1, "d", "", true)
        .build()
        .await
        .incrementally_build_crc(&base, None)
        .unwrap();
    // The tombstone consumed the entry; the (still Complete) map no longer contains "d".
    assert!(!crc
        .domain_metadata_state
        .expect_complete()
        .contains_key("d"));
}

// === SetTransaction ===

#[tokio::test]
async fn test_incrementally_build_crc_txn_newest_wins() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_txn(1, "app", 1, Some(100))
        .delta_with_txn(2, "app", 42, Some(200))
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    let map = crc.set_transaction_state.expect_partial();
    assert_eq!(map["app"].version, 42);
    assert_eq!(map["app"].last_updated, Some(200));
}

// === File stats ===

#[tokio::test]
async fn test_incrementally_build_crc_adds_accumulate() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_add(1, "a", 100)
        .delta_with_add(2, "b", 200)
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 2);
    assert_eq!(stats.table_size_bytes(), 300);
}

#[tokio::test]
async fn test_incrementally_build_crc_adds_and_removes_net_out() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_add(1, "a", 100)
        .delta_with_remove(2, "old", Some(30))
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 0); // +1 -1
    assert_eq!(stats.table_size_bytes(), 70); // +100 -30
}

#[tokio::test]
async fn test_incrementally_build_crc_remove_with_no_size_trips_indeterminate() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_remove(1, "orphan", None)
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    assert!(crc.file_stats_state().is_indeterminate());
}

#[tokio::test]
async fn test_incrementally_build_crc_unsafe_op_with_add_in_same_commit_trips_indeterminate() {
    // Single commit carrying an add AND an unsafe operation. Without `delta_with_add_and_op`,
    // separate `delta_with_add(1, ...)` + `delta_with_op(1, ...)` calls would collide on
    // file `001.json` and the builder's clobber-detection would panic.
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_add_and_op(1, "a", 100, "ANALYZE STATS")
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    assert!(crc.file_stats_state().is_indeterminate());
}

// === Stale CRC on disk (real histogram, no mocking) ===
//
// Pattern: write a CRC at version V (file_sizes drives a real on-disk `FileSizeHistogram`),
// then more commits at V+1..N without writing CRCs.
// `incrementally_build_crc_from_disk_crc_at(V, None)` loads the v=V CRC from disk (real
// `Crc` with real histogram) and advances it to the latest version.

#[tokio::test]
async fn test_incrementally_build_crc_from_disk_advances_file_stats() {
    let built = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .crc_with_files(0, protocol_v2(), metadata_a(), None, &[100, 200])
        .delta_with_add(1, "c", 300)
        .delta_with_add(2, "d", 400)
        .build()
        .await;

    let crc = built
        .incrementally_build_crc_from_disk_crc_at(0, None)
        .unwrap();
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 4); // 2 from disk + 2 from replay
    assert_eq!(stats.table_size_bytes(), 1000); // 300 + 700
                                                // Histogram boundaries are inherited from the base on disk.
    use crate::crc::{try_read_crc_file, FileSizeHistogram};
    use crate::path::ParsedLogPath;
    let base_path = ParsedLogPath::create_parsed_crc(&built.url, 0);
    let base = try_read_crc_file(&built.engine, &base_path).unwrap();
    let base_bins: Vec<i64> = base
        .file_stats()
        .unwrap()
        .file_size_histogram()
        .unwrap()
        .sorted_bin_boundaries()
        .to_vec();
    let merged = stats.file_size_histogram().unwrap();
    assert_eq!(merged.sorted_bin_boundaries(), base_bins.as_slice());
    // Boundaries match the kernel default (95 bins, see `FileSizeHistogram::create_default`).
    assert_eq!(
        base_bins.len(),
        FileSizeHistogram::create_default()
            .sorted_bin_boundaries()
            .len()
    );
}

#[tokio::test]
async fn test_incrementally_build_crc_from_disk_chain_stale_crc_at_v1() {
    // v0: create + CRC ([100])
    // v1: add + CRC ([100, 200])  -- stale base
    // v2: add (no CRC)
    // v3: add (no CRC)
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .crc_with_files(0, protocol_v2(), metadata_a(), None, &[100])
        .delta_with_add(1, "b", 200)
        .crc_with_files(1, protocol_v2(), metadata_a(), None, &[100, 200])
        .delta_with_add(2, "c", 300)
        .delta_with_add(3, "d", 400)
        .build()
        .await
        .incrementally_build_crc_from_disk_crc_at(1, None)
        .unwrap();
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 4); // 2 from disk + 2 from replay
    assert_eq!(stats.table_size_bytes(), 1000); // 300 + 700
    assert_eq!(crc.version, 3);
}

#[tokio::test]
async fn test_incrementally_build_crc_from_disk_remove_decrements_real_histogram() {
    // Stale CRC at v0 carries [100, 200, 300]; the segment removes one of those files.
    let built = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .crc_with_files(0, protocol_v2(), metadata_a(), None, &[100, 200, 300])
        .delta_with_remove(1, "x", Some(200))
        .build()
        .await;

    let crc = built
        .incrementally_build_crc_from_disk_crc_at(0, None)
        .unwrap();
    let stats = crc.file_stats().unwrap();
    assert_eq!(stats.num_files(), 2); // 3 from disk - 1 from replay
    assert_eq!(stats.table_size_bytes(), 400); // 600 - 200
    assert!(stats.file_size_histogram().is_some());
}

#[tokio::test]
async fn test_incrementally_build_crc_from_disk_no_histogram_means_no_result_histogram() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .crc(0, protocol_v2(), metadata_a(), None) // empty file_sizes -> no histogram on disk
        .delta_with_add(1, "a", 100)
        .build()
        .await
        .incrementally_build_crc_from_disk_crc_at(0, None)
        .unwrap();
    assert!(crc.file_stats().unwrap().file_size_histogram().is_none());
}

// === Result version stamping ===

#[tokio::test]
async fn test_incrementally_build_crc_stamps_version_with_segment_end() {
    let crc = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta(1)
        .delta(2)
        .build()
        .await
        .incrementally_build_crc(&Crc::default(), None)
        .unwrap();
    assert_eq!(crc.version, 2);
}

// === Preconditions ===

#[tokio::test]
async fn test_incrementally_build_crc_errors_when_base_version_at_end_version() {
    let test = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .build()
        .await;
    let err = test
        .incrementally_build_crc(&Crc::default(), Some(0))
        .unwrap_err();
    assert!(matches!(err, Error::InternalError(_)));
}

#[tokio::test]
async fn test_incrementally_build_crc_errors_when_base_version_above_end_version() {
    let test = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .build()
        .await;
    let base = Crc {
        version: 5,
        ..Default::default()
    };
    let err = test.incrementally_build_crc(&base, Some(0)).unwrap_err();
    assert!(matches!(err, Error::InternalError(_)));
}

#[tokio::test]
async fn test_incrementally_build_crc_errors_when_segment_has_gap_above_base_version() {
    // Builder writes v0 and v2 only — v1 is missing. `for_snapshot_impl`'s listing layer
    // catches this gap before `build_crc_from_stale` is even invoked, so the resulting
    // error isn't from our own gap check. We just want the call to fail.
    let test = CrcReadTest::new()
        .delta_with_p_m(0, protocol_v2(), metadata_a())
        .delta_with_p_m(2, protocol_v2(), metadata_a())
        .build()
        .await;
    assert!(test.incrementally_build_crc(&Crc::default(), None).is_err());
}
