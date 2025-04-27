use std::{error, sync::Arc};

use delta_kernel::engine::sync::SyncEngine;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::{DeltaResult, Table, Version};

mod common;
use common::load_test_data;

fn read_time_for_table(
    test_name: impl AsRef<str>,
    start_timestamp: i64,
    end_timestamp: Option<i64>,
) -> DeltaResult<(Version, Option<Version>)> {
    let test_dir = load_test_data("tests/data", test_name.as_ref()).unwrap();
    let test_path = test_dir.path().join(test_name.as_ref());
    let table = Table::try_from_uri(test_path.to_str().expect("table path to string")).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let manager = table.history_manager(engine.as_ref())?;
    manager.timestamp_range_to_versions(engine.as_ref(), start_timestamp, end_timestamp)
}

/*
 * Test file timestamps:
 * +----------------------+----------------+----------------+
 * | Filename             | Modification   | In-Commit      |
 * |                      | Timestamp      | Timestamp      |
 * +----------------------+----------------+----------------+
 * | 00000000000000000000 | 1745378841000  |                |
 * | 00000000000000000001 | 1745378841000  |                |
 * | 00000000000000000002 | 1745378841000  |                |
 * | 00000000000000000003 | 1745378841000  |                |
 * | 00000000000000000004 | 1745378842000  |                |
 * | 00000000000000000005 | 1745378842000  |                |
 * | 00000000000000000006 | 1745378842000  |                |
 * | 00000000000000000007 | 1745378842000  |                |
 * | 00000000000000000008 | 1745378842000  |                |
 * | 00000000000000000009 | 1745378842000  |                |
 * | 00000000000000000010 | 1745378843000  |                |
 * | 00000000000000000011 | 1745378843000  | 1745378843320  |
 * | 00000000000000000012 | 1745378843000  | 1745378843517  |
 * | 00000000000000000013 | 1745378843000  | 1745378843709  |
 * | 00000000000000000014 | 1745378843000  | 1745378843889  |
 * | 00000000000000000015 | 1745378844000  | 1745378844071  |
 * | 00000000000000000016 | 1745378844000  | 1745378844252  |
 * | 00000000000000000017 | 1745378844000  | 1745378844439  |
 * | 00000000000000000018 | 1745378844000  | 1745378844614  |
 * | 00000000000000000019 | 1745378844000  | 1745378844796  |
 * | 00000000000000000020 | 1745378845000  | 1745378844975  |
 * | 00000000000000000021 | 1745378845000  | 1745378845158  |
 * +----------------------+----------------+----------------+
 */

fn setup_test_timestamps() -> (Vec<i64>, Vec<i64>) {
    // File modification timestamps for versions 0-21
    let file_mod_timestamps = vec![
        1745378841000,
        1745378841000,
        1745378841000,
        1745378841000,
        1745378842000,
        1745378842000,
        1745378842000,
        1745378842000,
        1745378842000,
        1745378842000,
        1745378843000,
        1745378843000,
        1745378843000,
        1745378843000,
        1745378843000,
        1745378844000,
        1745378844000,
        1745378844000,
        1745378844000,
        1745378844000,
        1745378845000,
        1745378845000,
    ];

    // In-commit timestamps for versions 0-21 (-1 indicates no in-commit timestamp)
    let mut in_commit_timestamps = vec![-1; 11]; // Versions 0-10 have no in-commit timestamps
    in_commit_timestamps.extend(vec![
        1745378843320, // 11
        1745378843517, // 12
        1745378843709, // 13
        1745378843889, // 14
        1745378844071, // 15
        1745378844252, // 16
        1745378844439, // 17
        1745378844614, // 18
        1745378844796, // 19
        1745378844975, // 20
        1745378845158, // 21
    ]);

    (file_mod_timestamps, in_commit_timestamps)
}

#[test]
fn exact_file_modification_timestamp_match() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, _) = setup_test_timestamps();

    // Test exact timestamp match for start and end
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[0],       // 0.json
        Some(file_mod_timestamps[9]), // 9.json
    )?;
    assert_eq!(start, 0);
    assert_eq!(end, Some(9));
    Ok(())
}

#[test]
fn file_modification_timestamp_between_files() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, _) = setup_test_timestamps();

    // Test timestamp that falls between file timestamps
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        (file_mod_timestamps[3] + file_mod_timestamps[4]) / 2, // Between 3.json and 4.json
        Some((file_mod_timestamps[9] + file_mod_timestamps[10]) / 2), // Between 9.json and 10.json
    )?;
    assert_eq!(start, 4); // Should return first file with timestamp >= start
    assert_eq!(end, Some(9)); // Should return last file with timestamp <= end
    Ok(())
}

#[test]
fn timestamp_before_all_files() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, _) = setup_test_timestamps();

    // Test timestamp before any file timestamp
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[0] - 1000, // Before 0.json
        Some(file_mod_timestamps[9]),  // 9.json
    )?;
    assert_eq!(start, 0); // Should return the first file
    assert_eq!(end, Some(9));
    Ok(())
}

#[test]
fn timestamp_after_all_files_up_to_10() -> Result<(), Box<dyn error::Error>> {
    let (_, in_commit_timestamps) = setup_test_timestamps();

    let very_late_start = in_commit_timestamps[21] + 1000; // After all the commits
    let res = read_time_for_table(
        "in-commit-timestamps-test",
        very_late_start,
        Some(very_late_start + 1),
    );
    assert!(res.is_err());
    Ok(())
}

#[test]
fn same_timestamp_for_start_and_end() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, _) = setup_test_timestamps();

    // Test when start and end timestamps are the same
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[4], // 4.json through 9.json have same timestamp
        Some(file_mod_timestamps[4]),
    )?;
    assert_eq!(start, 4);
    assert_eq!(end, Some(9));
    Ok(())
}

#[test]
fn no_end_timestamp() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, _) = setup_test_timestamps();

    // Test with no end timestamp
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[4], // 4.json
        None,
    )?;
    assert_eq!(start, 4);
    assert_eq!(end, None);
    Ok(())
}

#[test]
fn timestamp_range_after_all_log_files() -> Result<(), Box<dyn error::Error>> {
    let (_, in_commit_timestamps) = setup_test_timestamps();

    // Test timestamp range where no files exist
    let res = read_time_for_table(
        "in-commit-timestamps-test",
        in_commit_timestamps[21] + 1, // After all files
        Some(in_commit_timestamps[21] + 2),
    );
    assert!(res.is_err());
    Ok(())
}

#[test]
fn file_modification_timestamp_start_timestamp_after_end_timestamp(
) -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, _) = setup_test_timestamps();

    // Test when start timestamp is after end timestamp
    let res = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[3] + 1,         // Immediately after version 3
        Some(file_mod_timestamps[3] + 100), // Before version 4
    );
    assert!(res.is_err()); // No timestamp in the range
    Ok(())
}

#[test]
fn in_commit_exact_timestamp_match() -> Result<(), Box<dyn error::Error>> {
    let (_, in_commit_timestamps) = setup_test_timestamps();

    // Test exact in-commit timestamp match
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        in_commit_timestamps[11], // Exact match with 11.json in-commit timestamp
        Some(in_commit_timestamps[21]), // Exact match with 21.json in-commit timestamp
    )?;
    assert_eq!(start, 11);
    assert_eq!(end, Some(21));
    Ok(())
}

#[test]
fn in_commit_timestamp_between_files() -> Result<(), Box<dyn error::Error>> {
    let (_, in_commit_timestamps) = setup_test_timestamps();

    // Test in-commit timestamp that falls between file timestamps
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        (in_commit_timestamps[11] + in_commit_timestamps[12]) / 2, // Between 11 and 12
        Some((in_commit_timestamps[17] + in_commit_timestamps[18]) / 2), // Between 17 and 18
    )?;
    assert_eq!(start, 12); // Should return first file with in-commit timestamp >= start
    assert_eq!(end, Some(17)); // Should return last file with in-commit timestamp <= end
    Ok(())
}

#[test]
fn mixed_timestamps() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, in_commit_timestamps) = setup_test_timestamps();

    // Test mix of file modification and in-commit timestamps
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[4],         // Version 4 (file modification)
        Some(in_commit_timestamps[16]), // Version 16 (ICT)
    )?;
    assert_eq!(start, 4);
    assert_eq!(end, Some(16));
    Ok(())
}

#[test]
fn file_modification_should_not_be_used_after_version_10() -> Result<(), Box<dyn error::Error>> {
    let (file_mod_timestamps, in_commit_timestamps) = setup_test_timestamps();

    // Files 10-14 share the same file modification timestamp that is less than the end timestamp.
    // However, version 11 has a greater in-commit than the end timestamp. This ensures that
    // commits after 10 are not included in the result
    let (start, end) = read_time_for_table(
        "in-commit-timestamps-test",
        file_mod_timestamps[0], // File modification timestamp for files 0-3
        Some((file_mod_timestamps[10] + in_commit_timestamps[11]) / 2), // Between versions 10 and 11
    )?;
    assert_eq!(start, 0);
    assert_eq!(end, Some(10));
    Ok(())
}

#[test]
fn in_commit_timestamp_range_between_commits_is_empty() -> Result<(), Box<dyn error::Error>> {
    let (_, in_commit_timestamps) = setup_test_timestamps();

    // Test in-commit timestamp range where no files exist
    let res = read_time_for_table(
        "in-commit-timestamps-test",
        in_commit_timestamps[12] + 1, // One greater than version 12
        Some(in_commit_timestamps[13] - 1), // One less than version 13
    );
    assert!(res.is_err());
    Ok(())
}
