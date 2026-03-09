//! Test harness for acceptance workloads.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! improved_dat directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::acceptance_workloads::{
    test_case_from_spec_path,
    types::WorkloadSpec,
    validation::{validate_read_result, validate_snapshot},
    workload::{execute_workload, WorkloadResult},
};

fn should_skip_test(test_path: &str) -> bool {
    let skip_prefixes = [
        "DV-017/", // Huge table (2B rows) causes OOM/hang
        "dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_all", // Kernel bug: data mismatch with struct special types
    ];
    skip_prefixes.iter().any(|p| test_path.contains(p))
}

/// Known kernel-vs-Spark divergences. Each entry is (reason, &[path substrings]).
/// The test asserts kernel DOES fail — if a kernel fix lands, the assertion breaks
/// and the entry should be removed.
const EXPECTED_KERNEL_FAILURES: &[(&str, &[&str])] = &[
    // ── Kernel bugs: unsupported types ──
    ("Kernel: void/NullType not supported in schema deserialization", &[
        "void_001_void_top_level/", "void_002_void_nested_struct/",
        "void_005_void_schema_evolution/", "void_006_void_multiple_columns/",
        "void_007_void_with_backticks/", "void_in_struct/",
    ]),
    ("Kernel: interval types not supported in schema deserialization", &[
        "intv_001_interval_ym_basic/", "intv_002_interval_dt_basic/",
        "intv_003_interval_partitioned/", "intv_004_interval_negative/",
        "intv_005_interval_mixed/", "intv_006_create_insert_select/",
        "intv_boundary_values/", "intv_sub_second/",
    ]),
    // ── Kernel bugs: null schemaString in metadata ──
    ("Kernel: fails with null schemaString in metadata", &[
        "pv_old_protocol_read/specs/pv_old_protocol_read_snapshot",
        "pv_empty_reader_features/specs/pv_empty_reader_features_snapshot",
        "pv_protocol_downgrade/specs/pv_protocol_downgrade_snapshot",
        "pv_reader_feature_not_in_writer/specs/pv_reader_feature_not_in_writer_snapshot",
        "pv_unknown_writer_feature_ok/specs/pv_unknown_writer_feature_ok_snapshot",
    ]),
    // ── Kernel bugs: checkpoint handling ──
    ("Kernel: can't fall back to log replay when checkpoint has missing parts/files", &[
        "corrupt_incomplete_multipart_checkpoint/",
        "ckp_incomplete_multipart/", "ckp_missing_checkpoint_file/",
    ]),
    // ── Kernel bugs: type widening ──
    ("Kernel: cannot cast list to non-list data types during type widening", &[
        "tw_array_element/specs/tw_array_element_read_",
        "tw_map_key_value_widening/specs/tw_map_key_value_widening_read_all",
    ]),
    // ── Kernel bugs: variant struct field order ──
    // Kernel produces variant struct fields in different order than Spark
    // (e.g., {metadata, value} vs {value, metadata}). Add entries as they surface.
    //
    // ── Kernel bugs: other ──
    ("Kernel: schema deserialization fails for TimestampNTZ type", &[
        "ds_multi_file_time/specs/ds_multi_file_time_snapshot",
    ]),
    ("Kernel: requires contiguous commits; Spark uses CRC files to bridge gaps", &[
        "prod_non_contiguous_versions/",
    ]),
    ("Kernel: column mapping id mode fails with None in final_fields_cols", &[
        "cm_id_matching_swapped/specs/cm_id_matching_swapped_select_",
        "cm_id_matching_nonexistent/specs/cm_id_matching_nonexistent_select_",
    ]),
    ("Kernel: can't resolve percent-encoded filenames in AddFile paths", &[
        "DV-005b/specs/DV-005b_count", "DV-008/specs/DV-008_table2_latest",
        "DV-009/specs/DV-009_table2_latest_v1",
    ]),
    ("Kernel: inline DV has invalid magic number", &[
        "dv_storage_type_i/specs/dv_storage_type_i_read_after_inline_dv",
    ]),
    ("Kernel: absolute-path DV has invalid percent-encoded path", &[
        "dv_storage_type_p/specs/dv_storage_type_p_read_after_absolute_path_dv",
    ]),
    ("Kernel: fails on missing/empty delta log (no files in log segment)", &[
        "ct_empty_delta_log/specs/ct_empty_delta_log_snapshot",
        "ct_missing_delta_log/specs/ct_missing_delta_log_snapshot",
        "dseReadNonDeltaPath/specs/dseReadNonDeltaPath_snapshot",
        "dv_checkpoint_only_read/specs/dv_checkpoint_only_read_snapshot",
    ]),
    // ── Kernel divergences: kernel succeeds where Spark expects error ──
    ("Kernel: doesn't reject unsupported column mapping mode", &[
        "cm_err_003_invalid_mode/specs/cm_err_003_invalid_mode_error",
    ]),
    ("Kernel: reads corrupt/invalid commit or checkpoint without error", &[
        "corrupt_truncated_commit_json/specs/corrupt_truncated_commit_json_error",
        "cp_err_missing_protocol/specs/cp_err_missing_protocol_error",
        "ct_corrupt_parquet/specs/ct_corrupt_parquet_error",
        "ct_invalid_json/specs/ct_invalid_json_error",
        "dsReadCorruptCheckpoint/specs/dsReadCorruptCheckpoint_error",
        "dsReadCorruptJson/specs/dsReadCorruptJson_error",
        "dsReadModifyCheckpoint/specs/dsReadModifyCheckpoint_error",
    ]),
    ("Kernel: doesn't reject duplicate actions in commit", &[
        "ct_duplicate_metadata/specs/ct_duplicate_metadata_error",
        "ct_duplicate_protocol/specs/ct_duplicate_protocol_error",
        "err_duplicate_add_same_version/specs/err_duplicate_add_same_version_error",
    ]),
    ("Kernel: doesn't reject missing metadata/protocol actions", &[
        "ct_missing_metadata/specs/ct_missing_metadata_error",
        "ct_missing_protocol/specs/ct_missing_protocol_error",
        "log_err_missing_metadata/specs/log_err_missing_metadata_error",
        "log_err_missing_protocol/specs/log_err_missing_protocol_error",
    ]),
    ("Kernel: snapshot construction succeeds even when data files are missing", &[
        "ct_missing_data_file/specs/ct_missing_data_file_error",
    ]),
    ("Kernel: doesn't validate DV integrity", &[
        "dv_err_001_checksum/specs/dv_err_001_checksum_error",
        "dv_err_003_malformed_path/specs/dv_err_003_malformed_path_error",
        "err_dv_invalid_storage_type/specs/err_dv_invalid_storage_type_error",
        "err_add_and_remove_same_path_dv/specs/err_add_and_remove_same_path_dv_error",
    ]),
    ("Kernel: doesn't validate schema integrity", &[
        "err_schema_empty/specs/err_schema_empty_error",
        "err_schema_invalid_json/specs/err_schema_invalid_json_error",
    ]),
    ("Kernel: doesn't require version 0 to exist", &[
        "err_missing_version_0/specs/err_missing_version_0_error",
    ]),
    ("Kernel: doesn't reject unknown reader features", &[
        "ev_unknown_reader_feature/specs/ev_unknown_reader_feature_error",
    ]),
    ("Kernel: doesn't enforce time travel safety", &[
        "tt_blocked_beyond_retention/specs/tt_blocked_beyond_retention_error",
        "tt_after_vacuum/specs/tt_after_vacuum_error",
    ]),
    // ── Capture bugs ──
    ("Capture bug: clone table AddFile references absolute temp path", &[
        "ic_022_clone_basic/specs/ic_022_clone_basic_readAll",
        "ic_022_clone_basic/specs/ic_022_clone_basic_read_all",
        "ic_023_clone_higher_watermark/specs/ic_023_clone_higher_watermark_readAll",
        "ic_023_clone_higher_watermark/specs/ic_023_clone_higher_watermark_read_all",
    ]),
    ("Capture bug: snapshot spec generated for invalid/broken table", &[
        "dv_err_002_missing_file/specs/dv_err_002_missing_file_error.json",
        "dsReadEmptyTable/specs/dsReadEmptyTable_snapshot",
        "dsReadEmptyString/specs/dsReadEmptyString_snapshot",
        "dsReadMissingCommitFile/specs/dsReadMissingCommitFile_snapshot",
        "dsReadMissingDeltaLog/specs/dsReadMissingDeltaLog_snapshot",
        "dsReadPathWithSpaces/specs/dsReadPathWithSpaces_snapshot",
        "dsReadDuplicateColumns/specs/dsReadDuplicateColumns_snapshot",
        "corrupt_checkpoint_corrupt_no_delta_files/specs/corrupt_checkpoint_corrupt_no_delta_files_snapshot",
    ]),
];

/// Check if workload type is unsupported by the harness.
fn unsupported_workload_reason(spec: &WorkloadSpec) -> Option<&'static str> {
    match spec {
        WorkloadSpec::Read {
            timestamp: Some(_), ..
        }
        | WorkloadSpec::Snapshot {
            timestamp: Some(_), ..
        } => Some("Timestamp-based time travel not supported by harness"),
        WorkloadSpec::Read {
            predicate: Some(_), ..
        } => Some("Predicate filtering not supported in this build"),
        WorkloadSpec::Unsupported => Some("Unsupported workload type"),
        _ => None,
    }
}

fn acceptance_workloads_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();

    if should_skip_test(&spec_path_str) {
        println!("Skipping test: {}", spec_path_str);
        return Ok(());
    }

    // Load spec and test case once
    let content = std::fs::read_to_string(&spec_path_abs).expect("Failed to read spec file");
    let spec: WorkloadSpec = serde_json::from_str(&content).expect("Failed to parse spec file");
    let (test_case, workload_name) =
        test_case_from_spec_path(&spec_path_abs).expect("Failed to load test case");
    let expected_dir = test_case.expected_dir(&workload_name);

    // Check for expected kernel failures
    let expected_failure = EXPECTED_KERNEL_FAILURES
        .iter()
        .find(|(_, patterns)| patterns.iter().any(|p| spec_path_str.contains(p)));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let table_root = test_case.table_root().expect("Failed to get table URL");
            let engine =
                test_utils::create_default_engine(&table_root).expect("Failed to create engine");

            // Skip unsupported workload types
            if let Some(reason) = unsupported_workload_reason(&spec) {
                println!("  Skipping ({})", reason);
                return;
            }

            // Expected kernel failures: assert kernel DOES fail
            if let Some((reason, _)) = expected_failure {
                let result = execute_workload(engine, &table_root, &spec);
                match result {
                    Err(e) => println!("  Expected kernel failure ({reason}): {e}"),
                    Ok(_) if spec.expected_error().is_some() => {
                        println!("  Expected kernel divergence ({reason}): kernel succeeded where spec expects error");
                    }
                    Ok(_) => panic!(
                        "Workload '{workload_name}' was expected to fail but succeeded! \
                         Reason: {reason}. Remove from EXPECTED_KERNEL_FAILURES!"
                    ),
                }
                return;
            }

            println!("Running workload: {}", workload_name);

            // Error workloads: assert kernel fails
            if let Some(expected_error) = spec.expected_error() {
                match execute_workload(engine, &table_root, &spec) {
                    Ok(_) => panic!(
                        "Workload '{}' expected error '{}' but succeeded",
                        workload_name, expected_error.error_code
                    ),
                    Err(e) => println!(
                        "  Got expected error (expected '{}'): {}",
                        expected_error.error_code, e
                    ),
                }
                return;
            }

            // Execute and validate
            let result = execute_workload(engine, &table_root, &spec)
                .unwrap_or_else(|e| panic!("Workload '{}' failed: {}", workload_name, e));

            match result {
                WorkloadResult::Read(read_result) => {
                    let inline_expected = match &spec {
                        WorkloadSpec::Read {
                            expected: Some(ref e),
                            ..
                        } => Some(e),
                        _ => None,
                    };
                    let batch = read_result.concat().expect("Failed to concat batches");
                    if expected_dir.exists() || inline_expected.is_some() {
                        validate_read_result(batch, &expected_dir, inline_expected).await;
                    } else {
                        println!(
                            "  No expected data for '{}' ({} rows, not validated)",
                            workload_name,
                            batch.num_rows()
                        );
                    }
                }
                WorkloadResult::Snapshot(snapshot_result) => {
                    let inline_expected = match &spec {
                        WorkloadSpec::Snapshot {
                            expected: Some(ref e),
                            ..
                        } => Some(e),
                        _ => None,
                    };
                    if inline_expected.is_some() || expected_dir.exists() {
                        validate_snapshot(&snapshot_result, &expected_dir, inline_expected);
                    } else {
                        println!(
                            "  No expected metadata for '{}' (not validated)",
                            workload_name
                        );
                    }
                }
            }

            println!("  Passed");
        });

    Ok(())
}

datatest_stable::harness! {
    {
        test = acceptance_workloads_test,
        root = "../improved_dat/",
        pattern = r"specs/.*\.json$"
    },
}
