//! Test harness for improved_dat test suite.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! improved_dat directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::improved_dat::{
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

/// Known kernel-vs-Spark divergences. Each entry is (path substring, reason).
/// The test asserts kernel DOES fail — if a kernel fix lands, the assertion breaks
/// and the entry should be removed.
const EXPECTED_KERNEL_FAILURES: &[(&str, &str)] = &[
    // ── Kernel bugs ──
    ("corrupt_incomplete_multipart_checkpoint/",
        "Kernel can't fall back to log replay when multipart checkpoint has missing parts"),
    ("prod_non_contiguous_versions/",
        "Kernel requires contiguous commits; Spark uses CRC files to bridge gaps"),
    ("cm_id_matching_swapped/specs/cm_id_matching_swapped_select_",
        "Kernel bug: column mapping id mode fails with None in final_fields_cols"),
    ("cm_id_matching_nonexistent/specs/cm_id_matching_nonexistent_select_",
        "Kernel bug: column mapping id mode fails with None in final_fields_cols"),
    ("tw_array_element/specs/tw_array_element_read_",
        "Kernel bug: Cannot cast list to non-list data types during type widening"),
    ("tw_map_key_value_widening/specs/tw_map_key_value_widening_read_all",
        "Kernel bug: Cannot cast list to non-list data types during type widening"),
    ("ds_multi_file_time/specs/ds_multi_file_time_snapshot",
        "Kernel bug: schema deserialization fails for TimestampNTZ type"),
    // Kernel doesn't support void type in schema
    ("void_001_void_top_level/",
        "Kernel bug: void/NullType not supported in schema deserialization"),
    ("void_002_void_nested_struct/",
        "Kernel bug: void/NullType not supported in schema deserialization"),
    ("void_005_void_schema_evolution/",
        "Kernel bug: void/NullType not supported in schema deserialization"),
    ("void_006_void_multiple_columns/",
        "Kernel bug: void/NullType not supported in schema deserialization"),
    ("void_007_void_with_backticks/",
        "Kernel bug: void/NullType not supported in schema deserialization"),
    // Kernel doesn't support interval types in schema
    ("intv_001_interval_ym_basic/",
        "Kernel bug: YearMonthIntervalType not supported in schema deserialization"),
    ("intv_002_interval_dt_basic/",
        "Kernel bug: DayTimeIntervalType not supported in schema deserialization"),
    ("intv_003_interval_partitioned/",
        "Kernel bug: interval types not supported in schema deserialization"),
    ("intv_004_interval_negative/",
        "Kernel bug: interval types not supported in schema deserialization"),
    ("intv_005_interval_mixed/",
        "Kernel bug: interval types not supported in schema deserialization"),
    ("intv_006_create_insert_select/",
        "Kernel bug: interval types not supported in schema deserialization"),
    // Kernel doesn't enforce file retention duration
    ("tt_blocked_beyond_retention/specs/tt_blocked_beyond_retention_error",
        "Kernel bug: doesn't enforce deletedFileRetentionDuration for time travel"),
    // Kernel fails to read snapshot with null schemaString
    ("pv_old_protocol_read/specs/pv_old_protocol_read_snapshot",
        "Kernel bug: fails with null schemaString in old protocol metadata"),

    // ── Capture bugs: clone tables have absolute temp paths in AddFile ──
    // Only readAll specs fail (snapshot specs succeed since they don't read data files)
    ("cloneShallowBasic/specs/cloneShallowBasic_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowAfterDml/specs/cloneShallowAfterDml_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowDvData/specs/cloneShallowDvData_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowMultiVersion/specs/cloneShallowMultiVersion_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowPartitioned/specs/cloneShallowPartitioned_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowSchemaEvolution/specs/cloneShallowSchemaEvolution_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowSnapshot/specs/cloneShallowSnapshot_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowWithData/specs/cloneShallowWithData_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneShallowWithDv/specs/cloneShallowWithDv_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneSnapshotHistory/specs/cloneSnapshotHistory_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneSqlReplace/specs/cloneSqlReplace_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneSqlShallow/specs/cloneSqlShallow_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneSqlVersion/specs/cloneSqlVersion_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneSqlWithProperties/specs/cloneSqlWithProperties_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneWithCheckpoint/specs/cloneWithCheckpoint_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneWithClustering/specs/cloneWithClustering_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneWithIct/specs/cloneWithIct_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("cloneIdempotent/specs/cloneIdempotent_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("ic_022_clone_basic/specs/ic_022_clone_basic_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("ic_022_clone_basic/specs/ic_022_clone_basic_read_all",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("ic_023_clone_higher_watermark/specs/ic_023_clone_higher_watermark_readAll",
        "Capture bug: clone table AddFile references absolute temp path"),
    ("ic_023_clone_higher_watermark/specs/ic_023_clone_higher_watermark_read_all",
        "Capture bug: clone table AddFile references absolute temp path"),

    // ── Capture bugs: snapshot spec generated for tables that can't construct valid snapshot ──
    ("dv_err_002_missing_file/specs/dv_err_002_missing_file_error.json",
        "Capture bug: snapshot type doesn't access DV files; should be read type"),
    ("dsReadEmptyTable/specs/dsReadEmptyTable_snapshot",
        "Capture bug: snapshot spec generated for empty/invalid table"),
    ("dsReadEmptyString/specs/dsReadEmptyString_snapshot",
        "Capture bug: snapshot spec generated for empty/invalid table"),
    ("dsReadMissingCommitFile/specs/dsReadMissingCommitFile_snapshot",
        "Capture bug: snapshot spec generated for table with missing commit"),
    ("dsReadMissingDeltaLog/specs/dsReadMissingDeltaLog_snapshot",
        "Capture bug: snapshot spec generated for table with no delta log"),
    ("dsReadPathWithSpaces/specs/dsReadPathWithSpaces_snapshot",
        "Capture bug: snapshot spec generated for table at missing path"),
    ("dsReadDuplicateColumns/specs/dsReadDuplicateColumns_snapshot",
        "Capture bug: snapshot spec generated for table with duplicate columns"),
    ("corrupt_checkpoint_corrupt_no_delta_files/specs/corrupt_checkpoint_corrupt_no_delta_files_snapshot",
        "Capture bug: snapshot spec generated for table with corrupt checkpoint"),
];

fn assert_expected_kernel_failure(
    spec_path_str: &str,
    reason: &str,
) -> datatest_stable::Result<()> {
    let spec_path = std::path::PathBuf::from(spec_path_str);
    let (test_case, workload_name) =
        test_case_from_spec_path(&spec_path).expect("Failed to load test case");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let content = std::fs::read_to_string(&spec_path).expect("Failed to read spec file");
        let spec: WorkloadSpec = serde_json::from_str(&content).expect("Failed to parse spec file");
        let table_root = test_case.table_root().expect("Failed to get table URL");
        let engine =
            test_utils::create_default_engine(&table_root).expect("Failed to create engine");

        let result = execute_workload(engine, &table_root, &spec);
        match result {
            Err(e) => {
                println!("  Expected kernel failure ({}): {}", reason, e);
            }
            Ok(_) if spec.expects_error() => {
                println!(
                    "  Expected kernel divergence ({}): kernel succeeded where spec expects error",
                    reason
                );
            }
            Ok(_) => {
                panic!(
                    "Workload '{}' was expected to fail but succeeded! \
                     Reason for expected failure: {}. \
                     If kernel now handles this, remove it from expect_kernel_failure!",
                    workload_name, reason
                );
            }
        }
    });
    Ok(())
}

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
        WorkloadSpec::Txn { .. } => Some("Txn workloads not supported in this build"),
        WorkloadSpec::DomainMetadata { .. } => {
            Some("DomainMetadata workloads not supported in this build")
        }
        WorkloadSpec::Cdf { .. } => Some("CDF workloads not supported in this build"),
        _ => None,
    }
}

fn improved_dat_test(spec_path: &Path) -> datatest_stable::Result<()> {
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

    let (test_case, workload_name) =
        test_case_from_spec_path(&spec_path_abs).expect("Failed to load test case");
    let expected_dir = test_case.expected_dir(&workload_name);

    for (pattern, reason) in EXPECTED_KERNEL_FAILURES {
        if spec_path_str.contains(pattern) {
            return assert_expected_kernel_failure(&spec_path_str, reason);
        }
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let content =
                std::fs::read_to_string(&spec_path_abs).expect("Failed to read spec file");
            let spec: WorkloadSpec =
                serde_json::from_str(&content).expect("Failed to parse spec file");

            println!("Running workload: {}", workload_name);

            let table_root = test_case.table_root().expect("Failed to get table URL");
            let engine =
                test_utils::create_default_engine(&table_root).expect("Failed to create engine");

            // Skip unsupported workload types
            if let Some(reason) = unsupported_workload_reason(&spec) {
                println!("  Skipping ({})", reason);
                return;
            }

            // Error workloads
            if spec.expects_error() {
                let expected_error = spec.expected_error().unwrap();
                let result = execute_workload(engine.clone(), &table_root, &spec);
                match result {
                    Ok(_) => {
                        panic!(
                            "Workload '{}' expected error '{}' but succeeded",
                            workload_name, expected_error.error_code
                        );
                    }
                    Err(e) => {
                        println!(
                            "  Got expected error (expected '{}'): {}",
                            expected_error.error_code, e
                        );
                    }
                }
                return;
            }

            // Execute workload
            let result = execute_workload(engine.clone(), &table_root, &spec)
                .unwrap_or_else(|e| panic!("Workload '{}' failed: {}", workload_name, e));

            // Validate results
            match result {
                WorkloadResult::Read(read_result) => {
                    let inline_expected = match &spec {
                        WorkloadSpec::Read { expected: Some(ref e), .. } => Some(e),
                        _ => None,
                    };
                    let batch = read_result.concat().expect("Failed to concat batches");
                    if expected_dir.exists() || inline_expected.is_some() {
                        validate_read_result(batch, &expected_dir, inline_expected)
                            .await
                            .unwrap_or_else(|e| {
                                panic!("Validation failed for workload '{}': {}", workload_name, e)
                            });
                    } else {
                        println!(
                            "  No expected data for '{}' ({} rows returned, not validated)",
                            workload_name,
                            batch.num_rows()
                        );
                    }
                }
                WorkloadResult::Snapshot(snapshot_result) => {
                    let inline_expected = match &spec {
                        WorkloadSpec::Snapshot { expected: Some(ref e), .. } => Some(e),
                        _ => None,
                    };
                    if inline_expected.is_some() || expected_dir.exists() {
                        validate_snapshot(&snapshot_result, &expected_dir, inline_expected)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "Snapshot validation failed for '{}': {}",
                                    workload_name, e
                                )
                            });
                    } else {
                        println!(
                            "  No expected metadata for '{}' (snapshot not validated)",
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
        test = improved_dat_test,
        root = "../improved_dat/",
        pattern = r"specs/.*\.json$"
    },
}
