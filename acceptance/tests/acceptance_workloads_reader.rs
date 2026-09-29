//! Test harness for acceptance workloads.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! acceptance_workloads directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::acceptance_workloads::workload::execute_and_validate_workload;
use acceptance::acceptance_workloads::{corpus_relative_spec_id, LoadedTestCase};

/// Tests that cannot be executed due to test harness limitations.
/// These fail at parse time or cause infrastructure issues (OOM, hang).
/// All other failures (bugs, divergences, missing features) go in EXPECTED_KERNEL_FAILURES.
const SKIP_LIST: &[(&str, &str)] = &[
    ("DV-017/", "Huge table (2B rows) causes OOM/hang"),
    // These predicates trigger a stats-schema mismatch and panic during struct reordering.
    // Skip them because the panic aborts the corpus run.
    (
        "cloneDeepMultiType_readFiltered",
        "Kernel panic in reorder_struct_array (index out of bounds)",
    ),
    (
        "restoreCheckData_filterBoolean",
        "Kernel panic in reorder_struct_array (index out of bounds)",
    ),
];

fn should_skip_test(test_path: &str) -> Option<&'static str> {
    for (pattern, reason) in SKIP_LIST {
        if test_path.contains(pattern) {
            return Some(reason);
        }
    }
    None
}

/// Tests that CAN be executed but are expected to fail (kernel bugs or divergences).
/// Unlike SKIP_LIST, these workloads run and we assert they produce wrong results or errors.
/// When a kernel fix lands, the test will pass and the entry should be removed.
struct ExpectedFailure {
    reason: &'static str,
    expected_error: &'static str,
    patterns: &'static [&'static str],
}

const EXPECTED_KERNEL_FAILURES: &[ExpectedFailure] = &[
    ExpectedFailure {
        reason: "Kernel cannot project the _metadata.file_path system column",
        expected_error: "_metadata.file_path",
        patterns: &["DV-003/specs/DV-003_metadata_file_path"],
    },
    // Delta requires schemaString in every metadata action. Spark produces a snapshot from this
    // malformed metadata, while Kernel rejects it with a low-level Arrow error.
    ExpectedFailure {
        reason: "Spark accepts malformed metadata without schemaString",
        expected_error: "Encountered unmasked nulls in non-nullable StructArray child",
        patterns: &["protocol_versions_protocol_downgrade/specs/protocol_versions_protocol_downgrade_snapshot"],
    },
    // The latest JSON commit is truncated, but its matching version checksum contains allFiles.
    // Spark recovers the file list from the checksum; Kernel still parses the broken JSON commit.
    ExpectedFailure {
        reason: "Kernel cannot recover a truncated commit from the version checksum",
        expected_error: "Truncated record whilst reading string",
        patterns: &["corruption_truncated_commit/specs/corruption_truncated_commit_read_all"],
    },
    // See delta-kernel-rs#3445.
    ExpectedFailure {
        reason: "Predicate parser cannot type date, string-length, or array-size functions",
        expected_error: "Cannot determine type",
        patterns: &[
        "cc_005_varchar_constraint/specs/cc_005_varchar_constraint_filter_short_string",
        "cc_009_array_constraint/specs/cc_009_array_constraint_filter_array_size",
        "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc8_hex_1111",
        "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc8_hex_3333",
        "ds_typed_stats/specs/ds_typed_stats_hit_c8_hex_1111",
        "ds_typed_stats/specs/ds_typed_stats_hit_c8_hex_3333",
        "data_skipping_date_add_sub/specs/data_skipping_date_add_sub_read_d_gte_date_adddate20240101_1",
        "data_skipping_date_add_sub/specs/data_skipping_date_add_sub_read_miss_date_sub",
        "data_skipping_month_function/specs/data_skipping_month_function_read_miss_month",
        "data_skipping_trunc_date/specs/data_skipping_trunc_date_read_truncd_month_eq_date20240301",
        "data_skipping_trunc_date/specs/data_skipping_trunc_date_read_miss_trunc",
        "data_skipping_trunc_date/specs/data_skipping_trunc_date_read_truncd_month_eq_date20240601",
        "check_constraints_array_constraint/specs/check_constraints_array_constraint_read_sizetags_gt_2",
        "data_skipping_year_function/specs/data_skipping_year_function_read_miss_year",
        "variant_array_variant/specs/variant_array_variant_filter_array_size",
        "data_skipping_date_trunc_timestamp/specs/data_skipping_date_trunc_timestamp_read_miss_trunc_ts",
        "data_skipping_month_function/specs/data_skipping_month_function_read_monthd_eq_6",
        "data_skipping_month_function/specs/data_skipping_month_function_read_monthd_eq_1",
        "data_skipping_datediff/specs/data_skipping_datediff_read_datediffd_date20240101_gt_100",
        "data_skipping_datediff/specs/data_skipping_datediff_read_datediffd_date20240101_lte_10",
        "data_skipping_year_function/specs/data_skipping_year_function_read_yeard_eq_2025",
        "check_constraints_varchar_constraint/specs/check_constraints_varchar_constraint_read_lengths_lt_4",
        "data_skipping_date_trunc_timestamp/specs/data_skipping_date_trunc_timestamp_read_date_truncmonth_ts_eq_timestamp20240301_000000",
        "data_skipping_year_function/specs/data_skipping_year_function_read_yeard_eq_2024",
    ],
    },
    // === CRC masking (delta-kernel-rs#2753) ===
    // Kernel trusts version-checksum state over conflicting or incomplete JSON log state.
    // Version 0 is missing its required protocol or metadata action.
    ExpectedFailure {
        reason: "Kernel lets a version checksum mask required actions missing from version 0",
        expected_error: "Expected error 'DELTA_STATE_RECOVER_ERROR' but succeeded",
        patterns: &[
        "log_replay_log_err_missing_metadata/specs/log_replay_log_err_missing_metadata_snapshot",
        "log_replay_log_err_missing_metadata/specs/log_replay_log_err_missing_metadata_read_all",
        "log_replay_log_err_missing_protocol/specs/log_replay_log_err_missing_protocol_snapshot",
        "log_replay_log_err_missing_protocol/specs/log_replay_log_err_missing_protocol_read_all",
    ],
    },
    // The version-zero JSON commit is absent.
    ExpectedFailure {
        reason: "Kernel lets a version checksum mask a missing version-zero JSON commit",
        expected_error: "Expected error 'DELTA_TRUNCATED_TRANSACTION_LOG' but succeeded",
        patterns: &[
        "time_travel_deleted_version_retention_error/specs/time_travel_deleted_version_retention_error_snapshot",
        "corruption_err_missing_version_0/specs/corruption_err_missing_version_0_snapshot",
        "corruption_err_missing_version_0/specs/corruption_err_missing_version_0_read_all",
        "time_travel_deleted_version/specs/time_travel_deleted_version_snapshot",
    ],
    },
    // The JSON protocol has an unknown or case-mismatched reader feature.
    ExpectedFailure {
        reason: "Kernel lets a version checksum mask unsupported reader features in the JSON log",
        expected_error: "Expected error 'DELTA_UNSUPPORTED_FEATURES_FOR_READ' but succeeded",
        patterns: &[
        "protocol_versions_unknown_reader_feature/specs/protocol_versions_unknown_reader_feature_snapshot",
        "protocol_versions_err_002_unsupported_feature/specs/protocol_versions_err_002_unsupported_feature_snapshot",
        "protocol_versions_features_case_sensitivity/specs/protocol_versions_features_case_sensitivity_snapshot",
    ],
    },
    // The JSON log has an invalid reader/writer version pair.
    ExpectedFailure {
        reason: "Kernel lets a version checksum mask invalid protocol versions in the JSON log",
        expected_error: "Expected error 'EXPRESSION_DECODING_FAILED' but succeeded",
        patterns: &[
        "protocol_versions_reader_v3_writer_lt_7/specs/protocol_versions_reader_v3_writer_lt_7_snapshot",
        "protocol_versions_reader_v4_error/specs/protocol_versions_reader_v4_error_snapshot",
    ],
    },
    // The JSON protocol requires unsupported reader and writer versions.
    ExpectedFailure {
        reason: "Kernel lets a version checksum mask unsupported protocol versions in the JSON log",
        expected_error: "Expected error 'DELTA_INVALID_PROTOCOL_VERSION' but succeeded",
        patterns: &[
        "protocol_versions_err_001_protocol_too_high/specs/protocol_versions_err_001_protocol_too_high_snapshot",
    ],
    },
    // Metadata enables deletion vectors without the required protocol features.
    ExpectedFailure {
        reason: "Kernel lets a version checksum mask protocol/metadata feature mismatches",
        expected_error: "Expected error 'DELTA_FEATURES_PROTOCOL_METADATA_MISMATCH' but succeeded",
        patterns: &[
        "protocol_versions_unknown_writer_feature_ok/specs/protocol_versions_unknown_writer_feature_ok_snapshot",
        "protocol_versions_reader_feature_not_in_writer/specs/protocol_versions_reader_feature_not_in_writer_snapshot",
        "protocol_versions_empty_reader_features/specs/protocol_versions_empty_reader_features_snapshot",
    ],
    },
    // The JSON metadata preserves custom configuration keys that its checksum drops.
    ExpectedFailure {
        reason: "JSON and version-checksum metadata disagree on table configuration",
        expected_error: "Expected metadata to match",
        patterns: &[
        "evolvability_fc_extra_metadata_keys/specs/evolvability_fc_extra_metadata_keys_snapshot",
    ],
    },
    // The version-zero JSON commit is corrupt.
    ExpectedFailure {
        reason: "A valid version checksum masks a corrupt version-zero JSON commit",
        expected_error: "Expected error 'SparkException' but succeeded",
        patterns: &[
        "corruption_err_schema_empty/specs/corruption_err_schema_empty_snapshot",
        "corruption_invalid_json/specs/corruption_invalid_json_snapshot",
        "corruption_truncated_commit_json/specs/corruption_truncated_commit_json_snapshot",
        "corruption_err_schema_invalid_json/specs/corruption_err_schema_invalid_json_snapshot",
    ],
    },
    ExpectedFailure {
        reason: "Kernel accepts a checkpoint whose corresponding JSON commit is missing",
        expected_error: "Expected error 'IllegalStateException' but succeeded",
        patterns: &[
        "checkpoints_checkpoint_only_table/specs/checkpoints_checkpoint_only_table_snapshot",
        "deletion_vectors_checkpoint_only_read/specs/deletion_vectors_checkpoint_only_read_snapshot",
        "checkpoints_checkpoint_only_table/specs/checkpoints_checkpoint_only_table_read_all",
    ],
    },
    // Parsing an unknown mode currently discards the error and treats the property as absent.
    // See delta-kernel-rs#1849 for centralized table-property parsing and validation.
    ExpectedFailure {
        reason: "Kernel ignores an invalid delta.columnMapping.mode value",
        expected_error: "Expected error 'ColumnMappingUnsupportedException' but succeeded",
        patterns: &[
        "column_mapping_cm_err_003_invalid_mode/specs/column_mapping_cm_err_003_invalid_mode_snapshot",
        "column_mapping_cm_err_003_invalid_mode/specs/column_mapping_cm_err_003_invalid_mode_read_all",
    ],
    },
    // This commit contains reconciling add/remove actions for the same (path, deletion-vector ID),
    // which the Delta protocol forbids because actions within a commit have no defined order. Spark
    // returns no rows, while Kernel retains the add and returns its undeleted rows. This is a
    // malformed-table oracle divergence, not a protocol-defined reader result. See
    // delta-kernel-rs#3441.
    ExpectedFailure {
        reason: "Spark and Kernel resolve a writer-invalid add/remove commit differently",
        expected_error: "Data mismatch",
        patterns: &[
        "corruption_err_add_and_remove_same_path_dv/specs/corruption_err_add_and_remove_same_path_dv_read_all",
    ],
    },
    // Parsing succeeds, but physical predicate resolution only recognizes primitive leaves and
    // reports an existing whole struct or Variant column as unknown. See delta-kernel-rs#3442.
    ExpectedFailure {
        reason: "Kernel reports existing struct and Variant columns as unknown for null predicates",
        expected_error: "Predicate references unknown column",
        patterns: &[
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_v_is_null",
        "variant_null_top_level/specs/variant_null_top_level_filter_not_null",
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_null_v_is_not_null",
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_v_structv_is_not_null",
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_v_is_not_null",
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_null_v_is_null",
        "variant_null_counts/specs/variant_null_counts_filter_non_null",
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_null_v_structv_is_null",
        "data_skipping_variant_null_stats/specs/data_skipping_variant_null_stats_read_v_structv_is_null",
        "default_values_read_default_nested/specs/default_values_read_default_nested_read_info_is_not_null",
        "variant_null_top_level/specs/variant_null_top_level_filter_null",
    ],
    },
    // See delta-kernel-rs#3446.
    ExpectedFailure {
        reason: "Predicate parser does not support LIKE expressions",
        expected_error: "Unsupported expression",
        patterns: &[
        "data_skipping_starts_with_nested/specs/data_skipping_starts_with_nested_read_miss_c",
        "data_skipping_starts_with_nested/specs/data_skipping_starts_with_nested_read_miss_z",
        "data_skipping_starts_with/specs/data_skipping_starts_with_read_a_like_a",
        "data_skipping_long_strings_max/specs/data_skipping_long_strings_max_read_a_like_zzz",
        "data_skipping_or_one_side_unsupported/specs/data_skipping_or_one_side_unsupported_read_a_eq_1_or_casta_as_string_like_x",
        "data_skipping_long_strings_min/specs/data_skipping_long_strings_min_read_a_like_aaa",
        "data_skipping_string_patterns/specs/data_skipping_string_patterns_read_miss_x",
        "data_skipping_starts_with_nested/specs/data_skipping_starts_with_nested_read_ab_like_b",
        "data_skipping_or_one_side_unsupported/specs/data_skipping_or_one_side_unsupported_read_a_gt_5_or_casta_as_string_like_1",
        "data_skipping_starts_with_nested/specs/data_skipping_starts_with_nested_read_ab_like_a",
        "data_skipping_starts_with/specs/data_skipping_starts_with_read_miss_c",
        "data_skipping_starts_with/specs/data_skipping_starts_with_read_miss_z",
        "data_skipping_starts_with_nested/specs/data_skipping_starts_with_nested_read_ab_like_app",
        "data_skipping_and_one_side_unsupported/specs/data_skipping_and_one_side_unsupported_read_a_eq_1_and_casta_as_string_like_1",
        "data_skipping_and_one_side_unsupported/specs/data_skipping_and_one_side_unsupported_read_miss_and_unsupported",
        "data_skipping_string_patterns/specs/data_skipping_string_patterns_read_miss_z",
        "data_skipping_starts_with/specs/data_skipping_starts_with_read_a_like_b",
        "data_skipping_starts_with/specs/data_skipping_starts_with_read_a_like_app",
        "data_skipping_nulls_only_null/specs/data_skipping_nulls_only_null_read_like_x",
        "data_skipping_string_patterns/specs/data_skipping_string_patterns_read_name_like_ch",
        "data_skipping_string_patterns/specs/data_skipping_string_patterns_read_name_like_a",
    ],
    },
    // Kernel provides latest_version_as_of, but the harness currently accepts only explicit
    // versions and does not parse a DAT timestamp or resolve it to a version. See
    // delta-kernel-rs#3444.
    ExpectedFailure {
        reason: "Acceptance harness does not resolve timestamp time travel to a table version",
        expected_error: "Timestamp-based time travel is not yet supported",
        patterns: &[
        "time_travel_relation_caching/specs/time_travel_relation_caching_read_ts_2026-09-29_19-01-41.756",
        "time_travel_exact_timestamp/specs/time_travel_exact_timestamp_read_ts_2026-09-29_19-01-50.422",
        "time_travel_column_defaults/specs/time_travel_column_defaults_read_ts_2026-09-29_19-01-24.310",
        "time_travel_sql_syntax/specs/time_travel_sql_syntax_read_ts_2026-09-29_19-01-46.205",
        "time_travel_timestamp_between_commits/specs/time_travel_timestamp_between_commits_read_ts_2026-09-29_19-02-31.567",
        "in_commit_timestamp_multiple_commits/specs/in_commit_timestamp_multiple_commits_timestamp_v0",
        "time_travel_timestamps/specs/time_travel_timestamps_read_ts_v2",
        "time_travel_schema_evolution/specs/time_travel_schema_evolution_read_ts_2026-09-29_19-02-53.323",
        "time_travel_partition_evolution/specs/time_travel_partition_evolution_read_ts_2026-09-29_19-01-58.646",
        "time_travel_column_defaults/specs/time_travel_column_defaults_read_ts_2026-09-29_19-01-24.426",
        "time_travel_timestamp_between/specs/time_travel_timestamp_between_read_ts_2026-09-29_19-02-04.582",
        "in_commit_timestamp_time_travel/specs/in_commit_timestamp_time_travel_timestamp_v2",
        "time_travel_timestamp_between_commits/specs/time_travel_timestamp_between_commits_read_ts_2026-09-29_19-02-31.446",
        "time_travel_timestamps/specs/time_travel_timestamps_snapshot_ts_2026-09-29_19-00-48.554",
        "time_travel_version_read/specs/time_travel_version_read_read_ts_2026-09-29_19-02-45.649",
        "time_travel_schema_evolution/specs/time_travel_schema_evolution_read_ts_2026-09-29_19-02-53.215",
        "time_travel_timestamps/specs/time_travel_timestamps_read_ts_v1",
        "time_travel_timestamp_between/specs/time_travel_timestamp_between_read_ts_2026-09-29_19-02-04.674",
        "in_commit_timestamp_time_travel/specs/in_commit_timestamp_time_travel_timestamp_v1",
        "time_travel_multi_version_scans/specs/time_travel_multi_version_scans_read_ts_2026-09-29_19-01-54.662",
        "time_travel_partition_evolution/specs/time_travel_partition_evolution_read_ts_2026-09-29_19-01-58.796",
        "time_travel_at_syntax/specs/time_travel_at_syntax_read_ts_2026-09-29_19-02-16.819",
        "in_commit_timestamp_time_travel/specs/in_commit_timestamp_time_travel_timestamp_v0",
        "in_commit_timestamp_multiple_commits/specs/in_commit_timestamp_multiple_commits_timestamp_v2",
        "time_travel_version_read/specs/time_travel_version_read_read_ts_2026-09-29_19-02-45.770",
        "time_travel_timestamp_travel/specs/time_travel_timestamp_travel_read_ts_2026-09-29_19-03-00.121",
        "time_travel_timestamp_travel/specs/time_travel_timestamp_travel_read_ts_2026-09-29_19-02-59.999",
        "in_commit_timestamp_multiple_commits/specs/in_commit_timestamp_multiple_commits_timestamp_v1",
    ],
    },
    // The parser produces Column IN Literal(Array), but the Arrow evaluator only implements other
    // operand shapes. See delta-kernel-rs#3447.
    ExpectedFailure {
        reason: "Kernel expression evaluation does not support a column IN a literal list",
        expected_error: "Invalid right value for (NOT) IN comparison",
        patterns: &[
        "DV-004/specs/DV-004_filter_300_787_239",
        "cks_dv_in_crc/specs/cks_dv_in_crc_read_remaining",
        "dpReadPartitionAfterAppend/specs/dpReadPartitionAfterAppend_filterPartInCD",
        "dpReadPartitionIn/specs/dpReadPartitionIn_filterPartInAC",
        "dpReadPartitionIn/specs/dpReadPartitionIn_filterPartInBDE",
        "ds_in_list/specs/ds_in_list_in_list_single_file",
        "ds_in_list/specs/ds_in_list_in_list",
        "ds_in_nested/specs/ds_in_nested_hit_code_in_1_2",
        "ds_in_nested/specs/ds_in_nested_miss_code_in_99",
        "ds_in_set/specs/ds_in_set_hit_in_1",
        "ds_in_set/specs/ds_in_set_hit_in_12",
        "ds_in_set/specs/ds_in_set_hit_in_123",
        "ds_in_set/specs/ds_in_set_miss_in_456",
        "ds_in_with_nulls_mixed/specs/ds_in_with_nulls_mixed_hit_in_1_null",
        "ds_in_with_nulls_mixed/specs/ds_in_with_nulls_mixed_miss_in_99_null",
        "ds_in_with_nulls_only/specs/ds_in_with_nulls_only_hit_in_1_2",
        "ds_in_with_nulls_only/specs/ds_in_with_nulls_only_hit_in_5",
        "ds_in_with_thresholds/specs/ds_in_with_thresholds_hit_in_cross_files",
        "ds_in_with_thresholds/specs/ds_in_with_thresholds_hit_in_small",
        "ds_in_with_thresholds/specs/ds_in_with_thresholds_miss_in_no_match",
        "ds_not_in/specs/ds_not_in_not_in_1_2",
        "ds_not_in/specs/ds_not_in_not_in_3",
        "ds_not_in/specs/ds_not_in_not_in_all",
        "ds_not_in/specs/ds_not_in_not_in_outside",
        "dsReadInPredicate/specs/dsReadInPredicate_readInSet",
        "dv_partition_pruning/specs/dv_partition_pruning_prune_north_or_east",
        "tt_partition_filter/specs/tt_partition_filter_v0_part_0_or_1",
        "data_skipping_not_in/specs/data_skipping_not_in_read_a_not_in_1",
        "data_skipping_in_with_thresholds/specs/data_skipping_in_with_thresholds_read_miss_in_large",
        "data_skipping_in_set/specs/data_skipping_in_set_read_a_in_3",
        "data_skipping_in_nested/specs/data_skipping_in_nested_read_miss_nested_in",
        "deletion_vectors_partition_pruning/specs/deletion_vectors_partition_pruning_read_region_in_north_east",
        "data_skipping_in_set/specs/data_skipping_in_set_read_a_in_1_2",
        "data_skipping_in_with_nulls_only/specs/data_skipping_in_with_nulls_only_read_a_in_null",
        "data_skipping_in_with_nulls_mixed/specs/data_skipping_in_with_nulls_mixed_read_in_null_miss",
        "data_skipping_in_with_thresholds/specs/data_skipping_in_with_thresholds_read_in_large",
        "data_skipping_in_list/specs/data_skipping_in_list_read_miss_in_99",
        "data_skipping_nulls_mixed/specs/data_skipping_nulls_mixed_read_in_miss_5",
        "data_skipping_not_in/specs/data_skipping_not_in_read_not_in_all",
        "data_skipping_in_set/specs/data_skipping_in_set_read_miss_in",
        "data_skipping_in_with_nulls_mixed/specs/data_skipping_in_with_nulls_mixed_read_a_in_1_null",
        "deletion_vectors_partition_pruning_combined/specs/deletion_vectors_partition_pruning_combined_read_part_in_ab",
        "data_skipping_nulls_only_null/specs/data_skipping_nulls_only_null_read_in_1_2",
        "data_skipping_in_nested/specs/data_skipping_in_nested_read_sx_in_1_5",
        "data_skipping_not_in/specs/data_skipping_not_in_read_a_not_in_4_5",
        "data_skipping_in_with_thresholds/specs/data_skipping_in_with_thresholds_read_a_in_1_2_3",
        "data_skipping_in_list/specs/data_skipping_in_list_read_a_in_10_30",
        "data_skipping_nulls_mixed/specs/data_skipping_nulls_mixed_read_a_in_1_3",
        "data_skipping_in_with_nulls_only/specs/data_skipping_in_with_nulls_only_read_a_in_1",
    ],
    },
    // The workload's expected Parquet data stores the Variant fields as value, metadata, while
    // Kernel exposes them in the table schema's metadata, value order.
    ExpectedFailure {
        reason: "Workload and Kernel disagree on Variant field order",
        expected_error: concat!(
            "Expected field order [\"value\", \"metadata\"] does not match result field order ",
            "[\"metadata\", \"value\"]"
        ),
        patterns: &[
            "variant_all_json_types/specs/variant_all_json_types_read_all",
            "variant_array_variant/specs/variant_array_variant_read_all",
            "variant_basic/specs/variant_basic_read_all",
            "variant_basic/specs/variant_basic_select_variant_col",
            "variant_basic_stats/specs/variant_basic_stats_read_all",
            "variant_change_tracking_read/specs/variant_change_tracking_read_read_all",
            "variant_column_mapping/specs/variant_column_mapping_filter_by_id",
            "variant_column_mapping/specs/variant_column_mapping_read_all",
            "variant_deeply_nested/specs/variant_deeply_nested_read_all",
            "variant_different_types/specs/variant_different_types_filter_by_id",
            "variant_different_types/specs/variant_different_types_read_all",
            "variant_extreme_values/specs/variant_extreme_values_read_all",
            "variant_in_struct/specs/variant_in_struct_filter_label",
            "variant_in_struct/specs/variant_in_struct_read_all",
            "variant_large_array/specs/variant_large_array_read_all",
            "variant_many_fields/specs/variant_many_fields_read_all",
            "variant_map_variant/specs/variant_map_variant_filter_by_id",
            "variant_map_variant/specs/variant_map_variant_read_all",
            "variant_missing_values/specs/variant_missing_values_read_all",
            "variant_mixed_types/specs/variant_mixed_types_filter_half",
            "variant_mixed_types/specs/variant_mixed_types_read_all",
            "variant_nested_fields/specs/variant_nested_fields_read_all",
            "variant_nested_stats/specs/variant_nested_stats_read_all",
            "variant_non_objects/specs/variant_non_objects_filter_first_three",
            "variant_non_objects/specs/variant_non_objects_read_all",
            "variant_null_counts/specs/variant_null_counts_read_all",
            "variant_null_top_level/specs/variant_null_top_level_read_all",
            "variant_numeric_precision/specs/variant_numeric_precision_read_all",
            "variant_optimized/specs/variant_optimized_filter_after_optimize",
            "variant_optimized/specs/variant_optimized_read_all",
            "variant_partitions/specs/variant_partitions_filter_partition",
            "variant_partitions/specs/variant_partitions_read_all",
            "variant_predicate_non_variant/specs/variant_predicate_non_variant_filter_category_A",
            "variant_predicate_non_variant/specs/variant_predicate_non_variant_read_all",
            "variant_projection/specs/variant_projection_project_id_data",
            "variant_projection/specs/variant_projection_read_all",
            "variant_schema_evolution/specs/variant_schema_evolution_filter_new_column",
            "variant_schema_evolution/specs/variant_schema_evolution_read_all",
            "variant_schema_evolution/specs/variant_schema_evolution_read_v2_before_evolution",
            "variant_stat_fields/specs/variant_stat_fields_filter_by_id",
            "variant_stat_fields/specs/variant_stat_fields_read_all",
            "variant_string_skipping/specs/variant_string_skipping_filter_middle",
            "variant_string_skipping/specs/variant_string_skipping_read_all",
            "variant_time_travel/specs/variant_time_travel_read_latest",
            "variant_time_travel/specs/variant_time_travel_read_v1",
            "variant_time_travel/specs/variant_time_travel_read_v2",
            "variant_unicode_escapes/specs/variant_unicode_escapes_read_all",
            "variant_unusual_chars/specs/variant_unusual_chars_read_all",
        ],
    },
    // The expected Parquet files use nanosecond timestamps for values outside Spark's normal
    // timestamp range. The validator cannot losslessly normalize them to Spark microseconds.
    ExpectedFailure {
        reason: "Expected timestamp data cannot be normalized to Spark microsecond precision",
        expected_error: "Expected Spark timestamp has sub-microsecond precision",
        patterns: &[
            "write_boundary_date_and_timestamp_range_ends/specs/write_boundary_date_and_timestamp_range_ends_latest",
            "write_boundary_date_and_timestamp_range_ends/specs/write_boundary_date_and_timestamp_range_ends_read_all",
            "write_boundary_date_and_timestamp_range_ends/specs/write_boundary_date_and_timestamp_range_ends_read_max_date",
            "write_boundary_extreme_values_with_checkpoint/specs/write_boundary_extreme_values_with_checkpoint_latest",
            "write_boundary_extreme_values_with_checkpoint/specs/write_boundary_extreme_values_with_checkpoint_read_all",
        ],
    },
    // Despite the workload names, the files contain no duplicate add or remove action. They contain
    // an empty line where Spark expects a JSON action. Kernel skips it while Spark rejects the commit.
    // See delta-kernel-rs#3448.
    ExpectedFailure {
        reason: "Kernel accepts empty lines in JSON commits",
        expected_error: "Expected error 'SparkException' but succeeded",
        patterns: &[
        "corruption_err_duplicate_add_same_version/specs/corruption_err_duplicate_add_same_version_snapshot",
        "corruption_err_duplicate_add_same_version/specs/corruption_err_duplicate_add_same_version_read_all",
        "corruption_only_remove_file/specs/corruption_only_remove_file_snapshot",
        "evolvability_fc_empty_json_line/specs/evolvability_fc_empty_json_line_snapshot",
        "corruption_only_remove_file/specs/corruption_only_remove_file_read_all",
        "evolvability_fc_empty_json_line/specs/evolvability_fc_empty_json_line_read_all",
    ],
    },
    // `_last_checkpoint` is only a hint. Spark falls back to an earlier checkpoint or replays the
    // JSON log when the referenced checkpoint is missing, incomplete, or nonexistent. Kernel treats
    // the unusable hint as fatal instead. See delta-kernel-rs#582.
    ExpectedFailure {
        reason: "Kernel cannot fall back to log replay when a checkpoint hint is unusable",
        expected_error: "Had a _last_checkpoint hint but didn't find any checkpoints",
        patterns: &[
        "checkpoints_missing_checkpoint_file/specs/checkpoints_missing_checkpoint_file_snapshot",
        "checkpoints_incomplete_multipart/specs/checkpoints_incomplete_multipart_read_all",
        "checkpoints_missing_checkpoint_file/specs/checkpoints_missing_checkpoint_file_read_all",
        "corruption_wrong_last_checkpoint/specs/corruption_wrong_last_checkpoint_read_id_lt_5",
        "corruption_wrong_last_checkpoint/specs/corruption_wrong_last_checkpoint_read_all",
        "checkpoints_incomplete_multipart/specs/checkpoints_incomplete_multipart_snapshot",
    ],
    },
];

fn expected_kernel_failure(spec_id: &str) -> Option<(&'static str, &'static str)> {
    EXPECTED_KERNEL_FAILURES
        .iter()
        .find(|failure| failure.patterns.contains(&spec_id))
        .map(|failure| (failure.reason, failure.expected_error))
}

fn acceptance_workloads_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let manifest_root = Path::new(env!["CARGO_MANIFEST_DIR"]);
    let corpus_root = manifest_root.join("workloads");
    let spec_path_abs = manifest_root.join(spec_path);
    let spec_id = corpus_relative_spec_id(&spec_path_abs, &corpus_root)?;
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();
    // Normalize Windows backslashes to forward slashes for pattern matching
    #[cfg(windows)]
    let spec_path_str = spec_path_str.replace('\\', "/");

    // A workload directory can contain several specs for the same table. Skip unsupported
    // operations individually so companion read and snapshot specs still exercise that table.
    let test_case = match acceptance::acceptance_workloads::TestCase::load(&spec_path_abs)? {
        LoadedTestCase::Supported(test_case) => test_case,
        LoadedTestCase::Unsupported(spec_type)
            if matches!(spec_type.as_str(), "cdf" | "checkpoint" | "crc" | "write") =>
        {
            return Ok(())
        }
        LoadedTestCase::Unsupported(spec_type) => {
            panic!(
                "Workload spec '{}' has unknown type '{spec_type}'",
                spec_path_abs.display()
            )
        }
    };

    // Expected failures still run so we detect when Kernel gains support. Error-checked entries
    // also reject failures from an unrelated regression.
    let expected_failure = expected_kernel_failure(&spec_id);

    if expected_failure.is_none() && should_skip_test(&spec_path_str).is_some() {
        return Ok(());
    }

    let table_root = test_case.table_root().expect("Failed to get table URL");
    let engine = test_utils::create_default_engine(&table_root).expect("Failed to create engine");
    let result = execute_and_validate_workload(
        engine,
        &table_root,
        &test_case.spec,
        &test_case.expected_dir(),
    );

    match (result, expected_failure) {
        (Err(error), Some((reason, expected_error)))
            if !error.to_string().contains(expected_error) =>
        {
            panic!(
                "Workload '{}' failed for an unexpected reason. Expected: {reason} \
                 ({expected_error}). Actual: {error}",
                test_case.workload_name
            )
        }
        (Err(_), Some(_)) => {}
        (Ok(_), None) => {}
        (Ok(_), Some((reason, _))) => panic!(
            "Workload '{}' was expected to fail but succeeded! \
             Reason: {reason}. Remove from EXPECTED_KERNEL_FAILURES!",
            test_case.workload_name
        ),
        (Err(e), None) => panic!("Workload '{}' failed: {}", test_case.workload_name, e),
    }
    Ok(())
}

datatest_stable::harness! {
    {
        test = acceptance_workloads_test,
        root = "workloads/",
        pattern = r"specs/.*\.json$"
    },
}
