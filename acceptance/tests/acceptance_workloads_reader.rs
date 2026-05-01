//! Test harness for acceptance workloads.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! acceptance_workloads directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::acceptance_workloads::workload::execute_and_validate_workload;
use acceptance::acceptance_workloads::TestCase;

/// Tests that cannot be executed due to test harness limitations.
/// These fail at parse time or cause infrastructure issues (OOM, hang).
/// All other failures (bugs, divergences, missing features) go in EXPECTED_KERNEL_FAILURES.
const SKIP_LIST: &[(&str, &str)] = &[
    ("DV-017/", "Huge table (2B rows) causes OOM/hang"),
    // The Spec enum only supports "read" and "snapshot" types.
    // CDF, transaction, and domain metadata specs fail at parse time.
    ("_cdf", "CDF (Change Data Feed) test type not yet supported"),
    ("_txn", "Transaction test type not yet supported"),
    (
        "_domain_metadata",
        "Domain metadata test type not yet supported",
    ),
    // These have `"type": "cdf"` which the harness cannot parse.
    (
        "cdc_err_",
        "CDF test type not yet supported (fails at parse time)",
    ),
    // These tests panic in reorder_struct_array (arrow_utils.rs:1068) with "index out of bounds".
    // Both have predicate `flag = true` on tables with 6 columns (including timestamp/decimal).
    // Both tables were created via CLONE/RESTORE operations. Simpler boolean predicate tests pass.
    //
    // Root cause: When data skipping evaluates the predicate, kernel builds a stats schema.
    // Stats have nested structure (minValues/maxValues/nullCount are nested structs), but
    // boolean columns have no min/max stats - only nullCount. This causes a mismatch between
    // the expected stats schema and actual stats, leading to an invalid array index access
    // during column reordering. The bug is in kernel's data skipping stats handling, not
    // predicate parsing. Tests without predicates on these same tables work fine.
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
const EXPECTED_KERNEL_FAILURES: &[(&str, &[&str])] = &[
    // Kernel reads timestamps as Timestamp(Microsecond, Some("UTC")),
    // Spark writes expected data as Timestamp(Nanosecond, None).
    // Same instant, different Arrow representation.
    (
        "Timestamp type divergence (kernel uses Microsecond/UTC, Spark uses Nanosecond/None)",
        &[
            "cloneDeepMultiType/specs/cloneDeepMultiType_readAll",
            "cr_timestamp_boundaries/specs/cr_timestamp_boundaries_read_all",
            "dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_high_amount",
            "dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_by_date",
            "dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_all",
            "dpReadPartitionTimestamp/specs/dpReadPartitionTimestamp_readAll",
            "dsReadMultipleTypes/specs/dsReadMultipleTypes_readAll",
            "dsReadTimestampType/specs/dsReadTimestampType_readAll",
            "ds_datetime/specs/ds_datetime_hit_dt_eq",
            "ds_datetime/specs/ds_datetime_hit_dt_gte",
            "ds_datetime/specs/ds_datetime_hit_dt_lt",
            "ds_datetime/specs/ds_datetime_miss_dt_2023",
            "ds_datetime/specs/ds_datetime_miss_dt_2025",
            "ds_date_trunc_timestamp/specs/ds_date_trunc_timestamp_hit_trunc_month_june",
            "ds_date_trunc_timestamp/specs/ds_date_trunc_timestamp_hit_trunc_month_march",
            "ds_date_trunc_timestamp/specs/ds_date_trunc_timestamp_miss_trunc_month_jan",
            "gc_datetime/specs/gc_datetime_filter_date",
            "gc_datetime/specs/gc_datetime_read_all",
            "ntz_mixed_tz_ntz/specs/ntz_mixed_tz_ntz_filter_ntz_col",
            "ntz_mixed_tz_ntz/specs/ntz_mixed_tz_ntz_full_scan",
            "pve_timestamp_partition/specs/pve_timestamp_partition_read_all",
            "restoreCheckData/specs/restoreCheckData_filterDecimal",
            "restoreCheckData/specs/restoreCheckData_readAll",
            "st_datetime_stats/specs/st_datetime_stats_filter_date_eq",
            "st_datetime_stats/specs/st_datetime_stats_filter_date_range",
            "st_datetime_stats/specs/st_datetime_stats_full_scan",
            "cdc_multiple_types/specs/cdc_multiple_types_read_all",
            "cdc_timestamp_tz_handling/specs/cdc_timestamp_tz_handling_read_all",
            "gc_append_data/specs/gc_append_data_read_all",
            "gc_delete/specs/gc_delete_read_all",
            "gc_insert_by_name/specs/gc_insert_by_name_read_all",
            "gc_update_source/specs/gc_update_source_read_all",
        ],
    ),
    // Kernel produces {metadata, value}, Spark produces {value, metadata}.
    (
        "Variant field order divergence (kernel {metadata,value}, Spark {value,metadata})",
        &[
            "var_001_basic/specs/var_001_basic_read_all",
            "var_001_basic/specs/var_001_basic_select_variant_col",
            "var_002_basic_stats/specs/var_002_basic_stats_read_all",
            "var_003_nested_stats/specs/var_003_nested_stats_read_all",
            "var_004_non_objects/specs/var_004_non_objects_filter_first_three",
            "var_004_non_objects/specs/var_004_non_objects_read_all",
            "var_005_null_counts/specs/var_005_null_counts_filter_non_null",
            "var_005_null_counts/specs/var_005_null_counts_read_all",
            "var_006_different_types/specs/var_006_different_types_filter_by_id",
            "var_006_different_types/specs/var_006_different_types_read_all",
            "var_007_partitions/specs/var_007_partitions_filter_partition",
            "var_007_partitions/specs/var_007_partitions_read_all",
            "var_008_many_fields/specs/var_008_many_fields_read_all",
            "var_009_unusual_chars/specs/var_009_unusual_chars_read_all",
            "var_010_nested_fields/specs/var_010_nested_fields_read_all",
            "var_011_missing_values/specs/var_011_missing_values_read_all",
            "var_012_mixed_types/specs/var_012_mixed_types_filter_half",
            "var_012_mixed_types/specs/var_012_mixed_types_read_all",
            "var_013_extreme_values/specs/var_013_extreme_values_read_all",
            "var_014_variant_in_struct/specs/var_014_variant_in_struct_filter_label",
            "var_014_variant_in_struct/specs/var_014_variant_in_struct_read_all",
            "var_015_string_skipping/specs/var_015_string_skipping_filter_middle",
            "var_015_string_skipping/specs/var_015_string_skipping_read_all",
            "var_016_array_variant/specs/var_016_array_variant_filter_array_size",
            "var_016_array_variant/specs/var_016_array_variant_read_all",
            "var_017_map_variant/specs/var_017_map_variant_filter_by_id",
            "var_017_map_variant/specs/var_017_map_variant_read_all",
            "var_018_column_mapping/specs/var_018_column_mapping_filter_by_id",
            "var_018_column_mapping/specs/var_018_column_mapping_read_all",
            "var_019_schema_evolution/specs/var_019_schema_evolution_filter_new_column",
            "var_019_schema_evolution/specs/var_019_schema_evolution_read_all",
            "var_019_schema_evolution/specs/var_019_schema_evolution_read_v2_before_evolution",
            "var_020_time_travel/specs/var_020_time_travel_read_latest",
            "var_020_time_travel/specs/var_020_time_travel_read_v1",
            "var_020_time_travel/specs/var_020_time_travel_read_v2",
            "var_021_optimized/specs/var_021_optimized_filter_after_optimize",
            "var_021_optimized/specs/var_021_optimized_read_all",
            "var_022_stat_fields/specs/var_022_stat_fields_read_all",
            "var_all_json_types_read_all",
            "var_cdf_read/specs/var_cdf_read_read_all",
            "var_deeply_nested/specs/var_deeply_nested_read_all",
            "var_large_array/specs/var_large_array_read_all",
            "var_null_top_level/specs/var_null_top_level_filter_not_null",
            "var_null_top_level/specs/var_null_top_level_read_all",
            "var_numeric_precision/specs/var_numeric_precision_read_all",
            "var_predicate_non_variant/specs/var_predicate_non_variant_filter_category_A",
            "var_predicate_non_variant/specs/var_predicate_non_variant_read_all",
            "var_projection/specs/var_projection_project_id_data",
            "var_projection/specs/var_projection_read_all",
            "var_unicode_escapes/specs/var_unicode_escapes_read_all",
            "ds_variant_null_stats/specs/ds_variant_null_stats_hit_null_v_is_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_hit_null_v_struct_v_is_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_hit_v_is_not_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_hit_v_struct_v_not_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_miss_null_v_is_not_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_miss_null_v_struct_v_not_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_miss_v_is_null",
            "ds_variant_null_stats/specs/ds_variant_null_stats_miss_v_struct_v_is_null",
            "var_null_top_level/specs/var_null_top_level_filter_null",
        ],
    ),
    (
        "Arrow parquet reader cannot annotate Unknown logical type from BOOLEAN physical type",
        &[
            "void_001_void_top_level/specs/void_001_void_top_level_read_all",
            "void_002_void_nested_struct/specs/void_002_void_nested_struct_read_all",
            "void_005_void_schema_evolution/specs/void_005_void_schema_evolution_read_all",
            "void_006_void_multiple_columns/specs/void_006_void_multiple_columns_read_all",
            "void_007_void_with_backticks/specs/void_007_void_with_backticks_read_all",
        ],
    ),
    (
        "Interval types not supported in schema deserialization",
        &[
            "intv_001_interval_ym_basic/",
            "intv_002_interval_dt_basic/",
            "intv_003_interval_partitioned/",
            "intv_004_interval_negative/",
            "intv_005_interval_mixed/",
            "intv_boundary_values/",
            "intv_sub_second/",
        ],
    ),
    (
        "Cannot fall back to log replay when checkpoint files are missing or incomplete",
        &[
            "corrupt_incomplete_multipart_checkpoint/",
            "ckp_incomplete_multipart/",
            "ckp_missing_checkpoint_file/",
        ],
    ),
    (
        "Cannot cast list to non-list data types during type widening",
        &[
            "tw_array_element/specs/tw_array_element_read_",
            "tw_map_key_value_widening/specs/tw_map_key_value_widening_read_all",
        ],
    ),
    (
        "Type widening metadata mismatch in nested struct fields",
        &[
            "tw_nested_field/specs/tw_nested_field_read_all",
            "tw_nested_field/specs/tw_nested_field_read_large_count",
        ],
    ),
    (
        "Schema deserialization fails for TimestampNTZ type",
        &["ds_multi_file_time/"],
    ),
    (
        "Column mapping id mode fails with None in final_fields_cols",
        &[
            "cm_id_matching_swapped/specs/cm_id_matching_swapped_select_",
            "cm_id_matching_nonexistent/specs/cm_id_matching_nonexistent_select_",
        ],
    ),
    (
        "Absolute-path DV has invalid percent-encoded path",
        &["dv_storage_type_p/specs/dv_storage_type_p_read_after_absolute_path_dv"],
    ),
    (
        "Fails on missing/empty delta log (no files in log segment)",
        &[
            "ct_empty_delta_log/specs/ct_empty_delta_log_snapshot",
            "ct_missing_delta_log/specs/ct_missing_delta_log_snapshot",
            "dseReadNonDeltaPath/specs/dseReadNonDeltaPath_snapshot",
        ],
    ),
    (
        "Accepts truncated log when initial commits are missing but CRC files exist",
        &["prod_truncated_log/"],
    ),
    (
        "Accepts checkpoint-only tables (no commits)",
        &["cp_checkpoint_only_table/specs/cp_checkpoint_only_table_error"],
    ),
    (
        "Projected column not found after column mapping/schema order change",
        &[
            "ds_schema_order_mismatch/specs/ds_schema_order_mismatch_single_col_last",
            "ds_with_dvs_edge/specs/ds_with_dvs_edge_proj_and_skip_with_dv",
            "dv_projection_with_pred/specs/dv_projection_with_pred_proj_and_pred",
        ],
    ),
    (
        "Does not reject unsupported column mapping mode",
        &["cm_err_003_invalid_mode/specs/cm_err_003_invalid_mode_error"],
    ),
    (
        "Reads corrupt/invalid commit or checkpoint without error",
        &[
            "corrupt_truncated_commit_json_error",
            "cp_err_missing_protocol/specs/cp_err_missing_protocol_error",
            "ct_corrupt_parquet/specs/ct_corrupt_parquet_error",
            "ct_invalid_json_error",
            "dsReadCorruptCheckpoint/specs/dsReadCorruptCheckpoint_error",
            "dsReadCorruptJson_error",
            "dsReadModifyCheckpoint/specs/dsReadModifyCheckpoint_error",
        ],
    ),
    (
        "Does not reject duplicate actions in commit",
        &[
            "ct_duplicate_metadata/specs/ct_duplicate_metadata_error",
            "ct_duplicate_protocol/specs/ct_duplicate_protocol_error",
            "err_duplicate_add_same_version/specs/err_duplicate_add_same_version_error",
        ],
    ),
    (
        "Snapshot construction succeeds even when data files are missing",
        &[
            "ct_missing_data_file/specs/ct_missing_data_file_error",
            "dv_err_002_missing_file/specs/dv_err_002_missing_file_error",
        ],
    ),
    (
        "Does not validate DV integrity",
        &[
            "dv_err_001_checksum/specs/dv_err_001_checksum_error",
            "dv_err_003_malformed_path/specs/dv_err_003_malformed_path_error",
            "err_dv_invalid_storage_type/specs/err_dv_invalid_storage_type_error",
            "err_add_and_remove_same_path_dv/specs/err_add_and_remove_same_path_dv_error",
        ],
    ),
    (
        "Does not validate schema integrity",
        &[
            "err_schema_empty/specs/err_schema_empty_error",
            "err_schema_invalid_json_error",
        ],
    ),
    (
        "Does not require version 0 to exist",
        &["err_missing_version_0/specs/err_missing_version_0_error"],
    ),
    (
        "Does not reject unknown reader features",
        &["ev_unknown_reader_feature/specs/ev_unknown_reader_feature_error"],
    ),
    (
        "Does not enforce time travel safety",
        &[
            "tt_blocked_beyond_retention/specs/tt_blocked_beyond_retention_error",
            "tt_after_vacuum/specs/tt_after_vacuum_error",
        ],
    ),
    (
        "_metadata.file_path column projection not supported",
        &["DV-003/specs/DV-003_metadata_file_path"],
    ),
    (
        "variantShredding feature not supported",
        &["pv_002_upgrade_to_current/specs/pv_002_upgrade_to_current_read_latest"],
    ),
    // Predicate parser: LIKE operator not supported
    (
        "Predicate parser: LIKE operator not supported",
        &[
            "ds_and_two_fields_hit_and_a_b_like",
            "ds_and_two_fields_miss_and_b_like_2016",
            "ds_long_strings_max_hit_like_A",
            "ds_long_strings_max_hit_like_C",
            "ds_long_strings_min_hit_like_A",
            "ds_or_two_fields_hit_or_a_b_like",
            "ds_or_two_fields_hit_or_a_b_like2",
            "ds_or_two_fields_miss_or_b_like_2016",
            "ds_starts_with_hit_like_a",
            "ds_starts_with_hit_like_all",
            "ds_starts_with_hit_like_ap",
            "ds_starts_with_hit_like_m",
            "ds_starts_with_hit_like_mic",
            "ds_starts_with_miss_like_xyz",
            "ds_starts_with_nested_hit_like_a",
            "ds_starts_with_nested_hit_like_all",
            "ds_starts_with_nested_hit_like_ap",
            "ds_starts_with_nested_hit_like_m",
            "ds_starts_with_nested_hit_like_mic",
            "ds_starts_with_nested_miss_like_xyz",
            "ds_string_patterns_hit_like_a",
            "ds_string_patterns_hit_like_all",
            "ds_string_patterns_hit_like_b",
            "ds_string_patterns_miss_like_z",
            "dsReadLikePredicate/specs/dsReadLikePredicate_filterLikeAlp",
            "se_rename_pred/specs/se_rename_pred_pred_full_name_like",
            "st_string_stats/specs/st_string_stats_filter_string_like",
        ],
    ),
    // Predicate parser: CAST function not supported
    (
        "Predicate parser: CAST function not supported",
        &[
            "dpReadPartitionTimestamp/specs/dpReadPartitionTimestamp_filterTsEq",
            "dpReadPartitionTimestamp/specs/dpReadPartitionTimestamp_filterTsGt",
            "pve_byte_partition/specs/pve_byte_partition_filter_max",
            "pve_byte_partition/specs/pve_byte_partition_filter_min",
            "pve_byte_partition/specs/pve_byte_partition_filter_positive",
            "pve_float_partition/specs/pve_float_partition_filter_eq",
            "pve_float_partition/specs/pve_float_partition_filter_positive",
            "pve_short_partition/specs/pve_short_partition_filter_max",
            "pve_short_partition/specs/pve_short_partition_filter_min",
            "pve_short_partition/specs/pve_short_partition_filter_range",
        ],
    ),
    // Predicate parser: date/time functions not supported (date_add, datediff, month, year, trunc)
    (
        "Predicate parser: date/time functions not supported",
        &[
            "ds_date_add_sub_hit_date_add_all",
            "ds_date_add_sub_hit_date_add_gt",
            "ds_date_add_sub_miss_date_add_future",
            "ds_datediff_hit_datediff_gt_30",
            "ds_datediff_hit_datediff_lt_20",
            "ds_month_function_hit_month_1",
            "ds_month_function_hit_month_6",
            "ds_month_function_miss_month_12",
            "ds_trunc_date_hit_trunc_year_2023",
            "ds_trunc_date_hit_trunc_year_2024",
            "ds_trunc_date_miss_trunc_year_2020",
            "ds_year_function_hit_year_2023",
            "ds_year_function_hit_year_2024",
            "ds_year_function_miss_year_2020",
        ],
    ),
    // Predicate parser: IS NULL only supported on column references, not expressions
    (
        "Predicate parser: IS NULL only supported on column references",
        &[
            "ds_nulls_mixed_hit_a_gt_0_is_null",
            "ds_nulls_mixed_hit_a_lt_0_is_null",
            "ds_nulls_mixed_hit_not_a_gt_0_is_null",
            "ds_nulls_nonnulls_only_hit_not_isnull",
            "ds_nulls_nonnulls_only_miss_and_isnull",
            "ds_nulls_nonnulls_only_miss_gt0_isnull",
            "ds_nulls_nonnulls_only_miss_lt0_isnull",
            "ds_nulls_nonnulls_only_miss_or_isnull",
            "ds_nulls_only_nonnull_miss_and_isnull",
            "ds_nulls_only_nonnull_miss_or_isnull",
            "ds_nulls_only_null_hit_a_gt_1_is_null",
            "ds_nulls_only_null_miss_not_a_gt_1_is_null",
            "ds_nulls_partial_stats_hit_b_and_a_isnull",
            "ds_nulls_partial_stats_hit_b_gt0_isnull",
            "ds_nulls_partial_stats_hit_b_lt0_isnull",
            "ds_nulls_partial_stats_hit_b_or_a_isnull",
            "ds_nulls_partial_stats_hit_not_a_isnull",
            "ds_nulls_partial_stats_miss_a_and_isnull",
            "ds_nulls_partial_stats_miss_a_gt0_isnull",
            "ds_nulls_partial_stats_miss_a_lt0_isnull",
            "ds_nulls_partial_stats_miss_a_or_isnull",
        ],
    ),
    // Predicate parser: function calls not supported (length, size, HEX)
    (
        "Predicate parser: function calls not supported (length, size, HEX)",
        &[
            "cc_005_varchar_constraint/specs/cc_005_varchar_constraint_filter_short_string",
            "cc_009_array_constraint/specs/cc_009_array_constraint_filter_array_size",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc8_hex_1111",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc8_hex_3333",
            "ds_typed_stats/specs/ds_typed_stats_hit_c8_hex_1111",
            "ds_typed_stats/specs/ds_typed_stats_hit_c8_hex_3333",
        ],
    ),
    // Predicate parser: variant_get function not supported
    (
        "Predicate parser: variant_get function not supported",
        &["var_022_stat_fields/specs/var_022_stat_fields_filter_variant_field"],
    ),
    // Predicate: _metadata columns not supported
    (
        "Predicate: _metadata columns not supported",
        &["rt_filter_read/specs/rt_filter_read_read_filtered"],
    ),
    // Kernel evaluator doesn't support `Column IN (literal_array)` pattern yet.
    // The predicate parser correctly generates this, but evaluate_expression only handles
    // `Literal IN Column` (scalar in array column) and `Literal IN Literal(Array)`.
    (
        "Kernel: Column IN (literal_array) evaluation not supported",
        &[
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
        ],
    ),
    // Tests with simple predicates that fail due to timestamp schema mismatch
    // These have predicates like "c1 = 1" that work, but the result comparison fails
    // because kernel uses Timestamp(Microsecond, UTC) while Spark uses Timestamp(Nanosecond, None)
    (
        "Simple predicates but timestamp schema mismatch in result validation",
        &[
            "cc_007_multiple_constraints/specs/cc_007_multiple_constraints_filter_amount",
            "cc_020_decimal_constraint/specs/cc_020_decimal_constraint_filter_high_price",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c1_eq_1",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c10_gt_1_5",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c3_lt_1_5",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c4_gt_1_0",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c5_gte_ts",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c6_gte_ts_ntz",
            "ds_stats_after_drop/specs/ds_stats_after_drop_hit_c7_eq_date",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c1_eq_10",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c10_gt_2_5",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c3_lt_0_5",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c4_gt_5_0",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c5_gte_future",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c6_gte_future_ntz",
            "ds_stats_after_drop/specs/ds_stats_after_drop_miss_c7_eq_future",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc1_eq_1",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc10_gt_1_5",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc2_eq_2",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc3_lt_1_5",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc4_gt_1_0",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc5_gte_ts",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc6_gte_ts_ntz",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc7_eq_date",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc9_false",
            "ds_stats_after_rename/specs/ds_stats_after_rename_hit_cc9_true",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc1_eq_10",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc10_gt_2_5",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc2_eq_4",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc3_lt_0_5",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc4_gt_5_0",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc5_gte_future",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc6_gte_future_ntz",
            "ds_stats_after_rename/specs/ds_stats_after_rename_miss_cc7_eq_future",
            "ds_timestamp_microsecond/specs/ds_timestamp_microsecond_filter_exact_microsecond",
            "ds_timestamp_microsecond/specs/ds_timestamp_microsecond_filter_microsecond",
            "ds_timestamp_microsecond/specs/ds_timestamp_microsecond_read_all",
            "pve_timestamp_partition/specs/pve_timestamp_partition_filter_eq",
            "pve_timestamp_partition/specs/pve_timestamp_partition_filter_range",
            "ds_typed_stats/specs/ds_typed_stats_hit_c1_eq_1",
            "ds_typed_stats/specs/ds_typed_stats_hit_c10_gt_1_5",
            "ds_typed_stats/specs/ds_typed_stats_hit_c2_eq_2",
            "ds_typed_stats/specs/ds_typed_stats_hit_c3_lt_1_5",
            "ds_typed_stats/specs/ds_typed_stats_hit_c4_gt_1_0",
            "ds_typed_stats/specs/ds_typed_stats_hit_c5_gte_ts",
            "ds_typed_stats/specs/ds_typed_stats_hit_c6_gte_ts_ntz",
            "ds_typed_stats/specs/ds_typed_stats_hit_c7_eq_date",
            "ds_typed_stats/specs/ds_typed_stats_hit_c9_false",
            "ds_typed_stats/specs/ds_typed_stats_hit_c9_true",
            "ds_typed_stats/specs/ds_typed_stats_miss_c1_eq_10",
            "ds_typed_stats/specs/ds_typed_stats_miss_c10_gt_2_5",
            "ds_typed_stats/specs/ds_typed_stats_miss_c2_eq_4",
            "ds_typed_stats/specs/ds_typed_stats_miss_c3_lt_0_5",
            "ds_typed_stats/specs/ds_typed_stats_miss_c4_gt_5_0",
            "ds_typed_stats/specs/ds_typed_stats_miss_c5_gte_future",
            "ds_typed_stats/specs/ds_typed_stats_miss_c6_gte_future_ntz",
            "ds_typed_stats/specs/ds_typed_stats_miss_c7_eq_future",
            "dsReadDecimalType/specs/dsReadDecimalType_readExpensive",
            "gc_append_data/specs/gc_append_data_filter_partition",
            "gc_delete/specs/gc_delete_filter_remaining",
            "gc_insert_by_name/specs/gc_insert_by_name_filter_c2_g",
            "gc_update_source/specs/gc_update_source_filter_c2_g_updated",
            "tw_decimal_precision_read_large_values",
        ],
    ),
    (
        "Timestamp-based time travel not yet supported",
        &[
            "ict_basic_read_v0_ts",
            "ict_dml_read_v0_ts",
            "ict_enable_later_read_v0_no_ict_ts",
            "ict_enable_later_read_v1_no_ict_ts",
            "ict_enable_later_read_v3_with_ict_ts",
            "ict_enabled_mid_lifecycle_read_v0_no_ict_ts",
            "ict_enabled_mid_lifecycle_read_v1_no_ict_ts",
            "ict_from_crc_read_v0_ts",
            "ict_from_crc_read_v1_ts",
            "ict_multiple_commits_read_v0_ts",
            "ict_multiple_commits_read_v1_ts",
            "ict_multiple_commits_read_v2_ts",
            "ict_time_travel_read_v0_ts",
            "ict_time_travel_read_v1_ts",
            "ict_time_travel_read_v2_ts",
            "ict_timestamp_resolution_edges_read_ts_v2_exact",
            "ict_timestamp_resolution_edges_read_ts_v2_minus_1ms",
            "ict_timestamp_resolution_edges_read_ts_v2_plus_1ms",
            "ict_timestamp_resolution_edges_read_ts_v3_exact",
            "ict_timestamp_resolution_edges_read_ts_v3_minus_1ms",
            "ict_with_checkpoint_read_v1_ts",
            "ict_with_checkpoint_read_v3_ts",
            "tt_at_syntax_timestamp_v0",
            "tt_at_syntax_timestamp_v1",
            "tt_column_defaults_timestamp_v0_empty",
            "tt_column_defaults_timestamp_v1_null",
            "tt_exact_timestamp_version_0_exact_ts",
            "tt_exact_timestamp_version_1_exact_ts",
            "tt_multi_version_scans_timestamp_v0",
            "tt_multi_version_scans_timestamp_v1",
            "tt_partition_evolution_timestamp_v0_old_partition",
            "tt_partition_evolution_timestamp_v1_new_partition",
            "tt_relation_caching_timestamp_v0",
            "tt_relation_caching_timestamp_v1",
            "tt_schema_evolution_timestamp_v0_old_schema",
            "tt_schema_evolution_timestamp_v1_new_schema",
            "tt_sql_syntax_timestamp_v0",
            "tt_sql_syntax_timestamp_v1",
            "tt_timestamp_between_commits_timestamp_v0",
            "tt_timestamp_between_commits_timestamp_v1",
            "tt_timestamp_between_commits_timestamp_v2",
            "tt_version_read_timestamp_v0",
            "tt_version_read_timestamp_v1",
            "tt_version_read_timestamp_v2",
        ],
    ),
    // IS NULL predicates on struct columns fail because kernel's GetReferencedFields only
    // resolves primitive columns (which have stats). Struct columns don't have nullCount stats
    // at the struct level - only their primitive children do. The error "Predicate references
    // unknown column: <col>" is misleading since the column exists, it's just a struct type.
    (
        "IS NULL on struct columns: kernel only resolves primitive columns for data skipping",
        &[
            "dcscStructWithNull/specs/dcscStructWithNull_read_null",
            "dcscStructWithNull/specs/dcscStructWithNull_read_non_null",
            "ddefReadDefaultNested/specs/ddefReadDefaultNested_readNonNull",
        ],
    ),
];

fn acceptance_workloads_test(spec_path: &Path) -> datatest_stable::Result<()> {
    let spec_path_raw = format!(
        "{}/{}",
        env!["CARGO_MANIFEST_DIR"],
        spec_path.to_str().unwrap()
    );
    let spec_path_abs = std::fs::canonicalize(&spec_path_raw)
        .unwrap_or_else(|_| std::path::PathBuf::from(&spec_path_raw));
    let spec_path_str = spec_path_abs.to_string_lossy().to_string();
    // Normalize Windows backslashes to forward slashes for pattern matching
    #[cfg(windows)]
    let spec_path_str = spec_path_str.replace('\\', "/");

    // Check expected kernel failures FIRST (path matching only - these need to
    // actually run to assert kernel still fails). Skip list checked second.
    let expected_failure = EXPECTED_KERNEL_FAILURES
        .iter()
        .find(|(_, patterns)| patterns.iter().any(|p| spec_path_str.contains(p)));

    if expected_failure.is_none() && should_skip_test(&spec_path_str).is_some() {
        return Ok(());
    }

    // Load and execute test case
    let test_case = TestCase::from_spec_path(&spec_path_abs);
    let table_root = test_case.table_root().expect("Failed to get table URL");
    let engine = test_utils::create_default_engine(&table_root).expect("Failed to create engine");
    let result = execute_and_validate_workload(
        engine,
        &table_root,
        &test_case.spec,
        &test_case.expected_dir(),
    );

    match (result, expected_failure) {
        (Err(_), Some(_)) => {} // Expected to fail, did fail
        (Ok(_), None) => {}     // Expected to pass, did pass
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
