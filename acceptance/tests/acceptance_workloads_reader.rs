//! Test harness for acceptance workloads.
//!
//! This test uses datatest-stable to discover and run all workload specs in the
//! acceptance_workloads directory. Each spec file becomes its own test.

use std::path::Path;

use acceptance::acceptance_workloads::{
    test_case_from_spec_path, workload::execute_and_validate_workload,
};
use delta_kernel_benchmarks::models::{Spec, TimeTravel};

/// Tests that cannot even be executed (skip before running).
/// These are either unsupported test types, infra issues, or known divergences
/// that would cause false test failures.
const SKIP_LIST: &[(&str, &str)] = &[
        // ── Infra/perf ──
        ("DV-017/", "Huge table (2B rows) causes OOM/hang"),

        // ── Kernel divergence: timestamp type (Microsecond/UTC vs Nanosecond/None) ──
        // Kernel reads timestamps as Timestamp(Microsecond, Some("UTC")),
        // Spark writes expected data as Timestamp(Nanosecond, None).
        // Same instant, different Arrow representation.
        ("cloneDeepMultiType/specs/cloneDeepMultiType_readAll", "Timestamp type: cloneDeepMultiType read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("cloneDeepMultiType/specs/cloneDeepMultiType_readFiltered", "Timestamp type: cloneDeepMultiType filtered read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("cr_timestamp_boundaries/specs/cr_timestamp_boundaries_read_all", "Timestamp type: cr_timestamp_boundaries read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_high_amount", "Timestamp type: dcscStructWithSpecialTypes read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_by_date", "Timestamp type: dcscStructWithSpecialTypes read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("dcscStructWithSpecialTypes/specs/dcscStructWithSpecialTypes_read_all", "Timestamp type: dcscStructWithSpecialTypes read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("dpReadPartitionTimestamp/specs/dpReadPartitionTimestamp_readAll", "Timestamp type: dpReadPartitionTimestamp read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("dsReadMultipleTypes/specs/dsReadMultipleTypes_readAll", "Timestamp type: dsReadMultipleTypes read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("dsReadTimestampType/specs/dsReadTimestampType_readAll", "Timestamp type: dsReadTimestampType read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_datetime/specs/ds_datetime_hit_dt_eq", "Timestamp type: ds_datetime predicate hit has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_datetime/specs/ds_datetime_hit_dt_gte", "Timestamp type: ds_datetime predicate hit has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_datetime/specs/ds_datetime_hit_dt_lt", "Timestamp type: ds_datetime predicate hit has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_datetime/specs/ds_datetime_miss_dt_2023", "Timestamp type: ds_datetime predicate miss has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_datetime/specs/ds_datetime_miss_dt_2025", "Timestamp type: ds_datetime predicate miss has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_date_trunc_timestamp/specs/ds_date_trunc_timestamp_hit_trunc_month_june", "Timestamp type: ds_date_trunc_timestamp read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_date_trunc_timestamp/specs/ds_date_trunc_timestamp_hit_trunc_month_march", "Timestamp type: ds_date_trunc_timestamp read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_date_trunc_timestamp/specs/ds_date_trunc_timestamp_miss_trunc_month_jan", "Timestamp type: ds_date_trunc_timestamp read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_stats_after_drop/specs/ds_stats_after_drop_hit_", "Timestamp type: ds_stats_after_drop predicate hit has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_stats_after_drop/specs/ds_stats_after_drop_miss_", "Timestamp type: ds_stats_after_drop predicate miss has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_stats_after_rename/specs/ds_stats_after_rename_hit_", "Timestamp type: ds_stats_after_rename predicate hit has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_stats_after_rename/specs/ds_stats_after_rename_miss_", "Timestamp type: ds_stats_after_rename predicate miss has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_timestamp_microsecond/specs/ds_timestamp_microsecond_filter_", "Timestamp type: ds_timestamp_microsecond filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_timestamp_microsecond/specs/ds_timestamp_microsecond_read_all", "Timestamp type: ds_timestamp_microsecond read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_typed_stats/specs/ds_typed_stats_hit_", "Timestamp type: ds_typed_stats predicate hit has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ds_typed_stats/specs/ds_typed_stats_miss_", "Timestamp type: ds_typed_stats predicate miss has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("gc_datetime/specs/gc_datetime_filter_date", "Timestamp type: gc_datetime filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("gc_datetime/specs/gc_datetime_filter_hour", "Timestamp type: gc_datetime filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("gc_datetime/specs/gc_datetime_read_all", "Timestamp type: gc_datetime read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("mergeLowShuffleTimestamp/specs/mergeLowShuffleTimestamp_filter_new", "Timestamp type: mergeLowShuffleTimestamp filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("mergeLowShuffleTimestamp/specs/mergeLowShuffleTimestamp_read_all", "Timestamp type: mergeLowShuffleTimestamp read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("mergeTimestampValues/specs/mergeTimestampValues_read_all", "Timestamp type: mergeTimestampValues read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ntz_mixed_tz_ntz/specs/ntz_mixed_tz_ntz_filter_ntz_col", "Timestamp type: ntz_mixed_tz_ntz filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("ntz_mixed_tz_ntz/specs/ntz_mixed_tz_ntz_full_scan", "Timestamp type: ntz_mixed_tz_ntz read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("pve_timestamp_partition/specs/pve_timestamp_partition_read_all", "Timestamp type: pve_timestamp_partition read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("restoreCheckData/specs/restoreCheckData_filterBoolean", "Timestamp type: restoreCheckData filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("restoreCheckData/specs/restoreCheckData_filterDecimal", "Timestamp type: restoreCheckData filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("restoreCheckData/specs/restoreCheckData_readAll", "Timestamp type: restoreCheckData read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("st_datetime_stats/specs/st_datetime_stats_filter_date_eq", "Timestamp type: st_datetime_stats filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("st_datetime_stats/specs/st_datetime_stats_filter_date_range", "Timestamp type: st_datetime_stats filter has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("st_datetime_stats/specs/st_datetime_stats_full_scan", "Timestamp type: st_datetime_stats read has Timestamp(us, UTC) vs Timestamp(ns, None)"),
        ("cdc_multiple_types/specs/cdc_multiple_types_read_all", "Timestamp type: cdc_multiple_types read has Timestamp(us, UTC) vs Timestamp(ns, None)"),

        // ── Kernel divergence: variant struct field order ──
        // Kernel produces {metadata, value}, Spark produces {value, metadata}.
        // Only read/filter specs affected — snapshot specs pass.
        ("var_001_basic/specs/var_001_basic_read_all", "Variant field order: var_001_basic kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_001_basic/specs/var_001_basic_select_variant_col", "Variant field order: var_001_basic kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_002_basic_stats/specs/var_002_basic_stats_read_all", "Variant field order: var_002_basic_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_003_nested_stats/specs/var_003_nested_stats_read_all", "Variant field order: var_003_nested_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_004_non_objects/specs/var_004_non_objects_filter_first_three", "Variant field order: var_004_non_objects kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_004_non_objects/specs/var_004_non_objects_read_all", "Variant field order: var_004_non_objects kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_005_null_counts/specs/var_005_null_counts_filter_non_null", "Variant field order: var_005_null_counts kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_005_null_counts/specs/var_005_null_counts_read_all", "Variant field order: var_005_null_counts kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_006_different_types/specs/var_006_different_types_filter_by_id", "Variant field order: var_006_different_types kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_006_different_types/specs/var_006_different_types_read_all", "Variant field order: var_006_different_types kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_007_partitions/specs/var_007_partitions_filter_partition", "Variant field order: var_007_partitions kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_007_partitions/specs/var_007_partitions_read_all", "Variant field order: var_007_partitions kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_008_many_fields/specs/var_008_many_fields_read_all", "Variant field order: var_008_many_fields kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_009_unusual_chars/specs/var_009_unusual_chars_read_all", "Variant field order: var_009_unusual_chars kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_010_nested_fields/specs/var_010_nested_fields_read_all", "Variant field order: var_010_nested_fields kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_011_missing_values/specs/var_011_missing_values_read_all", "Variant field order: var_011_missing_values kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_012_mixed_types/specs/var_012_mixed_types_filter_half", "Variant field order: var_012_mixed_types kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_012_mixed_types/specs/var_012_mixed_types_read_all", "Variant field order: var_012_mixed_types kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_013_extreme_values/specs/var_013_extreme_values_read_all", "Variant field order: var_013_extreme_values kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_014_variant_in_struct/specs/var_014_variant_in_struct_filter_label", "Variant field order: var_014_variant_in_struct kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_014_variant_in_struct/specs/var_014_variant_in_struct_read_all", "Variant field order: var_014_variant_in_struct kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_015_string_skipping/specs/var_015_string_skipping_filter_middle", "Variant field order: var_015_string_skipping kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_015_string_skipping/specs/var_015_string_skipping_read_all", "Variant field order: var_015_string_skipping kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_016_array_variant/specs/var_016_array_variant_filter_array_size", "Variant field order: var_016_array_variant kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_016_array_variant/specs/var_016_array_variant_read_all", "Variant field order: var_016_array_variant kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_017_map_variant/specs/var_017_map_variant_filter_by_id", "Variant field order: var_017_map_variant kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_017_map_variant/specs/var_017_map_variant_read_all", "Variant field order: var_017_map_variant kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_018_column_mapping/specs/var_018_column_mapping_filter_by_id", "Variant field order: var_018_column_mapping kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_018_column_mapping/specs/var_018_column_mapping_read_all", "Variant field order: var_018_column_mapping kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_019_schema_evolution/specs/var_019_schema_evolution_filter_new_column", "Variant field order: var_019_schema_evolution kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_019_schema_evolution/specs/var_019_schema_evolution_read_all", "Variant field order: var_019_schema_evolution kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_019_schema_evolution/specs/var_019_schema_evolution_read_v2_before_evolution", "Variant field order: var_019_schema_evolution kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_020_time_travel/specs/var_020_time_travel_read_latest", "Variant field order: var_020_time_travel kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_020_time_travel/specs/var_020_time_travel_read_v1", "Variant field order: var_020_time_travel kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_020_time_travel/specs/var_020_time_travel_read_v2", "Variant field order: var_020_time_travel kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_021_optimized/specs/var_021_optimized_filter_after_optimize", "Variant field order: var_021_optimized kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_021_optimized/specs/var_021_optimized_read_all", "Variant field order: var_021_optimized kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_022_stat_fields/specs/var_022_stat_fields_filter_by_id", "Variant field order: var_022_stat_fields kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_022_stat_fields/specs/var_022_stat_fields_read_all", "Variant field order: var_022_stat_fields kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_all_json_types/specs/var_all_json_types_read_all", "Variant field order: var_all_json_types kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_cdf_read/specs/var_cdf_read_read_all", "Variant field order: var_cdf_read kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_deeply_nested/specs/var_deeply_nested_read_all", "Variant field order: var_deeply_nested kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_large_array/specs/var_large_array_read_all", "Variant field order: var_large_array kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_null_top_level/specs/var_null_top_level_filter_not_null", "Variant field order: var_null_top_level kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_null_top_level/specs/var_null_top_level_filter_null", "Variant field order: var_null_top_level kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_null_top_level/specs/var_null_top_level_read_all", "Variant field order: var_null_top_level kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_numeric_precision/specs/var_numeric_precision_read_all", "Variant field order: var_numeric_precision kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_predicate_non_variant/specs/var_predicate_non_variant_filter_category_A", "Variant field order: var_predicate_non_variant kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_predicate_non_variant/specs/var_predicate_non_variant_read_all", "Variant field order: var_predicate_non_variant kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_projection/specs/var_projection_project_id_data", "Variant field order: var_projection kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_projection/specs/var_projection_read_all", "Variant field order: var_projection kernel {metadata,value} vs Spark {value,metadata}"),
        ("var_unicode_escapes/specs/var_unicode_escapes_read_all", "Variant field order: var_unicode_escapes kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_hit_null_v_is_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_hit_null_v_struct_v_is_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_hit_v_is_not_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_hit_v_struct_v_not_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_miss_null_v_is_not_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_miss_null_v_struct_v_not_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_miss_v_is_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),
        ("ds_variant_null_stats/specs/ds_variant_null_stats_miss_v_struct_v_is_null", "Variant field order: ds_variant_null_stats kernel {metadata,value} vs Spark {value,metadata}"),

        // ── Kernel divergence: snapshot metadata ──
        ("tw_row_tracking_combo/specs/tw_row_tracking_combo_snapshot", "Type widening metadata divergence in snapshot"),

        // ── Kernel bug: inconsistent schema metadata in transform_to_logical ──
        // When transform_to_logical has Some(transform), batches go through apply_schema which
        // adds field metadata (e.g. delta.typeChanges). When transform is None, batches are
        // returned as-is without metadata. concat_batches fails because schemas don't match.
        // See: kernel/src/scan/state.rs:transform_to_logical
        ("tw_byte_to_int/specs/tw_byte_to_int_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_cdf_across_widening/specs/tw_cdf_across_widening_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_colmap_rename/specs/tw_colmap_rename_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_cross_physical_decimal/specs/tw_cross_physical_decimal_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_date_to_timestamp_ntz/specs/tw_date_to_timestamp_ntz_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_decimal_precision/specs/tw_decimal_precision_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_float_to_double/specs/tw_float_to_double_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_full_numeric_chain/specs/tw_full_numeric_chain_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_int_to_long/specs/tw_int_to_long_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_nested_field/specs/tw_nested_field_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_null_handling/specs/tw_null_handling_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_project_non_widened/specs/tw_project_non_widened_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_project_widened/specs/tw_project_widened_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_short_to_int/specs/tw_short_to_int_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_short_to_long/specs/tw_short_to_long_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("tw_stats_after_change/specs/tw_stats_after_change_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),
        ("cp_v2_with_type_widening/specs/cp_v2_with_type_widening_read", "Schema metadata: transform_to_logical inconsistently applies field metadata"),

        // ── Unsupported test types ──
        // These test types are not yet supported by the harness (Spec enum only has read/snapshot).
        ("_cdf", "CDF (Change Data Feed) test type not yet supported"),
        ("_txn", "Transaction test type not yet supported"),
        ("_domain_metadata", "Domain metadata test type not yet supported"),

        // ── Column mapping metadata on nested structs ──
        // Kernel includes delta.columnMapping.* metadata on nested struct fields,
        // but expected data has no such metadata. Data values are identical.
        ("cm_array_of_structs/specs/cm_array_of_structs_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_array_of_structs/specs/cm_array_of_structs_project_items", "Column mapping metadata on nested struct fields"),
        ("cm_deeply_nested/specs/cm_deeply_nested_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_deeply_nested/specs/cm_deeply_nested_project_data", "Column mapping metadata on nested struct fields"),
        ("cm_nested_columns/specs/cm_nested_columns_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_nested_struct_id/specs/cm_nested_struct_id_project_info", "Column mapping metadata on nested struct fields"),
        ("cm_nested_struct_id/specs/cm_nested_struct_id_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_nested_struct_name/specs/cm_nested_struct_name_project_struct_only", "Column mapping metadata on nested struct fields"),
        ("cm_nested_struct_name/specs/cm_nested_struct_name_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_nested_rename_3_levels/specs/cm_nested_rename_3_levels_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_projection_complex_types/specs/cm_projection_complex_types_read_all", "Column mapping metadata on nested struct fields"),
        ("cm_projection_complex_types/specs/cm_projection_complex_types_project_struct_only", "Column mapping metadata on nested struct fields"),
        ("ice_complex_types/specs/ice_complex_types_read_all", "Column mapping metadata on nested struct fields"),

        // ── Kernel divergence: negative version handling ──
        // Kernel succeeds when reading at version -1, Spark returns DELTA_TABLE_RESTORE_VERSION_INVALID
        ("dsReadVersionNegative/specs/dsReadVersionNegative_error", "Kernel allows negative version, Spark rejects"),
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
/// Unlike SKIP_LIST, these workloads run and we assert they fail with an error.
/// When a kernel fix lands, the test will pass and the entry should be removed.
const EXPECTED_KERNEL_FAILURES: &[(&str, &[&str])] = &[
    (
        "void/NullType not supported in schema deserialization",
        &[
            "void_001_void_top_level/",
            "void_002_void_nested_struct/",
            "void_005_void_schema_evolution/",
            "void_006_void_multiple_columns/",
            "void_007_void_with_backticks/",
            "void_in_struct/",
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
            "intv_006_create_insert_select/",
            "intv_boundary_values/",
            "intv_sub_second/",
        ],
    ),
    (
        "Null schemaString in metadata action",
        &[
            "pv_old_protocol_read/specs/pv_old_protocol_read_snapshot",
            "pv_empty_reader_features/specs/pv_empty_reader_features_snapshot",
            "pv_protocol_downgrade/specs/pv_protocol_downgrade_snapshot",
            "pv_reader_feature_not_in_writer/specs/pv_reader_feature_not_in_writer_snapshot",
            "pv_unknown_writer_feature_ok/specs/pv_unknown_writer_feature_ok_snapshot",
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
        "Schema deserialization fails for TimestampNTZ type",
        &["ds_multi_file_time/"],
    ),
    (
        "Requires contiguous commits; Spark uses CRC files to bridge gaps",
        &["prod_non_contiguous_versions/"],
    ),
    (
        "Column mapping id mode fails with None in final_fields_cols",
        &[
            "cm_id_matching_swapped/specs/cm_id_matching_swapped_select_",
            "cm_id_matching_nonexistent/specs/cm_id_matching_nonexistent_select_",
        ],
    ),
    (
        "Cannot resolve percent-encoded filenames in AddFile paths",
        &[
            "DV-005b/specs/DV-005b_count",
            "DV-008/specs/DV-008_table2_latest",
            "DV-009/specs/DV-009_table2_latest_v1",
        ],
    ),
    (
        "Inline DV has invalid magic number",
        &["dv_storage_type_i/specs/dv_storage_type_i_read_after_inline_dv"],
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
            "dv_checkpoint_only_read/specs/dv_checkpoint_only_read_snapshot",
        ],
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
            "corrupt_truncated_commit_json/specs/corrupt_truncated_commit_json_error",
            "cp_err_missing_protocol/specs/cp_err_missing_protocol_error",
            "ct_corrupt_parquet/specs/ct_corrupt_parquet_error",
            "ct_invalid_json/specs/ct_invalid_json_error",
            "dsReadCorruptCheckpoint/specs/dsReadCorruptCheckpoint_error",
            "dsReadCorruptJson/specs/dsReadCorruptJson_error",
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
        "Does not reject missing metadata/protocol actions",
        &[
            "ct_missing_metadata/specs/ct_missing_metadata_error",
            "ct_missing_protocol/specs/ct_missing_protocol_error",
            "log_err_missing_metadata/specs/log_err_missing_metadata_error",
            "log_err_missing_protocol/specs/log_err_missing_protocol_error",
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
            "err_schema_invalid_json/specs/err_schema_invalid_json_error",
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
];

/// Check if workload type is unsupported by the harness.
fn unsupported_workload_reason(spec: &Spec) -> Option<&'static str> {
    match spec {
        Spec::Read(read_spec) => {
            if read_spec.predicate.is_some() {
                return Some("Predicates not yet supported");
            }
            match &read_spec.time_travel {
                Some(TimeTravel::Timestamp { .. }) => {
                    Some("Timestamp-based time travel not supported")
                }
                _ => None,
            }
        }
        Spec::SnapshotConstruction(snapshot_spec) => match &snapshot_spec.time_travel {
            Some(TimeTravel::Timestamp { .. }) => Some("Timestamp-based time travel not supported"),
            _ => None,
        },
        Spec::Snapshot(snapshot_spec) => match &snapshot_spec.time_travel {
            Some(TimeTravel::Timestamp { .. }) => Some("Timestamp-based time travel not supported"),
            _ => None,
        },
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

    // Check expected kernel failures FIRST (path matching only — these need to
    // actually run to assert kernel still fails). Skip list checked second.
    let expected_failure = EXPECTED_KERNEL_FAILURES
        .iter()
        .find(|(_, patterns)| patterns.iter().any(|p| spec_path_str.contains(p)));

    if expected_failure.is_none() && should_skip_test(&spec_path_str).is_some() {
        return Ok(());
    }

    // Load spec and test case once
    let content = std::fs::read_to_string(&spec_path_abs).expect("Failed to read spec file");
    let spec: Spec = serde_json::from_str(&content).expect("Failed to parse spec file");
    let (test_case, workload_name) =
        test_case_from_spec_path(&spec_path_abs).expect("Failed to load test case");
    let expected_dir = test_case.expected_dir(&workload_name);

    let table_root = test_case.table_root().expect("Failed to get table URL");
    let engine = test_utils::create_default_engine(&table_root).expect("Failed to create engine");

    // Skip unsupported workload types
    if unsupported_workload_reason(&spec).is_some() {
        return Ok(());
    }

    // Expected kernel failures: assert kernel DOES fail
    if let Some((reason, _)) = expected_failure {
        match execute_and_validate_workload(engine, &table_root, &spec, &expected_dir) {
            Err(e) => println!("  Expected kernel failure ({reason}): {e}"),
            Ok(_) => panic!(
                "Workload '{workload_name}' was expected to fail but succeeded! \
                 Reason: {reason}. Remove from EXPECTED_KERNEL_FAILURES!"
            ),
        }
        return Ok(());
    }

    println!("Running workload: {}", workload_name);

    execute_and_validate_workload(engine, &table_root, &spec, &expected_dir)
        .unwrap_or_else(|e| panic!("Workload '{}' failed: {}", workload_name, e));

    println!("  Passed");
    Ok(())
}

datatest_stable::harness! {
    {
        test = acceptance_workloads_test,
        root = "workloads/",
        pattern = r"specs/.*\.json$"
    },
}
