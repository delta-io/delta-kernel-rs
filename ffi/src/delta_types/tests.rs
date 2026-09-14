use std::collections::HashMap;

use test_utils::assert_result_error_with_message;

use super::*;
use crate::{FfiFileStats, KernelI64Slice, KernelStringSlice, OptionalValue};

fn slice(value: &'static str) -> KernelStringSlice {
    unsafe { KernelStringSlice::new_unsafe(value) }
}

fn invalid_utf8() -> KernelStringSlice {
    static INVALID_UTF8: [u8; 1] = [0xff];
    KernelStringSlice {
        ptr: INVALID_UTF8.as_ptr().cast(),
        len: INVALID_UTF8.len(),
    }
}

fn none_string() -> OptionalValue<KernelStringSlice> {
    OptionalValue::None
}

fn none_i64() -> OptionalValue<i64> {
    OptionalValue::None
}

fn last_checkpoint_with_parts(parts: u64) -> FfiLastCheckpoint {
    FfiLastCheckpoint {
        version: 0,
        size: 0,
        parts: OptionalValue::Some(parts),
        size_in_bytes: OptionalValue::None,
        num_of_add_files: OptionalValue::None,
        checkpoint_schema: OptionalValue::None,
        checksum: OptionalValue::None,
        tags: OptionalValue::None,
        v2_checkpoint: std::ptr::null(),
    }
}

fn empty_strings(present: bool) -> OptionalValue<FfiStringArray> {
    if present {
        OptionalValue::Some(FfiStringArray {
            ptr: std::ptr::null(),
            len: 0,
        })
    } else {
        OptionalValue::None
    }
}

fn empty_map() -> FfiStringMap {
    FfiStringMap {
        ptr: std::ptr::null(),
        len: 0,
    }
}

fn none_map() -> OptionalValue<FfiStringMap> {
    OptionalValue::None
}

fn test_protocol() -> FfiProtocol {
    FfiProtocol {
        min_reader_version: 1,
        min_writer_version: 2,
        reader_features: empty_strings(false),
        writer_features: empty_strings(false),
    }
}

fn test_metadata() -> FfiMetadata {
    FfiMetadata {
        id: slice("table-id"),
        name: none_string(),
        description: none_string(),
        format_provider: slice("parquet"),
        format_options: empty_map(),
        schema_string: slice(r#"{"type":"struct","fields":[]}"#),
        partition_columns: FfiStringArray {
            ptr: std::ptr::null(),
            len: 0,
        },
        created_time: none_i64(),
        configuration: empty_map(),
    }
}

fn empty_crc() -> FfiCrc {
    FfiCrc {
        version: 0,
        metadata: test_metadata(),
        protocol: test_protocol(),
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Complete,
            file_stats: FfiFileStats {
                num_files: 0,
                table_size_bytes: 0,
            },
            file_size_histogram: std::ptr::null(),
        },
        in_commit_timestamp: none_i64(),
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Partial,
            transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Partial,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        },
        txn_id: none_string(),
        all_files: OptionalValue::None,
        num_deleted_records: none_i64(),
        num_deletion_vectors: none_i64(),
        deleted_record_counts_histogram: std::ptr::null(),
    }
}

#[test]
fn typed_components_construct_rich_snapshot_state() {
    let transactions = [
        FfiSetTransaction {
            app_id: slice("app-a"),
            version: 7,
            last_updated: OptionalValue::Some(123),
        },
        FfiSetTransaction {
            app_id: slice("app-b"),
            version: 9,
            last_updated: OptionalValue::Some(456),
        },
    ];
    let domains = [
        FfiDomainMetadata {
            domain: slice("example.domain-a"),
            configuration: slice("payload-a"),
            removed: false,
        },
        FfiDomainMetadata {
            domain: slice("example.domain-b"),
            configuration: slice("payload-b"),
            removed: false,
        },
    ];
    let boundaries = [0, 1024];
    let counts = [1, 0];
    let bytes = [512, 0];
    let histogram = FfiFileSizeHistogram {
        sorted_bin_boundaries: KernelI64Slice {
            ptr: boundaries.as_ptr(),
            len: boundaries.len(),
        },
        file_counts: KernelI64Slice {
            ptr: counts.as_ptr(),
            len: counts.len(),
        },
        total_bytes: KernelI64Slice {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        },
    };
    let crc_value = FfiCrc {
        version: 5,
        metadata: test_metadata(),
        protocol: test_protocol(),
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Complete,
            file_stats: FfiFileStats {
                table_size_bytes: 512,
                num_files: 1,
            },
            file_size_histogram: &histogram,
        },
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Complete,
            transactions: FfiSetTransactionArray {
                ptr: transactions.as_ptr(),
                len: transactions.len(),
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Complete,
            domain_metadata: FfiDomainMetadataArray {
                ptr: domains.as_ptr(),
                len: domains.len(),
            },
        },
        ..empty_crc()
    };
    let complete_crc = unsafe { crc_value.try_to_kernel() }.unwrap();
    let file_stats = complete_crc.file_stats().unwrap();
    assert_eq!(file_stats.num_files(), 1);
    let histogram = file_stats.file_size_histogram().unwrap();
    assert_eq!(histogram.sorted_bin_boundaries(), &[0, 1024]);
    assert_eq!(histogram.file_counts(), &[1, 0]);
    assert_eq!(histogram.total_bytes(), &[512, 0]);
    let transaction_map = complete_crc.set_transaction_state.expect_complete();
    assert_eq!(transaction_map.len(), 2);
    for (app_id, version) in [("app-a", 7), ("app-b", 9)] {
        let transaction = serde_json::to_value(&transaction_map[app_id]).unwrap();
        assert_eq!(transaction["appId"], app_id);
        assert_eq!(transaction["version"], version);
    }
    let domain_map = complete_crc.domain_metadata_state.expect_complete();
    assert_eq!(domain_map.len(), 2);
    assert_eq!(domain_map["example.domain-a"].configuration(), "payload-a");
    assert_eq!(domain_map["example.domain-b"].configuration(), "payload-b");
    let partial_crc_value = FfiCrc {
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Partial,
            transactions: FfiSetTransactionArray {
                ptr: transactions.as_ptr(),
                len: transactions.len(),
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Partial,
            domain_metadata: FfiDomainMetadataArray {
                ptr: domains.as_ptr(),
                len: domains.len(),
            },
        },
        ..empty_crc()
    };
    let partial_crc = unsafe { partial_crc_value.try_to_kernel() }.unwrap();
    assert!(matches!(
        partial_crc.set_transaction_state,
        SetTransactionState::Partial(ref values)
            if values.len() == 2
                && values.contains_key("app-a")
                && values.contains_key("app-b")
    ));
    assert!(matches!(
        partial_crc.domain_metadata_state,
        DomainMetadataState::Partial(ref values)
            if values.len() == 2
                && values.contains_key("example.domain-a")
                && values.contains_key("example.domain-b")
    ));

    let checkpoint_metadata = FfiCheckpointMetadata {
        version: 5,
        tags: none_map(),
    };
    let non_file_action = FfiCheckpointNonFileAction::CheckpointMetadata(&checkpoint_metadata);
    let sidecar_tags = [FfiStringMapEntry {
        key: slice("source"),
        value: slice("ffi"),
    }];
    let sidecar = FfiSidecar {
        path: slice("sidecar.parquet"),
        size_in_bytes: 42,
        modification_time: 123,
        tags: OptionalValue::Some(FfiStringMap {
            ptr: sidecar_tags.as_ptr(),
            len: sidecar_tags.len(),
        }),
    };
    let v2 = FfiLastCheckpointV2 {
        path: slice("00000000000000000005.checkpoint.uuid.parquet"),
        size_in_bytes: none_i64(),
        modification_time: none_i64(),
        sidecar_files: OptionalValue::Some(FfiSidecarArray {
            ptr: &sidecar,
            len: 1,
        }),
        non_file_actions: OptionalValue::Some(FfiCheckpointNonFileActionArray {
            ptr: &non_file_action,
            len: 1,
        }),
    };
    let checkpoint_tags = [FfiStringMapEntry {
        key: slice("source"),
        value: slice("ffi"),
    }];
    let checkpoint = FfiLastCheckpoint {
        version: 5,
        size: 1,
        parts: OptionalValue::None,
        size_in_bytes: OptionalValue::Some(123),
        num_of_add_files: OptionalValue::Some(7),
        checkpoint_schema: OptionalValue::Some(slice(r#"{"type":"struct","fields":[]}"#)),
        checksum: OptionalValue::Some(slice("sha256")),
        tags: OptionalValue::Some(FfiStringMap {
            ptr: checkpoint_tags.as_ptr(),
            len: checkpoint_tags.len(),
        }),
        v2_checkpoint: &v2,
    };
    let checkpoint = unsafe { checkpoint.try_to_kernel() }.unwrap();
    assert_eq!(checkpoint.version, 5);
    let checkpoint_json = serde_json::to_value(checkpoint).unwrap();
    assert_eq!(checkpoint_json["sizeInBytes"], 123);
    assert_eq!(checkpoint_json["numOfAddFiles"], 7);
    assert_eq!(checkpoint_json["checksum"], "sha256");
    assert_eq!(checkpoint_json["tags"]["source"], "ffi");
    assert!(checkpoint_json["checkpointSchema"].is_object());
}

#[rstest::rstest]
#[case::persisted_relative(
    FfiDeletionVectorStorageType::PersistedRelative,
    DeletionVectorStorageType::PersistedRelative
)]
#[case::inline(
    FfiDeletionVectorStorageType::Inline,
    DeletionVectorStorageType::Inline
)]
#[case::persisted_absolute(
    FfiDeletionVectorStorageType::PersistedAbsolute,
    DeletionVectorStorageType::PersistedAbsolute
)]
fn deletion_vector_storage_type_maps_all_variants(
    #[case] ffi_type: FfiDeletionVectorStorageType,
    #[case] kernel_type: DeletionVectorStorageType,
) {
    assert_eq!(DeletionVectorStorageType::from(ffi_type), kernel_type);
}

#[test]
fn typed_crc_accepts_full_kernel_state() {
    let tag_entries = [
        FfiNullableStringMapEntry {
            key: slice("present"),
            value: OptionalValue::Some(slice("value")),
        },
        FfiNullableStringMapEntry {
            key: slice("absent"),
            value: OptionalValue::None,
        },
    ];
    let deletion_vector = FfiDeletionVectorDescriptor {
        storage_type: FfiDeletionVectorStorageType::PersistedAbsolute,
        path_or_inline_dv: slice("file:///deletion-vector.bin"),
        offset: OptionalValue::Some(7),
        size_in_bytes: 11,
        cardinality: 13,
    };
    let add = FfiAdd {
        path: slice("part-00000.parquet"),
        partition_values: empty_map(),
        size: 17,
        modification_time: 19,
        data_change: true,
        stats: OptionalValue::Some(slice(r#"{"numRecords":23}"#)),
        tags: OptionalValue::Some(FfiNullableStringMap {
            ptr: tag_entries.as_ptr(),
            len: tag_entries.len(),
        }),
        deletion_vector: &deletion_vector,
        base_row_id: OptionalValue::Some(29),
        default_row_commit_version: OptionalValue::Some(31),
        clustering_provider: OptionalValue::Some(slice("liquid")),
    };
    let deleted_record_counts = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let deleted_record_counts_histogram = FfiDeletedRecordCountsHistogram {
        deleted_record_counts: KernelI64Slice {
            ptr: deleted_record_counts.as_ptr(),
            len: deleted_record_counts.len(),
        },
    };
    let value = FfiCrc {
        version: 5,
        in_commit_timestamp: OptionalValue::Some(37),
        txn_id: OptionalValue::Some(slice("txn-id")),
        all_files: OptionalValue::Some(FfiAddArray { ptr: &add, len: 1 }),
        num_deleted_records: OptionalValue::Some(41),
        num_deletion_vectors: OptionalValue::Some(43),
        deleted_record_counts_histogram: &deleted_record_counts_histogram,
        ..empty_crc()
    };

    let actual = unsafe { value.try_to_kernel() }.unwrap();
    let expected_add = Add::from_parts(
        "part-00000.parquet".to_string(),
        HashMap::new(),
        17,
        19,
        true,
        Some(r#"{"numRecords":23}"#.to_string()),
        Some(HashMap::from([
            ("present".to_string(), Some("value".to_string())),
            ("absent".to_string(), None),
        ])),
        Some(
            DeletionVectorDescriptor::try_new(
                DeletionVectorStorageType::PersistedAbsolute,
                "file:///deletion-vector.bin",
                Some(7),
                11,
                13,
            )
            .unwrap(),
        ),
        Some(29),
        Some(31),
        Some("liquid".to_string()),
    );
    let expected = Crc::from_parts(
        5,
        unsafe { test_metadata().try_to_kernel() }.unwrap(),
        unsafe { test_protocol().try_to_kernel() }.unwrap(),
        FileStatsState::Complete(FileStats::try_new(0, 0, None).unwrap()),
        Some(37),
        SetTransactionState::Partial(HashMap::new()),
        DomainMetadataState::Partial(HashMap::new()),
        Some("txn-id".to_string()),
        Some(vec![expected_add]),
        Some(41),
        Some(43),
        Some(DeletedRecordCountsHistogram::from_parts(
            deleted_record_counts.to_vec(),
        )),
    );
    assert_eq!(actual, expected);

    let indeterminate = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Indeterminate,
            file_stats: FfiFileStats {
                num_files: -1,
                table_size_bytes: -1,
            },
            file_size_histogram: std::ptr::null(),
        },
        ..empty_crc()
    };
    assert!(unsafe { indeterminate.try_to_kernel() }
        .unwrap()
        .file_stats()
        .is_none());
}

#[test]
fn typed_arrays_preserve_absent_and_present_empty() {
    assert_eq!(
        Option::<&FfiStringArray>::from(&empty_strings(false))
            .map(|value| unsafe { value.try_to_strings() })
            .transpose()
            .unwrap(),
        None
    );
    assert_eq!(
        Option::<&FfiStringArray>::from(&empty_strings(true))
            .map(|value| unsafe { value.try_to_strings() })
            .transpose()
            .unwrap(),
        Some(vec![])
    );

    let v2 = FfiLastCheckpointV2 {
        path: slice("checkpoint.parquet"),
        size_in_bytes: OptionalValue::None,
        modification_time: OptionalValue::None,
        sidecar_files: OptionalValue::Some(FfiSidecarArray {
            ptr: std::ptr::null(),
            len: 0,
        }),
        non_file_actions: OptionalValue::Some(FfiCheckpointNonFileActionArray {
            ptr: std::ptr::null(),
            len: 0,
        }),
    };
    let parsed_v2 = unsafe { v2.try_to_kernel() }.unwrap();
    let parsed_v2_json = serde_json::to_value(parsed_v2).unwrap();
    assert_eq!(parsed_v2_json["sidecarFiles"], serde_json::json!([]));
    assert_eq!(parsed_v2_json["nonFileActions"], serde_json::json!([]));

    let crc_value = FfiCrc {
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Complete,
            transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Complete,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        },
        ..empty_crc()
    };
    let parsed_crc = unsafe { crc_value.try_to_kernel() }.unwrap();
    assert!(parsed_crc
        .set_transaction_state
        .expect_complete()
        .is_empty());
    assert!(parsed_crc
        .domain_metadata_state
        .expect_complete()
        .is_empty());
}

#[test]
fn checkpoint_part_count_has_a_target_independent_bound() {
    let max = last_checkpoint_with_parts(u64::from(u32::MAX));
    assert!(unsafe { max.try_to_kernel() }.is_ok());
    let too_large = last_checkpoint_with_parts(u64::from(u32::MAX) + 1);
    assert_result_error_with_message(
        unsafe { too_large.try_to_kernel() },
        "checkpoint part count exceeds u32",
    );
}

#[test]
fn typed_strings_accept_null_empty_and_reject_null_nonempty() {
    let null_empty = KernelStringSlice {
        ptr: std::ptr::null(),
        len: 0,
    };
    assert_eq!(unsafe { null_empty.try_to_string() }.unwrap(), "");

    let null_nonempty = KernelStringSlice {
        ptr: std::ptr::null(),
        len: 1,
    };
    assert_result_error_with_message(
        unsafe { null_nonempty.try_to_string() },
        "string pointer is null with length 1",
    );
}

#[test]
fn typed_sidecar_rejects_negative_size() {
    let sidecar_value = FfiSidecar {
        path: slice("sidecar.parquet"),
        size_in_bytes: -1,
        modification_time: 123,
        tags: none_map(),
    };
    assert_result_error_with_message(
        unsafe { sidecar_value.try_to_kernel() },
        "sidecar size must be non-negative: -1",
    );
}

#[test]
fn typed_metadata_preserves_non_empty_optional_and_container_fields() {
    let partition = [slice("p")];
    let format_options = [FfiStringMapEntry {
        key: slice("compression"),
        value: slice("zstd"),
    }];
    let configuration = [FfiStringMapEntry {
        key: slice("key"),
        value: slice("value"),
    }];
    let value = FfiMetadata {
        id: slice("table-id"),
        name: OptionalValue::Some(slice("table-name")),
        description: OptionalValue::Some(slice("description")),
        format_provider: slice("parquet"),
        format_options: FfiStringMap {
            ptr: format_options.as_ptr(),
            len: format_options.len(),
        },
        schema_string: slice(
            r#"{"type":"struct","fields":[{"name":"p","type":"string","nullable":true,"metadata":{}}]}"#,
        ),
        partition_columns: FfiStringArray {
            ptr: partition.as_ptr(),
            len: partition.len(),
        },
        created_time: OptionalValue::Some(123),
        configuration: FfiStringMap {
            ptr: configuration.as_ptr(),
            len: configuration.len(),
        },
    };

    let actual = unsafe { value.try_to_kernel() }.unwrap();
    assert_eq!(actual.name(), Some("table-name"));
    assert_eq!(actual.description(), Some("description"));
    assert_eq!(actual.created_time(), Some(123));
    assert_eq!(actual.partition_columns(), &["p"]);
    assert_eq!(
        actual.configuration().get("key").map(String::as_str),
        Some("value")
    );
}

#[rstest::rstest]
#[case::live(false)]
#[case::removed(true)]
fn typed_domain_metadata_preserves_removed_flag(#[case] removed: bool) {
    let value = FfiDomainMetadata {
        domain: slice("example.domain"),
        configuration: slice(r#"{"key":"value"}"#),
        removed,
    };

    let actual = unsafe { value.try_to_kernel() }.unwrap();
    assert_eq!(actual.domain(), "example.domain");
    assert_eq!(actual.configuration(), r#"{"key":"value"}"#);
    assert_eq!(actual.is_removed(), removed);
}

#[test]
fn typed_metadata_rejects_duplicate_map_keys() {
    let configuration = [
        FfiStringMapEntry {
            key: slice("key"),
            value: slice("first"),
        },
        FfiStringMapEntry {
            key: slice("key"),
            value: slice("second"),
        },
    ];
    let value = FfiMetadata {
        configuration: FfiStringMap {
            ptr: configuration.as_ptr(),
            len: configuration.len(),
        },
        ..test_metadata()
    };

    let error = unsafe { value.try_to_kernel() }.unwrap_err();
    assert!(error.to_string().contains("duplicate map key: key"));
}

#[test]
fn typed_maps_reject_invalid_nested_state() {
    let invalid_value = [FfiStringMapEntry {
        key: slice("key"),
        value: invalid_utf8(),
    }];
    let map = FfiStringMap {
        ptr: invalid_value.as_ptr(),
        len: invalid_value.len(),
    };
    assert!(unsafe { map.try_to_hash_map() }.is_err());

    let null_map = FfiNullableStringMap {
        ptr: std::ptr::null(),
        len: 1,
    };
    assert!(unsafe { null_map.try_to_hash_map() }.is_err());

    let invalid_key = [FfiNullableStringMapEntry {
        key: invalid_utf8(),
        value: OptionalValue::None,
    }];
    let map = FfiNullableStringMap {
        ptr: invalid_key.as_ptr(),
        len: invalid_key.len(),
    };
    assert!(unsafe { map.try_to_hash_map() }.is_err());

    let invalid_value = [FfiNullableStringMapEntry {
        key: slice("key"),
        value: OptionalValue::Some(invalid_utf8()),
    }];
    let map = FfiNullableStringMap {
        ptr: invalid_value.as_ptr(),
        len: invalid_value.len(),
    };
    assert!(unsafe { map.try_to_hash_map() }.is_err());

    let duplicate_keys = [
        FfiNullableStringMapEntry {
            key: slice("key"),
            value: OptionalValue::None,
        },
        FfiNullableStringMapEntry {
            key: slice("key"),
            value: OptionalValue::Some(slice("value")),
        },
    ];
    let map = FfiNullableStringMap {
        ptr: duplicate_keys.as_ptr(),
        len: duplicate_keys.len(),
    };
    assert_result_error_with_message(unsafe { map.try_to_hash_map() }, "duplicate map key: key");
}

#[test]
fn typed_array_rejects_null_nonempty_pointer() {
    let invalid_array = FfiStringArray {
        ptr: std::ptr::null(),
        len: 1,
    };
    assert!(unsafe { invalid_array.try_to_strings() }.is_err());
}

#[test]
fn typed_components_reject_invalid_strings() {
    assert!(unsafe { invalid_utf8().try_to_string() }.is_err());

    let invalid_feature = [invalid_utf8()];
    let invalid_reader_features = FfiProtocol {
        reader_features: OptionalValue::Some(FfiStringArray {
            ptr: invalid_feature.as_ptr(),
            len: invalid_feature.len(),
        }),
        ..test_protocol()
    };
    assert!(unsafe { invalid_reader_features.try_to_kernel() }.is_err());

    let invalid_feature = [invalid_utf8()];
    let invalid_writer_features = FfiProtocol {
        writer_features: OptionalValue::Some(FfiStringArray {
            ptr: invalid_feature.as_ptr(),
            len: invalid_feature.len(),
        }),
        ..test_protocol()
    };
    assert!(unsafe { invalid_writer_features.try_to_kernel() }.is_err());

    let invalid_name = FfiMetadata {
        name: OptionalValue::Some(invalid_utf8()),
        ..test_metadata()
    };
    assert!(unsafe { invalid_name.try_to_kernel() }.is_err());

    let invalid_description = FfiMetadata {
        description: OptionalValue::Some(invalid_utf8()),
        ..test_metadata()
    };
    assert!(unsafe { invalid_description.try_to_kernel() }.is_err());

    let invalid_format_provider = FfiMetadata {
        format_provider: invalid_utf8(),
        ..test_metadata()
    };
    assert!(unsafe { invalid_format_provider.try_to_kernel() }.is_err());

    let invalid_entry = [FfiStringMapEntry {
        key: slice("key"),
        value: invalid_utf8(),
    }];
    let invalid_format_options = FfiMetadata {
        format_options: FfiStringMap {
            ptr: invalid_entry.as_ptr(),
            len: invalid_entry.len(),
        },
        ..test_metadata()
    };
    assert!(unsafe { invalid_format_options.try_to_kernel() }.is_err());

    let invalid_schema = FfiMetadata {
        schema_string: invalid_utf8(),
        ..test_metadata()
    };
    assert!(unsafe { invalid_schema.try_to_kernel() }.is_err());

    let invalid_partition = [invalid_utf8()];
    let invalid_partition_columns = FfiMetadata {
        partition_columns: FfiStringArray {
            ptr: invalid_partition.as_ptr(),
            len: invalid_partition.len(),
        },
        ..test_metadata()
    };
    assert!(unsafe { invalid_partition_columns.try_to_kernel() }.is_err());

    let invalid_entry = [FfiStringMapEntry {
        key: invalid_utf8(),
        value: slice("value"),
    }];
    let invalid_configuration = FfiMetadata {
        configuration: FfiStringMap {
            ptr: invalid_entry.as_ptr(),
            len: invalid_entry.len(),
        },
        ..test_metadata()
    };
    assert!(unsafe { invalid_configuration.try_to_kernel() }.is_err());

    let transaction = FfiSetTransaction {
        app_id: invalid_utf8(),
        version: 1,
        last_updated: none_i64(),
    };
    assert!(unsafe { transaction.try_to_kernel() }.is_err());

    let domain = FfiDomainMetadata {
        domain: invalid_utf8(),
        configuration: slice("{}"),
        removed: false,
    };
    assert!(unsafe { domain.try_to_kernel() }.is_err());

    let domain = FfiDomainMetadata {
        domain: slice("domain"),
        configuration: invalid_utf8(),
        removed: false,
    };
    assert!(unsafe { domain.try_to_kernel() }.is_err());

    let invalid_sidecar = FfiSidecar {
        path: invalid_utf8(),
        size_in_bytes: 1,
        modification_time: 1,
        tags: none_map(),
    };
    assert!(unsafe { invalid_sidecar.try_to_kernel() }.is_err());

    let invalid_sidecar_tags = FfiSidecar {
        path: slice("sidecar.parquet"),
        size_in_bytes: 1,
        modification_time: 1,
        tags: OptionalValue::Some(FfiStringMap {
            ptr: std::ptr::null(),
            len: 1,
        }),
    };
    assert!(unsafe { invalid_sidecar_tags.try_to_kernel() }.is_err());
}

#[test]
fn typed_nested_arrays_reject_null_nonempty_pointers() {
    let values = [0, 1];
    let valid = KernelI64Slice {
        ptr: values.as_ptr(),
        len: values.len(),
    };
    let null = KernelI64Slice {
        ptr: std::ptr::null(),
        len: 1,
    };
    let invalid_boundaries = FfiFileSizeHistogram {
        sorted_bin_boundaries: null,
        file_counts: KernelI64Slice {
            ptr: values.as_ptr(),
            len: values.len(),
        },
        total_bytes: KernelI64Slice {
            ptr: values.as_ptr(),
            len: values.len(),
        },
    };
    assert!(unsafe { invalid_boundaries.try_to_kernel() }.is_err());

    let invalid_counts = FfiFileSizeHistogram {
        sorted_bin_boundaries: valid,
        file_counts: KernelI64Slice {
            ptr: std::ptr::null(),
            len: 1,
        },
        total_bytes: KernelI64Slice {
            ptr: values.as_ptr(),
            len: values.len(),
        },
    };
    assert!(unsafe { invalid_counts.try_to_kernel() }.is_err());

    let invalid_bytes = FfiFileSizeHistogram {
        sorted_bin_boundaries: KernelI64Slice {
            ptr: values.as_ptr(),
            len: values.len(),
        },
        file_counts: KernelI64Slice {
            ptr: values.as_ptr(),
            len: values.len(),
        },
        total_bytes: KernelI64Slice {
            ptr: std::ptr::null(),
            len: 1,
        },
    };
    assert!(unsafe { invalid_bytes.try_to_kernel() }.is_err());

    let checkpoint = FfiLastCheckpointV2 {
        path: slice("checkpoint.parquet"),
        size_in_bytes: none_i64(),
        modification_time: none_i64(),
        sidecar_files: OptionalValue::Some(FfiSidecarArray {
            ptr: std::ptr::null(),
            len: 1,
        }),
        non_file_actions: OptionalValue::None,
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let checkpoint = FfiLastCheckpointV2 {
        path: slice("checkpoint.parquet"),
        size_in_bytes: none_i64(),
        modification_time: none_i64(),
        sidecar_files: OptionalValue::None,
        non_file_actions: OptionalValue::Some(FfiCheckpointNonFileActionArray {
            ptr: std::ptr::null(),
            len: 1,
        }),
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Complete,
            transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 1,
            },
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Complete,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 1,
            },
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let transaction = FfiSetTransaction {
        app_id: invalid_utf8(),
        version: 1,
        last_updated: OptionalValue::None,
    };
    let transaction_state = FfiSetTransactionState {
        kind: FfiSetTransactionStateKind::Complete,
        transactions: FfiSetTransactionArray {
            ptr: &transaction,
            len: 1,
        },
    };
    assert!(unsafe { transaction_state.try_to_kernel() }.is_err());

    let domain_metadata = FfiDomainMetadata {
        domain: invalid_utf8(),
        configuration: slice("{}"),
        removed: false,
    };
    let domain_metadata_state = FfiDomainMetadataState {
        kind: FfiDomainMetadataStateKind::Complete,
        domain_metadata: FfiDomainMetadataArray {
            ptr: &domain_metadata,
            len: 1,
        },
    };
    assert!(unsafe { domain_metadata_state.try_to_kernel() }.is_err());

    let deleted_record_counts = FfiDeletedRecordCountsHistogram {
        deleted_record_counts: KernelI64Slice {
            ptr: std::ptr::null(),
            len: 1,
        },
    };
    assert!(unsafe { deleted_record_counts.try_to_kernel() }.is_err());
}

#[test]
fn typed_actions_validate_tags_and_convert_each_payload() {
    let metadata = test_metadata();
    let protocol = test_protocol();
    let transaction = FfiSetTransaction {
        app_id: slice("app"),
        version: 1,
        last_updated: none_i64(),
    };
    let domain_metadata = FfiDomainMetadata {
        domain: slice("domain"),
        configuration: slice("{}"),
        removed: false,
    };
    let checkpoint_metadata = FfiCheckpointMetadata {
        version: 1,
        tags: none_map(),
    };
    let actions = [
        FfiCheckpointNonFileAction::Metadata(&metadata),
        FfiCheckpointNonFileAction::Protocol(&protocol),
        FfiCheckpointNonFileAction::Transaction(&transaction),
        FfiCheckpointNonFileAction::DomainMetadata(&domain_metadata),
        FfiCheckpointNonFileAction::CheckpointMetadata(&checkpoint_metadata),
    ];
    for value in &actions {
        unsafe { value.try_to_kernel() }.unwrap();
    }

    let null_actions = [
        FfiCheckpointNonFileAction::Metadata(std::ptr::null()),
        FfiCheckpointNonFileAction::Protocol(std::ptr::null()),
        FfiCheckpointNonFileAction::Transaction(std::ptr::null()),
        FfiCheckpointNonFileAction::DomainMetadata(std::ptr::null()),
        FfiCheckpointNonFileAction::CheckpointMetadata(std::ptr::null()),
    ];
    for action in &null_actions {
        assert!(matches!(
            unsafe { action.try_to_kernel() },
            Err(Error::Generic(_))
        ));
    }
}

#[test]
fn typed_actions_reject_invalid_payload_contents() {
    let metadata = FfiMetadata {
        id: invalid_utf8(),
        ..test_metadata()
    };
    let protocol = FfiProtocol {
        min_reader_version: 0,
        ..test_protocol()
    };
    let transaction = FfiSetTransaction {
        app_id: invalid_utf8(),
        version: 1,
        last_updated: none_i64(),
    };
    let domain_metadata = FfiDomainMetadata {
        domain: invalid_utf8(),
        configuration: slice("{}"),
        removed: false,
    };
    let tags = FfiStringMap {
        ptr: std::ptr::null(),
        len: 1,
    };
    let checkpoint_metadata = FfiCheckpointMetadata {
        version: 1,
        tags: OptionalValue::Some(tags),
    };
    let actions = [
        FfiCheckpointNonFileAction::Metadata(&metadata),
        FfiCheckpointNonFileAction::Protocol(&protocol),
        FfiCheckpointNonFileAction::Transaction(&transaction),
        FfiCheckpointNonFileAction::DomainMetadata(&domain_metadata),
        FfiCheckpointNonFileAction::CheckpointMetadata(&checkpoint_metadata),
    ];

    for action in &actions {
        assert!(unsafe { action.try_to_kernel() }.is_err());
    }
}

#[test]
fn typed_add_rejects_invalid_nested_state() {
    let valid_add = || FfiAdd {
        path: slice("part-00000.parquet"),
        partition_values: empty_map(),
        size: 1,
        modification_time: 1,
        data_change: true,
        stats: OptionalValue::None,
        tags: OptionalValue::None,
        deletion_vector: std::ptr::null(),
        base_row_id: OptionalValue::None,
        default_row_commit_version: OptionalValue::None,
        clustering_provider: OptionalValue::None,
    };

    let invalid_deletion_vector = FfiDeletionVectorDescriptor {
        storage_type: FfiDeletionVectorStorageType::Inline,
        path_or_inline_dv: invalid_utf8(),
        offset: OptionalValue::None,
        size_in_bytes: 1,
        cardinality: 1,
    };
    let add = FfiAdd {
        deletion_vector: &invalid_deletion_vector,
        ..valid_add()
    };
    assert!(unsafe { add.try_to_kernel() }.is_err());

    let add = FfiAdd {
        stats: OptionalValue::Some(invalid_utf8()),
        ..valid_add()
    };
    assert!(unsafe { add.try_to_kernel() }.is_err());

    let add = FfiAdd {
        tags: OptionalValue::Some(FfiNullableStringMap {
            ptr: std::ptr::null(),
            len: 1,
        }),
        ..valid_add()
    };
    assert!(unsafe { add.try_to_kernel() }.is_err());

    let add = FfiAdd {
        clustering_provider: OptionalValue::Some(invalid_utf8()),
        ..valid_add()
    };
    assert!(unsafe { add.try_to_kernel() }.is_err());

    let add = FfiAdd {
        path: invalid_utf8(),
        ..valid_add()
    };
    assert!(unsafe { add.try_to_kernel() }.is_err());

    let add = FfiAdd {
        partition_values: FfiStringMap {
            ptr: std::ptr::null(),
            len: 1,
        },
        ..valid_add()
    };
    assert!(unsafe { add.try_to_kernel() }.is_err());
}

#[test]
fn typed_checkpoint_and_crc_reject_invalid_nested_state() {
    let invalid_v2 = FfiLastCheckpointV2 {
        path: invalid_utf8(),
        size_in_bytes: none_i64(),
        modification_time: none_i64(),
        sidecar_files: OptionalValue::None,
        non_file_actions: OptionalValue::None,
    };
    assert!(unsafe { invalid_v2.try_to_kernel() }.is_err());

    let checkpoint = FfiLastCheckpoint {
        version: 0,
        size: 1,
        parts: OptionalValue::None,
        size_in_bytes: none_i64(),
        num_of_add_files: none_i64(),
        checkpoint_schema: OptionalValue::Some(slice("not a schema")),
        checksum: none_string(),
        tags: none_map(),
        v2_checkpoint: std::ptr::null(),
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let checkpoint = FfiLastCheckpoint {
        checkpoint_schema: none_string(),
        checksum: OptionalValue::Some(invalid_utf8()),
        ..checkpoint
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let checkpoint = FfiLastCheckpoint {
        checkpoint_schema: OptionalValue::Some(invalid_utf8()),
        ..last_checkpoint_with_parts(1)
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let checkpoint = FfiLastCheckpoint {
        tags: OptionalValue::Some(FfiStringMap {
            ptr: std::ptr::null(),
            len: 1,
        }),
        ..last_checkpoint_with_parts(1)
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let invalid_v2 = FfiLastCheckpointV2 {
        path: invalid_utf8(),
        size_in_bytes: OptionalValue::None,
        modification_time: OptionalValue::None,
        sidecar_files: OptionalValue::None,
        non_file_actions: OptionalValue::None,
    };
    let checkpoint = FfiLastCheckpoint {
        v2_checkpoint: &invalid_v2,
        ..last_checkpoint_with_parts(1)
    };
    assert!(unsafe { checkpoint.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Complete,
            file_stats: FfiFileStats {
                num_files: -1,
                table_size_bytes: 0,
            },
            file_size_histogram: std::ptr::null(),
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let boundaries = [0, 1];
    let counts = [0, -1];
    let bytes = [0, 0];
    let histogram = FfiFileSizeHistogram {
        sorted_bin_boundaries: KernelI64Slice {
            ptr: boundaries.as_ptr(),
            len: boundaries.len(),
        },
        file_counts: KernelI64Slice {
            ptr: counts.as_ptr(),
            len: counts.len(),
        },
        total_bytes: KernelI64Slice {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        },
    };
    let crc = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Complete,
            file_stats: FfiFileStats {
                num_files: 0,
                table_size_bytes: 0,
            },
            file_size_histogram: &histogram,
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        txn_id: OptionalValue::Some(invalid_utf8()),
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        all_files: OptionalValue::Some(FfiAddArray {
            ptr: std::ptr::null(),
            len: 1,
        }),
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let deleted_record_counts = FfiDeletedRecordCountsHistogram {
        deleted_record_counts: KernelI64Slice {
            ptr: std::ptr::null(),
            len: 1,
        },
    };
    let crc = FfiCrc {
        deleted_record_counts_histogram: &deleted_record_counts,
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        metadata: FfiMetadata {
            id: invalid_utf8(),
            ..test_metadata()
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());

    let crc = FfiCrc {
        protocol: FfiProtocol {
            min_reader_version: 0,
            ..test_protocol()
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());
}
