use std::collections::HashMap;
use std::error::Error as _;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::actions::{Add, CheckpointMetadata, Sidecar};
use delta_kernel::crc::{
    Crc, DeletedRecordCountsHistogram, DomainMetadataState, FileStats, FileStatsState,
    SetTransactionState,
};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel_default_engine::DefaultEngineBuilder;
use test_utils::assert_result_error_with_message;

use super::*;
use crate::delta_types::*;
use crate::error::KernelError;
use crate::ffi_test_utils::{
    allocate_err, assert_extern_result_error_contains, assert_extern_result_error_with_message,
    ok_or_panic,
};
use crate::log_path::FfiLogPath;
use crate::{
    engine_to_handle, free_engine, free_snapshot, free_snapshot_builder, get_snapshot_builder,
    get_snapshot_builder_from, snapshot_builder_build, snapshot_builder_set_version, FfiFileStats,
    KernelI64Slice, KernelStringSlice, OptionalValue, SharedExternEngine,
};

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

fn copy_protocol(value: &FfiProtocol) -> FfiProtocol {
    let copy_features = |features: &OptionalValue<FfiStringArray>| match features {
        OptionalValue::Some(features) => OptionalValue::Some(FfiStringArray {
            ptr: features.ptr,
            len: features.len,
        }),
        OptionalValue::None => OptionalValue::None,
    };
    FfiProtocol {
        min_reader_version: value.min_reader_version,
        min_writer_version: value.min_writer_version,
        reader_features: copy_features(&value.reader_features),
        writer_features: copy_features(&value.writer_features),
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
            kind: FILE_STATS_STATE_COMPLETE,
            file_stats: FfiFileStats {
                num_files: 0,
                table_size_bytes: 0,
            },
            file_size_histogram: std::ptr::null(),
        },
        in_commit_timestamp: none_i64(),
        set_transaction_state: FfiSetTransactionState {
            kind: SET_TRANSACTION_STATE_PARTIAL,
            transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: DOMAIN_METADATA_STATE_PARTIAL,
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

fn test_engine() -> Handle<SharedExternEngine> {
    engine_to_handle(
        Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
        allocate_err,
    )
}

fn test_builder(engine: &Handle<SharedExternEngine>) -> Handle<MutableFfiSnapshotBuilder> {
    unsafe {
        ok_or_panic(get_snapshot_builder(
            slice("memory:///hinted-table/"),
            engine.shallow_copy(),
        ))
    }
}

fn test_snapshot_hint(
    log_paths: &[FfiLogPath],
    version: Version,
    freshness: FfiSnapshotHintFreshness,
) -> FfiSnapshotHint {
    FfiSnapshotHint {
        version,
        freshness,
        log_paths: LogPathArray {
            ptr: log_paths.as_ptr(),
            len: log_paths.len(),
        },
        protocol: test_protocol(),
        metadata: test_metadata(),
        last_checkpoint: std::ptr::null(),
        crc: std::ptr::null(),
    }
}

unsafe fn set_minimal_hint(builder: &mut Handle<MutableFfiSnapshotBuilder>) {
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    unsafe { ok_or_panic(snapshot_builder_set_snapshot_hint(builder, &hint)) };
}

#[test]
fn invalid_crc_preserves_source() {
    let error = invalid_crc(Error::internal_error("invalid CRC state"));
    let Error::SnapshotHint(source) = error else {
        panic!("expected SnapshotHint")
    };
    assert!(source
        .source()
        .expect("connector error must preserve its source")
        .to_string()
        .contains("invalid CRC state"));
}

#[test]
fn typed_components_construct_rich_snapshot_state() {
    let transaction = FfiSetTransaction {
        app_id: slice("app"),
        version: 7,
        last_updated: OptionalValue::Some(123),
    };
    let domain = FfiDomainMetadata {
        domain: slice("example.domain"),
        configuration: slice("payload"),
        removed: false,
    };
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
            kind: FILE_STATS_STATE_COMPLETE,
            file_stats: FfiFileStats {
                table_size_bytes: 512,
                num_files: 1,
            },
            file_size_histogram: &histogram,
        },
        set_transaction_state: FfiSetTransactionState {
            kind: SET_TRANSACTION_STATE_COMPLETE,
            transactions: FfiSetTransactionArray {
                ptr: &transaction,
                len: 1,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: DOMAIN_METADATA_STATE_COMPLETE,
            domain_metadata: FfiDomainMetadataArray {
                ptr: &domain,
                len: 1,
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
    assert_eq!(
        complete_crc.set_transaction_state.expect_complete().len(),
        1
    );
    assert_eq!(
        complete_crc.domain_metadata_state.expect_complete().len(),
        1
    );
    let partial_crc_value = FfiCrc {
        set_transaction_state: FfiSetTransactionState {
            kind: SET_TRANSACTION_STATE_PARTIAL,
            transactions: FfiSetTransactionArray {
                ptr: &transaction,
                len: 1,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: DOMAIN_METADATA_STATE_PARTIAL,
            domain_metadata: FfiDomainMetadataArray {
                ptr: &domain,
                len: 1,
            },
        },
        ..empty_crc()
    };
    let partial_crc = unsafe { partial_crc_value.try_to_kernel() }.unwrap();
    assert!(matches!(
        partial_crc.set_transaction_state,
        SetTransactionState::Partial(ref values) if values.len() == 1
    ));
    assert!(matches!(
        partial_crc.domain_metadata_state,
        DomainMetadataState::Partial(ref values) if values.len() == 1
    ));

    let checkpoint_metadata = FfiCheckpointMetadata {
        version: 5,
        tags: none_map(),
    };
    let non_file_action = FfiCheckpointNonFileAction::CheckpointMetadata(&checkpoint_metadata);
    let sidecar = FfiSidecar {
        path: slice("sidecar.parquet"),
        size_in_bytes: 42,
        modification_time: 123,
        tags: none_map(),
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
        storage_type: DELETION_VECTOR_STORAGE_TYPE_PERSISTED_ABSOLUTE,
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
            kind: FILE_STATS_STATE_INDETERMINATE,
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
            kind: SET_TRANSACTION_STATE_COMPLETE,
            transactions: FfiSetTransactionArray {
                ptr: std::ptr::null(),
                len: 0,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: DOMAIN_METADATA_STATE_COMPLETE,
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

    let invalid_sidecar = FfiSidecar {
        path: invalid_utf8(),
        size_in_bytes: 1,
        modification_time: 1,
        tags: none_map(),
    };
    assert!(unsafe { invalid_sidecar.try_to_kernel() }.is_err());
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
            kind: SET_TRANSACTION_STATE_COMPLETE,
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
            kind: DOMAIN_METADATA_STATE_COMPLETE,
            domain_metadata: FfiDomainMetadataArray {
                ptr: std::ptr::null(),
                len: 1,
            },
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());
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

    let null_action = FfiCheckpointNonFileAction::Metadata(std::ptr::null());
    assert!(matches!(
        unsafe { null_action.try_to_kernel() },
        Err(Error::Generic(_))
    ));
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

    let crc = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FILE_STATS_STATE_COMPLETE,
            file_stats: FfiFileStats {
                num_files: -1,
                table_size_bytes: 0,
            },
            file_size_histogram: std::ptr::null(),
        },
        ..empty_crc()
    };
    assert!(unsafe { crc.try_to_kernel() }.is_err());
}

#[derive(Clone, Copy)]
enum InvalidHintComponent {
    Protocol,
    Metadata,
    LastCheckpoint,
}

impl InvalidHintComponent {
    fn expected_token(self) -> &'static str {
        match self {
            Self::Protocol => "supplied protocol",
            Self::Metadata => "supplied metadata",
            Self::LastCheckpoint => "supplied _last_checkpoint",
        }
    }
}

#[rstest::rstest]
#[case::protocol(InvalidHintComponent::Protocol)]
#[case::metadata(InvalidHintComponent::Metadata)]
#[case::last_checkpoint(InvalidHintComponent::LastCheckpoint)]
fn aggregate_setter_wraps_invalid_top_level_state(#[case] component: InvalidHintComponent) {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let invalid_feature = [invalid_utf8()];
    let invalid_protocol = FfiProtocol {
        writer_features: OptionalValue::Some(FfiStringArray {
            ptr: invalid_feature.as_ptr(),
            len: invalid_feature.len(),
        }),
        ..test_protocol()
    };
    let invalid_metadata = FfiMetadata {
        id: invalid_utf8(),
        ..test_metadata()
    };
    let invalid_last_checkpoint = FfiLastCheckpoint {
        version: 0,
        size: 1,
        parts: OptionalValue::None,
        size_in_bytes: none_i64(),
        num_of_add_files: none_i64(),
        checkpoint_schema: none_string(),
        checksum: OptionalValue::Some(invalid_utf8()),
        tags: none_map(),
        v2_checkpoint: std::ptr::null(),
    };
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    match component {
        InvalidHintComponent::Protocol => hint.protocol = invalid_protocol,
        InvalidHintComponent::Metadata => hint.metadata = invalid_metadata,
        InvalidHintComponent::LastCheckpoint => hint.last_checkpoint = &invalid_last_checkpoint,
    }

    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &hint) };
    assert_extern_result_error_contains(
        result,
        KernelError::InvalidSnapshotHint,
        component.expected_token(),
    );

    unsafe {
        free_snapshot_builder(builder);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_rejects_unknown_freshness_without_mutating_builder() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    unsafe { set_minimal_hint(&mut builder) };

    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let invalid_hint = test_snapshot_hint(std::slice::from_ref(&log_path), 0, u32::MAX);
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &invalid_hint) };
    assert_extern_result_error_contains(
        result,
        KernelError::InvalidSnapshotHint,
        "unknown snapshot hint freshness",
    );

    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    assert!(!unsafe { snapshot.as_ref() }.is_built_as_latest());
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_late_failure_preserves_existing_hint() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    unsafe { set_minimal_hint(&mut builder) };

    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let invalid_crc_state = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FILE_STATS_STATE_COMPLETE,
            file_stats: FfiFileStats {
                num_files: -1,
                table_size_bytes: 0,
            },
            file_size_histogram: std::ptr::null(),
        },
        ..empty_crc()
    };
    let mut replacement = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_LATEST,
    );
    replacement.crc = &invalid_crc_state;
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &replacement) };
    assert_extern_result_error_contains(result, KernelError::InvalidSnapshotHint, "supplied CRC");

    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    assert!(!unsafe { snapshot.as_ref() }.is_built_as_latest());
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_replaces_existing_hint_after_successful_validation() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    unsafe { set_minimal_hint(&mut builder) };

    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let replacement = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_LATEST,
    );
    unsafe {
        ok_or_panic(snapshot_builder_set_snapshot_hint(
            &mut builder,
            &replacement,
        ))
    };

    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    assert!(unsafe { snapshot.as_ref() }.is_built_as_latest());
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case("not-a-url")]
#[case("memory:///hinted-table/_delta_log/not-a-log-file")]
fn aggregate_setter_wraps_invalid_log_path_errors(#[case] location: &'static str) {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let log_path = FfiLogPath::new(slice(location), 1, 1);
    let hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &hint) };
    assert_extern_result_error_contains(
        result,
        KernelError::InvalidSnapshotHint,
        "supplied log paths",
    );

    unsafe {
        free_snapshot_builder(builder);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_treats_null_log_path_pointer_as_empty() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let hint = FfiSnapshotHint {
        version: 0,
        freshness: SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
        log_paths: LogPathArray {
            ptr: std::ptr::null(),
            len: 1,
        },
        protocol: test_protocol(),
        metadata: test_metadata(),
        last_checkpoint: std::ptr::null(),
        crc: std::ptr::null(),
    };
    unsafe { ok_or_panic(snapshot_builder_set_snapshot_hint(&mut builder, &hint)) };

    unsafe {
        free_snapshot_builder(builder);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_rejects_log_compaction_paths() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let log_path = FfiLogPath::new(
        slice(concat!(
            "memory:///hinted-table/_delta_log/",
            "00000000000000000000.00000000000000000001.compacted.json"
        )),
        1,
        1,
    );
    let hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        1,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &hint) };
    assert_extern_result_error_contains(result, KernelError::InvalidSnapshotHint, "log compaction");

    unsafe {
        free_snapshot_builder(builder);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_rejects_single_bin_histogram() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let boundary = [0];
    let histogram = FfiFileSizeHistogram {
        sorted_bin_boundaries: KernelI64Slice {
            ptr: boundary.as_ptr(),
            len: boundary.len(),
        },
        file_counts: KernelI64Slice {
            ptr: boundary.as_ptr(),
            len: boundary.len(),
        },
        total_bytes: KernelI64Slice {
            ptr: boundary.as_ptr(),
            len: boundary.len(),
        },
    };
    let crc = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FILE_STATS_STATE_COMPLETE,
            file_stats: FfiFileStats {
                num_files: 0,
                table_size_bytes: 0,
            },
            file_size_histogram: &histogram,
        },
        ..empty_crc()
    };
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    hint.crc = &crc;
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &hint) };
    assert_extern_result_error_with_message(result, KernelError::InvalidSnapshotHint, None);

    unsafe {
        free_snapshot_builder(builder);
        free_engine(engine);
    }
}

#[test]
fn aggregate_setter_builds_latest_snapshot_without_storage_files() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let last_checkpoint = FfiLastCheckpoint {
        version: 0,
        size: 1,
        parts: OptionalValue::None,
        size_in_bytes: none_i64(),
        num_of_add_files: none_i64(),
        checkpoint_schema: none_string(),
        checksum: none_string(),
        tags: none_map(),
        v2_checkpoint: std::ptr::null(),
    };
    let crc = empty_crc();
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_LATEST,
    );
    hint.last_checkpoint = &last_checkpoint;
    hint.crc = &crc;
    unsafe { ok_or_panic(snapshot_builder_set_snapshot_hint(&mut builder, &hint)) };

    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let snapshot_ref = unsafe { snapshot.as_ref() };
    assert_eq!(snapshot_ref.version(), 0);
    assert!(snapshot_ref.is_built_as_latest());
    assert_eq!(
        snapshot_ref
            .get_file_stats_if_present()
            .unwrap()
            .num_files(),
        0
    );

    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case::matching(0, true)]
#[case::conflicting(1, false)]
fn aggregate_setter_validates_explicit_builder_version_at_build(
    #[case] requested_version: Version,
    #[case] should_build: bool,
) {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    unsafe { snapshot_builder_set_version(&mut builder, requested_version) };

    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    unsafe { ok_or_panic(snapshot_builder_set_snapshot_hint(&mut builder, &hint)) };

    let result = unsafe { snapshot_builder_build(builder) };
    if should_build {
        let snapshot = ok_or_panic(result);
        assert_eq!(unsafe { snapshot.as_ref() }.version(), requested_version);
        unsafe { free_snapshot(snapshot) };
    } else {
        assert_extern_result_error_with_message(result, KernelError::InvalidSnapshotHint, None);
    }
    unsafe { free_engine(engine) };
}

fn assert_typed_checkpoint_build(
    log_paths: &[FfiLogPath],
    protocol: FfiProtocol,
    last_checkpoint: &FfiLastCheckpoint,
    expected_filenames: &[&str],
    expected_hint: &LastCheckpointHint,
) {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let crc = FfiCrc {
        protocol: copy_protocol(&protocol),
        ..empty_crc()
    };
    let mut hint = test_snapshot_hint(log_paths, 0, SNAPSHOT_HINT_FRESHNESS_UNVERIFIED);
    hint.protocol = protocol;
    hint.last_checkpoint = last_checkpoint;
    hint.crc = &crc;
    unsafe { ok_or_panic(snapshot_builder_set_snapshot_hint(&mut builder, &hint)) };

    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let snapshot_ref = unsafe { snapshot.as_ref() };
    assert_eq!(snapshot_ref.version(), 0);
    assert_eq!(
        snapshot_ref
            .get_file_stats_if_present()
            .unwrap()
            .num_files(),
        0
    );
    let segment = snapshot_ref.log_segment();
    assert_eq!(segment.checkpoint_version, Some(0));
    assert_eq!(
        segment
            .listed
            .checkpoint_parts
            .iter()
            .map(|part| part.filename.as_str())
            .collect::<Vec<_>>(),
        expected_filenames
    );
    assert_eq!(segment.checkpoint_hint(), Some(expected_hint));

    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

fn typed_multipart_checkpoint_build() {
    const PART_1: &str = "00000000000000000000.checkpoint.0000000001.0000000002.parquet";
    const PART_2: &str = "00000000000000000000.checkpoint.0000000002.0000000002.parquet";
    const PART_1_URL: &str = concat!(
        "memory:///hinted-table/_delta_log/",
        "00000000000000000000.checkpoint.0000000001.0000000002.parquet"
    );
    const PART_2_URL: &str = concat!(
        "memory:///hinted-table/_delta_log/",
        "00000000000000000000.checkpoint.0000000002.0000000002.parquet"
    );
    let log_paths = [
        FfiLogPath::new(slice(PART_1_URL), 1, 1),
        FfiLogPath::new(slice(PART_2_URL), 1, 1),
    ];
    let checkpoint = FfiLastCheckpoint {
        version: 0,
        size: 2,
        parts: OptionalValue::Some(2),
        size_in_bytes: none_i64(),
        num_of_add_files: none_i64(),
        checkpoint_schema: none_string(),
        checksum: none_string(),
        tags: none_map(),
        v2_checkpoint: std::ptr::null(),
    };
    let expected =
        LastCheckpointHint::from_parts(0, 2, Some(2), None, None, None, None, None, None).unwrap();
    assert_typed_checkpoint_build(
        &log_paths,
        test_protocol(),
        &checkpoint,
        &[PART_1, PART_2],
        &expected,
    );
}

fn typed_v2_checkpoint_build() {
    const CHECKPOINT: &str =
        "00000000000000000000.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet";
    const CHECKPOINT_URL: &str = concat!(
        "memory:///hinted-table/_delta_log/",
        "00000000000000000000.checkpoint.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet"
    );
    let features = [slice("v2Checkpoint")];
    let protocol = FfiProtocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: OptionalValue::Some(FfiStringArray {
            ptr: features.as_ptr(),
            len: features.len(),
        }),
        writer_features: OptionalValue::Some(FfiStringArray {
            ptr: features.as_ptr(),
            len: features.len(),
        }),
    };
    let sidecar = FfiSidecar {
        path: slice("sidecar.parquet"),
        size_in_bytes: 42,
        modification_time: 123,
        tags: none_map(),
    };
    let checkpoint_metadata = FfiCheckpointMetadata {
        version: 0,
        tags: none_map(),
    };
    let action = FfiCheckpointNonFileAction::CheckpointMetadata(&checkpoint_metadata);
    let v2 = FfiLastCheckpointV2 {
        path: slice(CHECKPOINT),
        size_in_bytes: none_i64(),
        modification_time: none_i64(),
        sidecar_files: OptionalValue::Some(FfiSidecarArray {
            ptr: &sidecar,
            len: 1,
        }),
        non_file_actions: OptionalValue::Some(FfiCheckpointNonFileActionArray {
            ptr: &action,
            len: 1,
        }),
    };
    let checkpoint = FfiLastCheckpoint {
        version: 0,
        size: 2,
        parts: OptionalValue::None,
        size_in_bytes: none_i64(),
        num_of_add_files: none_i64(),
        checkpoint_schema: none_string(),
        checksum: none_string(),
        tags: none_map(),
        v2_checkpoint: &v2,
    };
    let expected = LastCheckpointHint::from_parts(
        0,
        2,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(LastCheckpointV2::from_parts(
            CHECKPOINT.to_string(),
            None,
            None,
            Some(vec![Sidecar::new(
                "sidecar.parquet".to_string(),
                42,
                123,
                None,
            )]),
            Some(vec![HintAction::CheckpointMetadata(
                CheckpointMetadata::new(0, None),
            )]),
        )),
    )
    .unwrap();
    let log_paths = [FfiLogPath::new(slice(CHECKPOINT_URL), 1, 1)];
    assert_typed_checkpoint_build(&log_paths, protocol, &checkpoint, &[CHECKPOINT], &expected);
}

#[rstest::rstest]
#[case::multipart_v1(typed_multipart_checkpoint_build)]
#[case::uuid_v2(typed_v2_checkpoint_build)]
fn aggregate_checkpoint_build_preserves_identity_and_reconstructed_state(#[case] run_case: fn()) {
    run_case();
}

#[test]
fn aggregate_setter_rejects_existing_snapshot_builder() {
    let engine = test_engine();
    let mut initial_builder = test_builder(&engine);
    unsafe { set_minimal_hint(&mut initial_builder) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(initial_builder)) };

    let mut update_builder = unsafe {
        ok_or_panic(get_snapshot_builder_from(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
        ))
    };
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        SNAPSHOT_HINT_FRESHNESS_UNVERIFIED,
    );
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut update_builder, &hint) };
    assert_extern_result_error_contains(
        result,
        KernelError::InvalidSnapshotHint,
        "builder created from a table path",
    );

    unsafe {
        free_snapshot_builder(update_builder);
        free_snapshot(snapshot);
        free_engine(engine);
    }
}
