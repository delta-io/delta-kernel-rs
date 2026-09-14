use std::error::Error as _;
use std::sync::Arc;

use delta_kernel::actions::{CheckpointMetadata, Sidecar};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::object_store::memory::InMemory;
use delta_kernel_default_engine::DefaultEngineBuilder;

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
        FfiSnapshotHintFreshness::Unverified,
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
        FfiSnapshotHintFreshness::Unverified,
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
            kind: FfiFileStatsStateKind::Complete,
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
        FfiSnapshotHintFreshness::Latest,
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
        FfiSnapshotHintFreshness::Latest,
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
        FfiSnapshotHintFreshness::Unverified,
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
        freshness: FfiSnapshotHintFreshness::Unverified,
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
        FfiSnapshotHintFreshness::Unverified,
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
            kind: FfiFileStatsStateKind::Complete,
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
        FfiSnapshotHintFreshness::Unverified,
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
        FfiSnapshotHintFreshness::Latest,
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
        FfiSnapshotHintFreshness::Unverified,
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
    let mut hint = test_snapshot_hint(log_paths, 0, FfiSnapshotHintFreshness::Unverified);
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
fn aggregate_setter_reports_unsupported_for_existing_snapshot_builder() {
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
        FfiSnapshotHintFreshness::Unverified,
    );
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut update_builder, &hint) };
    assert_extern_result_error_contains(
        result,
        KernelError::UnsupportedError,
        "builders created by get_snapshot_builder_from",
    );

    unsafe {
        free_snapshot_builder(update_builder);
        free_snapshot(snapshot);
        free_engine(engine);
    }
}
