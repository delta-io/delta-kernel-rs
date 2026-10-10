use std::collections::BTreeMap;
use std::error::Error as _;
use std::ptr::NonNull;
use std::sync::Arc;

#[cfg(feature = "adaptive-metadata-in-dev")]
use delta_kernel::actions::{Add, LastManifestCommit};
#[cfg(feature = "adaptive-metadata-in-dev")]
use delta_kernel::crc::Crc;
use delta_kernel::last_checkpoint_hint::{LastCheckpointHint, LastCheckpointV2};
use delta_kernel::log_segment::LogSegment;
use delta_kernel::path::ParsedLogPath;
use delta_kernel::snapshot::{IncrementalReplay, PublicationWatermark, Snapshot};
use delta_kernel::{
    Engine, EvaluationHandler, FileMeta, JsonHandler, ParquetHandler, StorageHandler,
};
use test_utils::table_builder::{LogState, TestTableBuilder};
use test_utils::{compacted_log_path_for_versions, create_log_path, TestCatalogCommitter};

use super::*;
use crate::delta_types::*;
use crate::error::FFIKernelError;
use crate::ffi_test_utils::{
    allocate_err, assert_extern_result_error_contains, assert_extern_result_error_with_message,
    ok_or_panic,
};
use crate::log_path::FfiLogPath;
use crate::{
    engine_to_handle, free_engine, free_snapshot, get_snapshot_builder, get_snapshot_builder_from,
    snapshot_builder_build, snapshot_builder_with_max_catalog_version,
    snapshot_builder_with_version, FfiFileStats, KernelI64Slice, KernelStringSlice, OptionalValue,
    SharedExternEngine,
};

fn slice(value: &'static str) -> KernelStringSlice {
    unsafe { KernelStringSlice::new_unsafe(value) }
}

#[cfg(feature = "declarative-plans")]
mod planning_tests {
    use super::*;
    use crate::error::EngineExecResult;
    use crate::plans::result::CPlanResult;
    use crate::plans::{get_plan_based_engine, get_plan_executor};
    use crate::scan::{free_scan, scan_builder, scan_builder_build};
    use crate::snapshot_hint::planning::{
        scan_declarative_metadata_plan_visit, snapshot_hint_declarative_metadata_plan_trusted,
        snapshot_schema_to_proto,
    };
    use crate::KernelBytesSlice;

    extern "C" fn no_checkpoint_executor(
        _context: NullableCvoid,
        _bytes: KernelBytesSlice,
        _out: *mut EngineExecResult<CPlanResult>,
    ) {
        panic!("a table without checkpoints must not execute a plan during planning");
    }

    unsafe extern "C" fn copy_bytes(context: NullableCvoid, bytes: KernelBytesSlice) -> bool {
        let output = unsafe { &mut *context.unwrap().as_ptr().cast::<Vec<u8>>() };
        match unsafe { bytes.try_as_slice() } {
            Ok(bytes) => {
                output.extend_from_slice(bytes);
                true
            }
            Err(_) => false,
        }
    }

    unsafe extern "C" fn reject_bytes(_context: NullableCvoid, _bytes: KernelBytesSlice) -> bool {
        false
    }

    struct PlanVisit<'a> {
        engine: &'a Handle<SharedExternEngine>,
        schema: &'a [u8],
        bytes: Vec<u8>,
        result: Option<ExternResult<bool>>,
    }

    extern "C" fn plan_hint(context: NullableCvoid, hint: *const FfiSnapshotHint) {
        let state = unsafe { &mut *context.unwrap().as_ptr().cast::<PlanVisit<'_>>() };
        state.result = Some(unsafe {
            snapshot_hint_declarative_metadata_plan_trusted(
                slice("memory:///hinted-table/"),
                hint,
                KernelBytesSlice::new_unsafe(state.schema),
                state.engine.shallow_copy(),
                Some(NonNull::from(&mut state.bytes).cast()),
                copy_bytes,
            )
        });
    }

    #[test]
    fn source_snapshot_plan_matches_retained_scan_with_borrowed_protobuf_delivery() {
        let fallback = test_engine();
        let paths = [
            FfiLogPath::new(
                slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
                12,
                34,
            ),
            FfiLogPath::new(
                slice("memory:///hinted-table/_delta_log/00000000000000000001.json"),
                56,
                78,
            ),
        ];
        let mut hint = test_snapshot_hint(&paths, 1, FfiSnapshotHintFreshness::Latest);
        hint.metadata.schema_string = slice(
            r#"{"type":"struct","fields":[
              {"name":"id","type":"integer","nullable":true,"metadata":{}}
            ]}"#,
        );
        let builder = unsafe {
            ok_or_panic(snapshot_builder_with_snapshot_hint(
                test_builder(&fallback),
                &hint,
            ))
        };
        let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
        let executor = unsafe { get_plan_executor(None, no_checkpoint_executor) };
        let engine = unsafe { get_plan_based_engine(executor, OptionalValue::None, allocate_err) };
        let mut schema = Vec::new();
        assert!(unsafe {
            ok_or_panic(snapshot_schema_to_proto(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::from(&mut schema).cast()),
                copy_bytes,
            ))
        });
        let scan = unsafe {
            ok_or_panic(scan_builder_build(
                scan_builder(snapshot.shallow_copy()),
                engine.shallow_copy(),
            ))
        };
        let mut baseline: Vec<u8> = Vec::new();
        assert!(unsafe {
            ok_or_panic(scan_declarative_metadata_plan_visit(
                scan.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::from(&mut baseline).cast()),
                copy_bytes,
            ))
        });
        let mut visit = PlanVisit {
            engine: &engine,
            schema: &schema,
            bytes: Vec::new(),
            result: None,
        };
        assert!(unsafe {
            ok_or_panic(snapshot_to_snapshot_hint(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::from(&mut visit).cast()),
                plan_hint,
            ))
        });
        assert!(ok_or_panic(visit.result.take().unwrap()));
        assert_eq!(baseline, visit.bytes);
        assert_extern_result_error_contains(
            unsafe {
                snapshot_schema_to_proto(
                    snapshot.shallow_copy(),
                    engine.shallow_copy(),
                    None,
                    reject_bytes,
                )
            },
            FFIKernelError::InvalidSnapshotHint,
            "byte callback rejected delivery",
        );
        assert_extern_result_error_contains(
            unsafe {
                snapshot_hint_declarative_metadata_plan_trusted(
                    slice("memory:///hinted-table/"),
                    std::ptr::null(),
                    KernelBytesSlice::new_unsafe(&schema),
                    engine.shallow_copy(),
                    None,
                    reject_bytes,
                )
            },
            FFIKernelError::InvalidSnapshotHint,
            "snapshot hint must not be null",
        );
        unsafe {
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
            free_engine(fallback);
        }
    }
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
        OptionalValue::Some(FfiStringArray::empty())
    } else {
        OptionalValue::None
    }
}

fn empty_map() -> FfiStringMap {
    FfiStringMap::empty()
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
        partition_columns: FfiStringArray::empty(),
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
            transactions: FfiSetTransactionArray::empty(),
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Partial,
            domain_metadata: FfiDomainMetadataArray::empty(),
        },
        txn_id: none_string(),
        all_files: OptionalValue::None,
        num_deleted_records: none_i64(),
        num_deletion_vectors: none_i64(),
        deleted_record_counts_histogram: std::ptr::null(),
    }
}

fn test_engine() -> Handle<SharedExternEngine> {
    engine_to_handle(Arc::new(NoIoEngine), allocate_err)
}

struct NoIoEngine;

impl Engine for NoIoEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        panic!("snapshot hints must not require evaluation")
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        panic!("snapshot hints must not require storage")
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        panic!("snapshot hints must not require JSON I/O")
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        panic!("snapshot hints must not require Parquet I/O")
    }
}

fn test_builder(engine: &Handle<SharedExternEngine>) -> Handle<ExclusiveSnapshotBuilder> {
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
        publication_watermark: FfiPublicationWatermark::InferFromLogPaths,
    }
}

unsafe fn with_minimal_hint(
    builder: Handle<ExclusiveSnapshotBuilder>,
) -> Handle<ExclusiveSnapshotBuilder> {
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
    unsafe { ok_or_panic(snapshot_builder_with_snapshot_hint(builder, &hint)) }
}

#[rstest::rstest]
#[case::latest(FfiSnapshotHintFreshness::Latest, true)]
#[case::unverified(FfiSnapshotHintFreshness::Unverified, false)]
fn exported_hint_copy_outlives_snapshot_and_preserves_state(
    #[case] freshness: FfiSnapshotHintFreshness,
    #[case] expected_latest: bool,
    #[values(false, true)] with_crc: bool,
) {
    let engine = test_engine();
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
        123,
        456,
    );
    let crc = empty_crc();
    let mut hint = test_snapshot_hint(std::slice::from_ref(&log_path), 0, freshness);
    if with_crc {
        hint.crc = &crc;
    }
    let builder = unsafe {
        ok_or_panic(snapshot_builder_with_snapshot_hint(
            test_builder(&engine),
            &hint,
        ))
    };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let expected_segment = unsafe { snapshot.as_ref() }.log_segment().clone();
    let expected_crc = unsafe { snapshot.as_ref() }.crc_at_version().cloned();
    let builder = ok_or_panic(install_through_visitor(
        &snapshot,
        test_builder(&engine),
        &engine,
    ));
    unsafe { free_snapshot(snapshot) };
    let rebuilt = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let rebuilt_ref = unsafe { rebuilt.as_ref() };
    assert_eq!(rebuilt_ref.version(), 0);
    assert_eq!(rebuilt_ref.is_built_as_latest(), expected_latest);
    assert_eq!(rebuilt_ref.log_segment(), &expected_segment);
    assert_eq!(rebuilt_ref.crc_at_version(), expected_crc.as_ref());
    unsafe {
        free_snapshot(rebuilt);
        free_engine(engine);
    }
}

#[test]
fn snapshot_hint_export_borrows_inputs_and_calls_visitor_once() {
    let engine = test_engine();
    let builder = unsafe { with_minimal_hint(test_builder(&engine)) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let mut visits = 0usize;
    for expected_visits in 1..=2 {
        assert!(unsafe {
            ok_or_panic(snapshot_to_snapshot_hint(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::from(&mut visits).cast()),
                count_hint_visits,
            ))
        });
        assert_eq!(visits, expected_visits);
        assert_eq!(unsafe { snapshot.as_ref() }.version(), 0);
    }
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

extern "C" fn count_hint_visits(context: NullableCvoid, _hint: *const FfiSnapshotHint) {
    let visits = unsafe { &mut *context.unwrap().as_ptr().cast::<usize>() };
    *visits += 1;
}

#[test]
fn snapshot_hint_export_rejects_compaction_without_calling_visitor() {
    let engine = test_engine();
    let paths = [
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
            1,
            1,
        ),
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000001.json"),
            2,
            2,
        ),
    ];
    let hint = test_snapshot_hint(&paths, 1, FfiSnapshotHintFreshness::Unverified);
    let builder = unsafe {
        ok_or_panic(snapshot_builder_with_snapshot_hint(
            test_builder(&engine),
            &hint,
        ))
    };
    let source = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let source_ref = unsafe { source.as_ref() };
    let mut files = source_ref.log_segment().listed.clone();
    files.ascending_compaction_files.push(
        create_log_path(
            source_ref.table_root(),
            compacted_log_path_for_versions(0, 1, "json"),
        )
        .into(),
    );
    let snapshot = Snapshot::new(
        LogSegment::try_new(
            files,
            source_ref.log_segment().log_root.clone(),
            Some(1),
            None,
        )
        .unwrap(),
        source_ref.table_configuration().clone(),
    )
    .unwrap();
    let snapshot: Handle<SharedSnapshot> = Arc::new(snapshot).into();
    unsafe { free_snapshot(source) };

    let mut visits = 0usize;
    let result = unsafe {
        snapshot_to_snapshot_hint(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut visits).cast()),
            count_hint_visits,
        )
    };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        "log compaction",
    );
    assert_eq!(visits, 0);
    assert_eq!(unsafe { snapshot.as_ref() }.version(), 1);
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

struct CopiedHintPaths {
    files: Vec<FileMeta>,
    crc_version: Option<Version>,
    visits: usize,
}

extern "C" fn copy_hint_paths(context: NullableCvoid, hint: *const FfiSnapshotHint) {
    let state = unsafe { &mut *context.unwrap().as_ptr().cast::<CopiedHintPaths>() };
    let hint = unsafe { &*hint };
    state.visits += 1;
    state.files = unsafe { hint.log_paths.log_paths() }
        .unwrap()
        .into_iter()
        .map(|path| ParsedLogPath::from(path).location)
        .collect();
    state.crc_version = unsafe { hint.crc.as_ref() }.map(|crc| crc.version);
}

fn assert_exported_paths_and_round_trip(snapshot: Handle<SharedSnapshot>) {
    let source = unsafe { snapshot.as_ref() };
    let table_root = source.table_root().as_str().to_owned();
    let expected_segment = source.log_segment().clone();
    let expected_crc = source.crc_at_version().cloned();
    let expected_latest = source.is_built_as_latest();
    let files = &expected_segment.listed;
    let expected_files: BTreeMap<_, _> = files
        .checkpoint_parts
        .iter()
        .chain(&files.ascending_commit_files)
        .chain(files.latest_commit_file.iter())
        .chain(files.latest_crc_file.iter())
        .map(|path| {
            (
                path.location.location.as_str().to_owned(),
                path.location.clone(),
            )
        })
        .collect();
    let engine = test_engine();
    let mut state = CopiedHintPaths {
        files: Vec::new(),
        crc_version: None,
        visits: 0,
    };
    assert!(unsafe {
        ok_or_panic(snapshot_to_snapshot_hint(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut state).cast()),
            copy_hint_paths,
        ))
    });
    assert_eq!(state.visits, 1);
    assert!(state
        .files
        .windows(2)
        .all(|pair| pair[0].location.as_str() < pair[1].location.as_str()));
    assert_eq!(
        state.files,
        expected_files.into_values().collect::<Vec<_>>()
    );
    assert_eq!(
        state.crc_version,
        expected_crc.as_ref().map(|crc| crc.version)
    );

    let builder = unsafe {
        ok_or_panic(get_snapshot_builder(
            KernelStringSlice::new_unsafe(&table_root),
            engine.shallow_copy(),
        ))
    };
    let builder = ok_or_panic(install_through_visitor(&snapshot, builder, &engine));
    unsafe { free_snapshot(snapshot) };
    let rebuilt = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let rebuilt_ref = unsafe { rebuilt.as_ref() };
    assert_eq!(rebuilt_ref.log_segment(), &expected_segment);
    assert_eq!(rebuilt_ref.crc_at_version(), expected_crc.as_ref());
    assert_eq!(rebuilt_ref.is_built_as_latest(), expected_latest);
    unsafe {
        free_snapshot(rebuilt);
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case::checkpoint_with_later_commits(1)]
#[case::checkpoint_at_end(3)]
fn exported_hint_sorts_and_deduplicates_supplied_paths(#[case] checkpoint_version: Version) {
    let engine = test_engine();
    let checkpoint = if checkpoint_version == 1 {
        "memory:///hinted-table/_delta_log/00000000000000000001.checkpoint.parquet"
    } else {
        "memory:///hinted-table/_delta_log/00000000000000000003.checkpoint.parquet"
    };
    let paths = [
        FfiLogPath::new(slice(checkpoint), 1, 10),
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000003.json"),
            3,
            30,
        ),
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000002.crc"),
            2,
            20,
        ),
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000002.json"),
            2,
            20,
        ),
    ];
    let hint = test_snapshot_hint(&paths, 3, FfiSnapshotHintFreshness::Latest);
    let builder = unsafe {
        ok_or_panic(snapshot_builder_with_snapshot_hint(
            test_builder(&engine),
            &hint,
        ))
    };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    assert_exported_paths_and_round_trip(snapshot);
    unsafe { free_engine(engine) };
}

#[rstest::rstest]
#[case::checkpoint_with_later_commits(1, Some(2))]
#[case::checkpoint_at_end(3, None)]
#[cfg_attr(
    miri,
    ignore = "checkpoint setup is costly; shared FFI coverage is kept in \
              exported_hint_sorts_and_deduplicates_supplied_paths"
)]
fn exported_storage_snapshot_preserves_paths_and_omits_stale_crc(
    #[case] checkpoint_version: Version,
    #[case] expected_crc_path: Option<Version>,
) {
    let table = TestTableBuilder::new()
        .with_log_state(
            LogState::with_latest_version(3)
                .with_checkpoint_at([checkpoint_version])
                .with_crc_at([2]),
        )
        .with_data(1, 1)
        .build()
        .unwrap();
    let snapshot = Snapshot::builder_for(table.table_root())
        .with_incremental_crc_replay(IncrementalReplay::Disabled)
        .build(&table.engine())
        .unwrap();
    let segment = snapshot.log_segment();
    assert_eq!(segment.checkpoint_version, Some(checkpoint_version));
    assert_eq!(
        segment
            .listed
            .latest_crc_file
            .as_ref()
            .map(|path| path.version),
        expected_crc_path
    );
    assert_eq!(
        segment.listed.latest_commit_file.as_ref().unwrap().version,
        3
    );
    assert_eq!(
        segment.listed.ascending_commit_files.is_empty(),
        checkpoint_version == 3
    );
    assert!(snapshot.crc_at_version().is_none());
    assert_exported_paths_and_round_trip(snapshot.into());
}

struct VisitedHint {
    builder: Option<Handle<ExclusiveSnapshotBuilder>>,
    result: Option<ExternResult<Handle<ExclusiveSnapshotBuilder>>>,
    watermark: Option<PublicationWatermark>,
    visits: usize,
}

extern "C" fn install_visited_hint(context: NullableCvoid, hint: *const FfiSnapshotHint) {
    let state = unsafe { &mut *context.unwrap().as_ptr().cast::<VisitedHint>() };
    let hint = unsafe { &*hint };
    state.visits += 1;
    state.watermark = Some(hint.publication_watermark.into());
    state.result =
        Some(unsafe { snapshot_builder_with_snapshot_hint(state.builder.take().unwrap(), hint) });
}

fn install_through_visitor(
    snapshot: &Handle<SharedSnapshot>,
    builder: Handle<ExclusiveSnapshotBuilder>,
    engine: &Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveSnapshotBuilder>> {
    let mut state = VisitedHint {
        builder: Some(builder),
        result: None,
        watermark: None,
        visits: 0,
    };
    assert!(unsafe {
        ok_or_panic(snapshot_to_snapshot_hint(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut state).cast()),
            install_visited_hint,
        ))
    });
    assert_eq!(state.visits, 1);
    state.result.take().unwrap()
}

fn rebuild_through_visitor(
    snapshot: &Handle<SharedSnapshot>,
    engine: &Handle<SharedExternEngine>,
) -> Handle<SharedSnapshot> {
    let builder = ok_or_panic(install_through_visitor(
        snapshot,
        test_builder(engine),
        engine,
    ));
    unsafe { ok_or_panic(snapshot_builder_build(builder)) }
}

#[rstest::rstest]
#[case::inferred(FfiPublicationWatermark::InferFromLogPaths, Some(0))]
#[case::explicit_absence(FfiPublicationWatermark::NoPublishedCommits, None)]
#[case::explicit_version(FfiPublicationWatermark::PublishedThrough(0), Some(0))]
fn publication_watermark_distinguishes_inference_from_explicit_state(
    #[case] watermark: FfiPublicationWatermark,
    #[case] expected: Option<Version>,
) {
    assert_eq!(
        FfiPublicationWatermark::from(PublicationWatermark::from(watermark)),
        watermark
    );
    let engine = test_engine();
    let path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
        1,
        1,
    );
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&path),
        0,
        FfiSnapshotHintFreshness::Unverified,
    );
    hint.publication_watermark = watermark;
    let builder = unsafe {
        ok_or_panic(snapshot_builder_with_snapshot_hint(
            test_builder(&engine),
            &hint,
        ))
    };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    assert_eq!(
        unsafe { snapshot.as_ref() }
            .log_segment()
            .listed
            .max_published_version,
        expected
    );
    let mut state = VisitedHint {
        builder: Some(test_builder(&engine)),
        result: None,
        watermark: None,
        visits: 0,
    };
    assert!(unsafe {
        ok_or_panic(snapshot_to_snapshot_hint(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut state).cast()),
            install_visited_hint,
        ))
    });
    assert_eq!(state.visits, 1);
    assert_eq!(
        state.watermark,
        Some(expected.map_or(
            PublicationWatermark::NoPublishedCommits,
            PublicationWatermark::PublishedThrough,
        ))
    );
    unsafe {
        crate::free_snapshot_builder(ok_or_panic(state.result.take().unwrap()));
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case::no_published_commits(None)]
#[case::watermark_absent_from_paths(Some(8))]
fn visited_hint_round_trips_explicit_watermark_and_outlives_source(
    #[case] watermark: Option<Version>,
    #[values(false, true)] with_crc: bool,
) {
    let engine = test_engine();
    let path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000010.checkpoint.parquet"),
        123,
        456,
    );
    let mut crc = empty_crc();
    crc.version = 10;
    crc.all_files = OptionalValue::Some(FfiAddArray::empty());
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&path),
        10,
        FfiSnapshotHintFreshness::Latest,
    );
    hint.publication_watermark = watermark.map_or(
        FfiPublicationWatermark::NoPublishedCommits,
        FfiPublicationWatermark::PublishedThrough,
    );
    if with_crc {
        hint.crc = &crc;
    }
    let builder = unsafe {
        ok_or_panic(snapshot_builder_with_snapshot_hint(
            test_builder(&engine),
            &hint,
        ))
    };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let expected_segment = unsafe { snapshot.as_ref() }.log_segment().clone();
    let expected_crc = unsafe { snapshot.as_ref() }.crc_at_version().cloned();
    let mut state = VisitedHint {
        builder: Some(test_builder(&engine)),
        result: None,
        watermark: None,
        visits: 0,
    };
    let visited = unsafe {
        ok_or_panic(snapshot_to_snapshot_hint(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut state).cast()),
            install_visited_hint,
        ))
    };
    assert!(visited);
    assert_eq!(state.visits, 1);
    unsafe { free_snapshot(snapshot) };
    assert_eq!(
        state.watermark,
        Some(watermark.map_or(
            PublicationWatermark::NoPublishedCommits,
            PublicationWatermark::PublishedThrough,
        ))
    );
    let builder = ok_or_panic(state.result.take().unwrap());
    let rebuilt = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    assert_eq!(unsafe { rebuilt.as_ref() }.log_segment(), &expected_segment);
    assert_eq!(
        unsafe { rebuilt.as_ref() }.crc_at_version(),
        expected_crc.as_ref()
    );
    assert!(unsafe { rebuilt.as_ref() }.is_built_as_latest());
    unsafe {
        free_snapshot(rebuilt);
        free_engine(engine);
    }
}

#[test]
fn visited_hint_preserves_partial_crc_maps_and_indeterminate_file_stats() {
    let engine = test_engine();
    let path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
        1,
        1,
    );
    let transaction = FfiSetTransaction {
        app_id: slice("app"),
        version: 7,
        last_updated: OptionalValue::Some(29),
    };
    let domain = FfiDomainMetadata {
        domain: slice("example.domain"),
        configuration: slice("payload"),
        removed: false,
    };
    let crc = FfiCrc {
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Indeterminate,
            file_stats: FfiFileStats {
                num_files: 0,
                table_size_bytes: 0,
            },
            file_size_histogram: std::ptr::null(),
        },
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Partial,
            transactions: FfiSetTransactionArray {
                ptr: &transaction,
                len: 1,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Partial,
            domain_metadata: FfiDomainMetadataArray {
                ptr: &domain,
                len: 1,
            },
        },
        ..empty_crc()
    };
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&path),
        0,
        FfiSnapshotHintFreshness::Unverified,
    );
    hint.crc = &crc;
    let builder = unsafe {
        ok_or_panic(snapshot_builder_with_snapshot_hint(
            test_builder(&engine),
            &hint,
        ))
    };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let rebuilt = rebuild_through_visitor(&snapshot, &engine);
    let rebuilt_ref = unsafe { rebuilt.as_ref() };
    assert_eq!(
        rebuilt_ref.crc_at_version(),
        unsafe { snapshot.as_ref() }.crc_at_version()
    );
    assert!(rebuilt_ref.get_file_stats_if_present().is_none());
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    assert_eq!(
        rebuilt_ref
            .get_app_id_version("app", kernel_engine.as_ref())
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        rebuilt_ref
            .get_domain_metadata("example.domain", kernel_engine.as_ref())
            .unwrap()
            .as_deref(),
        Some("payload")
    );
    unsafe {
        free_snapshot(rebuilt);
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[test]
fn visited_hint_after_publish_preserves_staged_paths_and_advanced_watermark() {
    let engine = test_engine();
    let reader_features = [slice("catalogManaged")];
    let writer_features = [slice("catalogManaged"), slice("inCommitTimestamp")];
    let configuration = [FfiStringMapEntry {
        key: slice("delta.enableInCommitTimestamps"),
        value: slice("true"),
    }];
    let protocol = || FfiProtocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: OptionalValue::Some(FfiStringArray {
            ptr: reader_features.as_ptr(),
            len: reader_features.len(),
        }),
        writer_features: OptionalValue::Some(FfiStringArray {
            ptr: writer_features.as_ptr(),
            len: writer_features.len(),
        }),
    };
    let metadata = || FfiMetadata {
        configuration: FfiStringMap {
            ptr: configuration.as_ptr(),
            len: configuration.len(),
        },
        ..test_metadata()
    };
    let paths = [
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
            1,
            1,
        ),
        FfiLogPath::new(
            slice(concat!(
                "memory:///hinted-table/_delta_log/_staged_commits/",
                "00000000000000000001.3a0d65cd-4056-49b8-937b-95f9e3ee90e5.json",
            )),
            2,
            1,
        ),
    ];
    let crc = FfiCrc {
        version: 1,
        protocol: protocol(),
        metadata: metadata(),
        in_commit_timestamp: OptionalValue::Some(2),
        ..empty_crc()
    };
    let mut hint = test_snapshot_hint(&paths, 1, FfiSnapshotHintFreshness::Latest);
    hint.protocol = protocol();
    hint.metadata = metadata();
    hint.crc = &crc;
    let builder = unsafe { snapshot_builder_with_max_catalog_version(test_builder(&engine), 1) };
    let builder = unsafe { ok_or_panic(snapshot_builder_with_snapshot_hint(builder, &hint)) };
    let source = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let source_ref = unsafe { source.clone_as_arc() };
    assert_eq!(
        source_ref.log_segment().listed.max_published_version,
        Some(0)
    );
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let published = source_ref
        .publish(kernel_engine.as_ref(), &TestCatalogCommitter)
        .unwrap();
    assert_eq!(
        published.log_segment().listed.max_published_version,
        Some(1)
    );
    assert_eq!(
        published.log_segment().listed.ascending_commit_files,
        source_ref.log_segment().listed.ascending_commit_files
    );
    let published: Handle<SharedSnapshot> = published.into();
    let expected_segment = unsafe { published.as_ref() }.log_segment().listed.clone();
    let builder = unsafe { snapshot_builder_with_max_catalog_version(test_builder(&engine), 1) };
    let mut state = VisitedHint {
        builder: Some(builder),
        result: None,
        watermark: None,
        visits: 0,
    };
    assert!(unsafe {
        ok_or_panic(snapshot_to_snapshot_hint(
            published.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut state).cast()),
            install_visited_hint,
        ))
    });
    assert_eq!(state.visits, 1);
    unsafe {
        free_snapshot(source);
        free_snapshot(published);
    }
    drop(source_ref);
    assert_eq!(
        state.watermark,
        Some(PublicationWatermark::PublishedThrough(1))
    );
    let rebuilt =
        unsafe { ok_or_panic(snapshot_builder_build(ok_or_panic(state.result.unwrap()))) };
    assert_eq!(
        unsafe { rebuilt.as_ref() }.log_segment().listed,
        expected_segment
    );
    unsafe {
        free_snapshot(rebuilt);
        free_engine(engine);
    }
}

#[cfg(feature = "adaptive-metadata-in-dev")]
#[rstest::rstest]
#[case::manifest(true, false)]
#[case::back_reference(false, true)]
#[case::both(true, true)]
fn typed_export_rejects_adaptive_crc_without_visiting_or_consuming_snapshot(
    #[case] with_manifest: bool,
    #[case] with_back_reference: bool,
) {
    let engine = test_engine();
    let mut crc = empty_crc();
    crc.file_stats_state.file_stats.num_files = 1;
    crc.file_stats_state.file_stats.table_size_bytes = 13;
    let crc = unsafe { crc.try_to_kernel() }.unwrap();
    let back_reference = with_back_reference
        .then(|| serde_json::json!({"manifest": "metadata/leaf.parquet", "pos": 0}));
    let add: Add = serde_json::from_value(serde_json::json!({
        "path": "part.parquet",
        "partitionValues": {},
        "size": 13,
        "modificationTime": 17,
        "dataChange": false,
        "backReference": back_reference,
    }))
    .unwrap();
    assert_eq!(add.back_reference().is_some(), with_back_reference);
    let expected_crc = Crc::try_from_parts(
        0,
        crc.metadata.clone(),
        crc.protocol.clone(),
        crc.file_stats_state().clone(),
        None,
        crc.set_transaction_state.clone(),
        crc.domain_metadata_state.clone(),
        None,
        Some(vec![add]),
        None,
        None,
        None,
        with_manifest.then(|| LastManifestCommit::new(0, 0).unwrap()),
    )
    .unwrap();
    let path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
        1,
        1,
    );
    let input = test_snapshot_hint(
        std::slice::from_ref(&path),
        0,
        FfiSnapshotHintFreshness::Unverified,
    );
    let hint = SnapshotHint::try_new(
        "memory:///hinted-table/",
        0,
        PublicationWatermark::InferFromLogPaths,
        unsafe { input.log_paths.log_paths() }.unwrap(),
        expected_crc.protocol.clone(),
        expected_crc.metadata.clone(),
        None,
        Some(Arc::new(expected_crc.clone())),
        SnapshotHintFreshness::Unverified,
    )
    .unwrap();
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    let source = Snapshot::builder_for("memory:///hinted-table/")
        .with_snapshot_hint(hint)
        .build(kernel_engine.as_ref())
        .unwrap();
    let source: Handle<SharedSnapshot> = source.into();
    let mut state = VisitedHint {
        builder: Some(test_builder(&engine)),
        result: None,
        watermark: None,
        visits: 0,
    };
    let result = unsafe {
        snapshot_to_snapshot_hint(
            source.shallow_copy(),
            engine.shallow_copy(),
            Some(NonNull::from(&mut state).cast()),
            install_visited_hint,
        )
    };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::UnsupportedError,
        if with_manifest {
            "lastManifestCommit"
        } else {
            "backReference"
        },
    );
    assert!(state.result.is_none());
    assert!(state.watermark.is_none());
    assert_eq!(state.visits, 0);
    assert_eq!(
        unsafe { source.as_ref() }.crc_at_version().map(Arc::as_ref),
        Some(&expected_crc)
    );
    unsafe {
        crate::free_snapshot_builder(state.builder.take().unwrap());
        free_snapshot(source);
        free_engine(engine);
    }
}

#[test]
fn visited_hint_rejects_incremental_builder_without_consuming_snapshot() {
    let engine = test_engine();
    let builder = unsafe { with_minimal_hint(test_builder(&engine)) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let builder = unsafe {
        ok_or_panic(get_snapshot_builder_from(
            snapshot.shallow_copy(),
            engine.shallow_copy(),
        ))
    };
    let result = install_through_visitor(&snapshot, builder, &engine);
    assert_extern_result_error_contains(
        result,
        FFIKernelError::UnsupportedError,
        "snapshot hints cannot be set",
    );
    assert_eq!(unsafe { snapshot.as_ref() }.version(), 0);
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case::different_table("memory:///other-table/", false)]
#[case::version_mismatch("memory:///hinted-table/", true)]
fn exported_hint_rejects_cross_table_paths_at_install_and_version_mismatch_at_build(
    #[case] table_root: &'static str,
    #[case] version_mismatch: bool,
) {
    let engine = test_engine();
    let builder = unsafe { with_minimal_hint(test_builder(&engine)) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let builder = unsafe {
        ok_or_panic(get_snapshot_builder(
            slice(table_root),
            engine.shallow_copy(),
        ))
    };
    let builder = if version_mismatch {
        unsafe { snapshot_builder_with_version(builder, 1) }
    } else {
        builder
    };
    let result = match install_through_visitor(&snapshot, builder, &engine) {
        ExternResult::Ok(builder) => {
            assert!(version_mismatch);
            unsafe { snapshot_builder_build(builder) }
        }
        ExternResult::Err(error) => {
            assert!(!version_mismatch);
            ExternResult::Err(error)
        }
    };
    assert_extern_result_error_with_message(result, FFIKernelError::InvalidSnapshotHint, None);
    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[test]
fn invalid_crc_preserves_source() {
    let error = invalid_crc(KernelError::internal_error("invalid CRC state"));
    let KernelError::SnapshotHint(source) = error else {
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
fn aggregate_with_wraps_invalid_top_level_state(#[case] component: InvalidHintComponent) {
    let engine = test_engine();
    let builder = test_builder(&engine);
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

    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &hint) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        component.expected_token(),
    );

    unsafe {
        free_engine(engine);
    }
}

#[test]
fn aggregate_with_late_failure_consumes_builder() {
    let engine = test_engine();
    let builder = unsafe { with_minimal_hint(test_builder(&engine)) };

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
    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &replacement) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        "supplied CRC",
    );

    unsafe { free_engine(engine) };
}

#[test]
fn aggregate_with_replaces_existing_hint_after_successful_validation() {
    let engine = test_engine();
    let builder = unsafe { with_minimal_hint(test_builder(&engine)) };

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
    let builder =
        unsafe { ok_or_panic(snapshot_builder_with_snapshot_hint(builder, &replacement)) };

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
fn aggregate_with_wraps_invalid_log_path_errors(#[case] location: &'static str) {
    let engine = test_engine();
    let builder = test_builder(&engine);
    let log_path = FfiLogPath::new(slice(location), 1, 1);
    let hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        FfiSnapshotHintFreshness::Unverified,
    );
    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &hint) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        "supplied log paths",
    );

    unsafe {
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case::published_commit("memory:///other/_delta_log/00000000000000000008.json")]
#[case::staged_commit(concat!(
    "memory:///other/_delta_log/_staged_commits/",
    "00000000000000000008.11111111-1111-1111-1111-111111111111.json"
))]
#[case::checkpoint("memory:///other/_delta_log/00000000000000000008.checkpoint.parquet")]
#[case::incomplete_checkpoint(concat!(
    "memory:///other/_delta_log/",
    "00000000000000000008.checkpoint.0000000001.0000000002.parquet"
))]
#[case::crc("memory:///other/_delta_log/00000000000000000008.crc")]
fn aggregate_with_rejects_foreign_paths_discarded_by_checkpoint_selection(
    #[case] location: &'static str,
) {
    let engine = test_engine();
    let builder = test_builder(&engine);
    let log_paths = [
        FfiLogPath::new(slice(location), 1, 1),
        FfiLogPath::new(
            slice("memory:///hinted-table/_delta_log/00000000000000000010.checkpoint.parquet"),
            1,
            1,
        ),
    ];
    let hint = test_snapshot_hint(&log_paths, 10, FfiSnapshotHintFreshness::Unverified);
    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &hint) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        &format!(
            "log path '{location}' is not beneath log root 'memory:///hinted-table/_delta_log/'"
        ),
    );
    unsafe { free_engine(engine) };
}

#[test]
fn aggregate_with_rejects_null_nonempty_log_path_array() {
    let engine = test_engine();
    let builder = test_builder(&engine);
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
        publication_watermark: FfiPublicationWatermark::InferFromLogPaths,
    };
    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &hint) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        "supplied log paths are invalid",
    );

    unsafe {
        free_engine(engine);
    }
}

#[test]
fn aggregate_with_rejects_log_compaction_paths() {
    let engine = test_engine();
    let builder = test_builder(&engine);
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
    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &hint) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        "log compaction",
    );

    unsafe {
        free_engine(engine);
    }
}

#[test]
fn aggregate_with_rejects_single_bin_histogram() {
    let engine = test_engine();
    let builder = test_builder(&engine);
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
    let result = unsafe { snapshot_builder_with_snapshot_hint(builder, &hint) };
    assert_extern_result_error_with_message(result, FFIKernelError::InvalidSnapshotHint, None);

    unsafe {
        free_engine(engine);
    }
}

#[rstest::rstest]
fn aggregate_with_builds_latest_snapshot_from_rich_crc(
    #[values(1, 3)] num_files: i64,
    #[values(
        None,
        Some(FfiDeletionVectorStorageType::Inline),
        Some(FfiDeletionVectorStorageType::PersistedRelative),
        Some(FfiDeletionVectorStorageType::PersistedAbsolute)
    )]
    dv_storage_type: Option<FfiDeletionVectorStorageType>,
) {
    const PARTITIONED_SCHEMA: &str = concat!(
        r#"{"type":"struct","fields":[{"name":"p","type":"string","nullable":true,"#,
        r#""metadata":{}}]}"#,
    );

    let engine = test_engine();
    let builder = test_builder(&engine);
    let partition_columns = [slice("p")];
    let format_options = [FfiStringMapEntry {
        key: slice("compression"),
        value: slice("snappy"),
    }];
    let metadata = || FfiMetadata {
        id: slice("table-id"),
        name: OptionalValue::Some(slice("partitioned-table")),
        description: OptionalValue::Some(slice("rich snapshot hint round trip")),
        format_provider: slice("parquet"),
        format_options: FfiStringMap {
            ptr: format_options.as_ptr(),
            len: format_options.len(),
        },
        schema_string: slice(PARTITIONED_SCHEMA),
        partition_columns: FfiStringArray {
            ptr: partition_columns.as_ptr(),
            len: partition_columns.len(),
        },
        created_time: OptionalValue::Some(123456789),
        configuration: empty_map(),
    };
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        1,
        1,
    );
    let checkpoint_tags = [FfiStringMapEntry {
        key: slice("checkpoint-tag"),
        value: slice("checkpoint-value"),
    }];
    let last_checkpoint = FfiLastCheckpoint {
        version: 0,
        size: num_files + 2,
        parts: OptionalValue::None,
        size_in_bytes: OptionalValue::Some(271),
        num_of_add_files: OptionalValue::Some(num_files),
        checkpoint_schema: none_string(),
        checksum: OptionalValue::Some(slice("checkpoint-checksum")),
        tags: OptionalValue::Some(FfiStringMap {
            ptr: checkpoint_tags.as_ptr(),
            len: checkpoint_tags.len(),
        }),
        v2_checkpoint: std::ptr::null(),
    };
    let partition_value = FfiStringMapEntry {
        key: slice("p"),
        value: slice("one"),
    };
    let tags = [
        FfiNullableStringMapEntry {
            key: slice("absent"),
            value: OptionalValue::None,
        },
        FfiNullableStringMapEntry {
            key: slice("present"),
            value: OptionalValue::Some(slice("value")),
        },
    ];
    let with_deletion_vectors = dv_storage_type.is_some();
    let paths: Vec<_> = (0..num_files)
        .map(|index| format!("p=one/part-{index:05}.parquet"))
        .collect();
    let deletion_vectors: Vec<_> = (0..num_files)
        .map(|index| FfiDeletionVectorDescriptor {
            storage_type: dv_storage_type.unwrap_or(FfiDeletionVectorStorageType::Inline),
            path_or_inline_dv: slice(match dv_storage_type {
                Some(FfiDeletionVectorStorageType::PersistedAbsolute) => {
                    "file:///deletion-vector.bin"
                }
                Some(FfiDeletionVectorStorageType::PersistedRelative) => "ab^-aqEH.-t@S}K{vb[*k^",
                _ => "encoded-dv",
            }),
            offset: match dv_storage_type {
                Some(FfiDeletionVectorStorageType::Inline) | None => OptionalValue::None,
                _ => OptionalValue::Some(7),
            },
            size_in_bytes: 8,
            cardinality: index + 1,
        })
        .collect();
    let adds: Vec<_> = paths
        .iter()
        .zip(&deletion_vectors)
        .map(|(path, deletion_vector)| FfiAdd {
            path: crate::kernel_string_slice!(path),
            partition_values: FfiStringMap {
                ptr: &partition_value,
                len: 1,
            },
            size: 17,
            modification_time: 19,
            data_change: true,
            stats: OptionalValue::Some(slice(r#"{"numRecords":23}"#)),
            tags: OptionalValue::Some(FfiNullableStringMap {
                ptr: tags.as_ptr(),
                len: tags.len(),
            }),
            deletion_vector: if with_deletion_vectors {
                deletion_vector
            } else {
                std::ptr::null()
            },
            base_row_id: OptionalValue::Some(29),
            default_row_commit_version: OptionalValue::Some(31),
            clustering_provider: OptionalValue::Some(slice("liquid")),
        })
        .collect();
    let transaction = FfiSetTransaction {
        app_id: slice("app"),
        version: 7,
        last_updated: OptionalValue::Some(29),
    };
    let domain_metadata = FfiDomainMetadata {
        domain: slice("example.domain"),
        configuration: slice("payload"),
        removed: false,
    };
    let boundaries = [0, 18];
    let counts = [num_files, 0];
    let total_bytes = [17 * num_files, 0];
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
            ptr: total_bytes.as_ptr(),
            len: total_bytes.len(),
        },
    };
    let deleted_record_counts = if with_deletion_vectors {
        [0, num_files, 0, 0, 0, 0, 0, 0, 0, 0]
    } else {
        [num_files, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    };
    let deleted_record_counts_histogram = FfiDeletedRecordCountsHistogram {
        deleted_record_counts: KernelI64Slice {
            ptr: deleted_record_counts.as_ptr(),
            len: deleted_record_counts.len(),
        },
    };
    let crc = FfiCrc {
        metadata: metadata(),
        file_stats_state: FfiFileStatsState {
            kind: FfiFileStatsStateKind::Complete,
            file_stats: FfiFileStats {
                num_files,
                table_size_bytes: 17 * num_files,
            },
            file_size_histogram: &histogram,
        },
        in_commit_timestamp: OptionalValue::Some(31),
        set_transaction_state: FfiSetTransactionState {
            kind: FfiSetTransactionStateKind::Complete,
            transactions: FfiSetTransactionArray {
                ptr: &transaction,
                len: 1,
            },
        },
        domain_metadata_state: FfiDomainMetadataState {
            kind: FfiDomainMetadataStateKind::Complete,
            domain_metadata: FfiDomainMetadataArray {
                ptr: &domain_metadata,
                len: 1,
            },
        },
        txn_id: OptionalValue::Some(slice("txn-id")),
        all_files: OptionalValue::Some(FfiAddArray {
            ptr: adds.as_ptr(),
            len: adds.len(),
        }),
        num_deleted_records: OptionalValue::Some(if with_deletion_vectors {
            num_files * (num_files + 1) / 2
        } else {
            0
        }),
        num_deletion_vectors: OptionalValue::Some(if with_deletion_vectors {
            num_files
        } else {
            0
        }),
        deleted_record_counts_histogram: &deleted_record_counts_histogram,
        ..empty_crc()
    };
    let mut hint = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        FfiSnapshotHintFreshness::Latest,
    );
    hint.metadata = metadata();
    hint.last_checkpoint = &last_checkpoint;
    hint.crc = &crc;
    let builder = unsafe { ok_or_panic(snapshot_builder_with_snapshot_hint(builder, &hint)) };

    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let snapshot_ref = unsafe { snapshot.as_ref() };
    assert_eq!(snapshot_ref.version(), 0);
    assert!(snapshot_ref.is_built_as_latest());
    assert_eq!(
        snapshot_ref
            .get_file_stats_if_present()
            .unwrap()
            .num_files(),
        num_files
    );
    let kernel_engine = unsafe { engine.as_ref() }.engine();
    assert_eq!(
        snapshot_ref
            .get_app_id_version("app", kernel_engine.as_ref())
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        snapshot_ref
            .get_domain_metadata("example.domain", kernel_engine.as_ref())
            .unwrap()
            .as_deref(),
        Some("payload")
    );

    let rebuilt = rebuild_through_visitor(&snapshot, &engine);
    let rebuilt_ref = unsafe { rebuilt.as_ref() };
    assert_eq!(rebuilt_ref.log_segment(), snapshot_ref.log_segment());
    assert_eq!(rebuilt_ref.crc_at_version(), snapshot_ref.crc_at_version());
    assert!(rebuilt_ref.is_built_as_latest());

    unsafe {
        free_snapshot(rebuilt);
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[rstest::rstest]
#[case::matching(0, true)]
#[case::conflicting(1, false)]
fn aggregate_with_validates_explicit_builder_version_at_build(
    #[case] requested_version: Version,
    #[case] should_build: bool,
) {
    let engine = test_engine();
    let builder = test_builder(&engine);
    let builder = unsafe { snapshot_builder_with_version(builder, requested_version) };

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
    let builder = unsafe { ok_or_panic(snapshot_builder_with_snapshot_hint(builder, &hint)) };

    let result = unsafe { snapshot_builder_build(builder) };
    if should_build {
        let snapshot = ok_or_panic(result);
        assert_eq!(unsafe { snapshot.as_ref() }.version(), requested_version);
        unsafe { free_snapshot(snapshot) };
    } else {
        assert_extern_result_error_with_message(result, FFIKernelError::InvalidSnapshotHint, None);
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
    let builder = test_builder(&engine);
    let crc = FfiCrc {
        protocol: copy_protocol(&protocol),
        ..empty_crc()
    };
    let mut hint = test_snapshot_hint(log_paths, 0, FfiSnapshotHintFreshness::Unverified);
    hint.protocol = protocol;
    hint.last_checkpoint = last_checkpoint;
    hint.crc = &crc;
    let builder = unsafe { ok_or_panic(snapshot_builder_with_snapshot_hint(builder, &hint)) };

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

    let visited = rebuild_through_visitor(&snapshot, &engine);
    assert_eq!(
        unsafe { visited.as_ref() }.log_segment(),
        snapshot_ref.log_segment()
    );
    assert_eq!(
        unsafe { visited.as_ref() }.crc_at_version(),
        snapshot_ref.crc_at_version()
    );
    let rebuilt_ref = unsafe { visited.as_ref() };
    assert_eq!(
        rebuilt_ref.log_segment().checkpoint_hint(),
        Some(expected_hint)
    );
    assert_eq!(
        rebuilt_ref.get_file_stats_if_present().unwrap().num_files(),
        0
    );
    unsafe {
        free_snapshot(visited);
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

const CHECKPOINT_SCHEMA: &str = concat!(
    r#"{"type":"struct","fields":[{"name":"add","type":{"type":"struct","fields":["#,
    r#"{"name":"path","type":"string","nullable":false,"metadata":{"description":"file"}},"#,
    r#"{"name":"tags","type":{"type":"map","keyType":"string","valueType":"string","#,
    r#""valueContainsNull":true},"nullable":true,"metadata":{}}]},"#,
    r#""nullable":true,"metadata":{}}]}"#,
);

fn typed_multipart_checkpoint_build(with_schema: bool) {
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
        checkpoint_schema: with_schema.then(|| slice(CHECKPOINT_SCHEMA)).into(),
        checksum: none_string(),
        tags: none_map(),
        v2_checkpoint: std::ptr::null(),
    };
    let expected = LastCheckpointHint::from_parts(
        0,
        2,
        Some(2),
        None,
        None,
        with_schema.then(|| CHECKPOINT_SCHEMA.to_string()),
        None,
        None,
        None,
    )
    .unwrap();
    assert_typed_checkpoint_build(
        &log_paths,
        test_protocol(),
        &checkpoint,
        &[PART_1, PART_2],
        &expected,
    );
}

fn typed_v2_checkpoint_build(with_schema: bool) {
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
    let tag = [FfiStringMapEntry {
        key: slice("tag"),
        value: slice("value"),
    }];
    let tags = || {
        OptionalValue::Some(FfiStringMap {
            ptr: tag.as_ptr(),
            len: tag.len(),
        })
    };
    let sidecars = [
        FfiSidecar {
            path: slice("sidecar-1.parquet"),
            size_in_bytes: 42,
            modification_time: 123,
            tags: none_map(),
        },
        FfiSidecar {
            path: slice("sidecar-2.parquet"),
            size_in_bytes: 84,
            modification_time: 456,
            tags: tags(),
        },
    ];
    let checkpoint_metadata = FfiCheckpointMetadata {
        version: 0,
        tags: tags(),
    };
    let metadata = test_metadata();
    let transactions = [
        FfiSetTransaction {
            app_id: slice("first"),
            version: 7,
            last_updated: none_i64(),
        },
        FfiSetTransaction {
            app_id: slice("second"),
            version: 11,
            last_updated: OptionalValue::Some(29),
        },
    ];
    let domains = [
        FfiDomainMetadata {
            domain: slice("first.domain"),
            configuration: slice("one"),
            removed: false,
        },
        FfiDomainMetadata {
            domain: slice("second.domain"),
            configuration: slice("two"),
            removed: true,
        },
    ];
    let actions = [
        FfiCheckpointNonFileAction::CheckpointMetadata(&checkpoint_metadata),
        FfiCheckpointNonFileAction::Metadata(&metadata),
        FfiCheckpointNonFileAction::Protocol(&protocol),
        FfiCheckpointNonFileAction::Transaction(&transactions[0]),
        FfiCheckpointNonFileAction::Transaction(&transactions[1]),
        FfiCheckpointNonFileAction::DomainMetadata(&domains[0]),
        FfiCheckpointNonFileAction::DomainMetadata(&domains[1]),
    ];
    let v2 = FfiLastCheckpointV2 {
        path: slice(CHECKPOINT),
        size_in_bytes: none_i64(),
        modification_time: none_i64(),
        sidecar_files: OptionalValue::Some(FfiSidecarArray {
            ptr: sidecars.as_ptr(),
            len: sidecars.len(),
        }),
        non_file_actions: OptionalValue::Some(FfiCheckpointNonFileActionArray {
            ptr: actions.as_ptr(),
            len: actions.len(),
        }),
    };
    let checkpoint = FfiLastCheckpoint {
        version: 0,
        size: 9,
        parts: OptionalValue::None,
        size_in_bytes: none_i64(),
        num_of_add_files: none_i64(),
        checkpoint_schema: with_schema.then(|| slice(CHECKPOINT_SCHEMA)).into(),
        checksum: none_string(),
        tags: none_map(),
        v2_checkpoint: &v2,
    };
    let expected = LastCheckpointHint::from_parts(
        0,
        9,
        None,
        None,
        None,
        with_schema.then(|| CHECKPOINT_SCHEMA.to_string()),
        None,
        None,
        Some(LastCheckpointV2::from_parts(
            CHECKPOINT.to_string(),
            None,
            None,
            Some(
                sidecars
                    .iter()
                    .map(|value| unsafe { value.try_to_kernel() }.unwrap())
                    .collect(),
            ),
            Some(
                actions
                    .iter()
                    .map(|value| unsafe { value.try_to_kernel() }.unwrap())
                    .collect(),
            ),
        )),
    )
    .unwrap();
    let log_paths = [FfiLogPath::new(slice(CHECKPOINT_URL), 1, 1)];
    assert_typed_checkpoint_build(&log_paths, protocol, &checkpoint, &[CHECKPOINT], &expected);
}

#[rstest::rstest]
#[case::multipart_v1(typed_multipart_checkpoint_build)]
#[case::uuid_v2(typed_v2_checkpoint_build)]
fn aggregate_checkpoint_build_preserves_identity_and_reconstructed_state(
    #[case] run_case: fn(bool),
    #[values(false, true)] with_schema: bool,
) {
    run_case(with_schema);
}

#[test]
fn aggregate_with_reports_unsupported_for_existing_snapshot_builder() {
    let engine = test_engine();
    let initial_builder = unsafe { with_minimal_hint(test_builder(&engine)) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(initial_builder)) };

    let update_builder = unsafe {
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
    let result = unsafe { snapshot_builder_with_snapshot_hint(update_builder, &hint) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::UnsupportedError,
        "builders created by get_snapshot_builder_from",
    );

    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}

#[test]
fn build_rejects_internally_supplied_hint_for_existing_snapshot_builder() {
    let engine = test_engine();
    let initial_builder = unsafe { with_minimal_hint(test_builder(&engine)) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(initial_builder)) };

    let mut update_builder = unsafe { with_minimal_hint(test_builder(&engine)) };
    unsafe { update_builder.as_mut() }.source =
        FfiSnapshotBuilderSource::ExistingSnapshot(unsafe { snapshot.clone_as_arc() });

    let result = unsafe { snapshot_builder_build(update_builder) };
    assert_extern_result_error_contains(
        result,
        FFIKernelError::InvalidSnapshotHint,
        "cannot be used with Snapshot::builder_from",
    );

    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}
