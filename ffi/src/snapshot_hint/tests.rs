use std::error::Error as _;
use std::sync::Arc;

use delta_kernel::actions::{CheckpointMetadata, Sidecar};
use delta_kernel::last_checkpoint_hint::{HintAction, LastCheckpointHint, LastCheckpointV2};
use delta_kernel::object_store::memory::InMemory;
#[cfg(feature = "declarative-plans")]
use delta_kernel::plans::proto::operation as proto_op;
use delta_kernel::snapshot::SnapshotState;
use delta_kernel_default_engine::DefaultEngineBuilder;
#[cfg(feature = "declarative-plans")]
use prost::Message as _;

use super::*;
use crate::delta_types::*;
#[cfg(feature = "declarative-plans")]
use crate::error::EngineExecResult;
use crate::error::KernelError;
use crate::ffi_test_utils::{
    allocate_err, assert_extern_result_error_contains, assert_extern_result_error_with_message,
    ok_or_panic,
};
use crate::log_path::FfiLogPath;
#[cfg(feature = "declarative-plans")]
use crate::plans::result::CPlanResult;
#[cfg(feature = "declarative-plans")]
use crate::plans::{get_plan_based_engine, get_plan_executor};
use crate::{
    engine_to_handle, free_engine, free_snapshot, free_snapshot_builder, get_snapshot_builder,
    get_snapshot_builder_from, snapshot_builder_build, snapshot_builder_set_version, FfiFileStats,
    KernelI64Slice, KernelStringSlice, OptionalValue, SharedExternEngine,
};
#[cfg(feature = "declarative-plans")]
use crate::{KernelBytesSlice, NullableCvoid};

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
    engine_to_handle(
        Arc::new(DefaultEngineBuilder::new(Arc::new(InMemory::new())).build()),
        allocate_err,
    )
}

#[cfg(feature = "declarative-plans")]
extern "C" fn no_plan_execution(
    _context: NullableCvoid,
    _plan_proto: KernelBytesSlice,
    _out: *mut EngineExecResult<CPlanResult>,
) {
    unreachable!("planning a hinted commit must not execute the plan");
}

#[cfg(feature = "declarative-plans")]
unsafe fn plan_based_engine(fallback: &Handle<SharedExternEngine>) -> Handle<SharedExternEngine> {
    let executor = unsafe { get_plan_executor(None, no_plan_execution) };
    unsafe {
        get_plan_based_engine(
            executor,
            OptionalValue::Some(fallback.shallow_copy()),
            allocate_err,
        )
    }
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

#[test]
fn externalized_core_borrows_validated_connector_state() {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
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
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let owned = unsafe { snapshot.as_ref() };
    assert_eq!(
        SnapshotState::table_root(owned).as_str(),
        "memory:///hinted-table/"
    );
    assert_eq!(SnapshotState::version(owned), 0);
    assert!(!SnapshotState::is_latest(owned));
    assert_eq!(
        SnapshotState::protocol(owned).unwrap().min_reader_version(),
        1
    );
    assert_eq!(SnapshotState::metadata(owned).unwrap().id(), "table-id");
    assert!(SnapshotState::logical_schema(owned).is_ok());
    assert!(SnapshotState::last_checkpoint(owned).unwrap().is_none());
    assert!(SnapshotState::crc(owned).unwrap().is_none());
    let mut path_count = 0;
    SnapshotState::visit_log_paths(owned, &mut |batch| {
        path_count += batch.len();
        Ok(())
    })
    .unwrap();
    assert_eq!(path_count, 1);
    let wrong_freshness = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        FfiSnapshotHintFreshness::Latest,
    );
    let rejected = unsafe {
        snapshot_externalize_core(
            snapshot.shallow_copy(),
            &wrong_freshness,
            42,
            engine.shallow_copy(),
        )
    };
    assert_extern_result_error_contains(rejected, KernelError::InvalidSnapshotHint, "freshness");
    let mut different_state = test_snapshot_hint(
        std::slice::from_ref(&log_path),
        0,
        FfiSnapshotHintFreshness::Unverified,
    );
    different_state.metadata.id = slice("different-table-id");
    let rejected = unsafe {
        snapshot_externalize_core(
            snapshot.shallow_copy(),
            &different_state,
            42,
            engine.shallow_copy(),
        )
    };
    assert_extern_result_error_contains(rejected, KernelError::InvalidSnapshotHint, "differs");
    let changed_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.checkpoint.parquet"),
        2,
        1,
    );
    let different_files = test_snapshot_hint(
        std::slice::from_ref(&changed_path),
        0,
        FfiSnapshotHintFreshness::Unverified,
    );
    let rejected = unsafe {
        snapshot_externalize_core(
            snapshot.shallow_copy(),
            &different_files,
            42,
            engine.shallow_copy(),
        )
    };
    assert_extern_result_error_contains(rejected, KernelError::InvalidSnapshotHint, "differs");
    assert_eq!(unsafe { snapshot.as_ref() }.version(), 0);
    let core = unsafe {
        ok_or_panic(snapshot_externalize_core(
            snapshot.shallow_copy(),
            &hint,
            42,
            engine.shallow_copy(),
        ))
    };
    unsafe { free_snapshot(snapshot) };

    assert_eq!(unsafe { snapshot_core_version(core.shallow_copy()) }, 0);
    let schema = unsafe {
        ok_or_panic(snapshot_core_logical_schema(
            core.shallow_copy(),
            &hint,
            42,
            engine.shallow_copy(),
        ))
    };
    unsafe { crate::free_schema(schema) };
    let protocol = unsafe {
        ok_or_panic(snapshot_core_get_protocol(
            core.shallow_copy(),
            &hint,
            42,
            engine.shallow_copy(),
        ))
    };
    unsafe { crate::free_protocol(protocol) };
    let metadata = unsafe {
        ok_or_panic(snapshot_core_get_metadata(
            core.shallow_copy(),
            &hint,
            42,
            engine.shallow_copy(),
        ))
    };
    unsafe { crate::free_metadata(metadata) };

    // Empty schemas can still be externalized and read through getters, but not scanned.
    #[cfg(feature = "declarative-plans")]
    {
        let plan_engine = unsafe { plan_based_engine(&engine) };
        let rejected = unsafe {
            snapshot_core_declarative_metadata_plan(
                core.shallow_copy(),
                &hint,
                42,
                plan_engine.shallow_copy(),
            )
        };
        assert_extern_result_error_contains(rejected, KernelError::GenericError, "empty schema");
        unsafe { free_engine(plan_engine) };
    }

    let wrong_generation = unsafe {
        snapshot_core_logical_schema(core.shallow_copy(), &hint, 43, engine.shallow_copy())
    };
    assert_extern_result_error_contains(
        wrong_generation,
        KernelError::InvalidSnapshotHint,
        "generation",
    );
    unsafe {
        free_snapshot_core(core);
        free_engine(engine);
    }
}

#[cfg(feature = "declarative-plans")]
#[rstest::rstest]
#[case(FfiSnapshotHintFreshness::Unverified)]
#[case(FfiSnapshotHintFreshness::Latest)]
fn externalized_core_builds_declarative_plan_from_scoped_host_state(
    #[case] freshness: FfiSnapshotHintFreshness,
    #[values(false, true)] partitioned: bool,
) {
    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let log_path = FfiLogPath::new(
        slice("memory:///hinted-table/_delta_log/00000000000000000000.json"),
        1,
        1,
    );
    let mut hint = test_snapshot_hint(std::slice::from_ref(&log_path), 0, freshness);
    hint.metadata.schema_string = slice(
        r#"{"type":"struct","fields":[{"name":"value","type":"long","nullable":true,"metadata":{}},{"name":"nested","type":{"type":"struct","fields":[{"name":"child","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}"#,
    );
    let partition_columns = [slice("value")];
    if partitioned {
        hint.metadata.partition_columns = unsafe { FfiStringArray::new_unsafe(&partition_columns) };
    }
    unsafe { ok_or_panic(snapshot_builder_set_snapshot_hint(&mut builder, &hint)) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(builder)) };
    let core = unsafe {
        ok_or_panic(snapshot_externalize_core(
            snapshot.shallow_copy(),
            &hint,
            42,
            engine.shallow_copy(),
        ))
    };
    let plan_engine = unsafe { plan_based_engine(&engine) };
    let inner_engine = unsafe { plan_engine.as_ref() }.engine();
    let native_snapshot = unsafe { snapshot.into_inner() };
    let native_plan = native_snapshot
        .clone()
        .scan_builder()
        .build()
        .unwrap()
        .declarative_metadata_scan_plan(inner_engine.as_ref())
        .unwrap()
        .expect("expected a native plan for a hinted commit");
    let native_bytes = delta_kernel::Operation::QueryPlan(native_plan).to_proto_bytes();

    // Fail immediately if the narrow planner asks for schema, metadata, protocol, or CRC.
    let validation = delta_kernel::scan::ValidatedMetadataScan::try_new(&native_snapshot).unwrap();
    let host_state = BorrowedSnapshotState {
        hint: &hint,
        table_root: native_snapshot.table_root(),
    };
    let narrow_plan = validation
        .plan(&LogOnlyState(&host_state), inner_engine.as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(
        delta_kernel::Operation::QueryPlan(narrow_plan).to_proto_bytes(),
        native_bytes
    );
    let rejected = unsafe {
        snapshot_core_declarative_metadata_plan(
            core.shallow_copy(),
            &hint,
            43,
            plan_engine.shallow_copy(),
        )
    };
    assert_extern_result_error_contains(rejected, KernelError::InvalidSnapshotHint, "generation");

    let result = unsafe {
        snapshot_core_declarative_metadata_plan(
            core.shallow_copy(),
            &hint,
            42,
            plan_engine.shallow_copy(),
        )
    };
    let bytes = match ok_or_panic(result) {
        OptionalValue::Some(bytes) => unsafe { bytes.into_vec() },
        OptionalValue::None => panic!("expected a plan for a hinted commit"),
    };
    assert_eq!(bytes, native_bytes);
    let operation = proto_op::Operation::decode(bytes.as_slice()).unwrap();
    assert!(matches!(
        operation.op,
        Some(proto_op::operation::Op::QueryPlan(_))
    ));

    unsafe {
        free_snapshot_core(core);
        free_engine(plan_engine);
        free_engine(engine);
    }
}

#[cfg(feature = "declarative-plans")]
struct LogOnlyState<'a>(&'a dyn SnapshotState);

#[cfg(feature = "declarative-plans")]
impl SnapshotState for LogOnlyState<'_> {
    fn table_root(&self) -> &url::Url {
        self.0.table_root()
    }
    fn version(&self) -> delta_kernel::Version {
        self.0.version()
    }
    fn is_latest(&self) -> bool {
        self.0.is_latest()
    }
    fn protocol(&self) -> delta_kernel::DeltaResult<delta_kernel::actions::Protocol> {
        panic!("metadata planning must not decode protocol")
    }
    fn metadata(&self) -> delta_kernel::DeltaResult<delta_kernel::actions::Metadata> {
        panic!("metadata planning must not decode metadata")
    }
    fn logical_schema(&self) -> delta_kernel::DeltaResult<delta_kernel::schema::SchemaRef> {
        panic!("metadata planning must not materialize the table schema")
    }
    fn crc(&self) -> delta_kernel::DeltaResult<Option<Arc<delta_kernel::crc::Crc>>> {
        panic!("metadata planning must not decode CRC state")
    }
    fn last_checkpoint(&self) -> delta_kernel::DeltaResult<Option<LastCheckpointHint>> {
        self.0.last_checkpoint()
    }
    fn visit_log_paths(
        &self,
        visitor: &mut dyn FnMut(&[delta_kernel::LogPath]) -> delta_kernel::DeltaResult<()>,
    ) -> delta_kernel::DeltaResult<()> {
        self.0.visit_log_paths(visitor)
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
fn aggregate_setter_rejects_null_nonempty_log_path_array() {
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
    let result = unsafe { snapshot_builder_set_snapshot_hint(&mut builder, &hint) };
    assert_extern_result_error_contains(
        result,
        KernelError::InvalidSnapshotHint,
        "supplied log paths are invalid",
    );

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
fn aggregate_setter_builds_latest_snapshot_from_rich_crc() {
    const PARTITIONED_SCHEMA: &str = concat!(
        r#"{"type":"struct","fields":[{"name":"p","type":"string","nullable":true,"#,
        r#""metadata":{}}]}"#,
    );

    let engine = test_engine();
    let mut builder = test_builder(&engine);
    let partition_columns = [slice("p")];
    let metadata = || FfiMetadata {
        id: slice("table-id"),
        name: none_string(),
        description: none_string(),
        format_provider: slice("parquet"),
        format_options: empty_map(),
        schema_string: slice(PARTITIONED_SCHEMA),
        partition_columns: FfiStringArray {
            ptr: partition_columns.as_ptr(),
            len: partition_columns.len(),
        },
        created_time: none_i64(),
        configuration: empty_map(),
    };
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
    let partition_value = FfiStringMapEntry {
        key: slice("p"),
        value: slice("one"),
    };
    let tag = FfiNullableStringMapEntry {
        key: slice("optional"),
        value: OptionalValue::None,
    };
    let add = FfiAdd {
        path: slice("p=one/part-00000.parquet"),
        partition_values: FfiStringMap {
            ptr: &partition_value,
            len: 1,
        },
        size: 17,
        modification_time: 19,
        data_change: true,
        stats: OptionalValue::Some(slice(r#"{"numRecords":23}"#)),
        tags: OptionalValue::Some(FfiNullableStringMap { ptr: &tag, len: 1 }),
        deletion_vector: std::ptr::null(),
        base_row_id: OptionalValue::None,
        default_row_commit_version: OptionalValue::None,
        clustering_provider: OptionalValue::None,
    };
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
    let counts = [1, 0];
    let total_bytes = [17, 0];
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
    let deleted_record_counts = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0];
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
                num_files: 1,
                table_size_bytes: 17,
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
        all_files: OptionalValue::Some(FfiAddArray { ptr: &add, len: 1 }),
        num_deleted_records: OptionalValue::Some(0),
        num_deletion_vectors: OptionalValue::Some(0),
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
        1
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

#[test]
fn build_rejects_internally_supplied_hint_for_existing_snapshot_builder() {
    let engine = test_engine();
    let mut initial_builder = test_builder(&engine);
    unsafe { set_minimal_hint(&mut initial_builder) };
    let snapshot = unsafe { ok_or_panic(snapshot_builder_build(initial_builder)) };

    let mut update_builder = test_builder(&engine);
    unsafe { set_minimal_hint(&mut update_builder) };
    unsafe { update_builder.as_mut() }.source =
        FfiSnapshotBuilderSource::ExistingSnapshot(unsafe { snapshot.clone_as_arc() });

    let result = unsafe { snapshot_builder_build(update_builder) };
    assert_extern_result_error_contains(
        result,
        KernelError::InvalidSnapshotHint,
        "cannot be used with Snapshot::builder_from",
    );

    unsafe {
        free_snapshot(snapshot);
        free_engine(engine);
    }
}
