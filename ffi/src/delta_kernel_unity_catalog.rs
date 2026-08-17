//! FFI hooks that enable constructing a UpdateTableClient.

use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::committer::Committer;
use delta_kernel::DeltaResult;
use delta_kernel_default_engine::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_ffi::handle::Handle;
use delta_kernel_ffi::{kernel_string_slice, KernelStringSlice, OptionalValue, TryFromStringSlice};
use delta_kernel_ffi_macros::handle_descriptor;
use delta_kernel_unity_catalog::UCCommitter;
use tracing::debug;
use unity_catalog_delta_client_api::{
    Result as ApiResult, TableIdentifier, UpdateTableClient,
    UpdateTableRequest as ClientUpdateTableRequest,
};

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult as _};
use crate::transaction::MutableCommitter;
use crate::{ExclusiveRustString, NullableCvoid, SharedMetadata, SharedProtocol};

/// Data representing a commit.
#[repr(C)]
pub struct Commit {
    pub version: i64,
    pub timestamp: i64,
    pub file_name: KernelStringSlice,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
}

/// Request to commit a new version to the table. It must include either a `commit_info` or
/// `latest_backfilled_version`.
#[repr(C)]
pub struct CommitRequest {
    pub table_id: KernelStringSlice,
    pub commit_info: OptionalValue<Commit>,
    pub latest_backfilled_version: OptionalValue<i64>,
    /// New table metadata. The callback owns this handle and must free it with `free_metadata`.
    pub metadata: OptionalValue<Handle<SharedMetadata>>,
    /// New table protocol. The callback owns this handle and must free it with `free_protocol`.
    pub protocol: OptionalValue<Handle<SharedProtocol>>,
}

/// The callback that will be called when the client wants to commit. Return `None` on success, or
/// `Some(error)` if an error occurred.
// Note, it doesn't make sense to return an ExternResult here because that can't hold the string
// error msg
pub type CCommit = extern "C" fn(
    context: NullableCvoid,
    request: CommitRequest,
) -> OptionalValue<Handle<ExclusiveRustString>>;

pub struct FfiUCCommitClient {
    context: NullableCvoid,
    commit_callback: CCommit,
}

// NullableCvoid is NOT `Send` by itself. Here we declare our struct to be Send as it's up to the
// caller to ensure they pass a thread safe pointer that remains valid
unsafe impl Send for FfiUCCommitClient {}
unsafe impl Sync for FfiUCCommitClient {}

impl UpdateTableClient for FfiUCCommitClient {
    /// Commit a new version to the table.
    ///
    /// `target` is unused: the FFI CommitRequest is addressed by table UUID (from the request's
    /// AssertTableUuid requirement), not the three-part identifier.
    async fn update_table(
        &self,
        _target: &TableIdentifier,
        request: ClientUpdateTableRequest,
    ) -> ApiResult<()> {
        let table_id = request
            .table_uuid()
            .ok_or_else(|| {
                unity_catalog_delta_client_api::Error::UnsupportedOperation(
                    "FFI UC commit client requires an assert-table-uuid requirement".to_string(),
                )
            })?
            .to_string();

        let latest_backfilled_version = request.latest_backfilled_version();
        let add_commit = request.staged_commit().cloned();
        let metadata = request
            .metadata()
            .cloned()
            .map(serde_json::from_value::<Metadata>)
            .transpose()
            .map_err(|e| {
                unity_catalog_delta_client_api::Error::Generic(format!(
                    "failed to decode metadata update for FFI UC commit client: {e}"
                ))
            })?
            .map(|metadata| Arc::new(metadata).into());
        let protocol = request
            .protocol()
            .cloned()
            .map(serde_json::from_value::<Protocol>)
            .transpose()
            .map_err(|e| {
                unity_catalog_delta_client_api::Error::Generic(format!(
                    "failed to decode protocol update for FFI UC commit client: {e}"
                ))
            })?
            .map(|protocol| Arc::new(protocol).into());

        // there is a subtle issue here where we need to ensure that the string we use to refer to
        // the commit_info.file_name stays in scope until _after_ the callback returns, so that the
        // KernelStringSlice remains valid. This means we can't get clever with
        // request.commit_info.map to build an Option<Commit>. Rather we just use a closure to hold
        // the common code and call it from a scope where the string remains valid until after the
        // closure finishes
        let send_request = |commit_info| -> ApiResult<()> {
            let c_commit_request = CommitRequest {
                table_id: kernel_string_slice!(table_id),
                commit_info,
                latest_backfilled_version: latest_backfilled_version.into(),
                metadata: metadata.into(),
                protocol: protocol.into(),
            };

            match (self.commit_callback)(self.context, c_commit_request) {
                OptionalValue::Some(e) => {
                    let boxed_str = unsafe { e.into_inner() }; // get the string back into Box<String>
                    let s: String = *boxed_str; // move back onto the stack
                    Err(unity_catalog_delta_client_api::Error::Generic(s))
                }
                OptionalValue::None => Ok(()),
            }
        };

        if let Some(client_commit_info) = add_commit {
            let file_name = client_commit_info.file_name;
            let commit_info = Some(Commit {
                version: client_commit_info.version,
                timestamp: client_commit_info.timestamp,
                file_name: kernel_string_slice!(file_name),
                file_size: client_commit_info.file_size,
                file_modification_timestamp: client_commit_info.file_modification_timestamp,
            });
            send_request(commit_info.into())
        } else {
            send_request(None.into())
        }
    }
}

#[handle_descriptor(target=FfiUCCommitClient, mutable=false, sized=true)]
pub struct SharedFfiUCCommitClient;

/// Get a commit client that will call the passed callbacks when it wants to make a commit. The
/// context will be passed back to the callback when called.
///
/// IMPORTANT: The pointer passed for the context MUST be thread-safe (i.e. be able to be sent
/// between threads safely) and MUST remain valid for as long as the client is used. It is valid to
/// pass NULL as the context.
///
/// # Safety
///
///  Caller is responsible for passing a valid pointer for the callback and a valid context pointer
#[no_mangle]
pub unsafe extern "C" fn get_uc_commit_client(
    context: NullableCvoid,
    commit_callback: CCommit,
) -> Handle<SharedFfiUCCommitClient> {
    Arc::new(FfiUCCommitClient {
        context,
        commit_callback,
    })
    .into()
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_uc_commit_client(commit_client: Handle<SharedFfiUCCommitClient>) {
    debug!("released uc commit client");
    commit_client.drop_handle();
}

// we need our own struct here because we want to override the calls to enter the tokio runtime
// before calling into the standard committer
struct FfiUCCommitter<C: UpdateTableClient> {
    inner: UCCommitter<C>,
}

impl<C: UpdateTableClient + 'static> Committer for FfiUCCommitter<C> {
    fn commit(
        &self,
        engine: &dyn delta_kernel::Engine,
        actions: Box<
            dyn Iterator<Item = DeltaResult<delta_kernel::FilteredEngineData>> + Send + '_,
        >,
        commit_metadata: delta_kernel::committer::CommitMetadata,
    ) -> DeltaResult<delta_kernel::committer::CommitResponse> {
        // We hold this guard until the end of the function so we stay in the tokio context until
        // we're done
        let _guard = engine
            .any_ref()
            .downcast_ref::<DefaultEngine<TokioMultiThreadExecutor>>()
            .map(|e| e.enter())
            .or_else(|| {
                engine
                    .any_ref()
                    .downcast_ref::<DefaultEngine<TokioBackgroundExecutor>>()
                    .map(|e| e.enter())
            })
            .ok_or_else(|| {
                delta_kernel::Error::generic(
                    "FFIUCCommitter can only be used with the default engine",
                )
            })?;
        self.inner.commit(engine, actions, commit_metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        self.inner.is_catalog_committer()
    }

    fn publish(
        &self,
        engine: &dyn delta_kernel::Engine,
        publish_metadata: delta_kernel::committer::PublishMetadata,
    ) -> DeltaResult<()> {
        self.inner.publish(engine, publish_metadata)
    }
}

/// Get a commit client that will call the passed callbacks when it wants to make a commit.
///
/// # Safety
///
///  Caller is responsible for passing a valid pointer to a SharedFfiUCCommitClient, obtained via
///  calling [`get_uc_commit_client`], valid KernelStringSlices for `table_id`, `catalog`,
///  `schema`, and `table_name`, and a valid error function pointer.
#[no_mangle]
pub unsafe extern "C" fn get_uc_committer(
    commit_client: Handle<SharedFfiUCCommitClient>,
    table_id: KernelStringSlice,
    catalog: KernelStringSlice,
    schema: KernelStringSlice,
    table_name: KernelStringSlice,
    error_fn: AllocateErrorFn,
) -> ExternResult<Handle<MutableCommitter>> {
    get_uc_committer_impl(commit_client, table_id, catalog, schema, table_name)
        .into_extern_result(&error_fn)
}

fn get_uc_committer_impl(
    commit_client: Handle<SharedFfiUCCommitClient>,
    table_id: KernelStringSlice,
    catalog: KernelStringSlice,
    schema: KernelStringSlice,
    table_name: KernelStringSlice,
) -> DeltaResult<Handle<MutableCommitter>> {
    let client: Arc<FfiUCCommitClient> = unsafe { commit_client.clone_as_arc() };
    let table_id_str: String = unsafe { TryFromStringSlice::try_from_slice(&table_id) }?;
    let catalog_str: String = unsafe { TryFromStringSlice::try_from_slice(&catalog) }?;
    let schema_str: String = unsafe { TryFromStringSlice::try_from_slice(&schema) }?;
    let table_name_str: String = unsafe { TryFromStringSlice::try_from_slice(&table_name) }?;
    let committer: Box<dyn Committer> = Box::new(FfiUCCommitter {
        inner: UCCommitter::new(
            client,
            table_id_str,
            TableIdentifier::new(catalog_str, schema_str, table_name_str),
        ),
    });
    Ok(committer.into())
}

/// Free a committer obtained via [`get_uc_committer`] that was not passed to a consuming API.
///
/// APIs that take ownership of the committer; `transaction_with_committer`,
/// `create_table_builder_build_with_committer`, and `snapshot_publish_with_committer`
/// consume the handle (do not free; it will not need to be freed afterward).
///
/// # Safety
///
/// Caller is responsible for passing a valid handle obtained via `get_uc_committer`
#[no_mangle]
pub unsafe extern "C" fn free_uc_committer(commit_client: Handle<MutableCommitter>) {
    debug!("released uc committer");
    commit_client.drop_handle();
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::Arc;

    use unity_catalog_delta_client_api::{
        Commit as ClientCommit, DeltaTableRequirement as ApiRequirement,
        DeltaTableUpdate as ApiUpdate, Error as ApiError,
    };

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::{
        allocate_kernel_string, free_metadata, free_protocol, kernel_string_slice, visit_metadata,
        visit_protocol, OptionalValue,
    };

    #[derive(Debug, Default, PartialEq, Eq)]
    pub(crate) struct ProtocolVisitState {
        min_reader: i32,
        min_writer: i32,
        reader_features: Vec<String>,
        writer_features: Vec<String>,
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    pub(crate) struct MetadataVisitState {
        id: Option<String>,
        name: Option<String>,
        description: Option<String>,
        format_provider: Option<String>,
        has_created_time: bool,
        created_time_ms: i64,
    }

    pub(crate) struct TestContext {
        pub(crate) commit_called: bool,
        pub(crate) last_commit_table_id: Option<String>,
        pub(crate) last_staged_filename: Option<String>,
        pub(crate) last_metadata: Option<MetadataVisitState>,
        pub(crate) last_protocol: Option<ProtocolVisitState>,
        pub(crate) should_fail_commit: bool,
    }

    pub(crate) fn get_test_context(should_fail_commit: bool) -> NullableCvoid {
        let context = Box::new(TestContext {
            commit_called: false,
            last_commit_table_id: None,
            last_staged_filename: None,
            last_metadata: None,
            last_protocol: None,
            should_fail_commit,
        });
        NonNull::new(Box::into_raw(context) as *mut c_void)
    }

    // take back ownership of the context. be aware that you therefore cannot call this twice with
    // the same context pointer
    pub(crate) fn recover_test_context(context: NullableCvoid) -> Option<Box<TestContext>> {
        context.map(|context| unsafe { Box::from_raw(context.as_ptr() as *mut TestContext) })
    }

    // get the context without taking ownership
    pub(crate) fn cast_test_context<'a>(context: NullableCvoid) -> Option<&'a mut TestContext> {
        context.map(|ptr| unsafe { &mut *(ptr.as_ptr() as *mut TestContext) })
    }

    extern "C" fn protocol_version_cb(context: NullableCvoid, min_reader: i32, min_writer: i32) {
        let state = unsafe { &mut *(context.unwrap().as_ptr() as *mut ProtocolVisitState) };
        state.min_reader = min_reader;
        state.min_writer = min_writer;
    }

    extern "C" fn protocol_feature_cb(
        context: NullableCvoid,
        is_reader: bool,
        feature: KernelStringSlice,
    ) {
        let state = unsafe { &mut *(context.unwrap().as_ptr() as *mut ProtocolVisitState) };
        let feature = unsafe { crate::TryFromStringSlice::try_from_slice(&feature).unwrap() };
        if is_reader {
            state.reader_features.push(feature);
        } else {
            state.writer_features.push(feature);
        }
    }

    extern "C" fn metadata_visit_cb(
        context: NullableCvoid,
        id: KernelStringSlice,
        name: OptionalValue<KernelStringSlice>,
        description: OptionalValue<KernelStringSlice>,
        format_provider: KernelStringSlice,
        has_created_time: bool,
        created_time_ms: i64,
    ) {
        let state = unsafe { &mut *(context.unwrap().as_ptr() as *mut MetadataVisitState) };
        let string_from_slice =
            |slice| unsafe { crate::TryFromStringSlice::try_from_slice(&slice).unwrap() };
        state.id = Some(string_from_slice(id));
        state.name = match name {
            OptionalValue::Some(name) => Some(string_from_slice(name)),
            OptionalValue::None => None,
        };
        state.description = match description {
            OptionalValue::Some(description) => Some(string_from_slice(description)),
            OptionalValue::None => None,
        };
        state.format_provider = Some(string_from_slice(format_provider));
        state.has_created_time = has_created_time;
        state.created_time_ms = created_time_ms;
    }

    fn collect_protocol(protocol: Handle<SharedProtocol>) -> ProtocolVisitState {
        let mut state = ProtocolVisitState::default();
        let context = NonNull::new(&mut state as *mut ProtocolVisitState as *mut c_void);
        unsafe {
            visit_protocol(
                protocol.shallow_copy(),
                context,
                protocol_version_cb,
                protocol_feature_cb,
            )
        };
        unsafe { free_protocol(protocol) };
        state
    }

    fn collect_metadata(metadata: Handle<SharedMetadata>) -> MetadataVisitState {
        let mut state = MetadataVisitState::default();
        let context = NonNull::new(&mut state as *mut MetadataVisitState as *mut c_void);
        unsafe { visit_metadata(metadata.shallow_copy(), context, metadata_visit_cb) };
        unsafe { free_metadata(metadata) };
        state
    }

    #[no_mangle]
    extern "C" fn test_commit_callback(
        context: NullableCvoid,
        request: CommitRequest,
    ) -> OptionalValue<Handle<ExclusiveRustString>> {
        let context = cast_test_context(context).unwrap();

        context.commit_called = true;

        let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };

        if let OptionalValue::Some(commit_info) = request.commit_info {
            let file_name = unsafe {
                crate::TryFromStringSlice::try_from_slice(&commit_info.file_name).unwrap()
            };
            context.last_staged_filename = Some(file_name);
        }
        if let OptionalValue::Some(protocol) = request.protocol {
            context.last_protocol = Some(collect_protocol(protocol));
        }
        if let OptionalValue::Some(metadata) = request.metadata {
            context.last_metadata = Some(collect_metadata(metadata));
        }
        context.last_commit_table_id = Some(table_id.clone());
        if context.should_fail_commit {
            let error_msg = "Test commit failure";
            let error_str = unsafe {
                ok_or_panic(allocate_kernel_string(
                    kernel_string_slice!(error_msg),
                    allocate_err,
                ))
            };
            OptionalValue::Some(error_str)
        } else {
            OptionalValue::None
        }
    }

    #[test]
    fn test_get_uc_commit_client() {
        let client = unsafe { get_uc_commit_client(None, test_commit_callback) };

        let _client_ref: Arc<FfiUCCommitClient> = unsafe { client.clone_as_arc() };
        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commit_client_commit_success() {
        let context = get_test_context(false);

        let client = unsafe { get_uc_commit_client(context, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitClient> = unsafe { client.clone_as_arc() };

        let request = ClientUpdateTableRequest::new(
            vec![ApiRequirement::AssertTableUuid {
                uuid: "test_table_id".to_string(),
            }],
            vec![
                ApiUpdate::AddCommit {
                    commit: ClientCommit {
                        version: 10,
                        timestamp: 2000000000,
                        file_name: "_staged_commits/00000000000000000010.uuid.json".to_string(),
                        file_size: 1024,
                        file_modification_timestamp: 2000000100,
                    },
                },
                ApiUpdate::UpdateProtocol {
                    protocol: serde_json::json!({
                        "minReaderVersion": 3,
                        "minWriterVersion": 7,
                        "readerFeatures": ["catalogManaged"],
                        "writerFeatures": ["catalogManaged", "inCommitTimestamp"],
                    }),
                },
                ApiUpdate::UpdateMetadata {
                    metadata: serde_json::json!({
                        "id": "test-id",
                        "format": {"provider": "parquet", "options": {}},
                        "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                        "partitionColumns": [],
                        "createdTime": 1234567890,
                        "configuration": {"delta.enableInCommitTimestamps": "true"},
                    }),
                },
            ],
        )
        .unwrap();

        let target = TableIdentifier::new("test_catalog", "test_schema", "test_table");
        let result: ApiResult<()> = client_arc.update_table(&target, request).await;

        assert!(result.is_ok());

        let context = recover_test_context(context).unwrap();

        assert!(context.commit_called);
        let table_id = context.last_commit_table_id.unwrap();
        assert_eq!(table_id, "test_table_id");
        assert_eq!(
            context.last_staged_filename.unwrap(),
            "_staged_commits/00000000000000000010.uuid.json"
        );

        let protocol = context.last_protocol.unwrap();
        assert_eq!(protocol.min_reader, 3);
        assert_eq!(protocol.min_writer, 7);
        assert_eq!(protocol.reader_features, vec!["catalogManaged"]);
        assert_eq!(
            protocol.writer_features,
            vec!["catalogManaged", "inCommitTimestamp"]
        );

        let metadata = context.last_metadata.unwrap();
        assert_eq!(metadata.id.as_deref(), Some("test-id"));
        assert_eq!(metadata.format_provider.as_deref(), Some("parquet"));
        assert_eq!(metadata.created_time_ms, 1234567890);

        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commit_client_commit_failure() {
        let context = get_test_context(true);

        let client = unsafe { get_uc_commit_client(context, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitClient> = unsafe { client.clone_as_arc() };

        let request = ClientUpdateTableRequest::new(
            vec![ApiRequirement::AssertTableUuid {
                uuid: "test_table_id".to_string(),
            }],
            vec![ApiUpdate::AddCommit {
                commit: ClientCommit {
                    version: 10,
                    timestamp: 2000000000,
                    file_name: "00000000000000000010.uuid.json".to_string(),
                    file_size: 1024,
                    file_modification_timestamp: 2000000100,
                },
            }],
        )
        .unwrap();

        let target = TableIdentifier::new("test_catalog", "test_schema", "test_table");
        let result: ApiResult<()> = client_arc.update_table(&target, request).await;

        assert!(result.is_err());

        let context = recover_test_context(context).unwrap();

        assert!(context.commit_called);

        let error = result.unwrap_err();
        assert!(matches!(error, ApiError::Generic(_)));
        if let unity_catalog_delta_client_api::Error::Generic(msg) = error {
            assert_eq!(msg, "Test commit failure");
        }

        unsafe { free_uc_commit_client(client) };
    }

    #[test]
    fn test_get_uc_committer() {
        let client = unsafe { get_uc_commit_client(None, test_commit_callback) };

        let table_id = "test_table_id";
        let catalog = "test_catalog";
        let schema = "test_schema";
        let table_name = "test_table";
        let committer = unsafe {
            ok_or_panic(get_uc_committer(
                client.shallow_copy(),
                kernel_string_slice!(table_id),
                kernel_string_slice!(catalog),
                kernel_string_slice!(schema),
                kernel_string_slice!(table_name),
                allocate_err,
            ))
        };

        unsafe {
            free_uc_commit_client(client);
            free_uc_committer(committer);
        }
    }
}
