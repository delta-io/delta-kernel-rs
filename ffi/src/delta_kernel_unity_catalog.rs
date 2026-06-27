//! FFI hooks that enable constructing a UpdateTableClient.

use std::sync::Arc;

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
    Result as ApiResult, Update as ClientUpdate, UpdateTableClient,
    UpdateTableRequest as ClientUpdateTableRequest,
    UpdateTableResponse as ClientUpdateTableResponse,
};

use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult as _};
use crate::transaction::MutableCommitter;
use crate::{ExclusiveRustString, NullableCvoid};

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
    pub table_uri: KernelStringSlice,
    pub commit_info: OptionalValue<Commit>,
    pub latest_backfilled_version: OptionalValue<i64>,
    /// json serialized version of the metadata
    pub metadata: OptionalValue<KernelStringSlice>,
    /// json serialized version of the protocol
    pub protocol: OptionalValue<KernelStringSlice>,
}

/// The callback that will be called when the client wants to commit. Return `None` on success, or
/// `Some(error)` if an error occurred.
///
/// # Safety
///
/// The `Some(...)` payload MUST be a `Handle<ExclusiveRustString>` produced by kernel's
/// string-allocation FFI (e.g. `allocate_string`); kernel reclaims it via `Box::from_raw`. A
/// null pointer or a handle from any other allocator is undefined behavior.
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
    /// Translates from the typed `requirements + updates`
    /// `ClientUpdateTableRequest` into the FFI's flat C layout: `table_id` is
    /// extracted from `AssertTableUuid`, `commit_info` from `AddCommit`,
    /// `latest_backfilled_version` from `SetLatestBackfilledVersion`. The
    /// `table_uri` field of the C layout is unused under the API and is
    /// passed as the empty string. The `metadata` / `protocol` fields stay
    /// `None`; any ALTER TABLE update the flat layout cannot carry is an error.
    async fn update_table(
        &self,
        request: ClientUpdateTableRequest,
    ) -> ApiResult<ClientUpdateTableResponse> {
        // Pull the table id out of the requirements list. Kernel's UCCommitter
        // always emits one assert-table-uuid; if it isn't there, fail loudly.
        let table_id = request
            .table_uuid()
            .ok_or_else(|| {
                unity_catalog_delta_client_api::Error::UnsupportedOperation(
                    "FFI UC commit client requires an assert-table-uuid requirement".to_string(),
                )
            })?
            .to_string();
        let table_uri = String::new();

        // Error on any ALTER TABLE update the flat C layout cannot carry; dropping it would leave
        // the staged commit file and the catalog out of sync.
        let mut latest_backfilled: Option<i64> = None;
        for update in &request.updates {
            match update {
                ClientUpdate::AddCommit { .. } => {}
                ClientUpdate::SetLatestBackfilledVersion {
                    latest_published_version,
                } => {
                    latest_backfilled = Some(*latest_published_version);
                }
                ClientUpdate::SetProperties { .. }
                | ClientUpdate::RemoveProperties { .. }
                | ClientUpdate::SetColumns { .. }
                | ClientUpdate::SetTableComment { .. }
                | ClientUpdate::SetPartitionColumns { .. }
                | ClientUpdate::UpdateMetadataSnapshotVersion { .. }
                | ClientUpdate::SetProtocol { .. }
                | ClientUpdate::SetDomainMetadata { .. }
                | ClientUpdate::RemoveDomainMetadata { .. } => {
                    return Err(unity_catalog_delta_client_api::Error::UnsupportedOperation(
                        "FFI UC commit client cannot forward ALTER TABLE updates (protocol, \
                         properties, columns, or domain metadata)"
                            .to_string(),
                    ));
                }
            }
        }
        let add_commit = request.add_commit().cloned();
        // A backfill-only request carries no AddCommit, so there is no committed version to report.
        let committed_version = add_commit.as_ref().map(|c| c.version);

        // The file-name `String` must outlive the callback so the KernelStringSlice pointing into
        // it stays valid for the duration of the call.
        let send_request = |commit_info| -> ApiResult<ClientUpdateTableResponse> {
            let c_commit_request = CommitRequest {
                table_id: kernel_string_slice!(table_id),
                table_uri: kernel_string_slice!(table_uri),
                commit_info,
                latest_backfilled_version: latest_backfilled.into(),
                metadata: None.into(),
                protocol: None.into(),
            };

            match (self.commit_callback)(self.context, c_commit_request) {
                OptionalValue::Some(e) => {
                    let boxed_str = unsafe { e.into_inner() };
                    let s: String = *boxed_str;
                    Err(unity_catalog_delta_client_api::Error::Generic(s))
                }
                OptionalValue::None => Ok(ClientUpdateTableResponse {
                    committed_version,
                    etag: None,
                }),
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
            catalog_str,
            schema_str,
            table_name_str,
        ),
    });
    Ok(committer.into())
}

/// Free a committer obtained via get_uc_committer. Warning! Normally the value returned here will
/// be consumed when creating a transaction via [`crate::transaction::transaction_with_committer`]
/// and will NOT need to be freed.
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
        Commit as ClientCommit, Error as ApiError, Requirement as ApiRequirement,
        Update as ApiUpdate,
    };

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::{allocate_kernel_string, kernel_string_slice, OptionalValue};

    pub(crate) struct TestContext {
        pub(crate) commit_called: bool,
        pub(crate) last_commit_request: Option<(String, String)>,
        pub(crate) last_staged_filename: Option<String>,
        pub(crate) should_fail_commit: bool,
    }

    pub(crate) fn get_test_context(should_fail_commit: bool) -> NullableCvoid {
        let context = Box::new(TestContext {
            commit_called: false,
            last_commit_request: None,
            last_staged_filename: None,
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

    #[no_mangle]
    extern "C" fn test_commit_callback(
        context: NullableCvoid,
        request: CommitRequest,
    ) -> OptionalValue<Handle<ExclusiveRustString>> {
        let context = cast_test_context(context).unwrap();

        context.commit_called = true;

        let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };
        let table_uri = unsafe { String::try_from_slice(&request.table_uri).unwrap() };

        if let OptionalValue::Some(commit_info) = request.commit_info {
            let file_name = unsafe {
                crate::TryFromStringSlice::try_from_slice(&commit_info.file_name).unwrap()
            };
            context.last_staged_filename = Some(file_name);
        }
        context.last_commit_request = Some((table_id.clone(), table_uri.clone()));
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

    fn make_ffi_test_request(file_name: &str) -> ClientUpdateTableRequest {
        ClientUpdateTableRequest::new(
            "test_catalog",
            "test_schema",
            "test_table",
            vec![ApiRequirement::AssertTableUuid {
                uuid: "test_table_id".to_string(),
            }],
            vec![ApiUpdate::AddCommit {
                commit: ClientCommit {
                    version: 10,
                    timestamp: 2000000000,
                    file_name: file_name.to_string(),
                    file_size: 1024,
                    file_modification_timestamp: 2000000100,
                },
            }],
        )
    }

    #[tokio::test]
    async fn test_ffi_uc_commit_client_commit_success() {
        let context = get_test_context(false);

        let client = unsafe { get_uc_commit_client(context, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitClient> = unsafe { client.clone_as_arc() };

        let request = make_ffi_test_request("_staged_commits/00000000000000000010.uuid.json");

        let result: ApiResult<ClientUpdateTableResponse> = client_arc.update_table(request).await;

        assert!(result.is_ok());

        let context = recover_test_context(context).unwrap();

        assert!(context.commit_called);
        let (table_id, table_uri) = context.last_commit_request.unwrap();
        assert_eq!(table_id, "test_table_id");
        // table_uri is unused under the UC API; it comes through as empty.
        assert_eq!(table_uri, "");
        assert_eq!(
            context.last_staged_filename.unwrap(),
            "_staged_commits/00000000000000000010.uuid.json"
        );

        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commit_client_commit_failure() {
        let context = get_test_context(true);

        let client = unsafe { get_uc_commit_client(context, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitClient> = unsafe { client.clone_as_arc() };

        let request = make_ffi_test_request("00000000000000000010.uuid.json");

        let result: ApiResult<ClientUpdateTableResponse> = client_arc.update_table(request).await;

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
