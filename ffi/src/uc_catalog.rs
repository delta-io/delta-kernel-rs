//! FFI hooks that enable constructing a UCCommitClient.

use crate::error::{ExternResult, IntoExternResult as _};
use crate::{error::AllocateErrorFn, transaction::MutableCommitter};
use crate::{ExclusiveRustString, NullableCvoid};
use std::sync::Arc;

use delta_kernel::committer::Committer;
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::DeltaResult;
use delta_kernel_ffi::{
    handle::Handle, kernel_string_slice, KernelStringSlice, OptionalValue, TryFromStringSlice,
};
use delta_kernel_ffi_macros::handle_descriptor;
use uc_catalog::UCCommitter;

use uc_client::models::CommitRequest as ClientCommitRequest;
use uc_client::UCCommitClient;

use tracing::debug;

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
/// `Some("error description")` if an error occured.
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

impl uc_client::UCCommitClient for FfiUCCommitClient {
    /// Commit a new version to the table.
    async fn commit(&self, request: ClientCommitRequest) -> uc_client::Result<()> {
        let table_id = request.table_id;
        let table_uri = request.table_uri;

        // there is a subtle issue here where we need to ensure that the string we use to refer to
        // the commit_info.file_name stays in scope until _after_ the callback returns, so that the
        // KernelStringSlice remains valid. This means we can't get clever with
        // request.commit_info.map to build an Option<Commit>. Rather we just use a closure to hold
        // the common code and call it from a scope where the string remains valid until after the
        // closure finishes

        let send_request = |commit_info| -> uc_client::Result<()> {
            let c_commit_request = CommitRequest {
                table_id: kernel_string_slice!(table_id),
                table_uri: kernel_string_slice!(table_uri),
                commit_info,
                latest_backfilled_version: request.latest_backfilled_version.into(),
                metadata: None.into(),
                protocol: None.into(),
            };

            match (self.commit_callback)(self.context, c_commit_request) {
                OptionalValue::Some(e) => {
                    let boxed_str = unsafe { e.into_inner() }; // get the string back into Box<String>
                    let s: String = *boxed_str; // move back onto the stack
                    uc_client::Result::Err(uc_client::Error::Generic(s))
                }
                OptionalValue::None => uc_client::Result::Ok(()),
            }
        };

        if let Some(client_commit_info) = request.commit_info {
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
struct FfiUCCommitter<C: UCCommitClient> {
    inner: UCCommitter<C>,
}

impl<C: UCCommitClient + 'static> Committer for FfiUCCommitter<C> {
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
///  calling [`get_uc_commit_client`], a valid KernelStringSlice as the table_id, and a valid error
///  function pointer.
#[no_mangle]
pub unsafe extern "C" fn get_uc_committer(
    commit_client: Handle<SharedFfiUCCommitClient>,
    table_id: KernelStringSlice,
    error_fn: AllocateErrorFn,
) -> ExternResult<Handle<MutableCommitter>> {
    get_uc_committer_impl(commit_client, table_id).into_extern_result(&error_fn)
}

fn get_uc_committer_impl(
    commit_client: Handle<SharedFfiUCCommitClient>,
    table_id: KernelStringSlice,
) -> DeltaResult<Handle<MutableCommitter>> {
    let client: Arc<FfiUCCommitClient> = unsafe { commit_client.clone_as_arc() };
    let table_id_str: String = unsafe { TryFromStringSlice::try_from_slice(&table_id) }?;
    let committer: Box<dyn Committer> = Box::new(FfiUCCommitter {
        inner: UCCommitter::new(client, table_id_str),
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
mod tests {
    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::{allocate_kernel_string, kernel_string_slice, OptionalValue};
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use uc_client::models::Commit as ClientCommit;
    use uc_client::UCCommitClient;

    struct TestContext {
        commit_called: bool,
        last_commit_request: Option<(String, String)>,
        last_staged_filename: Option<String>,
        should_fail_commit: bool,
    }

    fn get_test_context(should_fail_commit: bool) -> NullableCvoid {
        let context = Box::new(TestContext {
            commit_called: false,
            last_commit_request: None,
            last_staged_filename: None,
            should_fail_commit
        });
        NonNull::new(Box::into_raw(context) as *mut c_void)
    }

    // take back ownership of the context. be aware that you therefore cannot call this twice with
    // the same context pointer
    fn recover_test_context(context: NullableCvoid) -> Option<Box<TestContext>> {
        context.map(|context| unsafe { Box::from_raw(context.as_ptr() as *mut TestContext) })
    }

    // get the context without taking ownership
    fn cast_test_context<'a>(context: NullableCvoid) -> Option<&'a mut TestContext> {
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

    #[tokio::test]
    async fn test_ffi_uc_commit_client_commit_success() {
        let context = get_test_context(false);

        let client = unsafe { get_uc_commit_client(context, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitClient> = unsafe { client.clone_as_arc() };

        let request = ClientCommitRequest {
            table_id: "test_table_id".to_string(),
            table_uri: "s3://bucket/path".to_string(),
            commit_info: Some(ClientCommit {
                version: 10,
                timestamp: 2000000000,
                file_name: "_staged_commits/00000000000000000010.uuid.json".to_string(),
                file_size: 1024,
                file_modification_timestamp: 2000000100,
            }),
            latest_backfilled_version: None,
            metadata: None,
            protocol: None,
        };

        let result: uc_client::Result<()> = client_arc.commit(request).await;

        assert!(result.is_ok());

        let context = recover_test_context(context).unwrap();

        assert!(context.commit_called);
        let (table_id, table_uri) = context.last_commit_request.unwrap();
        assert_eq!(table_id, "test_table_id");
        assert_eq!(table_uri, "s3://bucket/path");
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

        let request = ClientCommitRequest {
            table_id: "test_table_id".to_string(),
            table_uri: "s3://bucket/path".to_string(),
            commit_info: Some(ClientCommit {
                version: 10,
                timestamp: 2000000000,
                file_name: "00000000000000000010.uuid.json".to_string(),
                file_size: 1024,
                file_modification_timestamp: 2000000100,
            }),
            latest_backfilled_version: None,
            metadata: None,
            protocol: None,
        };

        let result: uc_client::Result<()> = client_arc.commit(request).await;

        assert!(result.is_err());

        let context = recover_test_context(context).unwrap();

        assert!(context.commit_called);

        let error = result.unwrap_err();
        assert!(matches!(error, uc_client::Error::Generic(_)));
        if let uc_client::Error::Generic(msg) = error {
            assert_eq!(msg, "Test commit failure");
        }

        unsafe { free_uc_commit_client(client) };
    }

    #[test]
    fn test_get_uc_committer() {
        let client = unsafe { get_uc_commit_client(None, test_commit_callback) };

        let table_id = "test_table_id";
        let committer = unsafe {
            ok_or_panic(get_uc_committer(
                client.shallow_copy(),
                kernel_string_slice!(table_id),
                allocate_err,
            ))
        };

        unsafe {
            free_uc_commit_client(client);
            free_uc_committer(committer);
        }
    }
}
