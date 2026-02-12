//! FFI hooks that enable constructing a UCCommitsClient. On

use crate::error::{ExternResult, IntoExternResult as _};
use crate::{error::AllocateErrorFn, transaction::MutableCommitter};
use crate::{ExclusiveRustString, NullableCvoid};
use std::sync::Arc;

use delta_kernel::committer::Committer;
use delta_kernel::DeltaResult;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::{TokioBackgroundExecutor, TokioMultiThreadExecutor};
use delta_kernel_ffi::{
    handle::Handle, kernel_string_slice, KernelStringSlice, OptionalValue, TryFromStringSlice,
};
use delta_kernel_ffi_macros::handle_descriptor;
use uc_catalog::UCCommitter;

use uc_client::UCCommitsClient;
use uc_client::models::{
    Commit as ClientCommit, CommitRequest as ClientCommitRequest,
    CommitsRequest as ClientCommitsRequest, CommitsResponse as ClientCommitsResponse,
};

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

/// The data supplied when requesting commits
#[repr(C)]
pub struct CommitsRequest {
    pub table_id: KernelStringSlice,
    pub table_uri: KernelStringSlice,
    pub start_version: OptionalValue<i64>,
    pub end_version: OptionalValue<i64>,
}

#[handle_descriptor(target=ClientCommitsResponse, mutable=true, sized=true)]
pub struct ExclusiveCommitsResponse;

/// Get a handle to an `ExclusiveCommitsResponse`. This can be passed to [`add_commit_to_response`]
/// to populate the response, and then can be returned from the [`CGetCommits`] callback.
///
/// # Safety
///
///  Caller is responsible for passing a valid i64 as the table version
#[no_mangle]
pub unsafe extern "C" fn init_commits_response(
    latest_table_version: i64,
) -> Handle<ExclusiveCommitsResponse> {
    let res = Box::new(ClientCommitsResponse {
        commits: None,
        latest_table_version,
    });
    res.into()
}

/// Add a commit to the response. Important! This consumes the passed response and returns a new
/// one. The passed response is no longer valid after this call.
///
/// # Safety
///
///  Caller is responsible for passing a valid pointer to a response, valid `Commit` data, and a
///  valid error callback
#[no_mangle]
pub unsafe extern "C" fn add_commit_to_response(
    response: Handle<ExclusiveCommitsResponse>,
    commit: Commit,
    error_fn: AllocateErrorFn,
) -> ExternResult<Handle<ExclusiveCommitsResponse>> {
    let response = response.into_inner();
    add_commit_to_response_impl(response, commit).into_extern_result(&error_fn)
}

fn add_commit_to_response_impl(
    mut response: Box<ClientCommitsResponse>,
    commit: Commit,
) -> DeltaResult<Handle<ExclusiveCommitsResponse>> {
    let file_name: String = unsafe { TryFromStringSlice::try_from_slice(&commit.file_name) }?;

    let client_commit = ClientCommit {
        version: commit.version,
        timestamp: commit.timestamp,
        file_name,
        file_size: commit.file_size,
        file_modification_timestamp: commit.file_modification_timestamp,
    };
    match response.commits {
        Some(ref mut commits) => commits.push(client_commit),
        None => response.commits = Some(vec![client_commit]),
    }
    Ok(response.into())
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

/// The callback that will be called when the client wants to get a list of commits from UC. The general flow expected from a connector is:
/// ```ignored
/// CatalogResponse catalog_response = [rest call to catalog];
/// ExclusiveCommitsResponse* response = init_commits_response(catalog_response.latest_table_version);
/// for catalog_commit in catalog_response.list_of_commits {
///   Commit commit = [construct Commit from catalog_commit];
///   response = add_commit_to_response(response, commit); // need to handle errors here as well
/// }
/// return response;
/// ```
pub type CGetCommits = extern "C" fn(
    context: NullableCvoid,
    request: CommitsRequest,
) -> Handle<ExclusiveCommitsResponse>;

/// The callback that will be called when the client wants to commit. Return `None` on success, or
/// `Some("error description")` if an error occured.
// Note, it doesn't make sense to return an ExternResult here because that can't hold the string
// error msg
pub type CCommit = extern "C" fn(
    context: NullableCvoid,
    request: CommitRequest,
) -> OptionalValue<Handle<ExclusiveRustString>>;

pub struct FfiUCCommitsClient {
    context: NullableCvoid,
    get_commits_callback: CGetCommits,
    commit_callback: CCommit,
}

// NullableCvoid is NOT `Send` by itself. Here we declare our struct to be Send as it's up to the
// caller to ensure they pass a thread safe pointer that remains valid
unsafe impl Send for FfiUCCommitsClient {}
unsafe impl Sync for FfiUCCommitsClient {}

impl uc_client::UCCommitsClient for FfiUCCommitsClient {
    /// Get the latest commits for the table.
    async fn get_commits(
        &self,
        request: ClientCommitsRequest,
    ) -> uc_client::Result<ClientCommitsResponse> {
        let table_id = request.table_id;
        let table_uri = request.table_uri;
        let c_request = CommitsRequest {
            table_id: kernel_string_slice!(table_id),
            table_uri: kernel_string_slice!(table_uri),
            start_version: request.start_version.into(),
            end_version: request.end_version.into(),
        };
        let c_resp = (self.get_commits_callback)(self.context, c_request);
        let response = unsafe { c_resp.into_inner() };
        uc_client::Result::Ok(*response)
    }

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

#[handle_descriptor(target=FfiUCCommitsClient, mutable=false, sized=true)]
pub struct SharedFfiUCCommitsClient;

/// Get a commit client that will call the passed callbacks when it wants to request commits or to
/// make a commit. The context will be passed back to the callbacks when they are called.
/// The callbacks will be passed the supplied context.
///
/// IMPORTANT: The pointer passed for the context MUST be thread-safe (i.e. be able to be sent
/// between threads safely) and MUST remain valid for as long as the client is used. It is valid to
/// pass NULL as the context.
///
/// # Safety
///
///  Caller is responsible for passing a valid pointers for the callbacks and a valid context
///  pointer
#[no_mangle]
pub unsafe extern "C" fn get_uc_commit_client(
    context: NullableCvoid,
    get_commits_callback: CGetCommits,
    commit_callback: CCommit,
) -> Handle<SharedFfiUCCommitsClient> {
    Arc::new(FfiUCCommitsClient {
        context,
        get_commits_callback,
        commit_callback,
    })
    .into()
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_uc_commit_client(commit_client: Handle<SharedFfiUCCommitsClient>) {
    debug!("released uc commit client");
    commit_client.drop_handle();
}

// we need our own struct here because we want to override the calls to enter the tokio runtime
// before calling into the standard committer
struct FFIUCCommitter<C: UCCommitsClient> {
    inner: UCCommitter<C>,
}

impl<C: UCCommitsClient + 'static> Committer for FFIUCCommitter<C> {
    fn commit(
        &self,
        engine: &dyn delta_kernel::Engine,
        actions: Box<dyn Iterator<Item = DeltaResult<delta_kernel::FilteredEngineData>> + Send + '_>,
        commit_metadata: delta_kernel::committer::CommitMetadata,
    ) -> DeltaResult<delta_kernel::committer::CommitResponse> {
        let mut guard = (engine as &dyn std::any::Any).downcast_ref::<DefaultEngine<TokioMultiThreadExecutor>>().map(|e| e.enter());
        if guard.is_none() {
            guard = (engine as &dyn std::any::Any).downcast_ref::<DefaultEngine<TokioBackgroundExecutor>>().map(|e| e.enter());
        }
        if guard.is_none() {
            return Err(delta_kernel::Error::generic("FFIUCCommitter can only be used with the default engine"));
        }
        self.inner.commit(engine, actions, commit_metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        self.inner.is_catalog_committer()
    }

    fn publish(&self, engine: &dyn delta_kernel::Engine, publish_metadata: delta_kernel::committer::PublishMetadata) -> DeltaResult<()> {
        self.inner.publish(engine, publish_metadata)
    }
}

/// Get a commit client that will call the passed callbacks when it wants to request commits or to
/// make a commit.
///
/// # Safety
///
///  Caller is responsible for passing a valid pointer to a SharedFfiUCCommitsClient, obtained via
///  calling [`get_uc_commit_client`], a valid KernelStringSlice as the table_id, and a valid error
///  function pointer.
#[no_mangle]
pub unsafe extern "C" fn get_uc_committer(
    commit_client: Handle<SharedFfiUCCommitsClient>,
    table_id: KernelStringSlice,
    error_fn: AllocateErrorFn,
) -> ExternResult<Handle<MutableCommitter>> {
    get_uc_committer_impl(commit_client, table_id).into_extern_result(&error_fn)
}

fn get_uc_committer_impl(
    commit_client: Handle<SharedFfiUCCommitsClient>,
    table_id: KernelStringSlice,
) -> DeltaResult<Handle<MutableCommitter>> {
    let client: Arc<FfiUCCommitsClient> = unsafe { commit_client.clone_as_arc() };
    let table_id_str: String = unsafe { TryFromStringSlice::try_from_slice(&table_id) }?;
    let committer: Box<dyn Committer> = Box::new(FFIUCCommitter {
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
    use std::cell::RefCell;
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use uc_client::UCCommitsClient;

    struct TestContext {
        x: u32, // have a value so we can ensure it's passed correctly
    }

    fn get_test_context(x: u32) -> NullableCvoid {
        let context = Box::new(TestContext { x });
        NonNull::new(Box::into_raw(context) as *mut c_void)
    }

    fn recover_test_context(context: NullableCvoid) -> Option<Box<TestContext>> {
        context.map(|context| unsafe { Box::from_raw(context.as_ptr() as *mut TestContext) })
    }

    #[test]
    fn test_init_commits_response() {
        let latest_version = 42;
        let response = unsafe { init_commits_response(latest_version) };

        let response_inner = unsafe { response.into_inner() };
        assert_eq!(response_inner.latest_table_version, latest_version);
        assert!(response_inner.commits.is_none());
    }

    #[test]
    fn test_add_commit_to_response() {
        let latest_version = 10;
        let response = unsafe { init_commits_response(latest_version) };

        let file_name = String::from("00000000000000000005.uuid.json");
        let commit = Commit {
            version: 5,
            timestamp: 1234567890,
            file_name: kernel_string_slice!(file_name),
            file_size: 1024,
            file_modification_timestamp: 1234567900,
        };

        let response =
            unsafe { ok_or_panic(add_commit_to_response(response, commit, allocate_err)) };

        let response_inner = unsafe { response.into_inner() };
        assert_eq!(response_inner.latest_table_version, latest_version);
        assert!(response_inner.commits.is_some());

        let commits = response_inner.commits.unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].version, 5);
        assert_eq!(commits[0].timestamp, 1234567890);
        assert_eq!(commits[0].file_name, "00000000000000000005.uuid.json");
        assert_eq!(commits[0].file_size, 1024);
        assert_eq!(commits[0].file_modification_timestamp, 1234567900);
    }

    #[test]
    fn test_add_multiple_commits_to_response() {
        let latest_version = 20;
        let mut response = unsafe { init_commits_response(latest_version) };

        let commits_data = vec![
            (10, "00000000000000000010.uuid.json", 2048),
            (11, "00000000000000000011.uuid.json", 4096),
            (12, "00000000000000000012.uuid.json", 8192),
        ];

        for (version, file_name_str, file_size) in commits_data {
            let file_name = String::from(file_name_str);
            let commit = Commit {
                version,
                timestamp: 1234567890 + version,
                file_name: kernel_string_slice!(file_name),
                file_size,
                file_modification_timestamp: 1234567900 + version,
            };

            response =
                unsafe { ok_or_panic(add_commit_to_response(response, commit, allocate_err)) };
        }

        let response_inner = unsafe { response.into_inner() };
        assert_eq!(response_inner.latest_table_version, latest_version);
        assert!(response_inner.commits.is_some());

        let commits = response_inner.commits.unwrap();
        assert_eq!(commits.len(), 3);
        assert_eq!(commits[0].version, 10);
        assert_eq!(commits[1].version, 11);
        assert_eq!(commits[2].version, 12);
    }

    thread_local! {
        static GET_COMMITS_CALLED: RefCell<bool> = const { RefCell::new(false) };
        static COMMIT_CALLED: RefCell<bool> = const { RefCell::new(false) };
        static LAST_COMMITS_REQUEST: RefCell<Option<(String, String)>> = const { RefCell::new(None) };
        static LAST_COMMIT_REQUEST: RefCell<Option<(String, String)>> = const { RefCell::new(None) };
        static LAST_STAGED_FILENAME: RefCell<Option<String>> = const { RefCell::new(None) };
        static SHOULD_FAIL_COMMIT: RefCell<bool> = const { RefCell::new(false) };
        static CONTEXT_LAST_VALUE: RefCell<u32> = const { RefCell::new(0) };
    }

    #[no_mangle]
    extern "C" fn test_get_commits_callback(
        context: NullableCvoid,
        request: CommitsRequest,
    ) -> Handle<ExclusiveCommitsResponse> {
        GET_COMMITS_CALLED.with(|called| *called.borrow_mut() = true);
        if let Some(context) = recover_test_context(context) {
            CONTEXT_LAST_VALUE.with(|v| *v.borrow_mut() = context.x);
        }

        let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };
        let table_uri = unsafe { String::try_from_slice(&request.table_uri).unwrap() };

        LAST_COMMITS_REQUEST.with(|req| {
            *req.borrow_mut() = Some((table_id.clone(), table_uri.clone()));
        });

        let response = unsafe { init_commits_response(5) };

        let file_name = String::from("00000000000000000003.uuid.json");
        let commit = Commit {
            version: 3,
            timestamp: 1000000000,
            file_name: kernel_string_slice!(file_name),
            file_size: 512,
            file_modification_timestamp: 1000000100,
        };

        unsafe { ok_or_panic(add_commit_to_response(response, commit, allocate_err)) }
    }

    #[no_mangle]
    extern "C" fn test_commit_callback(
        context: NullableCvoid,
        request: CommitRequest,
    ) -> OptionalValue<Handle<ExclusiveRustString>> {
        COMMIT_CALLED.with(|called| *called.borrow_mut() = true);
        if let Some(context) = recover_test_context(context) {
            CONTEXT_LAST_VALUE.with(|v| *v.borrow_mut() = context.x);
        }

        let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };
        let table_uri = unsafe { String::try_from_slice(&request.table_uri).unwrap() };

        if let OptionalValue::Some(commit_info) = request.commit_info {
            let file_name = unsafe {
                crate::TryFromStringSlice::try_from_slice(&commit_info.file_name).unwrap()
            };
            LAST_STAGED_FILENAME.with(|sf| {
                *sf.borrow_mut() = Some(file_name);
            });
        }

        LAST_COMMIT_REQUEST.with(|req| {
            *req.borrow_mut() = Some((table_id.clone(), table_uri.clone()));
        });

        SHOULD_FAIL_COMMIT.with(|should_fail| {
            if *should_fail.borrow() {
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
        })
    }

    #[test]
    fn test_get_uc_commit_client() {
        let client =
            unsafe { get_uc_commit_client(None, test_get_commits_callback, test_commit_callback) };

        let _client_ref: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };
        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commits_client_get_commits() {
        GET_COMMITS_CALLED.with(|c| *c.borrow_mut() = false);

        let context = get_test_context(4);

        let client = unsafe {
            get_uc_commit_client(context, test_get_commits_callback, test_commit_callback)
        };

        let client_arc: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };

        let request = ClientCommitsRequest {
            table_id: "test_table_id".to_string(),
            table_uri: "s3://bucket/path".to_string(),
            start_version: None,
            end_version: None,
        };

        let result = client_arc.get_commits(request).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.latest_table_version, 5);
        assert!(response.commits.is_some());

        let commits = response.commits.unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].version, 3);
        assert_eq!(commits[0].file_name, "00000000000000000003.uuid.json");

        assert!(GET_COMMITS_CALLED.with(|c| *c.borrow()));
        CONTEXT_LAST_VALUE.with(|v| assert_eq!(*v.borrow(), 4));

        LAST_COMMITS_REQUEST.with(|req| {
            let req = req.borrow();
            let (table_id, table_uri) = req.as_ref().unwrap();
            assert_eq!(table_id, "test_table_id");
            assert_eq!(table_uri, "s3://bucket/path");
        });

        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commits_client_commit_success() {
        COMMIT_CALLED.with(|c| *c.borrow_mut() = false);
        SHOULD_FAIL_COMMIT.with(|f| *f.borrow_mut() = false);

        let context = get_test_context(5);

        let client = unsafe {
            get_uc_commit_client(context, test_get_commits_callback, test_commit_callback)
        };

        let client_arc: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };

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
        assert!(COMMIT_CALLED.with(|c| *c.borrow()));
        CONTEXT_LAST_VALUE.with(|v| assert_eq!(*v.borrow(), 5));

        LAST_COMMIT_REQUEST.with(|req| {
            let req = req.borrow();
            let (table_id, table_uri) = req.as_ref().unwrap();
            assert_eq!(table_id, "test_table_id");
            assert_eq!(table_uri, "s3://bucket/path");
        });

        LAST_STAGED_FILENAME.with(|sf| {
            let sf = sf.borrow();
            let staged_path = sf.as_ref().unwrap();
            assert_eq!(
                staged_path,
                "_staged_commits/00000000000000000010.uuid.json"
            );
        });

        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commits_client_commit_failure() {
        COMMIT_CALLED.with(|c| *c.borrow_mut() = false);
        SHOULD_FAIL_COMMIT.with(|f| *f.borrow_mut() = true);

        let context = get_test_context(6);

        let client = unsafe {
            get_uc_commit_client(context, test_get_commits_callback, test_commit_callback)
        };

        let client_arc: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };

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
        assert!(COMMIT_CALLED.with(|c| *c.borrow()));
        CONTEXT_LAST_VALUE.with(|v| assert_eq!(*v.borrow(), 6));

        let error = result.unwrap_err();
        assert!(matches!(error, uc_client::Error::Generic(_)));
        if let uc_client::Error::Generic(msg) = error {
            assert_eq!(msg, "Test commit failure");
        }

        unsafe { free_uc_commit_client(client) };
    }

    #[test]
    fn test_get_uc_committer() {
        let client =
            unsafe { get_uc_commit_client(None, test_get_commits_callback, test_commit_callback) };

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
