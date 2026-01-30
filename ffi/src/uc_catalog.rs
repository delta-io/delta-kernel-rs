//! FFI hooks that enable constructing a UCCommitsClient. On

use crate::error::{ExternResult, IntoExternResult as _};
use crate::ExclusiveRustString;
use crate::{error::AllocateErrorFn, transaction::MutableCommitter};
use std::sync::Arc;

use delta_kernel::committer::Committer;
use delta_kernel::DeltaResult;
use delta_kernel_ffi::{
    handle::Handle, kernel_string_slice, KernelStringSlice, OptionalValue, TryFromStringSlice,
};
use delta_kernel_ffi_macros::handle_descriptor;
use uc_catalog::UCCommitter;

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
pub type CGetCommits = extern "C" fn(request: CommitsRequest) -> Handle<ExclusiveCommitsResponse>;

/// The callback that will be called when the client wants to commit. Return `None` on success, or
/// `Some("error description")` if an error occured.
// Note, it doesn't make sense to return an ExternResult here because that can't hold the string
// error msg
pub type CCommit =
    extern "C" fn(request: CommitRequest) -> OptionalValue<Handle<ExclusiveRustString>>;

pub struct FfiUCCommitsClient {
    get_commits_callback: CGetCommits,
    commit_callback: CCommit,
}

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
        let c_resp = (self.get_commits_callback)(c_request);
        let response = unsafe { c_resp.into_inner() };
        uc_client::Result::Ok(*response)
    }

    /// Commit a new version to the table.
    async fn commit(&self, request: ClientCommitRequest) -> uc_client::Result<()> {
        let table_id = request.table_id;
        let table_uri = request.table_uri;
        let commit_info = request
            .commit_info
            .map(|ci| {
                let file_name = ci.file_name;
                let file_name = kernel_string_slice!(file_name);
                Commit {
                    version: ci.version,
                    timestamp: ci.timestamp,
                    file_name,
                    file_size: ci.file_size,
                    file_modification_timestamp: ci.file_modification_timestamp,
                }
            })
            .into();
        let c_commit_request = CommitRequest {
            table_id: kernel_string_slice!(table_id),
            table_uri: kernel_string_slice!(table_uri),
            commit_info,
            latest_backfilled_version: request.latest_backfilled_version.into(),
            metadata: None.into(),
            protocol: None.into(),
        };

        match (self.commit_callback)(c_commit_request) {
            OptionalValue::Some(e) => {
                let boxed_str = unsafe { e.into_inner() }; // get the string back into Box<String>
                let s: String = *boxed_str; // move back onto the stack
                uc_client::Result::Err(uc_client::Error::Generic(s))
            }
            OptionalValue::None => uc_client::Result::Ok(()),
        }
    }
}

#[handle_descriptor(target=FfiUCCommitsClient, mutable=false, sized=true)]
pub struct SharedFfiUCCommitsClient;

/// Get a commit client that will call the passed callbacks when it wants to request commits or to
/// make a commit.
///
/// # Safety
///
///  Caller is responsible for passing a valid pointers for the callbacks
#[no_mangle]
pub unsafe extern "C" fn get_uc_commit_client(
    get_commits_callback: CGetCommits,
    commit_callback: CCommit,
) -> Handle<SharedFfiUCCommitsClient> {
    Arc::new(FfiUCCommitsClient {
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
    let committer: Box<dyn Committer> = Box::new(UCCommitter::new(client, table_id_str));
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
pub unsafe extern "C" fn free_uc_commiter(commit_client: Handle<MutableCommitter>) {
    debug!("released uc committer");
    commit_client.drop_handle();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::{allocate_kernel_string, kernel_string_slice, OptionalValue};
    use std::cell::RefCell;
    use std::sync::Arc;
    use uc_client::UCCommitsClient;

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

        let file_name = String::from("00000000000000000005.json");
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
        assert_eq!(commits[0].file_name, "00000000000000000005.json");
        assert_eq!(commits[0].file_size, 1024);
        assert_eq!(commits[0].file_modification_timestamp, 1234567900);
    }

    #[test]
    fn test_add_multiple_commits_to_response() {
        let latest_version = 20;
        let mut response = unsafe { init_commits_response(latest_version) };

        let commits_data = vec![
            (10, "00000000000000000010.json", 2048),
            (11, "00000000000000000011.json", 4096),
            (12, "00000000000000000012.json", 8192),
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
        static SHOULD_FAIL_COMMIT: RefCell<bool> = const { RefCell::new(false) };
    }

    #[no_mangle]
    extern "C" fn test_get_commits_callback(
        request: CommitsRequest,
    ) -> Handle<ExclusiveCommitsResponse> {
        GET_COMMITS_CALLED.with(|called| *called.borrow_mut() = true);

        let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };
        let table_uri = unsafe { String::try_from_slice(&request.table_uri).unwrap() };

        LAST_COMMITS_REQUEST.with(|req| {
            *req.borrow_mut() = Some((table_id.clone(), table_uri.clone()));
        });

        let response = unsafe { init_commits_response(5) };

        let file_name = String::from("00000000000000000003.json");
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
        request: CommitRequest,
    ) -> OptionalValue<Handle<ExclusiveRustString>> {
        COMMIT_CALLED.with(|called| *called.borrow_mut() = true);

        let table_id = unsafe { String::try_from_slice(&request.table_id).unwrap() };
        let table_uri = unsafe { String::try_from_slice(&request.table_uri).unwrap() };

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
            unsafe { get_uc_commit_client(test_get_commits_callback, test_commit_callback) };

        let _client_ref: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };
        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commits_client_get_commits() {
        GET_COMMITS_CALLED.with(|c| *c.borrow_mut() = false);

        let client =
            unsafe { get_uc_commit_client(test_get_commits_callback, test_commit_callback) };

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
        assert_eq!(commits[0].file_name, "00000000000000000003.json");

        assert!(GET_COMMITS_CALLED.with(|c| *c.borrow()));

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

        let client =
            unsafe { get_uc_commit_client(test_get_commits_callback, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };

        let request = ClientCommitRequest {
            table_id: "test_table_id".to_string(),
            table_uri: "s3://bucket/path".to_string(),
            commit_info: Some(ClientCommit {
                version: 10,
                timestamp: 2000000000,
                file_name: "00000000000000000010.json".to_string(),
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

        LAST_COMMIT_REQUEST.with(|req| {
            let req = req.borrow();
            let (table_id, table_uri) = req.as_ref().unwrap();
            assert_eq!(table_id, "test_table_id");
            assert_eq!(table_uri, "s3://bucket/path");
        });

        unsafe { free_uc_commit_client(client) };
    }

    #[tokio::test]
    async fn test_ffi_uc_commits_client_commit_failure() {
        COMMIT_CALLED.with(|c| *c.borrow_mut() = false);
        SHOULD_FAIL_COMMIT.with(|f| *f.borrow_mut() = true);

        let client =
            unsafe { get_uc_commit_client(test_get_commits_callback, test_commit_callback) };

        let client_arc: Arc<FfiUCCommitsClient> = unsafe { client.clone_as_arc() };

        let request = ClientCommitRequest {
            table_id: "test_table_id".to_string(),
            table_uri: "s3://bucket/path".to_string(),
            commit_info: Some(ClientCommit {
                version: 10,
                timestamp: 2000000000,
                file_name: "00000000000000000010.json".to_string(),
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
            unsafe { get_uc_commit_client(test_get_commits_callback, test_commit_callback) };

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
            free_uc_commiter(committer);
        }
    }
}
