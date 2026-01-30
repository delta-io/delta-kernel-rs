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
/// to populate the response, and then can be returned from [`get_commits`]
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
type CGetCommits = extern "C" fn(request: CommitsRequest) -> Handle<ExclusiveCommitsResponse>;

/// The callback that will be called when the client wants to commit. Return `None` on success, or
/// `Some("error description")` if an error occured.
// Note, it doesn't make sense to return an ExternResult here because that can't hold the string
// error msg
type CCommit = extern "C" fn(request: CommitRequest) -> OptionalValue<Handle<ExclusiveRustString>>;

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

#[no_mangle]
pub unsafe extern "C" fn get_uc_commiter(
    commit_client: Handle<SharedFfiUCCommitsClient>,
) -> Handle<MutableCommitter> {
    let client: Arc<FfiUCCommitsClient> = unsafe { commit_client.clone_as_arc() };
    let committer: Box<dyn Committer> = Box::new(UCCommitter::new(client, "foo"));
    committer.into()
}
