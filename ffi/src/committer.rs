//! FFI for Committers

use std::ffi::c_void;

use delta_kernel::committer::{
    CatalogCommitter, CommitResponse, Committer, Context, StagedCommitter,
};
use delta_kernel::{DeltaResult, Engine, Error};
use url::Url;

use crate::error::ExternResult;
use crate::handle::Handle;
use crate::{KernelStringSlice, SharedExternEngine};
use delta_kernel_ffi_macros::handle_descriptor;

/// This is an opaque pointer to external context. This allows engines to store additional metadata
/// to 'pass through' to its [`CatalogCommitCallback`].
pub type ExternContextPtr = *mut c_void;

/// FFI callback for catalog commit operations
pub type CatalogCommitCallback = extern "C" fn(
    engine: Handle<SharedExternEngine>,
    staged_commit_path: KernelStringSlice,
    context: ExternContextPtr,
) -> ExternResult<CommitResponse>;

/// Handle for a mutable boxed committer that can be passed across FFI
#[handle_descriptor(target = dyn Committer, mutable = true)]
pub struct MutableCommitter;

/// Wrapper for external context - just holds an opaque pointer
#[derive(Debug)]
struct ExternContext {
    ptr: ExternContextPtr,
}

// SAFETY: External code is responsible for ensuring thread safety
unsafe impl Send for ExternContext {}
unsafe impl Sync for ExternContext {}

impl Context for ExternContext {}

/// Wrapper for external catalog committer
struct ExternCatalogCommitter {
    callback: CatalogCommitCallback,
    engine_handle: Handle<SharedExternEngine>,
}

// SAFETY: Callback is extern "C" fn which is Send+Sync
unsafe impl Send for ExternCatalogCommitter {}
unsafe impl Sync for ExternCatalogCommitter {}

impl std::fmt::Debug for ExternCatalogCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternCatalogCommitter").finish()
    }
}

impl CatalogCommitter for ExternCatalogCommitter {
    fn commit_request(
        &self,
        _engine: &dyn Engine,
        staged_commit_path: &Url,
        context: &dyn Context,
    ) -> DeltaResult<CommitResponse> {
        let extern_context = context
            .any_ref()
            .downcast_ref::<ExternContext>()
            .ok_or_else(|| Error::generic("Invalid context type for external committer"))?;

        let path_str = staged_commit_path.as_str();
        let path_slice = unsafe { KernelStringSlice::new_unsafe(path_str) };

        let engine_handle = unsafe { self.engine_handle.clone_handle() };

        // call the callback and convert result
        match (self.callback)(engine_handle, path_slice, extern_context.ptr) {
            ExternResult::Ok(response) => Ok(response),
            ExternResult::Err(_) => Err(Error::generic("External catalog commit callback failed")),
        }
    }
}

/// Create a staged committer with external catalog implementation
///
/// # Safety
/// - `callback` must be a valid function pointer
/// - `context` must remain valid for the lifetime of the committer
/// - `engine` must be a valid handle
#[no_mangle]
pub unsafe extern "C" fn create_staged_committer(
    callback: CatalogCommitCallback,
    context: ExternContextPtr,
    engine: Handle<SharedExternEngine>,
) -> Handle<MutableCommitter> {
    // just double-boxing the context
    let extern_context = Box::new(ExternContext { ptr: context });
    let extern_committer = Box::new(ExternCatalogCommitter {
        callback,
        engine_handle: engine,
    });

    let staged_committer = StagedCommitter::new(extern_committer, extern_context);
    let staged_committer: Box<dyn Committer> = Box::new(staged_committer);
    staged_committer.into()
}

/// Free a committer handle
///
/// # Safety
/// Caller must pass a valid handle
#[no_mangle]
pub unsafe extern "C" fn free_committer(committer: Handle<MutableCommitter>) {
    committer.drop_handle();
}
