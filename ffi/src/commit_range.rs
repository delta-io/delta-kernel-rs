//! FFI bindings for building and inspecting a [`CommitRange`].

use std::sync::Arc;

use delta_kernel::commit_range::CommitRange;
use delta_kernel::{DeltaResult, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

use crate::handle::Handle;
use crate::{
    unwrap_and_parse_path_as_url, ExternEngine, ExternResult, IntoExternResult, KernelStringSlice,
    SharedExternEngine,
};

/// An opaque, exclusive handle owning a [`CommitRange`] produced by
/// [`commit_range_builder_build`]. The caller owns the handle and must release it with
/// [`free_commit_range`].
#[handle_descriptor(target=CommitRange, mutable=true, sized=true)]
pub struct ExclusiveCommitRange;

/// Opaque builder for constructing a [`CommitRange`] from a table path.
///
/// Create with [`commit_range_builder_for`]. Optionally pin the end of the range with
/// [`commit_range_builder_with_end_version`]. Finally, call [`commit_range_builder_build`] to
/// consume the builder and obtain the range. To discard the builder without building, call
/// [`free_commit_range_builder`].
pub struct FfiCommitRangeBuilder {
    engine: Arc<dyn ExternEngine>,
    table_root: Url,
    start_version: Version,
    end_version: Option<Version>,
}

/// An opaque handle with exclusive (Box-like) ownership of a [`FfiCommitRangeBuilder`].
#[handle_descriptor(target=FfiCommitRangeBuilder, mutable=true, sized=true)]
pub struct MutableFfiCommitRangeBuilder;

fn make_commit_range_builder(
    table_root: Url,
    start_version: Version,
    engine: Arc<dyn ExternEngine>,
) -> Handle<MutableFfiCommitRangeBuilder> {
    Box::new(FfiCommitRangeBuilder {
        engine,
        table_root,
        start_version,
        end_version: None,
    })
    .into()
}

/// Get a builder for constructing a [`CommitRange`] from a table path, starting at
/// `start_version`.
///
/// The caller owns the returned handle and must eventually call either
/// [`commit_range_builder_build`] to produce a [`CommitRange`], or [`free_commit_range_builder`]
/// to drop it without building.
///
/// # Safety
///
/// Caller is responsible for passing a valid path and engine handle.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_for(
    path: KernelStringSlice,
    start_version: Version,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<MutableFfiCommitRangeBuilder>> {
    let engine_ref = unsafe { engine.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    url.map(|url| make_commit_range_builder(url, start_version, engine_arc))
        .into_extern_result(&engine_ref)
}

/// Pin the end version of the range. When omitted, the range extends to the latest committed
/// version observed at build time.
///
/// # Safety
///
/// Caller must pass a valid builder pointer.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_with_end_version(
    builder: &mut Handle<MutableFfiCommitRangeBuilder>,
    end_version: Version,
) {
    unsafe { builder.as_mut() }.end_version = Some(end_version);
}

/// Consume the builder and return a [`CommitRange`]. After calling, the builder pointer is _no
/// longer valid_. The builder is always freed by this call, whether or not it succeeds.
///
/// Returns an error if the resolved version range is invalid (start > end), the listed commits
/// are non-contiguous, or the requested start version is not present.
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn commit_range_builder_build(
    mut builder: Handle<MutableFfiCommitRangeBuilder>,
) -> ExternResult<Handle<ExclusiveCommitRange>> {
    // Clone the engine Arc before consuming the handle so we can still use it for error reporting.
    let engine_arc = unsafe { builder.as_mut() }.engine.clone();
    let engine_ref = engine_arc.as_ref();
    let builder_box = unsafe { builder.into_inner() };
    commit_range_builder_build_impl(*builder_box).into_extern_result(&engine_ref)
}

fn commit_range_builder_build_impl(
    builder: FfiCommitRangeBuilder,
) -> DeltaResult<Handle<ExclusiveCommitRange>> {
    let engine = builder.engine.engine();
    let mut rust_builder = CommitRange::builder_for(builder.table_root, builder.start_version);
    if let Some(end_version) = builder.end_version {
        rust_builder = rust_builder.with_end_version(end_version);
    }
    let range = rust_builder.build(engine.as_ref())?;
    Ok(Box::new(range).into())
}

/// Free a commit range builder without building a range (e.g. on an error path).
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_commit_range_builder(builder: Handle<MutableFfiCommitRangeBuilder>) {
    builder.drop_handle();
}

/// Free a [`CommitRange`].
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_commit_range(commit_range: Handle<ExclusiveCommitRange>) {
    commit_range.drop_handle();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use test_utils::{actions_to_string, add_commit, TestAction};

    use super::*;
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_err, assert_extern_result_error_with_message, ok_or_panic,
    };
    use crate::{engine_to_handle, free_engine, kernel_string_slice};

    /// Build an in-memory engine handle pre-loaded with a metadata-only commit at each version in
    /// `0..=last_version`. Returns `(engine_handle, table_root)`; the caller frees the engine.
    async fn setup_engine_with_commits(
        last_version: u64,
    ) -> (Handle<SharedExternEngine>, &'static str) {
        let table_root = "memory:///test_table/";
        let storage = Arc::new(InMemory::new());
        for version in 0..=last_version {
            add_commit(
                table_root,
                storage.as_ref(),
                version,
                actions_to_string(vec![TestAction::Metadata]),
            )
            .await
            .unwrap();
        }
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        (engine_to_handle(Arc::new(engine), allocate_err), table_root)
    }

    #[tokio::test]
    async fn test_commit_range_builder_for_build_succeeds() -> Result<(), Box<dyn std::error::Error>>
    {
        let (engine, table_root) = setup_engine_with_commits(1).await;

        let mut builder = unsafe {
            ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(table_root),
                0,
                engine.shallow_copy(),
            ))
        };
        unsafe { commit_range_builder_with_end_version(&mut builder, 1) };
        let mut range = unsafe { ok_or_panic(commit_range_builder_build(builder)) };

        assert_eq!(unsafe { range.as_mut() }.start_version(), 0);
        assert_eq!(unsafe { range.as_mut() }.end_version(), 1);

        unsafe { free_commit_range(range) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_range_builder_for_invalid_path_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _) = setup_engine_with_commits(0).await;

        let invalid_path = "not a valid url!";
        let result = unsafe {
            commit_range_builder_for(kernel_string_slice!(invalid_path), 0, engine.shallow_copy())
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidTableLocationError,
            None,
        );

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_range_builder_build_errors_on_missing_start_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Table has only v=0, but we request a range starting at v=5.
        let (engine, table_root) = setup_engine_with_commits(0).await;

        let builder = unsafe {
            ok_or_panic(commit_range_builder_for(
                kernel_string_slice!(table_root),
                5,
                engine.shallow_copy(),
            ))
        };
        let result = unsafe { commit_range_builder_build(builder) };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);

        unsafe { free_engine(engine) }
        Ok(())
    }
}
