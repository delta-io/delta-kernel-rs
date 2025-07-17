use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    kernel_string_slice, AllocateStringFn, ExternEngine, KernelStringSlice, NullableCvoid,
    SharedExternEngine, SharedSnapshot, TryFromStringSlice,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::DeltaResult;

/// Get the domain metadata as an optional string allocated by `AllocatedStringFn` for a specific domain in this snapshot
///
/// # Safety
///
/// Caller is responsible for passing in a valid handle
#[no_mangle]
pub unsafe extern "C" fn get_domain_metadata(
    snapshot: Handle<SharedSnapshot>,
    domain: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    allocate_fn: AllocateStringFn,
) -> ExternResult<NullableCvoid> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };

    get_domain_metadata_impl(snapshot, domain, engine, allocate_fn).into_extern_result(&engine)
}

unsafe fn get_domain_metadata_impl(
    snapshot: &Snapshot,
    domain: KernelStringSlice,
    extern_engine: &dyn ExternEngine,
    allocate_fn: AllocateStringFn,
) -> DeltaResult<NullableCvoid> {
    let domain = unsafe { String::try_from_slice(&domain)? };

    Ok(snapshot
        .get_domain_metadata(&domain, extern_engine.engine().as_ref())?
        .and_then(|config: String| allocate_fn(kernel_string_slice!(config))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_err, allocate_str, assert_extern_result_error, ok_or_panic, recover_string,
    };
    use crate::{engine_to_handle, kernel_string_slice, snapshot};
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::DeltaResult;
    use serde_json::json;
    use std::sync::Arc;
    use test_utils::add_commit;

    #[tokio::test]
    async fn test_domain_metadata() -> DeltaResult<()> {
        let storage = Arc::new(InMemory::new());

        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine_handle = engine_to_handle(Arc::new(engine), allocate_err);
        let path = "memory:///";

        // commit0
        // - domain1: not removed
        // - domain2: not removed
        let commit = [
            json!({
                "protocol": {
                    "minReaderVersion": 1,
                    "minWriterVersion": 1
                }
            }),
            json!({
                "metaData": {
                    "id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9",
                    "format": { "provider": "parquet", "options": {} },
                    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495i64
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit0",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit0",
                    "removed": false
                }
            }),
        ]
            .map(|json| json.to_string())
            .join("\n");

        add_commit(storage.clone().as_ref(), 0, commit)
            .await
            .unwrap();

        // commit1
        // - domain1: removed
        // - domain2: not-removed
        // - internal domain
        let commit = [
            json!({
                "domainMetadata": {
                    "domain": "domain1",
                    "configuration": "domain1_commit1",
                    "removed": true
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "domain2",
                    "configuration": "domain2_commit1",
                    "removed": false
                }
            }),
            json!({
                "domainMetadata": {
                    "domain": "delta.domain3",
                    "configuration": "domain3_commit1",
                    "removed": false
                }
            }),
        ]
        .map(|json| json.to_string())
        .join("\n");

        add_commit(storage.as_ref(), 1, commit).await.unwrap();

        let snapshot = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(path),
                engine_handle.shallow_copy(),
            ))
        };

        let domain1 = "domain1";
        let res = unsafe {
            ok_or_panic(get_domain_metadata(
                snapshot.clone_handle(),
                kernel_string_slice!(domain1),
                engine_handle.clone_handle(),
                allocate_str,
            ))
        };
        assert!(res.is_none());

        let domain1 = "domain2";
        let res = unsafe {
            ok_or_panic(get_domain_metadata(
                snapshot.clone_handle(),
                kernel_string_slice!(domain1),
                engine_handle.clone_handle(),
                allocate_str,
            ))
        };
        let result = recover_string(res.unwrap());
        assert_eq!(result, "domain2_commit1");

        let domain1 = "delta.domain3";
        let res = unsafe {
            get_domain_metadata(
                snapshot.clone_handle(),
                kernel_string_slice!(domain1),
                engine_handle.clone_handle(),
                allocate_str,
            )
        };
        assert_extern_result_error(res, KernelError::GenericError, "Generic delta kernel error: User DomainMetadata are not allowed to use system-controlled 'delta.*' domain");

        Ok(())
    }
}
