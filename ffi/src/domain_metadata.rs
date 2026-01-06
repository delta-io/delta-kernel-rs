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
    let domain = unsafe { String::try_from_slice(&domain) };

    get_domain_metadata_impl(snapshot, domain, engine, allocate_fn).into_extern_result(&engine)
}

fn get_domain_metadata_impl(
    snapshot: &Snapshot,
    domain: DeltaResult<String>,
    extern_engine: &dyn ExternEngine,
    allocate_fn: AllocateStringFn,
) -> DeltaResult<NullableCvoid> {
    Ok(snapshot
        .get_domain_metadata(&domain?, extern_engine.engine().as_ref())?
        .and_then(|config: String| allocate_fn(kernel_string_slice!(config))))
}

/// Get the domain metadata as an optional string allocated by `AllocatedStringFn` for a specific domain in this snapshot
///
/// # Safety
///
/// Caller is responsible for passing in a valid handle
#[no_mangle]
pub unsafe extern "C" fn visit_domain_metadata(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        domain: KernelStringSlice,
        configuration: KernelStringSlice,
    ),
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };

    visit_domain_metadata_impl(snapshot, engine, engine_context, visitor)
        .into_extern_result(&engine)
}

fn visit_domain_metadata_impl(
    snapshot: &Snapshot,
    extern_engine: &dyn ExternEngine,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        key: KernelStringSlice,
        value: KernelStringSlice,
    ),
) -> DeltaResult<bool> {
    let res = snapshot.get_all_domain_metadata(extern_engine.engine().as_ref())?;
    res.iter().for_each(|metadata| {
        let domain = &metadata.domain();
        let configuration = &metadata.configuration();
        visitor(
            engine_context,
            kernel_string_slice!(domain),
            kernel_string_slice!(configuration),
        );
    });

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_err, allocate_str, assert_extern_result_error_with_message, ok_or_panic,
        recover_string,
    };
    use crate::{engine_to_handle, free_engine, free_snapshot, kernel_string_slice, snapshot};
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::DeltaResult;
    use object_store::memory::InMemory;
    use serde_json::json;
    use std::collections::HashMap;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use test_utils::add_commit;

    #[tokio::test]
    async fn test_domain_metadata() -> DeltaResult<()> {
        let storage = Arc::new(InMemory::new());

        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
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

        let snapshot =
            unsafe { ok_or_panic(snapshot(kernel_string_slice!(path), engine.shallow_copy())) };

        let get_domain_metadata_helper = |domain: &str| unsafe {
            get_domain_metadata(
                snapshot.shallow_copy(),
                kernel_string_slice!(domain),
                engine.shallow_copy(),
                allocate_str,
            )
        };

        // First, we test fetching the domain metadata one-by-one

        let domain1 = "domain1";
        let res = ok_or_panic(get_domain_metadata_helper(domain1));
        assert!(res.is_none());

        let domain2 = "domain2";
        let res = ok_or_panic(get_domain_metadata_helper(domain2));
        assert_eq!(recover_string(res.unwrap()), "domain2_commit1");

        let domain3 = "delta.domain3";
        let res = get_domain_metadata_helper(domain3);
        assert_extern_result_error_with_message(res, KernelError::GenericError, "Generic delta kernel error: User DomainMetadata are not allowed to use system-controlled 'delta.*' domain");

        // Secondly, we visit the entire domain metadata

        // Create visitor state
        let visitor_state: Box<HashMap<String, String>> = Box::default();
        let visitor_state_ptr = Box::into_raw(visitor_state);

        // Test visitor function
        extern "C" fn visitor(
            state: NullableCvoid,
            key: KernelStringSlice,
            value: KernelStringSlice,
        ) {
            let mut collected_metadata = unsafe {
                Box::from_raw(
                    state.unwrap().as_ptr() as *mut std::collections::HashMap<String, String>
                )
            };
            let key: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&key) };
            let value: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&value) };
            collected_metadata.insert(key.unwrap(), value.unwrap());
            Box::leak(collected_metadata);
        }

        // Visit all (user) domain metadata
        let res = unsafe {
            ok_or_panic(visit_domain_metadata(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::new_unchecked(visitor_state_ptr).cast()),
                visitor,
            ))
        };

        // Confirm visitor picked up all entries in map
        let collected_metadata = unsafe { Box::from_raw(visitor_state_ptr) };
        assert!(res);
        assert!(collected_metadata.get("domain1").is_none());
        assert!(collected_metadata.get("delta.domain3").is_none());
        assert_eq!(
            collected_metadata.get("domain2").unwrap(),
            "domain2_commit1"
        );

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }

        Ok(())
    }
}
