use std::sync::Arc;

use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Snapshot};

use crate::error::ExternResult;
use crate::handle::Handle;
use crate::transaction::ExclusiveTransaction;
use crate::{
    ExternEngine, IntoExternResult, KernelStringSlice, OptionalValue, SharedExternEngine,
    SharedSnapshot, TryFromStringSlice,
};

/// Associates an app_id and version with a transaction. These will be applied to the table on
/// commit.
///
/// # Returns
/// A new handle to the transaction that will set the `app_id` version to `version` on commit
///
/// # Safety
/// Caller is responsible for passing [valid][Handle#Validity] handles. The `app_id` string slice
/// must be valid. CONSUMES TRANSACTION
#[no_mangle]
pub unsafe extern "C" fn with_transaction_id(
    txn: Handle<ExclusiveTransaction>,
    app_id: KernelStringSlice,
    version: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveTransaction>> {
    let txn = unsafe { txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let app_id_res: DeltaResult<String> = unsafe { TryFromStringSlice::try_from_slice(&app_id) };
    with_transaction_id_impl(*txn, app_id_res, version).into_extern_result(&engine)
}

fn with_transaction_id_impl(
    txn: Transaction,
    app_id_res: DeltaResult<String>,
    version: i64,
) -> DeltaResult<Handle<ExclusiveTransaction>> {
    Ok(Box::new(txn.with_transaction_id(app_id_res?, version)).into())
}

/// Retrieves the version associated with an app_id from a snapshot.
///
/// # Returns
/// The version number if found, or an error of type `MissingDataError` when the app_id was not set
///
/// # Safety
/// Caller must ensure [valid][Handle#Validity] handles are provided for snapshot and engine. The
/// `app_id` string slice must be valid.
#[no_mangle]
pub unsafe extern "C" fn get_app_id_version(
    snapshot: Handle<SharedSnapshot>,
    app_id: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<OptionalValue<i64>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    let engine = unsafe { engine.as_ref() };
    let app_id_res = unsafe { String::try_from_slice(&app_id) };

    get_app_id_version_impl(snapshot, app_id_res, engine)
        .map(OptionalValue::from)
        .into_extern_result(&engine)
}

fn get_app_id_version_impl(
    snapshot: Arc<Snapshot>,
    app_id_res: DeltaResult<String>,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<Option<i64>> {
    snapshot.get_app_id_version(&app_id_res?, extern_engine.engine().as_ref())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::schema::schema_ref;
    use delta_kernel::Snapshot;
    use test_utils::setup_test_tables;

    use super::*;
    use crate::ffi_test_utils::{engine_handle_for_store, ok_or_panic};
    use crate::transaction::{commit, free_committed_transaction, transaction};
    use crate::{free_engine, free_snapshot, kernel_string_slice};

    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    async fn test_write_txn_actions() -> Result<(), Box<dyn std::error::Error>> {
        // create a simple table: one int column named 'number'
        let schema = schema_ref! { nullable "number": INTEGER };

        for (table_url, engine, store, _table_name) in
            setup_test_tables(schema, &[], None, "test_table").await?
        {
            let table_url_str = table_url.as_str();
            let default_engine_handle = engine_handle_for_store(store);

            // Start the transaction
            let txn = ok_or_panic(unsafe {
                transaction(
                    kernel_string_slice!(table_url_str),
                    default_engine_handle.shallow_copy(),
                )
            });

            // Add app ids
            let app_id1 = "app_id1";
            let app_id2 = "app_id2";
            let txn = ok_or_panic(unsafe {
                with_transaction_id(
                    txn,
                    kernel_string_slice!(app_id1),
                    1,
                    default_engine_handle.shallow_copy(),
                )
            });
            let txn = ok_or_panic(unsafe {
                with_transaction_id(
                    txn,
                    kernel_string_slice!(app_id2),
                    2,
                    default_engine_handle.shallow_copy(),
                )
            });

            // commit!
            let committed =
                ok_or_panic(unsafe { commit(txn, default_engine_handle.shallow_copy()) });
            unsafe { free_committed_transaction(committed) };

            let snapshot: Arc<Snapshot> = Snapshot::builder_for(table_url.clone())
                .at_version(1)
                .build(&engine)?;

            // Check versions
            assert_eq!(snapshot.get_app_id_version("app_id1", &engine)?, Some(1));
            assert_eq!(snapshot.get_app_id_version("app_id2", &engine)?, Some(2));
            assert_eq!(snapshot.get_app_id_version("app_id3", &engine)?, None);

            // Check versions through ffi handles. `get_app_id_version` borrows the handle, so
            // one handle serves all three calls and is freed once at the end.
            let snapshot_handle: Handle<SharedSnapshot> = snapshot.clone().into();
            let version1 = ok_or_panic(unsafe {
                get_app_id_version(
                    snapshot_handle.shallow_copy(),
                    kernel_string_slice!(app_id1),
                    default_engine_handle.shallow_copy(),
                )
            });
            assert_eq!(version1, OptionalValue::Some(1));

            let version2 = ok_or_panic(unsafe {
                get_app_id_version(
                    snapshot_handle.shallow_copy(),
                    kernel_string_slice!(app_id2),
                    default_engine_handle.shallow_copy(),
                )
            });
            assert_eq!(version2, OptionalValue::Some(2));

            let app_id3 = "app_id3";
            let version3 = ok_or_panic(unsafe {
                get_app_id_version(
                    snapshot_handle.shallow_copy(),
                    kernel_string_slice!(app_id3),
                    default_engine_handle.shallow_copy(),
                )
            });
            assert_eq!(version3, OptionalValue::None);

            unsafe { free_snapshot(snapshot_handle) };
            unsafe { free_engine(default_engine_handle) };
        }
        Ok(())
    }
}
