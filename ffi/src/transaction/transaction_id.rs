use std::sync::Arc;

use crate::error::ExternResult;
use crate::handle::Handle;
use crate::transaction::ExclusiveTransaction;
use crate::{
    ExternEngine, IntoExternResult, KernelStringSlice, OptionalValue, SharedExternEngine,
    SharedSnapshot, TryFromStringSlice,
};

use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Snapshot};

/// Associates an app_id and version with a transaction. These will be applied to the table on commit.
///
/// # Returns
/// A new handle to the transaction that will set the `app_id` version to `version` on commit
///
/// # Safety
/// Caller is responsible for passing [valid][Handle#Validity] handles. The `app_id` string slice must be valid.
/// CONSUMES TRANSACTION
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
/// Caller must ensure [valid][Handle#Validity] handles are provided for snapshot and engine. The `app_id`
/// string slice must be valid.
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
    use super::*;

    use crate::ffi_test_utils::ok_or_panic;
    use crate::kernel_string_slice;
    use crate::tests::get_default_engine;
    use crate::transaction::{commit, transaction};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::Snapshot;
    use std::sync::Arc;
    use tempfile::tempdir;
    use test_utils::setup_test_tables;
    use url::Url;

    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // FIXME: re-enable miri (can't call foreign function `linkat` on OS `linux`)
    async fn test_write_txn_actions() -> Result<(), Box<dyn std::error::Error>> {
        // Create a temporary local directory for use during this test
        let tmp_test_dir = tempdir()?;
        let tmp_dir_local_url = Url::from_directory_path(tmp_test_dir.path()).unwrap();

        // create a simple table: one int column named 'number'
        let schema = Arc::new(
            StructType::try_new(vec![StructField::nullable("number", DataType::INTEGER)]).unwrap(),
        );

        for (table_url, engine, _store, _table_name) in
            setup_test_tables(schema, &[], Some(&tmp_dir_local_url), "test_table").await?
        {
            let table_path = table_url.to_file_path().unwrap();
            let table_path_str = table_path.to_str().unwrap();
            let default_engine_handle = get_default_engine(table_path_str);

            // Start the transaction
            let txn = ok_or_panic(unsafe {
                transaction(
                    kernel_string_slice!(table_path_str),
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
            ok_or_panic(unsafe { commit(txn, default_engine_handle.shallow_copy()) });

            let snapshot: Arc<Snapshot> = Snapshot::builder_for(table_url.clone())
                .at_version(1)
                .build(&engine)?;

            // Check versions
            assert_eq!(
                snapshot.clone().get_app_id_version("app_id1", &engine)?,
                Some(1)
            );
            assert_eq!(
                snapshot.clone().get_app_id_version("app_id2", &engine)?,
                Some(2)
            );
            assert_eq!(
                snapshot.clone().get_app_id_version("app_id3", &engine)?,
                None
            );

            // Check versions through ffi handles
            let version1 = ok_or_panic(unsafe {
                get_app_id_version(
                    Handle::from(snapshot.clone()),
                    kernel_string_slice!(app_id1),
                    default_engine_handle.shallow_copy(),
                )
            });
            assert_eq!(version1, OptionalValue::Some(1));

            let version2 = ok_or_panic(unsafe {
                get_app_id_version(
                    Handle::from(snapshot.clone()),
                    kernel_string_slice!(app_id2),
                    default_engine_handle.shallow_copy(),
                )
            });
            assert_eq!(version2, OptionalValue::Some(2));

            let app_id3 = "app_id3";
            let version3 = ok_or_panic(unsafe {
                get_app_id_version(
                    Handle::from(snapshot.clone()),
                    kernel_string_slice!(app_id3),
                    default_engine_handle.shallow_copy(),
                )
            });
            assert_eq!(version3, OptionalValue::None);
        }
        Ok(())
    }
}
