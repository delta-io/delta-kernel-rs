use std::sync::Arc;

use delta_kernel::{KernelResult, Snapshot};

use crate::error::ExternResult;
use crate::handle::Handle;
use crate::transaction::ExclusiveUpdateTableTransactionBuilder;
use crate::{
    ExternEngine, IntoExternResult, KernelStringSlice, OptionalValue, SharedExternEngine,
    SharedSnapshot, TryFromStringSlice,
};

/// Associates an app_id and version with a transaction. These will be applied to the table on
/// commit.
///
/// # Returns
/// A new handle to the update-table transaction builder.
///
/// # Safety
/// `builder` and `engine` must be [valid][Handle#Validity] handles, and `app_id` must be a valid
/// string slice. This call borrows `engine` and `app_id` and unconditionally consumes `builder`,
/// including on error.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_builder_with_transaction_id(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    app_id: KernelStringSlice,
    version: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveUpdateTableTransactionBuilder>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let app_id_res: KernelResult<String> = unsafe { TryFromStringSlice::try_from_slice(&app_id) };
    app_id_res
        .map(|app_id| Box::new(builder.with_transaction_id(app_id, version)).into())
        .into_extern_result(&engine)
}

/// Retrieves the version associated with an app_id from a snapshot.
///
/// # Returns
/// The version number if found, or [`OptionalValue::None`] when the app_id was not set.
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
    app_id_res: KernelResult<String>,
    extern_engine: &dyn ExternEngine,
) -> KernelResult<Option<i64>> {
    snapshot.get_app_id_version(&app_id_res?, extern_engine.engine().as_ref())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::schema::schema_ref;
    use delta_kernel::{KernelResult, Snapshot};
    use rstest::rstest;
    use test_utils::setup_test_tables;

    use super::*;
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{
        assert_extern_result_error_contains, engine_handle_for_store, ok_or_panic,
    };
    use crate::transaction::{
        KernelUpdateTableOperation, free_committed_transaction, new_update_table_txn_builder,
        update_table_txn_builder_build, update_table_txn_commit,
        update_table_txn_with_transaction_id,
    };
    use crate::{free_engine, free_snapshot, kernel_string_slice};

    #[cfg(feature = "default-engine-base")]
    #[rstest]
    #[case::builder(2, false)]
    #[case::transaction(0, false)]
    #[case::mixed(1, false)]
    #[case::duplicate_app_id_after_build(0, true)]
    #[case::duplicate_app_id_across_build(1, true)]
    #[tokio::test]
    async fn test_write_txn_actions(
        #[case] builder_ids: usize,
        #[case] duplicate_app_id: bool,
    ) -> KernelResult<()> {
        // create a simple table: one int column named 'number'
        let schema = schema_ref! { nullable "number": INTEGER };

        for (table_url, engine, store, _table_name) in
            setup_test_tables(schema, &[], None, "test_table").await?
        {
            let default_engine_handle = engine_handle_for_store(store);

            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            let snapshot_handle: Handle<SharedSnapshot> = snapshot.into();
            let engine_info = "test-engine/1.0";
            let mut builder = unsafe {
                ok_or_panic(new_update_table_txn_builder(
                    snapshot_handle.shallow_copy(),
                    KernelUpdateTableOperation::Write,
                    kernel_string_slice!(engine_info),
                    true,
                    default_engine_handle.shallow_copy(),
                ))
            };

            // Add app ids
            let app_id1 = "app_id1";
            let app_id2 = "app_id2";
            let ids = [
                (app_id1, 1),
                (if duplicate_app_id { app_id1 } else { app_id2 }, 2),
            ];
            for (app_id, version) in ids.iter().take(builder_ids) {
                builder = ok_or_panic(unsafe {
                    update_table_txn_builder_with_transaction_id(
                        builder,
                        kernel_string_slice!(app_id),
                        *version,
                        default_engine_handle.shallow_copy(),
                    )
                });
            }
            let mut txn = ok_or_panic(unsafe {
                update_table_txn_builder_build(builder, default_engine_handle.shallow_copy())
            });
            for (app_id, version) in ids.iter().skip(builder_ids) {
                txn = ok_or_panic(unsafe {
                    update_table_txn_with_transaction_id(
                        txn,
                        kernel_string_slice!(app_id),
                        *version,
                        default_engine_handle.shallow_copy(),
                    )
                });
            }
            unsafe { free_snapshot(snapshot_handle) };

            let result =
                unsafe { update_table_txn_commit(txn, default_engine_handle.shallow_copy()) };
            if duplicate_app_id {
                assert_extern_result_error_contains(
                    result,
                    FFIKernelError::GenericError,
                    "app_id app_id1",
                );
                assert_eq!(
                    Snapshot::builder_for(table_url).build(&engine)?.version(),
                    0
                );
                unsafe { free_engine(default_engine_handle) };
                continue;
            }
            let committed = ok_or_panic(result);
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
