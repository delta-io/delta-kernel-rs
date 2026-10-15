use delta_kernel::transaction::Transaction;
use delta_kernel::KernelResult;

use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::transaction::ExclusiveUpdateTableTransaction;
use crate::{kernel_string_slice, KernelStringSlice, NullableCvoid, SharedExternEngine};

/// Acknowledges that the connector generates and fills this table's Concurrent Identity Column
/// values before writing data files.
///
/// Required before requesting a write context for a table that has any CIC column; without it,
/// write-context creation fails with `InvalidTransactionState`.
///
/// # Safety
///
/// Caller is responsible for passing a valid transaction handle. The handle is borrowed and
/// mutated in place, NOT consumed: unlike the `with_*` transaction builders, `txn` stays valid
/// after this call and must still be freed by the caller.
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_ack_concurrent_identity_columns(
    mut txn: Handle<ExclusiveUpdateTableTransaction>,
) {
    let txn = unsafe { txn.as_mut() };
    txn.ack_concurrent_identity_columns();
}

/// Callback invoked once per top-level Concurrent Identity Column by
/// [`update_table_txn_visit_concurrent_identity_columns`].
///
/// `column_name` is the column's logical name; `sequence_id` points to the catalog sequence that
/// issues its values. `start` and `step` are the sequence's first value and non-zero increment;
/// `allow_explicit_insert` reports the classic `delta.identity.allowExplicitInsert` flag.
///
/// # Safety
///
/// `column_name` and `sequence_id` are valid only for the duration of the call.
pub type ConcurrentIdentityColumnVisitor = extern "C" fn(
    engine_context: NullableCvoid,
    column_name: KernelStringSlice,
    sequence_id: KernelStringSlice,
    start: i64,
    step: i64,
    allow_explicit_insert: bool,
);

/// Visits every top-level Concurrent Identity Column of this transaction's table, and returns how
/// many there were.
///
/// Either an error is returned before the first callback, or every CIC column is visited.
///
/// # Safety
///
/// Caller is responsible for passing valid transaction and engine handles, a valid
/// `engine_context` pointer passed through to each `visitor` invocation, and a valid `visitor`
/// function pointer. The `txn` handle is borrowed (not consumed).
#[no_mangle]
pub unsafe extern "C" fn update_table_txn_visit_concurrent_identity_columns(
    txn: Handle<ExclusiveUpdateTableTransaction>,
    engine: Handle<SharedExternEngine>,
    engine_context: NullableCvoid,
    visitor: ConcurrentIdentityColumnVisitor,
) -> ExternResult<usize> {
    let engine = unsafe { engine.as_ref() };
    let txn = unsafe { txn.as_ref() };
    visit_concurrent_identity_columns_impl(txn, engine_context, visitor).into_extern_result(&engine)
}

fn visit_concurrent_identity_columns_impl(
    txn: &Transaction,
    engine_context: NullableCvoid,
    visitor: ConcurrentIdentityColumnVisitor,
) -> KernelResult<usize> {
    let columns = txn.concurrent_identity_columns()?;
    for column in columns.values() {
        let column_name = column.column_name();
        let sequence_id = column.sequence_id();
        visitor(
            engine_context,
            kernel_string_slice!(column_name),
            kernel_string_slice!(sequence_id),
            column.start(),
            column.step(),
            column.allow_explicit_insert(),
        );
    }
    Ok(columns.len())
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::*;
    use crate::ffi_test_utils::{build_update_table_txn, ok_or_panic};
    use crate::tests::get_default_engine;
    use crate::transaction::free_update_table_txn;
    use crate::{free_engine, TryFromStringSlice};

    /// A table with column defaults but no concurrent identity columns.
    const NON_CIC_FIXTURE: &str = "../kernel/tests/data/table-with-column-defaults/";

    /// One visited CIC column, with the callback's borrowed slices copied into owned data.
    #[derive(Debug, PartialEq)]
    struct VisitedColumn {
        column_name: String,
        sequence_id: String,
        start: i64,
        step: i64,
        allow_explicit_insert: bool,
    }

    extern "C" fn collect_column(
        engine_context: NullableCvoid,
        column_name: KernelStringSlice,
        sequence_id: KernelStringSlice,
        start: i64,
        step: i64,
        allow_explicit_insert: bool,
    ) {
        let collected: *mut Vec<VisitedColumn> = engine_context
            .unwrap()
            .as_ptr()
            .cast::<Vec<VisitedColumn>>();
        let visited = unsafe {
            VisitedColumn {
                column_name: String::try_from_slice(&column_name).unwrap(),
                sequence_id: String::try_from_slice(&sequence_id).unwrap(),
                start,
                step,
                allow_explicit_insert,
            }
        };
        unsafe { (*collected).push(visited) };
    }

    /// A non-CIC table reports zero concurrent identity columns and its acknowledgement is a safe
    /// no-op.
    #[test]
    fn visit_reports_no_columns_for_non_cic_table() {
        let table_root = delta_kernel::try_parse_uri(NON_CIC_FIXTURE)
            .unwrap()
            .to_string();
        let engine = get_default_engine(&table_root);
        let txn = ok_or_panic(unsafe {
            build_update_table_txn(kernel_string_slice!(table_root), engine.shallow_copy())
        });

        let mut collected: Vec<VisitedColumn> = Vec::new();
        let count = ok_or_panic(unsafe {
            update_table_txn_visit_concurrent_identity_columns(
                txn.shallow_copy(),
                engine.shallow_copy(),
                NonNull::new((&mut collected as *mut Vec<VisitedColumn>).cast()),
                collect_column,
            )
        });
        assert_eq!(count, 0);
        assert!(collected.is_empty());

        // Acknowledging a table with no CIC columns is a harmless no-op that leaves `txn` usable.
        unsafe { update_table_txn_ack_concurrent_identity_columns(txn.shallow_copy()) };

        unsafe { free_update_table_txn(txn) };
        unsafe { free_engine(engine) };
    }
}
