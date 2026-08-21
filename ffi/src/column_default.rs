//! FFI surface for column defaults (the `allowColumnDefaults` writer feature).
//!
//! The kernel reads and validates `CURRENT_DEFAULT` metadata but never materializes a default: the
//! connector fills every omitted column itself. This module exposes exactly that contract, so the
//! write flow from C is:
//!
//! ```text
//! transaction(path, engine)
//! transaction_visit_top_level_column_defaults(txn, engine, ctx, visitor)
//!         // default_expression non-null -> a parsed literal; read it with visit_expression, then
//!         //                                 free_kernel_expression
//!         // default_expression null     -> a non-literal (e.g. current_timestamp()); use raw_sql
//! transaction_ack_column_defaults(txn)                   
//! get_unpartitioned_write_context(txn, engine)           
//! ```

use std::sync::Arc;

use delta_kernel::expressions::Expression;
use delta_kernel::transaction::Transaction;
use delta_kernel::DeltaResult;

use crate::error::{ExternResult, IntoExternResult};
use crate::expressions::SharedExpression;
use crate::handle::Handle;
use crate::transaction::ExclusiveTransaction;
use crate::{kernel_string_slice, KernelStringSlice, NullableCvoid, SharedExternEngine};

/// Acknowledges that the connector materializes this table's column defaults before writing data
/// files.
///
/// Required before requesting a write context for a table that enables `allowColumnDefaults` and
/// declares at least one default; without it, write-context creation fails. Visiting the defaults
/// does not imply the acknowledgement.
///
/// # Safety
///
/// Caller is responsible for passing a valid transaction handle. The handle is borrowed and
/// mutated in place, NOT consumed: unlike the `with_*` transaction builders, `txn` stays valid
/// after this call and must still be freed by the caller.
#[no_mangle]
pub unsafe extern "C" fn transaction_ack_column_defaults(mut txn: Handle<ExclusiveTransaction>) {
    let txn = unsafe { txn.as_mut() };
    txn.ack_column_defaults();
}

/// Callback invoked once per top-level column default by
/// [`transaction_visit_top_level_column_defaults`].
///
/// `default_expression` is an owned handle to the parsed `Expression::Literal` when the kernel
/// parsed the default as a literal, and `None` for a non-literal (e.g. `current_timestamp()`) or
/// otherwise unparseable default `raw_sql` in that case.
///
/// SAFETY:
///   engine MUST released the `default_expression` with `free_kernel_expression`
pub type ColumnDefaultVisitor = extern "C" fn(
    engine_context: NullableCvoid,
    name: KernelStringSlice,
    raw_sql: KernelStringSlice,
    default_expression: Option<Handle<SharedExpression>>,
);

/// Visits every top-level column of this transaction's table that declares a default, and returns
/// how many there were. Each literal default is emitted as an owned [`SharedExpression`] handle the
/// engine must release with `free_kernel_expression`; a non-literal default is emitted with a null
/// handle (use `raw_sql`).
///
/// Returns an error if a stored default parsed to a non-literal expression.
///
/// # Safety
///
/// Caller is responsible for passing valid transaction and engine handles, a valid
/// `engine_context` pointer passed through to each `visitor` invocation, and a valid `visitor`
/// function pointer.
#[no_mangle]
pub unsafe extern "C" fn transaction_visit_top_level_column_defaults(
    txn: Handle<ExclusiveTransaction>,
    engine: Handle<SharedExternEngine>,
    engine_context: NullableCvoid,
    visitor: ColumnDefaultVisitor,
) -> ExternResult<usize> {
    let engine = unsafe { engine.as_ref() };
    let txn = unsafe { txn.as_ref() };
    visit_top_level_column_defaults_impl(txn, engine_context, visitor).into_extern_result(&engine)
}

fn visit_top_level_column_defaults_impl(
    txn: &Transaction,
    engine_context: NullableCvoid,
    visitor: ColumnDefaultVisitor,
) -> DeltaResult<usize> {
    let defaults = txn.top_level_column_defaults()?;
    for (name, column_default) in &defaults {
        let name = name.as_str();
        let raw_sql = column_default.raw_sql();
        let default_expression: Option<Handle<SharedExpression>> = column_default
            .to_scalar()?
            .map(|scalar| Arc::new(Expression::literal(scalar)).into());
        visitor(
            engine_context,
            kernel_string_slice!(name),
            kernel_string_slice!(raw_sql),
            default_expression,
        );
    }
    Ok(defaults.len())
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use delta_kernel::expressions::Scalar;

    use super::*;
    use crate::expressions::free_kernel_expression;
    use crate::ffi_test_utils::ok_or_panic;
    use crate::tests::get_default_engine;
    use crate::transaction::{free_transaction, transaction};
    use crate::{free_engine, TryFromStringSlice};

    const FIXTURE: &str = "../kernel/tests/data/table-with-column-defaults/";

    /// One visited column default, with the callback's borrowed slices copied into owned Strings.
    #[derive(Debug, PartialEq)]
    struct VisitedDefault {
        name: String,
        raw_sql: String,
        literal: Option<Scalar>,
    }

    extern "C" fn collect_default(
        engine_context: NullableCvoid,
        name: KernelStringSlice,
        raw_sql: KernelStringSlice,
        default_expression: Option<Handle<SharedExpression>>,
    ) {
        let collected: *mut Vec<VisitedDefault> = engine_context
            .unwrap()
            .as_ptr()
            .cast::<Vec<VisitedDefault>>();
        let literal = default_expression.map(|handle| {
            let scalar = match unsafe { handle.as_ref() } {
                Expression::Literal(scalar) => scalar.clone(),
                other => panic!("expected a literal expression, got {other:?}"),
            };
            unsafe { free_kernel_expression(handle) };
            scalar
        });
        let visited = unsafe {
            VisitedDefault {
                name: String::try_from_slice(&name).unwrap(),
                raw_sql: String::try_from_slice(&raw_sql).unwrap(),
                literal,
            }
        };
        unsafe { (*collected).push(visited) };
    }

    /// Visit `table_path`'s top-level defaults through the FFI, returning the reported count and
    /// everything the callback saw. Panics if any FFI call fails.
    fn visit_defaults_of(table_path: &str) -> (usize, Vec<VisitedDefault>) {
        let table_root = delta_kernel::try_parse_uri(table_path).unwrap().to_string();
        let engine = get_default_engine(&table_root);
        let txn = unsafe {
            ok_or_panic(transaction(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

        let mut collected: Vec<VisitedDefault> = Vec::new();
        let count = unsafe {
            ok_or_panic(transaction_visit_top_level_column_defaults(
                txn.shallow_copy(),
                engine.shallow_copy(),
                NonNull::new((&mut collected as *mut Vec<VisitedDefault>).cast()),
                collect_default,
            ))
        };

        unsafe { free_transaction(txn) };
        unsafe { free_engine(engine) };
        (count, collected)
    }

    #[test]
    fn visit_reports_every_top_level_default() {
        let (count, mut visited) = visit_defaults_of(FIXTURE);
        // The callback order is unspecified, so sort before asserting the expected set.
        visited.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(count, 3);
        assert_eq!(
            visited,
            vec![
                VisitedDefault {
                    name: "amount".to_string(),
                    raw_sql: "4.95".to_string(),
                    literal: Some(Scalar::decimal(495, 10, 2).unwrap()),
                },
                VisitedDefault {
                    name: "status".to_string(),
                    raw_sql: "'pending'".to_string(),
                    literal: Some(Scalar::from("pending")),
                },
                VisitedDefault {
                    name: "ts".to_string(),
                    raw_sql: "current_timestamp()".to_string(),
                    literal: None,
                },
            ]
        );
    }
}
