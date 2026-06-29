//! FFI bindings for discovering a table's column defaults.
//!
//! Exposes the kernel's column-default API to engines through a single visitor,
//! [`EngineColumnDefaultVisitor`], driven by two entry points:
//! - [`visit_column_defaults`] over a transaction (the kernel's
//!   [`Transaction::column_defaults`](delta_kernel::transaction::Transaction::column_defaults),
//!   which enforces the `allowColumnDefaults` writer feature), and
//! - [`visit_schema_column_defaults`] over a schema (the kernel's
//!   [`StructField::column_default`](delta_kernel::schema::StructField::column_default)), which
//!   simply reports whatever defaults the schema's top-level fields declare.
//!
//! For each top-level column that declares a `CURRENT_DEFAULT`, the engine receives the
//! logical column name, the raw default SQL, whether the kernel could parse it, and -- when
//! parsable -- the evaluated literal as a [`SharedExpression`] handle it can read via
//! [`visit_expression`](crate::expressions::visit_expression). When the kernel cannot parse
//! the default, the engine falls back to the raw SQL.

use std::ffi::c_void;
use std::sync::Arc;

use delta_kernel::expressions::Expression;
use delta_kernel::schema::{ColumnDefault, StructType};
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Engine};

use super::ExclusiveTransaction;
use crate::error::{ExternResult, IntoExternResult};
use crate::expressions::SharedExpression;
use crate::handle::Handle;
use crate::{
    kernel_string_slice, ExternEngine, KernelStringSlice, OptionalValue, SharedExternEngine,
    SharedSchema,
};

/// Visitor invoked once per top-level column that declares a `CURRENT_DEFAULT`.
///
/// The engine allocates and owns this struct (and its `data` context), and it must outlive the
/// `visit_*_column_defaults` call it is passed to. Every callback runs synchronously inside that
/// call.
#[repr(C)]
pub struct EngineColumnDefaultVisitor {
    /// Opaque engine context, passed unchanged as the first argument to `visit_default`.
    pub data: *mut c_void,

    /// Called once per defaulted top-level column.
    ///
    /// - `data`:               the opaque [`EngineColumnDefaultVisitor::data`] context.
    /// - `name`:               logical column name. Borrowed; valid only for the duration of this
    ///   callback and must not be retained.
    /// - `raw_sql`:            the raw `CURRENT_DEFAULT` SQL. Always present -- the fallback when
    ///   the kernel cannot parse it. Borrowed; must not be retained.
    /// - `is_kernel_parsable`: `true` when the kernel parsed `raw_sql` into a literal the engine
    ///   can read from `default_expr`.
    /// - `default_expr`:       `Some(handle)` exactly when `is_kernel_parsable` is `true`; an
    ///   owned [`Handle<SharedExpression>`] wrapping an `Expression::Literal`. The engine takes
    ///   ownership and MUST free it with
    ///   [`free_kernel_expression`](crate::expressions::free_kernel_expression). `None` when the
    ///   default is not kernel-parsable -- the engine then evaluates `raw_sql` itself.
    pub visit_default: extern "C" fn(
        data: *mut c_void,
        name: KernelStringSlice,
        raw_sql: KernelStringSlice,
        is_kernel_parsable: bool,
        default_expr: OptionalValue<Handle<SharedExpression>>,
    ),
}

/// Visit the column defaults of `txn`'s table, invoking `visitor.visit_default` once per
/// top-level column that declares a `CURRENT_DEFAULT`. Returns the number of defaults visited.
///
/// This mirrors
/// [`Transaction::column_defaults`](delta_kernel::transaction::Transaction::column_defaults):
/// it requires the table to enable the `allowColumnDefaults` writer feature, and a default
/// declared on a table that does not enable it is an error (not silently ignored).
///
/// Neither the transaction handle nor the engine handle is consumed; the caller still owns both.
///
/// # Errors
///
/// Returns `ExternResult::Err` (allocated via the engine's error allocator) when the table
/// declares a default without enabling `allowColumnDefaults`, when a `CURRENT_DEFAULT` is
/// malformed (non-string metadata, or a non-`NULL` default on a non-primitive type), or in the
/// defensive case where a parsed default is unexpectedly non-literal.
///
/// # Safety
///
/// Caller is responsible for passing valid transaction and engine handles, and a `visitor` whose
/// `visit_default` pointer and `data` context outlive this call.
#[no_mangle]
pub unsafe extern "C" fn visit_column_defaults(
    txn: &Handle<ExclusiveTransaction>,
    engine: Handle<SharedExternEngine>,
    visitor: &mut EngineColumnDefaultVisitor,
) -> ExternResult<usize> {
    let txn = unsafe { txn.as_ref() };
    let extern_engine = unsafe { engine.as_ref() };
    visit_column_defaults_impl(txn, extern_engine, visitor).into_extern_result(&extern_engine)
}

fn visit_column_defaults_impl(
    txn: &Transaction,
    extern_engine: &dyn ExternEngine,
    visitor: &mut EngineColumnDefaultVisitor,
) -> DeltaResult<usize> {
    let engine = extern_engine.engine();
    let defaults = txn.column_defaults()?;
    let mut count = 0;
    for (name, col_default) in &defaults {
        emit_column_default(name, col_default, engine.as_ref(), visitor)?;
        count += 1;
    }
    Ok(count)
}

/// Visit the column defaults declared by `schema`'s top-level fields, invoking
/// `visitor.visit_default` once per field that declares a `CURRENT_DEFAULT`. Returns the number
/// of defaults visited.
///
/// This mirrors [`StructField::column_default`](delta_kernel::schema::StructField::column_default)
/// over each top-level field. Unlike [`visit_column_defaults`], it does NOT require the
/// `allowColumnDefaults` writer feature -- it reports whatever defaults the fields declare, which
/// lets an engine inspect a snapshot's logical schema before opening a transaction.
///
/// Neither the schema handle nor the engine handle is consumed; the caller still owns both.
///
/// # Errors
///
/// Returns `ExternResult::Err` (allocated via the engine's error allocator) when a
/// `CURRENT_DEFAULT` is malformed (non-string metadata, or a non-`NULL` default on a
/// non-primitive type), or in the defensive case where a parsed default is unexpectedly
/// non-literal.
///
/// # Safety
///
/// Caller is responsible for passing valid schema and engine handles, and a `visitor` whose
/// `visit_default` pointer and `data` context outlive this call.
#[no_mangle]
pub unsafe extern "C" fn visit_schema_column_defaults(
    schema: &Handle<SharedSchema>,
    engine: Handle<SharedExternEngine>,
    visitor: &mut EngineColumnDefaultVisitor,
) -> ExternResult<usize> {
    let schema = unsafe { schema.as_ref() };
    let extern_engine = unsafe { engine.as_ref() };
    visit_schema_column_defaults_impl(schema, extern_engine, visitor)
        .into_extern_result(&extern_engine)
}

fn visit_schema_column_defaults_impl(
    schema: &StructType,
    extern_engine: &dyn ExternEngine,
    visitor: &mut EngineColumnDefaultVisitor,
) -> DeltaResult<usize> {
    let engine = extern_engine.engine();
    let mut count = 0;
    for field in schema.fields() {
        if let Some(col_default) = field.column_default()? {
            emit_column_default(field.name(), &col_default, engine.as_ref(), visitor)?;
            count += 1;
        }
    }
    Ok(count)
}

/// Evaluate a single column default and hand it to the engine via `visitor.visit_default`.
///
/// A kernel-parsable default is evaluated to its literal and wrapped as a fresh
/// `Expression::Literal` handle whose ownership transfers to the engine; an unparsable default
/// passes `None` and the engine falls back to `raw_sql`.
fn emit_column_default(
    name: &str,
    col_default: &ColumnDefault<'_>,
    engine: &dyn Engine,
    visitor: &mut EngineColumnDefaultVisitor,
) -> DeltaResult<()> {
    let default_expr = match col_default.evaluate(engine)? {
        Some(scalar) => {
            let handle: Handle<SharedExpression> = Arc::new(Expression::literal(scalar)).into();
            OptionalValue::Some(handle)
        }
        None => OptionalValue::None,
    };
    let raw_sql = col_default.raw_sql();
    (visitor.visit_default)(
        visitor.data,
        kernel_string_slice!(name),
        kernel_string_slice!(raw_sql),
        col_default.is_kernel_parsable(),
        default_expr,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::c_void;

    use delta_kernel::expressions::{Expression, Scalar};
    use delta_kernel::schema::{ArrayType, DataType, StructField, StructType};
    use tempfile::tempdir;
    use test_utils::{create_table, engine_store_setup, schema_with_column_defaults};

    use super::{visit_column_defaults, visit_schema_column_defaults, EngineColumnDefaultVisitor};
    use crate::error::KernelError;
    use crate::expressions::{free_kernel_expression, SharedExpression};
    use crate::ffi_test_utils::{
        assert_extern_result_error_with_message, build_snapshot, ok_or_panic,
    };
    use crate::handle::Handle;
    use crate::tests::get_default_engine;
    use crate::transaction::{free_transaction, transaction};
    use crate::{
        free_engine, free_schema, free_snapshot, kernel_string_slice, logical_schema,
        KernelStringSlice, OptionalValue, SharedExternEngine, TryFromStringSlice, Url,
    };

    /// What a `visit_default` callback observed for one column.
    #[derive(Debug)]
    struct RecordedDefault {
        name: String,
        raw_sql: String,
        is_kernel_parsable: bool,
        /// The literal extracted from `default_expr`, or `None` when the default was not
        /// kernel-parsable (and thus no expression handle was passed).
        value: Option<Scalar>,
    }

    /// `EngineColumnDefaultVisitor::visit_default` implementation that records each callback into
    /// the `Vec<RecordedDefault>` behind `data`, reading the literal out of the expression handle
    /// (and freeing it, as the engine is required to).
    extern "C" fn record_default(
        data: *mut c_void,
        name: KernelStringSlice,
        raw_sql: KernelStringSlice,
        is_kernel_parsable: bool,
        default_expr: OptionalValue<Handle<SharedExpression>>,
    ) {
        let recorded = unsafe { &mut *(data as *mut Vec<RecordedDefault>) };
        let name = unsafe { String::try_from_slice(&name) }.unwrap();
        let raw_sql = unsafe { String::try_from_slice(&raw_sql) }.unwrap();
        let value = match default_expr {
            OptionalValue::Some(handle) => {
                let scalar = match unsafe { handle.as_ref() } {
                    Expression::Literal(scalar) => Some(scalar.clone()),
                    _ => None,
                };
                unsafe { free_kernel_expression(handle) };
                scalar
            }
            OptionalValue::None => None,
        };
        recorded.push(RecordedDefault {
            name,
            raw_sql,
            is_kernel_parsable,
            value,
        });
    }

    /// The four-column base schema (`a`, `b`, `c`, `d`) most column-default tests build on.
    fn base_schema() -> StructType {
        StructType::try_new(vec![
            StructField::nullable("a", DataType::INTEGER),
            StructField::nullable("b", DataType::INTEGER),
            StructField::nullable("c", DataType::TIMESTAMP),
            StructField::nullable("d", DataType::INTEGER),
        ])
        .unwrap()
    }

    /// Build a table from `base` whose listed columns carry the given `CURRENT_DEFAULT`s, then
    /// return an FFI engine handle and the table's local path.
    async fn setup_defaults_table(
        dir_url: &Url,
        name: &str,
        base: StructType,
        defaults: HashMap<&str, &str>,
        writer_features: Vec<&str>,
    ) -> (Handle<SharedExternEngine>, String) {
        let schema = schema_with_column_defaults(&base, defaults).unwrap();
        let (store, _engine, table_location) = engine_store_setup(name, Some(dir_url));
        let table_url = create_table(
            store,
            table_location,
            schema,
            &[],    /* partition_columns */
            true,   /* use_37_protocol */
            vec![], /* reader_features */
            writer_features,
        )
        .await
        .unwrap();
        let table_path = table_url.to_file_path().unwrap();
        let table_path = table_path.to_str().unwrap().to_string();
        let engine = get_default_engine(&table_path);
        (engine, table_path)
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_visit_column_defaults_exposes_name_raw_sql_and_parsed_value() {
        let tmp = tempdir().unwrap();
        let dir_url = Url::from_directory_path(tmp.path()).unwrap();
        let (engine, table_path) = setup_defaults_table(
            &dir_url,
            "ffi_col_defaults",
            base_schema(),
            // b: parsable literal, c: non-parsable function call, d: typed NULL.
            HashMap::from([("b", "1337"), ("c", "current_timestamp()"), ("d", "NULL")]),
            vec!["allowColumnDefaults"],
        )
        .await;

        let txn = ok_or_panic(unsafe {
            transaction(kernel_string_slice!(table_path), engine.shallow_copy())
        });

        let mut recorded: Vec<RecordedDefault> = Vec::new();
        let mut visitor = EngineColumnDefaultVisitor {
            data: &mut recorded as *mut _ as *mut c_void,
            visit_default: record_default,
        };
        let count = ok_or_panic(unsafe {
            visit_column_defaults(&txn, engine.shallow_copy(), &mut visitor)
        });
        assert_eq!(count, 3, "only b, c, and d declare a default (a does not)");

        recorded.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(recorded[0].name, "b");
        assert_eq!(recorded[0].raw_sql, "1337");
        assert!(recorded[0].is_kernel_parsable);
        assert_eq!(recorded[0].value, Some(Scalar::Integer(1337)));

        assert_eq!(recorded[1].name, "c");
        assert_eq!(recorded[1].raw_sql, "current_timestamp()");
        assert!(!recorded[1].is_kernel_parsable);
        assert_eq!(recorded[1].value, None);

        assert_eq!(recorded[2].name, "d");
        assert_eq!(recorded[2].raw_sql, "NULL");
        assert!(recorded[2].is_kernel_parsable);
        assert_eq!(recorded[2].value, Some(Scalar::Null(DataType::INTEGER)));

        unsafe { free_transaction(txn) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_visit_schema_column_defaults_reports_defaults_without_feature_gate() {
        let tmp = tempdir().unwrap();
        let dir_url = Url::from_directory_path(tmp.path()).unwrap();
        // The table does NOT enable `allowColumnDefaults`, yet column `b` declares a default. The
        // schema variant reports it regardless (unlike the transaction variant, which would error).
        let (engine, table_path) = setup_defaults_table(
            &dir_url,
            "ffi_col_defaults_schema",
            base_schema(),
            HashMap::from([("b", "1337")]),
            vec![],
        )
        .await;

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_path), engine.shallow_copy()) };
        let schema = unsafe { logical_schema(snapshot.shallow_copy()) };

        let mut recorded: Vec<RecordedDefault> = Vec::new();
        let mut visitor = EngineColumnDefaultVisitor {
            data: &mut recorded as *mut _ as *mut c_void,
            visit_default: record_default,
        };
        let count = ok_or_panic(unsafe {
            visit_schema_column_defaults(&schema, engine.shallow_copy(), &mut visitor)
        });
        assert_eq!(count, 1);
        assert_eq!(recorded[0].name, "b");
        assert_eq!(recorded[0].raw_sql, "1337");
        assert!(recorded[0].is_kernel_parsable);
        assert_eq!(recorded[0].value, Some(Scalar::Integer(1337)));

        unsafe { free_schema(schema) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_visit_schema_column_defaults_errors_on_malformed_default() {
        let tmp = tempdir().unwrap();
        let dir_url = Url::from_directory_path(tmp.path()).unwrap();
        // A non-NULL default on a non-primitive (array) column: `create_table` accepts it, but
        // `column_default()` rejects it on read -- the schema variant's only error route.
        let base = StructType::try_new(vec![StructField::nullable(
            "arr",
            ArrayType::new(DataType::INTEGER, true),
        )])
        .unwrap();
        let (engine, table_path) = setup_defaults_table(
            &dir_url,
            "ffi_col_defaults_schema_err",
            base,
            HashMap::from([("arr", "ARRAY(1)")]),
            vec!["allowColumnDefaults"],
        )
        .await;

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_path), engine.shallow_copy()) };
        let schema = unsafe { logical_schema(snapshot.shallow_copy()) };

        let mut recorded: Vec<RecordedDefault> = Vec::new();
        let mut visitor = EngineColumnDefaultVisitor {
            data: &mut recorded as *mut _ as *mut c_void,
            visit_default: record_default,
        };
        let result =
            unsafe { visit_schema_column_defaults(&schema, engine.shallow_copy(), &mut visitor) };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);
        assert!(recorded.is_empty(), "error must abort before any callback fires");

        unsafe { free_schema(schema) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_visit_column_defaults_errors_when_feature_not_enabled() {
        let tmp = tempdir().unwrap();
        let dir_url = Url::from_directory_path(tmp.path()).unwrap();
        // A default is present but the table does not enable `allowColumnDefaults`.
        let (engine, table_path) = setup_defaults_table(
            &dir_url,
            "ffi_col_defaults_no_feature",
            base_schema(),
            HashMap::from([("b", "1337")]),
            vec![],
        )
        .await;

        let txn = ok_or_panic(unsafe {
            transaction(kernel_string_slice!(table_path), engine.shallow_copy())
        });

        let mut recorded: Vec<RecordedDefault> = Vec::new();
        let mut visitor = EngineColumnDefaultVisitor {
            data: &mut recorded as *mut _ as *mut c_void,
            visit_default: record_default,
        };
        let result = unsafe { visit_column_defaults(&txn, engine.shallow_copy(), &mut visitor) };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);

        unsafe { free_transaction(txn) };
        unsafe { free_engine(engine) };
    }
}
