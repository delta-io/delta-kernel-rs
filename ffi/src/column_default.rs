//! FFI surface for column defaults (the `allowColumnDefaults` writer feature).
//!
//! The kernel reads and validates `CURRENT_DEFAULT` metadata but never materializes a default: the
//! connector fills every omitted column itself. This module exposes exactly that contract, so the
//! write flow from C is:
//!
//! ```text
//! snapshot_has_column_defaults(snapshot)                 // optional cheap branch
//! transaction(path, engine)
//! transaction_visit_top_level_column_defaults(txn, engine, ctx, visitor)
//!         // Literal  -> use parsed_value
//!         // NonLiteral -> evaluate raw_sql with the engine's own SQL evaluator
//! transaction_ack_column_defaults(txn)                   // REQUIRED before a write context
//! get_unpartitioned_write_context(txn, engine)           // errors before the ack
//! ```
//!
//! A default's value crosses as text plus a [`CColumnDefaultKind`] tag rather than as a repr(C)
//! scalar union, following the `CStringMap` precedent. The column's [`DataType`] is not carried
//! anywhere: the engine already received every column's type from
//! [`visit_schema`](crate::schema::visit_schema) keyed by the same name, so it joins on the name to
//! recover the type (including a decimal's precision and scale).
//!
//! # Parsed-value encoding
//!
//! A literal is serialized with [`serialize_partition_value`], the protocol's encoding of a typed
//! scalar as a string -- the same encoding `visit_partition_values` hands the engine, so a
//! connector already knows how to read it: strings verbatim (`pending`, not `'pending'`), decimals
//! at the column's scale (`4.95`), dates as `YYYY-MM-DD`, timestamps as ISO-8601, booleans as
//! `true` / `false`.
//!
//! That encoding has no representation for a NULL, an empty string, or empty binary, so a literal
//! of any of those three carries **no** parsed value (a NULL pointer from the getter, an empty
//! slice in the visitor) even though `kind` is [`CColumnDefaultKind::Literal`]. `raw_sql`
//! distinguishes them: `NULL` vs `''` vs `X''`.
//!
//! [`DataType`]: delta_kernel::schema::DataType
//! [`serialize_partition_value`]: delta_kernel::partition::serialization::serialize_partition_value

use delta_kernel::expressions::ColumnName;
use delta_kernel::partition::serialization::serialize_partition_value;
use delta_kernel::schema::{ColumnDefault, StructType};
use delta_kernel::transaction::Transaction;
use delta_kernel::DeltaResult;

use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::transaction::ExclusiveTransaction;
use crate::{
    kernel_string_slice, AllocateStringFn, KernelStringSlice, NullableCvoid, OptionalValue,
    SharedExternEngine, SharedSchema, SharedSnapshot, TryFromStringSlice,
};

/// Tells the engine how to read a column default's parsed value.
///
/// `NonLiteral` means the kernel's built-in SQL parser could not reduce the default to a literal
/// (e.g. `current_timestamp()`), so the engine must evaluate the raw SQL itself.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CColumnDefaultKind {
    /// The kernel could not parse the default as a literal; evaluate the raw SQL instead.
    NonLiteral = 0,
    /// The kernel parsed the default as a literal, rendered as text.
    Literal = 1,
}

impl CColumnDefaultKind {
    /// Renders `column_default` as the FFI kind tag plus the literal's serialized text.
    ///
    /// The text is `None` for a non-literal default, and also for a literal the protocol
    /// serializes to nothing -- see [Parsed-value encoding](self#parsed-value-encoding).
    ///
    /// # Errors
    ///
    /// Propagates any error from [`ColumnDefault::to_scalar`] or from serializing the literal.
    fn render(column_default: &ColumnDefault<'_>) -> DeltaResult<(Self, Option<String>)> {
        Ok(match column_default.to_scalar()? {
            Some(scalar) => (Self::Literal, serialize_partition_value(&scalar)?),
            None => (Self::NonLiteral, None),
        })
    }
}

/// One field's column default, with every string allocated in the engine's memory.
///
/// The engine owns `raw_sql` and `parsed_value` and must free both.
#[repr(C)]
pub struct KernelColumnDefault {
    /// The `CURRENT_DEFAULT` metadata verbatim, as the table author wrote it.
    pub raw_sql: NullableCvoid,
    /// Whether the kernel parsed `raw_sql` into a literal.
    pub kind: CColumnDefaultKind,
    /// The parsed literal, encoded as described in the [module
    /// docs](self#parsed-value-encoding). NULL for a non-literal default, and also for a literal
    /// with no serialized form (NULL, the empty string, empty binary) -- read `raw_sql` then.
    pub parsed_value: NullableCvoid,
}

/// Reports the column default declared on the field at `field_path` in `schema`, or
/// [`OptionalValue::None`] when that field declares none.
///
/// `field_path` is a dot-separated path into nested structs, split naively -- a field name that
/// itself contains a dot cannot be addressed. The field's data type is deliberately not returned;
/// the caller already has it from [`visit_schema`](crate::schema::visit_schema) and joins on
/// `field_path`.
///
/// The returned [`KernelColumnDefault`]'s strings are allocated with `allocate_fn` and owned by the
/// caller.
///
/// Returns an error if `field_path` does not resolve to a field of `schema`, if the field's
/// `CURRENT_DEFAULT` metadata is malformed, or if a parsed literal cannot be serialized.
///
/// # Safety
///
/// Caller is responsible for passing valid schema and engine handles and a valid `field_path`
/// slice.
#[no_mangle]
pub unsafe extern "C" fn schema_field_column_default(
    schema: Handle<SharedSchema>,
    field_path: KernelStringSlice,
    allocate_fn: AllocateStringFn,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<OptionalValue<KernelColumnDefault>> {
    let engine = unsafe { engine.as_ref() };
    let schema = unsafe { schema.as_ref() };
    let field_path: DeltaResult<&str> = unsafe { TryFromStringSlice::try_from_slice(&field_path) };
    schema_field_column_default_impl(schema, field_path, allocate_fn).into_extern_result(&engine)
}

fn schema_field_column_default_impl(
    schema: &StructType,
    field_path: DeltaResult<&str>,
    allocate_fn: AllocateStringFn,
) -> DeltaResult<OptionalValue<KernelColumnDefault>> {
    let field = schema.field_at(&ColumnName::from_naive_str_split(field_path?))?;
    let Some(column_default) = field.column_default()? else {
        return Ok(OptionalValue::None);
    };
    let raw_sql = column_default.raw_sql();
    let (kind, parsed) = CColumnDefaultKind::render(&column_default)?;
    Ok(OptionalValue::Some(KernelColumnDefault {
        raw_sql: allocate_fn(kernel_string_slice!(raw_sql)),
        kind,
        parsed_value: match parsed.as_deref() {
            Some(parsed) => allocate_fn(kernel_string_slice!(parsed)),
            None => None,
        },
    }))
}

/// Reports whether this snapshot's logical schema declares a column default on any field, nested
/// fields included.
///
/// This reflects schema metadata only: a `CURRENT_DEFAULT` present without the
/// `allowColumnDefaults` writer feature is orphaned metadata and still reports `true`. It is
/// therefore a conservative pre-check -- when it returns `false`, no write needs
/// [`transaction_ack_column_defaults`]; when it returns `true`, the ack may still be unnecessary.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_has_column_defaults(snapshot: Handle<SharedSnapshot>) -> bool {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot.has_column_defaults()
}

/// Acknowledges that the connector materializes this table's column defaults before writing data
/// files.
///
/// Required before requesting a write context for a table that enables `allowColumnDefaults` and
/// declares at least one default; without it, write-context creation fails. Visiting the defaults
/// does not imply the acknowledgement. Records the responsibility only -- the kernel never applies
/// a default itself.
///
/// # Safety
///
/// Caller is responsible for passing a valid transaction handle.
#[no_mangle]
pub unsafe extern "C" fn transaction_ack_column_defaults(mut txn: Handle<ExclusiveTransaction>) {
    let txn = unsafe { txn.as_mut() };
    txn.ack_column_defaults();
}

/// Callback invoked once per top-level column default by
/// [`transaction_visit_top_level_column_defaults`].
///
/// `parsed_value` carries the encoding described in the [module docs](self#parsed-value-encoding),
/// and is an empty slice for a non-literal default as well as for a literal with no serialized form
/// (NULL, the empty string, empty binary) -- read `raw_sql` then. All slices are only valid for the
/// duration of the call; copy anything the engine needs to keep.
pub type ColumnDefaultVisitor = extern "C" fn(
    engine_context: NullableCvoid,
    name: KernelStringSlice,
    raw_sql: KernelStringSlice,
    kind: CColumnDefaultKind,
    parsed_value: KernelStringSlice,
);

/// Visits every top-level column of this transaction's table that declares a default, and returns
/// how many there were.
///
/// Entries are visited in sorted name order, so the callback sequence is deterministic across runs.
/// Nested defaults are not visited (a kernel limitation), and neither are defaults on a table that
/// does not enable `allowColumnDefaults` -- both cases simply produce fewer callbacks. A table with
/// no visitable default yields no callbacks and returns `0`.
///
/// This does not acknowledge the defaults; call [`transaction_ack_column_defaults`] for that.
///
/// Returns an error if the kernel parsed a default into something other than a literal, or if a
/// parsed literal cannot be serialized.
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
    // `ColumnDefault` borrows the column's `DataType` from the snapshot, so render owned text per
    // entry and let the slices live only as long as the callback.
    let defaults = txn.top_level_column_defaults()?;
    let mut entries: Vec<_> = defaults.iter().collect();
    entries.sort_by_key(|(name, _)| *name);
    for (name, column_default) in &entries {
        let name = name.as_str();
        let raw_sql = column_default.raw_sql();
        let (kind, parsed) = CColumnDefaultKind::render(column_default)?;
        let parsed = parsed.as_deref().unwrap_or_default();
        visitor(
            engine_context,
            kernel_string_slice!(name),
            kernel_string_slice!(raw_sql),
            kind,
            kernel_string_slice!(parsed),
        );
    }
    Ok(entries.len())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::ptr::NonNull;
    use std::sync::Arc;

    use delta_kernel::schema::{
        ColumnMetadataKey, DataType, MetadataValue, StructField, StructType,
    };
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::error::KernelError;
    use crate::ffi_test_utils::{
        allocate_str, assert_extern_result_error_with_message, build_snapshot, ok_or_panic,
        recover_string, setup_snapshot,
    };
    use crate::tests::get_default_engine;
    use crate::transaction::{free_transaction, transaction};
    use crate::{free_engine, free_schema, free_snapshot};

    /// The hand-authored fixture whose schema declares one literal string default, one
    /// non-literal default, and one literal decimal default (plus an `id` column with none).
    const FIXTURE: &str = "../kernel/tests/data/table-with-column-defaults/";
    /// A table that enables `deletionVectors` and declares no column default.
    const FIXTURE_NO_DEFAULTS: &str = "../kernel/tests/data/table-with-dv-small/";

    /// One visited column default, with the callback's borrowed slices copied into owned Strings.
    #[derive(Debug, PartialEq, Eq)]
    struct VisitedDefault {
        name: String,
        raw_sql: String,
        kind: CColumnDefaultKind,
        parsed_value: String,
    }

    extern "C" fn collect_default(
        engine_context: NullableCvoid,
        name: KernelStringSlice,
        raw_sql: KernelStringSlice,
        kind: CColumnDefaultKind,
        parsed_value: KernelStringSlice,
    ) {
        let collected: *mut Vec<VisitedDefault> = engine_context
            .unwrap()
            .as_ptr()
            .cast::<Vec<VisitedDefault>>();
        let visited = unsafe {
            VisitedDefault {
                name: String::try_from_slice(&name).unwrap(),
                raw_sql: String::try_from_slice(&raw_sql).unwrap(),
                kind,
                parsed_value: String::try_from_slice(&parsed_value).unwrap(),
            }
        };
        unsafe { (*collected).push(visited) };
    }

    /// Visit `table_path`'s top-level defaults through the FFI, returning the reported count and
    /// everything the callback saw. Panics if any FFI call fails.
    fn visit_defaults_of(table_path: &str) -> (usize, Vec<VisitedDefault>) {
        let table_root = canonical_url(table_path);
        let engine = get_default_engine(&table_root);
        let txn = unsafe {
            ok_or_panic(transaction(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

        let collected: *mut Vec<VisitedDefault> = Box::into_raw(Box::default());
        let count = unsafe {
            ok_or_panic(transaction_visit_top_level_column_defaults(
                txn.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::new_unchecked(collected.cast())),
                collect_default,
            ))
        };
        let collected = *unsafe { Box::from_raw(collected) };

        unsafe { free_transaction(txn) };
        unsafe { free_engine(engine) };
        (count, collected)
    }

    fn canonical_url(table_path: &str) -> String {
        let path = std::fs::canonicalize(table_path).unwrap();
        Url::from_directory_path(path).unwrap().to_string()
    }

    /// A nullable field carrying `raw_sql` as its `CURRENT_DEFAULT` metadata.
    fn field_with_default(
        name: &str,
        data_type: impl Into<DataType>,
        raw_sql: MetadataValue,
    ) -> StructField {
        StructField::nullable(name, data_type).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            raw_sql,
        )])
    }

    /// The schema the per-field getter tests probe: a literal default, a literal with no
    /// serialized form, a non-literal default, a field with no default, a malformed (non-string)
    /// default, and a nested default.
    fn getter_schema() -> Handle<SharedSchema> {
        let nested = StructType::try_new([field_with_default(
            "inner",
            DataType::INTEGER,
            MetadataValue::String("42".to_string()),
        )])
        .unwrap();
        let schema = StructType::try_new([
            StructField::nullable("plain", DataType::INTEGER),
            field_with_default(
                "literal",
                DataType::STRING,
                MetadataValue::String("'pending'".to_string()),
            ),
            field_with_default(
                "null_literal",
                DataType::INTEGER,
                MetadataValue::String("NULL".to_string()),
            ),
            field_with_default(
                "non_literal",
                DataType::TIMESTAMP,
                MetadataValue::String("current_timestamp()".to_string()),
            ),
            field_with_default("malformed", DataType::INTEGER, MetadataValue::Number(7)),
            StructField::nullable("outer", nested),
        ])
        .unwrap();
        Arc::new(schema).into()
    }

    /// Recover the engine-allocated strings of a getter result so the test does not leak them.
    fn recover_default(
        result: ExternResult<OptionalValue<KernelColumnDefault>>,
    ) -> Option<(String, CColumnDefaultKind, Option<String>)> {
        match ok_or_panic(result) {
            OptionalValue::None => None,
            OptionalValue::Some(default) => Some((
                recover_string(default.raw_sql.unwrap()),
                default.kind,
                default.parsed_value.map(recover_string),
            )),
        }
    }

    #[rstest]
    // A string literal is serialized verbatim, so `parsed_value` drops the SQL quoting.
    #[case::literal(
        "literal",
        Some(("'pending'".to_string(), CColumnDefaultKind::Literal, Some("pending".to_string())))
    )]
    // A NULL literal has no serialized form, so it is a `Literal` carrying no `parsed_value`.
    #[case::null_literal(
        "null_literal",
        Some(("NULL".to_string(), CColumnDefaultKind::Literal, None))
    )]
    #[case::non_literal(
        "non_literal",
        Some(("current_timestamp()".to_string(), CColumnDefaultKind::NonLiteral, None))
    )]
    #[case::nested(
        "outer.inner",
        Some(("42".to_string(), CColumnDefaultKind::Literal, Some("42".to_string())))
    )]
    #[case::no_default("plain", None)]
    fn schema_field_column_default_reports_the_fields_default(
        #[case] field_path: &str,
        #[case] expected: Option<(String, CColumnDefaultKind, Option<String>)>,
    ) {
        let engine = get_default_engine("memory:///doesntmatter/");
        let schema = getter_schema();

        let result = unsafe {
            schema_field_column_default(
                schema.shallow_copy(),
                kernel_string_slice!(field_path),
                allocate_str,
                engine.shallow_copy(),
            )
        };
        assert_eq!(recover_default(result), expected);

        unsafe { free_schema(schema) };
        unsafe { free_engine(engine) };
    }

    #[rstest]
    #[case::missing_field("nope", KernelError::GenericError)]
    #[case::not_a_struct_on_the_way("plain.inner", KernelError::GenericError)]
    #[case::malformed_metadata("malformed", KernelError::SchemaError)]
    fn schema_field_column_default_errors(#[case] field_path: &str, #[case] expected: KernelError) {
        let engine = get_default_engine("memory:///doesntmatter/");
        let schema = getter_schema();

        let result = unsafe {
            schema_field_column_default(
                schema.shallow_copy(),
                kernel_string_slice!(field_path),
                allocate_str,
                engine.shallow_copy(),
            )
        };
        assert_extern_result_error_with_message(result, expected, None);

        unsafe { free_schema(schema) };
        unsafe { free_engine(engine) };
    }

    #[rstest]
    #[case::declares_defaults(FIXTURE, true)]
    #[case::declares_none(FIXTURE_NO_DEFAULTS, false)]
    fn snapshot_has_column_defaults_reflects_the_schema(
        #[case] table_path: &str,
        #[case] expected: bool,
    ) {
        let table_root = canonical_url(table_path);
        let engine = get_default_engine(&table_root);
        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        assert_eq!(
            unsafe { snapshot_has_column_defaults(snapshot.shallow_copy()) },
            expected
        );

        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[test]
    fn visit_reports_every_top_level_default_in_sorted_order() {
        let (count, visited) = visit_defaults_of(FIXTURE);

        assert_eq!(count, 3);
        assert_eq!(
            visited,
            vec![
                VisitedDefault {
                    name: "amount".to_string(),
                    raw_sql: "4.95".to_string(),
                    kind: CColumnDefaultKind::Literal,
                    parsed_value: "4.95".to_string(),
                },
                VisitedDefault {
                    name: "status".to_string(),
                    raw_sql: "'pending'".to_string(),
                    kind: CColumnDefaultKind::Literal,
                    parsed_value: "pending".to_string(),
                },
                VisitedDefault {
                    name: "ts".to_string(),
                    raw_sql: "current_timestamp()".to_string(),
                    kind: CColumnDefaultKind::NonLiteral,
                    parsed_value: String::new(),
                },
            ]
        );
    }

    #[test]
    fn visit_reports_nothing_for_a_table_without_defaults() {
        assert_eq!(visit_defaults_of(FIXTURE_NO_DEFAULTS), (0, vec![]));
    }

    #[tokio::test]
    async fn visit_reports_nothing_when_the_feature_is_not_enabled(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Orphaned metadata: the schema declares a default but the table does not list
        // `allowColumnDefaults`, so the kernel does not surface it on the write path.
        let commit = [
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}"#,
            r#"{"metaData":{"id":"orphaned","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"status\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"CURRENT_DEFAULT\":\"'pending'\"}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
        ]
        .join("\n");
        let table_root = "memory:///";
        let (engine, snapshot) = setup_snapshot(commit).await?;

        // The default is still visible on the schema itself -- only the write path filters it out.
        assert!(unsafe { snapshot_has_column_defaults(snapshot.shallow_copy()) });

        let txn = unsafe {
            ok_or_panic(transaction(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        let collected: *mut Vec<VisitedDefault> = Box::into_raw(Box::default());
        let count = unsafe {
            ok_or_panic(transaction_visit_top_level_column_defaults(
                txn.shallow_copy(),
                engine.shallow_copy(),
                Some(NonNull::new_unchecked(collected.cast())),
                collect_default,
            ))
        };
        assert_eq!(count, 0);
        assert!(unsafe { Box::from_raw(collected) }.is_empty());

        unsafe { free_transaction(txn) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
        Ok(())
    }
}
