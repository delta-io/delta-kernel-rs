//! A typed wrapper around Delta's `CURRENT_DEFAULT` column metadata.
//!
//! # Invariants
//!
//! The rules kernel enforces on column defaults, and where each comes from. Each is enforced in
//! code below (see [`ColumnDefault`]'s constructor and [`validate_column_defaults_metadata`],
//! plus the IcebergCompatV3 check in [`crate::table_features`]).
//!
//! - `CURRENT_DEFAULT` metadata present without the `allowColumnDefaults` writer feature is
//!   orphaned metadata, which kernel tolerates on both read and write. Most Delta features tolerate
//!   metadata whose owning feature is absent, and this matches DBR, which reads and writes such
//!   tables. (Protocol.)
//! - `CURRENT_DEFAULT` metadata must be a SQL string. (Protocol.)
//! - A Variant column may default only to `NULL`. (Protocol: the Default Columns section requires
//!   that all columns of `variant` type default to null.)
//! - An Array, Map, or Struct column may likewise default only to `NULL`. (Delta/Spark parity: the
//!   protocol does not explicitly restrict these, but kernel matches Spark, which does not
//!   materialize non-`NULL` defaults on complex types.)
//! - Defaults may appear on nested struct fields, not just top-level columns. Kernel only *writes*
//!   top-level defaults, but supports *loading* a snapshot of a table authored elsewhere that
//!   carries nested defaults, so validation descends into nested fields. (Protocol permits nesting;
//!   the write-side restriction is a kernel limitation.)
//! - Parsing is best-effort: unparseable SQL (e.g. `current_timestamp()`) is not an error; the
//!   connector falls back to the raw SQL. (Kernel.)
//! - On an IcebergCompatV3 table, kernel imposes two extra limitations on every default:
//!     - It must be a literal. (Protocol requires a literal; kernel enforces this via its own SQL
//!       parser, so a literal kernel cannot parse is rejected too.)
//!     - Its column must be primitive. (Kernel limitation: kernel does not materialize
//!       non-primitive defaults, so it rejects them even where the protocol would allow a `NULL`.)

use crate::expressions::{parse_sql, Expression, Scalar};
use crate::schema::{DataType, StructField, StructType};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::{DeltaResult, Error};

/// A column-level default parsed from the `CURRENT_DEFAULT` metadata key of a
/// [`StructField`](crate::schema::StructField).
///
/// Holds the raw SQL and the column's declared type. On construction the kernel parses the SQL
/// with its built-in parser and caches the result. [`to_scalar`](Self::to_scalar) returns the
/// parsed [`Scalar`], or `None` when the kernel could not parse the SQL (e.g.
/// `current_timestamp()`), in which case a connector can evaluate [`raw_sql`](Self::raw_sql)
/// itself.
///
/// The declared type is borrowed from the logical schema, which outlives the carrier.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefault<'a> {
    raw_sql: String,
    data_type: &'a DataType,
    /// The default parsed as a kernel [`Expression`], or `None` if the kernel's
    /// built-in SQL parser could not parse it.
    parsed_sql: Option<Expression>,
}

impl<'a> ColumnDefault<'a> {
    /// Build a `ColumnDefault` from a raw SQL string and the column's declared type.
    ///
    /// Parses `raw_sql` with the built-in SQL parser, targeting `data_type`. A parse failure is
    /// not an error: the cached form is left empty and [`to_scalar`](Self::to_scalar) returns
    /// `None` so the connector can handle the raw SQL.
    ///
    /// # Errors
    ///
    /// Returns an [`Error::unsupported`] when `data_type` is non-primitive (Array, Map, Struct, or
    /// Variant) and `raw_sql` is not `NULL` (case-insensitive). The protocol permits such a
    /// default, but kernel does not materialize it (matching Spark), so it is unsupported
    /// rather than corrupt.
    pub(crate) fn new(raw_sql: String, data_type: &'a DataType) -> DeltaResult<Self> {
        let is_null = raw_sql.trim().eq_ignore_ascii_case("null");
        // Enforces the "non-primitive columns default only to NULL" invariant (see module docs).
        // This is a kernel limitation, not table corruption: the protocol permits a non-`NULL`
        // default on an Array/Map/Struct column, but kernel does not materialize one (matching
        // Spark), so it reports the value as unsupported rather than rejecting the table as
        // corrupt.
        if data_type.as_primitive_opt().is_none() && !is_null {
            return Err(Error::unsupported(format!(
                "non-null column default for non-primitive type {data_type:?} is not \
                 supported, got {raw_sql:?}"
            )));
        }
        let parsed_sql = parse_sql(&raw_sql, data_type).ok();
        Ok(Self {
            raw_sql,
            data_type,
            parsed_sql,
        })
    }

    /// The raw SQL expression as stored in the column's metadata.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }

    /// The declared type of the column whose default this is.
    pub fn data_type(&self) -> &DataType {
        self.data_type
    }

    /// The default as a [`Scalar`], or `None` when the kernel could not parse the SQL.
    ///
    /// On `None` the connector can evaluate [`raw_sql`](Self::raw_sql) with its own SQL engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the parsed default is not a literal. The parser only emits literals, so
    /// this is defensive.
    pub fn to_scalar(&self) -> DeltaResult<Option<Scalar>> {
        match &self.parsed_sql {
            None => Ok(None),
            Some(Expression::Literal(scalar)) => Ok(Some(scalar.clone())),
            Some(other) => Err(Error::generic(format!(
                "kernel cannot evaluate non-literal column default expression: {other:?}"
            ))),
        }
    }

    /// Returns `true` iff the default parsed to a literal expression.
    ///
    /// SQL the kernel could not parse (e.g. arithmetic or function calls) returns `false`. Note
    /// that `NULL` parses to a literal, so this is `true` for a `NULL` default regardless of the
    /// column type.
    pub(crate) fn is_literal(&self) -> bool {
        matches!(self.parsed_sql, Some(Expression::Literal(_)))
    }
}

/// Walks a schema and returns the parsed [`ColumnDefault`] of every field carrying
/// a `CURRENT_DEFAULT`, descending through nested structs, arrays, and maps (see the nesting
/// invariant in the module docs).
///
/// A `CURRENT_DEFAULT` lives on a [`StructField`], so container slots contribute a synthetic path
/// segment on the way to any struct nested inside them: `element` for an array, `key`/`value` for a
/// map. `path` is collected for clarity in error reporting.
///
/// # Errors
///
/// Propagates any error from
/// [`StructField::column_default`](crate::schema::StructField::column_default): a `CURRENT_DEFAULT`
/// whose value is not a SQL string, or a non-`NULL` default on a non-primitive column.
pub(crate) fn collect_column_defaults(
    schema: &StructType,
) -> DeltaResult<Vec<(String, ColumnDefault<'_>)>> {
    let mut collector = ColumnDefaultCollector {
        path: Vec::new(),
        defaults: Vec::new(),
    };
    collector.transform_struct(schema)?;
    Ok(collector.defaults)
}

/// Recursive [`SchemaTransform`] that gathers every field's column default with its dotted path.
///
/// Uses the `DeltaResult<()>` carrier so [`StructField::column_default`]'s error propagates with
/// `?` and short-circuits the traversal. Container element types have no metadata of their own, so
/// the array/map hooks only push a synthetic path segment before recursing toward any nested
/// struct.
///
/// [`StructField::column_default`]: crate::schema::StructField::column_default
struct ColumnDefaultCollector<'a> {
    path: Vec<String>,
    defaults: Vec<(String, ColumnDefault<'a>)>,
}

impl<'a> ColumnDefaultCollector<'a> {
    /// Recurse into a container element type under a synthetic path `segment`
    /// (`element` for arrays, `key`/`value` for maps).
    fn descend(&mut self, segment: &str, element: &'a DataType) -> DeltaResult<()> {
        self.path.push(segment.to_string());
        let result = self.transform(element);
        self.path.pop();
        result
    }
}

impl<'a> SchemaTransform<'a> for ColumnDefaultCollector<'a> {
    transform_output_type!(|'a, T| DeltaResult<()>);

    fn transform_struct_field(&mut self, field: &'a StructField) -> DeltaResult<()> {
        self.path.push(field.name().clone());
        if let Some(column_default) = field.column_default()? {
            self.defaults.push((self.path.join("."), column_default));
        }
        let result = self.recurse_into_struct_field(field);
        self.path.pop();
        result
    }

    fn transform_array_element(&mut self, etype: &'a DataType) -> DeltaResult<()> {
        self.descend("element", etype)
    }

    fn transform_map_key(&mut self, ktype: &'a DataType) -> DeltaResult<()> {
        self.descend("key", ktype)
    }

    fn transform_map_value(&mut self, vtype: &'a DataType) -> DeltaResult<()> {
        self.descend("value", vtype)
    }

    fn transform_variant(&mut self, _stype: &'a StructType) -> DeltaResult<()> {
        Ok(())
    }
}

/// Validates the column-default metadata on a table's logical schema (see [Errors](#errors)).
///
/// Run eagerly at [`TableConfiguration`] construction so an invalid table is rejected at load.
/// Inspects nested fields as well as top-level columns; see [`collect_column_defaults`].
///
/// This does not couple defaults to the `allowColumnDefaults` feature: a `CURRENT_DEFAULT`
/// present without the feature is orphaned metadata, which kernel tolerates (see the module docs).
///
/// [`TableConfiguration`]: crate::table_configuration::TableConfiguration
///
/// # Errors
///
/// Propagates any error from [`collect_column_defaults`]: a `CURRENT_DEFAULT` whose value is not
/// a SQL string (corrupt, since the protocol defines it as a SQL string), or a non-`NULL` default
/// on a non-primitive column (unsupported by kernel, though the protocol permits it).
pub(crate) fn validate_column_defaults_metadata(schema: &StructType) -> DeltaResult<()> {
    collect_column_defaults(schema)?;
    Ok(())
}

/// A nullable field named `name` carrying `raw_sql` as its `CURRENT_DEFAULT` metadata.
///
/// Shared across the crate's column-default unit tests (here, `schema`, and `transaction`).
#[cfg(test)]
pub(crate) fn field_with_default(
    name: &str,
    data_type: impl Into<DataType>,
    raw_sql: &str,
) -> StructField {
    use crate::schema::{ColumnMetadataKey, MetadataValue};
    StructField::nullable(name, data_type).add_metadata([(
        ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
        MetadataValue::String(raw_sql.to_string()),
    )])
}

/// A nullable field named `name` whose `CURRENT_DEFAULT` metadata is a non-string value (corrupt).
#[cfg(test)]
pub(crate) fn field_with_invalid_default(name: &str) -> StructField {
    use crate::schema::{ColumnMetadataKey, MetadataValue};
    StructField::nullable(name, DataType::INTEGER).add_metadata([(
        ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
        MetadataValue::Number(7),
    )])
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use rstest::rstest;

    use super::*;
    use crate::schema::{ArrayType, MapType, StructField};

    fn struct_ty() -> DataType {
        DataType::try_struct_type([StructField::nullable("a", DataType::INTEGER)]).unwrap()
    }

    fn date_days(year: i32, month: u32, day: u32) -> i32 {
        let nd = NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        Utc.from_utc_datetime(&nd)
            .signed_duration_since(DateTime::UNIX_EPOCH)
            .num_days() as i32
    }

    /// Expected outcome of [`ColumnDefault::new`] for one `(raw_sql, data_type)` pair.
    #[derive(Debug)]
    enum Expect {
        /// `to_scalar` yields `Some(this)`.
        Parsed(Scalar),
        /// `to_scalar` yields `Some(Scalar::Null)` of the column's type.
        ParsedNull,
        /// `to_scalar` yields `None`.
        Unparsable,
        /// `new` fails with an error containing this substring.
        NewErr(&'static str),
    }

    #[rstest]
    #[case::integer("42", DataType::INTEGER, Expect::Parsed(Scalar::Integer(42)))]
    #[case::string("'hello'", DataType::STRING, Expect::Parsed(Scalar::String("hello".into())))]
    #[case::boolean("TRUE", DataType::BOOLEAN, Expect::Parsed(Scalar::Boolean(true)))]
    #[case::date(
        "DATE '2024-01-01'",
        DataType::DATE,
        Expect::Parsed(Scalar::Date(date_days(2024, 1, 1)))
    )]
    #[case::null_primitive("NULL", DataType::INTEGER, Expect::ParsedNull)]
    #[case::null_array(
        "NULL",
        DataType::from(ArrayType::new(DataType::INTEGER, true)),
        Expect::ParsedNull
    )]
    #[case::null_map(
        "NULL",
        DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
        Expect::ParsedNull
    )]
    #[case::null_struct("NULL", struct_ty(), Expect::ParsedNull)]
    #[case::null_variant("NULL", DataType::unshredded_variant(), Expect::ParsedNull)]
    #[case::function_call("current_timestamp()", DataType::TIMESTAMP, Expect::Unparsable)]
    #[case::type_mismatch("'not an int'", DataType::INTEGER, Expect::Unparsable)]
    #[case::arithmetic("1 + 1", DataType::INTEGER, Expect::Unparsable)]
    #[case::non_primitive_array(
        "ARRAY(1)",
        DataType::from(ArrayType::new(DataType::INTEGER, true)),
        Expect::NewErr("not supported")
    )]
    #[case::non_primitive_map(
        "MAP('k', 1)",
        DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
        Expect::NewErr("not supported")
    )]
    #[case::non_primitive_struct("STRUCT(1)", struct_ty(), Expect::NewErr("not supported"))]
    #[case::non_primitive_variant(
        "1",
        DataType::unshredded_variant(),
        Expect::NewErr("not supported")
    )]
    fn column_default_from_new(
        #[case] raw_sql: &str,
        #[case] data_type: DataType,
        #[case] expect: Expect,
    ) {
        match (ColumnDefault::new(raw_sql.into(), &data_type), expect) {
            (Ok(d), Expect::Parsed(scalar)) => {
                assert_eq!(d.raw_sql(), raw_sql);
                assert_eq!(d.data_type(), &data_type);
                assert_eq!(d.to_scalar().unwrap(), Some(scalar));
            }
            (Ok(d), Expect::ParsedNull) => {
                assert_eq!(
                    d.to_scalar().unwrap(),
                    Some(Scalar::Null(data_type.clone()))
                );
            }
            (Ok(d), Expect::Unparsable) => {
                assert_eq!(d.to_scalar().unwrap(), None);
                // A parse failure must not drop the raw SQL; connectors fall back to it.
                assert_eq!(d.raw_sql(), raw_sql);
            }
            (Err(e), Expect::NewErr(needle)) => {
                assert!(e.to_string().contains(needle), "got: {e}");
            }
            (result, expect) => {
                panic!("unexpected outcome for {raw_sql:?}: {result:?} vs {expect:?}")
            }
        }
    }

    /// Validation is independent of the `allowColumnDefaults` feature: it checks only that the
    /// metadata is well-formed (string-typed, primitive-or-NULL), never whether the feature is
    /// enabled. `None` expects `Ok`; `Some(needle)` expects an error containing `needle`.
    #[rstest]
    #[case::well_formed_default(
        vec![
            field_with_default("c", DataType::INTEGER, "42"),
            StructField::nullable("no_default", DataType::STRING),
        ],
        None
    )]
    #[case::no_defaults(vec![StructField::nullable("c", DataType::INTEGER)], None)]
    #[case::non_string_metadata(vec![field_with_invalid_default("c")], Some("non-string"))]
    #[case::non_null_default_on_non_primitive(
        vec![field_with_default("arr", ArrayType::new(DataType::INTEGER, true), "ARRAY(1)")],
        Some("not supported")
    )]
    #[case::nested_default(
        vec![StructField::nullable(
            "s",
            DataType::try_struct_type([field_with_default("inner", DataType::INTEGER, "42")]).unwrap(),
        )],
        None
    )]
    fn validate_column_defaults_cases(
        #[case] fields: Vec<StructField>,
        #[case] expected_error: Option<&str>,
    ) {
        let schema = StructType::try_new(fields).unwrap();
        match (validate_column_defaults_metadata(&schema), expected_error) {
            (Ok(()), None) => {}
            (Err(e), Some(needle)) => assert!(e.to_string().contains(needle), "got: {e}"),
            (result, expected) => panic!("unexpected outcome: {result:?} vs {expected:?}"),
        }
    }

    #[test]
    fn to_scalar_errors_on_non_literal_parsed_expression() {
        // The parser only emits literals, so construct a non-literal directly to reach the
        // defensive error arm.
        let int_ty = DataType::INTEGER;
        let d = ColumnDefault {
            raw_sql: "x".into(),
            data_type: &int_ty,
            parsed_sql: Some(Expression::column(["x"])),
        };
        let err = d
            .to_scalar()
            .expect_err("non-literal parsed expression must error")
            .to_string();
        assert!(err.contains("non-literal"), "got: {err}");
    }
}
