//! A typed wrapper around Delta's `CURRENT_DEFAULT` column metadata

use crate::expressions::{parse_sql, Expression, Scalar};
use crate::schema::{DataType, StructType};
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
    /// Returns an error when `data_type` is non-primitive (Array, Map, Struct, or Variant) and
    /// `raw_sql` is not `NULL` (case-insensitive).
    pub(crate) fn new(raw_sql: String, data_type: &'a DataType) -> DeltaResult<Self> {
        let is_null = raw_sql.trim().eq_ignore_ascii_case("null");
        // Spark only allows a non-primitive column default when it is NULL; match that behavior.
        if data_type.as_primitive_opt().is_none() && !is_null {
            return Err(Error::generic(format!(
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

/// Validates the column-default metadata on a table's logical schema, treating an ill-formed
/// default as table corruption.
///
/// Run eagerly at [`TableConfiguration`] construction so a corrupt table is rejected at load.
/// Inspects top-level columns only, consistent with the rest of kernel's column-default handling.
///
/// [`TableConfiguration`]: crate::table_configuration::TableConfiguration
///
/// # Errors
///
/// - A column declares a `CURRENT_DEFAULT` but `allow_column_defaults` is `false`. The protocol
///   only honors defaults "when enabled", so such metadata is stray and the table is corrupt.
/// - Propagates any error from
///   [`StructField::column_default`](crate::schema::StructField::column_default)
pub(crate) fn validate_column_defaults_metadata_respects_protocol(
    schema: &StructType,
    allow_column_defaults: bool,
) -> DeltaResult<()> {
    for field in schema.fields() {
        // `column_default` validates the metadata is well-formed; we discard the parsed default.
        if field.column_default()?.is_some() && !allow_column_defaults {
            return Err(Error::generic(format!(
                "Field '{}' declares a `CURRENT_DEFAULT` but the table does not enable the \
                 `allowColumnDefaults` writer feature",
                field.name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use rstest::rstest;

    use super::*;
    use crate::schema::{ArrayType, ColumnMetadataKey, MapType, MetadataValue, StructField};

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

    /// A field carrying `raw_sql` as its `CURRENT_DEFAULT`.
    fn field_with_default(
        name: &str,
        data_type: impl Into<DataType>,
        raw_sql: &str,
    ) -> StructField {
        StructField::nullable(name, data_type).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            MetadataValue::String(raw_sql.to_string()),
        )])
    }

    /// A field with an invalid `CURRENT_DEFAULT`: the metadata is a non-string value (malformed).
    fn field_with_invalid_default(name: &str) -> StructField {
        StructField::nullable(name, DataType::INTEGER).add_metadata([(
            ColumnMetadataKey::CurrentDefault.as_ref().to_string(),
            MetadataValue::Number(7),
        )])
    }

    /// `None` expects `Ok`; `Some(needle)` expects an error containing `needle`.
    #[rstest]
    #[case::well_formed_default_when_enabled(
        vec![
            field_with_default("c", DataType::INTEGER, "42"),
            StructField::nullable("no_default", DataType::STRING),
        ],
        true,
        None
    )]
    #[case::no_defaults_when_disabled(
        vec![StructField::nullable("c", DataType::INTEGER)],
        false,
        None
    )]
    #[case::stray_default_when_disabled(
        vec![field_with_default("c", DataType::INTEGER, "42")],
        false,
        Some("allowColumnDefaults")
    )]
    #[case::non_string_metadata(vec![field_with_invalid_default("c")], true, Some("non-string"))]
    #[case::non_null_default_on_non_primitive(
        vec![field_with_default("arr", ArrayType::new(DataType::INTEGER, true), "ARRAY(1)")],
        true,
        Some("not supported")
    )]
    fn validate_column_defaults_cases(
        #[case] fields: Vec<StructField>,
        #[case] allow_column_defaults: bool,
        #[case] expected_error: Option<&str>,
    ) {
        let schema = StructType::try_new(fields).unwrap();
        match (
            validate_column_defaults_metadata_respects_protocol(&schema, allow_column_defaults),
            expected_error,
        ) {
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
