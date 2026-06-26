//! A typed wrapper around Delta's `CURRENT_DEFAULT` column metadata

use crate::expressions::{parse_sql, Expression, Scalar};
use crate::schema::DataType;
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
    // No production caller yet, remove this allow when that wiring exists.
    #[allow(unused)]
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
