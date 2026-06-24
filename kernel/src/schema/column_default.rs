//! A typed wrapper around Delta's `CURRENT_DEFAULT` column metadata

use crate::expressions::{parse_sql, Expression, Scalar};
use crate::schema::DataType;
use crate::{DeltaResult, Engine, Error};

/// A column-level default value parsed from the `CURRENT_DEFAULT` metadata key of a
/// [`StructField`](crate::schema::StructField).
///
/// The carrier holds the raw SQL string ([`raw_sql`](Self::raw_sql)) and the column's declared
/// type ([`data_type`](Self::data_type)). On construction the kernel eagerly parses the SQL with
/// its built-in SQL parser and caches the result. Callers check whether that parse succeeded
/// via [`is_kernel_parsable`](Self::is_kernel_parsable) and resolve the default to a [`Scalar`]
/// via [`evaluate`](Self::evaluate). When the kernel cannot parse the SQL (e.g. a function call
/// such as `current_timestamp()`), connectors with richer SQL support may evaluate
/// [`raw_sql`](Self::raw_sql) themselves.
///
/// The declared type is borrowed from the logical schema, which always outlives the carrier.
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
    /// The kernel attempts to parse `raw_sql` via the built-in SQL parser using `data_type` as the
    /// expected target type. A parse failure is **not** an error -- the parsed form is left empty
    /// and `Ok` is returned -- because not every stored default is a literal the kernel knows how
    /// to interpret (use [`is_kernel_parsable`](Self::is_kernel_parsable) to check).
    ///
    /// # Errors
    ///
    /// Returns an error when `data_type` is non-primitive (Array, Map, Struct, or Variant) and
    /// `raw_sql` is not `NULL` (case-insensitive).
    pub fn new(raw_sql: String, data_type: &'a DataType) -> DeltaResult<Self> {
        let is_null = raw_sql.trim().eq_ignore_ascii_case("null");
        // Spark only allows a non-primitive column default when it is NULL; match that behavior.
        if data_type.as_primitive_opt().is_none() && !is_null {
            return Err(Error::generic(format!(
                "column default for non-primitive type {data_type:?} must be NULL, got {raw_sql:?}"
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
    ///
    /// Connectors with a SQL engine richer than the kernel's SQL parser can evaluate this
    /// directly when [`is_kernel_parsable`](Self::is_kernel_parsable) returns `false`.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }

    /// The declared type of the column whose default this is.
    pub fn data_type(&self) -> &DataType {
        self.data_type
    }

    /// Returns `true` when the kernel parsed [`raw_sql`](Self::raw_sql) into a form it can
    /// evaluate, and `false` otherwise.
    pub fn is_kernel_parsable(&self) -> bool {
        self.parsed_sql.is_some()
    }

    /// Evaluate the parsed default to a [`Scalar`].
    ///
    /// Returns `Ok(None)` when the kernel could not parse [`raw_sql`](Self::raw_sql) (i.e.
    /// [`is_kernel_parsable`](Self::is_kernel_parsable) is `false`); the caller may fall back to
    /// its own SQL engine using [`raw_sql`](Self::raw_sql).
    ///
    /// # Errors
    ///
    /// Returns an error if the parsed default is not a literal (the parser never produces such
    /// expressions, so this is defensive).
    pub fn evaluate(&self, _engine: &dyn Engine) -> DeltaResult<Option<Scalar>> {
        let Some(expr) = self.parsed_sql.as_ref() else {
            return Ok(None);
        };
        match expr {
            Expression::Literal(scalar) => Ok(Some(scalar.clone())),
            other => Err(Error::generic(format!(
                "kernel cannot evaluate non-literal column default expression: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::schema::{ArrayType, MapType, StructField};
    use crate::{EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

    /// A stand-in [`Engine`] whose handlers all panic. [`ColumnDefault::evaluate`] resolves
    /// literal defaults by AST destructure, so it must never reach a handler.
    struct UnusedEngine;

    impl Engine for UnusedEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            unimplemented!("evaluate must not touch the engine for literal defaults")
        }
        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            unimplemented!()
        }
        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }
        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }
    }

    fn struct_ty() -> DataType {
        DataType::try_struct_type([StructField::nullable("a", DataType::INTEGER)]).unwrap()
    }

    #[test]
    fn new_parsable_literal_exposes_raw_sql_type_and_evaluates() {
        let data_type = DataType::INTEGER;
        let d = ColumnDefault::new("42".into(), &data_type).unwrap();
        assert_eq!(d.raw_sql(), "42");
        assert_eq!(d.data_type(), &DataType::INTEGER);
        assert!(d.is_kernel_parsable());
        assert_eq!(d.parsed_sql, Some(Expression::literal(Scalar::Integer(42))));
        assert_eq!(
            d.evaluate(&UnusedEngine).unwrap(),
            Some(Scalar::Integer(42))
        );
    }

    #[test]
    fn new_parses_string_and_null_literals() {
        let string_ty = DataType::STRING;
        let s = ColumnDefault::new("'hello'".into(), &string_ty).unwrap();
        assert_eq!(
            s.parsed_sql,
            Some(Expression::literal(Scalar::String("hello".into())))
        );

        let int_ty = DataType::INTEGER;
        let n = ColumnDefault::new("NULL".into(), &int_ty).unwrap();
        assert_eq!(
            n.parsed_sql,
            Some(Expression::literal(Scalar::Null(DataType::INTEGER)))
        );
    }

    #[rstest]
    #[case::integer("42", DataType::INTEGER, true)]
    #[case::string("'hello'", DataType::STRING, true)]
    #[case::boolean("TRUE", DataType::BOOLEAN, true)]
    #[case::date("DATE '2024-01-01'", DataType::DATE, true)]
    #[case::null_primitive("NULL", DataType::INTEGER, true)]
    #[case::function_call("current_timestamp()", DataType::TIMESTAMP, false)]
    #[case::type_mismatch("'not an int'", DataType::INTEGER, false)]
    #[case::arithmetic("1 + 1", DataType::INTEGER, false)]
    fn is_kernel_parsable_reflects_parse_outcome(
        #[case] raw_sql: &str,
        #[case] data_type: DataType,
        #[case] parsable: bool,
    ) {
        let d = ColumnDefault::new(raw_sql.into(), &data_type).unwrap();
        assert_eq!(d.is_kernel_parsable(), parsable);
        // A parse failure must not drop the raw SQL -- connectors fall back to it.
        assert_eq!(d.raw_sql(), raw_sql);
    }

    #[rstest]
    #[case::array(DataType::from(ArrayType::new(DataType::INTEGER, true)), "ARRAY(1)")]
    #[case::map(
        DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)),
        "MAP('k', 1)"
    )]
    #[case::struct_type(struct_ty(), "STRUCT(1)")]
    #[case::variant(DataType::unshredded_variant(), "1")]
    fn new_rejects_non_primitive_with_non_null_default(
        #[case] data_type: DataType,
        #[case] raw_sql: &str,
    ) {
        let err = ColumnDefault::new(raw_sql.into(), &data_type)
            .expect_err("non-primitive non-NULL default must error")
            .to_string();
        assert!(err.contains("must be NULL"), "got: {err}");
    }

    #[rstest]
    #[case::array(DataType::from(ArrayType::new(DataType::INTEGER, true)))]
    #[case::map(DataType::from(MapType::new(DataType::STRING, DataType::INTEGER, true)))]
    #[case::struct_type(struct_ty())]
    #[case::variant(DataType::unshredded_variant())]
    fn new_allows_non_primitive_null_default(#[case] data_type: DataType) {
        let d = ColumnDefault::new("NULL".into(), &data_type).unwrap();
        assert!(d.is_kernel_parsable());
        assert_eq!(
            d.evaluate(&UnusedEngine).unwrap(),
            Some(Scalar::Null(data_type.clone()))
        );
    }

    #[test]
    fn evaluate_returns_none_for_unparsable_default() {
        let data_type = DataType::TIMESTAMP;
        let d = ColumnDefault::new("unparsable_sql()".into(), &data_type).unwrap();
        assert!(!d.is_kernel_parsable());
        assert_eq!(d.evaluate(&UnusedEngine).unwrap(), None);
    }

    #[test]
    fn evaluate_errors_on_non_literal_parsed_expression() {
        // The parser only emits literals in normal use; construct a non-literal directly to reach
        // the defensive error arm.
        let int_ty = DataType::INTEGER;
        let d = ColumnDefault {
            raw_sql: "x".into(),
            data_type: &int_ty,
            parsed_sql: Some(Expression::column(["x"])),
        };
        let err = d
            .evaluate(&UnusedEngine)
            .expect_err("non-literal parsed expression must error")
            .to_string();
        assert!(err.contains("non-literal"), "got: {err}");
    }
}
