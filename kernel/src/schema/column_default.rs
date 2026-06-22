//! A typed wrapper around Delta's `CURRENT_DEFAULT` column metadata

use crate::expressions::{parse_sql, Expression, Scalar};
use crate::schema::DataType;
use crate::{DeltaResult, Engine, Error};

/// A column-level default value parsed from the `CURRENT_DEFAULT` metadata key of a
/// [`StructField`](crate::schema::StructField).
///
/// The carrier holds the raw SQL string ([`raw_sql`](Self::raw_sql)) and the column's declared
/// type ([`data_type`](Self::data_type)). On construction the kernel eagerly parses the SQL with
/// its built-in literal parser and caches the result. Callers check whether that parse succeeded
/// via [`is_kernel_parsable`](Self::is_kernel_parsable) and resolve the default to a [`Scalar`]
/// via [`evaluate`](Self::evaluate). When the kernel cannot parse the SQL (e.g. a function call
/// such as `current_timestamp()`), connectors with richer SQL support fall back to evaluating
/// [`raw_sql`](Self::raw_sql) themselves.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefault {
    raw_sql: String,
    data_type: DataType,
    /// The default parsed as a kernel [`Expression`], or `None` if the kernel's
    /// built-in SQL parser could not parse it.
    parsed_sql: Option<Expression>,
}

impl ColumnDefault {
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
    /// `raw_sql` is not `NULL`. The Delta protocol requires defaults on these types -- in
    /// particular Variant columns -- to be `NULL`; this is a defense-in-depth check independent of
    /// the parser.
    pub fn new(raw_sql: String, data_type: DataType) -> DeltaResult<Self> {
        // The protocol only permits NULL defaults on non-primitive types (incl. Variant). Reject
        // anything else up front rather than silently treating it as unparsable.
        let is_null = raw_sql.trim().eq_ignore_ascii_case("null");
        if data_type.as_primitive_opt().is_none() && !is_null {
            return Err(Error::generic(format!(
                "column default for non-primitive type {data_type:?} must be NULL, got {raw_sql:?}"
            )));
        }
        let parsed_sql = parse_sql(&raw_sql, &data_type).ok();
        Ok(Self {
            raw_sql,
            data_type,
            parsed_sql,
        })
    }

    /// The raw SQL expression as stored in the column's metadata.
    ///
    /// Connectors with a SQL engine richer than the kernel's literal parser can evaluate this
    /// directly when [`is_kernel_parsable`](Self::is_kernel_parsable) returns `false`.
    pub fn raw_sql(&self) -> &str {
        &self.raw_sql
    }

    /// The declared type of the column whose default this is.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns `true` when the kernel parsed [`raw_sql`](Self::raw_sql) into a form it can
    /// evaluate, and `false` otherwise.
    ///
    /// A cheap, side-effect-free predicate -- it does not require an [`Engine`] -- that lets a
    /// connector decide upfront whether to resolve the default via [`evaluate`](Self::evaluate)
    /// (kernel-parsable defaults) or through its own SQL engine acting on
    /// [`raw_sql`](Self::raw_sql) (everything else).
    pub fn is_kernel_parsable(&self) -> bool {
        self.parsed_sql.is_some()
    }

    /// Evaluate the parsed default to a [`Scalar`].
    ///
    /// Returns `Ok(None)` when the kernel could not parse [`raw_sql`](Self::raw_sql) (i.e.
    /// [`is_kernel_parsable`](Self::is_kernel_parsable) is `false`); the caller is expected to
    /// fall back to its own SQL engine using [`raw_sql`](Self::raw_sql).
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
        let d = ColumnDefault::new("42".into(), DataType::INTEGER).unwrap();
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
        let s = ColumnDefault::new("'hello'".into(), DataType::STRING).unwrap();
        assert_eq!(
            s.parsed_sql,
            Some(Expression::literal(Scalar::String("hello".into())))
        );

        let n = ColumnDefault::new("NULL".into(), DataType::INTEGER).unwrap();
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
        let d = ColumnDefault::new(raw_sql.into(), data_type).unwrap();
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
        let err = ColumnDefault::new(raw_sql.into(), data_type)
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
        let d = ColumnDefault::new("NULL".into(), data_type.clone()).unwrap();
        assert!(d.is_kernel_parsable());
        assert_eq!(
            d.evaluate(&UnusedEngine).unwrap(),
            Some(Scalar::Null(data_type))
        );
    }

    #[test]
    fn evaluate_returns_none_for_unparsable_default() {
        let d = ColumnDefault::new("current_timestamp()".into(), DataType::TIMESTAMP).unwrap();
        assert!(!d.is_kernel_parsable());
        assert_eq!(d.evaluate(&UnusedEngine).unwrap(), None);
    }

    #[test]
    fn evaluate_errors_on_non_literal_parsed_expression() {
        // The parser only emits literals in normal use; construct a non-literal directly to reach
        // the defensive error arm.
        let d = ColumnDefault {
            raw_sql: "x".into(),
            data_type: DataType::INTEGER,
            parsed_sql: Some(Expression::column(["x"])),
        };
        let err = d
            .evaluate(&UnusedEngine)
            .expect_err("non-literal parsed expression must error")
            .to_string();
        assert!(err.contains("non-literal"), "got: {err}");
    }
}
