//! Definitions and functions to create and manipulate kernel expressions

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use itertools::Itertools;
use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};

#[doc(hidden)]
pub use self::column_names::{__require_valid_simple_column_segment, column_expr};
pub use self::column_names::{
    col, column_expr_ref, column_name, column_pred, joined_column_expr, joined_column_name,
    ColumnName,
};
pub use self::scalars::{ArrayData, DecimalData, MapData, Scalar, StructData};
use crate::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use crate::schema::SchemaRef;
pub use crate::struct_patch::{ExpressionFieldPatch, ExpressionStructPatch};
use crate::transforms::{transform_output_type, ExpressionTransform};
use crate::utils::CollectInto;
use crate::{DataType, DeltaResult, DynPartialEq, Error};

mod column_names;
mod scalars;
mod sql;
pub(crate) use self::sql::parse_sql;

pub type ExpressionRef = std::sync::Arc<Expression>;
pub type PredicateRef = std::sync::Arc<Predicate>;

/// Build an [`Expression::Literal`] from anything that converts into a [`Scalar`].
///
/// Concise alternative to [`Expression::literal`] for plan builders. Accepts the same value
/// types [`Scalar`] does (`i32`, `i64`, `&str`, `bool`, ...).
///
/// ```
/// use delta_kernel::expressions::lit;
/// let _zero = lit(0i64);
/// ```
pub fn lit(value: impl Into<Scalar>) -> Expression {
    Expression::literal(value)
}

/// Build a typed NULL [`Expression::Literal`].
///
/// Prefer this over `lit(Scalar::Null(...))`. Accepts anything convertible into a [`DataType`]
/// (including container types like [`StructType`](crate::schema::StructType)), so callers can
/// skip an explicit `DataType::from(...)` wrapper.
///
/// ```
/// # use delta_kernel::expressions::{lit, null_lit, Scalar};
/// # use delta_kernel::schema::DataType;
/// assert_eq!(
///     lit(Scalar::Null(DataType::LONG)),
///     null_lit(DataType::LONG),
/// );
/// ```
pub fn null_lit(data_type: impl Into<DataType>) -> Expression {
    Expression::Literal(Scalar::null(data_type))
}

/// A [`StructPatchBuilder`](crate::struct_patch::StructPatchBuilder) whose emitted items are
/// expressions, lowered into an [`ExpressionStructPatch`] that can be embedded in an
/// [`Expression`].
pub type ExpressionStructPatchBuilder = crate::struct_patch::StructPatchBuilder<ExpressionRef>;

////////////////////////////////////////////////////////////////////////
// Operators
////////////////////////////////////////////////////////////////////////

/// A unary predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UnaryPredicateOp {
    /// SQL `expr IS NULL`: `true` when the input is null and `false` otherwise, never null itself.
    /// A wrapping `NOT` inverts it to `IS NOT NULL`.
    IsNull,
}

/// A binary predicate operator.
///
/// The ordering and equality comparisons follow SQL three-valued logic, so a NULL operand yields
/// NULL rather than `false`. [`Distinct`](Self::Distinct) and [`In`](Self::In) instead tolerate
/// NULL and always answer `true` or `false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryPredicateOp {
    /// `left < right`.
    LessThan,
    /// `left > right`.
    GreaterThan,
    /// `left = right`.
    Equal,
    /// SQL `left IS DISTINCT FROM right`: null-safe inequality, treating NULL as an ordinary
    /// value. `DISTINCT(1, 1)` and `DISTINCT(NULL, NULL)` are `false`; `DISTINCT(NULL, 1)` is
    /// `true`.
    Distinct,
    /// SQL `left IN (elements)`: returns `false` when `left` is NULL. Otherwise, it returns `true`
    /// when `left` equals any element and `false` when none match. NULL elements never match.
    /// `left` must be a literal, and the elements are either a list-typed column or an
    /// [`Expression::Literal`] holding a [`Scalar::Array`], never a list of expressions or a
    /// subquery:
    ///
    /// ```sql
    /// 2 IN (1, 2, 3)         -- literal elements
    /// 2 IN (SELECT ...)      -- unsupported: no subquery form
    /// col IN (1, 2, 3)       -- unsupported: the left operand must be a literal
    /// ```
    ///
    /// Testing a literal against a list column is the shape data skipping uses, and it is the
    /// reason the operands sit this way around rather than the more familiar
    /// column-on-the-left form.
    In,
}

/// A unary expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryExpressionOp {
    /// SQL `to_json(expr)`: encode a struct as a JSON object string, one string per row. The input
    /// must be a struct and the output is STRING. A NULL input row produces a NULL string rather
    /// than `"null"`. This is the inverse of [`ParseJsonExpression`] for every type except
    /// timestamps, whose sub-millisecond precision this operator discards (see below).
    ///
    /// Nested structs and arrays encode as JSON objects and arrays. Binary encodes as lowercase
    /// hex rather than base64, two digits per byte in the order the bytes appear, so
    /// `{ b: 0xABCD, l: [1, 2], n: { z: 7 } }` becomes:
    ///
    /// ```text
    /// {"b":"abcd","l":[1,2],"n":{"z":7}}
    /// ```
    ///
    /// Timestamps must encode with exactly three fractional digits, truncated toward negative
    /// infinity, because kernel writes `add.stats` with this operator and [Per-file Statistics]
    /// truncates timestamp statistics down to milliseconds. TIMESTAMP takes a literal `Z` suffix
    /// and TIMESTAMP_NTZ takes no offset, so `{ ts: 2026-07-02T15:55:55.298677Z }` becomes
    /// `{"ts":"2026-07-02T15:55:55.298Z"}`. Emitting more digits, or rounding up, makes readers
    /// prune files that hold matching rows.
    ///
    /// [Per-file Statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics
    ToJson,
}

/// A binary arithmetic operator over two numeric operands.
///
/// Both operands share a numeric type and the result takes that type. Kernel inserts no implicit
/// casts, so widening a result (decimal precision or scale, for instance) needs an explicit
/// [`Expression::cast`] to match a declared output type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryExpressionOp {
    /// `left + right`.
    Plus,
    /// `left - right`.
    Minus,
    /// `left * right`.
    Multiply,
    /// `left / right`. A zero divisor never yields NULL.
    ///
    /// Integer operands divide truncating toward zero, and a zero divisor fails. Float operands
    /// follow IEEE 754: `+/-inf` for a non-zero numerator, `NaN` for `0.0 / 0.0`. In a dialect
    /// whose `/` is always fractional, the integer case is the other division operator:
    ///
    /// ```sql
    /// 7 DIV 2      -- 3, this operator over integers
    /// 7 / 2        -- 3.5, NOT this operator
    /// ```
    Divide,
}

/// A variadic expression operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VariadicExpressionOp {
    /// SQL `COALESCE(exprs...)`: the first non-null value, or null when every input is null. All
    /// inputs share one type, which is also the result type. Requires at least one input.
    Coalesce,
    /// SQL `ARRAY(exprs...)`: an array built by evaluating each input per row, so
    /// `ARRAY(1, 1 + 2, my_int_col)` yields `[1, 3, <my_int_col value>]`. All inputs must share
    /// the same element type. Requires at least one element; the element type is inferred from
    /// the inputs.
    ///
    /// For static array literals whose elements are all compile-time constants, use
    /// [`Scalar::Array`] instead. The difference is that `Array` is evaluated at runtime, while
    /// `Scalar::Array` is evaluated at compile time.
    Array,
}

/// A junction (AND/OR) predicate operator over N child predicates.
///
/// Both use SQL three-valued (Kleene) logic, treating a NULL child as unknown rather than false: a
/// decisive child wins over a NULL sibling, so `AND` is `false` and `OR` is `true` despite the
/// NULL, and only an undecided junction yields NULL. [`Predicate::junction`] folds an empty
/// junction to its operator's identity, `true` for `AND` and `false` for `OR`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum JunctionPredicateOp {
    /// SQL `a AND b AND ...`: true when every child is true.
    And,
    /// SQL `a OR b OR ...`: true when any child is true.
    Or,
}

/// A kernel-supplied scalar expression evaluator which in particular can convert column references
/// (i.e. [`Expression::Column`]) to [`Scalar`] values. [`OpaqueExpressionOp::eval_expr_scalar`] and
/// [`OpaquePredicateOp::eval_pred_scalar`] rely on this evaluator.
///
/// If the evaluator produces `None`, it means kernel was unable to evaluate
/// the input expression. Otherwise, `Some(Scalar)` is the result of that evaluation (possibly
/// `Scalar::Null` if the output was NULL).
pub type ScalarExpressionEvaluator<'a> = dyn Fn(&Expression) -> Option<Scalar> + 'a;

/// An opaque expression operation (ie defined and implemented by the engine).
pub trait OpaqueExpressionOp: DynPartialEq + std::fmt::Debug {
    /// Succinctly identifies this op
    fn name(&self) -> &str;

    /// Attempts scalar evaluation of this opaque expression, e.g. for partition pruning.
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by
    /// calling back to the provided [`ScalarExpressionEvaluator`],
    ///
    /// An output of `Err` indicates that this operation does not support scalar evaluation, or was
    /// invoked incorrectly (e.g. with the wrong number and/or types of arguments, None input,
    /// etc); the operation is disqualified from participating in partition pruning.
    ///
    /// `Ok(Scalar::Null)` means the operation actually produced a legitimately NULL result.
    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expression],
    ) -> DeltaResult<Scalar>;
}

/// An opaque predicate operation (ie defined and implemented by the engine).
pub trait OpaquePredicateOp: DynPartialEq + std::fmt::Debug {
    /// Succinctly identifies this op
    fn name(&self) -> &str;

    /// Attempts scalar evaluation of this (possibly inverted) opaque predicate on behalf of a
    /// [`DirectPredicateEvaluator`], e.g. for partition pruning or to evaluate an opaque data
    /// skipping predicate produced previously by an [`IndirectDataSkippingPredicateEvaluator`].
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by calling
    /// back to the provided [`ScalarExpressionEvaluator`] and/or [`DirectPredicateEvaluator`].
    ///
    /// An output of `Err` indicates that this operation does not support scalar evaluation, or was
    /// invoked incorrectly (e.g. wrong number and/or types of arguments, None input, etc); the
    /// operation is disqualified from participating in partition pruning and/or data skipping.
    ///
    /// `Ok(None)` means the operation actually produced a legitimately NULL output.
    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        eval_pred: &DirectPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> DeltaResult<Option<bool>>;

    /// Evaluates this (possibly inverted) opaque predicate for data skipping on behalf of a
    /// [`DirectDataSkippingPredicateEvaluator`], e.g. for parquet row group skipping.
    ///
    /// Implementations can evaluate the child expressions however they see fit, possibly by
    /// calling back to the provided [`DirectDataSkippingPredicateEvaluator`].
    ///
    /// An output of `None` indicates that this operation does not support evaluation as a data
    /// skipping predicate, or was invoked incorrectly (e.g. wrong number and/or types of arguments,
    /// None input, etc.); the operation is disqualified from participating in row group skipping.
    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool>;

    /// Converts this (possibly inverted) opaque predicate to a data skipping predicate on behalf of
    /// an [`IndirectDataSkippingPredicateEvaluator`], e.g. for stats-based file pruning.
    ///
    /// Implementations can transform the predicate and its child expressions however they see fit,
    /// possibly by calling back to the owning [`IndirectDataSkippingPredicateEvaluator`].
    ///
    /// An output of `None` indicates that this operation does not support conversion to a data
    /// skipping predicate, or was invoked incorrectly (e.g. wrong number and/or types of arguments,
    /// None input, etc.); the operation is disqualified from participating in file pruning.
    //
    // NOTE: It would be nicer if this method could accept an `Arc<Self>`, in case the data skipping
    // predicate rewrite can reuse the same operation. But sadly, that would not be dyn-compatible.
    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate>;
}

/// A shared reference to an [`OpaqueExpressionOp`] instance.
pub type OpaqueExpressionOpRef = Arc<dyn OpaqueExpressionOp>;

/// A shared reference to an [`OpaquePredicateOp`] instance.
pub type OpaquePredicateOpRef = Arc<dyn OpaquePredicateOp>;

////////////////////////////////////////////////////////////////////////
// Expressions and predicates
////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryPredicate {
    /// The operator.
    pub op: UnaryPredicateOp,
    /// The input expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinaryPredicate {
    /// The operator.
    pub op: BinaryPredicateOp,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryExpression {
    /// The operator.
    pub op: UnaryExpressionOp,
    /// The input expression.
    pub expr: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinaryExpression {
    /// The operator.
    pub op: BinaryExpressionOp,
    /// The left-hand side of the operation.
    pub left: Box<Expression>,
    /// The right-hand side of the operation.
    pub right: Box<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VariadicExpression {
    /// The operator.
    pub op: VariadicExpressionOp,
    /// The input expressions.
    pub exprs: Vec<Expression>,
}

/// Parses a non-null UTF-8 JSON object into a typed row.
///
/// Parsing is directed by `output_schema`; types are selected from the schema, not inferred from
/// JSON tokens. The result has exactly the type and nullability declared by `output_schema`.
/// Field matching is exact and case-sensitive, extra object members are ignored, and the root row
/// is present for every input in the required domain.
///
/// # Standards
///
/// This contract uses existing standards where they completely define the required value:
///
/// - JSON text, tokens, strings, numbers, objects, and arrays have the meanings defined by RFC
///   8259;
/// - row, array, exact-numeric, approximate-numeric, boolean, character, date, timestamp, and
///   SQL-null values use the corresponding ISO/IEC 9075 (standard SQL) value concepts;
/// - conversion of a finite JSON number to `FLOAT` or `DOUBLE` uses IEEE 754 `roundTiesToEven`;
/// - `BINARY` uses the Base64 alphabet and padding of RFC 4648 section 4 and the encoding
///   requirements of RFC 4648 section 3.5.
///
/// # Null and JSON-string semantics
///
/// These rules apply recursively at requested object members, array elements, and map values. The
/// document root must be an object as stated above.
///
/// - a missing object member becomes SQL null;
/// - JSON `null` becomes SQL null;
/// - the JSON string `""` becomes an empty SQL string for `STRING` and an empty byte sequence for
///   `BINARY`;
/// - for every other target, `""` has the wrong JSON token kind and is undefined.
///
/// After decoding, the result must be checked against the nullability declared by `output_schema`.
/// A null in a non-nullable struct field, array element, or map value is an evaluation error. A
/// null parent struct does not violate the nullability of its children.
///
/// Empty-string-to-null conversion is a rule of Delta partition-value serialization, not generic
/// JSON or per-file statistics decoding. [`ParseJsonExpression`] does not apply it.
///
/// Container decoding applies recursively:
///
/// - `STRUCT` requires a JSON object and decodes requested children by exact field name. A missing
///   or JSON-null struct is null; `{}` and an object whose requested children are all null are
///   present structs with null children. Parent validity must be preserved at every depth.
/// - `ARRAY<T>` requires a JSON array and applies the statically compiled `T` decoder to each
///   element. It preserves element order, cardinality, and JSON-null elements as SQL-null elements.
/// - `MAP<STRING,T>` requires a JSON object, converts member names to string keys, and applies the
///   statically compiled `T` decoder to each member value. Entries with JSON-null values remain
///   present with SQL-null values. Other map key types are unsupported.
///
/// In all three cases, a child or element type may itself be a supported struct, array, or map.
/// The same null, token-kind, and leaf rules apply at every depth.
///
/// # Example SQL-engine lowering
///
/// The following two-stage lowering is one way a SQL engine can implement the semantics. It is not
/// a required implementation strategy:
///
/// 1. Compile a raw type from `output_schema` and evaluate `from_json(json_text, raw_type)`. The
///    raw type replaces `BINARY`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, and `VOID` leaves with
///    `STRING` and applies that replacement recursively through structs, arrays, and maps. Other
///    supported leaves keep their target types. Decimal decoding in this stage must preserve the
///    exact JSON number.
/// 2. Statically transform the raw value to `output_schema`: Base64-decode binary strings, parse
///    date and timestamp strings with the rules below, replace raw `VOID` values with typed nulls,
///    and recursively rebuild structs, arrays, and maps.
///
/// # Complete two-stage SQL example
///
/// Suppose the requested Delta type is:
///
/// ```text
/// STRUCT<
///   byte_value: BYTE,
///   short_value: SHORT,
///   int_value: INTEGER,
///   long_value: LONG,
///   float_value: FLOAT,
///   double_value: DOUBLE,
///   decimal_value: DECIMAL(10,2),
///   boolean_value: BOOLEAN,
///   string_value: STRING,
///   binary_value: BINARY,
///   date_value: DATE,
///   timestamp_value: TIMESTAMP,
///   timestamp_ntz_value: TIMESTAMP_NTZ,
///   void_value: VOID,
///   nested_value: STRUCT<ts: TIMESTAMP, binary: BINARY>,
///   times: ARRAY<TIMESTAMP>,
///   blobs: MAP<STRING,BINARY>
/// >
/// ```
///
/// This input covers every supported scalar type and all three recursive container types:
///
/// ```json
/// {
///   "byte_value": -8,
///   "short_value": 32000,
///   "int_value": -7,
///   "long_value": 9007199254740993,
///   "float_value": 1.5,
///   "double_value": "Infinity",
///   "decimal_value": 12345678.90,
///   "boolean_value": true,
///   "string_value": "delta",
///   "binary_value": "AQI=",
///   "date_value": "2024-02-29",
///   "timestamp_value": "2024-01-02T05:04:05.123456789+02:00",
///   "timestamp_ntz_value": "2024-01-02 03:04:05.987654321",
///   "void_value": null,
///   "nested_value": {"ts": "2024-06-01T00:00:00Z", "binary": "AAI="},
///   "times": ["2024-01-02T03:04:05Z", null],
///   "blobs": {"first": "AAE=", "missing": null}
/// }
/// ```
///
/// In the engine-neutral SQL lowering dialect, phase one is the following schema-bound parse.
/// `ROW<...>` supplies the raw result type to `from_json`; it is not runtime type inference.
///
/// ```sql
/// WITH raw AS (
///   SELECT from_json(
///     json_text,
///     ROW<
///       byte_value TINYINT,
///       short_value SMALLINT,
///       int_value INTEGER,
///       long_value BIGINT,
///       float_value REAL,
///       double_value DOUBLE,
///       decimal_value DECIMAL(10,2),
///       boolean_value BOOLEAN,
///       string_value VARCHAR,
///       binary_value VARCHAR,
///       date_value VARCHAR,
///       timestamp_value VARCHAR,
///       timestamp_ntz_value VARCHAR,
///       void_value VARCHAR,
///       nested_value ROW<ts VARCHAR, binary VARCHAR>,
///       times ARRAY<VARCHAR>,
///       blobs MAP<VARCHAR,VARCHAR>
///     >
///   ) AS r
///   FROM input_rows
/// )
/// SELECT * FROM raw;
/// ```
///
/// The raw row is:
///
/// ```text
/// ROW(
///   byte_value=-8,
///   short_value=32000,
///   int_value=-7,
///   long_value=9007199254740993,
///   float_value=1.5,
///   double_value=+Infinity,
///   decimal_value=12345678.90,
///   boolean_value=TRUE,
///   string_value='delta',
///   binary_value='AQI=',
///   date_value='2024-02-29',
///   timestamp_value='2024-01-02T05:04:05.123456789+02:00',
///   timestamp_ntz_value='2024-01-02 03:04:05.987654321',
///   void_value=NULL::VARCHAR,
///   nested_value=ROW(ts='2024-06-01T00:00:00Z', binary='AAI='),
///   times=['2024-01-02T03:04:05Z', NULL],
///   blobs={'first': 'AAE=', 'missing': NULL}
/// )
/// ```
///
/// Phase two is compiled from the requested Delta type. This example uses `CASE` because a plain
/// `ROW(...)` constructor does not retain the nullable nested struct's parent validity. An engine
/// may instead use a struct-update operation that preserves the source validity bitmap. Scalar
/// functions propagate null. `transform(r.times, ...)` calls `transform` as a function with
/// `r.times` as its array argument; likewise, `transform_values` receives `r.blobs` as its map
/// argument. These denote the engine's schema-bound array-element and map-value transforms. The
/// final `ROW` is bound to the requested named output type, in schema field order.
///
/// ```sql
/// WITH raw AS (
///   SELECT from_json(
///     json_text,
///     ROW<
///       byte_value TINYINT,
///       short_value SMALLINT,
///       int_value INTEGER,
///       long_value BIGINT,
///       float_value REAL,
///       double_value DOUBLE,
///       decimal_value DECIMAL(10,2),
///       boolean_value BOOLEAN,
///       string_value VARCHAR,
///       binary_value VARCHAR,
///       date_value VARCHAR,
///       timestamp_value VARCHAR,
///       timestamp_ntz_value VARCHAR,
///       void_value VARCHAR,
///       nested_value ROW<ts VARCHAR, binary VARCHAR>,
///       times ARRAY<VARCHAR>,
///       blobs MAP<VARCHAR,VARCHAR>
///     >
///   ) AS r
///   FROM input_rows
/// )
/// SELECT ROW(
///   r.byte_value,
///   r.short_value,
///   r.int_value,
///   r.long_value,
///   r.float_value,
///   r.double_value,
///   r.decimal_value,
///   r.boolean_value,
///   r.string_value,
///   base64_decode(r.binary_value),
///   CAST(r.date_value AS DATE),
///   parse_timestamp_utc(r.timestamp_value),
///   parse_timestamp_ntz(r.timestamp_ntz_value),
///   CAST(NULL AS VOID),
///   CASE WHEN r.nested_value IS NULL
///        THEN CAST(NULL AS ROW<ts TIMESTAMP WITH TIME ZONE, binary VARBINARY>)
///        ELSE ROW(
///          parse_timestamp_utc(r.nested_value.ts),
///          base64_decode(r.nested_value.binary)
///        )
///   END,
///   transform(r.times, ts -> parse_timestamp_utc(ts)),
///   transform_values(r.blobs, (key, value) -> base64_decode(value))
/// ) AS parsed
/// FROM raw;
/// ```
///
/// Here `parse_timestamp_utc` returns a `TIMESTAMP WITH TIME ZONE` value normalized to UTC;
/// timezone display conversion is not part of parsing. `parse_timestamp_ntz` returns `TIMESTAMP`
/// without a timezone. The final row is:
///
/// ```text
/// ROW(
///   byte_value=-8,
///   short_value=32000,
///   int_value=-7,
///   long_value=9007199254740993,
///   float_value=1.5,
///   double_value=+Infinity,
///   decimal_value=12345678.90,
///   boolean_value=TRUE,
///   string_value='delta',
///   binary_value=X'0102',
///   date_value=DATE '2024-02-29',
///   timestamp_value=UTC '2024-01-02T03:04:05.123456Z',
///   timestamp_ntz_value=TIMESTAMP '2024-01-02 03:04:05.987654',
///   void_value=NULL::VOID,
///   nested_value=ROW(ts=UTC '2024-06-01T00:00:00Z', binary=X'0002'),
///   times=[UTC '2024-01-02T03:04:05Z', NULL],
///   blobs={'first': X'0001', 'missing': NULL::BINARY}
/// )
/// ```
///
/// `from_json`, `base64_decode`, `parse_timestamp_utc`, `parse_timestamp_ntz`, `transform`, and
/// `transform_values` are semantic operations in this cross-engine example. An engine lowers them
/// to native SQL functions or schema-bound physical expressions. The function names in this
/// example are notation for those operations, not required engine APIs.
///
/// Before applying the type-specific decoder, apply the missing and JSON-null rules above. Wrong
/// token kinds and other values outside the table are undefined; implementations need not add
/// guards merely to make those inputs deterministic.
///
/// # Required representation and lowering by type
///
/// The input column below describes the accepted type-specific input. The common rules above may
/// return before this pipeline for missing members and JSON null. Every other value not admitted
/// by the required-input column is undefined. The final column gives examples; it is not an
/// alternative conversion rule.
///
/// | Delta target | Required JSON input | Required SQL-engine decoding pipeline | Undefined examples |
/// |---|---|---|---|
/// | `BYTE` | integer token | extract the exact integer and checked-convert to signed 8-bit | `1.0`, `1e0`, `128`, or `"1"` |
/// | `SHORT` | integer token | extract the exact integer and checked-convert to signed 16-bit | `1.0`, `1e0`, `32768`, or `"1"` |
/// | `INTEGER` | integer token | extract the exact integer and checked-convert to signed 32-bit | `1.0`, `1e0`, `2147483648`, or `"1"` |
/// | `LONG` | integer token | extract without passing through `DOUBLE`; checked-convert to signed 64-bit | `1.0`, `1e0`, overflow, or `"1"` |
/// | `FLOAT` | finite number token | extract as IEEE-754 binary32 using `roundTiesToEven` | finite overflow or a non-numeric token |
/// | `DOUBLE` | finite number token | extract as IEEE-754 binary64 using `roundTiesToEven` | finite overflow or a non-numeric token |
/// | `FLOAT` or `DOUBLE` special | exactly `"NaN"`, `"Infinity"`, or `"-Infinity"` | extract as `VARCHAR`, recognize the three strings, and construct the IEEE value | any other string or unquoted non-finite spelling |
/// | `DECIMAL(p,s)` | number token exactly representable by the target type | read the original number text as arbitrary-precision decimal and convert it exactly, without passing through `DOUBLE` | a string token, a value requiring rounding to scale `s`, or a value exceeding precision `p` |
/// | `BOOLEAN` | `true` or `false` token | extract as SQL `BOOLEAN` | `"true"`, `0`, or `1` |
/// | `STRING` | string token | decode JSON escapes and return SQL UTF-8 character data; preserve `""` | a number, boolean, array, or object |
/// | `BINARY` | padded Base64 string satisfying RFC 4648 sections 3.5 and 4 | extract as `VARCHAR`, validate and decode RFC 4648 Base64, and return SQL binary data; `""` becomes empty bytes | bad alphabet, padding, whitespace, or non-zero unused bits |
/// | `DATE` | string in exact `YYYY-MM-DD` grammar | extract as `VARCHAR`, parse strictly in the proleptic Gregorian calendar, and return SQL `DATE` | invalid calendar date, non-four-digit year, or alternate spelling |
/// | `TIMESTAMP` | string in the timestamp grammar below | extract as `VARCHAR`, parse offsetless text as UTC or apply its explicit offset, truncate to microseconds, and return a UTC instant | invalid grammar, leap second, bad offset, or out-of-range instant |
/// | `TIMESTAMP_NTZ` | offset-free string in the grammar below | extract as `VARCHAR`, parse directly as a timezone-free local date-time, truncate to microseconds, and return timestamp-without-time-zone data | `Z`, a numeric offset, invalid grammar, or out-of-range value |
/// | `VOID` | no non-null JSON value; only missing and JSON null are defined | return typed SQL null | every non-null JSON value, including `""` |
/// | `STRUCT` | object token | retain the object's parent validity and build a SQL `ROW` by recursively decoding requested members | an array, scalar, or string |
/// | `ARRAY<T>` | array token | iterate in input order and build a SQL array by applying the compiled `T` decoder to every element | an object, scalar, or string |
/// | `MAP<STRING,T>` | object token | iterate members, use decoded member names as SQL string keys, and recursively decode every value | an array, scalar, or string |
///
/// `VARIANT`, both interval types, and feature-gated `GEOMETRY` and `GEOGRAPHY` are unsupported.
/// An output schema containing one of these types anywhere in its recursive type tree must be
/// rejected during planning.
///
/// A lexical integer has JSON grammar `-?(0|[1-9][0-9]*)`; `1.0` and `1e0` are not integer
/// representations. Decimal and floating-point targets accept the full JSON number grammar.
/// Finite floating-point underflow, including underflow to signed zero, is defined.
///
/// Decimal conversion must operate on the original number text without passing through a binary
/// floating-point value. The number must be exactly representable by the target `DECIMAL(p,s)`.
/// Fewer fractional digits are zero-padded as needed. An input that requires discarding non-zero
/// fractional digits to reach scale `s`, or whose coefficient at scale `s` contains more than `p`
/// digits, is undefined. For example, `999.9` decoded as `DECIMAL(5,2)` becomes `999.90`, while
/// `999.994` is undefined for that target.
///
/// `BINARY` requires the RFC 4648 alphabet `A-Z`, `a-z`, `0-9`, `+`, `/`, a length divisible by
/// four, zero to two trailing `=` characters exactly where required, zero unused trailing bits,
/// and no whitespace. Re-encoding the decoded bytes with a padded RFC 4648 encoder must reproduce
/// the input. For example, `"AQI="` decodes to the bytes `01 02`, and `""` decodes to an empty
/// byte sequence.
///
/// # Date and timestamp grammar
///
/// Dates use the proleptic Gregorian calendar. Required timestamps have this grammar:
///
/// ```text
/// date          := YYYY "-" MM "-" DD
/// time          := hh ":" mm ":" ss ["." 1*9DIGIT]
/// offset        := "Z" | ("+" | "-") hh ":" mm
/// timestamp     := date ("T" | " ") time [offset]
/// timestamp_ntz := date ("T" | " ") time
/// ```
///
/// Years are `0001..9999`; hours, minutes, and seconds are respectively `00..23`, `00..59`, and
/// `00..59`. Numeric offsets range from `-18:00` through `+18:00`, with an offset hour of 18 only
/// valid when its minutes are zero. The normalized UTC instant must also remain within the stated
/// year range.
///
/// `TIMESTAMP` is parsed directly as a UTC instant. An offsetless value is interpreted as UTC; an
/// explicit offset is applied and the result normalized to UTC. Converting that instant to a
/// display timezone is a separate operation. `TIMESTAMP_NTZ` is a timezone-free local date-time
/// and does not accept `Z` or a numeric offset. Fractions beyond microseconds are truncated. For
/// example:
///
/// - for `TIMESTAMP`, `"2020-01-02T03:04:05.123456789+02:30"` becomes the UTC instant
///   `2020-01-02T00:34:05.123456Z`;
/// - for `TIMESTAMP`, `"2020-01-02 03:04:05"` becomes the UTC instant `2020-01-02T03:04:05Z`;
/// - for `TIMESTAMP_NTZ`, `"2020-01-02T03:04:05.123456789"` becomes the timezone-free value
///   `2020-01-02T03:04:05.123456`.
///
/// # Undefined portability behavior
///
/// An input row is outside the portable contract, and therefore has undefined behavior, for any
/// of the following reasons:
///
/// - the SQL input is null;
/// - the text is not exactly one complete strict RFC 8259 JSON object, including an empty or
///   partial document, multiple roots, trailing non-whitespace, a byte-order mark, comments, single
///   quotes, unquoted names, unescaped controls, or non-standard number syntax;
/// - the root JSON value is not an object: it is JSON null, a string, a number, a boolean, or an
///   array;
/// - an object anywhere in the document has duplicate keys, including in an ignored extra field;
/// - a decoded string is not valid Unicode or contains an unpaired surrogate escape;
/// - a requested value has the wrong JSON token kind, including `""` for a target other than
///   `STRING` or `BINARY`;
/// - an integer is not in lexical integer form or is outside the target range;
/// - a finite float, decimal, date, timestamp, or normalized UTC instant is outside its target
///   range, or a decimal would require rounding to the target scale;
/// - Base64 violates RFC 4648 sections 3.5 or 4, is URL-safe, is incorrectly padded, or contains
///   whitespace;
/// - a date is calendar-invalid, or a timestamp contains a leap second, lowercase `z`, a named
///   zone, or an unsupported offset;
/// - a `TIMESTAMP_NTZ` contains an offset;
/// - any other value is outside the required representation for its target type.
///
/// Here "undefined" is a cross-engine portability boundary, not Rust memory-model undefined
/// behavior. For such a row, an implementation may return SQL null, return a partial result,
/// return any value representable by the declared output type, or report an evaluation error at
/// row, batch, or query scope.
///
/// # Invalid expressions and schemas
///
/// The child expression must produce `STRING`; a type mismatch is an expression error, not an
/// undefined JSON value. Unsupported or ambiguous output schemas must fail when the evaluator is
/// planned. These are a map with a non-`STRING` key, case-insensitive duplicate field names in any
/// struct, `VARIANT`, an interval, `GEOMETRY`, or `GEOGRAPHY` anywhere in the recursive output
/// schema, or `VOID` inside an array or map.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParseJsonExpression {
    /// Expression producing the non-null `STRING` JSON text described by this type's contract.
    pub json_expr: Box<Expression>,
    /// Root struct shape directing recursive decoding, including result nullability.
    pub output_schema: SchemaRef,
}

/// An expression that casts a child expression to a target type, following SQL `CAST` semantics: a
/// value that cannot be represented in the target type evaluates to NULL rather than erroring.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CastExpression {
    /// The expression whose value is cast.
    pub expr: Box<Expression>,
    /// The type the value is cast to.
    pub target: DataType,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JunctionPredicate {
    /// The operator.
    pub op: JunctionPredicateOp,
    /// The input predicates.
    pub preds: Vec<Predicate>,
}

// NOTE: We have to use `Arc<dyn OpaquePredicateOp>` instead of `Box<dyn OpaquePredicateOp>` because
// we cannot require `OpaquePredicateOp: Clone` (not a dyn-compatible trait). Instead, we must rely
// on cheap `Arc` clone, which does not duplicate the inner object.
//
// TODO(#1564): OpaquePredicate currently does not support serialization or deserialization. In the
// future, the [`OpaquePredicateOp`] trait can be extended to support ser/de.
#[derive(Clone, Debug)]
pub struct OpaquePredicate {
    pub op: OpaquePredicateOpRef,
    pub exprs: Vec<Expression>,
}
fn fail_serialize_opaque_predicate<S>(
    _value: &OpaquePredicate,
    _serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Err(ser::Error::custom("Cannot serialize an Opaque Predicate"))
}

fn fail_deserialize_opaque_predicate<'de, D>(_deserializer: D) -> Result<OpaquePredicate, D::Error>
where
    D: Deserializer<'de>,
{
    Err(de::Error::custom("Cannot deserialize an Opaque Predicate"))
}

impl OpaquePredicate {
    pub(crate) fn new(
        op: OpaquePredicateOpRef,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        let exprs = exprs.into_iter().collect();
        Self { op, exprs }
    }
}

// NOTE: We have to use `Arc<dyn OpaqueExpressionOp>` instead of `Box<dyn OpaqueExpressionOp>`
// because we cannot require `OpaqueExpressionOp: Clone` (not a dyn-compatible trait). Instead, we
// must rely on cheap `Arc` clone, which does not duplicate the inner object.
//
// TODO(#1564): OpaqueExpression currently does not support serialization or deserialization. In the
// future, the [`OpaqueExpressionOp`] trait can be extended to support ser/de.
#[derive(Clone, Debug)]
pub struct OpaqueExpression {
    pub op: OpaqueExpressionOpRef,
    pub exprs: Vec<Expression>,
}

impl OpaqueExpression {
    pub(crate) fn new(
        op: OpaqueExpressionOpRef,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        let exprs = exprs.into_iter().collect();
        Self { op, exprs }
    }
}

fn fail_serialize_opaque_expression<S>(
    _value: &OpaqueExpression,
    _serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Err(ser::Error::custom("Cannot serialize an Opaque Expression"))
}

fn fail_deserialize_opaque_expression<'de, D>(
    _deserializer: D,
) -> Result<OpaqueExpression, D::Error>
where
    D: Deserializer<'de>,
{
    Err(de::Error::custom("Cannot deserialize an Opaque Expression"))
}

/// A SQL expression.
///
/// These expressions do not track or validate data types, other than the type
/// of literals. It is up to the expression evaluator to validate the
/// expression against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A literal value.
    Literal(Scalar),
    /// A column reference by name. A [`ColumnName`] is a path, so a multi-segment name like
    /// `add.stats.numRecords` descends one nested struct field per segment, matching by name.
    Column(ColumnName),
    /// A predicate treated as a boolean expression
    Predicate(Box<Predicate>), // should this be Arc?
    /// A struct computed from one expression per output field, in field order.
    ///
    /// Field names and nullability come from the surrounding output schema (the evaluator's
    /// `result_type`, such as a [`Project`]'s target field), and each field expression's type is
    /// validated against the schema, so building a struct takes both. The expression count must
    /// equal the output field count.
    ///
    /// The optional nullability predicate says when to *keep* the struct: a row survives only
    /// where it is true, and nulls entirely where it is false or null.
    ///
    /// ```sql
    /// CASE WHEN keep_pred THEN struct(expr1, expr2, ...) END
    /// ```
    ///
    /// [`Project`]: crate::plans::ir::nodes::Project
    Struct(Vec<ExpressionRef>, Option<ExpressionRef>),
    /// A sparse patch of a struct. More efficient than `Struct` for wide schemas
    /// where only a few fields change, achieving O(changes) instead of O(schema_width) complexity.
    #[serde(alias = "Transform")]
    StructPatch(ExpressionStructPatch),
    /// An expression that takes one expression as input.
    Unary(UnaryExpression),
    /// An expression that takes two expressions as input.
    Binary(BinaryExpression),
    /// An expression that takes a variable number of expressions as input.
    Variadic(VariadicExpression),
    /// An expression that the engine defines and implements. Kernel interacts with the expression
    /// only through methods provided by the [`OpaqueExpressionOp`] trait.
    #[serde(serialize_with = "fail_serialize_opaque_expression")]
    #[serde(deserialize_with = "fail_deserialize_opaque_expression")]
    Opaque(OpaqueExpression),
    /// An unknown expression (i.e. one that neither kernel nor engine attempts to evaluate). For
    /// data skipping purposes, kernel treats unknown expressions as if they were literal NULL
    /// values (which may disable skipping if it "poisons" the predicate), but engines MUST NOT
    /// attempt to interpret them as NULL when evaluating query filters because it could produce
    /// incorrect results. For example, converting `WHERE <fancy-udf-invocation> IS NULL` to `WHERE
    /// <unknown> IS NULL` to `WHERE NULL IS NULL` is equivalent to `WHERE TRUE` and would include
    /// all rows -- almost certainly NOT what the query author intended. Use `Expression::Opaque`
    /// for expressions kernel doesn't understand but which engine can still evaluate.
    Unknown(String),
    /// Parse a JSON string expression into a struct with the given schema. Unparseable input,
    /// which includes an empty string, must yield NULL rather than error; see
    /// [`ParseJsonExpression`].
    ParseJson(ParseJsonExpression),
    /// Parse an Add action's `partitionValues` map into a typed struct. A null map produces a null
    /// struct, and the behavior for duplicate keys is undefined. See [`MapToStructExpression`] for
    /// the complete Delta partition-value contract.
    // TODO: Rename MapToStruct to ParsePartitionValues.
    MapToStruct(MapToStructExpression),
    /// Cast a child expression to a target type. See [`CastExpression`].
    Cast(CastExpression),
}

/// A SQL predicate.
///
/// These predicates do not track or validate data types, other than the type
/// of literals. It is up to the predicate evaluator to validate the
/// predicate against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    /// A boolean-valued expression, useful for e.g. `AND(<boolean_col1>, <boolean_col2>)`.
    BooleanExpression(Expression),
    /// Boolean inversion (true <-> false)
    ///
    /// NOTE: NOT is not a normal unary predicate, because it requires a predicate as input (not an
    /// expression), and is never directly evaluated. Instead, observing that all predicates are
    /// invertible, NOT is always pushed down into its child predicate, inverting it. For example,
    /// `NOT (a < b)` pushes down and inverts `<` to `>=`, producing `a >= b`.
    Not(Box<Predicate>),
    /// A unary operation.
    Unary(UnaryPredicate),
    /// A binary operation.
    Binary(BinaryPredicate),
    /// A junction operation (AND/OR).
    Junction(JunctionPredicate),
    /// A predicate that the engine defines and implements. Kernel interacts with the predicate
    /// only through methods provided by the [`OpaquePredicateOp`] trait.
    #[serde(serialize_with = "fail_serialize_opaque_predicate")]
    #[serde(deserialize_with = "fail_deserialize_opaque_predicate")]
    Opaque(OpaquePredicate),
    /// An unknown predicate (i.e. one that neither kernel nor engine attempts to evaluate). For
    /// data skipping purposes, kernel treats unknown predicates as if they were literal NULL
    /// values (which may disable skipping if it "poisons" the predicate), but engines MUST NOT
    /// attempt to interpret them as NULL when evaluating query filters because it could
    /// produce incorrect results. For example, converting `WHERE <fancy-udf-invocation>` to
    /// `WHERE NULL` is equivalent to `WHERE FALSE` and would filter out all rows -- almost
    /// certainly NOT what the query author intended. Use `Predicate::Opaque` for predicates
    /// kernel doesn't understand but which engine can still evaluate.
    Unknown(String),
}

////////////////////////////////////////////////////////////////////////
// Struct/Enum impls
////////////////////////////////////////////////////////////////////////

impl BinaryPredicateOp {
    /// True if this is a comparison for which NULL input always produces NULL output
    pub(crate) fn is_null_intolerant(&self) -> bool {
        use BinaryPredicateOp::*;
        match self {
            LessThan | GreaterThan | Equal => true,
            Distinct | In => false, // tolerates NULL input
        }
    }
}

impl JunctionPredicateOp {
    pub(crate) fn invert(&self) -> JunctionPredicateOp {
        use JunctionPredicateOp::*;
        match self {
            And => Or,
            Or => And,
        }
    }
}

impl UnaryExpression {
    pub(crate) fn new(op: UnaryExpressionOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl UnaryPredicate {
    pub(crate) fn new(op: UnaryPredicateOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self { op, expr }
    }
}

impl BinaryExpression {
    pub(crate) fn new(
        op: BinaryExpressionOp,
        left: impl Into<Expression>,
        right: impl Into<Expression>,
    ) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

impl BinaryPredicate {
    pub(crate) fn new(
        op: BinaryPredicateOp,
        left: impl Into<Expression>,
        right: impl Into<Expression>,
    ) -> Self {
        let left = Box::new(left.into());
        let right = Box::new(right.into());
        Self { op, left, right }
    }
}

impl VariadicExpression {
    pub(crate) fn new(
        op: VariadicExpressionOp,
        exprs: impl IntoIterator<Item = impl Into<Expression>>,
    ) -> Self {
        let exprs = exprs.into_iter().map(Into::into).collect();
        Self { op, exprs }
    }
}

impl ParseJsonExpression {
    pub(crate) fn new(json_expr: impl Into<Expression>, output_schema: SchemaRef) -> Self {
        Self {
            json_expr: Box::new(json_expr.into()),
            output_schema,
        }
    }
}

/// A Delta-specific expression that parses an Add action's `partitionValues` map into a typed
/// `partitionValues_parsed` struct.
///
/// This is not a generic map-to-struct conversion. The input expression must evaluate to
/// `Map<String, String>`, and the evaluator supplies a target [`StructType`] as this expression's
/// result type. The expression does not carry this type itself and cannot be evaluated from
/// `map_expr` alone. Its evaluation context must provide and propagate the target output type. The
/// target struct is semantic input: field names select partition values, field types determine how
/// they are parsed, and field order and nullability define the result struct. Names are matched
/// exactly and case-sensitively against the map keys. With column mapping, both the map keys and
/// target-schema field names are physical names.
///
/// # Null and Missing Key behavior
///
/// For each input row:
///
/// - A null map produces a null struct, not a non-null struct whose fields are all null.
/// - A non-null map, including an empty map, produces a non-null struct.
/// - A key that is in the target output schema, but missing from the map, produces a null field.
/// - A key that is in the input map, but not in the target output schema, is ignored.
/// - A key with a null value in the input map produces a null field in the output struct.
/// - Fields appear in target-schema order and must have primitive types.
/// - Duplicate keys are not valid `partitionValues` input and their result is undefined. Engines
///   may ignore, reject, or arbitrarily select one; callers must not depend on the behavior.
///
/// # Value parsing
///
/// A literal empty string has special read compatibility behavior: it stays empty for STRING,
/// becomes empty bytes for BINARY, and becomes null for every other primitive type. Delta writers
/// normally encode an empty partition value as JSON null, but readers must preserve this behavior
/// for existing tables that contain a literal empty string.
///
/// Non-empty strings must follow the Delta [protocol] partition value serialization rules. Kernel's
/// reference evaluator implements these rules with [`PrimitiveType::parse_scalar`] and equivalent
/// Arrow parsers for dates and timestamps, establishing this engine-facing behavior:
///
/// - STRING: return the input string unchanged.
/// - BINARY: return the input string's UTF-8 bytes after JSON string unescaping, not hex or base64.
/// - BYTE, SHORT, INTEGER, and LONG: parse a signed base-10 integer in the target type's range.
/// - FLOAT and DOUBLE: parse a floating-point number, including `NaN`, `Infinity`, and `-Infinity`.
/// - DECIMAL(p, s): require an effective scale of exactly `s` and precision that fits `p`; never
///   round or rescale.
/// - BOOLEAN: accept case-insensitive `true` or `false`, with no numeric or yes/no aliases.
/// - DATE: parse `{year}-{month}-{day}`.
/// - TIMESTAMP: parse either 1) an ISO 8601 timestamp with an explicit offset, normalized to UTC;
///   or 2) a space-separated timestamp without a zone. Currently, kernel expects case 2 to be
///   interpreted as UTC, however the protocol specifies that it should be interpreted in the
///   writer's time zone. TODO: MapToStruct needs to be updated to take in a timezone as a
///   parameter.
/// - TIMESTAMP_NTZ: parse a space-separated timestamp without an offset and preserve the local
///   wall-clock value.
/// - Interval types: parse an ANSI interval literal accepted by [`PrimitiveType::parse_scalar`].
/// - VOID: reject every non-empty value.
///
/// Kernel's UTC interpretation of a zone-less TIMESTAMP is explicit here: the Delta protocol says
/// that form is interpreted in the writer's time zone, but that time zone is not carried by this
/// expression. Modern writers should use the protocol's UTC-adjusted ISO 8601 form.
///
/// Non-empty geometry and geography values are unsupported. Struct, array, map, and variant target
/// fields are not primitive partition types and are rejected. Any other unparseable non-empty value
/// returns [`Error::ParseError`] and fails evaluation; it does not silently become null.
///
/// # Implementing this expression
///
/// A connector generally cannot implement this contract as `CAST(map[key] AS target_type)`.
/// Generic casts may accept additional boolean spellings, round or rescale decimals, use a session
/// time zone, or turn malformed values into null. Implement a dedicated parser (or validate before
/// casting), handle the empty-string cases before parsing, and preserve the input map's row-level
/// nulls on the output struct.
///
/// [protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
/// [`PrimitiveType::parse_scalar`]: crate::schema::PrimitiveType::parse_scalar
/// [`StructType`]: crate::schema::StructType
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MapToStructExpression {
    /// The expression that evaluates to a `Map<String, String>` column.
    pub map_expr: Box<Expression>,
}

impl MapToStructExpression {
    pub(crate) fn new(map_expr: impl Into<Expression>) -> Self {
        Self {
            map_expr: Box::new(map_expr.into()),
        }
    }
}

impl CastExpression {
    pub(crate) fn new(expr: impl Into<Expression>, target: DataType) -> Self {
        Self {
            expr: Box::new(expr.into()),
            target,
        }
    }
}

impl JunctionPredicate {
    pub(crate) fn new(op: JunctionPredicateOp, preds: Vec<Predicate>) -> Self {
        Self { op, preds }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        references.transform_expr(self);
        references.0
    }

    /// Create a new column name expression from input satisfying `FromIterator for ColumnName`.
    pub fn column(field_names: impl CollectInto<ColumnName>) -> Expression {
        ColumnName::new(field_names).into()
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    /// Wraps a predicate as a boolean-valued expression
    pub fn from_pred(value: Predicate) -> Self {
        match value {
            Predicate::BooleanExpression(expr) => expr,
            _ => Self::Predicate(Box::new(value)),
        }
    }

    /// Create a new struct expression.
    ///
    /// The field names and types are supplied by the caller at evaluation time via the
    /// `result_type` parameter of the expression evaluator. Use this when the schema is
    /// always available from external context (e.g. the expression is the top-level output
    /// of [`crate::ExpressionEvaluator`]).
    pub fn struct_from(exprs: impl IntoIterator<Item = impl Into<Arc<Self>>>) -> Self {
        Self::Struct(exprs.into_iter().map(Into::into).collect(), None)
    }

    /// Create a new struct expression with a nullability predicate.
    ///
    /// When the predicate evaluates to false or null for a row, the entire struct is null
    /// for that row.
    pub fn struct_with_nullability_from(
        exprs: impl IntoIterator<Item = impl Into<Arc<Self>>>,
        nullability_predicate: impl Into<Arc<Self>>,
    ) -> Self {
        Self::Struct(
            exprs.into_iter().map(Into::into).collect(),
            Some(nullability_predicate.into()),
        )
    }

    /// Creates a new struct patch expression from a raw patch or patch builder.
    ///
    /// Returns an expression that applies the supplied sparse patch to an input struct. Passing a
    /// raw [`ExpressionStructPatch`] is infallible; passing an [`ExpressionStructPatchBuilder`]
    /// validates and lowers the recorded operations before constructing the expression.
    ///
    /// # Errors
    ///
    /// Returns an error if the supplied patch builder contains conflicting operations.
    pub fn struct_patch<P>(patch: P) -> DeltaResult<Self>
    where
        P: TryInto<ExpressionStructPatch>,
        Error: From<P::Error>,
    {
        Ok(Self::StructPatch(patch.try_into()?))
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(self) -> Predicate {
        Predicate::is_null(self)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(self) -> Predicate {
        Predicate::is_not_null(self)
    }

    /// Create a new predicate `self == other`
    pub fn eq(self, other: impl Into<Self>) -> Predicate {
        Predicate::eq(self, other)
    }

    /// Create a new predicate `self != other`
    pub fn ne(self, other: impl Into<Self>) -> Predicate {
        Predicate::ne(self, other)
    }

    /// Create a new predicate `self <= other`
    pub fn le(self, other: impl Into<Self>) -> Predicate {
        Predicate::le(self, other)
    }

    /// Create a new predicate `self < other`
    pub fn lt(self, other: impl Into<Self>) -> Predicate {
        Predicate::lt(self, other)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(self, other: impl Into<Self>) -> Predicate {
        Predicate::ge(self, other)
    }

    /// Create a new predicate `self > other`
    pub fn gt(self, other: impl Into<Self>) -> Predicate {
        Predicate::gt(self, other)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(self, other: impl Into<Self>) -> Predicate {
        Predicate::distinct(self, other)
    }

    /// Creates a new unary expression
    pub fn unary(op: UnaryExpressionOp, expr: impl Into<Expression>) -> Self {
        Self::Unary(UnaryExpression::new(op, expr))
    }

    /// Creates a new binary expression lhs OP rhs
    pub fn binary(
        op: BinaryExpressionOp,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryExpression::new(op, lhs, rhs))
    }

    /// Creates a new variadic expression
    pub fn variadic(
        op: VariadicExpressionOp,
        exprs: impl IntoIterator<Item = impl Into<Expression>>,
    ) -> Self {
        Self::Variadic(VariadicExpression::new(op, exprs))
    }

    /// Creates a new COALESCE expression that returns the first non-null value.
    ///
    /// COALESCE evaluates expressions in order and returns the first non-null result.
    /// If all expressions evaluate to null, the result is null.
    pub fn coalesce(exprs: impl IntoIterator<Item = impl Into<Expression>>) -> Self {
        Self::variadic(VariadicExpressionOp::Coalesce, exprs)
    }

    /// Creates a new Array constructor expression. See [`VariadicExpressionOp::Array`].
    pub fn array(exprs: impl IntoIterator<Item = impl Into<Expression>>) -> Self {
        Self::variadic(VariadicExpressionOp::Array, exprs)
    }

    /// Creates a new opaque expression
    pub fn opaque(
        op: impl OpaqueExpressionOp,
        exprs: impl IntoIterator<Item = Expression>,
    ) -> Self {
        Self::Opaque(OpaqueExpression::new(Arc::new(op), exprs))
    }

    /// Creates a new unknown expression
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::Unknown(name.into())
    }

    /// Creates a schema-directed JSON decoding expression.
    ///
    /// See [`ParseJsonExpression`] for the complete semantic contract.
    pub fn parse_json(json_expr: impl Into<Expression>, output_schema: SchemaRef) -> Self {
        Self::ParseJson(ParseJsonExpression::new(json_expr, output_schema))
    }

    /// Parses an Add action's `partitionValues` map into a typed struct whose schema comes from the
    /// evaluator's result type. A null map produces a null struct; missing and null values produce
    /// null fields; literal empty strings stay empty only for STRING and BINARY. Duplicate-key
    /// behavior is undefined. See [`MapToStructExpression`] for the complete contract.
    pub fn map_to_struct(map_expr: impl Into<Expression>) -> Self {
        Self::MapToStruct(MapToStructExpression::new(map_expr))
    }

    /// Creates a new cast of `expr` to `target`, following SQL `CAST` semantics (unrepresentable
    /// values become NULL). See [`CastExpression`].
    pub fn cast(expr: impl Into<Expression>, target: DataType) -> Self {
        Self::Cast(CastExpression::new(expr, target))
    }
}

impl Predicate {
    /// Literal boolean true.
    pub const TRUE: Self = Self::literal(true);
    /// Literal boolean false.
    pub const FALSE: Self = Self::literal(false);
    /// NULL boolean literal.
    pub const NULL: Self =
        Self::BooleanExpression(Expression::Literal(Scalar::Null(DataType::BOOLEAN)));

    /// Returns a set of columns referenced by this predicate.
    pub fn references(&self) -> HashSet<&ColumnName> {
        let mut references = GetColumnReferences::default();
        references.transform_pred(self);
        references.0
    }

    /// Creates a new boolean column reference. See also [`Expression::column`].
    pub fn column(field_names: impl CollectInto<ColumnName>) -> Self {
        Self::from_expr(ColumnName::new(field_names))
    }

    /// Create a boolean literal predicate from a runtime `bool`.
    ///
    /// Prefer [`Self::TRUE`] / [`Self::FALSE`] when the value is statically known.
    pub const fn literal(value: bool) -> Self {
        Self::BooleanExpression(Expression::Literal(Scalar::Boolean(value)))
    }

    /// Converts a boolean-valued expression into a predicate
    pub fn from_expr(expr: impl Into<Expression>) -> Self {
        match expr.into() {
            Expression::Predicate(p) => *p,
            expr => Predicate::BooleanExpression(expr),
        }
    }

    /// Logical NOT (boolean inversion)
    pub fn not(pred: impl Into<Self>) -> Self {
        Self::Not(Box::new(pred.into()))
    }

    /// Create a new predicate `self IS NULL`
    pub fn is_null(expr: impl Into<Expression>) -> Self {
        Self::unary(UnaryPredicateOp::IsNull, expr)
    }

    /// Create a new predicate `self IS NOT NULL`
    pub fn is_not_null(expr: impl Into<Expression>) -> Self {
        Self::not(Self::is_null(expr))
    }

    /// Create a new predicate `self == other`
    pub fn eq(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::Equal, a, b)
    }

    /// Create a new predicate `self != other`
    pub fn ne(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::Equal, a, b))
    }

    /// Create a new predicate `self <= other`
    pub fn le(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::GreaterThan, a, b))
    }

    /// Create a new predicate `self < other`
    pub fn lt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::LessThan, a, b)
    }

    /// Create a new predicate `self >= other`
    pub fn ge(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::not(Self::binary(BinaryPredicateOp::LessThan, a, b))
    }

    /// Create a new predicate `self > other`
    pub fn gt(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::GreaterThan, a, b)
    }

    /// Create a new predicate `DISTINCT(self, other)`
    pub fn distinct(a: impl Into<Expression>, b: impl Into<Expression>) -> Self {
        Self::binary(BinaryPredicateOp::Distinct, a, b)
    }

    /// Create a new predicate `self AND other`
    pub fn and(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::and_from([a.into(), b.into()])
    }

    /// Create a new predicate `self OR other`
    pub fn or(a: impl Into<Self>, b: impl Into<Self>) -> Self {
        Self::or_from([a.into(), b.into()])
    }

    /// Creates a new predicate AND(preds...). See [`Self::junction`] for normalization of
    /// empty and single-element inputs.
    pub fn and_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionPredicateOp::And, preds)
    }

    /// Creates a new predicate OR(preds...). See [`Self::junction`] for normalization of
    /// empty and single-element inputs.
    pub fn or_from(preds: impl IntoIterator<Item = Self>) -> Self {
        Self::junction(JunctionPredicateOp::Or, preds)
    }

    /// Creates a new unary predicate OP expr
    pub fn unary(op: UnaryPredicateOp, expr: impl Into<Expression>) -> Self {
        let expr = Box::new(expr.into());
        Self::Unary(UnaryPredicate { op, expr })
    }

    /// Creates a new binary predicate lhs OP rhs
    pub fn binary(
        op: BinaryPredicateOp,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::Binary(BinaryPredicate {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        })
    }

    /// Creates a new junction predicate OP(preds...). Normalizes degenerate cases:
    ///
    /// - Empty junction returns the identity element (the value that has no effect when combined
    ///   with other predicates under the same operator):
    ///   - `AND()` -> `true`, because `true AND p` == `p` for any predicate `p`.
    ///   - `OR()` -> `false`, because `false OR p` == `p` for any predicate `p`.
    /// - Single-element junction unwraps the element: `AND(p)` / `OR(p)` -> `p`.
    pub fn junction(op: JunctionPredicateOp, preds: impl IntoIterator<Item = Self>) -> Self {
        let mut preds: Vec<_> = preds.into_iter().collect();
        match preds.len() {
            0 => match op {
                JunctionPredicateOp::And => Self::TRUE,
                JunctionPredicateOp::Or => Self::FALSE,
            },
            // A junction of one predicate is just that predicate.
            1 => preds.remove(0),
            _ => Self::Junction(JunctionPredicate { op, preds }),
        }
    }

    /// Creates a new opaque predicate
    pub fn opaque(op: impl OpaquePredicateOp, exprs: impl IntoIterator<Item = Expression>) -> Self {
        Self::Opaque(OpaquePredicate::new(Arc::new(op), exprs))
    }

    /// Creates a new unknown predicate
    pub fn unknown(name: impl Into<String>) -> Self {
        Self::Unknown(name.into())
    }
}

////////////////////////////////////////////////////////////////////////
// Trait impls
////////////////////////////////////////////////////////////////////////

impl PartialEq for OpaquePredicate {
    fn eq(&self, other: &Self) -> bool {
        self.op.dyn_eq(other.op.any_ref()) && self.exprs == other.exprs
    }
}

impl PartialEq for OpaqueExpression {
    fn eq(&self, other: &Self) -> bool {
        self.op.dyn_eq(other.op.any_ref()) && self.exprs == other.exprs
    }
}

impl Display for UnaryExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use UnaryExpressionOp::*;
        match self {
            ToJson => write!(f, "TO_JSON"),
        }
    }
}

impl Display for BinaryExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryExpressionOp::*;
        match self {
            Plus => write!(f, "+"),
            Minus => write!(f, "-"),
            Multiply => write!(f, "*"),
            Divide => write!(f, "/"),
        }
    }
}

impl Display for VariadicExpressionOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use VariadicExpressionOp::*;
        match self {
            Coalesce => write!(f, "COALESCE"),
            Array => write!(f, "ARRAY"),
        }
    }
}

impl Display for BinaryPredicateOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use BinaryPredicateOp::*;
        match self {
            LessThan => write!(f, "<"),
            GreaterThan => write!(f, ">"),
            Equal => write!(f, "="),
            // TODO(roeap): AFAIK DISTINCT does not have a commonly used operator symbol
            // so ideally this would not be used as we use Display for rendering expressions
            // in our code we take care of this, but theirs might not ...
            Distinct => write!(f, "DISTINCT"),
            In => write!(f, "IN"),
        }
    }
}

// Helper for displaying the children of variadic expressions and predicates
fn format_child_list<T: Display>(children: &[T]) -> String {
    children.iter().map(|c| format!("{c}")).join(", ")
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        match self {
            Literal(l) => write!(f, "{l}"),
            Column(name) => write!(f, "Column({name})"),
            Predicate(p) => write!(f, "{p}"),
            Struct(exprs, _) => write!(f, "Struct({})", format_child_list(exprs)),
            StructPatch(patch) => {
                write!(f, "StructPatch(")?;
                let mut sep = "";
                if !patch.prepended_fields.is_empty() {
                    let prepended_fields = format_child_list(&patch.prepended_fields);
                    write!(f, "prepend [{prepended_fields}]")?;
                    sep = ", ";
                }
                for (field_name, field_patch) in &patch.field_patches {
                    if !field_patch.keep_input && field_patch.insertions.is_empty() {
                        write!(f, "{sep}drop {field_name}")?;
                        sep = ", ";
                    }
                    if !field_patch.insertions.is_empty() {
                        let insertions = format_child_list(&field_patch.insertions);
                        let action = if field_patch.keep_input {
                            "after"
                        } else {
                            "replace/after"
                        };
                        write!(f, "{sep}{action} {field_name} insert [{insertions}]")?;
                        sep = ", ";
                    }
                }
                if !patch.appended_fields.is_empty() {
                    let appended_fields = format_child_list(&patch.appended_fields);
                    write!(f, "{sep}append [{appended_fields}]")?;
                }
                write!(f, ")")
            }
            Unary(UnaryExpression { op, expr }) => write!(f, "{op}({expr})"),
            Binary(BinaryExpression { op, left, right }) => write!(f, "{left} {op} {right}"),
            Variadic(VariadicExpression { op, exprs }) => {
                write!(f, "{op}({})", format_child_list(exprs))
            }
            Opaque(OpaqueExpression { op, exprs }) => {
                write!(f, "{op:?}({})", format_child_list(exprs))
            }
            Unknown(name) => write!(f, "<unknown: {name}>"),
            ParseJson(p) => {
                write!(
                    f,
                    "PARSE_JSON({}, <schema:{} fields>)",
                    p.json_expr,
                    p.output_schema.fields().len()
                )
            }
            MapToStruct(m) => write!(f, "MAP_TO_STRUCT({})", m.map_expr),
            Cast(c) => write!(f, "CAST({} AS {})", c.expr, c.target),
        }
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Predicate::*;
        match self {
            BooleanExpression(expr) => write!(f, "{expr}"),
            Not(pred) => write!(f, "NOT({pred})"),
            Binary(BinaryPredicate {
                op: BinaryPredicateOp::Distinct,
                left,
                right,
            }) => write!(f, "DISTINCT({left}, {right})"),
            Binary(BinaryPredicate { op, left, right }) => write!(f, "{left} {op} {right}"),
            Unary(UnaryPredicate { op, expr }) => match op {
                UnaryPredicateOp::IsNull => write!(f, "{expr} IS NULL"),
            },
            Junction(JunctionPredicate { op, preds }) => {
                let op = match op {
                    JunctionPredicateOp::And => "AND",
                    JunctionPredicateOp::Or => "OR",
                };
                write!(f, "{op}({})", format_child_list(preds))
            }
            Opaque(OpaquePredicate { op, exprs }) => {
                write!(f, "{op:?}({})", format_child_list(exprs))
            }
            Unknown(name) => write!(f, "<unknown: {name}>"),
        }
    }
}

impl From<Scalar> for Expression {
    fn from(value: Scalar) -> Self {
        Self::literal(value)
    }
}

impl From<ColumnName> for Expression {
    fn from(value: ColumnName) -> Self {
        Self::Column(value)
    }
}

impl From<Predicate> for Expression {
    fn from(value: Predicate) -> Self {
        Self::from_pred(value)
    }
}

impl From<ColumnName> for Predicate {
    fn from(value: ColumnName) -> Self {
        Self::from_expr(value)
    }
}

impl<R: Into<Expression>> std::ops::Add<R> for Expression {
    type Output = Self;

    fn add(self, rhs: R) -> Self::Output {
        Self::binary(BinaryExpressionOp::Plus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Sub<R> for Expression {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Minus, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Mul<R> for Expression {
    type Output = Self;

    fn mul(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Multiply, self, rhs)
    }
}

impl<R: Into<Expression>> std::ops::Div<R> for Expression {
    type Output = Self;

    fn div(self, rhs: R) -> Self {
        Self::binary(BinaryExpressionOp::Divide, self, rhs)
    }
}

/// Retrieves the set of column names referenced by an expression.
#[derive(Default)]
struct GetColumnReferences<'a>(HashSet<&'a ColumnName>);

impl<'a> ExpressionTransform<'a> for GetColumnReferences<'a> {
    transform_output_type!(|'a, T| ());

    fn transform_expr_column(&mut self, name: &'a ColumnName) {
        self.0.insert(name);
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use super::{col, column_pred, lit, DataType, Expression as Expr, Predicate as Pred};

    /// Helper function to verify roundtrip serialization/deserialization
    fn assert_roundtrip<T: Serialize + DeserializeOwned + PartialEq + Debug>(value: &T) {
        let json = serde_json::to_string(value).expect("serialization should succeed");
        let deserialized: T = serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(value, &deserialized, "roundtrip should preserve value");
    }

    #[test]
    fn test_expression_format() {
        let cases = [
            (col!("x"), "Column(x)"),
            (
                (col!("x") + lit(4)) / lit(10) * lit(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (
                Expr::struct_from([col!("x"), lit(2), lit(10)]),
                "Struct(Column(x), 2, 10)",
            ),
            (
                Expr::array([col!("x"), col!("y"), lit(0)]),
                "ARRAY(Column(x), Column(y), 0)",
            ),
            (
                Expr::cast(col!("x"), DataType::DATE),
                "CAST(Column(x) AS date)",
            ),
        ];

        for (expr, expected) in cases {
            let result = format!("{expr}");
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_predicate_format() {
        let cases = [
            (column_pred!("x"), "Column(x)"),
            (col!("x").eq(lit(2)), "Column(x) = 2"),
            ((col!("x") - lit(4)).lt(lit(10)), "Column(x) - 4 < 10"),
            (
                Pred::and(col!("x").ge(lit(2)), col!("x").le(lit(10))),
                "AND(NOT(Column(x) < 2), NOT(Column(x) > 10))",
            ),
            (
                Pred::and_from([
                    col!("x").ge(lit(2)),
                    col!("x").le(lit(10)),
                    col!("x").le(lit(100)),
                ]),
                "AND(NOT(Column(x) < 2), NOT(Column(x) > 10), NOT(Column(x) > 100))",
            ),
            (
                Pred::or(col!("x").gt(lit(2)), col!("x").lt(lit(10))),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (col!("x").eq(lit("foo")), "Column(x) = 'foo'"),
        ];

        for (pred, expected) in cases {
            let result = format!("{pred}");
            assert_eq!(result, expected);
        }
    }

    // ==================== Serde Roundtrip Tests ====================

    mod serde_tests {
        use std::sync::Arc;

        use super::assert_roundtrip;
        use crate::expressions::scalars::{ArrayData, DecimalData, MapData, StructData};
        use crate::expressions::{
            col, column_name, lit, null_lit, BinaryExpressionOp, BinaryPredicateOp, ColumnName,
            Expression, ExpressionStructPatchBuilder, Predicate, Scalar, UnaryExpressionOp,
        };
        use crate::schema::{ArrayType, DataType, DecimalType, MapType, StructField};
        use crate::unit_test_utils::assert_result_error_with_message;

        // ==================== Expression::Literal Tests ====================

        #[test]
        fn test_literal_scalars_roundtrip() {
            // Test all primitive scalar types that have proper PartialEq
            let cases: Vec<Expression> = vec![
                // Numeric types
                lit(42i32),         // Integer
                lit(9999999999i64), // Long
                lit(123i16),        // Short
                lit(42i8),          // Byte
                lit(1.12345677_32), // Float
                lit(1.12345667_64), // Double
                // String and Boolean
                lit("hello world"),
                lit(true),
                lit(false),
                // Temporal types
                Expression::Literal(Scalar::Timestamp(1234567890000000)),
                Expression::Literal(Scalar::TimestampNtz(1234567890000000)),
                Expression::Literal(Scalar::Date(19000)),
                // Binary
                lit(vec![1u8, 2, 3, 4, 5]),
                // Decimal
                Expression::Literal(Scalar::Decimal(
                    DecimalData::try_new(12345i128, DecimalType::try_new(10, 2).unwrap()).unwrap(),
                )),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        #[test]
        fn test_literal_complex_scalars_roundtrip() {
            // Test complex scalar types that need JSON comparison (partial_cmp returns None)
            let cases: Vec<Expression> = vec![
                // Null with different types
                null_lit(DataType::INTEGER),
                null_lit(DataType::STRING),
                null_lit(DataType::BOOLEAN),
                // Array
                Expression::Literal(Scalar::Array(
                    ArrayData::try_new(
                        ArrayType::new(DataType::INTEGER, false),
                        vec![Scalar::Integer(1), Scalar::Integer(2), Scalar::Integer(3)],
                    )
                    .unwrap(),
                )),
                // Map
                Expression::Literal(Scalar::Map(
                    MapData::try_new(
                        MapType::new(DataType::STRING, DataType::INTEGER, false),
                        vec![
                            (Scalar::String("a".to_string()), Scalar::Integer(1)),
                            (Scalar::String("b".to_string()), Scalar::Integer(2)),
                        ],
                    )
                    .unwrap(),
                )),
                // Struct
                Expression::Literal(Scalar::Struct(
                    StructData::try_new(
                        vec![
                            StructField::nullable("x", DataType::INTEGER),
                            StructField::nullable("y", DataType::STRING),
                        ],
                        vec![Scalar::Integer(42), Scalar::String("hello".to_string())],
                    )
                    .unwrap(),
                )),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        // ==================== Expression::Column Tests ====================

        #[test]
        fn test_column_expressions_roundtrip() {
            let cases: Vec<Expression> =
                vec![col!("my_column"), col!("parent.child"), col!("a.b.c.d")];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        #[test]
        fn test_column_names_roundtrip() {
            let cases: Vec<ColumnName> = vec![
                column_name!("simple"),
                column_name!("a.b.c"),
                ColumnName::default(),
            ];

            for col in &cases {
                assert_roundtrip(col);
            }
        }

        // ==================== Expression Operations Tests ====================

        #[test]
        fn test_unary_expression_roundtrip() {
            let expr = Expression::unary(UnaryExpressionOp::ToJson, col!("data"));
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_binary_expressions_roundtrip() {
            let ops = [
                BinaryExpressionOp::Plus,
                BinaryExpressionOp::Minus,
                BinaryExpressionOp::Multiply,
                BinaryExpressionOp::Divide,
            ];

            for op in ops {
                let expr = Expression::binary(op, col!("a"), lit(10));
                assert_roundtrip(&expr);
            }
        }

        #[test]
        fn test_variadic_expression_roundtrip() {
            let expr = Expression::coalesce([col!("a"), col!("b"), lit("default")]);
            assert_roundtrip(&expr);
        }

        #[rstest::rstest]
        #[case::column(Expression::cast(col!("part"), DataType::DATE))]
        #[case::literal(Expression::cast(lit("2025-01-01"), DataType::DATE))]
        #[case::nested(Expression::cast(
            Expression::cast(col!("part"), DataType::STRING),
            DataType::INTEGER,
        ))]
        fn test_cast_expression_roundtrip(#[case] expr: Expression) {
            assert_roundtrip(&expr);
        }

        #[rstest::rstest]
        #[case::array_single(Expression::array([lit(7i32)]))]
        #[case::array_mixed(Expression::array([
            col!("a"),
            col!("b"),
            lit(42i64),
        ]))]
        fn test_array_expression_roundtrip(#[case] expr: Expression) {
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_nested_arithmetic_expression_roundtrip() {
            // (a + b) * (c - d) / 2
            let left = Expression::binary(BinaryExpressionOp::Plus, col!("a"), col!("b"));
            let right = Expression::binary(BinaryExpressionOp::Minus, col!("c"), col!("d"));
            let mul = Expression::binary(BinaryExpressionOp::Multiply, left, right);
            let expr = Expression::binary(BinaryExpressionOp::Divide, mul, lit(2));
            assert_roundtrip(&expr);
        }

        // ==================== Expression::Struct/StructPatch/Other Tests ====================

        #[test]
        fn test_struct_expression_roundtrip() {
            let expr = Expression::struct_from([
                Arc::new(col!("x")),
                Arc::new(lit(42)),
                Arc::new(lit("hello")),
            ]);
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_transform_expressions_roundtrip() {
            let cases: Vec<Expression> = vec![
                // Identity transform
                Expression::struct_patch(ExpressionStructPatchBuilder::new()).unwrap(),
                // Drop field
                Expression::struct_patch(ExpressionStructPatchBuilder::new().drop("old_column"))
                    .unwrap(),
                // Replace field
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new().replace("original", lit(0)),
                )
                .unwrap(),
                // Insert fields
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new()
                        .insert_after("after_col", col!("new_col"))
                        .prepend(lit("prepended"))
                        .append(lit("appended")),
                )
                .unwrap(),
                // Nested transform
                Expression::struct_patch(
                    ExpressionStructPatchBuilder::new_nested(["parent", "child"]).drop("to_drop"),
                )
                .unwrap(),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        #[test]
        fn test_expression_wrapping_predicate_roundtrip() {
            let pred = Predicate::eq(col!("x"), lit(10));
            let expr = Expression::from_pred(pred);
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_expression_unknown_roundtrip() {
            let expr = Expression::unknown("some_unknown_function()");
            assert_roundtrip(&expr);
        }

        #[test]
        fn test_map_to_struct_expression_roundtrip() {
            let cases: Vec<Expression> = vec![
                Expression::map_to_struct(col!("pv")),
                Expression::map_to_struct(lit("ignored")),
            ];

            for expr in &cases {
                assert_roundtrip(expr);
            }
        }

        // ==================== Predicate Tests ====================

        #[test]
        fn test_predicate_basics_roundtrip() {
            let cases: Vec<Predicate> = vec![
                // Boolean expression
                Predicate::from_expr(col!("is_active")),
                // Literals
                Predicate::TRUE,
                Predicate::FALSE,
                // NOT
                Predicate::not(Predicate::from_expr(col!("x"))),
                // Nested NOT
                Predicate::not(Predicate::not(Predicate::gt(col!("x"), lit(5)))),
                // Unknown
                Predicate::unknown("some_unknown_predicate()"),
                // Unary predicates
                Predicate::is_null(col!("nullable_col")),
                Predicate::is_not_null(col!("nullable_col")),
            ];

            for pred in &cases {
                assert_roundtrip(pred);
            }
        }

        #[test]
        fn test_predicate_null_literal_roundtrip() {
            assert_roundtrip(&Predicate::NULL);
        }

        #[test]
        fn test_predicate_comparisons_roundtrip() {
            let cases: Vec<Predicate> = vec![
                Predicate::eq(col!("x"), lit(42)),
                Predicate::ne(col!("status"), lit("active")),
                Predicate::lt(col!("age"), lit(18)),
                Predicate::le(col!("price"), lit(100)),
                Predicate::gt(col!("score"), lit(90)),
                Predicate::ge(col!("quantity"), lit(1)),
                Predicate::distinct(col!("a"), col!("b")),
            ];

            for pred in &cases {
                assert_roundtrip(pred);
            }
        }

        #[test]
        fn test_predicate_in_roundtrip() {
            let array_data = ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                vec![Scalar::Integer(1), Scalar::Integer(2), Scalar::Integer(3)],
            )
            .unwrap();
            let pred = Predicate::binary(
                BinaryPredicateOp::In,
                col!("x"),
                Expression::Literal(Scalar::Array(array_data)),
            );
            assert_roundtrip(&pred);
        }

        #[test]
        fn test_predicate_junctions_roundtrip() {
            let cases: Vec<Predicate> = vec![
                // Simple AND
                Predicate::and(
                    Predicate::gt(col!("x"), lit(0)),
                    Predicate::lt(col!("x"), lit(100)),
                ),
                // Simple OR
                Predicate::or(
                    Predicate::eq(col!("status"), lit("active")),
                    Predicate::eq(col!("status"), lit("pending")),
                ),
                // Multiple AND
                Predicate::and_from([
                    Predicate::gt(col!("x"), lit(0)),
                    Predicate::lt(col!("x"), lit(100)),
                    Predicate::is_not_null(col!("x")),
                ]),
                // Multiple OR
                Predicate::or_from([
                    Predicate::eq(col!("type"), lit("A")),
                    Predicate::eq(col!("type"), lit("B")),
                    Predicate::eq(col!("type"), lit("C")),
                ]),
                // Nested: (a > 0 AND b < 100) OR (c = 'special')
                Predicate::or(
                    Predicate::and(
                        Predicate::gt(col!("a"), lit(0)),
                        Predicate::lt(col!("b"), lit(100)),
                    ),
                    Predicate::eq(col!("c"), lit("special")),
                ),
            ];

            for pred in &cases {
                assert_roundtrip(pred);
            }
        }

        // ==================== Complex Nested Structures ====================

        #[test]
        fn test_deeply_nested_structures_roundtrip() {
            // COALESCE(a + b, c * d, 0) > 100
            let add = Expression::binary(BinaryExpressionOp::Plus, col!("a"), col!("b"));
            let mul = Expression::binary(BinaryExpressionOp::Multiply, col!("c"), col!("d"));
            let coalesce = Expression::coalesce([add, mul, lit(0)]);
            let pred = Predicate::gt(coalesce, lit(100));
            assert_roundtrip(&pred);

            // Expression wrapping a predicate that references expressions
            let inner_pred = Predicate::and(
                Predicate::eq(col!("x"), lit(1)),
                Predicate::gt(
                    Expression::binary(BinaryExpressionOp::Plus, col!("y"), col!("z")),
                    lit(10),
                ),
            );
            let expr = Expression::from_pred(inner_pred);
            assert_roundtrip(&expr);
        }

        // ==================== Opaque Variant Failure Tests ====================

        #[test]
        fn test_opaque_expression_serialize_fails() {
            use crate::expressions::{OpaqueExpressionOp, ScalarExpressionEvaluator};
            use crate::DeltaResult;

            #[derive(Debug, PartialEq)]
            struct TestOpaqueExprOp;

            impl OpaqueExpressionOp for TestOpaqueExprOp {
                fn name(&self) -> &str {
                    "test_opaque"
                }
                fn eval_expr_scalar(
                    &self,
                    _eval_expr: &ScalarExpressionEvaluator<'_>,
                    _exprs: &[Expression],
                ) -> DeltaResult<Scalar> {
                    Ok(Scalar::Integer(0))
                }
            }

            let expr = Expression::opaque(TestOpaqueExprOp, [lit(1)]);
            let result = serde_json::to_string(&expr);
            assert_result_error_with_message(result, "Cannot serialize an Opaque Expression");
        }

        #[test]
        fn test_opaque_predicate_serialize_fails() {
            use crate::expressions::{OpaquePredicateOp, ScalarExpressionEvaluator};
            use crate::kernel_predicates::{
                DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
                IndirectDataSkippingPredicateEvaluator,
            };
            use crate::DeltaResult;

            #[derive(Debug, PartialEq)]
            struct TestOpaquePredOp;

            impl OpaquePredicateOp for TestOpaquePredOp {
                fn name(&self) -> &str {
                    "test_opaque_pred"
                }
                fn eval_pred_scalar(
                    &self,
                    _eval_expr: &ScalarExpressionEvaluator<'_>,
                    _eval_pred: &DirectPredicateEvaluator<'_>,
                    _exprs: &[Expression],
                    _inverted: bool,
                ) -> DeltaResult<Option<bool>> {
                    Ok(Some(true))
                }
                fn eval_as_data_skipping_predicate(
                    &self,
                    _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
                    _exprs: &[Expression],
                    _inverted: bool,
                ) -> Option<bool> {
                    Some(true)
                }
                fn as_data_skipping_predicate(
                    &self,
                    _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
                    _exprs: &[Expression],
                    _inverted: bool,
                ) -> Option<Predicate> {
                    None
                }
            }

            let pred = Predicate::opaque(TestOpaquePredOp, [lit(1)]);
            let result = serde_json::to_string(&pred);
            assert_result_error_with_message(result, "Cannot serialize an Opaque Predicate");
        }
    }

    #[test]
    fn single_element_and_from_returns_unwrapped_predicate() {
        let inner = Pred::gt(col!("x"), lit(0));
        let result = Pred::and_from([inner.clone()]);
        assert_eq!(result, inner);
    }

    #[test]
    fn single_element_or_from_returns_unwrapped_predicate() {
        let inner = Pred::gt(col!("x"), lit(0));
        let result = Pred::or_from([inner.clone()]);
        assert_eq!(result, inner);
    }

    #[test]
    fn multi_element_and_from_returns_junction() {
        let p1 = Pred::gt(col!("x"), lit(0));
        let p2 = Pred::lt(col!("x"), lit(100));
        let result = Pred::and_from([p1.clone(), p2.clone()]);
        assert!(matches!(result, Pred::Junction(ref j) if j.preds.len() == 2));
        assert_eq!(result, Pred::and(p1, p2));
    }

    #[test]
    fn empty_and_from_returns_identity_literal() {
        let result = Pred::and_from(std::iter::empty());
        assert_eq!(result, Pred::TRUE);
    }

    #[test]
    fn empty_or_from_returns_identity_literal() {
        let result = Pred::or_from(std::iter::empty());
        assert_eq!(result, Pred::FALSE);
    }
}
