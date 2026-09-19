# Standard SQL expression semantics

This page defines the SQL semantics for Kernel's standard expression operators. It fixes choices
that vary across SQL implementations, including decimal precision, floating-point comparisons,
temporal conversions, and empty `IN` sets. Evaluation settings do not change these rules.

An evaluator may reject a valid expression as unsupported. If it evaluates one, the result type,
value, null behavior, and errors must follow this contract. An expression outside the contract is
not portable, even when an evaluator accepts it.

Expression nodes with specialized behavior document their contracts on their Rust types.

## Analysis and common types

The expression tree is unresolved. Before evaluation, an implementation resolves column
references, validates each operator's operands, finds required common types, and inserts casts.
Failure to find a permitted type is an analysis error.

Kernel does not implicitly parse `STRING` as a number or Boolean. For example, `long_col + '1'`,
`long_col = '1'`, and `COALESCE(long_col, '0')` are outside the contract. An evaluator may accept
such expressions as an extension, but their behavior is not portable.

`VOID` is the type of an untyped null. `VOID` and `T` have common type `T`; inputs that are all
`VOID` retain that type. Identical scalar types have themselves as their common type. The following
rules handle differing types. Arithmetic whose operands are all `VOID` is invalid because no
numeric type can be inferred.

### Numeric common types

Among non-decimal numeric types, the precedence order is:

```text
TINYINT -> SMALLINT -> INT -> BIGINT -> FLOAT -> DOUBLE
```

Kernel names these types `BYTE`, `SHORT`, `INTEGER`, `LONG`, `FLOAT`, and `DOUBLE` in its Rust API.
Normally, the type farther to the right is the common type. An integral type combined with `FLOAT`
resolves to `DOUBLE`. Decimal operands follow separate rules:

- A decimal combined with `FLOAT` or `DOUBLE` resolves to `DOUBLE`.
- A decimal combined with an integral type stays decimal. The integral operand first becomes
  `DECIMAL(3,0)`, `DECIMAL(5,0)`, `DECIMAL(10,0)`, or `DECIMAL(20,0)`, respectively.

For common-type resolution outside arithmetic, two decimals use:

```text
scale          = max(s1, s2)
integer_digits = max(p1 - s1, p2 - s2)
precision      = integer_digits + scale
```

If `precision` exceeds 38, reduce `scale` by `precision - 38`, without taking it below zero, and
use precision 38. This retains the available integer digits before fractional digits. Any scale
reduction in an implicit decimal conversion rounds half up, with a midpoint rounded away from
zero. A value whose integer part does not fit raises an arithmetic-overflow error.

For decimal arithmetic and numeric comparisons, an integral literal combined with a decimal uses
only the digits needed by its value, with one digit for zero. Any other integral expression uses
the full decimal precision listed above. Other common-type operations, including `COALESCE`, use
the full precision of the integral type for both literals and non-literal expressions.

Examples:

```sql
COALESCE(int_col, bigint_col)  -- BIGINT
bigint_col + float_col         -- DOUBLE
decimal_col = int_col          -- compare as a DECIMAL common type
decimal_col + double_col       -- DOUBLE
```

### Other scalar and complex types

`DATE`, `TIMESTAMP_NTZ`, and `TIMESTAMP` widen in that order. A date becomes midnight. When a date
or timezone-free timestamp widens to `TIMESTAMP`, Kernel interprets it in UTC. A session time zone
must not alter the conversion. Converting a date to either timestamp type raises a runtime
arithmetic-overflow error when its midnight microsecond offset does not fit in an `i64`.

Strings use binary UTF-8 ordering: their encoded bytes compare lexicographically with no case
folding or Unicode normalization. Binary values compare lexicographically by unsigned byte,
`FALSE` sorts before `TRUE`, and intervals compare by their signed month or microsecond count
within the same interval family. Binary, Boolean, and interval values require an exact type match.
Other scalar types also require an exact type match.

Complex types resolve recursively:

- Arrays widen their element types. The result allows null elements when either input does.
- Maps widen keys and values separately. `VOID` may widen as a key type because it can occur only in
  an empty map. The result allows null values when either input does.
- Structs must have the same field count and case-insensitively matching field names. Corresponding
  fields widen recursively, and the result field is nullable when either input field is nullable.
  The result preserves the first input's field-name spelling.

```text
common_type(ARRAY<INT>, ARRAY<BIGINT>) -> ARRAY<BIGINT>

common_type(
  STRUCT<a: INT, b: STRING>,
  STRUCT<a: BIGINT, b: STRING>
) -> STRUCT<a: BIGINT, b: STRING>
```

## Declared `Project` output types

Operator analysis determines an expression's result before a surrounding `Project` assigns it to
a declared output field. The declared schema cannot change an operator's inference. In particular,
it cannot make integer `/` truncate or force decimal arithmetic to keep an operand's precision.

After inference, a `Project` may keep the exact type or apply one of these implicit assignments:

```text
TINYINT  -> SMALLINT, INT, BIGINT, FLOAT, DOUBLE
SMALLINT -> INT, BIGINT, FLOAT, DOUBLE
INT      -> BIGINT, DOUBLE
FLOAT    -> DOUBLE
DATE     -> TIMESTAMP_NTZ
```

Together with the `VOID` and decimal rules below, these are the only implicit `Project`
assignments. Common-type promotion does not add assignments to this list.

`VOID` may take any target type, subject to nullability at the same position. A top-level `VOID`
expression is nullable and therefore cannot be assigned to a non-null field. Recursively, a
`VOID` array element or map value may take a non-null target type when the source container says
that position cannot contain null. An empty `ARRAY<VOID>` with `containsNull = false`, an empty map
with a `VOID` key, and an empty map with a `VOID` value whose `valueContainsNull` is false may
therefore adopt concrete target types.

An integral or decimal value may be assigned to `DECIMAL(p2,s2)` when the target has at least the
source scale and enough added precision to preserve every integer digit. Integral sources use the
full decimal precision listed under numeric common types. The same rule applies recursively to
array elements, map keys and values, and corresponding struct fields. Map keys and values are
checked independently. Struct fields are aligned positionally; the target field names replace the
source names. A nullable result cannot be assigned to a non-null field. Recursively, an array that
may contain null elements cannot be assigned to one that forbids them, a map whose values may be
null cannot be assigned to one whose values may not be null, and a nullable struct field cannot be
assigned to a non-null field.

Other conversions need an explicit `Cast` expression. Narrowing, parsing a string, or rounding a
decimal at the `Project` boundary is not an implicit assignment.

The numeric assignments above preserve every source value. `DATE` to `TIMESTAMP_NTZ` is also
allowed, but it raises an arithmetic-overflow error when the date's midnight microsecond offset
does not fit in an `i64`.

## `COALESCE`

`COALESCE` requires at least one child. It casts every child to their common type and returns that
type. Its result is nullable only when every child is nullable.

Every child must analyze successfully. Runtime evaluation then proceeds from left to right and
stops at the first non-null value. An unselected child is not evaluated, so its runtime errors are
not raised.

```sql
COALESCE(CAST(NULL AS INT), CAST(7 AS BIGINT))  -- BIGINT 7
COALESCE(NULL, 7, 9)                            -- INT 7
COALESCE(2, 5 / 0)                              -- DOUBLE 2.0; no DIVIDE_BY_ZERO error
```

## `ARRAY`

`ARRAY` casts every element to their common type and returns `ARRAY<common type>`. The array itself
is never null. Its `containsNull` flag is true exactly when an element expression can return null.
`ARRAY()` is an empty, non-null `ARRAY<VOID>` with `containsNull = false`.

```sql
ARRAY(CAST(1 AS INT), CAST(2 AS BIGINT))  -- ARRAY<BIGINT>[1, 2]
ARRAY(1, NULL)                            -- ARRAY<INT>[1, NULL], containsNull = true
ARRAY()                                   -- ARRAY<VOID>[], containsNull = false
```

For an array literal that is constant across input rows, use
[`Scalar::Array`](crate::expressions::Scalar::Array). The
[`VariadicExpressionOp::Array`](crate::expressions::VariadicExpressionOp::Array) node evaluates its
elements for each input row.

## Numeric addition, subtraction, and multiplication

`+`, `-`, and `*` accept numeric operands only. When the common type is not decimal, it is also the
result type. When the common type is decimal, results use the formulas below. A decimal combined
with `FLOAT` or `DOUBLE` therefore returns `DOUBLE`, not a decimal. The result is nullable when
either operand is nullable.

Integral and decimal arithmetic is checked. A value outside the result type raises a runtime
arithmetic-overflow error instead of wrapping or becoming null. Floating-point arithmetic keeps
IEEE 754 results, including infinities and NaNs.

```sql
CAST(1 AS INT) + CAST(2 AS BIGINT)    -- BIGINT 3
CAST(3 AS BIGINT) - CAST(1 AS INT)    -- BIGINT 2
CAST(3 AS INT) * CAST(2 AS BIGINT)    -- BIGINT 6
CAST(2147483647 AS INT) + 1           -- arithmetic overflow error
CAST(1e308 AS DOUBLE) * 1e308         -- positive infinity
NULL + CAST(1 AS INT)                 -- NULL
```

### Decimal result types

For `DECIMAL(p1,s1)` and `DECIMAL(p2,s2)`, first compute an unbounded result type:

```text
Add/Subtract:
  scale     = max(s1, s2)
  precision = max(p1 - s1, p2 - s2) + scale + 1

Multiply:
  scale     = s1 + s2
  precision = p1 + p2 + 1

Divide:
  scale     = max(6, s1 + p2 + 1)
  precision = p1 - s1 + s2 + scale
```

When `precision` exceeds 38, adjust it as follows:

```text
integer_digits = precision - scale
minimum_scale  = min(scale, 6)
adjusted_scale = max(38 - integer_digits, minimum_scale)
result         = DECIMAL(38, adjusted_scale)
```

Scale reduction rounds half up. A midpoint rounds away from zero. If the integer part still does
not fit, evaluation raises an arithmetic-overflow error.

```text
DECIMAL(10,2) + DECIMAL(10,2) -> DECIMAL(11,2)
DECIMAL(10,2) - DECIMAL(10,2) -> DECIMAL(11,2)
DECIMAL(10,2) * DECIMAL(10,2) -> DECIMAL(21,4)
DECIMAL(10,2) / DECIMAL(10,2) -> DECIMAL(23,13)
```

A non-literal integral expression uses its type's full decimal precision during decimal arithmetic:
3 digits for `TINYINT`, 5 for `SMALLINT`, 10 for `INT`, and 20 for `BIGINT`. An integral literal
uses only the digits needed for its value.

```text
DECIMAL(10,2) column + INT column -> DECIMAL(13,2)
DECIMAL(10,2) column + literal 1  -> DECIMAL(11,2)
```

Kernel caps decimal precision at 38, permits the scale reduction above, preserves at least
`min(original scale, 6)` fractional digits, and gives each integral literal the smallest precision
that represents its value. Evaluation settings do not alter these choices.

## Division

`/` accepts numeric operands and always means fractional division. Inputs whose common type is not
decimal become `DOUBLE`, so integer inputs do not truncate. Inputs whose common type is decimal use
the decimal division formula. A decimal combined with `FLOAT` or `DOUBLE` therefore returns
`DOUBLE`. The result is nullable when either operand is nullable.

A divisor of positive or negative zero raises `DIVIDE_BY_ZERO` for integral, decimal, or
floating-point operands when the dividend is non-null. A null operand produces null, so `NULL / 0`
is null rather than an error. This is the deliberate exception to raw IEEE 754 division. Kernel
has no truncating `DIV` operator.

```sql
3 / 2                                            -- DOUBLE 1.5
CAST(3 AS DECIMAL(10,2)) /
  CAST(2 AS DECIMAL(10,2))                       -- DECIMAL(23,13) 1.5000000000000
CAST(1 AS DOUBLE) / CAST(0.0 AS DOUBLE)          -- DIVIDE_BY_ZERO error
CAST(1 AS DOUBLE) / CAST('-0.0' AS DOUBLE)       -- DIVIDE_BY_ZERO error
NULL / 0                                         -- NULL; no DIVIDE_BY_ZERO error
```

## Comparisons

`=`, `<`, `>`, and `IS DISTINCT FROM` cast both operands to one common orderable type. Numeric,
string, binary, Boolean, temporal, and interval values are orderable. Arrays and structs are
orderable when every nested value is orderable. Maps are not orderable, and this contract does not
define comparisons for `VARIANT` or geospatial values. `VOID` operands follow the untyped-null
rules above and require no non-null ordering.

When nested types have identical leaf types but struct field names or nested nullability differ,
comparison may align them positionally without a cast. Field names do not affect the aligned value
comparison. Ordinary widening still requires case-insensitively matching names, so differently
named structs whose corresponding leaf types differ are invalid.

```text
STRUCT<a: INT>(1) = STRUCT<b: INT>(1)       -> TRUE
STRUCT<a: INT>(1) = STRUCT<b: BIGINT>(1)    -> analysis error
```

Ordinary comparisons use three-valued logic. If either operand is null, the result is null.
`IS DISTINCT FROM` is null-safe inequality and never returns null.

```sql
CAST(1 AS INT) = CAST(1 AS BIGINT)  -- TRUE after widening to BIGINT
NULL = NULL                         -- NULL
NULL < 1                            -- NULL

1 IS DISTINCT FROM 1                -- FALSE
1 IS DISTINCT FROM 2                -- TRUE
NULL IS DISTINCT FROM NULL          -- FALSE
NULL IS DISTINCT FROM 1             -- TRUE
```

Floating-point comparisons treat all NaN payloads as one value that sorts after every non-NaN.
Positive and negative zero compare equal. These rules also apply inside arrays and structs and to
`IN`.

Arrays and structs compare lexicographically. Array elements are visited in index order and struct
fields in declared order. A nested null equals another nested null and sorts before a non-null
value. This nested-null rule does not change the top-level three-valued result: a null array or
struct operand still makes an ordinary comparison null.

```sql
CAST('NaN' AS DOUBLE) = CAST('NaN' AS DOUBLE)  -- TRUE
CAST('NaN' AS DOUBLE) > 1.0                    -- TRUE
1.0 < CAST('NaN' AS DOUBLE)                    -- TRUE
CAST('-0.0' AS DOUBLE) = CAST('0.0' AS DOUBLE)  -- TRUE
CAST('-0.0' AS DOUBLE) < CAST('0.0' AS DOUBLE)  -- FALSE
CAST('-0.0' AS DOUBLE) > CAST('0.0' AS DOUBLE)  -- FALSE
```

## `IN`

Kernel's `IN` node compares any left expression with the elements of one array-valued right
expression. The right expression is not a parenthesized SQL value list or subquery.

The left operand and the array element type resolve to one common orderable type, or use the same
positional structural alignment permitted by comparisons when their leaf types already match.
Membership uses the same equality relation as `=`, including its NaN and signed-zero behavior. A
null array returns null. For a non-empty array:

| Condition | Result |
|---|---|
| At least one element matches | `TRUE` |
| No match and the left operand is null | `NULL` |
| No match and an element is null | `NULL` |
| No match and no null is involved | `FALSE` |

In SQL, a column on the left is the common form. For example, `customer_id IN (1, 2, 3)` maps to a
Kernel `IN` node whose left expression is `customer_id` and whose right expression is
`ARRAY(1, 2, 3)`. The parenthesized SQL list is not itself a Kernel expression node.

```sql
customer_id IN (1, 2, 3)                         -- valid: left side is a column
1 IN (1, 2)                                      -- TRUE
1 IN (2, 3)                                      -- FALSE
1 IN (2, NULL)                                   -- NULL
NULL IN (1, 2)                                   -- NULL
CAST('NaN' AS DOUBLE) IN (CAST('NaN' AS DOUBLE))  -- TRUE
CAST('-0.0' AS DOUBLE) IN (CAST('0.0' AS DOUBLE))  -- TRUE
```

SQL text does not permit `IN ()`, but an `IN` subquery can return no rows. Kernel likewise returns
false for an empty array, including when the left value is null:

```text
1    IN [] -> FALSE
NULL IN [] -> FALSE
```

This fixes the result, but it does not require an evaluator to skip the left expression. `IN` does
not suppress errors from its children.

`NOT IN` applies SQL `NOT` to the membership result, so it preserves null. Data-skipping consumers
must treat a null predicate result as unknown and keep the file.

The inferred result is nullable when the left operand, array, or array element is nullable. This
remains true for an empty array even though that row's value is always false.

## Boolean predicates and nulls

`BooleanExpression`, `AND`, `OR`, and `NOT` require Boolean inputs. Kernel does not define numeric
or string truthiness. `AND`, `OR`, and `NOT` use SQL three-valued logic. `IS NULL` accepts any type
and always returns a non-null Boolean.

`BooleanExpression` keeps its input nullability, and `NOT` keeps its child's nullability. An `AND`
or `OR` result is nullable when any child is nullable, even when a decisive child determines a
non-null value for a particular row.

```sql
TRUE  AND TRUE   -- TRUE
TRUE  AND FALSE  -- FALSE
TRUE  AND NULL   -- NULL
FALSE AND NULL   -- FALSE

TRUE  OR FALSE   -- TRUE
FALSE OR FALSE   -- FALSE
TRUE  OR NULL    -- TRUE
FALSE OR NULL    -- NULL

NOT TRUE         -- FALSE
NOT FALSE        -- TRUE
NOT NULL         -- NULL

NULL IS NULL     -- TRUE
1 IS NULL        -- FALSE
```

The normalized empty junctions use their Boolean identities: `AND()` is true and `OR()` is false.

## Evaluation order and errors

Only `COALESCE` guarantees left-to-right evaluation and stops after the first non-null value.
Kernel does not promise evaluation order or error suppression for Boolean junctions, arithmetic,
comparisons, or `IN`; an evaluator may rewrite or reorder them.

Failures fall into these categories:

| Situation | Required result |
|---|---|
| Operand types have no permitted common type | Analysis error |
| An operator has the wrong number or kind of operands | Analysis error |
| The expression is valid but the evaluator cannot implement it | Explicit unsupported error |
| Checked numeric operation or temporal conversion overflows | Runtime arithmetic-overflow error |
| A non-null value is divided by positive or negative zero | Runtime `DIVIDE_BY_ZERO` error |
| A nullable operand is null | Null according to the operator rules |
| An unselected later `COALESCE` child would fail | No error; the child is not evaluated |

An evaluator must not use a legacy, permissive, or `TRY` mode for the standard operators covered
here. Such a mode must not make overflow wrap or turn an error into null.

## Sources

The common-type, decimal-arithmetic, overflow, null, and floating-point rules use public
[Apache Spark ANSI semantics], [SQL data types], and [NULL semantics] as a baseline. The Apache
Spark implementations of [ANSI type coercion], [arithmetic expressions], [predicates],
[`COALESCE`], [floating-point ordering], and the [default binary string type] cover details that
the SQL reference does not spell out.

The operator sections above define Kernel's fixed choices where SQL implementations vary.
`Project` assignment is Kernel-specific and limited to the conversions listed above.

[Apache Spark ANSI semantics]: https://spark.apache.org/docs/4.0.1/sql-ref-ansi-compliance.html
[SQL data types]: https://spark.apache.org/docs/4.0.1/sql-ref-datatypes.html
[NULL semantics]: https://spark.apache.org/docs/4.0.1/sql-ref-null-semantics.html
[ANSI type coercion]: https://github.com/apache/spark/blob/v4.0.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiTypeCoercion.scala
[arithmetic expressions]: https://github.com/apache/spark/blob/v4.0.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala
[predicates]: https://github.com/apache/spark/blob/v4.0.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/predicates.scala
[`COALESCE`]: https://github.com/apache/spark/blob/v4.0.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/nullExpressions.scala#L52-L93
[floating-point ordering]: https://github.com/apache/spark/blob/v4.0.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/SQLOrderingUtil.scala#L20-L40
[default binary string type]: https://github.com/apache/spark/blob/v4.0.1/sql/api/src/main/scala/org/apache/spark/sql/types/StringType.scala#L108-L121
