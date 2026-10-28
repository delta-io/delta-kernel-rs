use delta_kernel_derive::pub_macro;

/// Builds an [`Expression`](crate::expressions::Expression) from constructors and comparisons.
///
/// Numeric, boolean, and string literals can be written directly. Use `col("name")` or
/// `path("name")` for column references. Parentheses can be used for grouping.
/// Prefer infix comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`). The equivalent constructor forms
/// (`lt`, `le`, `gt`, `ge`, `eq`, `ne`) are also supported.
///
/// Also supports `lit(value)`, typed NULLs such as `null(LONG)`, `col("a.b")`, explicit column
/// segments such as `path("a.b", "c")`, `coalesce`, `and`, `or`, `not`, `is_null`, `is_not_null`,
/// and `distinct`. Every form returns an `Expression`, including comparisons and predicate calls.
///
/// Literals retain their Rust type: `1` is INTEGER and `1_i64` is LONG.
/// `null` takes a [`DataType`](crate::schema::DataType) associated constant name, such as `LONG`.
/// `col` takes one string literal and follows [`col!`](crate::expressions::col) path rules.
/// `col("a.b")` addresses nested fields. `path("a.b")` addresses one field named `a.b`, and
/// `path("a.b", "c")` addresses its nested field `c`. `path` also accepts computed segment names.
/// Segments can mix types implementing `Into<String>`, such as `&str` and `String`.
/// `lit` and `path` arguments accept Rust expressions and are evaluated once, in order.
///
/// Expression construction delegates to the existing builders without resolving types or
/// evaluating rows. Other Rust expressions must appear inside `lit` or `path` arguments.
/// AND/OR preserve [`Predicate::junction`](crate::expressions::Predicate::junction)
/// normalization: empty AND is TRUE, empty OR is FALSE, and single-input junctions unwrap.
/// Invalid call shapes, unknown builders, unsupported operators, and unsupported literal types
/// fail to compile. Chained comparisons require separate comparisons inside `and`.
///
/// ```
/// use delta_kernel::expressions::{expr, lit, Expression, Predicate};
///
/// assert_eq!(
///     expr!(and(
///         col("status") == "active",
///         (coalesce(col("size"), 0_i64) <= 100_i64),
///     )),
///     Expression::from_pred(Predicate::and(
///         Expression::column(["status"]).eq(lit("active")),
///         Expression::coalesce([Expression::column(["size"]), lit(0_i64)]).le(lit(100_i64)),
///     )),
/// );
/// ```
///
/// ```
/// use delta_kernel::expressions::{col, expr, lit, Expression, Predicate};
///
/// assert_eq!(
///     expr!(and(
///         is_not_null(col("path")),
///         le(coalesce(col("size"), lit(0_i64)), lit(100_i64)),
///     )),
///     Expression::from_pred(Predicate::and(
///         col!("path").is_not_null(),
///         Expression::coalesce([col!("size"), lit(0_i64)]).le(lit(100_i64)),
///     )),
/// );
/// ```
///
/// Macro and crate imports can be aliased:
///
/// ```
/// use delta_kernel as kernel;
/// use kernel::expressions::{expr as kernel_expr, null_lit, Expression};
/// use kernel::schema::DataType;
///
/// assert_eq!(
///     kernel_expr!(coalesce(path("a.b"), null(LONG))),
///     Expression::coalesce([Expression::column(["a.b"]), null_lit(DataType::LONG)]),
/// );
/// assert_eq!(
///     kernel_expr!(path("a.b") == null(LONG)),
///     Expression::from_pred(Expression::column(["a.b"]).eq(null_lit(DataType::LONG))),
/// );
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(a + b);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(not(col("a"), col("b"), col("c")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(unknown_predicate(col("a")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::{expr, Expression, Predicate};
/// trait CustomPredicate {
///     fn custom_predicate(self) -> Predicate;
/// }
/// impl CustomPredicate for Expression {
///     fn custom_predicate(self) -> Predicate { Predicate::TRUE }
/// }
/// let _ = expr!(custom_predicate(col("a")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(not());
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(not(col("a"), col("b")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::{expr, Expression, Predicate};
/// trait BinaryNot {
///     fn not(self, other: Expression) -> Predicate;
/// }
/// impl BinaryNot for Expression {
///     fn not(self, _other: Expression) -> Predicate { Predicate::TRUE }
/// }
/// let _ = expr!(not(col("a"), col("b")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(gt(col("a")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(is_null(col("a"), col("b")));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(path());
/// ```
///
/// ```compile_fail,E0277
/// use delta_kernel::expressions::expr;
/// let _ = expr!(path("outer", 1));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(col("a", "b"));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// const COLUMN: &str = "a";
/// let _ = expr!(col(COLUMN));
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(null(decimal(10, 2)));
/// ```
///
/// ```compile_fail,E0277
/// use delta_kernel::expressions::expr;
/// let _ = expr!(lit(1_u64));
/// ```
///
/// ```compile_fail,E0277
/// use delta_kernel::expressions::expr;
/// let _ = expr!(col("size") > 1_u64);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(col("size") + 1);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(col("size") = 1);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(0 < col("size") < 100);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let size = 1_i64;
/// let _ = expr!(col("size") >= size);
/// ```
///
/// ```compile_fail
/// use delta_kernel::expressions::expr;
/// let _ = expr!(col("size") > unknown_predicate(col("other")));
/// ```
#[pub_macro]
macro_rules! expr {
    ($($expression:tt)*) => {
        $crate::delta_kernel_derive::expr!($crate; $($expression)*)
    };
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::expressions::{
        BinaryPredicateOp, Expression, Predicate, Scalar, UnaryPredicateOp, col, expr, lit,
        null_lit,
    };
    use crate::schema::DataType;

    #[test]
    fn nested_predicate_preserves_boolean_structure_and_coalesce_order() {
        let actual = expr!(and(
            is_not_null(col("path")),
            not(or(
                distinct(col("deleted"), lit(false)),
                le(
                    coalesce(col("size"), coalesce(col("fallback_size"), lit(0_i64))),
                    lit(15_i64),
                ),
            )),
        ));
        let expected = Expression::from_pred(Predicate::and(
            col!("path").is_not_null(),
            Predicate::not(Predicate::or(
                col!("deleted").distinct(lit(false)),
                Expression::coalesce([
                    col!("size"),
                    Expression::coalesce([col!("fallback_size"), lit(0_i64)]),
                ])
                .le(lit(15_i64)),
            )),
        ));
        assert_eq!(actual, expected);
    }

    #[test]
    fn infix_comparisons_mix_with_builders_and_grouping() {
        let actual = expr!(and(
            is_not_null(col("path")),
            not(or(
                distinct(col("deleted"), false),
                ((coalesce(col("size"), coalesce(col("fallback_size"), 0_i64))) <= (15_i64)),
            )),
            eq((col("nested.status") == "active"), true),
        ));
        let expected = Expression::from_pred(Predicate::and_from([
            col!("path").is_not_null(),
            Predicate::not(Predicate::or(
                col!("deleted").distinct(lit(false)),
                Expression::coalesce([
                    col!("size"),
                    Expression::coalesce([col!("fallback_size"), lit(0_i64)]),
                ])
                .le(lit(15_i64)),
            )),
            Expression::from_pred(col!("nested.status").eq(lit("active"))).eq(lit(true)),
        ]));
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::lt(expr!(7_i64 < col("right")), BinaryPredicateOp::LessThan, false)]
    #[case::gt(expr!(7_i64 > col("right")), BinaryPredicateOp::GreaterThan, false)]
    #[case::eq(expr!(7_i64 == col("right")), BinaryPredicateOp::Equal, false)]
    #[case::le(expr!(7_i64 <= col("right")), BinaryPredicateOp::GreaterThan, true)]
    #[case::ge(expr!(7_i64 >= col("right")), BinaryPredicateOp::LessThan, true)]
    #[case::ne(expr!(7_i64 != col("right")), BinaryPredicateOp::Equal, true)]
    fn comparisons_preserve_literal_left_operands(
        #[case] actual: Expression,
        #[case] op: BinaryPredicateOp,
        #[case] negated: bool,
    ) {
        let expected = Predicate::binary(op, lit(7_i64), col!("right"));
        let expected = if negated {
            Predicate::not(expected)
        } else {
            expected
        };
        assert_eq!(actual, Expression::from_pred(expected));
    }

    #[rstest]
    #[case::columns(
        expr!(path("a.b") == path("c.d")),
        Expression::column(["a.b"]), Expression::column(["c.d"]),
    )]
    #[case::nested_columns(expr!(col("a.b") == col("c.d")), col!("a.b"), col!("c.d"))]
    #[case::string_values(expr!("a.b" == "c.d"), lit("a.b"), lit("c.d"))]
    #[case::wrapped_string_values(expr!(lit("a.b") == lit("c.d")), lit("a.b"), lit("c.d"))]
    #[case::column_and_string(expr!(path("a.b") == "c.d"), Expression::column(["a.b"]), lit("c.d"))]
    #[case::string_and_column(expr!("a.b" == path("c.d")), lit("a.b"), Expression::column(["c.d"]))]
    fn explicit_columns_and_string_values_remain_distinct(
        #[case] actual: Expression,
        #[case] left: Expression,
        #[case] right: Expression,
    ) {
        assert_eq!(
            actual,
            Expression::from_pred(Predicate::binary(BinaryPredicateOp::Equal, left, right)),
        );
    }

    #[rstest]
    #[case::coalesce_empty(expr!(coalesce()), Expression::coalesce(Vec::<Expression>::new()))]
    #[case::coalesce_single(expr!(coalesce(col("a"))), Expression::coalesce([col!("a")]))]
    #[case::coalesce_three(
        expr!(coalesce(col("a"), col("b"), col("c"))),
        Expression::coalesce([col!("a"), col!("b"), col!("c")]),
    )]
    #[case::and_empty(expr!(and()), Expression::from_pred(Predicate::TRUE))]
    #[case::and_single(expr!(and(col("a"))), col!("a"))]
    #[case::and_three(
        expr!(and(col("a"), col("b"), col("c"))),
        Expression::from_pred(Predicate::and_from([
            Predicate::column(["a"]), Predicate::column(["b"]), Predicate::column(["c"]),
        ])),
    )]
    #[case::or_empty(expr!(or()), Expression::from_pred(Predicate::FALSE))]
    #[case::or_single(expr!(or(col("a"))), col!("a"))]
    #[case::or_three(
        expr!(or(col("a"), col("b"), col("c"))),
        Expression::from_pred(Predicate::or_from([
            Predicate::column(["a"]), Predicate::column(["b"]), Predicate::column(["c"]),
        ])),
    )]
    #[case::boolean_expressions(
        expr!(and(col("a"), not(col("b")), gt(col("size"), lit(0_i64)))),
        Expression::from_pred(Predicate::and_from([
            Predicate::column(["a"]),
            Predicate::not(Predicate::column(["b"])),
            col!("size").gt(lit(0_i64)),
        ])),
    )]
    #[case::bare_coalesce(
        expr!(coalesce(path("a.b"), -1_i64,)),
        Expression::coalesce([Expression::column(["a.b"]), lit(-1_i64)]),
    )]
    #[case::string_coalesce(
        expr!(coalesce(col("status"), "active")),
        Expression::coalesce([col!("status"), lit("active")]),
    )]
    #[case::bare_and(
        expr!(and(true, col("a"),)),
        Expression::from_pred(Predicate::and(Predicate::TRUE, Predicate::column(["a"]))),
    )]
    #[case::bare_or(
        expr!(or(false, col("a"),)),
        Expression::from_pred(Predicate::or(Predicate::FALSE, Predicate::column(["a"]))),
    )]
    fn variadic_calls_preserve_operands_and_junction_identities(
        #[case] actual: Expression,
        #[case] expected: Expression,
    ) {
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case::nested(expr!(col("a.b")), &["a", "b"])]
    #[case::literal_dot(expr!(path("a.b")), &["a.b"])]
    #[case::literal_space(expr!(path("column with spaces")), &["column with spaces"])]
    #[case::literal_case(expr!(path("MiXeD")), &["MiXeD"])]
    #[case::literal_escape(expr!(path("a\"b\\c")), &["a\"b\\c"])]
    #[case::raw_literal(expr!(path(r#"a"b\c"#)), &["a\"b\\c"])]
    #[case::explicit_segments(expr!(path("outer", "inner.with.dot", "leaf")),
        &["outer", "inner.with.dot", "leaf"])]
    fn column_paths_distinguish_literal_dots_from_nested_fields(
        #[case] actual: Expression,
        #[case] segments: &[&str],
    ) {
        assert_eq!(actual, Expression::column(segments.iter().copied()));
    }

    #[rstest]
    #[case::lt(expr!(lt(col("left"), lit(7_i64))), BinaryPredicateOp::LessThan, false)]
    #[case::gt(expr!(gt(col("left"), lit(7_i64))), BinaryPredicateOp::GreaterThan, false)]
    #[case::eq(expr!(eq(col("left"), lit(7_i64))), BinaryPredicateOp::Equal, false)]
    #[case::distinct(expr!(distinct(col("left"), lit(7_i64))), BinaryPredicateOp::Distinct, false)]
    #[case::le(expr!(le(col("left"), lit(7_i64))), BinaryPredicateOp::GreaterThan, true)]
    #[case::ge(expr!(ge(col("left"), lit(7_i64))), BinaryPredicateOp::LessThan, true)]
    #[case::ne(expr!(ne(col("left"), lit(7_i64))), BinaryPredicateOp::Equal, true)]
    #[case::infix_lt(expr!(col("left") < 7_i64), BinaryPredicateOp::LessThan, false)]
    #[case::infix_gt(expr!(col("left") > 7_i64), BinaryPredicateOp::GreaterThan, false)]
    #[case::infix_eq(expr!(col("left") == 7_i64), BinaryPredicateOp::Equal, false)]
    #[case::infix_le(expr!(col("left") <= 7_i64), BinaryPredicateOp::GreaterThan, true)]
    #[case::infix_ge(expr!(col("left") >= 7_i64), BinaryPredicateOp::LessThan, true)]
    #[case::infix_ne(expr!(col("left") != 7_i64), BinaryPredicateOp::Equal, true)]
    fn comparisons_preserve_operators_operand_order_and_negation(
        #[case] actual: Expression,
        #[case] op: BinaryPredicateOp,
        #[case] negated: bool,
    ) {
        let expected = Predicate::binary(op, col!("left"), lit(7_i64));
        let expected = if negated {
            Predicate::not(expected)
        } else {
            expected
        };
        assert_eq!(actual, Expression::from_pred(expected));
    }

    #[rstest]
    #[case::is_null(expr!(is_null(col("a"))), false)]
    #[case::is_not_null(expr!(is_not_null(col("a"))), true)]
    fn null_predicates_preserve_is_null_and_negation(
        #[case] actual: Expression,
        #[case] negated: bool,
    ) {
        let expected = Predicate::unary(UnaryPredicateOp::IsNull, col!("a"));
        let expected = if negated {
            Predicate::not(expected)
        } else {
            expected
        };
        assert_eq!(actual, Expression::from_pred(expected));
    }

    #[rstest]
    #[case::unsuffixed_integer(expr!(lit(1)), Scalar::Integer(1))]
    #[case::long(expr!(lit(1_i64)), Scalar::Long(1))]
    #[case::byte(expr!(lit(i8::MIN)), Scalar::Byte(i8::MIN))]
    #[case::short(expr!(lit(i16::MIN)), Scalar::Short(i16::MIN))]
    #[case::integer_min(expr!(lit(i32::MIN)), Scalar::Integer(i32::MIN))]
    #[case::long_max(expr!(lit(i64::MAX)), Scalar::Long(i64::MAX))]
    #[case::true_literal(expr!(lit(true)), Scalar::Boolean(true))]
    #[case::false_literal(expr!(lit(false)), Scalar::Boolean(false))]
    #[case::float(expr!(lit(1.5_f32)), Scalar::Float(1.5))]
    #[case::double(expr!(lit(-2.5_f64)), Scalar::Double(-2.5))]
    #[case::empty_string(expr!(lit("")), Scalar::String(String::new()))]
    #[case::owned_string(expr!(lit(String::from("a\0b\n"))), Scalar::String("a\0b\n".into()))]
    #[case::binary(expr!(lit(vec![0_u8, 255])), Scalar::Binary(vec![0, 255]))]
    #[case::scalar(expr!(lit(Scalar::Date(-1))), Scalar::Date(-1))]
    #[case::bare_integer(expr!(1), Scalar::Integer(1))]
    #[case::bare_negative(expr!(-7), Scalar::Integer(-7))]
    #[case::bare_hex(expr!(0xff), Scalar::Integer(255))]
    #[case::bare_long(expr!(1_i64), Scalar::Long(1))]
    #[case::bare_byte_min(expr!(-128_i8), Scalar::Byte(i8::MIN))]
    #[case::bare_short_min(expr!(-32768_i16), Scalar::Short(i16::MIN))]
    #[case::bare_integer_min(expr!(-2147483648_i32), Scalar::Integer(i32::MIN))]
    #[case::bare_long_min(expr!(-9223372036854775808_i64), Scalar::Long(i64::MIN))]
    #[case::bare_true(expr!(true), Scalar::Boolean(true))]
    #[case::bare_false(expr!(false), Scalar::Boolean(false))]
    #[case::bare_float(expr!(1.5_f32), Scalar::Float(1.5))]
    #[case::bare_double(expr!(-2.5), Scalar::Double(-2.5))]
    #[case::bare_suffixed_double(expr!(-2.5_f64), Scalar::Double(-2.5))]
    #[case::bare_string(expr!("active"), Scalar::String("active".into()))]
    #[case::bare_empty_string(expr!(""), Scalar::String(String::new()))]
    #[case::bare_dotted_string(expr!("a.b"), Scalar::String("a.b".into()))]
    #[case::bare_string_with_spaces(
        expr!("column with spaces"), Scalar::String("column with spaces".into()),
    )]
    #[case::bare_escaped_string(expr!("a\0b\n\"c\\d"), Scalar::String("a\0b\n\"c\\d".into()))]
    #[case::bare_raw_string(expr!(r#"a"b\c"#), Scalar::String("a\"b\\c".into()))]
    #[case::bare_unicode_string(expr!("café"), Scalar::String("café".into()))]
    fn literals_preserve_exact_scalar_variants(
        #[case] actual: Expression,
        #[case] expected: Scalar,
    ) {
        assert_eq!(actual, Expression::Literal(expected));
    }

    #[rstest]
    #[case::integer(expr!(null(INTEGER)), DataType::INTEGER)]
    #[case::long(expr!(null(LONG)), DataType::LONG)]
    #[case::boolean(expr!(null(BOOLEAN)), DataType::BOOLEAN)]
    #[case::string(expr!(null(STRING)), DataType::STRING)]
    #[case::double(expr!(null(DOUBLE)), DataType::DOUBLE)]
    fn nulls_preserve_data_types(#[case] actual: Expression, #[case] data_type: DataType) {
        assert_eq!(actual, null_lit(data_type));
    }

    #[test]
    fn literal_arguments_are_evaluated_once_in_order() {
        let mut calls = 0_i64;
        let mut next = || {
            calls += 1;
            calls
        };
        let actual = expr!(coalesce(lit(next()), lit(next())));
        assert_eq!(calls, 2);
        assert_eq!(actual, Expression::coalesce([lit(1_i64), lit(2_i64)]));
    }

    #[test]
    fn path_segments_accept_mixed_string_types_and_are_evaluated_once_in_order() {
        let mut calls = 0;
        let mut next = || {
            calls += 1;
            format!("field.{calls}")
        };
        let borrowed = String::from("borrowed");
        let leaf = String::from("leaf");
        let actual = expr!(path("outer", next(), &borrowed, next(), leaf,));
        assert_eq!(calls, 2);
        assert_eq!(
            actual,
            Expression::column(["outer", "field.1", "borrowed", "field.2", "leaf"]),
        );
    }

    #[test]
    fn binary_predicate_arguments_are_evaluated_once_in_order() {
        let mut calls = 0_i64;
        let mut next = || {
            calls += 1;
            calls
        };
        let actual = expr!(gt(lit(next()), lit(next()),));
        assert_eq!(calls, 2);
        assert_eq!(actual, Expression::from_pred(lit(1_i64).gt(lit(2_i64))));
    }

    #[test]
    fn mixed_infix_and_builder_arguments_are_evaluated_once_in_order() {
        let mut calls = 0_i64;
        let mut next = || {
            calls += 1;
            calls
        };
        let actual = expr!(and(
            lit(next()) < lit(next()),
            le(lit(next()), lit(next())),
            coalesce(lit(next()), lit(next())) >= lit(next()),
        ));
        assert_eq!(calls, 7);
        assert_eq!(
            actual,
            Expression::from_pred(Predicate::and_from([
                lit(1_i64).lt(lit(2_i64)),
                lit(3_i64).le(lit(4_i64)),
                Expression::coalesce([lit(5_i64), lit(6_i64)]).ge(lit(7_i64)),
            ])),
        );
    }

    #[test]
    fn infix_path_segments_are_evaluated_before_the_right_operand() {
        let mut calls = 0;
        let mut next = || {
            calls += 1;
            format!("field.{calls}")
        };
        let actual = expr!(path(next(), next(),) == lit(next()));
        assert_eq!(calls, 3);
        assert_eq!(
            actual,
            Expression::from_pred(Expression::column(["field.1", "field.2"]).eq(lit("field.3"))),
        );
    }

    #[test]
    fn forwarded_rust_expression_fragments_preserve_bare_literals_and_comparisons() {
        macro_rules! forward {
            ($expression:expr) => {
                expr!($expression)
            };
        }
        assert_eq!(
            forward!(col("size") >= 1_i64),
            Expression::from_pred(col!("size").ge(lit(1_i64)))
        );
        assert_eq!(forward!(-7), lit(-7));
        assert_eq!(forward!("active"), lit("active"));
    }

    #[test]
    fn forwarded_literal_and_identifier_fragments_preserve_constructor_syntax() {
        macro_rules! forward_col {
            ($column:literal) => {
                expr!(col($column))
            };
        }
        macro_rules! forward_null {
            ($data_type:ident) => {
                expr!(null($data_type))
            };
        }
        macro_rules! forward_negative {
            ($number:literal) => {
                expr!(-$number)
            };
        }
        assert_eq!(forward_col!("a.b"), col!("a.b"));
        assert_eq!(forward_null!(LONG), null_lit(DataType::LONG));
        assert_eq!(forward_negative!(7_i64), lit(-7_i64));
        assert_eq!(
            expr!(r#gt(col("a"), 7_i64)),
            Expression::from_pred(col!("a").gt(lit(7_i64)))
        );
    }

    mod macro_hygiene {
        // Simulate a caller defining types named Vec, Expression, and Predicate.
        // expr! must still use std::vec::Vec and Kernel's Expression and Predicate.
        // Bare type names in its expansion would resolve to these () aliases and fail to compile.
        type Vec = ();
        type Expression = ();
        type Predicate = ();

        #[test]
        fn expressions_resolve_types_independently_of_caller_names() {
            // Reference the aliases to avoid dead-code warnings.
            let _caller_types: (Vec, Expression, Predicate) = ((), (), ());
            assert_eq!(
                crate::expressions::expr!(coalesce(lit(false), null(BOOLEAN))),
                crate::expressions::Expression::coalesce([
                    crate::expressions::lit(false),
                    crate::expressions::null_lit(crate::schema::DataType::BOOLEAN),
                ]),
            );
            assert_eq!(
                crate::expressions::expr!(coalesce(path("a.b"), 0_i64) < 1_i64),
                crate::expressions::Expression::from_pred(
                    crate::expressions::Expression::coalesce([
                        crate::expressions::Expression::column(["a.b"]),
                        crate::expressions::lit(0_i64),
                    ])
                    .lt(crate::expressions::lit(1_i64)),
                ),
            );
        }
    }
}
