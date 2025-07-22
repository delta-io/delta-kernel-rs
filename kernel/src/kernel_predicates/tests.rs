use super::*;
use crate::expressions::{
    column_expr, column_name, column_pred, ArrayData, Expression as Expr, OpaqueExpressionOp,
    OpaquePredicateOp, Predicate as Pred, ScalarExpressionEvaluator, StructData,
};
use crate::kernel_predicates::parquet_stats_skipping::ParquetStatsProvider;
use crate::scan::data_skipping::as_data_skipping_predicate;
use crate::schema::ArrayType;
use crate::DataType;
use crate::DeltaResult;

use std::collections::HashMap;

macro_rules! expect_eq {
    ( $expr: expr, $expect: expr, $fmt: literal ) => {
        let expect = ($expect);
        let result = ($expr);
        assert!(
            result == expect,
            "Expected {} = {:?}, got {:?}",
            format!($fmt),
            expect,
            result
        );
    };
}

impl ResolveColumnAsScalar for Scalar {
    fn resolve_column(&self, _col: &ColumnName) -> Option<Scalar> {
        Some(self.clone())
    }
}

#[test]
fn test_default_eval_scalar() {
    let test_cases = [
        (Scalar::Boolean(true), false, Some(true)),
        (Scalar::Boolean(true), true, Some(false)),
        (Scalar::Boolean(false), false, Some(false)),
        (Scalar::Boolean(false), true, Some(true)),
        (Scalar::Long(1), false, None),
        (Scalar::Long(1), true, None),
        (Scalar::Null(DataType::BOOLEAN), false, None),
        (Scalar::Null(DataType::BOOLEAN), true, None),
        (Scalar::Null(DataType::LONG), false, None),
        (Scalar::Null(DataType::LONG), true, None),
    ];
    for (value, inverted, expect) in test_cases.into_iter() {
        assert_eq!(
            KernelPredicateEvaluatorDefaults::eval_pred_scalar(&value, inverted),
            expect,
            "value: {value:?} inverted: {inverted}"
        );
    }
}

// verifies that partial orderings behave as expected for all Scalar types
#[test]
fn test_default_partial_cmp_scalars() {
    use Ordering::*;
    use Scalar::*;

    let smaller_values = &[
        Integer(1),
        Long(1),
        Short(1),
        Byte(1),
        Float(1.0),
        Double(1.0),
        String("1".into()),
        Boolean(false),
        Timestamp(1),
        TimestampNtz(1),
        Date(1),
        Binary(vec![1]),
        Scalar::decimal(1, 10, 10).unwrap(),
        Null(DataType::LONG),
        Struct(StructData::try_new(vec![], vec![]).unwrap()),
        Array(ArrayData::try_new(ArrayType::new(DataType::LONG, false), &[] as &[i64]).unwrap()),
    ];
    let larger_values = &[
        Integer(10),
        Long(10),
        Short(10),
        Byte(10),
        Float(10.0),
        Double(10.0),
        String("10".into()),
        Boolean(true),
        Timestamp(10),
        TimestampNtz(10),
        Date(10),
        Binary(vec![10]),
        Scalar::decimal(10, 10, 10).unwrap(),
        Null(DataType::LONG),
        Struct(StructData::try_new(vec![], vec![]).unwrap()),
        Array(ArrayData::try_new(ArrayType::new(DataType::LONG, false), &[] as &[i64]).unwrap()),
    ];

    // scalars of different types are always incomparable
    let compare = KernelPredicateEvaluatorDefaults::partial_cmp_scalars;
    for (i, a) in smaller_values.iter().enumerate() {
        for b in smaller_values.iter().skip(i + 1) {
            for op in [Less, Equal, Greater] {
                for inverted in [true, false] {
                    assert!(
                        compare(op, a, b, inverted).is_none(),
                        "{:?} should not be comparable to {:?}",
                        a.data_type(),
                        b.data_type()
                    );
                }
            }
        }
    }

    let expect_if_comparable_type = |s: &_, expect| match s {
        Null(_) | Struct(_) | Array(_) => None,
        _ => Some(expect),
    };

    // Test same-type comparisons where a == b
    for (a, b) in smaller_values.iter().zip(smaller_values) {
        for inverted in [true, false] {
            expect_eq!(
                compare(Less, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} < {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Equal, a, b, inverted),
                expect_if_comparable_type(a, !inverted),
                "{a:?} == {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Greater, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} > {b:?} (inverted: {inverted})"
            );
        }
    }

    // Test same-type comparisons where a < b
    for (a, b) in smaller_values.iter().zip(larger_values) {
        for inverted in [true, false] {
            expect_eq!(
                compare(Less, a, b, inverted),
                expect_if_comparable_type(a, !inverted),
                "{a:?} < {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Equal, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} == {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Greater, a, b, inverted),
                expect_if_comparable_type(a, inverted),
                "{a:?} < {b:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Less, b, a, inverted),
                expect_if_comparable_type(a, inverted),
                "{b:?} < {a:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Equal, b, a, inverted),
                expect_if_comparable_type(a, inverted),
                "{b:?} == {a:?} (inverted: {inverted})"
            );

            expect_eq!(
                compare(Greater, b, a, inverted),
                expect_if_comparable_type(a, !inverted),
                "{b:?} < {a:?} (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_default_scalar_arithmetic() {
    use Scalar::*;
    let left = &[Byte(2), Short(200), Integer(20000), Long(2000000)];
    let right = &[Byte(3), Short(30), Integer(3000), Long(300000)];
    let expected = [
        (Byte(5), Byte(-1), Byte(6), Byte(0)),
        (Short(230), Short(170), Short(6000), Short(6)),
        (
            Integer(23000),
            Integer(17000),
            Integer(60000000),
            Integer(6),
        ),
        (Long(2300000), Long(1700000), Long(600000000000), Long(6)),
    ];

    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(1));
    for ((l, r), (add, sub, mul, div)) in left.iter().zip(right).zip(expected) {
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) + Expr::literal(r.clone()))),
            Some(add),
            "add({l:?}, {r:?})"
        );
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) - Expr::literal(r.clone()))),
            Some(sub),
            "sub({l:?}, {r:?})"
        );
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) * Expr::literal(r.clone()))),
            Some(mul),
            "mul({l:?}, {r:?})"
        );
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) / Expr::literal(r.clone()))),
            Some(div),
            "div({l:?}, {r:?})"
        );
    }

    // Invalid type combinations
    expect_eq!(
        filter.eval_expr(&(Expr::literal("hi") + Expr::literal("ho"))),
        None,
        "add(string, string)"
    );
    expect_eq!(
        filter.eval_expr(&(Expr::literal(1i8) + Expr::literal(1i64))),
        None,
        "add(byte, long)"
    );
    expect_eq!(
        filter.eval_expr(&(Expr::literal(1i8) - Expr::literal(1i64))),
        None,
        "sub(byte, long)"
    );
    expect_eq!(
        filter.eval_expr(&(Expr::literal(1i8) * Expr::literal(1i64))),
        None,
        "mul(byte, long)"
    );
    expect_eq!(
        filter.eval_expr(&(Expr::literal(1i8) / Expr::literal(1i64))),
        None,
        "div(byte, long)"
    );

    // Addition overflow
    let args = [
        (Byte(i8::MAX), Byte(1)),
        (Short(i16::MAX), Short(1)),
        (Integer(i32::MAX), Integer(1)),
        (Long(i64::MAX), Long(1)),
    ];
    for (l, r) in args {
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) + Expr::literal(r.clone()))),
            None,
            "add({l:?}, {r:?})"
        );
    }

    // Subtraction overflow
    let args = [
        (Byte(i8::MIN), Byte(1)),
        (Short(i16::MIN), Short(1)),
        (Integer(i32::MIN), Integer(1)),
        (Long(i64::MIN), Long(1)),
    ];
    for (l, r) in args {
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) - Expr::literal(r.clone()))),
            None,
            "sub({l:?}, {r:?})"
        );
    }

    // Multiplication overflow
    let args = [
        Byte(i8::MAX),
        Short(i16::MAX),
        Integer(i32::MAX),
        Long(i64::MAX),
    ];
    for arg in args {
        expect_eq!(
            filter.eval_expr(&(Expr::literal(arg.clone()) * Expr::literal(arg.clone()))),
            None,
            "mul({arg:?}, {arg:?})"
        );
    }

    // Division overflow
    let args = [
        (Byte(i8::MAX), Byte(0)),
        (Short(i16::MAX), Short(0)),
        (Integer(i32::MAX), Integer(0)),
        (Long(i64::MAX), Long(0)),
    ];
    for (l, r) in args {
        expect_eq!(
            filter.eval_expr(&(Expr::literal(l.clone()) / Expr::literal(r.clone()))),
            None,
            "div({l:?}, {r:?})"
        );
    }
}

// Verifies that eval_binary_scalars uses partial_cmp_scalars correctly
#[test]
fn test_eval_binary_scalars() {
    use BinaryPredicateOp::*;
    let smaller_value = Scalar::Long(1);
    let larger_value = Scalar::Long(10);
    for inverted in [true, false] {
        let compare = KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars;
        expect_eq!(
            compare(Equal, &smaller_value, &smaller_value, inverted),
            Some(!inverted),
            "{smaller_value} == {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(Equal, &smaller_value, &larger_value, inverted),
            Some(inverted),
            "{smaller_value} == {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(LessThan, &smaller_value, &smaller_value, inverted),
            Some(inverted),
            "{smaller_value} < {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(LessThan, &smaller_value, &larger_value, inverted),
            Some(!inverted),
            "{smaller_value} < {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(GreaterThan, &smaller_value, &smaller_value, inverted),
            Some(inverted),
            "{smaller_value} > {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(GreaterThan, &smaller_value, &larger_value, inverted),
            Some(inverted),
            "{smaller_value} > {larger_value} (inverted: {inverted})"
        );
    }
}

// NOTE: We're testing routing here -- the actual comparisons are already validated by test_eval_binary_scalars.
#[test]
fn test_eval_binary_columns() {
    let columns = HashMap::from_iter(vec![
        (column_name!("x"), Scalar::from(1)),
        (column_name!("y"), Scalar::from(10)),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(columns);
    let x = column_expr!("x");
    let y = column_expr!("y");
    for inverted in [true, false] {
        assert_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Equal, &x, &y, inverted),
            Some(inverted),
            "x = y (inverted: {inverted})"
        );
        assert_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Equal, &x, &x, inverted),
            Some(!inverted),
            "x = x (inverted: {inverted})"
        );
    }
}

#[test]
fn test_eval_junction() {
    let test_cases: Vec<(&[_], _, _)> = vec![
        // input, AND expect, OR expect
        (&[], Some(true), Some(false)),
        (&[Some(true)], Some(true), Some(true)),
        (&[Some(false)], Some(false), Some(false)),
        (&[None], None, None),
        (&[Some(true), Some(false)], Some(false), Some(true)),
        (&[Some(false), Some(true)], Some(false), Some(true)),
        (&[Some(true), None], None, Some(true)),
        (&[None, Some(true)], None, Some(true)),
        (&[Some(false), None], Some(false), None),
        (&[None, Some(false)], Some(false), None),
        (&[None, Some(false), Some(true)], Some(false), Some(true)),
        (&[None, Some(true), Some(false)], Some(false), Some(true)),
        (&[Some(false), None, Some(true)], Some(false), Some(true)),
        (&[Some(true), None, Some(false)], Some(false), Some(true)),
        (&[Some(false), Some(true), None], Some(false), Some(true)),
        (&[Some(true), Some(false), None], Some(false), Some(true)),
    ];
    let filter = DefaultKernelPredicateEvaluator::from(UnimplementedColumnResolver);
    for (inputs, expect_and, expect_or) in test_cases.iter() {
        let inputs: Vec<_> = inputs
            .iter()
            .cloned()
            .map(|v| match v {
                Some(v) => Pred::literal(v),
                None => Pred::null_literal(),
            })
            .collect();
        for inverted in [true, false] {
            let invert_if_needed = |v: &Option<_>| v.map(|v| v != inverted);
            expect_eq!(
                filter.eval_pred_junction(JunctionPredicateOp::And, &inputs, inverted),
                invert_if_needed(expect_and),
                "AND({inputs:?}) (inverted: {inverted})"
            );
            expect_eq!(
                filter.eval_pred_junction(JunctionPredicateOp::Or, &inputs, inverted),
                invert_if_needed(expect_or),
                "OR({inputs:?}) (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_eval_column() {
    let test_cases = [
        (Scalar::from(true), Some(true)),
        (Scalar::from(false), Some(false)),
        (Scalar::Null(DataType::BOOLEAN), None),
        (Scalar::from(1), None),
    ];
    let col = &column_name!("x");
    for (input, expect) in &test_cases {
        let filter = DefaultKernelPredicateEvaluator::from(input.clone());
        for inverted in [true, false] {
            expect_eq!(
                filter.eval_pred_column(col, inverted),
                expect.map(|v| v != inverted),
                "{input:?} (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_eval_not() {
    let test_cases = [
        (Scalar::Boolean(true), Some(false)),
        (Scalar::Boolean(false), Some(true)),
        (Scalar::Null(DataType::BOOLEAN), None),
        (Scalar::Long(1), None),
    ];
    let filter = DefaultKernelPredicateEvaluator::from(UnimplementedColumnResolver);
    for (input, expect) in test_cases {
        let input = Pred::from_expr(input);
        for inverted in [true, false] {
            expect_eq!(
                filter.eval_pred_not(&input, inverted),
                expect.map(|v| v != inverted),
                "NOT({input:?}) (inverted: {inverted})"
            );
        }
    }
}

#[test]
fn test_eval_is_null() {
    use crate::expressions::UnaryPredicateOp::IsNull;
    let expr = column_expr!("x");
    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(1));
    expect_eq!(
        filter.eval_pred_unary(IsNull, &expr, true),
        Some(true),
        "x IS NOT NULL"
    );
    expect_eq!(
        filter.eval_pred_unary(IsNull, &expr, false),
        Some(false),
        "x IS NULL"
    );

    let expr = Expr::literal(1);
    expect_eq!(
        filter.eval_pred_unary(IsNull, &expr, true),
        Some(true),
        "1 IS NOT NULL"
    );
    expect_eq!(
        filter.eval_pred_unary(IsNull, &expr, false),
        Some(false),
        "1 IS NULL"
    );
}

#[test]
fn test_eval_distinct() {
    let one = &Scalar::from(1);
    let two = &Scalar::from(2);
    let null = &Scalar::Null(DataType::INTEGER);
    let filter = DefaultKernelPredicateEvaluator::from(one.clone());
    let col = &column_name!("x");
    expect_eq!(
        filter.eval_pred_distinct(col, one, true),
        Some(true),
        "NOT DISTINCT(x, 1) (x = 1)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, one, false),
        Some(false),
        "DISTINCT(x, 1) (x = 1)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, two, true),
        Some(false),
        "NOT DISTINCT(x, 2) (x = 1)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, two, false),
        Some(true),
        "DISTINCT(x, 2) (x = 1)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, null, true),
        Some(false),
        "NOT DISTINCT(x, NULL) (x = 1)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, null, false),
        Some(true),
        "DISTINCT(x, NULL) (x = 1)"
    );

    let filter = DefaultKernelPredicateEvaluator::from(null.clone());
    expect_eq!(
        filter.eval_pred_distinct(col, one, true),
        Some(false),
        "NOT DISTINCT(x, 1) (x = NULL)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, one, false),
        Some(true),
        "DISTINCT(x, 1) (x = NULL)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, null, true),
        Some(true),
        "NOT DISTINCT(x, NULL) (x = NULL)"
    );
    expect_eq!(
        filter.eval_pred_distinct(col, null, false),
        Some(false),
        "DISTINCT(x, NULL) (x = NULL)"
    );
}

// NOTE: We're testing routing here -- the actual comparisons are already validated by
// test_eval_binary_scalars.
#[test]
fn eval_binary() {
    use crate::expressions::BinaryPredicateOp;

    let col = column_expr!("x");
    let val = Expr::literal(10);
    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(1));

    for inverted in [true, false] {
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::LessThan, &col, &val, inverted),
            Some(!inverted),
            "x < 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Equal, &col, &val, inverted),
            Some(inverted),
            "x = 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::GreaterThan, &col, &val, inverted),
            Some(inverted),
            "x > 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Distinct, &col, &val, inverted),
            Some(!inverted),
            "DISTINCT(x, 10) (inverted: {inverted})"
        );

        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::LessThan, &val, &col, inverted),
            Some(inverted),
            "10 < x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Equal, &val, &col, inverted),
            Some(inverted),
            "10 = x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::GreaterThan, &val, &col, inverted),
            Some(!inverted),
            "10 > x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Distinct, &val, &col, inverted),
            Some(!inverted),
            "DISTINCT(10, x) (inverted: {inverted})"
        );
    }
}

#[derive(Debug, PartialEq)]
struct OpaqueLessThanOp;
impl OpaqueLessThanOp {
    fn name(&self) -> &str {
        "less_than"
    }

    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<bool> {
        let [a, b] = exprs else {
            return None; // wrong arg count
        };
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(
            BinaryPredicateOp::LessThan,
            &eval_expr(a)?,
            &eval_expr(b)?,
            inverted,
        )
    }
}

impl OpaqueExpressionOp for OpaqueLessThanOp {
    fn name(&self) -> &str {
        self.name()
    }
    fn eval_expr_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        exprs: &[Expr],
    ) -> DeltaResult<Scalar> {
        let result = match self.eval_expr_scalar(eval_expr, exprs, false) {
            Some(value) => Scalar::from(value),
            None => Scalar::Null(DataType::BOOLEAN),
        };
        Ok(result)
    }
}

impl OpaquePredicateOp for OpaqueLessThanOp {
    fn name(&self) -> &str {
        self.name()
    }
    fn eval_pred_scalar(
        &self,
        eval_expr: &ScalarExpressionEvaluator<'_>,
        _evaluator: &DirectPredicateEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        Ok(self.eval_expr_scalar(eval_expr, exprs, inverted))
    }

    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<bool> {
        let (col, val, ord) = match exprs {
            [Expr::Column(col), Expr::Literal(val)] => (col, val, Ordering::Less),
            [Expr::Literal(val), Expr::Column(col)] => (col, val, Ordering::Greater),
            _ => return None,
        };
        evaluator.partial_cmp_min_stat(col, val, ord, inverted)
    }

    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Pred> {
        let (col, val, ord) = match exprs {
            [Expr::Column(col), Expr::Literal(val)] => (col, val, Ordering::Less),
            [Expr::Literal(val), Expr::Column(col)] => (col, val, Ordering::Greater),
            _ => return None,
        };

        // NOTE: `evaluator.partial_cmp_min`_stat returns `Pred::Binary`. That's fine, because we have
        // separate testing for the `eval_pred_scalar` path.
        evaluator.partial_cmp_min_stat(col, val, ord, inverted)
    }
}

struct MinStatsValue(Scalar);

impl ParquetStatsProvider for MinStatsValue {
    fn get_parquet_min_stat(&self, _col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        (self.0.data_type() == *data_type).then(|| self.0.clone())
    }

    fn get_parquet_max_stat(&self, _col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_parquet_nullcount_stat(&self, _col: &ColumnName) -> Option<i64> {
        Some(0)
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        1
    }
}

#[test]
fn test_eval_opaque_simple() {
    let expr = Expr::opaque(OpaqueLessThanOp, vec![column_expr!("x"), Expr::literal(10)]);
    let pred = Pred::opaque(OpaqueLessThanOp, vec![column_expr!("x"), Expr::literal(10)]);
    let skipping_pred = as_data_skipping_predicate(&pred).unwrap();

    assert_eq!(expr, expr);
    assert_eq!(pred, pred);

    // Test direct expression and predicate eval, and indirect data skipping
    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(1));
    assert_eq!(filter.eval_expr(&expr), Some(Scalar::from(true)), "x < 10");
    assert_eq!(filter.eval(&pred), Some(true), "x < 10");
    assert_eq!(filter.eval(&skipping_pred), Some(true), "x < 10");

    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(100));
    assert_eq!(filter.eval_expr(&expr), Some(Scalar::from(false)), "x < 10");
    assert_eq!(filter.eval(&pred), Some(false), "x < 10");
    assert_eq!(filter.eval(&skipping_pred), Some(false), "x < 10");

    // Test direct data skipping
    let filter = MinStatsValue(Scalar::from(1));
    assert_eq!(filter.eval(&pred), Some(true), "x < 10");

    let filter = MinStatsValue(Scalar::from(100));
    assert_eq!(filter.eval(&pred), Some(false), "x < 10");

    // Verify round trip evaluation of pred -> expr -> pred
    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(1));
    let pred = Pred::from_expr(Expr::from(Pred::opaque(
        OpaqueLessThanOp,
        vec![column_expr!("x"), Expr::literal(10)],
    )));
    assert_eq!(filter.eval(&pred), Some(true), "pred(expr(x < 10))");
}

#[derive(Debug, PartialEq)]
struct OpaqueAndOp;
impl OpaquePredicateOp for OpaqueAndOp {
    fn name(&self) -> &str {
        "and"
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        evaluator: &DirectPredicateEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> DeltaResult<Option<bool>> {
        let mut values = exprs
            .iter()
            .map(|expr| evaluator.eval_pred_expr(expr, inverted));
        Ok(evaluator.finish_eval_pred_junction(JunctionPredicateOp::And, &mut values, inverted))
    }

    fn eval_as_data_skipping_predicate(
        &self,
        evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<bool> {
        let mut values = exprs
            .iter()
            .map(|expr| evaluator.eval_pred_expr(expr, inverted));
        evaluator.finish_eval_pred_junction(JunctionPredicateOp::And, &mut values, inverted)
    }

    fn as_data_skipping_predicate(
        &self,
        evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Pred> {
        let mut values = exprs
            .iter()
            .map(|expr| evaluator.eval_pred_expr(expr, inverted));
        evaluator.finish_eval_pred_junction(JunctionPredicateOp::And, &mut values, inverted)
    }
}

struct OneStatsValue(Scalar);

impl ParquetStatsProvider for OneStatsValue {
    fn get_parquet_min_stat(&self, _col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        (self.0.data_type() == *data_type).then(|| self.0.clone())
    }

    fn get_parquet_max_stat(&self, _col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        (self.0.data_type() == *data_type).then(|| self.0.clone())
    }

    fn get_parquet_nullcount_stat(&self, _col: &ColumnName) -> Option<i64> {
        let nullcount = match self.0 {
            Scalar::Null(_) => 1,
            _ => 0,
        };
        Some(nullcount)
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        1
    }
}

#[test]
fn test_eval_opaque_predicate() {
    let pred = Pred::opaque(OpaqueAndOp, vec![column_expr!("x"), Expr::literal(true)]);
    let skipping_pred = as_data_skipping_predicate(&pred).unwrap();

    // Test direct evaluation and indirect data skipping
    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(true));
    assert_eq!(filter.eval(&pred), Some(true), "AND(x, TRUE)");
    assert_eq!(filter.eval(&skipping_pred), Some(true), "AND(x, TRUE)");

    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(false));
    assert_eq!(filter.eval(&pred), Some(false), "AND(x, TRUE)");
    assert_eq!(filter.eval(&skipping_pred), Some(false), "AND(x, TRUE)");

    let filter = DefaultKernelPredicateEvaluator::from(Scalar::Null(DataType::BOOLEAN));
    assert_eq!(filter.eval(&pred), None, "AND(x, TRUE)");
    assert_eq!(filter.eval(&skipping_pred), None, "AND(x, TRUE)");

    // Test direct data skipping
    let filter = OneStatsValue(Scalar::from(true));
    assert_eq!(filter.eval(&pred), Some(true), "AND(x, TRUE)");

    let filter = OneStatsValue(Scalar::from(false));
    assert_eq!(filter.eval(&pred), Some(false), "AND(x, TRUE)");

    let filter = OneStatsValue(Scalar::Null(DataType::BOOLEAN));
    assert_eq!(filter.eval(&pred), None, "AND(x, TRUE)");
}

#[test]
fn test_eval_opaque_complex() {
    // A contrived example that uses an opaque predicate that references an opaque expression
    let complex_pred = Pred::and(
        Pred::lt(column_expr!("x"), Scalar::from(true)),
        Pred::opaque(
            OpaqueLessThanOp,
            vec![
                column_expr!("x"),
                Expr::opaque(OpaqueLessThanOp, vec![Expr::literal(2), Expr::literal(5)]),
            ],
        ),
    );

    // NOTE: The opaque expression does not support indirect data skipping for complex expression
    // inputs, so we end up with `AND(NULL, ...)` which is NULL unless another leg is FALSE.
    let complex_skipping_pred = as_data_skipping_predicate(&complex_pred).unwrap();

    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(false));
    assert_eq!(
        filter.eval(&complex_pred),
        Some(true),
        "AND(x < TRUE, x < (2 < 5))"
    );
    assert_eq!(
        filter.eval(&complex_skipping_pred),
        None,
        "AND(x < TRUE, x < (2 < 5))"
    );

    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(true));
    assert_eq!(
        filter.eval(&complex_pred),
        Some(false),
        "AND(x < TRUE, x < (2 < 5))"
    );
    assert_eq!(
        filter.eval(&complex_skipping_pred),
        Some(false),
        "AND(x < TRUE, x < (2 < 5))"
    );
}

#[test]
fn test_eval_unknown() {
    let filter = DefaultKernelPredicateEvaluator::from(Scalar::from(1));
    expect_eq!(filter.eval_expr(&Expr::unknown("unknown")), None, "UNKNOWN");
    expect_eq!(filter.eval(&Pred::unknown("unknown")), None, "UNKNOWN");
}

// NOTE: `None` is NOT equivalent to `Some(Scalar::Null)`
struct NullColumnResolver;
impl ResolveColumnAsScalar for NullColumnResolver {
    fn resolve_column(&self, _col: &ColumnName) -> Option<Scalar> {
        Some(Scalar::Null(DataType::INTEGER))
    }
}

#[test]
fn test_sql_where() {
    let col = &column_expr!("x");
    let col_pred = &column_pred!("x");
    const VAL: Expr = Expr::Literal(Scalar::Integer(1));
    const NULL: Pred = Pred::null_literal();
    const FALSE: Pred = Pred::literal(false);
    const TRUE: Pred = Pred::literal(true);
    let null_filter = DefaultKernelPredicateEvaluator::from(NullColumnResolver);
    let empty_filter = DefaultKernelPredicateEvaluator::from(EmptyColumnResolver);

    // Basic sanity check
    expect_eq!(
        null_filter.eval_sql_where(&Pred::from_expr(VAL)),
        None,
        "WHERE {VAL}"
    );
    expect_eq!(
        empty_filter.eval_sql_where(&Pred::from_expr(VAL)),
        None,
        "WHERE {VAL}"
    );

    expect_eq!(
        null_filter.eval_sql_where(col_pred),
        Some(false),
        "WHERE {col_pred}"
    );
    expect_eq!(
        empty_filter.eval_sql_where(col_pred),
        None,
        "WHERE {col_pred}"
    );

    // SQL eval does not modify behavior of IS NULL
    let pred = &Pred::is_null(col.clone());
    expect_eq!(null_filter.eval_sql_where(pred), Some(true), "{pred}");

    // NOT a gets skipped when NULL but not when missing
    let pred = &Pred::not(col_pred.clone());
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    // Injected NULL checks only short circuit if inputs are NULL
    let pred = &Pred::lt(FALSE, TRUE);
    expect_eq!(null_filter.eval_sql_where(pred), Some(true), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), Some(true), "{pred}");

    // Contrast normal vs SQL WHERE semantics - comparison
    let pred = &Pred::lt(col.clone(), VAL);
    expect_eq!(null_filter.eval(pred), None, "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    // NULL check produces NULL due to missing column
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    let pred = &Pred::lt(VAL, col.clone());
    expect_eq!(null_filter.eval(pred), None, "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    let pred = &Pred::distinct(VAL, col.clone());
    expect_eq!(null_filter.eval(pred), Some(true), "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(true), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    let pred = &Pred::distinct(NULL, col.clone());
    expect_eq!(null_filter.eval(pred), Some(false), "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    // Contrast normal vs SQL WHERE semantics - comparison inside AND
    let pred = &Pred::and(TRUE, Pred::lt(col.clone(), VAL));
    expect_eq!(null_filter.eval(pred), None, "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    // NULL allows static skipping under SQL semantics
    let pred = &Pred::and(NULL, Pred::lt(col.clone(), VAL));
    expect_eq!(null_filter.eval(pred), None, "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), Some(false), "{pred}");

    // Contrast normal vs. SQL WHERE semantics - comparison inside AND inside AND
    let pred = &Pred::and(TRUE, Pred::and(TRUE, Pred::lt(col.clone(), VAL)));
    expect_eq!(null_filter.eval(pred), None, "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");

    // Ditto for comparison inside OR inside AND
    let pred = &Pred::or(FALSE, Pred::and(TRUE, Pred::lt(col.clone(), VAL)));
    expect_eq!(null_filter.eval(pred), None, "{pred}");
    expect_eq!(null_filter.eval_sql_where(pred), Some(false), "{pred}");
    expect_eq!(empty_filter.eval_sql_where(pred), None, "{pred}");
}
