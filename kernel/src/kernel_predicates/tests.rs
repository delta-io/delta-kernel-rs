use super::*;
use crate::expressions::{
    column_expr, column_name, column_pred, ArrayData, Expression as Expr, Predicate as Pred,
    StructData,
};
use crate::schema::ArrayType;
use crate::DataType;

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
            compare(NotEqual, &smaller_value, &smaller_value, inverted),
            Some(inverted),
            "{smaller_value} != {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(NotEqual, &smaller_value, &larger_value, inverted),
            Some(!inverted),
            "{smaller_value} != {larger_value} (inverted: {inverted})"
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

        expect_eq!(
            compare(LessThanOrEqual, &smaller_value, &smaller_value, inverted),
            Some(!inverted),
            "{smaller_value} <= {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(LessThanOrEqual, &smaller_value, &larger_value, inverted),
            Some(!inverted),
            "{smaller_value} <= {larger_value} (inverted: {inverted})"
        );

        expect_eq!(
            compare(GreaterThanOrEqual, &smaller_value, &smaller_value, inverted),
            Some(!inverted),
            "{smaller_value} >= {smaller_value} (inverted: {inverted})"
        );
        expect_eq!(
            compare(GreaterThanOrEqual, &smaller_value, &larger_value, inverted),
            Some(inverted),
            "{smaller_value} >= {larger_value} (inverted: {inverted})"
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
            filter.eval_pred_binary(BinaryPredicateOp::LessThanOrEqual, &col, &val, inverted),
            Some(!inverted),
            "x <= 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Equal, &col, &val, inverted),
            Some(inverted),
            "x = 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::NotEqual, &col, &val, inverted),
            Some(!inverted),
            "x != 10 (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::GreaterThanOrEqual, &col, &val, inverted),
            Some(inverted),
            "x >= 10 (inverted: {inverted})"
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
            filter.eval_pred_binary(BinaryPredicateOp::LessThanOrEqual, &val, &col, inverted),
            Some(inverted),
            "10 <= x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::Equal, &val, &col, inverted),
            Some(inverted),
            "10 = x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::NotEqual, &val, &col, inverted),
            Some(!inverted),
            "10 != x (inverted: {inverted})"
        );
        expect_eq!(
            filter.eval_pred_binary(BinaryPredicateOp::GreaterThanOrEqual, &val, &col, inverted),
            Some(!inverted),
            "10 >= x (inverted: {inverted})"
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
