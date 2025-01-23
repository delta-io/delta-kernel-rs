use super::*;

use crate::expressions::column_name;
use crate::predicates::{DefaultPredicateEvaluator, UnimplementedColumnResolver};
use std::collections::HashMap;

const TRUE: Option<bool> = Some(true);
const FALSE: Option<bool> = Some(false);
const NULL: Option<bool> = None;

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

#[test]
fn test_eval_is_null() {
    let col = &column_expr!("x");
    let expressions = [Expr::is_null(col.clone()), !Expr::is_null(col.clone())];

    let do_test = |nullcount: i64, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            (column_name!("numRecords"), Scalar::from(2i64)),
            (column_name!("nullCount.x"), Scalar::from(nullcount)),
        ]);
        let filter = DefaultPredicateEvaluator::from(resolver);
        for (expr, expect) in expressions.iter().zip(expected) {
            let pred = as_data_skipping_predicate(expr).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                *expect,
                "{expr:#?} became {pred:#?} ({nullcount} nulls)"
            );
        }
    };

    // no nulls
    do_test(0, &[FALSE, TRUE]);

    // some nulls
    do_test(1, &[TRUE, TRUE]);

    // all nulls
    do_test(2, &[TRUE, FALSE]);
}

#[test]
fn test_eval_binary_comparisons() {
    let col = &column_expr!("x");
    let five = &Scalar::from(5);
    let ten = &Scalar::from(10);
    let fifteen = &Scalar::from(15);
    let null = &Scalar::Null(DataType::INTEGER);

    let expressions = [
        Expr::lt(col.clone(), ten.clone()),
        Expr::le(col.clone(), ten.clone()),
        Expr::eq(col.clone(), ten.clone()),
        Expr::ne(col.clone(), ten.clone()),
        Expr::gt(col.clone(), ten.clone()),
        Expr::ge(col.clone(), ten.clone()),
    ];

    let do_test = |min: &Scalar, max: &Scalar, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            (column_name!("minValues.x"), min.clone()),
            (column_name!("maxValues.x"), max.clone()),
        ]);
        let filter = DefaultPredicateEvaluator::from(resolver);
        for (expr, expect) in expressions.iter().zip(expected.iter()) {
            let pred = as_data_skipping_predicate(expr).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                *expect,
                "{expr:#?} became {pred:#?} with [{min}..{max}]"
            );
        }
    };

    // value < min = max (15..15 = 10, 15..15 <= 10, etc)
    do_test(fifteen, fifteen, &[FALSE, FALSE, FALSE, TRUE, TRUE, TRUE]);

    // min = max = value (10..10 = 10, 10..10 <= 10, etc)
    //
    // NOTE: missing min or max stat produces NULL output if the expression needed it.
    do_test(ten, ten, &[FALSE, TRUE, TRUE, FALSE, FALSE, TRUE]);
    do_test(null, ten, &[NULL, NULL, NULL, NULL, FALSE, TRUE]);
    do_test(ten, null, &[FALSE, TRUE, NULL, NULL, NULL, NULL]);

    // min = max < value (5..5 = 10, 5..5 <= 10, etc)
    do_test(five, five, &[TRUE, TRUE, FALSE, TRUE, FALSE, FALSE]);

    // value = min < max (5..15 = 10, 5..15 <= 10, etc)
    do_test(ten, fifteen, &[FALSE, TRUE, TRUE, TRUE, TRUE, TRUE]);

    // min < value < max (5..15 = 10, 5..15 <= 10, etc)
    do_test(five, fifteen, &[TRUE, TRUE, TRUE, TRUE, TRUE, TRUE]);
}

#[test]
fn test_eval_variadic() {
    let test_cases = &[
        (&[] as &[Option<bool>], TRUE, FALSE),
        (&[TRUE], TRUE, TRUE),
        (&[FALSE], FALSE, FALSE),
        (&[NULL], NULL, NULL),
        (&[TRUE, TRUE], TRUE, TRUE),
        (&[TRUE, FALSE], FALSE, TRUE),
        (&[TRUE, NULL], NULL, TRUE),
        (&[FALSE, TRUE], FALSE, TRUE),
        (&[FALSE, FALSE], FALSE, FALSE),
        (&[FALSE, NULL], FALSE, NULL),
        (&[NULL, TRUE], NULL, TRUE),
        (&[NULL, FALSE], FALSE, NULL),
        (&[NULL, NULL], NULL, NULL),
        // Every combo of 1:2
        (&[TRUE, FALSE, FALSE], FALSE, TRUE),
        (&[FALSE, TRUE, FALSE], FALSE, TRUE),
        (&[FALSE, FALSE, TRUE], FALSE, TRUE),
        (&[TRUE, NULL, NULL], NULL, TRUE),
        (&[NULL, TRUE, NULL], NULL, TRUE),
        (&[NULL, NULL, TRUE], NULL, TRUE),
        (&[FALSE, TRUE, TRUE], FALSE, TRUE),
        (&[TRUE, FALSE, TRUE], FALSE, TRUE),
        (&[TRUE, TRUE, FALSE], FALSE, TRUE),
        (&[FALSE, NULL, NULL], FALSE, NULL),
        (&[NULL, FALSE, NULL], FALSE, NULL),
        (&[NULL, NULL, FALSE], FALSE, NULL),
        (&[NULL, TRUE, TRUE], NULL, TRUE),
        (&[TRUE, NULL, TRUE], NULL, TRUE),
        (&[TRUE, TRUE, NULL], NULL, TRUE),
        (&[NULL, FALSE, FALSE], FALSE, NULL),
        (&[FALSE, NULL, FALSE], FALSE, NULL),
        (&[FALSE, FALSE, NULL], FALSE, NULL),
        // Every unique ordering of 3
        (&[TRUE, FALSE, NULL], FALSE, TRUE),
        (&[TRUE, NULL, FALSE], FALSE, TRUE),
        (&[FALSE, TRUE, NULL], FALSE, TRUE),
        (&[FALSE, NULL, TRUE], FALSE, TRUE),
        (&[NULL, TRUE, FALSE], FALSE, TRUE),
        (&[NULL, FALSE, TRUE], FALSE, TRUE),
    ];
    let filter = DefaultPredicateEvaluator::from(UnimplementedColumnResolver);
    for (inputs, expect_and, expect_or) in test_cases {
        let inputs: Vec<_> = inputs
            .iter()
            .map(|val| match val {
                Some(v) => Expr::literal(v),
                None => Expr::null_literal(DataType::BOOLEAN),
            })
            .collect();

        let expr = Expr::and_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr).unwrap();
        expect_eq!(
            filter.eval_expr(&pred, false),
            *expect_and,
            "AND({inputs:?})"
        );

        let expr = Expr::or_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr).unwrap();
        expect_eq!(filter.eval_expr(&pred, false), *expect_or, "OR({inputs:?})");

        let expr = !Expr::and_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr).unwrap();
        expect_eq!(
            filter.eval_expr(&pred, false),
            expect_and.map(|val| !val),
            "NOT AND({inputs:?})"
        );

        let expr = !Expr::or_from(inputs.clone());
        let pred = as_data_skipping_predicate(&expr).unwrap();
        expect_eq!(
            filter.eval_expr(&pred, false),
            expect_or.map(|val| !val),
            "NOT OR({inputs:?})"
        );
    }
}

// DISTINCT is actually quite complex internally. It indirectly exercises IS [NOT] NULL and
// AND/OR. A different test validates min/max comparisons, so here we're mostly worried about NULL
// vs. non-NULL literals and nullcount/rowcount stats.
#[test]
fn test_eval_distinct() {
    let col = &column_expr!("x");
    let five = &Scalar::from(5);
    let ten = &Scalar::from(10);
    let fifteen = &Scalar::from(15);
    let null = &Scalar::Null(DataType::INTEGER);

    let expressions = [
        Expr::distinct(col.clone(), ten.clone()),
        !Expr::distinct(col.clone(), ten.clone()),
        Expr::distinct(col.clone(), null.clone()),
        !Expr::distinct(col.clone(), null.clone()),
    ];

    let do_test = |min: &Scalar, max: &Scalar, nullcount: i64, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            (column_name!("numRecords"), Scalar::from(2i64)),
            (column_name!("nullCount.x"), Scalar::from(nullcount)),
            (column_name!("minValues.x"), min.clone()),
            (column_name!("maxValues.x"), max.clone()),
        ]);
        let filter = DefaultPredicateEvaluator::from(resolver);
        for (expr, expect) in expressions.iter().zip(expected) {
            let pred = as_data_skipping_predicate(expr).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                *expect,
                "{expr:#?} became {pred:#?} ({min}..{max}, {nullcount} nulls)"
            );
        }
    };

    // min = max = value, no nulls
    do_test(ten, ten, 0, &[FALSE, TRUE, TRUE, FALSE]);

    // min = max = value, some nulls
    do_test(ten, ten, 1, &[TRUE, TRUE, TRUE, TRUE]);

    // min = max = value, all nulls
    do_test(ten, ten, 2, &[TRUE, FALSE, FALSE, TRUE]);

    // value < min = max, no nulls
    do_test(fifteen, fifteen, 0, &[TRUE, FALSE, TRUE, FALSE]);

    // value < min = max, some nulls
    do_test(fifteen, fifteen, 1, &[TRUE, FALSE, TRUE, TRUE]);

    // value < min = max, all nulls
    do_test(fifteen, fifteen, 2, &[TRUE, FALSE, FALSE, TRUE]);

    // min < value < max, no nulls
    do_test(five, fifteen, 0, &[TRUE, TRUE, TRUE, FALSE]);

    // min < value < max, some nulls
    do_test(five, fifteen, 1, &[TRUE, TRUE, TRUE, TRUE]);

    // min < value < max, all nulls
    do_test(five, fifteen, 2, &[TRUE, FALSE, FALSE, TRUE]);
}

#[test]
fn test_sql_where() {
    let col = &column_expr!("x");
    const VAL: Expr = Expr::Literal(Scalar::Integer(10));
    const NULL: Expr = Expr::Literal(Scalar::Null(DataType::BOOLEAN));
    const FALSE: Expr = Expr::Literal(Scalar::Boolean(false));
    const TRUE: Expr = Expr::Literal(Scalar::Boolean(true));

    const ROWCOUNT: i64 = 2;
    const ALL_NULL: i64 = ROWCOUNT;
    const SOME_NULL: i64 = 1;
    const NO_NULL: i64 = 0;
    let do_test =
        |nulls: i64, expr: &Expr, missing: bool, expect: Option<bool>, expect_sql: Option<bool>| {
            assert!((0..=ROWCOUNT).contains(&nulls));
            let (min, max) = if nulls < ROWCOUNT {
                (Scalar::Integer(5), Scalar::Integer(15))
            } else {
                (
                    Scalar::Null(DataType::INTEGER),
                    Scalar::Null(DataType::INTEGER),
                )
            };
            let resolver = if missing {
                HashMap::new()
            } else {
                HashMap::from_iter([
                    (column_name!("numRecords"), Scalar::from(ROWCOUNT)),
                    (column_name!("nullCount.x"), Scalar::from(nulls)),
                    (column_name!("minValues.x"), min.clone()),
                    (column_name!("maxValues.x"), max.clone()),
                ])
            };
            let filter = DefaultPredicateEvaluator::from(resolver);
            let pred = as_data_skipping_predicate(expr).unwrap();
            expect_eq!(
                filter.eval_expr(&pred, false),
                expect,
                "{expr:#?} became {pred:#?} ({min}..{max}, {nulls} nulls)"
            );
            let sql_pred = as_sql_data_skipping_predicate(expr).unwrap();
            expect_eq!(
                filter.eval_expr(&sql_pred, false),
                expect_sql,
                "{expr:#?} became {sql_pred:#?} ({min}..{max}, {nulls} nulls)"
            );
        };

    // Sanity tests -- only all-null columns should behave differently between normal and SQL WHERE.
    const MISSING: bool = true;
    const PRESENT: bool = false;
    let expr = &Expr::lt(TRUE, FALSE);
    do_test(ALL_NULL, expr, MISSING, Some(false), Some(false));

    let expr = &Expr::is_not_null(col.clone());
    do_test(ALL_NULL, expr, PRESENT, Some(false), Some(false));
    do_test(ALL_NULL, expr, MISSING, None, None);

    // SQL WHERE allows a present-but-all-null column to be pruned, but not a missing column.
    let expr = &Expr::lt(col.clone(), VAL);
    do_test(NO_NULL, expr, PRESENT, Some(true), Some(true));
    do_test(SOME_NULL, expr, PRESENT, Some(true), Some(true));
    do_test(ALL_NULL, expr, PRESENT, None, Some(false));
    do_test(ALL_NULL, expr, MISSING, None, None);

    // Comparison inside AND works
    let expr = &Expr::and(TRUE, Expr::lt(VAL, col.clone()));
    do_test(ALL_NULL, expr, PRESENT, None, Some(false));
    do_test(ALL_NULL, expr, MISSING, None, None);

    // NULL inside AND allows static skipping under SQL semantics
    let expr = &Expr::and(NULL, Expr::lt(col.clone(), VAL));
    do_test(ALL_NULL, expr, PRESENT, None, Some(false));
    do_test(ALL_NULL, expr, MISSING, None, Some(false));

    // Comparison inside AND inside AND works
    let expr = &Expr::and(TRUE, Expr::and(TRUE, Expr::lt(col.clone(), VAL)));
    do_test(ALL_NULL, expr, PRESENT, None, Some(false));
    do_test(ALL_NULL, expr, MISSING, None, None);

    // Comparison inside OR works
    let expr = &Expr::or(FALSE, Expr::lt(col.clone(), VAL));
    do_test(ALL_NULL, expr, PRESENT, None, Some(false));
    do_test(ALL_NULL, expr, MISSING, None, None);

    // Comparison inside AND inside OR works
    let expr = &Expr::or(FALSE, Expr::and(TRUE, Expr::lt(col.clone(), VAL)));
    do_test(ALL_NULL, expr, PRESENT, None, Some(false));
    do_test(ALL_NULL, expr, MISSING, None, None);
}
