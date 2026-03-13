use super::*;

use crate::expressions::column_name;
use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, UnimplementedColumnResolver};
use rstest::rstest;
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
    let predicates = [Pred::is_null(col.clone()), Pred::is_not_null(col.clone())];

    let do_test = |nullcount: i64, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            (column_name!("stats_parsed.numRecords"), Scalar::from(2i64)),
            (
                column_name!("stats_parsed.nullCount.x"),
                Scalar::from(nullcount),
            ),
        ]);
        let filter = DefaultKernelPredicateEvaluator::from(resolver);
        for (pred, expect) in predicates.iter().zip(expected) {
            let skipping_pred = as_data_skipping_predicate(pred).unwrap();
            expect_eq!(
                filter.eval(&skipping_pred),
                *expect,
                "{pred:#?} became {skipping_pred:#?} ({nullcount} nulls)"
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

    let predicates = [
        Pred::lt(col.clone(), ten.clone()),
        Pred::le(col.clone(), ten.clone()),
        Pred::eq(col.clone(), ten.clone()),
        Pred::ne(col.clone(), ten.clone()),
        Pred::gt(col.clone(), ten.clone()),
        Pred::ge(col.clone(), ten.clone()),
    ];

    let do_test = |min: &Scalar, max: &Scalar, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            (column_name!("stats_parsed.minValues.x"), min.clone()),
            (column_name!("stats_parsed.maxValues.x"), max.clone()),
        ]);
        let filter = DefaultKernelPredicateEvaluator::from(resolver);
        for (pred, expect) in predicates.iter().zip(expected.iter()) {
            let skipping_pred = as_data_skipping_predicate(pred).unwrap();
            expect_eq!(
                filter.eval(&skipping_pred),
                *expect,
                "{pred:#?} became {skipping_pred:#?} with [{min}..{max}]"
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
fn test_eval_junction() {
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
    let filter = DefaultKernelPredicateEvaluator::from(UnimplementedColumnResolver);

    // Helper: evaluate a skipping predicate, treating None (can't create skipping predicate)
    // as NULL (unknown/can't skip) -- both mean "keep all files".
    let eval_skipping = |pred: &Pred| -> Option<bool> {
        let skipping_pred = as_data_skipping_predicate(pred)?;
        filter.eval(&skipping_pred)
    };

    for (inputs, expect_and, expect_or) in test_cases {
        let inputs: Vec<_> = inputs
            .iter()
            .map(|val| match val {
                Some(v) => Pred::literal(*v),
                None => Pred::null_literal(),
            })
            .collect();

        let pred = Pred::and_from(inputs.clone());
        expect_eq!(eval_skipping(&pred), *expect_and, "AND({inputs:?})");

        let pred = Pred::or_from(inputs.clone());
        expect_eq!(eval_skipping(&pred), *expect_or, "OR({inputs:?})");

        let pred = Pred::not(Pred::and_from(inputs.clone()));
        expect_eq!(
            eval_skipping(&pred),
            expect_and.map(|val| !val),
            "NOT AND({inputs:?})"
        );

        let pred = Pred::not(Pred::or_from(inputs.clone()));
        expect_eq!(
            eval_skipping(&pred),
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

    let predicates = [
        Pred::distinct(col.clone(), ten.clone()),
        Pred::not(Pred::distinct(col.clone(), ten.clone())),
        Pred::distinct(col.clone(), null.clone()),
        Pred::not(Pred::distinct(col.clone(), null.clone())),
    ];

    let do_test = |min: &Scalar, max: &Scalar, nullcount: i64, expected: &[Option<bool>]| {
        let resolver = HashMap::from_iter([
            (column_name!("stats_parsed.numRecords"), Scalar::from(2i64)),
            (
                column_name!("stats_parsed.nullCount.x"),
                Scalar::from(nullcount),
            ),
            (column_name!("stats_parsed.minValues.x"), min.clone()),
            (column_name!("stats_parsed.maxValues.x"), max.clone()),
        ]);
        let filter = DefaultKernelPredicateEvaluator::from(resolver);
        for (pred, expect) in predicates.iter().zip(expected) {
            let skipping_pred = as_data_skipping_predicate(pred).unwrap();
            expect_eq!(
                filter.eval(&skipping_pred),
                *expect,
                "{pred:#?} became {skipping_pred:#?} ({min}..{max}, {nullcount} nulls)"
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
    const NULL: Pred = Pred::null_literal();
    const FALSE: Pred = Pred::literal(false);
    const TRUE: Pred = Pred::literal(true);

    const ROWCOUNT: i64 = 2;
    const ALL_NULL: i64 = ROWCOUNT;
    const SOME_NULL: i64 = 1;
    const NO_NULL: i64 = 0;
    let do_test =
        |nulls: i64, pred: &Pred, missing: bool, expect: Option<bool>, expect_sql: Option<bool>| {
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
                    (
                        column_name!("stats_parsed.numRecords"),
                        Scalar::from(ROWCOUNT),
                    ),
                    (
                        column_name!("stats_parsed.nullCount.x"),
                        Scalar::from(nulls),
                    ),
                    (column_name!("stats_parsed.minValues.x"), min.clone()),
                    (column_name!("stats_parsed.maxValues.x"), max.clone()),
                ])
            };
            let filter = DefaultKernelPredicateEvaluator::from(resolver);
            let skipping_pred = as_data_skipping_predicate(pred).unwrap();
            expect_eq!(
                filter.eval(&skipping_pred),
                expect,
                "{pred:#?} became {skipping_pred:#?} ({min}..{max}, {nulls} nulls)"
            );
            let skipping_sql_pred =
                as_sql_data_skipping_predicate(pred, &Default::default()).unwrap();
            expect_eq!(
                filter.eval(&skipping_sql_pred),
                expect_sql,
                "{pred:#?} became {skipping_sql_pred:#?} ({min}..{max}, {nulls} nulls)"
            );
        };

    // Sanity tests -- only all-null columns should behave differently between normal and SQL WHERE.
    const MISSING: bool = true;
    const PRESENT: bool = false;
    let pred = &Pred::lt(TRUE, FALSE);
    do_test(ALL_NULL, pred, MISSING, Some(false), Some(false));

    let pred = &Pred::is_not_null(col.clone());
    do_test(ALL_NULL, pred, PRESENT, Some(false), Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);

    // SQL WHERE allows a present-but-all-null column to be pruned, but not a missing column.
    let pred = &Pred::lt(col.clone(), VAL);
    do_test(NO_NULL, pred, PRESENT, Some(true), Some(true));
    do_test(SOME_NULL, pred, PRESENT, Some(true), Some(true));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);

    // Comparison inside AND works
    let pred = &Pred::and(TRUE, Pred::lt(VAL, col.clone()));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);

    // NULL inside AND allows static skipping under SQL semantics
    let pred = &Pred::and(NULL, Pred::lt(col.clone(), VAL));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, Some(false));

    // Comparison inside AND inside AND works
    let pred = &Pred::and(TRUE, Pred::and(TRUE, Pred::lt(col.clone(), VAL)));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);

    // Comparison inside OR works
    let pred = &Pred::or(FALSE, Pred::lt(col.clone(), VAL));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);

    // Comparison inside AND inside OR works
    let pred = &Pred::or(FALSE, Pred::and(TRUE, Pred::lt(col.clone(), VAL)));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);
}

// TODO(#1002): we currently don't support file skipping on timestamp columns' max stat since they
// are truncated to milliseconds in add.stats.
#[test]
fn test_timestamp_skipping_disabled() {
    let empty = HashSet::new();
    let creator = DataSkippingPredicateCreator {
        partition_columns: &empty,
    };
    let col = &column_name!("timestamp_col");

    assert!(
        creator.get_min_stat(col, &DataType::TIMESTAMP).is_some(),
        "get_min_stat should return Some: allow data skipping on timestamp minValues"
    );
    assert_eq!(
        creator.get_max_stat(col, &DataType::TIMESTAMP),
        None,
        "get_max_stat should return None: no data skipping on timestamp maxValues"
    );
    assert!(
        creator
            .get_min_stat(col, &DataType::TIMESTAMP_NTZ)
            .is_some(),
        "get_min_stat should return Some: allow data skipping on timestamp_ntz minValues"
    );
    assert_eq!(
        creator.get_max_stat(col, &DataType::TIMESTAMP_NTZ),
        None,
        "get_max_stat should return None: no data skipping on timestamp_ntz maxValues"
    );
}

// Verifies the guarded checkpoint skipping predicate:
// - Prunes when stats are present and below threshold
// - Keeps when stats are present and above threshold
// - Conservatively keeps when stats are null (IS NULL guard fires)
#[rstest]
#[case::stats_below_threshold(Scalar::from(50), FALSE, "max=50, col>100 should skip")]
#[case::stats_above_threshold(Scalar::from(150), TRUE, "max=150, col>100 should keep")]
#[case::stats_null(
    Scalar::Null(DataType::INTEGER),
    TRUE,
    "null max should keep (IS NULL guard)"
)]
fn test_checkpoint_skipping_semantic(
    #[case] max_val: Scalar,
    #[case] expected: Option<bool>,
    #[case] description: &str,
) {
    let pred = Pred::gt(column_expr!("x"), Scalar::from(100));
    let skipping_pred = as_checkpoint_skipping_predicate(&pred, &[]).unwrap();
    let resolver = HashMap::from_iter([(column_name!("maxValues.x"), max_val)]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    expect_eq!(filter.eval(&skipping_pred), expected, "{description}");
}

// Verifies that the IS NULL guard changes behavior compared to a regular data skipping predicate:
// without the guard, null stats produce NULL (unknown); with the guard, they produce TRUE (keep).
#[test]
fn test_checkpoint_skipping_null_guard_vs_regular() {
    let pred = Pred::gt(column_expr!("x"), Scalar::from(100));
    let resolver =
        HashMap::from_iter([(column_name!("maxValues.x"), Scalar::Null(DataType::INTEGER))]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);

    let guarded = as_checkpoint_skipping_predicate(&pred, &[]).unwrap();
    expect_eq!(
        filter.eval(&guarded),
        TRUE,
        "guarded pred with null stats → TRUE (keep)"
    );

    let regular = as_data_skipping_predicate(&pred).unwrap();
    expect_eq!(
        filter.eval(&regular),
        NULL,
        "regular pred with null stats → NULL (unknown)"
    );
}

// Verifies that a conjunction can still prune when one column has null stats but the other
// column's stats are sufficient. For `col_a > 100 AND col_b < 50`, the guarded predicate is:
//
//   AND(
//     OR(maxValues.col_a IS NULL, maxValues.col_a > 100),
//     OR(minValues.col_b IS NULL, minValues.col_b < 50)
//   )
//
// Even if col_a's stats are null, col_b's stats alone can prune the row group.
#[test]
fn test_checkpoint_skipping_conjunction_partial_null_stats() {
    let pred = Pred::and(
        Pred::gt(column_expr!("col_a"), Scalar::from(100)),
        Pred::lt(column_expr!("col_b"), Scalar::from(50)),
    );
    let skipping_pred = as_checkpoint_skipping_predicate(&pred, &[]).unwrap();

    // Both stats present and both allow pruning → skip
    let resolver = HashMap::from_iter([
        (column_name!("maxValues.col_a"), Scalar::from(50)),
        (column_name!("minValues.col_b"), Scalar::from(60)),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    expect_eq!(
        filter.eval(&skipping_pred),
        FALSE,
        "both cols prunable → skip"
    );

    // col_a stats null, but col_b stats alone are enough to prune → still skip
    let resolver = HashMap::from_iter([
        (
            column_name!("maxValues.col_a"),
            Scalar::Null(DataType::INTEGER),
        ),
        (column_name!("minValues.col_b"), Scalar::from(60)),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    expect_eq!(
        filter.eval(&skipping_pred),
        FALSE,
        "col_a null but col_b prunable → still skip"
    );

    // col_a stats null and col_b doesn't allow pruning → keep
    let resolver = HashMap::from_iter([
        (
            column_name!("maxValues.col_a"),
            Scalar::Null(DataType::INTEGER),
        ),
        (column_name!("minValues.col_b"), Scalar::from(30)),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    expect_eq!(
        filter.eval(&skipping_pred),
        TRUE,
        "col_a null and col_b not prunable → keep"
    );
}

// TODO(#1002): we currently don't support file skipping on timestamp columns' max stat since they
// are truncated to milliseconds in add.stats.
#[test]
fn test_timestamp_predicates_dont_data_skip() {
    let col = &column_expr!("ts_col");
    for timestamp in [&Scalar::Timestamp(1000000), &Scalar::TimestampNtz(1000000)] {
        // LT will do minValues -> OK
        let pred = Pred::lt(col.clone(), timestamp.clone());
        let skipping_pred = as_data_skipping_predicate(&pred);
        assert_eq!(
            skipping_pred.unwrap().to_string(),
            "Column(stats_parsed.minValues.ts_col) < 1000000"
        );

        // GT will do maxValues -> BLOCKED
        let pred = Pred::gt(col.clone(), timestamp.clone());
        let skipping_pred = as_data_skipping_predicate(&pred);
        assert!(
            skipping_pred.is_none(),
            "Expected no data skipping for timestamp predicate: {pred:#?}, got {skipping_pred:#?}"
        );

        let pred = Pred::eq(col.clone(), timestamp.clone());
        let skipping_pred = as_data_skipping_predicate(&pred);
        assert_eq!(
            skipping_pred.unwrap().to_string(),
            "AND(NOT(Column(stats_parsed.minValues.ts_col) > 1000000), null)"
        );

        let pred = Pred::ne(col.clone(), timestamp.clone());
        let skipping_pred = as_data_skipping_predicate(&pred);
        assert_eq!(
            skipping_pred.unwrap().to_string(),
            "OR(NOT(Column(stats_parsed.minValues.ts_col) = 1000000), null)"
        );
    }
}

// Tests for partition-aware data skipping

/// Helper to build a partition columns set with a single "part_col" entry.
fn test_partition_columns() -> HashSet<String> {
    ["part_col".to_string()].into()
}

/// Helper to build a resolver for mixed partition + data stats evaluation.
fn mixed_resolver(
    part_val: &str,
    max_data: i32,
) -> DefaultKernelPredicateEvaluator<HashMap<ColumnName, Scalar>> {
    DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_col"),
            Scalar::from(part_val),
        ),
        (
            column_name!("stats_parsed.maxValues.data_col"),
            Scalar::from(max_data),
        ),
    ]))
}

#[test]
fn test_partition_column_rewrite() {
    let partition_columns = test_partition_columns();

    // Partition column equality rewrites to partitionValues (not minValues/maxValues)
    let pred = Pred::eq(column_expr!("part_col"), Scalar::from("2025-01-01"));
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns);
    let pred_str = skipping_pred.as_ref().map(|p| p.to_string());
    assert!(
        pred_str
            .as_ref()
            .is_some_and(|s| s.contains("partitionValues_parsed.part_col")),
        "Expected partitionValues_parsed.part_col, got {pred_str:?}"
    );
    assert!(
        pred_str
            .as_ref()
            .is_some_and(|s| !s.contains("minValues") && !s.contains("maxValues")),
        "Should not contain minValues/maxValues for partition columns"
    );

    // Data column still rewrites to stats_parsed.minValues/maxValues
    let pred = Pred::gt(column_expr!("data_col"), Scalar::from(100));
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns);
    let pred_str = skipping_pred.as_ref().map(|p| p.to_string());
    assert!(
        pred_str
            .as_ref()
            .is_some_and(|s| s.contains("stats_parsed.maxValues.data_col")),
        "Expected stats_parsed.maxValues.data_col for data column, got {pred_str:?}"
    );
}

#[rstest]
#[case::is_null(
    Pred::is_null(column_expr!("part_col")),
    "Column(partitionValues_parsed.part_col) IS NULL"
)]
#[case::is_not_null(
    Pred::is_not_null(column_expr!("part_col")),
    "NOT(Column(partitionValues_parsed.part_col) IS NULL)"
)]
fn test_partition_column_is_null(#[case] pred: Pred, #[case] expected: &str) {
    let partition_columns = test_partition_columns();
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns);
    assert_eq!(
        skipping_pred.as_ref().map(|p| p.to_string()).as_deref(),
        Some(expected),
    );
}

#[test]
fn test_mixed_partition_and_data_or_predicate() {
    let partition_columns = test_partition_columns();

    // Mixed OR: partition_col = 'X' OR data_col > 100
    // This should produce a valid skipping predicate (not None) because both
    // operands are now eligible for data skipping.
    let pred = Pred::or(
        Pred::eq(column_expr!("part_col"), Scalar::from("X")),
        Pred::gt(column_expr!("data_col"), Scalar::from(100)),
    );
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns);
    assert!(
        skipping_pred.is_some(),
        "Mixed partition+data OR should produce a valid skipping predicate"
    );
    let pred_str = skipping_pred.as_ref().map(|p| p.to_string());
    assert!(
        pred_str
            .as_ref()
            .is_some_and(|s| s.contains("partitionValues_parsed.part_col")),
        "Should reference partitionValues for partition column"
    );
    assert!(
        pred_str
            .as_ref()
            .is_some_and(|s| s.contains("stats_parsed.maxValues.data_col")),
        "Should reference stats_parsed.maxValues for data column"
    );
}

#[rstest]
#[case::both_miss("Y", 50, FALSE)]
#[case::partition_match("X", 50, TRUE)]
#[case::data_match("Y", 200, TRUE)]
fn test_mixed_partition_and_data_or_evaluation(
    #[case] part_val: &str,
    #[case] max_data: i32,
    #[case] expected: Option<bool>,
) {
    let partition_columns = test_partition_columns();

    // WHERE part_col = 'X' OR data_col > 100
    let pred = Pred::or(
        Pred::eq(column_expr!("part_col"), Scalar::from("X")),
        Pred::gt(column_expr!("data_col"), Scalar::from(100)),
    );
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");

    let filter = mixed_resolver(part_val, max_data);
    assert_eq!(
        filter.eval(&skipping_pred),
        expected,
        "part_col='{part_val}' max(data_col)={max_data}"
    );
}

#[rstest]
#[case::both_match("X", 200, TRUE)]
#[case::partition_miss("Y", 200, FALSE)]
#[case::data_miss("X", 50, FALSE)]
#[case::both_miss("Y", 50, FALSE)]
fn test_mixed_partition_and_data_and_evaluation(
    #[case] part_val: &str,
    #[case] max_data: i32,
    #[case] expected: Option<bool>,
) {
    let partition_columns = test_partition_columns();

    // WHERE part_col = 'X' AND data_col > 100
    let pred = Pred::and(
        Pred::eq(column_expr!("part_col"), Scalar::from("X")),
        Pred::gt(column_expr!("data_col"), Scalar::from(100)),
    );
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");

    let filter = mixed_resolver(part_val, max_data);
    assert_eq!(
        filter.eval(&skipping_pred),
        expected,
        "part_col='{part_val}' max(data_col)={max_data}"
    );
}

#[test]
fn test_partition_column_comparison_uses_exact_value() {
    let partition_columns = test_partition_columns();

    // part_col > 'B' rewrites both min and max to partitionValues_parsed.part_col
    let pred = Pred::gt(column_expr!("part_col"), Scalar::from("B"));
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");

    // part_col='A': 'A' > 'B' is false -> skip
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([(
        column_name!("partitionValues_parsed.part_col"),
        Scalar::from("A"),
    )]));
    assert_eq!(resolver.eval(&skipping_pred), FALSE);

    // part_col='C': 'C' > 'B' is true -> keep
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([(
        column_name!("partitionValues_parsed.part_col"),
        Scalar::from("C"),
    )]));
    assert_eq!(resolver.eval(&skipping_pred), TRUE);
}

#[test]
fn test_partition_only_predicate() {
    let partition_columns = test_partition_columns();

    // Partition-only: no data columns involved
    let pred = Pred::eq(column_expr!("part_col"), Scalar::from("X"));
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");
    let pred_str = skipping_pred.to_string();
    assert!(
        pred_str.contains("partitionValues_parsed.part_col"),
        "Should reference partitionValues_parsed"
    );
    assert!(
        !pred_str.contains("stats_parsed"),
        "Partition-only predicate should not reference stats_parsed"
    );
}

#[test]
fn test_sql_where_partition_rewrite() {
    let partition_columns = test_partition_columns();

    // Partition column equality: SQL WHERE should rewrite to partitionValues_parsed
    let pred = Pred::eq(column_expr!("part_col"), Scalar::from("X"));
    let sql_pred = as_sql_data_skipping_predicate(&pred, &partition_columns)
        .expect("partition eq should produce SQL skipping pred");
    let pred_str = sql_pred.to_string();
    assert!(
        pred_str.contains("partitionValues_parsed.part_col"),
        "SQL WHERE should reference partitionValues_parsed, got {pred_str}"
    );
}

#[rstest]
#[case::partition_match_data_above("X", 200, TRUE)]
#[case::partition_miss_data_above("Y", 200, FALSE)]
#[case::partition_match_data_below("X", 50, FALSE)]
#[case::both_miss("Y", 50, FALSE)]
fn test_sql_where_mixed_partition_and_data_evaluation(
    #[case] part_val: &str,
    #[case] max_data: i32,
    #[case] expected: Option<bool>,
) {
    let partition_columns = test_partition_columns();

    // WHERE part_col = 'X' AND data_col > 100
    let pred = Pred::and(
        Pred::eq(column_expr!("part_col"), Scalar::from("X")),
        Pred::gt(column_expr!("data_col"), Scalar::from(100)),
    );
    let sql_pred = as_sql_data_skipping_predicate(&pred, &partition_columns)
        .expect("mixed AND should produce SQL skipping pred");

    let resolver = HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_col"),
            Scalar::from(part_val),
        ),
        (column_name!("stats_parsed.numRecords"), Scalar::from(2i64)),
        (
            column_name!("stats_parsed.nullCount.data_col"),
            Scalar::from(0i64),
        ),
        (
            column_name!("stats_parsed.maxValues.data_col"),
            Scalar::from(max_data),
        ),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    assert_eq!(
        filter.eval(&sql_pred),
        expected,
        "part_col='{part_val}' max(data_col)={max_data}"
    );
}

/// Tests checkpoint skipping with unsupported predicate arms (timestamp max stats, partition
/// columns). Unsupported arms are replaced with TRUE, which is conservative: AND(TRUE, P) = P
/// (doesn't block pruning), OR(TRUE, P) = TRUE (keeps the row group).
/// Bare unsupported predicates (not in a junction) return None.
#[rstest]
// Bare unsupported predicate -> None (no junction to substitute TRUE into)
#[case::bare_timestamp_gt(
    Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
    &[],
    true,
)]
// All-unsupported junctions -> Some (all arms become TRUE)
#[case::and_all_unsupported_timestamp(
    Pred::and(
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(5_000_000)),
    ),
    &[],
    false,
)]
#[case::and_all_unsupported_timestamp_ntz(
    Pred::and(
        Pred::gt(column_expr!("ts_col"), Scalar::TimestampNtz(2_000_000)),
        Pred::gt(column_expr!("ts_col"), Scalar::TimestampNtz(5_000_000)),
    ),
    &[],
    false,
)]
#[case::or_all_unsupported_timestamp(
    Pred::or(
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(5_000_000)),
    ),
    &[],
    false,
)]
// Mixed AND: unsupported arm becomes TRUE, supported arm retained
#[case::and_supported_then_unsupported(
    Pred::and(
        Pred::gt(column_expr!("id"), Scalar::from(100i64)),
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
    ),
    &[],
    false,
)]
#[case::and_unsupported_then_supported(
    Pred::and(
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        Pred::gt(column_expr!("id"), Scalar::from(100i64)),
    ),
    &[],
    false,
)]
#[case::and_multiple_supported_one_unsupported(
    Pred::and(
        Pred::and(
            Pred::gt(column_expr!("id"), Scalar::from(100i64)),
            Pred::lt(column_expr!("id"), Scalar::from(500i64)),
        ),
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
    ),
    &[],
    false,
)]
// Mixed OR: unsupported arm becomes TRUE -> OR(supported, TRUE) = TRUE (conservative keep)
#[case::or_supported_and_unsupported(
    Pred::or(
        Pred::gt(column_expr!("id"), Scalar::from(100i64)),
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
    ),
    &[],
    false,
)]
#[case::or_unsupported_then_supported(
    Pred::or(
        Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        Pred::gt(column_expr!("id"), Scalar::from(100i64)),
    ),
    &[],
    false,
)]
// NOT(junction) via De Morgan's law with partition columns (always unsupported)
#[case::not_and_mixed_becomes_or_with_true(
    // NOT(AND(supported, partition)) -> effective OR(NOT(supported), TRUE)
    Pred::not(Pred::and(
        Pred::gt(column_expr!("id"), Scalar::from(100i64)),
        Pred::gt(column_expr!("part_col"), Scalar::from(42i64)),
    )),
    &["part_col"],
    false,
)]
#[case::not_or_mixed_becomes_and_with_true(
    // NOT(OR(supported, partition)) -> effective AND(NOT(supported), TRUE)
    Pred::not(Pred::or(
        Pred::gt(column_expr!("id"), Scalar::from(100i64)),
        Pred::gt(column_expr!("part_col"), Scalar::from(42i64)),
    )),
    &["part_col"],
    false,
)]
#[case::not_and_all_partition(
    // NOT(AND(partition, partition)) -> effective OR(TRUE, TRUE)
    Pred::not(Pred::and(
        Pred::gt(column_expr!("part_col"), Scalar::from(1i64)),
        Pred::gt(column_expr!("part_col"), Scalar::from(2i64)),
    )),
    &["part_col"],
    false,
)]
#[case::not_or_all_partition(
    // NOT(OR(partition, partition)) -> effective AND(TRUE, TRUE)
    Pred::not(Pred::or(
        Pred::gt(column_expr!("part_col"), Scalar::from(1i64)),
        Pred::gt(column_expr!("part_col"), Scalar::from(2i64)),
    )),
    &["part_col"],
    false,
)]
fn test_checkpoint_skipping_unsupported_predicate(
    #[case] pred: Pred,
    #[case] partition_columns: &[&str],
    #[case] expect_none: bool,
) {
    let partition_columns: Vec<String> =
        partition_columns.iter().map(|s| s.to_string()).collect();
    let result = as_checkpoint_skipping_predicate(&pred, &partition_columns);
    if expect_none {
        assert!(
            result.is_none(),
            "expected None for unsupported predicate: {pred:#?}"
        );
    } else {
        assert!(
            result.is_some(),
            "expected Some for predicate: {pred:#?}"
        );
    }
}

#[test]
fn test_checkpoint_skipping_deeply_nested_mixed() {
    // AND(AND(id > 100, ts > ...), id < 500): inner AND drops ts, outer keeps both.
    let pred = Pred::and(
        Pred::and(
            Pred::gt(column_expr!("id"), Scalar::from(100i64)),
            Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        ),
        Pred::lt(column_expr!("id"), Scalar::from(500i64)),
    );
    assert!(as_checkpoint_skipping_predicate(&pred, &[]).is_some());

    // OR(AND(id > 100, ts > ...), id < 500): inner AND drops ts -> AND(id > 100),
    // outer OR both arms supported -> keeps OR.
    let pred = Pred::or(
        Pred::and(
            Pred::gt(column_expr!("id"), Scalar::from(100i64)),
            Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        ),
        Pred::lt(column_expr!("id"), Scalar::from(500i64)),
    );
    assert!(as_checkpoint_skipping_predicate(&pred, &[]).is_some());

    // AND(OR(id > 100, ts > ...), id < 500): inner OR has unsupported arm -> None,
    // outer AND drops it -> AND(id < 500).
    let pred = Pred::and(
        Pred::or(
            Pred::gt(column_expr!("id"), Scalar::from(100i64)),
            Pred::gt(column_expr!("ts_col"), Scalar::Timestamp(2_000_000)),
        ),
        Pred::lt(column_expr!("id"), Scalar::from(500i64)),
    );
    assert!(as_checkpoint_skipping_predicate(&pred, &[]).is_some());
}

// Without normalization, `AND([unknown])` would become `AND([NULL])` via
// `collect_junction_preds`, which evaluates to `Some(false)` under `eval_sql_where` and
// incorrectly prunes all row groups. The junction constructor normalizes `AND([unknown])`
// to just `unknown`, which correctly returns `None` (no pushdown).
#[test]
fn single_unsupported_pred_in_junction_disables_checkpoint_pushdown() {
    let pred = Pred::and_from([Pred::unknown("unsupported")]);
    let skipping_pred = as_checkpoint_skipping_predicate(&pred, &[]);
    assert!(
        skipping_pred.is_none(),
        "Single unsupported predicate in a junction should disable pushdown, got: {skipping_pred:?}"
    );
}
