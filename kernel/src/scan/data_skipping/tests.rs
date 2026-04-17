use std::collections::HashMap;

use rstest::rstest;

use super::*;
use crate::expressions::column_name;
use crate::kernel_predicates::{DefaultKernelPredicateEvaluator, UnimplementedColumnResolver};

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

    // NULL literal is treated as unknown (not false) under eval_sql_where, so it does not
    // force static skipping on its own. With present-but-all-null stats, the comparison arm
    // still evaluates to false (null-safe check fails), so AND(unknown, false) = false.
    // With missing stats, both arms are unknown, so AND(unknown, unknown) = unknown.
    let pred = &Pred::and(NULL, Pred::lt(col.clone(), VAL));
    do_test(ALL_NULL, pred, PRESENT, None, Some(false));
    do_test(ALL_NULL, pred, MISSING, None, None);

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

#[test]
fn test_timestamp_stats_enabled() {
    let empty = HashSet::new();
    let creator = DataSkippingPredicateCreator {
        partition_columns: &empty,
    };
    let col = &column_name!("timestamp_col");

    assert!(
        creator.get_min_stat(col, &DataType::TIMESTAMP).is_some(),
        "get_min_stat should return Some for timestamp minValues"
    );
    assert!(
        creator.get_max_stat(col, &DataType::TIMESTAMP).is_some(),
        "get_max_stat should return Some for timestamp maxValues"
    );
    assert!(
        creator
            .get_min_stat(col, &DataType::TIMESTAMP_NTZ)
            .is_some(),
        "get_min_stat should return Some for timestamp_ntz minValues"
    );
    assert!(
        creator
            .get_max_stat(col, &DataType::TIMESTAMP_NTZ)
            .is_some(),
        "get_max_stat should return Some for timestamp_ntz maxValues"
    );
}

#[test]
fn test_adjust_scalar_for_max_stat_truncation() {
    // Timestamp: subtracts 999us
    assert_eq!(
        adjust_scalar_for_max_stat_truncation(&Scalar::Timestamp(1_000_000)),
        Scalar::Timestamp(999_001)
    );
    // TimestampNtz: subtracts 999us
    assert_eq!(
        adjust_scalar_for_max_stat_truncation(&Scalar::TimestampNtz(1_000_000)),
        Scalar::TimestampNtz(999_001)
    );
    // Non-timestamp: unchanged
    assert_eq!(
        adjust_scalar_for_max_stat_truncation(&Scalar::from(42i64)),
        Scalar::from(42i64)
    );
    // Saturating at i64::MIN
    assert_eq!(
        adjust_scalar_for_max_stat_truncation(&Scalar::Timestamp(i64::MIN)),
        Scalar::Timestamp(i64::MIN)
    );
    // Near-zero: goes negative
    assert_eq!(
        adjust_scalar_for_max_stat_truncation(&Scalar::Timestamp(500)),
        Scalar::Timestamp(-499)
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
        "guarded pred with null stats -> TRUE (keep)"
    );

    let regular = as_data_skipping_predicate(&pred).unwrap();
    expect_eq!(
        filter.eval(&regular),
        NULL,
        "regular pred with null stats -> NULL (unknown)"
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

    // Both stats present and both allow pruning -> skip
    let resolver = HashMap::from_iter([
        (column_name!("maxValues.col_a"), Scalar::from(50)),
        (column_name!("minValues.col_b"), Scalar::from(60)),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    expect_eq!(
        filter.eval(&skipping_pred),
        FALSE,
        "both cols prunable -> skip"
    );

    // col_a stats null, but col_b stats alone are enough to prune -> still skip
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
        "col_a null but col_b prunable -> still skip"
    );

    // col_a stats null and col_b doesn't allow pruning -> keep
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
        "col_a null and col_b not prunable -> keep"
    );
}

// Verifies the null-guarded checkpoint skipping path also applies the 999us timestamp
// truncation adjustment to max stat comparisons.
#[rstest]
fn test_checkpoint_skipping_timestamp_adjustment(
    #[values(Scalar::Timestamp(1_000_000), Scalar::TimestampNtz(1_000_000))] timestamp: Scalar,
) {
    let col = &column_expr!("ts_col");

    // GT: should produce OR(maxValues.ts_col IS NULL, maxValues.ts_col > 999001)
    let pred = Pred::gt(col.clone(), timestamp.clone());
    let skipping_pred = as_checkpoint_skipping_predicate(&pred, &[]).unwrap();
    assert_eq!(
        skipping_pred.to_string(),
        "OR(Column(maxValues.ts_col) IS NULL, Column(maxValues.ts_col) > 999001)"
    );

    // EQ: max stat leg should use adjusted literal
    let pred = Pred::eq(col.clone(), timestamp.clone());
    let skipping_pred = as_checkpoint_skipping_predicate(&pred, &[]).unwrap();
    assert_eq!(
        skipping_pred.to_string(),
        "AND(OR(Column(minValues.ts_col) IS NULL, NOT(Column(minValues.ts_col) > 1000000)), \
         OR(Column(maxValues.ts_col) IS NULL, NOT(Column(maxValues.ts_col) < 999001)))"
    );
}

// Timestamp predicates use max stats with a 999us adjustment to account for millisecond
// truncation in Delta JSON stats.
#[rstest]
fn test_timestamp_predicates_use_adjusted_max_stats(
    #[values(Scalar::Timestamp(1_000_000), Scalar::TimestampNtz(1_000_000))] timestamp: Scalar,
) {
    let col = &column_expr!("ts_col");

    // LT uses minValues (no adjustment needed for min stats)
    let pred = Pred::lt(col.clone(), timestamp.clone());
    assert_eq!(
        as_data_skipping_predicate(&pred).unwrap().to_string(),
        "Column(stats_parsed.minValues.ts_col) < 1000000"
    );

    // GT uses maxValues with adjusted literal (1000000 - 999 = 999001)
    let pred = Pred::gt(col.clone(), timestamp.clone());
    assert_eq!(
        as_data_skipping_predicate(&pred).unwrap().to_string(),
        "Column(stats_parsed.maxValues.ts_col) > 999001"
    );

    // EQ uses both min (unadjusted) and max (adjusted)
    let pred = Pred::eq(col.clone(), timestamp.clone());
    assert_eq!(
        as_data_skipping_predicate(&pred).unwrap().to_string(),
        "AND(NOT(Column(stats_parsed.minValues.ts_col) > 1000000), \
         NOT(Column(stats_parsed.maxValues.ts_col) < 999001))"
    );

    // NE uses both min (unadjusted) and max (adjusted)
    let pred = Pred::ne(col.clone(), timestamp.clone());
    assert_eq!(
        as_data_skipping_predicate(&pred).unwrap().to_string(),
        "OR(NOT(Column(stats_parsed.minValues.ts_col) = 1000000), \
         NOT(Column(stats_parsed.maxValues.ts_col) = 999001))"
    );

    // GE (col >= val) uses maxValues with adjusted literal
    let pred = Pred::ge(col.clone(), timestamp.clone());
    assert_eq!(
        as_data_skipping_predicate(&pred).unwrap().to_string(),
        "NOT(Column(stats_parsed.maxValues.ts_col) < 999001)"
    );

    // LE (col <= val) uses minValues only (no adjustment needed)
    let pred = Pred::le(col.clone(), timestamp.clone());
    assert_eq!(
        as_data_skipping_predicate(&pred).unwrap().to_string(),
        "NOT(Column(stats_parsed.minValues.ts_col) > 1000000)"
    );
}

// Partition timestamp columns use exact values (not truncated), so no adjustment is applied.
#[test]
fn test_partition_timestamp_column_no_adjustment() {
    let partition_columns: HashSet<String> = ["ts_part".to_string()].into();
    let pred = Pred::gt(column_expr!("ts_part"), Scalar::Timestamp(1_000_000));
    let skipping_pred =
        as_data_skipping_predicate_with_partitions(&pred, &partition_columns).unwrap();
    assert_eq!(
        skipping_pred.to_string(),
        "OR(NOT(Column(is_add)), Column(partitionValues_parsed.ts_part) > 1000000)"
    );
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
        (column_name!("is_add"), Scalar::from(true)),
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
    "OR(NOT(Column(is_add)), Column(partitionValues_parsed.part_col) IS NULL)"
)]
#[case::is_not_null(
    Pred::is_not_null(column_expr!("part_col")),
    "OR(NOT(Column(is_add)), NOT(Column(partitionValues_parsed.part_col) IS NULL))"
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
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_col"),
            Scalar::from("A"),
        ),
        (column_name!("is_add"), Scalar::from(true)),
    ]));
    assert_eq!(resolver.eval(&skipping_pred), FALSE);

    // part_col='C': 'C' > 'B' is true -> keep
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_col"),
            Scalar::from("C"),
        ),
        (column_name!("is_add"), Scalar::from(true)),
    ]));
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
        (column_name!("is_add"), Scalar::from(true)),
    ]);
    let filter = DefaultKernelPredicateEvaluator::from(resolver);
    assert_eq!(
        filter.eval(&sql_pred),
        expected,
        "part_col='{part_val}' max(data_col)={max_data}"
    );
}

// The is_add guard (OR(NOT is_add, pred)) ensures Remove rows are never pruned by
// partition predicates, regardless of whether the partition value matches.
#[rstest]
#[case::non_matching_partition("Y", false, TRUE, "non-matching partition, Remove kept via guard")]
#[case::matching_partition("X", false, TRUE, "matching partition, Remove kept via guard")]
#[case::add_non_matching("Y", true, FALSE, "non-matching partition, Add correctly pruned")]
#[case::add_matching("X", true, TRUE, "matching partition, Add correctly kept")]
fn is_add_guard_keeps_remove_rows(
    #[case] part_val: &str,
    #[case] is_add: bool,
    #[case] expected: Option<bool>,
    #[case] _scenario: &str,
) {
    let partition_columns = test_partition_columns();
    let pred = Pred::eq(column_expr!("part_col"), Scalar::from("X"));
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");

    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_col"),
            Scalar::from(part_val),
        ),
        (column_name!("is_add"), Scalar::from(is_add)),
    ]));
    assert_eq!(
        resolver.eval(&skipping_pred),
        expected,
        "part_col='{part_val}' is_add={is_add}"
    );
}

// Mixed AND with is_add=false and null stats: Remove rows have null data stats, so the data
// arm evaluates to NULL. AND(true_from_guard, NULL) = NULL, which the DISTINCT filter treats
// as "keep". This verifies Removes are not pruned even when the data arm cannot be satisfied.
#[rstest]
#[case::remove_null_stats("Y", false, "Remove: AND(guard=true, stats=NULL) = NULL -> kept")]
#[case::add_null_stats_partition_match("X", true, "Add: AND(true, NULL) = NULL -> kept")]
#[case::add_null_stats_partition_miss("Y", true, "Add: AND(false, NULL) = false -> pruned")]
fn mixed_and_with_null_stats_and_is_add_guard(
    #[case] part_val: &str,
    #[case] is_add: bool,
    #[case] _scenario: &str,
) {
    let partition_columns = test_partition_columns();
    let pred = Pred::and(
        Pred::eq(column_expr!("part_col"), Scalar::from("X")),
        Pred::gt(column_expr!("data_col"), Scalar::from(100)),
    );
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");

    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_col"),
            Scalar::from(part_val),
        ),
        (
            column_name!("stats_parsed.maxValues.data_col"),
            Scalar::Null(DataType::INTEGER),
        ),
        (column_name!("is_add"), Scalar::from(is_add)),
    ]));
    let result = resolver.eval(&skipping_pred);
    if !is_add {
        assert_ne!(result, FALSE, "Remove rows must never be pruned");
    }
}

// Null partition values: IS NULL / IS NOT NULL predicates on partition columns must
// correctly evaluate against null values in partitionValues_parsed.
#[rstest]
#[case::is_null_with_null_value(
    Pred::is_null(column_expr!("part_col")),
    Scalar::Null(DataType::STRING),
    TRUE,
    "null partition value matches IS NULL"
)]
#[case::is_null_with_non_null_value(
    Pred::is_null(column_expr!("part_col")),
    Scalar::from("X"),
    FALSE,
    "non-null partition value rejected by IS NULL"
)]
#[case::is_not_null_with_null_value(
    Pred::is_not_null(column_expr!("part_col")),
    Scalar::Null(DataType::STRING),
    FALSE,
    "null partition value rejected by IS NOT NULL"
)]
#[case::is_not_null_with_non_null_value(
    Pred::is_not_null(column_expr!("part_col")),
    Scalar::from("X"),
    TRUE,
    "non-null partition value matches IS NOT NULL"
)]
fn null_partition_value_evaluation(
    #[case] pred: Pred,
    #[case] part_val: Scalar,
    #[case] expected: Option<bool>,
    #[case] _scenario: &str,
) {
    let partition_columns = test_partition_columns();
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");

    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (column_name!("partitionValues_parsed.part_col"), part_val),
        (column_name!("is_add"), Scalar::from(true)),
    ]));
    assert_eq!(resolver.eval(&skipping_pred), expected);
}

// Multiple partition columns: predicates referencing two partition columns should both
// rewrite to partitionValues_parsed and both get is_add guards.
#[test]
fn multiple_partition_columns_rewrite_and_evaluation() {
    let partition_columns: HashSet<String> =
        ["part_a", "part_b"].iter().map(|s| s.to_string()).collect();

    let pred = Pred::and(
        Pred::eq(column_expr!("part_a"), Scalar::from("X")),
        Pred::eq(column_expr!("part_b"), Scalar::from("Y")),
    );
    let skipping_pred = as_data_skipping_predicate_with_partitions(&pred, &partition_columns)
        .expect("should exist");
    let pred_str = skipping_pred.to_string();
    assert!(
        pred_str.contains("partitionValues_parsed.part_a"),
        "Should reference partitionValues_parsed.part_a, got {pred_str}"
    );
    assert!(
        pred_str.contains("partitionValues_parsed.part_b"),
        "Should reference partitionValues_parsed.part_b, got {pred_str}"
    );
    assert!(
        !pred_str.contains("stats_parsed"),
        "Should not reference stats_parsed for partition-only pred, got {pred_str}"
    );

    // Both match -> kept
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_a"),
            Scalar::from("X"),
        ),
        (
            column_name!("partitionValues_parsed.part_b"),
            Scalar::from("Y"),
        ),
        (column_name!("is_add"), Scalar::from(true)),
    ]));
    assert_eq!(resolver.eval(&skipping_pred), TRUE);

    // First misses -> pruned
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_a"),
            Scalar::from("Z"),
        ),
        (
            column_name!("partitionValues_parsed.part_b"),
            Scalar::from("Y"),
        ),
        (column_name!("is_add"), Scalar::from(true)),
    ]));
    assert_eq!(resolver.eval(&skipping_pred), FALSE);

    // Remove row: both miss but is_add=false -> kept via guard
    let resolver = DefaultKernelPredicateEvaluator::from(HashMap::from_iter([
        (
            column_name!("partitionValues_parsed.part_a"),
            Scalar::from("Z"),
        ),
        (
            column_name!("partitionValues_parsed.part_b"),
            Scalar::from("W"),
        ),
        (column_name!("is_add"), Scalar::from(false)),
    ]));
    assert_ne!(
        resolver.eval(&skipping_pred),
        FALSE,
        "Remove must not be pruned"
    );
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

// -- Integration tests: end-to-end data skipping with real tables -------------------
//
// Two test tables are used:
//
// `app-txn-checkpoint` (4 files, partitioned by `modified` (string)):
//   - 2 files: modified="2021-02-01", value in [4, 11]
//   - 2 files: modified="2021-02-02", value in [1, 3]
//   - Version 0 (JSON) + version 1 (JSON + checkpoint) exercises both code paths.
//
// `parsed-stats` (6 files, non-partitioned):
//   - File 1-6: id ranges [1,100]..[501,600], ts_col min values 1M..11M microseconds
//   - Version 3 checkpoint + versions 4-5 JSON commits.

use std::path::PathBuf;

use crate::engine::sync::SyncEngine;
use crate::Snapshot;

/// Counts files selected after data skipping for the given predicate and table.
fn count_selected(table_dir: &str, pred: PredicateRef) -> usize {
    let path = std::fs::canonicalize(PathBuf::from(table_dir)).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());
    let scan = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .unwrap()
        .scan_builder()
        .with_predicate(pred)
        .build()
        .unwrap();
    scan.scan_metadata(engine.as_ref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .iter()
        .flat_map(|sm| sm.scan_files.selection_vector())
        .filter(|&&s| s)
        .count()
}

const PARTITIONED_TABLE: &str = "./tests/data/app-txn-checkpoint/";
const STATS_TABLE: &str = "./tests/data/parsed-stats/";

// -- Partition-only predicates (app-txn-checkpoint) ---------------------------

#[rstest]
#[case::eq_match(Pred::eq(column_expr!("modified"), Expr::literal("2021-02-01")), 2)]
#[case::eq_no_match(Pred::eq(column_expr!("modified"), Expr::literal("2099-01-01")), 0)]
#[case::neq(Pred::ne(column_expr!("modified"), Expr::literal("2021-02-01")), 2)]
#[case::gt(Pred::gt(column_expr!("modified"), Expr::literal("2021-02-01")), 2)]
#[case::lt(Pred::lt(column_expr!("modified"), Expr::literal("2021-02-02")), 2)]
#[case::gte_all(Pred::ge(column_expr!("modified"), Expr::literal("2021-02-01")), 4)]
#[case::lte_all(Pred::le(column_expr!("modified"), Expr::literal("2021-02-02")), 4)]
#[case::range_anded(
    Pred::and(
        Pred::ge(column_expr!("modified"), Expr::literal("2021-02-01")),
        Pred::le(column_expr!("modified"), Expr::literal("2021-02-01")),
    ),
    2
)]
fn partition_only_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(PARTITIONED_TABLE, Arc::new(pred)), expected);
}

// -- Data-stats-only predicates (app-txn-checkpoint) --------------------------

#[rstest]
#[case::gt_prunes_low(Pred::gt(column_expr!("value"), Expr::literal(9i32)), 2)]
#[case::lt_prunes_high(Pred::lt(column_expr!("value"), Expr::literal(4i32)), 2)]
#[case::gt_above_max(Pred::gt(column_expr!("value"), Expr::literal(11i32)), 0)]
#[case::le_at_max(Pred::le(column_expr!("value"), Expr::literal(11i32)), 4)]
#[case::range_anded(
    Pred::and(
        Pred::ge(column_expr!("value"), Expr::literal(1i32)),
        Pred::le(column_expr!("value"), Expr::literal(3i32)),
    ),
    2
)]
fn data_stats_only_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(PARTITIONED_TABLE, Arc::new(pred)), expected);
}

// -- Mixed AND: both partition and data conditions must hold -------------------

#[rstest]
#[case::partition_match_data_match(
    "2021-02-01",
    3i32,
    2,
    "partition prunes 02-02; data keeps 02-01 (max=11 > 3)"
)]
#[case::partition_match_data_miss(
    "2021-02-02",
    3i32,
    0,
    "partition keeps 02-02 but max=3 NOT >3; partition prunes 02-01"
)]
#[case::partition_miss("2099-01-01", 0i32, 0, "no files match partition")]
fn mixed_and_skipping(
    #[case] partition_val: &str,
    #[case] data_threshold: i32,
    #[case] expected: usize,
    #[case] _scenario: &str,
) {
    let pred = Arc::new(Pred::and(
        column_expr!("modified").eq(Expr::literal(partition_val)),
        column_expr!("value").gt(Expr::literal(data_threshold)),
    ));
    assert_eq!(count_selected(PARTITIONED_TABLE, pred), expected);
}

// -- Mixed OR: a file survives if either leg matches --------------------------

#[rstest]
#[case::both_match("2021-02-02", 9i32, 4, "02-02 matches partition; 02-01 has max=11 > 9")]
#[case::partition_saves_some(
    "2021-02-02",
    11i32,
    2,
    "02-02 matches partition; 02-01 max=11 NOT >11 -> pruned"
)]
#[case::data_saves_some(
    "2099-01-01", -1i32, 4,
    "no partition match; all files have max >= 0 so value > -1 keeps all"
)]
#[case::both_miss(
    "2099-01-01",
    11i32,
    0,
    "no partition match; max=11 NOT >11 -> all pruned"
)]
fn mixed_or_skipping(
    #[case] partition_val: &str,
    #[case] data_threshold: impl Into<Scalar>,
    #[case] expected: usize,
    #[case] _scenario: &str,
) {
    let pred = Arc::new(Pred::or(
        column_expr!("modified").eq(Expr::literal(partition_val)),
        column_expr!("value").gt(Expr::literal(data_threshold.into())),
    ));
    assert_eq!(count_selected(PARTITIONED_TABLE, pred), expected);
}

// -- Nested AND(partition, OR(data, data)) ------------------------------------

#[rstest]
#[case::loose_bound(10i32, 4, "max=11 > 10 keeps 02-01; min=1 < 2 keeps 02-02")]
#[case::strict_bound(11i32, 2, "max=11 NOT >11 prunes 02-01; min=1 < 2 keeps 02-02")]
fn nested_and_or_skipping(
    #[case] upper_bound: i32,
    #[case] expected: usize,
    #[case] _scenario: &str,
) {
    let pred = Arc::new(Pred::and(
        Pred::ge(column_expr!("modified"), Expr::literal("2021-02-01")),
        Pred::or(
            Pred::lt(column_expr!("value"), Expr::literal(2i32)),
            Pred::gt(column_expr!("value"), Expr::literal(upper_bound)),
        ),
    ));
    assert_eq!(count_selected(PARTITIONED_TABLE, pred), expected);
}

// -- Parsed stats skipping (non-partitioned table) ----------------------------

#[test]
fn parsed_stats_skipping() {
    // id > 400 should skip files 1-4 (max id: 100, 200, 300, 400) and keep files 5-6
    let pred = Arc::new(Pred::gt(column_expr!("id"), Expr::literal(400i64)));
    assert_eq!(count_selected(STATS_TABLE, pred), 2);
}

// -- Timestamp predicate skipping (parsed-stats table) ------------------------
// Timestamp predicates now use max stats with a 999us adjustment for truncation.
// Table has 6 files with ts_col ranges: [1M,2M], [3M,4M], [5M,6M], [7M,8M], [9M,10M], [11M,12M]

#[rstest]
#[case::bare_ts_gt_keeps_all(
    // ts_col > 2M -> adjusted: max > 1,999,001 -> all 6 files have max >= 2M -> 6
    Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
    6
)]
#[case::bare_ts_lt_skips(
    // ts_col < 3M -> min < 3M -> file 1 (min=1M) -> 1
    Pred::lt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(3_000_000))),
    1
)]
#[case::and_mixed_id_and_ts(
    // id > 400 keeps files 5-6; ts_col > 2M keeps all 6; AND -> 2
    Pred::and(
        Pred::gt(column_expr!("id"), Expr::literal(400i64)),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
    ),
    2
)]
#[case::or_mixed_id_and_ts(
    // id > 400 keeps 5-6; ts_col > 2M keeps 1-6; OR -> 6
    Pred::or(
        Pred::gt(column_expr!("id"), Expr::literal(400i64)),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
    ),
    6
)]
#[case::and_two_ts_predicates(
    // ts_col > 2M (adjusted max > 1,999,001 -> all) AND ts_col > 5M (adjusted max > 4,999,001
    // -> files 3-6) -> 4
    Pred::and(
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(5_000_000))),
    ),
    4
)]
#[case::or_two_ts_predicates(
    // ts_col > 2M keeps all; ts_col > 5M keeps files 3-6; OR -> 6
    Pred::or(
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(2_000_000))),
        Pred::gt(column_expr!("ts_col"), Expr::literal(Scalar::Timestamp(5_000_000))),
    ),
    6
)]
fn timestamp_predicate_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(STATS_TABLE, Arc::new(pred)), expected);
}

// -- Unsupported predicate handling (parsed-stats table) ----------------------
// Column-column comparisons are unsupported for data skipping (no literal to infer type).
// Verifies that junctions degrade gracefully when one or both legs can't be evaluated.

#[rstest]
#[case::bare_unsupported_returns_all(
    // col > col is unsupported -> None -> keep all files
    Pred::gt(column_expr!("id"), column_expr!("salary")),
    6
)]
#[case::and_supported_with_unsupported(
    // id > 400 keeps files 5-6; id > salary is unsupported; AND -> 2
    Pred::and(
        Pred::gt(column_expr!("id"), Expr::literal(400i64)),
        Pred::gt(column_expr!("id"), column_expr!("salary")),
    ),
    2
)]
#[case::or_supported_with_unsupported(
    // id > 400 keeps 5-6; id > salary is unsupported; OR -> all 6
    Pred::or(
        Pred::gt(column_expr!("id"), Expr::literal(400i64)),
        Pred::gt(column_expr!("id"), column_expr!("salary")),
    ),
    6
)]
#[case::and_all_unsupported(
    // Both legs unsupported -> None -> keep all 6
    Pred::and(
        Pred::gt(column_expr!("id"), column_expr!("salary")),
        Pred::gt(column_expr!("id"), column_expr!("age")),
    ),
    6
)]
#[case::or_all_unsupported(
    // Both legs unsupported -> None -> keep all 6
    Pred::or(
        Pred::gt(column_expr!("id"), column_expr!("salary")),
        Pred::gt(column_expr!("id"), column_expr!("age")),
    ),
    6
)]
fn unsupported_predicate_skipping(#[case] pred: Pred, #[case] expected: usize) {
    assert_eq!(count_selected(STATS_TABLE, Arc::new(pred)), expected);
}
