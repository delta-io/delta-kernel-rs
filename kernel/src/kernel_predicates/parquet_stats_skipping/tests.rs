use super::*;
use crate::expressions::{column_expr, Expression as Expr, Predicate as Pred};
use crate::DataType;
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

struct UnimplementedTestFilter;
impl ParquetStatsProvider for UnimplementedTestFilter {
    fn get_parquet_min_stat(&self, _col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_parquet_max_stat(&self, _col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_parquet_nullcount_stat(&self, _col: &ColumnName) -> Option<i64> {
        unimplemented!()
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        unimplemented!()
    }
}

/// Tests apply_junction and apply_scalar
#[test]
fn test_junctions() {
    use JunctionPredicateOp::*;

    let test_cases = &[
        // Every combo of 0, 1 and 2 inputs
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

    let filter = UnimplementedTestFilter;
    for (inputs, expect_and, expect_or) in test_cases {
        let inputs: Vec<_> = inputs
            .iter()
            .map(|val| match val {
                Some(v) => Pred::literal(*v),
                None => Pred::null_literal(),
            })
            .collect();

        expect_eq!(
            filter.eval_pred_junction(And, &inputs, false),
            *expect_and,
            "AND({inputs:?})"
        );
        expect_eq!(
            filter.eval_pred_junction(Or, &inputs, false),
            *expect_or,
            "OR({inputs:?})"
        );
        expect_eq!(
            filter.eval_pred_junction(And, &inputs, true),
            expect_and.map(|val| !val),
            "NOT(AND({inputs:?}))"
        );
        expect_eq!(
            filter.eval_pred_junction(Or, &inputs, true),
            expect_or.map(|val| !val),
            "NOT(OR({inputs:?}))"
        );
    }
}

struct MinMaxTestFilter {
    min: Option<Scalar>,
    max: Option<Scalar>,
}
impl MinMaxTestFilter {
    fn new(min: Option<Scalar>, max: Option<Scalar>) -> Self {
        Self { min, max }
    }
    fn get_stat_value(stat: &Option<Scalar>, data_type: &DataType) -> Option<Scalar> {
        stat.as_ref()
            .filter(|v| v.data_type() == *data_type)
            .cloned()
    }
}
impl ParquetStatsProvider for MinMaxTestFilter {
    fn get_parquet_min_stat(&self, _col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        Self::get_stat_value(&self.min, data_type)
    }

    fn get_parquet_max_stat(&self, _col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        Self::get_stat_value(&self.max, data_type)
    }

    fn get_parquet_nullcount_stat(&self, _col: &ColumnName) -> Option<i64> {
        unimplemented!()
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        unimplemented!()
    }
}

#[test]
fn test_eval_binary_comparisons() {
    const FIVE: Scalar = Scalar::Integer(5);
    const TEN: Scalar = Scalar::Integer(10);
    const FIFTEEN: Scalar = Scalar::Integer(15);
    const NULL_VAL: Scalar = Scalar::Null(DataType::INTEGER);

    let predicates = [
        Pred::lt(column_expr!("x"), Expr::literal(10)),
        Pred::le(column_expr!("x"), Expr::literal(10)),
        Pred::eq(column_expr!("x"), Expr::literal(10)),
        Pred::ne(column_expr!("x"), Expr::literal(10)),
        Pred::gt(column_expr!("x"), Expr::literal(10)),
        Pred::ge(column_expr!("x"), Expr::literal(10)),
    ];

    let do_test = |min: Scalar, max: Scalar, expected: &[Option<bool>]| {
        let filter = MinMaxTestFilter::new(Some(min.clone()), Some(max.clone()));
        for (pred, expect) in predicates.iter().zip(expected.iter()) {
            expect_eq!(filter.eval(pred), *expect, "{pred:#?} with [{min}..{max}]");
        }
    };

    // value < min = max (15..15 = 10, 15..15 <= 10, etc)
    do_test(FIFTEEN, FIFTEEN, &[FALSE, FALSE, FALSE, TRUE, TRUE, TRUE]);

    // min = max = value (10..10 = 10, 10..10 <= 10, etc)
    //
    // NOTE: missing min or max stat produces NULL output if the expression needed it.
    do_test(TEN, TEN, &[FALSE, TRUE, TRUE, FALSE, FALSE, TRUE]);
    do_test(NULL_VAL, TEN, &[NULL, NULL, NULL, NULL, FALSE, TRUE]);
    do_test(TEN, NULL_VAL, &[FALSE, TRUE, NULL, NULL, NULL, NULL]);

    // min = max < value (5..5 = 10, 5..5 <= 10, etc)
    do_test(FIVE, FIVE, &[TRUE, TRUE, FALSE, TRUE, FALSE, FALSE]);

    // value = min < max (5..15 = 10, 5..15 <= 10, etc)
    do_test(TEN, FIFTEEN, &[FALSE, TRUE, TRUE, TRUE, TRUE, TRUE]);

    // min < value < max (5..15 = 10, 5..15 <= 10, etc)
    do_test(FIVE, FIFTEEN, &[TRUE, TRUE, TRUE, TRUE, TRUE, TRUE]);
}

struct NullCountTestFilter {
    nullcount: Option<i64>,
    rowcount: i64,
}
impl NullCountTestFilter {
    fn new(nullcount: Option<i64>, rowcount: i64) -> Self {
        Self {
            nullcount,
            rowcount,
        }
    }
}
impl ParquetStatsProvider for NullCountTestFilter {
    fn get_parquet_min_stat(&self, _col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_parquet_max_stat(&self, _col: &ColumnName, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_parquet_nullcount_stat(&self, _col: &ColumnName) -> Option<i64> {
        self.nullcount
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        self.rowcount
    }
}

#[test]
fn test_eval_is_null() {
    let expressions = [
        Pred::is_null(column_expr!("x")),
        Pred::is_not_null(column_expr!("x")),
    ];

    let do_test = |nullcount: i64, expected: &[Option<bool>]| {
        let filter = NullCountTestFilter::new(Some(nullcount), 2);
        for (expr, expect) in expressions.iter().zip(expected) {
            expect_eq!(filter.eval(expr), *expect, "{expr:#?} ({nullcount} nulls)");
        }
    };

    // no nulls
    do_test(0, &[FALSE, TRUE]);

    // some nulls
    do_test(1, &[TRUE, TRUE]);

    // all nulls
    do_test(2, &[TRUE, FALSE]);
}

// --- MetadataSkippingFilter tests ---

use crate::expressions::column_name;
use crate::MetadataStatResolver;
use std::collections::HashSet;

/// Builds a checkpoint resolver for tests.
fn checkpoint_resolver(
    predicate: &Predicate,
    partition_columns: &HashSet<ColumnName>,
) -> MetadataStatResolver {
    MetadataStatResolver::for_checkpoint(predicate, partition_columns)
}

/// A mock stats provider keyed by column name, with per-column nullcounts.
struct MockStatsProvider {
    stats: HashMap<ColumnName, (Option<Scalar>, Option<Scalar>)>,
    nullcounts: HashMap<ColumnName, i64>,
    rowcount: i64,
}

impl MockStatsProvider {
    fn new() -> Self {
        Self {
            stats: HashMap::new(),
            nullcounts: HashMap::new(),
            rowcount: 100,
        }
    }

    fn with_stat(
        mut self,
        col: ColumnName,
        min: Option<Scalar>,
        max: Option<Scalar>,
        nullcount: i64,
    ) -> Self {
        self.stats.insert(col.clone(), (min, max));
        self.nullcounts.insert(col, nullcount);
        self
    }
}

impl ParquetStatsProvider for MockStatsProvider {
    fn get_parquet_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.stats
            .get(col)?
            .0
            .as_ref()
            .filter(|s| s.data_type() == *data_type)
            .cloned()
    }

    fn get_parquet_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.stats
            .get(col)?
            .1
            .as_ref()
            .filter(|s| s.data_type() == *data_type)
            .cloned()
    }

    fn get_parquet_nullcount_stat(&self, col: &ColumnName) -> Option<i64> {
        self.nullcounts.get(col).copied()
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        self.rowcount
    }
}

#[test]
fn test_metadata_filter_data_column_with_zero_nullcount() {
    // When the nested stat column has nullcount=0, we trust the stat values.
    let inner = MockStatsProvider::new()
        .with_stat(
            column_name!("add.stats_parsed.minValues.id"),
            Some(Scalar::from(1i64)),
            Some(Scalar::from(1i64)),
            0,
        )
        .with_stat(
            column_name!("add.stats_parsed.maxValues.id"),
            Some(Scalar::from(500i64)),
            Some(Scalar::from(500i64)),
            0,
        );

    let pred = Pred::gt(column_expr!("id"), Expr::literal(1000i64));
    let partition_cols = HashSet::new();
    let resolver = checkpoint_resolver(&pred, &partition_cols);
    // max=500, predicate is id > 1000 → should prune (return false)
    assert!(!MetadataSkippingFilter::apply(inner, &pred, &resolver));
}

#[test]
fn test_metadata_filter_data_column_with_nonzero_nullcount() {
    // When the nested stat column has nullcount > 0, stats are unreliable → conservatively keep.
    let inner = MockStatsProvider::new()
        .with_stat(
            column_name!("add.stats_parsed.minValues.id"),
            Some(Scalar::from(1i64)),
            Some(Scalar::from(1i64)),
            1, // Non-zero nullcount: some files have missing stats
        )
        .with_stat(
            column_name!("add.stats_parsed.maxValues.id"),
            Some(Scalar::from(500i64)),
            Some(Scalar::from(500i64)),
            0,
        );

    let pred = Pred::lt(column_expr!("id"), Expr::literal(0i64));
    let partition_cols = HashSet::new();
    let resolver = checkpoint_resolver(&pred, &partition_cols);
    // min has non-zero nullcount → min stat not trusted → can't prune
    assert!(MetadataSkippingFilter::apply(inner, &pred, &resolver));
}

#[test]
fn test_metadata_filter_partition_column_pruning() {
    // Partition columns resolve to add.partitionValues_parsed.<logical_name>.
    // Both min and max come from the same partition value column.
    let inner = MockStatsProvider::new().with_stat(
        column_name!("add.partitionValues_parsed.region"),
        Some(Scalar::from("us-east")),
        Some(Scalar::from("us-east")),
        0,
    );

    let partition_cols = HashSet::from([column_name!("region")]);

    // Predicate: region = "eu-west" — all partition values are "us-east" → should prune
    let pred = Pred::eq(column_expr!("region"), Expr::literal("eu-west"));
    let resolver = checkpoint_resolver(&pred, &partition_cols);
    assert!(!MetadataSkippingFilter::apply(inner, &pred, &resolver));
}

#[test]
fn test_metadata_filter_partition_column_keeps_matching() {
    let inner = MockStatsProvider::new().with_stat(
        column_name!("add.partitionValues_parsed.region"),
        Some(Scalar::from("us-east")),
        Some(Scalar::from("us-east")),
        0,
    );

    let partition_cols = HashSet::from([column_name!("region")]);

    // Predicate: region = "us-east" — matches → should keep
    let pred = Pred::eq(column_expr!("region"), Expr::literal("us-east"));
    let resolver = checkpoint_resolver(&pred, &partition_cols);
    assert!(MetadataSkippingFilter::apply(inner, &pred, &resolver));
}

#[test]
fn test_metadata_filter_partition_nullcount_always_zero() {
    // Partition values are never null, so IS NULL should be false and IS NOT NULL should be true.
    let inner = MockStatsProvider::new().with_stat(
        column_name!("add.partitionValues_parsed.region"),
        Some(Scalar::from("us-east")),
        Some(Scalar::from("us-east")),
        0,
    );

    let partition_cols = HashSet::from([column_name!("region")]);
    // Use a dummy predicate that references the partition column
    let dummy_pred = Pred::is_null(column_expr!("region"));
    let resolver = checkpoint_resolver(&dummy_pred, &partition_cols);

    let filter = MetadataSkippingFilter::new(inner, &resolver);

    // IS NULL on a partition column → false (partition values are never null)
    expect_eq!(
        filter.eval(&Pred::is_null(column_expr!("region"))),
        FALSE,
        "IS NULL on partition column"
    );
    // IS NOT NULL on a partition column → true
    expect_eq!(
        filter.eval(&Pred::is_not_null(column_expr!("region"))),
        TRUE,
        "IS NOT NULL on partition column"
    );
}

#[test]
fn test_metadata_filter_partition_with_column_mapping() {
    // With column mapping, the predicate uses the physical name and partitionValues_parsed
    // also stores values under the physical name.
    let inner = MockStatsProvider::new().with_stat(
        column_name!("add.partitionValues_parsed.col_abc123"),
        Some(Scalar::from("us-east")),
        Some(Scalar::from("us-east")),
        0,
    );

    let partition_cols = HashSet::from([column_name!("col_abc123")]);

    let pred = Pred::eq(column_expr!("col_abc123"), Expr::literal("eu-west"));
    let resolver = checkpoint_resolver(&pred, &partition_cols);
    assert!(!MetadataSkippingFilter::apply(inner, &pred, &resolver));
}

#[test]
fn test_metadata_filter_mixed_partition_and_data_columns() {
    // Conjunction: partition column AND data column.
    let inner = MockStatsProvider::new()
        .with_stat(
            column_name!("add.partitionValues_parsed.region"),
            Some(Scalar::from("us-east")),
            Some(Scalar::from("us-east")),
            0,
        )
        .with_stat(
            column_name!("add.stats_parsed.minValues.id"),
            Some(Scalar::from(1i64)),
            Some(Scalar::from(1i64)),
            0,
        )
        .with_stat(
            column_name!("add.stats_parsed.maxValues.id"),
            Some(Scalar::from(500i64)),
            Some(Scalar::from(500i64)),
            0,
        );

    let partition_cols = HashSet::from([column_name!("region")]);

    // region = "us-east" AND id > 1000 → partition matches but data doesn't → prune
    let pred = Pred::and(
        Pred::eq(column_expr!("region"), Expr::literal("us-east")),
        Pred::gt(column_expr!("id"), Expr::literal(1000i64)),
    );
    let resolver = checkpoint_resolver(&pred, &partition_cols);
    assert!(!MetadataSkippingFilter::apply(inner, &pred, &resolver));
}
