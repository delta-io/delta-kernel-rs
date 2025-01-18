use super::*;
use crate::expressions::{column_expr, Expression as Expr};
use crate::predicates::PredicateEvaluator as _;
use crate::DataType;

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

/// Tests apply_variadic and apply_scalar
#[test]
fn test_junctions() {
    use VariadicOperator::*;

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
                Some(v) => Expr::literal(v),
                None => Expr::null_literal(DataType::BOOLEAN),
            })
            .collect();

        expect_eq!(
            filter.eval_variadic(And, &inputs, false),
            *expect_and,
            "AND({inputs:?})"
        );
        expect_eq!(
            filter.eval_variadic(Or, &inputs, false),
            *expect_or,
            "OR({inputs:?})"
        );
        expect_eq!(
            filter.eval_variadic(And, &inputs, true),
            expect_and.map(|val| !val),
            "NOT(AND({inputs:?}))"
        );
        expect_eq!(
            filter.eval_variadic(Or, &inputs, true),
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

    let expressions = [
        Expr::lt(column_expr!("x"), 10),
        Expr::le(column_expr!("x"), 10),
        Expr::eq(column_expr!("x"), 10),
        Expr::ne(column_expr!("x"), 10),
        Expr::gt(column_expr!("x"), 10),
        Expr::ge(column_expr!("x"), 10),
    ];

    let do_test = |min: Scalar, max: Scalar, expected: &[Option<bool>]| {
        let filter = MinMaxTestFilter::new(Some(min.clone()), Some(max.clone()));
        for (expr, expect) in expressions.iter().zip(expected.iter()) {
            expect_eq!(filter.eval(expr), *expect, "{expr:#?} with [{min}..{max}]");
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
        Expr::is_null(column_expr!("x")),
        !Expr::is_null(column_expr!("x")),
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
