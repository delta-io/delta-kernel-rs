use super::*;
use crate::expressions::{column_expr, ArrayData, StructData};
use crate::schema::ArrayType;
use crate::DataType;

struct UnimplementedTestFilter;
impl ParquetStatsSkippingFilter for UnimplementedTestFilter {
    fn get_min_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_max_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
        unimplemented!()
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        unimplemented!()
    }
}

struct JunctionTest {
    inputs: &'static [Option<bool>],
    expect_and: Option<bool>,
    expect_or: Option<bool>,
}

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
impl JunctionTest {
    fn new(
        inputs: &'static [Option<bool>],
        expect_and: Option<bool>,
        expect_or: Option<bool>,
    ) -> Self {
        Self {
            inputs,
            expect_and,
            expect_or,
        }
    }
    fn do_test(&self) {
        use VariadicOperator::*;
        let filter = UnimplementedTestFilter;
        let inputs: Vec<_> = self
            .inputs
            .iter()
            .map(|val| match val {
                Some(v) => Expression::literal(v),
                None => Expression::null_literal(DataType::BOOLEAN),
            })
            .collect();

        expect_eq!(
            filter.apply_variadic(And, &inputs, false),
            self.expect_and,
            "AND({inputs:?})"
        );
        expect_eq!(
            filter.apply_variadic(Or, &inputs, false),
            self.expect_or,
            "OR({inputs:?})"
        );
        expect_eq!(
            filter.apply_variadic(And, &inputs, true),
            self.expect_and.map(|val| !val),
            "NOT(AND({inputs:?}))"
        );
        expect_eq!(
            filter.apply_variadic(Or, &inputs, true),
            self.expect_or.map(|val| !val),
            "NOT(OR({inputs:?}))"
        );
    }
}

/// Tests apply_variadic and apply_scalar
#[test]
fn test_junctions() {
    let test_case = JunctionTest::new;
    const TRUE: Option<bool> = Some(true);
    const FALSE: Option<bool> = Some(false);
    const NULL: Option<bool> = None;
    let test_cases = &[
        // Every combo of 0, 1 and 2 inputs
        test_case(&[], TRUE, FALSE),
        test_case(&[TRUE], TRUE, TRUE),
        test_case(&[FALSE], FALSE, FALSE),
        test_case(&[NULL], NULL, NULL),
        test_case(&[TRUE, TRUE], TRUE, TRUE),
        test_case(&[TRUE, FALSE], FALSE, TRUE),
        test_case(&[TRUE, NULL], NULL, TRUE),
        test_case(&[FALSE, TRUE], FALSE, TRUE),
        test_case(&[FALSE, FALSE], FALSE, FALSE),
        test_case(&[FALSE, NULL], FALSE, NULL),
        test_case(&[NULL, TRUE], NULL, TRUE),
        test_case(&[NULL, FALSE], FALSE, NULL),
        test_case(&[NULL, NULL], NULL, NULL),
        // Every combo of 1:2
        test_case(&[TRUE, FALSE, FALSE], FALSE, TRUE),
        test_case(&[FALSE, TRUE, FALSE], FALSE, TRUE),
        test_case(&[FALSE, FALSE, TRUE], FALSE, TRUE),
        test_case(&[TRUE, NULL, NULL], NULL, TRUE),
        test_case(&[NULL, TRUE, NULL], NULL, TRUE),
        test_case(&[NULL, NULL, TRUE], NULL, TRUE),
        test_case(&[FALSE, TRUE, TRUE], FALSE, TRUE),
        test_case(&[TRUE, FALSE, TRUE], FALSE, TRUE),
        test_case(&[TRUE, TRUE, FALSE], FALSE, TRUE),
        test_case(&[FALSE, NULL, NULL], FALSE, NULL),
        test_case(&[NULL, FALSE, NULL], FALSE, NULL),
        test_case(&[NULL, NULL, FALSE], FALSE, NULL),
        test_case(&[NULL, TRUE, TRUE], NULL, TRUE),
        test_case(&[TRUE, NULL, TRUE], NULL, TRUE),
        test_case(&[TRUE, TRUE, NULL], NULL, TRUE),
        test_case(&[NULL, FALSE, FALSE], FALSE, NULL),
        test_case(&[FALSE, NULL, FALSE], FALSE, NULL),
        test_case(&[FALSE, FALSE, NULL], FALSE, NULL),
        // Every unique ordering of 3
        test_case(&[TRUE, FALSE, NULL], FALSE, TRUE),
        test_case(&[TRUE, NULL, FALSE], FALSE, TRUE),
        test_case(&[FALSE, TRUE, NULL], FALSE, TRUE),
        test_case(&[FALSE, NULL, TRUE], FALSE, TRUE),
        test_case(&[NULL, TRUE, FALSE], FALSE, TRUE),
        test_case(&[NULL, FALSE, TRUE], FALSE, TRUE),
    ];
    for test_case in test_cases {
        test_case.do_test();
    }
}

// tests apply_binary_scalars
#[test]
fn test_binary_scalars() {
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
        Decimal(1, 10, 10), // invalid value,
        Null(DataType::LONG),
        Struct(StructData::try_new(vec![], vec![]).unwrap()),
        Array(ArrayData::new(
            ArrayType::new(DataType::LONG, false),
            Vec::<Scalar>::new(),
        )),
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
        Decimal(10, 10, 10), // invalid value
        Null(DataType::LONG),
        Struct(StructData::try_new(vec![], vec![]).unwrap()),
        Array(ArrayData::new(
            ArrayType::new(DataType::LONG, false),
            Vec::<Scalar>::new(),
        )),
    ];

    // scalars of different types are always incomparable
    use BinaryOperator::*;
    let binary_ops = [
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
    ];
    let compare = UnimplementedTestFilter::apply_binary_scalars;
    for (i, a) in smaller_values.iter().enumerate() {
        for b in smaller_values.iter().skip(i + 1) {
            for op in binary_ops {
                let result = compare(op, a, b);
                let a_type = a.data_type();
                let b_type = b.data_type();
                assert!(
                    result.is_none(),
                    "{a_type:?} should not be comparable to {b_type:?}"
                );
            }
        }
    }

    let expect_if_comparable_type = |s: &_, expect| match s {
        Null(_) | Decimal(..) | Struct(_) | Array(_) => None,
        _ => Some(expect),
    };

    // Test same-type comparisons where a == b
    for (a, b) in smaller_values.iter().zip(smaller_values.iter()) {
        expect_eq!(
            compare(Equal, a, b),
            expect_if_comparable_type(a, true),
            "{a:?} == {b:?}"
        );

        expect_eq!(
            compare(NotEqual, a, b),
            expect_if_comparable_type(a, false),
            "{a:?} != {b:?}"
        );

        expect_eq!(
            compare(LessThan, a, b),
            expect_if_comparable_type(a, false),
            "{a:?} < {b:?}"
        );

        expect_eq!(
            compare(GreaterThan, a, b),
            expect_if_comparable_type(a, false),
            "{a:?} > {b:?}"
        );

        expect_eq!(
            compare(LessThanOrEqual, a, b),
            expect_if_comparable_type(a, true),
            "{a:?} <= {b:?}"
        );

        expect_eq!(
            compare(GreaterThanOrEqual, a, b),
            expect_if_comparable_type(a, true),
            "{a:?} >= {b:?}"
        );
    }

    // Test same-type comparisons where a < b
    for (a, b) in smaller_values.iter().zip(larger_values.iter()) {
        expect_eq!(
            compare(Equal, a, b),
            expect_if_comparable_type(a, false),
            "{a:?} == {b:?}"
        );

        expect_eq!(
            compare(NotEqual, a, b),
            expect_if_comparable_type(a, true),
            "{a:?} != {b:?}"
        );

        expect_eq!(
            compare(LessThan, a, b),
            expect_if_comparable_type(a, true),
            "{a:?} < {b:?}"
        );

        expect_eq!(
            compare(GreaterThan, a, b),
            expect_if_comparable_type(a, false),
            "{a:?} > {b:?}"
        );

        expect_eq!(
            compare(LessThanOrEqual, a, b),
            expect_if_comparable_type(a, true),
            "{a:?} <= {b:?}"
        );

        expect_eq!(
            compare(GreaterThanOrEqual, a, b),
            expect_if_comparable_type(a, false),
            "{a:?} >= {b:?}"
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
impl ParquetStatsSkippingFilter for MinMaxTestFilter {
    fn get_min_stat_value(&self, _col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
        Self::get_stat_value(&self.min, data_type)
    }

    fn get_max_stat_value(&self, _col: &ColumnPath, data_type: &DataType) -> Option<Scalar> {
        Self::get_stat_value(&self.max, data_type)
    }

    fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
        unimplemented!()
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        unimplemented!()
    }
}

#[test]
fn test_binary_eq_ne() {
    use BinaryOperator::*;

    const LO: Scalar = Scalar::Long(1);
    const MID: Scalar = Scalar::Long(10);
    const HI: Scalar = Scalar::Long(100);
    let col = &column_expr!("x");

    for inverted in [false, true] {
        // negative test -- mismatched column type
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                Equal,
                col,
                &Expression::literal("10"),
                inverted,
            ),
            None,
            "{col} == '10' (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        // quick test for literal-literal comparisons
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                Equal,
                &MID.into(),
                &MID.into(),
                inverted,
            ),
            Some(!inverted),
            "{MID} == {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        // quick test for literal-column comparisons
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                Equal,
                &MID.into(),
                col,
                inverted,
            ),
            Some(!inverted),
            "{MID} == {col} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                Equal,
                col,
                &MID.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} == {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                Equal,
                col,
                &MID.into(),
                inverted,
            ),
            Some(true), // min..max range includes both EQ and NE
            "{col} == {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                Equal,
                col,
                &HI.into(),
                inverted,
            ),
            Some(inverted),
            "{col} == {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                Equal,
                col,
                &LO.into(),
                inverted,
            ),
            Some(inverted),
            "{col} == {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
        );

        // negative test -- mismatched column type
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                NotEqual,
                col,
                &Expression::literal("10"),
                inverted,
            ),
            None,
            "{col} != '10' (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                NotEqual,
                col,
                &MID.into(),
                inverted,
            ),
            Some(inverted),
            "{col} != {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                NotEqual,
                col,
                &MID.into(),
                inverted,
            ),
            Some(true), // min..max range includes both EQ and NE
            "{col} != {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                NotEqual,
                col,
                &HI.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} != {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                NotEqual,
                col,
                &LO.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} != {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
        );
    }
}

#[test]
fn test_binary_lt_ge() {
    use BinaryOperator::*;

    const LO: Scalar = Scalar::Long(1);
    const MID: Scalar = Scalar::Long(10);
    const HI: Scalar = Scalar::Long(100);
    let col = &column_expr!("x");

    for inverted in [false, true] {
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                LessThan,
                col,
                &MID.into(),
                inverted,
            ),
            Some(inverted),
            "{col} < {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                LessThan,
                col,
                &MID.into(),
                inverted,
            ),
            Some(true), // min..max range includes both LT and GE
            "{col} < {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                LessThan,
                col,
                &HI.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} < {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                LessThan,
                col,
                &LO.into(),
                inverted,
            ),
            Some(inverted),
            "{col} < {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                GreaterThanOrEqual,
                col,
                &MID.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} >= {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                GreaterThanOrEqual,
                col,
                &MID.into(),
                inverted,
            ),
            Some(true), // min..max range includes both EQ and NE
            "{col} >= {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                GreaterThanOrEqual,
                col,
                &HI.into(),
                inverted,
            ),
            Some(inverted),
            "{col} >= {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                GreaterThanOrEqual,
                col,
                &LO.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} >= {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
        );
    }
}

#[test]
fn test_binary_le_gt() {
    use BinaryOperator::*;

    const LO: Scalar = Scalar::Long(1);
    const MID: Scalar = Scalar::Long(10);
    const HI: Scalar = Scalar::Long(100);
    let col = &column_expr!("x");

    for inverted in [false, true] {
        // negative test -- mismatched column type
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                LessThanOrEqual,
                col,
                &Expression::literal("10"),
                inverted,
            ),
            None,
            "{col} <= '10' (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                LessThanOrEqual,
                col,
                &MID.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} <= {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                LessThanOrEqual,
                col,
                &MID.into(),
                inverted,
            ),
            Some(true), // min..max range includes both LT and GE
            "{col} <= {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                LessThanOrEqual,
                col,
                &HI.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} <= {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                LessThanOrEqual,
                col,
                &LO.into(),
                inverted,
            ),
            Some(inverted),
            "{col} <= {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
        );

        // negative test -- mismatched column type
        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                GreaterThan,
                col,
                &Expression::literal("10"),
                inverted,
            ),
            None,
            "{col} > '10' (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), MID.into()).apply_binary(
                GreaterThan,
                col,
                &MID.into(),
                inverted,
            ),
            Some(inverted),
            "{col} > {MID} (min: {MID}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), HI.into()).apply_binary(
                GreaterThan,
                col,
                &MID.into(),
                inverted,
            ),
            Some(true), // min..max range includes both EQ and NE
            "{col} > {MID} (min: {LO}, max: {HI}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(LO.into(), MID.into()).apply_binary(
                GreaterThan,
                col,
                &HI.into(),
                inverted,
            ),
            Some(inverted),
            "{col} > {HI} (min: {LO}, max: {MID}, inverted: {inverted})"
        );

        expect_eq!(
            MinMaxTestFilter::new(MID.into(), HI.into()).apply_binary(
                GreaterThan,
                col,
                &LO.into(),
                inverted,
            ),
            Some(!inverted),
            "{col} > {LO} (min: {MID}, max: {HI}, inverted: {inverted})"
        );
    }
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
impl ParquetStatsSkippingFilter for NullCountTestFilter {
    fn get_min_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_max_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
        unimplemented!()
    }

    fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
        self.nullcount
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        self.rowcount
    }
}

#[test]
fn test_not_null() {
    use UnaryOperator::IsNull;

    let col = &column_expr!("x");
    for inverted in [false, true] {
        expect_eq!(
            NullCountTestFilter::new(None, 10).apply_unary(IsNull, col, inverted),
            None,
            "{col} IS NULL (nullcount: None, rowcount: 10, inverted: {inverted})"
        );

        expect_eq!(
            NullCountTestFilter::new(Some(0), 10).apply_unary(IsNull, col, inverted),
            Some(inverted),
            "{col} IS NULL (nullcount: 0, rowcount: 10, inverted: {inverted})"
        );

        expect_eq!(
            NullCountTestFilter::new(Some(5), 10).apply_unary(IsNull, col, inverted),
            Some(true),
            "{col} IS NULL (nullcount: 5, rowcount: 10, inverted: {inverted})"
        );

        expect_eq!(
            NullCountTestFilter::new(Some(10), 10).apply_unary(IsNull, col, inverted),
            Some(!inverted),
            "{col} IS NULL (nullcount: 10, rowcount: 10, inverted: {inverted})"
        );
    }
}

#[test]
fn test_bool_col() {
    use Scalar::Boolean;
    const TRUE: Scalar = Boolean(true);
    const FALSE: Scalar = Boolean(false);
    for inverted in [false, true] {
        expect_eq!(
            MinMaxTestFilter::new(TRUE.into(), TRUE.into()).apply_column("x", inverted),
            Some(!inverted),
            "x as boolean (min: TRUE, max: TRUE, inverted: {inverted})"
        );
        expect_eq!(
            MinMaxTestFilter::new(FALSE.into(), TRUE.into()).apply_column("x", inverted),
            Some(true),
            "x as boolean (min: FALSE, max: TRUE, inverted: {inverted})"
        );
        expect_eq!(
            MinMaxTestFilter::new(FALSE.into(), FALSE.into()).apply_column("x", inverted),
            Some(inverted),
            "x as boolean (min: FALSE, max: FALSE, inverted: {inverted})"
        );
    }
}

struct AllNullTestFilter;
impl ParquetStatsSkippingFilter for AllNullTestFilter {
    fn get_min_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
        None
    }

    fn get_max_stat_value(&self, _col: &ColumnPath, _data_type: &DataType) -> Option<Scalar> {
        None
    }

    fn get_nullcount_stat_value(&self, _col: &ColumnPath) -> Option<i64> {
        Some(self.get_rowcount_stat_value())
    }

    fn get_rowcount_stat_value(&self) -> i64 {
        10
    }
}

#[test]
fn test_sql_where() {
    let col = &column_expr!("x");
    let val = &Expression::literal(1);
    const NULL: Expression = Expression::Literal(Scalar::Null(DataType::BOOLEAN));
    const FALSE: Expression = Expression::Literal(Scalar::Boolean(false));
    const TRUE: Expression = Expression::Literal(Scalar::Boolean(true));

    // Basic sanity checks
    expect_eq!(AllNullTestFilter.apply_sql_where(val), None, "WHERE {val}");
    expect_eq!(AllNullTestFilter.apply_sql_where(col), None, "WHERE {col}");
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::is_null(col.clone())),
        Some(true), // No injected NULL checks
        "WHERE {col} IS NULL"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::lt(TRUE, FALSE)),
        Some(false), // Injected NULL checks don't short circuit when inputs are NOT NULL
        "WHERE {TRUE} < {FALSE}"
    );

    // Constrast normal vs SQL WHERE semantics - comparison
    expect_eq!(
        AllNullTestFilter.apply_expr(&Expression::lt(col.clone(), val.clone()), false),
        None,
        "{col} < {val}"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::lt(col.clone(), val.clone())),
        Some(false),
        "WHERE {col} < {val}"
    );
    expect_eq!(
        AllNullTestFilter.apply_expr(&Expression::lt(val.clone(), col.clone()), false),
        None,
        "{val} < {col}"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::lt(val.clone(), col.clone())),
        Some(false),
        "WHERE {val} < {col}"
    );

    // Constrast normal vs SQL WHERE semantics - comparison inside AND
    expect_eq!(
        AllNullTestFilter.apply_expr(
            &Expression::and(NULL, Expression::lt(col.clone(), val.clone())),
            false
        ),
        None,
        "{NULL} AND {col} < {val}"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::and(
            NULL,
            Expression::lt(col.clone(), val.clone()),
        )),
        Some(false),
        "WHERE {NULL} AND {col} < {val}"
    );

    expect_eq!(
        AllNullTestFilter.apply_expr(
            &Expression::and(TRUE, Expression::lt(col.clone(), val.clone())),
            false
        ),
        None, // NULL (from the NULL check) is stronger than TRUE
        "{TRUE} AND {col} < {val}"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::and(
            TRUE,
            Expression::lt(col.clone(), val.clone()),
        )),
        Some(false), // FALSE (from the NULL check) is stronger than TRUE
        "WHERE {TRUE} AND {col} < {val}"
    );

    // Contrast normal vs. SQL WHERE semantics - comparison inside AND inside AND
    expect_eq!(
        AllNullTestFilter.apply_expr(
            &Expression::and(
                TRUE,
                Expression::and(NULL, Expression::lt(col.clone(), val.clone())),
            ),
            false,
        ),
        None,
        "{TRUE} AND ({NULL} AND {col} < {val})"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::and(
            TRUE,
            Expression::and(NULL, Expression::lt(col.clone(), val.clone())),
        )),
        Some(false),
        "WHERE {TRUE} AND ({NULL} AND {col} < {val})"
    );

    // Semantics are the same for comparison inside OR inside AND
    expect_eq!(
        AllNullTestFilter.apply_expr(
            &Expression::or(
                FALSE,
                Expression::and(NULL, Expression::lt(col.clone(), val.clone())),
            ),
            false,
        ),
        None,
        "{FALSE} OR ({NULL} AND {col} < {val})"
    );
    expect_eq!(
        AllNullTestFilter.apply_sql_where(&Expression::or(
            FALSE,
            Expression::and(NULL, Expression::lt(col.clone(), val.clone())),
        )),
        None,
        "WHERE {FALSE} OR ({NULL} AND {col} < {val})"
    );
}
