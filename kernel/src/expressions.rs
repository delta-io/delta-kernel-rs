use arrow_array::{
    array::PrimitiveArray, types::Int32Type, BooleanArray, Int32Array, RecordBatch, StructArray,
};
use arrow_ord::cmp::lt;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
};

use arrow_schema::ArrowError;

use crate::{
    schema::{DataType, PrimitiveType},
    DeltaResult, Error,
};

use self::scalars::{ColumnBounds, Scalar};

pub mod scalars;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
}

impl Display for ComparisonOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LessThan => write!(f, "<"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThan => write!(f, ">"),
            Self::GreaterThanOrEqual => write!(f, ">="),
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "!="),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    // TODO: how to we handle scalar values?
    // TODO: how do we handle is null expressions?
    Literal(Scalar),
    Column {
        name: String,
        data_type: DataType,
    }, // TODO make path to column (stats.min)
    BinaryOperator {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    BinaryComparison {
        op: ComparisonOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    And {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Or {
        left: Box<Expression>,
        right: Box<Expression>,
    },
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{}", l),
            Self::Column { name, .. } => write!(f, "Column({})", name),
            Self::BinaryOperator { op, left, right } => {
                write!(f, "{} {} {}", left, op, right)
            }
            Self::BinaryComparison { op, left, right } => {
                write!(f, "{} {} {}", left, op, right)
            }
            Self::And { left, right } => write!(f, "{} AND {}", left, right),
            Self::Or { left, right } => write!(f, "({} OR {})", left, right),
        }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<String> {
        let mut set = HashSet::new();

        self.visit(|expr| {
            if let Self::Column { name, .. } = &expr {
                set.insert(name.to_string());
            }
        });

        set
    }

    /// Returns the data type of this expression.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Literal(scalar) => scalar.data_type(),
            Self::Column { data_type, .. } => data_type.clone(),
            Self::BinaryOperator { left, .. } => left.data_type(),
            Self::BinaryComparison { .. } | Self::And { .. } | Self::Or { .. } => {
                DataType::Primitive(PrimitiveType::Boolean)
            }
        }
    }

    /// Create an new expression for a column reference
    pub fn column(name: impl Into<String>, data_type: DataType) -> Self {
        Self::Column {
            name: name.into(),
            data_type,
        }
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    fn cmp_impl(&self, other: &Self, op: ComparisonOperator) -> DeltaResult<Self> {
        if self.data_type() != other.data_type() {
            return Err(Error::Generic(format!(
                "Cannot compare expressions of different types: {} and {}",
                self.data_type(),
                other.data_type()
            ))
            .into());
        }
        Ok(Self::BinaryComparison {
            op,
            left: Box::new(self.clone()),
            right: Box::new(other.clone()),
        })
    }

    /// Create a new expression `self == other`
    pub fn eq(&self, other: &Self) -> DeltaResult<Self> {
        self.cmp_impl(other, ComparisonOperator::Equal)
    }

    /// Create a new expression `self != other`
    pub fn ne(&self, other: &Self) -> DeltaResult<Self> {
        self.cmp_impl(other, ComparisonOperator::NotEqual)
    }

    /// Create a new expression `self < other`
    pub fn lt(&self, other: &Self) -> DeltaResult<Self> {
        self.cmp_impl(other, ComparisonOperator::LessThan)
    }

    /// Create a new expression `self > other`
    pub fn gt(&self, other: &Self) -> DeltaResult<Self> {
        self.cmp_impl(other, ComparisonOperator::GreaterThan)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(&self, other: &Self) -> DeltaResult<Self> {
        self.cmp_impl(other, ComparisonOperator::GreaterThanOrEqual)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(&self, other: &Self) -> DeltaResult<Self> {
        self.cmp_impl(other, ComparisonOperator::LessThanOrEqual)
    }

    fn assert_boolean(&self) -> DeltaResult<()> {
        if !matches!(
            self.data_type(),
            DataType::Primitive(PrimitiveType::Boolean)
        ) {
            return Err(Error::Generic(format!(
                "Cannot use expression of type {} as boolean",
                self.data_type()
            ))
            .into());
        }
        Ok(())
    }

    /// Create a new expression `self AND other`
    pub fn and(&self, other: &Self) -> DeltaResult<Self> {
        self.assert_boolean()?;
        other.assert_boolean()?;
        Ok(Self::And {
            left: Box::new(self.clone()),
            right: Box::new(other.clone()),
        })
    }

    /// Create a new expression `self OR other`
    pub fn or(&self, other: &Self) -> DeltaResult<Self> {
        self.assert_boolean()?;
        other.assert_boolean()?;
        Ok(Self::Or {
            left: Box::new(self.clone()),
            right: Box::new(other.clone()),
        })
    }

    fn binary_op_impl(&self, other: &Expression, op: BinaryOperator) -> DeltaResult<Self> {
        if self.data_type() != other.data_type() {
            return Err(Error::Generic(format!(
                "Cannot apply operator {} to expressions of different types: {} and {}",
                op,
                self.data_type(),
                other.data_type()
            ))
            .into());
        }
        Ok(Self::BinaryOperator {
            op,
            left: Box::new(self.clone()),
            right: Box::new(other.clone()),
        })
    }

    fn visit(&self, mut visit_fn: impl FnMut(&Self)) {
        let mut stack = vec![self];
        while let Some(expr) = stack.pop() {
            visit_fn(expr);
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::BinaryOperator { left, right, op } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::BinaryComparison { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::And { left, right } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::Or { left, right } => {
                    stack.push(left);
                    stack.push(right);
                }
            }
        }
    }

    fn visit_mut(&mut self, mut visit_fn: impl FnMut(&mut Self)) {
        let mut stack = vec![self];
        while let Some(mut expr) = stack.pop() {
            visit_fn(&mut expr);
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::BinaryOperator { left, right, op } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::BinaryComparison { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::And { left, right } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::Or { left, right } => {
                    stack.push(left);
                    stack.push(right);
                }
            }
        }
    }

    /// Rewrite the expression in a canonical form.
    ///
    /// For example, the expressions `x > 2` and `2 > x` should be considered
    /// identical, but aren't.
    ///
    /// This is useful for comparing expressions for equality or to simplify
    /// implementations that manipulate expressions.
    ///
    /// Rules:
    ///  * In binary comparisons, the left side should be a column reference
    fn canonicalize(&mut self) {
        self.visit_mut(|expr| match expr {
            Self::BinaryComparison { left, right, .. } => match (&mut **left, &mut **right) {
                (Self::Column { .. }, Self::Literal(_)) => {}
                (Self::Literal(_), Self::Column { .. }) => {
                    std::mem::swap(left, right);
                }
                _ => {}
            },
            _ => {}
        })
    }

    /// Simplify the expression, if possible.
    ///
    /// Does not nest.
    fn simplify(&mut self) -> Self {
        todo!()
    }

    fn rewrite_with_bounds(&mut self, bounds: &HashMap<String, ColumnBounds>) -> Self {
        todo!()
    }

    /// Get the residual expression after applying the column bounds.
    fn get_residual(&self, column_bounds: &HashMap<String, ColumnBounds>) -> DeltaResult<Self> {
        if self.data_type() != DataType::Primitive(PrimitiveType::Boolean) {
            return Err(Error::Generic(format!(
                "Cannot apply column bounds to expression of type {}",
                self.data_type()
            )));
        }

        // match self {
        //     Self::Literal(_) => Ok(self.clone()),
        //     Self
        // }
        todo!();
    }

    /// Apply the predicate to a stats record batch, returning a boolean array
    ///
    /// The boolean array will represent a mask of the files that could match
    /// the predicate.
    ///
    /// For example, if the predicate is `x > 2` and the stats record batch has
    /// `maxValues.x = 1`, then the returned boolean array will have `false` at
    /// that index.
    pub(crate) fn construct_metadata_filters(
        &self,
        stats: RecordBatch,
    ) -> Result<BooleanArray, ArrowError> {
        match self {
            // col < value
            Expression::BinaryComparison { op, left, right } => {
                match op {
                    ComparisonOperator::LessThan => {
                        match (left.as_ref(), right.as_ref()) {
                            (Expression::Column(name), Expression::Literal(l)) => {
                                // column_min < value
                                lt(
                                    stats
                                        .column_by_name("minValues")
                                        .unwrap()
                                        .as_any()
                                        .downcast_ref::<StructArray>()
                                        .unwrap()
                                        .column_by_name(name)
                                        .unwrap()
                                        .as_any()
                                        .downcast_ref::<Int32Array>()
                                        .unwrap(),
                                    &PrimitiveArray::<Int32Type>::new_scalar(*l),
                                )
                            }
                            _ => todo!(),
                        }
                    }
                    _ => todo!(),
                }
            }

            _ => todo!(),
        }
    }

    /// Extract the names of all columns referenced by this expression.
    pub(crate) fn columns(&self) -> HashSet<String> {
        let mut columns = HashSet::new();

        let mut stack = vec![self];

        while let Some(expr) = stack.pop() {
            match expr {
                Expression::Literal(_) => {}
                Expression::Column(c) => {
                    columns.insert(c.to_string());
                }
                Expression::BinaryOperator { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Expression::BinaryComparison { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Expression::And { left, right } => {
                    stack.push(left);
                    stack.push(right);
                }
                Expression::Or { left, right } => {
                    stack.push(left);
                    stack.push(right);
                }
            }
        }

        columns
    }
}

// impl Expression {
//     fn to_arrow(&self, stats: &StructArray) -> Result<BooleanArray, ArrowError> {
//         match self {
//             Expression::LessThan(left, right) => {
//                 lt_scalar(left.to_arrow(stats), right.to_arrow(stats))
//             }
//             Expression::Column(c) => todo!(),
//             Expression::Literal(l) => todo!(),
//         }
//     }
// }

// transform data predicate into metadata predicate
// WHERE x < 10 -> min(x) < 10
// fn construct_metadata_filters(e: Expression) -> Expression {
//     match e {
//         // col < value
//         Expression::LessThan(left, right) => {
//             match (*left, *right.clone()) {
//                 (Expression::Column(name), Expression::Literal(_)) => {
//                     // column_min < value
//                     Expression::LessThan(Box::new(min_stat_col(name)), right)
//                 }
//                 _ => todo!(),
//             }
//         }
//         _ => todo!(),
//     }
// }

// fn min_stat_col(col_name: Vec<String>) -> Expression {
//     stat_col("min", col_name)
// }
//
// fn stat_col(stat: &str, name: Vec<String>) -> Expression {
//     let mut v = vec![stat.to_owned()];
//     v.extend(name);
//     Expression::Column(v)
// }

impl std::ops::Add<Expression> for Expression {
    type Output = DeltaResult<Self>;

    fn add(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(&rhs, BinaryOperator::Plus)
    }
}

impl std::ops::Sub<Expression> for Expression {
    type Output = DeltaResult<Self>;

    fn sub(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(&rhs, BinaryOperator::Minus)
    }
}

impl std::ops::Mul<Expression> for Expression {
    type Output = DeltaResult<Self>;

    fn mul(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(&rhs, BinaryOperator::Multiply)
    }
}

impl std::ops::Div<Expression> for Expression {
    type Output = DeltaResult<Self>;

    fn div(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(&rhs, BinaryOperator::Divide)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_format() {
        let col_ref = Expression::column("x", DataType::Primitive(PrimitiveType::Integer));
        let cases = [
            (col_ref.clone(), "Column(x)"),
            (
                col_ref.clone().eq(&Expression::literal(2)).unwrap(),
                "Column(x) = 2",
            ),
            (
                col_ref
                    .gt_eq(&Expression::literal(2))
                    .unwrap()
                    .and(&(col_ref.lt_eq(&Expression::literal(10))).unwrap())
                    .unwrap(),
                "Column(x) >= 2 AND Column(x) <= 10",
            ),
            (
                col_ref
                    .clone()
                    .gt(&Expression::literal(2))
                    .unwrap()
                    .or(&col_ref.lt(&Expression::literal(10)).unwrap())
                    .unwrap(),
                "(Column(x) > 2 OR Column(x) < 10)",
            ),
            (
                (col_ref.clone() - Expression::literal(4))
                    .unwrap()
                    .lt(&Expression::literal(10))
                    .unwrap(),
                "Column(x) - 4 < 10",
            ),
            (
                (((col_ref + Expression::literal(4)).unwrap() / (Expression::literal(10)))
                    .unwrap()
                    * Expression::literal(42))
                .unwrap(),
                "Column(x) + 4 / 10 * 42",
            ),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_canonicalize() {
        let x_ref = Expression::column("x", DataType::Primitive(PrimitiveType::Integer));
        let ten = Expression::literal(10);
        let cases = vec![(
            ten.clone().eq(&x_ref).unwrap(),
            x_ref.clone().eq(&ten).unwrap(),
        )];

        for (mut expr, expected) in cases {
            expr.canonicalize();
            assert_eq!(expr, expected);
        }
    }

    #[test]
    fn test_simplify() {
        // x + 0 -> x
        // true AND false -> false
        // true AND true -> true
        // true OR false -> true
    }

    #[test]
    fn test_residual() {
        // x > 0 + x = 1 -> true
        // x > 0 + x = -1 -> false
        // x > 0 + minValues.x = 1 -> true
        // x > 0 + minValues.x = -1 -> x > 0
        // x > 0 + maxValues.x = -1 -> false
        // x > 0 + maxValues.x = 1 -> x > 0
    }
}
