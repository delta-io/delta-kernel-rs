use arrow_array::{array::PrimitiveArray, BooleanArray, Int32Array, RecordBatch, StructArray, types::Int32Type};
use arrow_ord::cmp::lt;
use arrow_schema::ArrowError;

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(i32),
    Column(String), // TODO make path to column (stats.min)
    LessThan(Box<Expression>, Box<Expression>),
}

impl Expression {
    // consume predicate, produce filter vector
    pub(crate) fn construct_metadata_filters(
        &self,
        stats: RecordBatch,
    ) -> Result<BooleanArray, ArrowError> {
        match self {
            // col < value
            Expression::LessThan(left, right) => {
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

    pub(crate) fn columns(&self) -> Vec<String> {
        match self {
            Expression::Literal(_) => vec![],
            Expression::Column(name) => vec![name.to_string()],
            Expression::LessThan(left, right) => {
                let mut l = left.columns();
                l.append(&mut right.columns());
                l
            }
        }
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
