use std::collections::HashSet;
use std::io::BufReader;
use std::sync::Arc;

use arrow_arith::boolean::is_null;
use arrow_array::{new_null_array, Array, BooleanArray, RecordBatch, StringArray, StructArray};
use arrow_json::ReaderBuilder;
use arrow_schema::Schema as ArrowSchema;
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::nullif::nullif;
use tracing::debug;

use crate::error::{DeltaResult, Error};
use crate::expressions::{BinaryOperator, Expression, VariadicOperator};
use crate::schema::{SchemaRef, StructField, StructType};
use crate::{ExpressionEvaluator, TableClient};

fn rewite(expr: &Expression) -> Option<Expression> {
    use BinaryOperator::*;
    use Expression::*;
    use VariadicOperator::*;

    match expr {
        BinaryOperation { op, left, right } => match op {
            LessThan | LessThanOrEqual => match (left.as_ref(), right.as_ref()) {
                // column <lt | lt_eq> value
                (Column(col), Literal(value)) => Some(BinaryOperation {
                    op: op.clone(),
                    left: Box::new(Column(format!("minValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                // value <lt | lt_eq> column
                (Literal(value), Column(col)) => Some(BinaryOperation {
                    op: match op {
                        LessThan => GreaterThan,
                        LessThanOrEqual => GreaterThanOrEqual,
                        _ => unreachable!(),
                    },
                    left: Box::new(Column(format!("maxValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                _ => None,
            },
            GreaterThan | GreaterThanOrEqual => match (left.as_ref(), right.as_ref()) {
                // column <gt | gt_eq> value
                (Column(col), Literal(value)) => Some(BinaryOperation {
                    op: op.clone(),
                    left: Box::new(Column(format!("maxValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                // value <gt | gt_eq> column
                (Literal(value), Column(col)) => Some(BinaryOperation {
                    op: match op {
                        GreaterThan => LessThan,
                        GreaterThanOrEqual => LessThanOrEqual,
                        _ => unreachable!(),
                    },
                    left: Box::new(Column(format!("minValues.{}", col))),
                    right: Box::new(Literal(value.clone())),
                }),
                _ => None,
            },
            // column == value
            // column == value is equivalent to min values being less than or equal to value
            // and max values being greater than or equal to value
            Equal => match (left.as_ref(), right.as_ref()) {
                (Column(col), Literal(value)) | (Literal(value), Column(col)) => {
                    Some(VariadicOperation {
                        op: And,
                        exprs: vec![
                            BinaryOperation {
                                op: GreaterThanOrEqual,
                                left: Box::new(Column(format!("maxValues.{}", col))),
                                right: Box::new(Literal(value.clone())),
                            },
                            BinaryOperation {
                                op: LessThanOrEqual,
                                left: Box::new(Column(format!("minValues.{}", col))),
                                right: Box::new(Literal(value.clone())),
                            },
                        ],
                    })
                }
                _ => None,
            },
            // column != value
            // column != value is equivalent to min values being strictly greater than value
            // or max values being strictly less than value
            NotEqual => match (left.as_ref(), right.as_ref()) {
                (Column(col), Literal(value)) | (Literal(value), Column(col)) => {
                    Some(VariadicOperation {
                        op: Or,
                        exprs: vec![
                            BinaryOperation {
                                op: LessThan,
                                left: Box::new(Column(format!("maxValues.{}", col))),
                                right: Box::new(Literal(value.clone())),
                            },
                            BinaryOperation {
                                op: GreaterThan,
                                left: Box::new(Column(format!("minValues.{}", col))),
                                right: Box::new(Literal(value.clone())),
                            },
                        ],
                    })
                }
                _ => None,
            },
            _ => None,
        },
        VariadicOperation { op, exprs } => Some(VariadicOperation {
            op: op.clone(),
            exprs: exprs.iter().filter_map(rewite).collect::<Vec<_>>(),
        }),
        _ => None,
    }
}

pub(crate) struct DataSkippingFilter {
    stats_schema: SchemaRef,
    evaluator: Arc<dyn ExpressionEvaluator>,
}

impl DataSkippingFilter {
    /// Creates a new data skipping filter. Returns None if there is no predicate, or the predicate
    /// is ineligible for data skipping.
    ///
    /// NOTE: None is equivalent to a trivial filter that always returns TRUE (= keeps all files),
    /// but using an Option lets the engine easily avoid the overhead of applying trivial filters.
    pub(crate) fn new<JRC: Send, PRC: Send>(
        table_client: &dyn TableClient<JsonReadContext = JRC, ParquetReadContext = PRC>,
        table_schema: &SchemaRef,
        predicate: &Option<Expression>,
    ) -> Option<Self> {
        let predicate = match predicate {
            Some(predicate) => predicate,
            None => return None,
        };

        debug!("Creating a data skipping filter for {}", &predicate);
        let field_names: HashSet<_> = predicate.references();

        // Build the stats read schema by extracting the column names referenced by the predicate,
        // extracting the corresponding field from the table schema, and inserting that field.
        let data_fields: Vec<_> = table_schema
            .fields
            .iter()
            .filter(|field| field_names.contains(&field.name.as_str()))
            .cloned()
            .collect();
        if data_fields.is_empty() {
            // The predicate didn't reference any eligible stats columns, so skip it.
            return None;
        }

        let stats_schema = Arc::new(StructType::new(vec![
            StructField::new("minValues", StructType::new(data_fields.clone()), true),
            StructField::new("maxValues", StructType::new(data_fields.clone()), true),
        ]));

        let skipping_predicate = rewite(predicate)?;
        let evaluator = table_client
            .get_expression_handler()
            .get_evaluator(stats_schema.clone(), skipping_predicate);

        Some(Self {
            stats_schema,
            evaluator,
        })
    }

    pub(crate) fn apply(&self, actions: &RecordBatch) -> DeltaResult<RecordBatch> {
        let adds = actions
            .column_by_name("add")
            .ok_or(Error::MissingColumn("Column 'add' not found.".into()))?
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or(Error::UnexpectedColumnType(
                "Expected type 'StructArray'.".into(),
            ))?;
        let stats = adds
            .column_by_name("stats")
            .ok_or(Error::MissingColumn("Column 'stats' not found.".into()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::UnexpectedColumnType(
                "Expected type 'StringArray'.".into(),
            ))?;

        let stats_schema = Arc::new(self.stats_schema.as_ref().try_into()?);

        let parsed_stats = concat_batches(
            &stats_schema,
            stats
                .iter()
                .map(|json_string| Self::hack_parse(&stats_schema, json_string))
                .collect::<Result<Vec<_>, _>>()?
                .iter(),
        )?;

        // Skipping happens in several steps:
        //
        // 1. The predicate produces false for any file whose stats prove we can safely skip it. A
        //    value of true means the stats say we must keep the file, and null means we could not
        //    determine whether the file is safe to skip, because its stats were missing/null.
        //
        // 2. The nullif(skip, skip) converts true (= keep) to null, producing a result
        //    that contains only false (= skip) and null (= keep) values.
        //
        // 3. The is_null converts null to true, producing a result that contains only true (=
        //    keep) and false (= skip) values.
        //
        // 4. The filter discards every file whose selection vector entry is false.
        let skipping_vector = self.evaluator.evaluate(&parsed_stats)?;
        let skipping_vector = skipping_vector
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(Error::UnexpectedColumnType(
                "Expected type 'BooleanArray'.".into(),
            ))?;

        // let skipping_vector = self.predicate.invoke(&parsed_stats)?;
        let skipping_vector = &is_null(&nullif(skipping_vector, skipping_vector)?)?;

        let before_count = actions.num_rows();
        let after = filter_record_batch(actions, skipping_vector)?;
        debug!(
            "number of actions before/after data skipping: {before_count} / {}",
            after.num_rows()
        );
        Ok(after)
    }

    fn hack_parse(
        stats_schema: &Arc<ArrowSchema>,
        json_string: Option<&str>,
    ) -> DeltaResult<RecordBatch> {
        match json_string {
            Some(s) => Ok(ReaderBuilder::new(stats_schema.clone())
                .build(BufReader::new(s.as_bytes()))?
                .collect::<Vec<_>>()
                .into_iter()
                .next()
                .transpose()?
                .ok_or(Error::MissingData("Expected data".into()))?),
            None => Ok(RecordBatch::try_new(
                stats_schema.clone(),
                stats_schema
                    .fields
                    .iter()
                    .map(|field| new_null_array(field.data_type(), 1))
                    .collect(),
            )?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Scalar;

    #[test]
    fn test_rewrite_basic_comparison() {
        let column = Expression::Column("a".to_string());
        let lit_int = Expression::Literal(Scalar::Integer(1));

        let cases = [
            (
                column.clone().lt(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThan,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().lt(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().gt(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().gt(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThan,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().lt_eq(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThanOrEqual,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().lt_eq(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThanOrEqual,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().gt_eq(lit_int.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::GreaterThanOrEqual,
                    left: Box::new(Expression::Column("maxValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                lit_int.clone().gt_eq(column.clone()),
                Expression::BinaryOperation {
                    op: BinaryOperator::LessThanOrEqual,
                    left: Box::new(Expression::Column("minValues.a".to_string())),
                    right: Box::new(lit_int.clone()),
                },
            ),
            (
                column.clone().eq(lit_int.clone()),
                Expression::VariadicOperation {
                    op: VariadicOperator::And,
                    exprs: vec![
                        Expression::BinaryOperation {
                            op: BinaryOperator::GreaterThanOrEqual,
                            left: Box::new(Expression::Column("maxValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                        Expression::BinaryOperation {
                            op: BinaryOperator::LessThanOrEqual,
                            left: Box::new(Expression::Column("minValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                    ],
                },
            ),
            (
                lit_int.clone().eq(column.clone()),
                Expression::VariadicOperation {
                    op: VariadicOperator::And,
                    exprs: vec![
                        Expression::BinaryOperation {
                            op: BinaryOperator::GreaterThanOrEqual,
                            left: Box::new(Expression::Column("maxValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                        Expression::BinaryOperation {
                            op: BinaryOperator::LessThanOrEqual,
                            left: Box::new(Expression::Column("minValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                    ],
                },
            ),
            (
                column.clone().ne(lit_int.clone()),
                Expression::VariadicOperation {
                    op: VariadicOperator::Or,
                    exprs: vec![
                        Expression::BinaryOperation {
                            op: BinaryOperator::LessThan,
                            left: Box::new(Expression::Column("maxValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                        Expression::BinaryOperation {
                            op: BinaryOperator::GreaterThan,
                            left: Box::new(Expression::Column("minValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                    ],
                },
            ),
            (
                lit_int.clone().ne(column.clone()),
                Expression::VariadicOperation {
                    op: VariadicOperator::Or,
                    exprs: vec![
                        Expression::BinaryOperation {
                            op: BinaryOperator::LessThan,
                            left: Box::new(Expression::Column("maxValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                        Expression::BinaryOperation {
                            op: BinaryOperator::GreaterThan,
                            left: Box::new(Expression::Column("minValues.a".to_string())),
                            right: Box::new(lit_int.clone()),
                        },
                    ],
                },
            ),
        ];

        for (input, expected) in cases {
            let rewritten = rewite(&input).unwrap();
            assert_eq!(rewritten, expected)
        }
    }
}
