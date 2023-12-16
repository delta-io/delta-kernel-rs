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
use crate::expressions::{BinaryOperator, Expression as Expr, VariadicOperator};
use crate::schema::{SchemaRef, StructField, StructType};
use crate::{ExpressionEvaluator, TableClient};

/// Returns <op2> (if any) such that B <op2> A is equivalent to A <op> B.
fn commute(op: &BinaryOperator) -> Option<BinaryOperator> {
    use BinaryOperator::*;
    match op {
        GreaterThan => Some(LessThan),
        GreaterThanOrEqual => Some(LessThanOrEqual),
        LessThan => Some(GreaterThan),
        LessThanOrEqual => Some(GreaterThanOrEqual),
        Equal | NotEqual | Plus | Multiply => Some(op.clone()),
        _ => None,
    }
}

fn as_data_skipping_predicate(expr: &Expr) -> Option<Expr> {
    use BinaryOperator::*;
    use Expr::*;
    use VariadicOperator::*;

    match expr {
        BinaryOperation { op, left, right } => {
            let (op, col, val) = match (left.as_ref(), right.as_ref()) {
                (Column(col), Literal(val)) => (op.clone(), col, val),
                (Literal(val), Column(col)) => (commute(op)?, col, val),
                _ => return None, // unsupported combination of operands
            };
            let stats_col = match op {
                LessThan | LessThanOrEqual => "minValues",
                GreaterThan | GreaterThanOrEqual => "maxValues",
                Equal => {
                    let exprs = [
                        Expr::binary(LessThanOrEqual, Column(col.clone()), Literal(val.clone())),
                        Expr::binary(LessThanOrEqual, Literal(val.clone()), Column(col.clone())),
                    ];
                    return as_data_skipping_predicate(&Expr::variadic(And, exprs));
                }
                NotEqual => {
                    let exprs = [
                        Expr::binary(
                            GreaterThan,
                            Column(format!("minValues.{}", col)),
                            Literal(val.clone()),
                        ),
                        Expr::binary(
                            LessThan,
                            Column(format!("maxValues.{}", col)),
                            Literal(val.clone()),
                        ),
                    ];
                    return Some(Expr::variadic(Or, exprs));
                }
                _ => return None, // unsupported operation
            };
            let col = format!("{}.{}", stats_col, col);
            Some(Expr::binary(op, Column(col), Literal(val.clone())))
        }
        VariadicOperation {
            op: op @ VariadicOperator::And,
            exprs,
        } => Some(VariadicOperation {
            op: op.clone(),
            exprs: exprs
                .iter()
                .filter_map(as_data_skipping_predicate)
                .collect::<Vec<_>>(),
        }),
        VariadicOperation {
            op: op @ VariadicOperator::Or,
            exprs,
        } => Some(VariadicOperation {
            op: op.clone(),
            exprs: exprs
                .iter()
                .map(as_data_skipping_predicate)
                .collect::<Option<Vec<_>>>()?,
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
        predicate: &Option<Expr>,
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
            StructField::new("maxValues", StructType::new(data_fields), true),
        ]));

        let skipping_predicate = as_data_skipping_predicate(predicate)?;
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

    #[test]
    fn test_rewrite_basic_comparison() {
        let column = Expr::column("a");
        let lit_int = Expr::literal(1_i32);
        let min_col = Expr::column("minValues.a");
        let max_col = Expr::column("maxValues.a");

        let cases = [
            (
                column.clone().lt(lit_int.clone()),
                Expr::binary(BinaryOperator::LessThan, &min_col, &lit_int),
            ),
            (
                lit_int.clone().lt(column.clone()),
                Expr::binary(BinaryOperator::GreaterThan, &max_col, &lit_int),
            ),
            (
                column.clone().gt(lit_int.clone()),
                Expr::binary(BinaryOperator::GreaterThan, &max_col, &lit_int),
            ),
            (
                lit_int.clone().gt(column.clone()),
                Expr::binary(BinaryOperator::LessThan, &min_col, &lit_int),
            ),
            (
                column.clone().lt_eq(lit_int.clone()),
                Expr::binary(BinaryOperator::LessThanOrEqual, &min_col, &lit_int),
            ),
            (
                lit_int.clone().lt_eq(column.clone()),
                Expr::binary(BinaryOperator::GreaterThanOrEqual, &max_col, &lit_int),
            ),
            (
                column.clone().gt_eq(lit_int.clone()),
                Expr::binary(BinaryOperator::GreaterThanOrEqual, &max_col, &lit_int),
            ),
            (
                lit_int.clone().gt_eq(column.clone()),
                Expr::binary(BinaryOperator::LessThanOrEqual, &min_col, &lit_int),
            ),
            (
                column.clone().eq(lit_int.clone()),
                Expr::variadic(
                    VariadicOperator::And,
                    [
                        Expr::binary(BinaryOperator::LessThanOrEqual, &min_col, &lit_int),
                        Expr::binary(BinaryOperator::GreaterThanOrEqual, &max_col, &lit_int),
                    ],
                ),
            ),
            (
                lit_int.clone().eq(column.clone()),
                Expr::variadic(
                    VariadicOperator::And,
                    [
                        Expr::binary(BinaryOperator::LessThanOrEqual, &min_col, &lit_int),
                        Expr::binary(BinaryOperator::GreaterThanOrEqual, &max_col, &lit_int),
                    ],
                ),
            ),
            (
                column.clone().ne(lit_int.clone()),
                Expr::variadic(
                    VariadicOperator::Or,
                    [
                        Expr::binary(BinaryOperator::GreaterThan, &min_col, &lit_int),
                        Expr::binary(BinaryOperator::LessThan, &max_col, &lit_int),
                    ],
                ),
            ),
            (
                lit_int.clone().ne(column.clone()),
                Expr::variadic(
                    VariadicOperator::Or,
                    [
                        Expr::binary(BinaryOperator::GreaterThan, &min_col, &lit_int),
                        Expr::binary(BinaryOperator::LessThan, &max_col, &lit_int),
                    ],
                ),
            ),
        ];

        for (input, expected) in cases {
            let rewritten = as_data_skipping_predicate(&input).unwrap();
            assert_eq!(rewritten, expected)
        }
    }
}
