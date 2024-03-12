use std::collections::HashSet;
use std::sync::Arc;

use tracing::debug;

use crate::actions::visitors::SelectionVectorVisitor;
use crate::error::DeltaResult;
use crate::expressions::{BinaryOperator, Expression as Expr, VariadicOperator};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{EngineData, EngineInterface, ExpressionEvaluator, JsonHandler};

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

/// Rewrites a predicate to a predicate that can be used to skip files based on their stats.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// We normalize each binary operation to a comparison between a column and a literal value
/// and rewite that in terms of the min/max values of the column.
/// For example, `1 < a` is rewritten as `minValues.a > 1`.
///
/// The variadic operations are rewritten as follows:
/// - `AND` is rewritten as a conjunction of the rewritten operands where we just skip
///   operands that are not eligible for data skipping.
/// - `OR` is rewritten only if all operands are eligible for data skipping. Otherwise,
///   the whole OR expression is dropped.
fn as_data_skipping_predicate(expr: &Expr) -> Option<Expr> {
    use BinaryOperator::*;
    use Expr::*;

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
                        Expr::le(Column(col.clone()), Literal(val.clone())),
                        Expr::le(Literal(val.clone()), Column(col.clone())),
                    ];
                    return as_data_skipping_predicate(&Expr::and_from(exprs));
                }
                NotEqual => {
                    let exprs = [
                        Expr::gt(Column(format!("minValues.{}", col)), Literal(val.clone())),
                        Expr::lt(Column(format!("maxValues.{}", col)), Literal(val.clone())),
                    ];
                    return Some(Expr::or_from(exprs));
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
    select_stats_evaluator: Arc<dyn ExpressionEvaluator>,
    skipping_evaluator: Arc<dyn ExpressionEvaluator>,
    filter_evaluator: Arc<dyn ExpressionEvaluator>,
    json_handler: Arc<dyn JsonHandler>,
}

impl DataSkippingFilter {
    /// Creates a new data skipping filter. Returns None if there is no predicate, or the predicate
    /// is ineligible for data skipping.
    ///
    /// NOTE: None is equivalent to a trivial filter that always returns TRUE (= keeps all files),
    /// but using an Option lets the engine easily avoid the overhead of applying trivial filters.
    pub(crate) fn new(
        table_client: &dyn EngineInterface,
        table_schema: &SchemaRef,
        predicate: &Option<Expr>,
    ) -> Option<Self> {
        lazy_static::lazy_static!(
            static ref PREDICATE_SCHEMA: DataType = StructType::new(vec![
                StructField::new("predicate", DataType::BOOLEAN, true),
            ]).into();
            static ref STATS_EXPR: Expr = Expr::column("add.stats");
            static ref FILTER_EXPR: Expr = Expr::column("predicate").distinct(Expr::literal(false));
        );

        let predicate = match predicate {
            Some(predicate) => predicate,
            None => return None,
        };

        debug!("Creating a data skipping filter for {}", &predicate);
        let field_names: HashSet<_> = predicate.references();

        // Build the stats read schema by extracting the column names referenced by the predicate,
        // extracting the corresponding field from the table schema, and inserting that field.
        let data_fields: Vec<_> = table_schema
            .fields()
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

        // Skipping happens in several steps:
        //
        // 1. The stats selector fetches add.stats from the metadata
        //
        // 2. The predicate (skipping evaluator) produces false for any file whose stats prove we
        //    can safely skip it. A value of true means the stats say we must keep the file, and
        //    null means we could not determine whether the file is safe to skip, because its stats
        //    were missing/null.
        //
        // 3. The selection evaluator does DISTINCT(col(predicate), 'false') to produce true (= keep) when
        //    the predicate is true/null and false (= skip) when the predicate is false.
        let select_stats_evaluator = table_client.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            STATS_EXPR.clone(),
            DataType::STRING,
        );

        let skipping_evaluator = table_client.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            Expr::struct_expr([as_data_skipping_predicate(predicate)?]),
            PREDICATE_SCHEMA.clone(),
        );

        let filter_evaluator = table_client.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            FILTER_EXPR.clone(),
            DataType::BOOLEAN,
        );

        Some(Self {
            stats_schema,
            select_stats_evaluator,
            skipping_evaluator,
            filter_evaluator,
            json_handler: table_client.get_json_handler(),
        })
    }

    /// Apply the DataSkippingFilter to an EngineData batch of actions. Returns a selection vector
    /// which can be applied to the actions to find those that passed data skipping.
    pub(crate) fn apply(&self, actions: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        // retrieve and parse stats from actions data
        let stats = self.select_stats_evaluator.evaluate(actions)?;
        let parsed_stats = self
            .json_handler
            .parse_json(stats, self.stats_schema.clone())?;

        // evaluate the predicate on the parsed stats, then convert to selection vector
        let skipping_predicate = self.skipping_evaluator.evaluate(&*parsed_stats)?;
        let selection_vector = self
            .filter_evaluator
            .evaluate(skipping_predicate.as_ref())?;

        // visit the engine's selection vector to produce a Vec<bool>
        let mut visitor = SelectionVectorVisitor::default();
        let schema = StructType::new(vec![StructField::new("output", DataType::BOOLEAN, false)]);
        selection_vector
            .as_ref()
            .extract(Arc::new(schema), &mut visitor)?;
        Ok(visitor.selection_vector)

        // TODO(zach): add some debug info about data skipping that occurred
        // let before_count = actions.length();
        // debug!(
        //     "number of actions before/after data skipping: {before_count} / {}",
        //     filtered_actions.num_rows()
        // );
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
                Expr::lt(min_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().lt(column.clone()),
                Expr::gt(max_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().gt(lit_int.clone()),
                Expr::gt(max_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().gt(column.clone()),
                Expr::lt(min_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().lt_eq(lit_int.clone()),
                Expr::le(min_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().lt_eq(column.clone()),
                Expr::ge(max_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().gt_eq(lit_int.clone()),
                Expr::ge(max_col.clone(), lit_int.clone()),
            ),
            (
                lit_int.clone().gt_eq(column.clone()),
                Expr::le(min_col.clone(), lit_int.clone()),
            ),
            (
                column.clone().eq(lit_int.clone()),
                Expr::and_from([
                    Expr::le(min_col.clone(), lit_int.clone()),
                    Expr::ge(max_col.clone(), lit_int.clone()),
                ]),
            ),
            (
                lit_int.clone().eq(column.clone()),
                Expr::and_from([
                    Expr::le(min_col.clone(), lit_int.clone()),
                    Expr::ge(max_col.clone(), lit_int.clone()),
                ]),
            ),
            (
                column.clone().ne(lit_int.clone()),
                Expr::or_from([
                    Expr::gt(min_col.clone(), lit_int.clone()),
                    Expr::lt(max_col.clone(), lit_int.clone()),
                ]),
            ),
            (
                lit_int.clone().ne(column.clone()),
                Expr::or_from([
                    Expr::gt(min_col.clone(), lit_int.clone()),
                    Expr::lt(max_col.clone(), lit_int.clone()),
                ]),
            ),
        ];

        for (input, expected) in cases {
            let rewritten = as_data_skipping_predicate(&input).unwrap();
            assert_eq!(rewritten, expected)
        }
    }
}
