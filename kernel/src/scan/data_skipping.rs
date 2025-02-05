use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::{Arc, LazyLock};

use tracing::debug;

use crate::actions::get_log_add_schema;
use crate::actions::visitors::SelectionVectorVisitor;
use crate::error::DeltaResult;
use crate::expressions::{
    column_expr, joined_column_expr, BinaryOperator, ColumnName, Expression as Expr, ExpressionRef,
    Scalar, VariadicOperator,
};
use crate::predicates::{
    DataSkippingPredicateEvaluator, PredicateEvaluator, PredicateEvaluatorDefaults,
};
use crate::schema::{DataType, PrimitiveType, SchemaRef, SchemaTransform, StructField, StructType};
use crate::{Engine, EngineData, ExpressionEvaluator, JsonHandler, RowVisitor as _};

#[cfg(test)]
mod tests;

/// Rewrites a predicate to a predicate that can be used to skip files based on their stats.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// We normalize each binary operation to a comparison between a column and a literal value and
/// rewrite that in terms of the min/max values of the column.
/// For example, `1 < a` is rewritten as `minValues.a > 1`.
///
/// For Unary `Not`, we push the Not down using De Morgan's Laws to invert everything below the Not.
///
/// Unary `IsNull` checks if the null counts indicate that the column could contain a null.
///
/// The variadic operations are rewritten as follows:
/// - `AND` is rewritten as a conjunction of the rewritten operands where we just skip operands that
///         are not eligible for data skipping.
/// - `OR` is rewritten only if all operands are eligible for data skipping. Otherwise, the whole OR
///        expression is dropped.
#[cfg(test)]
fn as_data_skipping_predicate(expr: &Expr) -> Option<Expr> {
    DataSkippingPredicateCreator.eval(expr)
}

/// Like `as_data_skipping_predicate`, but invokes [`PredicateEvaluator::eval_sql_where`] instead
/// of [`PredicateEvaluator::eval_expr`].
fn as_sql_data_skipping_predicate(expr: &Expr) -> Option<Expr> {
    DataSkippingPredicateCreator.eval_sql_where(expr)
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
        engine: &dyn Engine,
        physical_predicate: Option<(ExpressionRef, SchemaRef)>,
    ) -> Option<Self> {
        static PREDICATE_SCHEMA: LazyLock<DataType> = LazyLock::new(|| {
            DataType::struct_type([StructField::nullable("predicate", DataType::BOOLEAN)])
        });
        static STATS_EXPR: LazyLock<Expr> = LazyLock::new(|| column_expr!("add.stats"));
        static FILTER_EXPR: LazyLock<Expr> =
            LazyLock::new(|| column_expr!("predicate").distinct(false));

        let (predicate, referenced_schema) = physical_predicate?;
        debug!("Creating a data skipping filter for {:#?}", predicate);

        // Convert a min/max stats schema into a nullcount schema (all leaf fields are LONG)
        struct NullCountStatsTransform;
        impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
            fn transform_primitive(
                &mut self,
                _ptype: &'a PrimitiveType,
            ) -> Option<Cow<'a, PrimitiveType>> {
                Some(Cow::Owned(PrimitiveType::Long))
            }
        }
        let nullcount_schema = NullCountStatsTransform
            .transform_struct(&referenced_schema)?
            .into_owned();
        let stats_schema = Arc::new(StructType::new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", nullcount_schema),
            StructField::nullable("minValues", referenced_schema.clone()),
            StructField::nullable("maxValues", referenced_schema),
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
        let select_stats_evaluator = engine.get_expression_handler().get_evaluator(
            // safety: kernel is very broken if we don't have the schema for Add actions
            get_log_add_schema().clone(),
            STATS_EXPR.clone(),
            DataType::STRING,
        );

        let skipping_evaluator = engine.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            Expr::struct_from([as_sql_data_skipping_predicate(&predicate)?]),
            PREDICATE_SCHEMA.clone(),
        );

        let filter_evaluator = engine.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            FILTER_EXPR.clone(),
            DataType::BOOLEAN,
        );

        Some(Self {
            stats_schema,
            select_stats_evaluator,
            skipping_evaluator,
            filter_evaluator,
            json_handler: engine.get_json_handler(),
        })
    }

    /// Apply the DataSkippingFilter to an EngineData batch of actions. Returns a selection vector
    /// which can be applied to the actions to find those that passed data skipping.
    pub(crate) fn apply(&self, actions: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        // retrieve and parse stats from actions data
        let stats = self.select_stats_evaluator.evaluate(actions)?;
        assert_eq!(stats.len(), actions.len());
        let parsed_stats = self
            .json_handler
            .parse_json(stats, self.stats_schema.clone())?;
        assert_eq!(parsed_stats.len(), actions.len());

        // evaluate the predicate on the parsed stats, then convert to selection vector
        let skipping_predicate = self.skipping_evaluator.evaluate(&*parsed_stats)?;
        assert_eq!(skipping_predicate.len(), actions.len());
        let selection_vector = self
            .filter_evaluator
            .evaluate(skipping_predicate.as_ref())?;
        assert_eq!(selection_vector.len(), actions.len());

        // visit the engine's selection vector to produce a Vec<bool>
        let mut visitor = SelectionVectorVisitor::default();
        visitor.visit_rows_of(selection_vector.as_ref())?;
        Ok(visitor.selection_vector)

        // TODO(zach): add some debug info about data skipping that occurred
        // let before_count = actions.length();
        // debug!(
        //     "number of actions before/after data skipping: {before_count} / {}",
        //     filtered_actions.num_rows()
        // );
    }
}

struct DataSkippingPredicateCreator;

impl DataSkippingPredicateEvaluator for DataSkippingPredicateCreator {
    type Output = Expr;
    type TypedStat = Expr;
    type IntStat = Expr;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Expr> {
        Some(joined_column_expr!("minValues", col))
    }

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    fn get_max_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Expr> {
        Some(joined_column_expr!("maxValues", col))
    }

    /// Retrieves the null count of a column, if it exists.
    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        Some(joined_column_expr!("nullCount", col))
    }

    /// Retrieves the row count of a column (parquet footers always include this stat).
    fn get_rowcount_stat(&self) -> Option<Expr> {
        Some(column_expr!("numRecords"))
    }

    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Expr,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Expr> {
        let op = match (ord, inverted) {
            (Ordering::Less, false) => BinaryOperator::LessThan,
            (Ordering::Less, true) => BinaryOperator::GreaterThanOrEqual,
            (Ordering::Equal, false) => BinaryOperator::Equal,
            (Ordering::Equal, true) => BinaryOperator::NotEqual,
            (Ordering::Greater, false) => BinaryOperator::GreaterThan,
            (Ordering::Greater, true) => BinaryOperator::LessThanOrEqual,
        };
        Some(Expr::binary(op, col, val.clone()))
    }

    fn eval_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Expr> {
        PredicateEvaluatorDefaults::eval_scalar_is_null(val, inverted).map(Expr::literal)
    }

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<Expr> {
        PredicateEvaluatorDefaults::eval_scalar(val, inverted).map(Expr::literal)
    }

    fn eval_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Expr> {
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?, // all-null
            false => Expr::literal(0i64),      // no-null
        };
        Some(Expr::ne(self.get_nullcount_stat(col)?, safe_to_skip))
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Expr> {
        PredicateEvaluatorDefaults::eval_binary_scalars(op, left, right, inverted)
            .map(Expr::literal)
    }

    fn finish_eval_variadic(
        &self,
        mut op: VariadicOperator,
        exprs: impl IntoIterator<Item = Option<Expr>>,
        inverted: bool,
    ) -> Option<Expr> {
        if inverted {
            op = op.invert();
        }
        // NOTE: We can potentially see a LOT of NULL inputs in a big WHERE clause with lots of
        // unsupported data skipping operations. We can't "just" flatten them all away for AND,
        // because that could produce TRUE where NULL would otherwise be expected. Similarly, we
        // don't want to "just" try_collect inputs for OR, because that can cause OR to produce NULL
        // where FALSE would otherwise be expected. So, we filter out all nulls except the first,
        // observing that one NULL is enough to produce the correct behavior during predicate eval.
        let mut keep_null = true;
        let exprs: Vec<_> = exprs
            .into_iter()
            .flat_map(|e| match e {
                Some(expr) => Some(expr),
                None => keep_null.then(|| {
                    keep_null = false;
                    Expr::null_literal(DataType::BOOLEAN)
                }),
            })
            .collect();
        Some(Expr::variadic(op, exprs))
    }
}
