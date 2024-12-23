use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray};
use tracing::debug;

use crate::actions::get_log_add_schema;
use crate::actions::visitors::SelectionVectorVisitor;
use crate::engine::arrow_data::ArrowEngineData;
use crate::error::DeltaResult;
use crate::expressions::{
    column_expr, joined_column_expr, BinaryOperator, ColumnName, Expression as Expr, ExpressionRef,
    Scalar, VariadicOperator,
};
use crate::predicates::{
    DataSkippingPredicateEvaluator, PredicateEvaluator, PredicateEvaluatorDefaults,
};
use crate::schema::{
    DataType, MapType, PrimitiveType, SchemaRef, SchemaTransform, StructField, StructType,
};
use crate::{Engine, EngineData, Error, ExpressionEvaluator, JsonHandler, RowVisitor as _};

#[cfg(test)]
mod tests;

/// Rewrites a predicate to a predicate that can be used to skip files based on their stats.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// We normalize each binary operation to a comparison between a column and a literal value and
/// rewite that in terms of the min/max values of the column.
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
fn as_data_skipping_predicate(expr: &Expr, inverted: bool) -> Option<Expr> {
    DataSkippingPredicateCreator.eval_expr(expr, inverted)
}

pub(crate) struct DataSkippingFilter {
    stats_schema: SchemaRef,
    select_stats_evaluator: Arc<dyn ExpressionEvaluator>,
    partitions_evaluator: Arc<dyn ExpressionEvaluator>,
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
            DataType::struct_type([StructField::new("predicate", DataType::BOOLEAN, true)])
        });
        static PARITIONS_EXPR: LazyLock<Expr> =
            LazyLock::new(|| column_expr!("add.partitionValues"));
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
            StructField::new("numRecords", DataType::LONG, true),
            StructField::new("nullCount", nullcount_schema, true),
            StructField::new("minValues", referenced_schema.clone(), true),
            StructField::new("maxValues", referenced_schema, true),
        ]));

        let partitions_map_type = MapType::new(DataType::STRING, DataType::STRING, true);

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

        let partitions_evaluator = engine.get_expression_handler().get_evaluator(
            get_log_add_schema().clone(),
            PARITIONS_EXPR.clone(),
            partitions_map_type.into(),
        );

        let skipping_evaluator = engine.get_expression_handler().get_evaluator(
            stats_schema.clone(),
            Expr::struct_from([as_data_skipping_predicate(&predicate, false)?]),
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
            partitions_evaluator,
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

        let parsed_partitions = self.partitions_evaluator.evaluate(actions)?;
        assert_eq!(parsed_partitions.len(), actions.len());

        let parsed_stats = merge_partitions_into_stats(parsed_partitions, parsed_stats)?;

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

/// This function computes the values for the partition arrays that are added to the stats
/// fields to follow. Since the partition columns are a MapArray, we need to find for each
/// key the value assigned for each log.
fn compute_partition_arrays(
    partitions_column: &ArrayRef,
    output_schema: &Arc<arrow_schema::Schema>,
) -> DeltaResult<HashMap<String, ArrayRef>> {
    let output_types: HashMap<String, arrow_schema::DataType> =
        match output_schema.field_with_name("minValues")?.data_type() {
            arrow_schema::DataType::Struct(fields) => fields
                .iter()
                .map(|field| (field.name().to_owned(), field.data_type().to_owned())),
            _ => return Err(Error::engine_data_type("minValues")),
        }
        .collect();

    let partitions_array = partitions_column
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| Error::engine_data_type("Partitions"))?;

    let keys: HashSet<String> = partitions_array
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::engine_data_type("Partition keys"))?
        .iter()
        .filter_map(|s| s.map(|t| t.to_string()))
        .collect();

    let partition_values: HashMap<String, _> = keys
        .iter()
        .filter_map(|key| {
            let cast_type = output_types.get(key)?;

            let values = partitions_array
                .iter()
                .map(|maybe_partition| {
                    maybe_partition.and_then(|partition_data| {
                        let keys = partition_data
                            .column_by_name("key")?
                            .as_any()
                            .downcast_ref::<StringArray>()?;
                        let values = partition_data
                            .column_by_name("value")?
                            .as_any()
                            .downcast_ref::<StringArray>()?;

                        let mut kv =
                            keys.iter()
                                .zip(values.iter())
                                .filter_map(|(k, v)| match (k, v) {
                                    (Some(k), Some(v)) => Some((k, v)),
                                    _ => None,
                                });

                        kv.find(|(k, _)| *k == key.as_str())
                            .map(|(_, v)| v.to_string())
                    })
                })
                .collect::<Vec<Option<String>>>();

            let string_array = StringArray::from(values);
            let value_array = arrow_cast::cast(&string_array, &cast_type).ok()?;
            Some((key.to_owned(), value_array))
        })
        .collect();

    Ok(partition_values)
}

/// This funtion builds up the stats fields for the min and max values. It assumes
/// that the arrays for the partition fields already exist. It will only build those
/// that match the predicate filters.
fn merge_partition_fields_into_stats(
    stats_batch: &RecordBatch,
    idx: usize,
    partition_values: &HashMap<String, Arc<dyn Array>>,
) -> DeltaResult<Arc<dyn arrow_array::Array + 'static>> {
    let (fields, mut arrays, nulls) = stats_batch
        .column(idx)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::engine_data_type("minValues"))?
        .to_owned()
        .into_parts();
    for (idx, field) in itertools::enumerate(fields.iter()) {
        if let Some(arr) = partition_values.get(field.name()) {
            arrays[idx] = Arc::clone(arr);
        }
    }
    Ok(Arc::new(StructArray::new(fields, arrays, nulls))
        as Arc<(dyn arrow_array::Array + 'static)>)
}

/// This function adds partition data to the stats fields. For each partition field for a log
/// it adds the partition value to both the `minValues` and `maxValues` fields, so that when
/// we match against it with the data skipping filters we can effectively skip files.
fn merge_partitions_into_stats(
    partitions: Box<dyn EngineData>,
    stats: Box<dyn EngineData>,
) -> DeltaResult<Box<dyn EngineData>> {
    let partitions = ArrowEngineData::try_from_engine_data(partitions)?;
    let partitions_batch = partitions.record_batch();

    // If the struct is partitions data is emtpy, return the original stats
    let partitions_column = match partitions_batch.column_by_name("output") {
        Some(c) => c,
        None => return Ok(stats),
    };

    let stats = ArrowEngineData::try_from_engine_data(stats)?;
    let stats_batch = stats.record_batch();
    let output_schema = stats_batch.schema();

    // For each unique partition key, generate the associated array
    // to add to the stats fields
    let partition_values = compute_partition_arrays(partitions_column, &output_schema)?;
    if partition_values.is_empty() {
        return Ok(stats);
    }

    let mut columns = Vec::default();
    for (idx, field) in itertools::enumerate(output_schema.fields()) {
        match field.name().as_str() {
            "minValues" => columns.push(merge_partition_fields_into_stats(
                stats_batch,
                idx,
                &partition_values,
            )?),
            "maxValues" => columns.push(merge_partition_fields_into_stats(
                stats_batch,
                idx,
                &partition_values,
            )?),
            _ => {
                columns.push(Arc::clone(stats_batch.column(idx)));
            }
        }
    }

    let record_batch = RecordBatch::try_new(output_schema, columns)?;
    Ok(Box::new(ArrowEngineData::new(record_batch)))
}
