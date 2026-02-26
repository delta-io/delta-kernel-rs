use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use tracing::{debug, error, info};

use crate::actions::visitors::SelectionVectorVisitor;
use crate::error::DeltaResult;
use crate::expressions::{
    column_expr, joined_column_expr, BinaryPredicateOp, ColumnName, Expression as Expr,
    ExpressionRef, JunctionPredicateOp, OpaquePredicateOpRef, Predicate as Pred, PredicateRef,
    Scalar,
};
use crate::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator, KernelPredicateEvaluatorDefaults,
};
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::utils::require;
use crate::{Engine, EngineData, Error, ExpressionEvaluator, PredicateEvaluator, RowVisitor as _};

pub(crate) mod stats_schema;
#[cfg(test)]
mod tests;

use delta_kernel_derive::internal_api;

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
/// The junction operations are rewritten as follows:
/// - `AND` is rewritten as a conjunction of the rewritten operands where we just skip operands that
///   are not eligible for data skipping.
/// - `OR` is rewritten only if all operands are eligible for data skipping. Otherwise, the whole OR
///   predicate is dropped.
#[cfg(test)]
pub(crate) fn as_data_skipping_predicate(pred: &Pred) -> Option<Pred> {
    DataSkippingPredicateCreator::new(Default::default()).eval(pred)
}

#[cfg(test)]
pub(crate) fn as_data_skipping_predicate_with_partitions(
    pred: &Pred,
    partition_columns: HashSet<String>,
) -> Option<Pred> {
    DataSkippingPredicateCreator::new(partition_columns).eval(pred)
}

/// Like `as_data_skipping_predicate`, but invokes [`KernelPredicateEvaluator::eval_sql_where`]
/// instead of [`KernelPredicateEvaluator::eval`].
fn as_sql_data_skipping_predicate(
    pred: &Pred,
    partition_columns: &HashSet<String>,
) -> Option<Pred> {
    DataSkippingPredicateCreator::new(partition_columns.clone()).eval_sql_where(pred)
}

#[internal_api]
pub(crate) struct DataSkippingFilter {
    /// Evaluator that extracts file-level statistics from the input batch. The caller provides
    /// the expression at construction time, which determines where stats come from:
    /// - Scan path: `column_expr!("stats_parsed")` reads the already-parsed struct from
    ///   a transformed batch (where `add.*` fields are flattened to top-level columns).
    /// - Table changes path: `Expression::parse_json(column_expr!("add.stats"), schema)` parses
    ///   JSON from a raw action batch (where stats are nested under `add.stats`).
    stats_evaluator: Arc<dyn ExpressionEvaluator>,
    skipping_evaluator: Arc<dyn PredicateEvaluator>,
    filter_evaluator: Arc<dyn PredicateEvaluator>,
}

impl DataSkippingFilter {
    /// Creates a new data skipping filter. Returns `None` if there is no predicate, or the
    /// predicate is ineligible for data skipping.
    ///
    /// NOTE: `None` is equivalent to a trivial filter that always returns TRUE (= keeps all files),
    /// but using an `Option` lets the engine easily avoid the overhead of applying trivial filters.
    ///
    /// # Parameters
    /// - `engine`: Engine for creating evaluators
    /// - `predicate`: Optional predicate for data skipping
    /// - `stats_schema`: The data stats schema (numRecords, nullCount, minValues, maxValues).
    ///   Pass `None` if no data stats are available.
    /// - `stats_expr`: Expression to extract data stats from the batch, producing output matching
    ///   `stats_schema`. For example, `column_expr!("stats_parsed")` for pre-parsed stats, or
    ///   `Expression::parse_json(column_expr!("add.stats"), stats_schema)` for JSON parsing.
    /// - `partition_schema`: Schema of typed partition columns referenced by the predicate
    ///   (physical names). Pass `None` if no partition columns are referenced.
    /// - `partition_expr`: Expression to extract partition values from the batch, producing output
    ///   matching `partition_schema`. Typically a `MapToStruct` expression that converts the
    ///   `partitionValues` string map into a typed struct.
    /// - `input_schema`: Schema of the batch that will be passed to [`apply()`](Self::apply)
    pub(crate) fn new(
        engine: &dyn Engine,
        predicate: Option<PredicateRef>,
        stats_schema: Option<&SchemaRef>,
        stats_expr: ExpressionRef,
        partition_schema: Option<&SchemaRef>,
        partition_expr: Option<ExpressionRef>,
        input_schema: SchemaRef,
    ) -> Option<Self> {
        static FILTER_PRED: LazyLock<PredicateRef> =
            LazyLock::new(|| Arc::new(column_expr!("output").distinct(Expr::literal(false))));

        let predicate = predicate?;
        debug!("Creating a data skipping filter for {:#?}", predicate);

        // Build the unified evaluation schema and extraction expression. Data stats and partition
        // values are conceptually separate, but the evaluator needs a single schema/expression.
        let (unified_schema, unified_expr, partition_columns) = Self::build_unified_schema_and_expr(
            stats_schema,
            stats_expr,
            partition_schema,
            partition_expr,
        )?;

        let stats_evaluator = engine
            .evaluation_handler()
            .new_expression_evaluator(
                input_schema,
                unified_expr,
                unified_schema.as_ref().clone().into(),
            )
            .inspect_err(|e| error!("Failed to create stats evaluator: {e}"))
            .ok()?;

        // Skipping happens in several steps:
        //
        // 1. The stats evaluator extracts file-level statistics from the batch (the expression
        //    provided by the caller determines how: reading a pre-parsed column, parsing JSON, etc.)
        //
        // 2. The predicate (skipping evaluator) produces false for any file whose stats prove we
        //    can safely skip it. A value of true means the stats say we must keep the file, and
        //    null means we could not determine whether the file is safe to skip, because its stats
        //    were missing/null.
        //
        // 3. The selection evaluator does DISTINCT(col(predicate), 'false') to produce true
        //    (= keep) when the predicate is true/null and false (= skip) when the predicate
        //    is false.
        let skipping_evaluator = engine
            .evaluation_handler()
            .new_predicate_evaluator(
                unified_schema.clone(),
                Arc::new(as_sql_data_skipping_predicate(
                    &predicate,
                    &partition_columns,
                )?),
            )
            .inspect_err(|e| error!("Failed to create skipping evaluator: {e}"))
            .ok()?;

        let filter_evaluator = engine
            .evaluation_handler()
            .new_predicate_evaluator(unified_schema, FILTER_PRED.clone())
            .inspect_err(|e| error!("Failed to create filter evaluator: {e}"))
            .ok()?;

        Some(Self {
            stats_evaluator,
            skipping_evaluator,
            filter_evaluator,
        })
    }

    /// Builds the unified schema and extraction expression from separate data stats and partition
    /// value inputs. Returns `None` if neither stats nor partition values are available.
    fn build_unified_schema_and_expr(
        stats_schema: Option<&SchemaRef>,
        stats_expr: ExpressionRef,
        partition_schema: Option<&SchemaRef>,
        partition_expr: Option<ExpressionRef>,
    ) -> Option<(SchemaRef, ExpressionRef, HashSet<String>)> {
        let has_stats = stats_schema.is_some();
        let pv_schema = partition_schema.filter(|s| s.num_fields() > 0);
        let has_partitions = pv_schema.is_some();

        if !has_stats && !has_partitions {
            return None;
        }

        let partition_columns: HashSet<String> = pv_schema
            .map(|s| s.fields().map(|f| f.name().to_string()).collect())
            .unwrap_or_default();

        let unified_schema = match (stats_schema, pv_schema) {
            (Some(stats), Some(pv)) => {
                let mut fields: Vec<StructField> = stats.fields().cloned().collect();
                fields.push(StructField::nullable(
                    "partitionValues",
                    DataType::Struct(Box::new(pv.as_ref().clone())),
                ));
                Arc::new(StructType::new_unchecked(fields))
            }
            (Some(stats), None) => stats.clone(),
            (None, Some(pv)) => Arc::new(StructType::new_unchecked([StructField::nullable(
                "partitionValues",
                DataType::Struct(Box::new(pv.as_ref().clone())),
            )])),
            (None, None) => unreachable!("checked above"),
        };

        let unified_expr = match (has_stats, pv_schema, partition_expr) {
            (true, Some(_), Some(pv_expr)) => {
                // Combine data stats fields + partition values into a unified struct
                Arc::new(Expr::Struct(vec![
                    Arc::new(Expr::column(["stats_parsed", "numRecords"])),
                    Arc::new(Expr::column(["stats_parsed", "nullCount"])),
                    Arc::new(Expr::column(["stats_parsed", "minValues"])),
                    Arc::new(Expr::column(["stats_parsed", "maxValues"])),
                    pv_expr,
                ]))
            }
            (false, Some(_), Some(pv_expr)) => {
                // Only partition columns, no data stats
                Arc::new(Expr::Struct(vec![pv_expr]))
            }
            _ => stats_expr,
        };

        Some((unified_schema, unified_expr, partition_columns))
    }

    /// Apply the DataSkippingFilter to an EngineData batch. Returns a selection vector
    /// which can be applied to the batch to find rows that passed data skipping.
    pub(crate) fn apply(&self, batch: &dyn EngineData) -> DeltaResult<Vec<bool>> {
        let batch_len = batch.len();

        let file_stats = self.stats_evaluator.evaluate(batch)?;
        debug_assert_eq!(file_stats.len(), batch_len);
        require!(
            file_stats.len() == batch_len,
            Error::internal_error(format!(
                "stats evaluator output length {} != batch length {}",
                file_stats.len(),
                batch_len
            ))
        );

        let skipping_predicate = self.skipping_evaluator.evaluate(&*file_stats)?;
        debug_assert_eq!(skipping_predicate.len(), batch_len);
        require!(
            skipping_predicate.len() == batch_len,
            Error::internal_error(format!(
                "skipping evaluator output length {} != batch length {}",
                skipping_predicate.len(),
                batch_len
            ))
        );

        let selection_vector = self
            .filter_evaluator
            .evaluate(skipping_predicate.as_ref())?;
        debug_assert_eq!(selection_vector.len(), batch_len);
        require!(
            selection_vector.len() == batch_len,
            Error::internal_error(format!(
                "filter evaluator output length {} != batch length {}",
                selection_vector.len(),
                batch_len
            ))
        );

        let mut visitor = SelectionVectorVisitor::default();
        visitor.visit_rows_of(selection_vector.as_ref())?;

        let skipped = visitor
            .selection_vector
            .iter()
            .filter(|&&kept| !kept)
            .count();
        if skipped > 0 {
            info!("data skipping filtered {skipped}/{batch_len} rows from batch",);
        }

        Ok(visitor.selection_vector)
    }
}

/// Rewrites a predicate for parquet row group skipping in checkpoint/sidecar files.
/// Returns `None` if the predicate is not eligible for data skipping.
///
/// Adds IS NULL guards on each stat column reference so the parquet RowGroupFilter
/// conservatively keeps row groups containing files with missing stats (null stat values
/// are invisible to footer min/max). For example, `col_a > 100` becomes:
/// ```text
/// OR(maxValues.col_a IS NULL, maxValues.col_a > 100)
/// ```
///
/// Partition columns are excluded since their values live in `add.partitionValues_parsed`,
/// not `add.stats_parsed`.
pub(crate) fn as_checkpoint_skipping_predicate(
    pred: &Pred,
    partition_columns: &[String],
) -> Option<Pred> {
    let partition_columns: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    NullGuardedDataSkippingPredicateCreator { partition_columns }.eval(pred)
}

/// Maps an ordering and inversion flag to the corresponding comparison predicate.
fn comparison_predicate(ord: Ordering, col: Expr, val: &Scalar, inverted: bool) -> Pred {
    let pred_fn = match (ord, inverted) {
        (Ordering::Less, false) => Pred::lt,
        (Ordering::Less, true) => Pred::ge,
        (Ordering::Equal, false) => Pred::eq,
        (Ordering::Equal, true) => Pred::ne,
        (Ordering::Greater, false) => Pred::gt,
        (Ordering::Greater, true) => Pred::le,
    };
    pred_fn(col, val.clone())
}

/// Collects sub-predicates into a junction (AND/OR), replacing unsupported sub-predicates (None)
/// with a single NULL literal to preserve correct three-valued logic. One NULL is enough to
/// produce the correct behavior during predicate evaluation; additional NULLs are redundant.
fn collect_junction_preds(
    mut op: JunctionPredicateOp,
    preds: &mut dyn Iterator<Item = Option<Pred>>,
    inverted: bool,
) -> Pred {
    if inverted {
        op = op.invert();
    }
    let mut keep_null = true;
    let preds: Vec<_> = preds
        .flat_map(|p| match p {
            Some(pred) => Some(pred),
            None => keep_null.then(|| {
                keep_null = false;
                Pred::null_literal()
            }),
        })
        .collect();
    Pred::junction(op, preds)
}

/// Rewrites user predicates into stats-based predicates for data skipping.
///
/// For data columns, rewrites to `minValues.*`/`maxValues.*`/`nullCount.*` as before.
/// For partition columns, rewrites to `partitionValues.*` since the partition value is the exact
/// value for every row in the file (serving as both min and max).
struct DataSkippingPredicateCreator {
    /// Physical names of partition columns. For these columns, stats come from
    /// `partitionValues.<col>` (exact values) instead of min/max ranges.
    partition_columns: HashSet<String>,
}

impl DataSkippingPredicateCreator {
    fn new(partition_columns: HashSet<String>) -> Self {
        Self { partition_columns }
    }

    fn is_partition_column(&self, col: &ColumnName) -> bool {
        let path = col.path();
        path.len() == 1 && self.partition_columns.contains(path[0].as_str())
    }
}

impl DataSkippingPredicateEvaluator for DataSkippingPredicateCreator {
    type Output = Pred;
    type ColumnStat = Expr;

    /// Retrieves the minimum value of a column. For partition columns, returns the exact
    /// partition value (which serves as both min and max).
    fn get_min_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            Some(joined_column_expr!("partitionValues", col))
        } else {
            Some(joined_column_expr!("minValues", col))
        }
    }

    /// Retrieves the maximum value of a column. For partition columns, returns the exact
    /// partition value. For data columns, excludes timestamps due to millisecond truncation.
    // TODO(#1002): we currently don't support file skipping on timestamp columns' max stat since
    // they are truncated to milliseconds in add.stats.
    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            Some(joined_column_expr!("partitionValues", col))
        } else {
            match data_type {
                &DataType::TIMESTAMP | &DataType::TIMESTAMP_NTZ => None,
                _ => Some(joined_column_expr!("maxValues", col)),
            }
        }
    }

    /// Retrieves the null count of a column. Partition columns don't have nullCount stats.
    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        if self.is_partition_column(col) {
            None
        } else {
            Some(joined_column_expr!("nullCount", col))
        }
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
    ) -> Option<Pred> {
        Some(comparison_predicate(ord, col, val, inverted))
    }

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted).map(Pred::literal)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted).map(Pred::literal)
    }

    /// For partition columns, checks `partitionValues.<col> IS [NOT] NULL` directly.
    /// For data columns, uses nullCount stats.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Pred> {
        if self.is_partition_column(col) {
            let pv_expr = joined_column_expr!("partitionValues", col);
            if inverted {
                Some(Pred::is_not_null(pv_expr))
            } else {
                Some(Pred::is_null(pv_expr))
            }
        } else {
            let safe_to_skip = match inverted {
                true => self.get_rowcount_stat()?, // all-null
                false => Expr::literal(0i64),      // no-null
            };
            Some(Pred::ne(self.get_nullcount_stat(col)?, safe_to_skip))
        }
    }

    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
            .map(Pred::literal)
    }

    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expr],
        inverted: bool,
    ) -> Option<Pred> {
        op.as_data_skipping_predicate(self, exprs, inverted)
    }

    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Pred>>,
        inverted: bool,
    ) -> Option<Pred> {
        Some(collect_junction_preds(op, preds, inverted))
    }
}

/// Like [`DataSkippingPredicateCreator`] but adds IS NULL guards on stat column references
/// for safe parquet row group filtering. Partition columns are excluded since their values
/// live in `add.partitionValues_parsed`, not `add.stats_parsed`.
struct NullGuardedDataSkippingPredicateCreator<'a> {
    partition_columns: HashSet<&'a str>,
}

impl NullGuardedDataSkippingPredicateCreator<'_> {
    /// Returns true if the column is a partition column (no stats in `stats_parsed`).
    fn is_partition_column(&self, col: &ColumnName) -> bool {
        let path = col.path();
        path.len() == 1 && self.partition_columns.contains(path[0].as_str())
    }

    fn no_partitions_creator() -> DataSkippingPredicateCreator {
        DataSkippingPredicateCreator::new(Default::default())
    }
}

impl DataSkippingPredicateEvaluator for NullGuardedDataSkippingPredicateCreator<'_> {
    type Output = Pred;
    type ColumnStat = Expr;

    // The get_*_stat methods delegate to DataSkippingPredicateCreator but return None for
    // partition columns, which don't have stats in stats_parsed.

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        Self::no_partitions_creator().get_min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        Self::no_partitions_creator().get_max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        Self::no_partitions_creator().get_nullcount_stat(col)
    }

    fn get_rowcount_stat(&self) -> Option<Expr> {
        Self::no_partitions_creator().get_rowcount_stat()
    }

    /// Wraps a stat column comparison with an IS NULL guard.
    ///
    /// `col > 100` → `OR(maxValues.col IS NULL, maxValues.col > 100)`
    ///
    /// `col = 100` (calls this twice, once per stat):
    /// ```text
    /// AND(
    ///   OR(minValues.col IS NULL, minValues.col <= 100),
    ///   OR(maxValues.col IS NULL, maxValues.col >= 100)
    /// )
    /// ```
    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Expr,
        val: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        let comparison = comparison_predicate(ord, col.clone(), val, inverted);
        Some(Pred::or(Pred::is_null(col), comparison))
    }

    /// No guard needed — no stat column reference. `TRUE` → `Some(true)`.
    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted).map(Pred::literal)
    }

    /// No guard needed — no stat column reference. `NULL IS NULL` → `Some(true)`.
    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted).map(Pred::literal)
    }

    /// IS NULL guard on nullCount stat.
    ///
    /// `IS NULL` → `OR(nullCount.col IS NULL, nullCount.col != 0)`:
    /// column vs literal — RowGroupFilter can evaluate via footer stats.
    ///
    /// `IS NOT NULL` → returns `None`. The unguarded version produces
    /// `nullCount.col != numRecords`, which is column vs column. The RowGroupFilter can
    /// only resolve one column at a time, so it can never prune with this predicate.
    // TODO(#1873): IS NOT NULL pruning requires cross-column range comparison in RowGroupFilter.
    // Skippable when the nullCount and numRecords ranges don't overlap (e.g. nullCount in
    // [0, 0] vs numRecords in [500, 2000] proves all files have non-null values).
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Pred> {
        if inverted {
            return None; // IS NOT NULL: column vs column, can't prune (#1873)
        }
        let nullcount = self.get_nullcount_stat(col)?;
        let comparison = Pred::ne(nullcount.clone(), Expr::literal(0i64));
        Some(Pred::or(Pred::is_null(nullcount), comparison))
    }

    /// No guard needed — no stat column reference. `5 < 10` → `Some(true)`.
    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
            .map(Pred::literal)
    }

    /// Unsupported. Opaque predicates can construct stat column references directly,
    /// bypassing IS NULL guards and risking false pruning. Returns `None` to conservatively
    /// drop these from the skipping predicate.
    fn eval_pred_opaque(
        &self,
        _op: &OpaquePredicateOpRef,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<Pred> {
        None
    }

    /// Combines sub-predicates with AND/OR. `col_a > 100 AND col_b < 50` →
    /// ```text
    /// AND(
    ///   OR(maxValues.col_a IS NULL, maxValues.col_a > 100),
    ///   OR(minValues.col_b IS NULL, minValues.col_b < 50)
    /// )
    /// ```
    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<Pred>>,
        inverted: bool,
    ) -> Option<Pred> {
        Some(collect_junction_preds(op, preds, inverted))
    }
}
