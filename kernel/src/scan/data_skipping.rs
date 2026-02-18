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
use crate::schema::{DataType, SchemaRef};
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
    DataSkippingPredicateCreator.eval(pred)
}

/// Like `as_data_skipping_predicate`, but invokes [`KernelPredicateEvaluator::eval_sql_where`]
/// instead of [`KernelPredicateEvaluator::eval`].
fn as_sql_data_skipping_predicate(pred: &Pred) -> Option<Pred> {
    DataSkippingPredicateCreator.eval_sql_where(pred)
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
    /// - `stats_schema`: The stats schema (numRecords, nullCount, minValues, maxValues)
    /// - `input_schema`: Schema of the batch that will be passed to [`apply()`](Self::apply)
    /// - `stats_expr`: Expression to extract stats from the batch, producing output matching
    ///   `stats_schema`. For example, `column_expr!("stats_parsed")` for pre-parsed stats, or
    ///   `Expression::parse_json(column_expr!("add.stats"), stats_schema)` for JSON parsing.
    pub(crate) fn new(
        engine: &dyn Engine,
        predicate: Option<PredicateRef>,
        stats_schema: SchemaRef,
        input_schema: SchemaRef,
        stats_expr: ExpressionRef,
    ) -> Option<Self> {
        static FILTER_PRED: LazyLock<PredicateRef> =
            LazyLock::new(|| Arc::new(column_expr!("output").distinct(Expr::literal(false))));

        let predicate = predicate?;
        debug!("Creating a data skipping filter for {:#?}", predicate);

        let stats_evaluator = engine
            .evaluation_handler()
            .new_expression_evaluator(
                input_schema,
                stats_expr,
                stats_schema.as_ref().clone().into(),
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
                stats_schema.clone(),
                Arc::new(as_sql_data_skipping_predicate(&predicate)?),
            )
            .inspect_err(|e| error!("Failed to create skipping evaluator: {e}"))
            .ok()?;

        let filter_evaluator = engine
            .evaluation_handler()
            .new_predicate_evaluator(stats_schema, FILTER_PRED.clone())
            .inspect_err(|e| error!("Failed to create filter evaluator: {e}"))
            .ok()?;

        Some(Self {
            stats_evaluator,
            skipping_evaluator,
            filter_evaluator,
        })
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
/// Similar to [`as_data_skipping_predicate`], but adds IS NULL guards on each stat column
/// reference. This is necessary because parquet footer min/max statistics ignore null values:
/// if a stat column (e.g., `maxValues.col_a`) is null for some files (meaning stats were not
/// collected), the footer min/max won't reflect those files. The IS NULL guard ensures the
/// row group filter conservatively keeps any row group containing files with missing stats.
///
/// For example, `col_a > 100` becomes:
/// ```text
/// OR(maxValues.col_a IS NULL, maxValues.col_a > 100)
/// ```
///
/// At the parquet row group level:
/// - If any file has null `maxValues.col_a` (null_count > 0): IS NULL is satisfiable, keep
/// - If all files have non-null stats (null_count = 0): evaluate comparison using footer stats
///
/// Partition columns are excluded because their values live in `add.partitionValues_parsed`,
/// not in `add.stats_parsed`. Predicates on partition columns would reference non-existent
/// columns in the checkpoint and be ignored by the parquet reader.
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

struct DataSkippingPredicateCreator;

impl DataSkippingPredicateEvaluator for DataSkippingPredicateCreator {
    type Output = Pred;
    type ColumnStat = Expr;

    /// Retrieves the minimum value of a column, if it exists and has the requested type.
    fn get_min_stat(&self, col: &ColumnName, _data_type: &DataType) -> Option<Expr> {
        Some(joined_column_expr!("minValues", col))
    }

    /// Retrieves the maximum value of a column, if it exists and has the requested type.
    // TODO(#1002): we currently don't support file skipping on timestamp columns' max stat since
    // they are truncated to milliseconds in add.stats.
    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        match data_type {
            &DataType::TIMESTAMP | &DataType::TIMESTAMP_NTZ => None,
            _ => Some(joined_column_expr!("maxValues", col)),
        }
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
    ) -> Option<Pred> {
        Some(comparison_predicate(ord, col, val, inverted))
    }

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted).map(Pred::literal)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted).map(Pred::literal)
    }

    // NOTE: This is nearly identical to the impl for ParquetStatsProvider in
    // parquet_stats_skipping.rs, except it uses `Expression` and `Predicate` instead of `Scalar`.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Pred> {
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?, // all-null
            false => Expr::literal(0i64),      // no-null
        };
        Some(Pred::ne(self.get_nullcount_stat(col)?, safe_to_skip))
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

/// A data-skipping predicate creator that adds IS NULL guards on stat column references,
/// making the output safe for parquet row group filtering where null stats are invisible
/// to footer min/max statistics. Partition columns are excluded since their values are
/// stored in `add.partitionValues_parsed`, not `add.stats_parsed`.
struct NullGuardedDataSkippingPredicateCreator<'a> {
    partition_columns: HashSet<&'a str>,
}

impl NullGuardedDataSkippingPredicateCreator<'_> {
    /// Returns true if the column is a partition column (no stats in `stats_parsed`).
    fn is_partition_column(&self, col: &ColumnName) -> bool {
        let path = col.path();
        path.len() == 1 && self.partition_columns.contains(path[0].as_str())
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
        DataSkippingPredicateCreator.get_min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        DataSkippingPredicateCreator.get_max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Expr> {
        if self.is_partition_column(col) {
            return None;
        }
        DataSkippingPredicateCreator.get_nullcount_stat(col)
    }

    fn get_rowcount_stat(&self) -> Option<Expr> {
        DataSkippingPredicateCreator.get_rowcount_stat()
    }

    /// Produces a comparison between a stat column and a literal, wrapped in an IS NULL guard.
    /// The guard ensures that row groups containing files with missing stats (null stat values)
    /// are not incorrectly pruned, since parquet footer min/max statistics ignore nulls.
    ///
    /// Example: `col_a > 100` → `eval_partial_cmp(Greater, maxValues.col_a, 100, false)` →
    /// ```text
    /// OR(maxValues.col_a IS NULL, maxValues.col_a > 100)
    /// ```
    ///
    /// Example: `col_a = 100` calls this twice (min and max):
    /// ```text
    /// AND(
    ///   OR(minValues.col_a IS NULL, minValues.col_a <= 100),
    ///   OR(maxValues.col_a IS NULL, maxValues.col_a >= 100)
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

    /// Evaluates a scalar predicate (e.g., `TRUE`, `FALSE`). No guard needed since there
    /// is no stat column reference.
    ///
    /// Example: `TRUE` → `Some(Pred::literal(true))`
    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted).map(Pred::literal)
    }

    /// Evaluates `literal IS [NOT] NULL`. No guard needed since there is no stat column reference.
    ///
    /// Example: `NULL IS NULL` → `Some(Pred::literal(true))`
    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<Pred> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted).map(Pred::literal)
    }

    /// Produces a nullCount-based check for IS [NOT] NULL, wrapped in an IS NULL guard on the
    /// nullCount stat itself.
    ///
    /// **IS NULL** (`col_a IS NULL`) →
    /// ```text
    /// OR(nullCount.col_a IS NULL, nullCount.col_a != 0)
    /// ```
    /// The comparison `nullCount.col_a != 0` is **column vs literal**, which the parquet
    /// RowGroupFilter can evaluate using footer min/max stats for `nullCount.col_a`.
    ///
    /// **IS NOT NULL** (`col_a IS NOT NULL`) →
    /// ```text
    /// OR(nullCount.col_a IS NULL, nullCount.col_a != numRecords)
    /// ```
    /// The comparison `nullCount.col_a != numRecords` is **column vs column**. The parquet
    /// RowGroupFilter can only resolve a single column to a scalar from footer stats, so it
    /// cannot evaluate cross-column comparisons and conservatively keeps the row group. This
    /// is safe (no false pruning) but means IS NOT NULL predicates do not enable row group
    /// skipping at the checkpoint level.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<Pred> {
        let nullcount = self.get_nullcount_stat(col)?;
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?, // IS NOT NULL: all-null
            false => Expr::literal(0i64),      // IS NULL: no-null
        };
        let comparison = Pred::ne(nullcount.clone(), safe_to_skip);
        Some(Pred::or(Pred::is_null(nullcount), comparison))
    }

    /// Evaluates a comparison between two scalar literals. No guard needed since there are
    /// no stat column references.
    ///
    /// Example: `5 < 10` → `Some(Pred::literal(true))`
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

    /// Delegates to the opaque predicate's own data skipping implementation, passing `self`
    /// so that any recursive evaluations also use guarded comparisons.
    /// Opaque predicates are not supported for checkpoint row group skipping. The double-layer
    /// data skipping (our predicate generation + parquet RowGroupFilter) makes it difficult to
    /// reason about correctness for arbitrary opaque predicates.
    fn eval_pred_opaque(
        &self,
        _op: &OpaquePredicateOpRef,
        _exprs: &[Expr],
        _inverted: bool,
    ) -> Option<Pred> {
        None
    }

    /// Combines sub-predicates with AND/OR. Unsupported sub-predicates (None) are replaced
    /// with a single NULL literal to preserve correct three-valued logic.
    ///
    /// Example: `col_a > 100 AND col_b < 50` →
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
