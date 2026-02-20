//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{
    column_name, BinaryPredicateOp, ColumnName, Expression, JunctionPredicateOp,
    OpaquePredicateOpRef, Predicate, Scalar,
};
use crate::kernel_predicates::{DataSkippingPredicateEvaluator, KernelPredicateEvaluatorDefaults};
use crate::schema::DataType;

use std::cmp::Ordering;
use std::collections::HashMap;

#[cfg(test)]
mod tests;

/// A helper trait (mostly exposed for testing). It provides the four stats getters needed by
/// [`DataSkippingStatsProvider`]. From there, we can automatically derive a
/// [`DataSkippingPredicateEvaluator`].
pub(crate) trait ParquetStatsProvider {
    /// Pre-registers a set of columns that may be queried shortly after. Callers are not strictly
    /// required to invoke this method, but some providers may choose to only provide stats for
    /// prepared columns because repeatedly traversing parquet footers can be expensive.
    fn prepare_stats<'a, I>(&mut self, _cols: I)
    where
        I: IntoIterator<Item = &'a ColumnName>,
    {
    }

    /// The min-value stat for this column, if the column exists in this file, has the expected
    /// type, and the parquet footer provides stats for it.
    fn get_parquet_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar>;

    /// The max-value stat for this column, if the column exists in this file, has the expected
    /// type, and the parquet footer provides stats for it.
    fn get_parquet_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar>;

    /// The nullcount stat for this column, if the column exists in this file, has the expected
    /// type, and the parquet footer provides stats for it.
    fn get_parquet_nullcount_stat(&self, col: &ColumnName) -> Option<i64>;

    /// The rowcount stat for this row group. It is always available in the parquet footer.
    fn get_parquet_rowcount_stat(&self) -> i64;
}

// Blanket implementation for all types that impl ParquetStatsProvider.
impl<T: ParquetStatsProvider> DataSkippingPredicateEvaluator for T {
    type Output = bool;
    type ColumnStat = Scalar;

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.get_parquet_min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.get_parquet_max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Scalar> {
        self.get_parquet_nullcount_stat(col).map(Scalar::from)
    }

    fn get_rowcount_stat(&self) -> Option<Scalar> {
        Some(Scalar::from(self.get_parquet_rowcount_stat()))
    }

    fn eval_partial_cmp(
        &self,
        ord: Ordering,
        col: Scalar,
        val: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::partial_cmp_scalars(ord, &col, val, inverted)
    }

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted)
    }

    // NOTE: This is nearly identical to the impl for DataSkippingPredicateEvaluator in
    // data_skipping.rs, except it uses `Scalar` instead of `Expression` and `Predicate`.
    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?, // all-null
            false => Scalar::from(0i64),       // no-null
        };
        Some(self.get_nullcount_stat(col)? != safe_to_skip)
    }

    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
    }

    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        op.eval_as_data_skipping_predicate(self, exprs, inverted)
    }

    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::finish_eval_pred_junction(op, preds, inverted)
    }
}

/// A data skipping filter for checkpoint/sidecar metadata pruning over `add.stats_parsed`.
///
/// It evaluates the original predicate with direct data-skipping semantics, resolving column
/// `x` as:
/// - min-stat: footer min of `add.stats_parsed.minValues.x`
/// - max-stat: footer max of `add.stats_parsed.maxValues.x`
///
/// To prevent unsound pruning when nested stat entries are NULL/missing, min/max are only exposed
/// if parquet footer nullcount for the nested stat column exists and is exactly 0.
pub(crate) struct CheckpointMetaSkippingFilter<T: ParquetStatsProvider> {
    inner: T,
    min_stats: HashMap<ColumnName, ColumnName>,
    max_stats: HashMap<ColumnName, ColumnName>,
}

impl<T: ParquetStatsProvider> CheckpointMetaSkippingFilter<T> {
    pub(crate) fn new(mut inner: T, predicate: &Predicate) -> Self {
        let min_prefix = column_name!("add.stats_parsed.minValues");
        let max_prefix = column_name!("add.stats_parsed.maxValues");
        let mapped: Vec<_> = predicate
            .references()
            .iter()
            .map(|&col| {
                (
                    (col.clone(), min_prefix.join(col)),
                    (col.clone(), max_prefix.join(col)),
                )
            })
            .collect();

        let cols = mapped.iter().flat_map(|((_, min), (_, max))| [min, max]);
        inner.prepare_stats(cols);

        let (min_stats, max_stats) = mapped.into_iter().unzip();

        Self {
            inner,
            min_stats,
            max_stats,
        }
    }

    pub(crate) fn apply(inner: T, predicate: &Predicate) -> bool {
        use crate::kernel_predicates::KernelPredicateEvaluator as _;
        CheckpointMetaSkippingFilter::new(inner, predicate).eval_sql_where(predicate) != Some(false)
    }
}

impl<T: ParquetStatsProvider> DataSkippingPredicateEvaluator for CheckpointMetaSkippingFilter<T> {
    type Output = bool;
    type ColumnStat = Scalar;

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        let nested_col = self.min_stats.get(col)?;
        match self.inner.get_parquet_nullcount_stat(nested_col) {
            Some(0) => self.inner.get_parquet_min_stat(nested_col, data_type),
            _ => None, // Can't prove the min-stat is present for all files
        }
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        let nested_col = self.max_stats.get(col)?;
        match self.inner.get_parquet_nullcount_stat(nested_col) {
            Some(0) => self.inner.get_parquet_max_stat(nested_col, data_type),
            _ => None, // Can't prove the max-stat is present for all files
        }
    }

    // Delta nullcount semantics are not safely derivable from checkpoint footer stats.
    fn get_nullcount_stat(&self, _col: &ColumnName) -> Option<Scalar> {
        None
    }

    // Delta rowcount semantics are not safely derivable from checkpoint footer stats.
    fn get_rowcount_stat(&self) -> Option<Scalar> {
        None
    }

    fn eval_partial_cmp(
        &self,
        ord: std::cmp::Ordering,
        col: Scalar,
        val: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::partial_cmp_scalars(ord, &col, val, inverted)
    }

    fn eval_pred_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar(val, inverted)
    }

    fn eval_pred_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_scalar_is_null(val, inverted)
    }

    // Delta IS [NOT] NULL semantics rely on file-level nullcount/rowcount stats, which are not
    // safely derivable from checkpoint footer stats.
    fn eval_pred_is_null(&self, _col: &ColumnName, _inverted: bool) -> Option<bool> {
        None
    }

    fn eval_pred_binary_scalars(
        &self,
        op: BinaryPredicateOp,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_pred_binary_scalars(op, left, right, inverted)
    }

    fn eval_pred_opaque(
        &self,
        op: &OpaquePredicateOpRef,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<bool> {
        op.eval_as_data_skipping_predicate(self, exprs, inverted)
    }

    fn finish_eval_pred_junction(
        &self,
        op: JunctionPredicateOp,
        preds: &mut dyn Iterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::finish_eval_pred_junction(op, preds, inverted)
    }
}
