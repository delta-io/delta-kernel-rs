//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{
    BinaryPredicateOp, ColumnName, Expression, JunctionPredicateOp, OpaquePredicateOpRef, Scalar,
};
use crate::kernel_predicates::{DataSkippingPredicateEvaluator, KernelPredicateEvaluatorDefaults};
use crate::schema::DataType;

use std::cmp::Ordering;

#[cfg(test)]
mod tests;

/// A helper trait (mostly exposed for testing). It provides the four stats getters needed by
/// [`DataSkippingStatsProvider`]. From there, we can automatically derive a
/// [`DataSkippingPredicateEvaluator`].
pub(crate) trait ParquetStatsProvider {
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
