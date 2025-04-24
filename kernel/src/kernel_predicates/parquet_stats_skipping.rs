//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{BinaryOperator, ColumnName, JunctionOperator, Scalar};
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

/// Blanket implementation that converts a [`ParquetStatsProvider`] into a
/// [`DataSkippingPredicateEvaluator`].
impl<T: ParquetStatsProvider> DataSkippingPredicateEvaluator for T {
    type Output = bool;
    type TypedStat = Scalar;
    type IntStat = i64;

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.get_parquet_min_stat(col, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.get_parquet_max_stat(col, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<i64> {
        self.get_parquet_nullcount_stat(col)
    }

    fn get_rowcount_stat(&self) -> Option<i64> {
        Some(self.get_parquet_rowcount_stat())
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

    fn eval_scalar(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_scalar(val, inverted)
    }

    fn eval_scalar_is_null(&self, val: &Scalar, inverted: bool) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_scalar_is_null(val, inverted)
    }

    fn eval_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        let safe_to_skip = match inverted {
            true => self.get_rowcount_stat()?, // all-null
            false => 0i64,                     // no-null
        };
        Some(self.get_nullcount_stat(col)? != safe_to_skip)
    }

    fn eval_binary_scalars(
        &self,
        op: BinaryOperator,
        left: &Scalar,
        right: &Scalar,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::eval_binary_scalars(op, left, right, inverted)
    }

    fn finish_eval_junction(
        &self,
        op: JunctionOperator,
        exprs: impl IntoIterator<Item = Option<bool>>,
        inverted: bool,
    ) -> Option<bool> {
        KernelPredicateEvaluatorDefaults::finish_eval_junction(op, exprs, inverted)
    }
}
