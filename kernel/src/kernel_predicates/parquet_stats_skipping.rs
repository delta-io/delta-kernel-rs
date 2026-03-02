//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{
    BinaryPredicateOp, ColumnName, Expression, JunctionPredicateOp, OpaquePredicateOpRef,
    Predicate, Scalar,
};
use crate::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator as _, KernelPredicateEvaluatorDefaults,
};
use crate::schema::DataType;

use crate::{MetadataStatResolver, StatType};

use std::cmp::Ordering;

#[cfg(test)]
mod tests;

/// Provides the four stats getters needed by [`DataSkippingPredicateEvaluator`]. From there,
/// we can automatically derive a [`DataSkippingPredicateEvaluator`].
///
/// Implement this trait to plug in custom stats sources (e.g., a different parquet reader)
/// and gain data-skipping predicate evaluation for free via [`DataSkippingPredicateEvaluator`].
pub(crate) trait ParquetStatsProvider {
    /// Pre-registers a set of columns that may be queried shortly after. Callers are not strictly
    /// required to invoke this method, but some providers may choose to only provide stats for
    /// prepared columns because repeatedly traversing parquet footers can be expensive.
    #[allow(dead_code)] // Used by MetadataSkippingFilter (requires default-engine-base)
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

/// A data skipping filter for metadata file pruning (e.g. checkpoints and sidecars).
///
/// Uses a [`MetadataStatResolver`] to map predicate columns to their physical stat locations
/// in the metadata file. The resolver handles the details of different column types (data vs.
/// partition) and their stat layouts.
///
/// To prevent unsound pruning when nested stat entries are NULL/missing, min/max are only exposed
/// if the parquet footer nullcount for the nested stat column exists and is exactly 0 — unless the
/// column is a partition column, in which case the nullcount guard is bypassed (partition values
/// are never null).
///
/// NOTE: Due to <https://github.com/apache/arrow-rs/issues/9451>, arrow-rs currently reports
/// `nullcount = 0` even when column statistics are entirely missing, making the nullcount
/// guard ineffective for non-partition columns. Until the upstream fix lands, metadata row
/// group skipping will conservatively keep all row groups (no pruning) for nested stat columns.
/// Partition columns are unaffected because they bypass the nullcount guard.
#[allow(dead_code)] // Only constructed when default-engine-base is enabled
pub(crate) struct MetadataSkippingFilter<T: ParquetStatsProvider> {
    inner: T,
    resolver: MetadataStatResolver,
}

#[allow(dead_code)] // Only used when default-engine-base is enabled
impl<T: ParquetStatsProvider> MetadataSkippingFilter<T> {
    pub(crate) fn new(mut inner: T, resolver: &MetadataStatResolver) -> Self {
        inner.prepare_stats(resolver.all_stat_columns());
        Self {
            inner,
            resolver: resolver.clone(),
        }
    }

    pub(crate) fn apply(inner: T, predicate: &Predicate, resolver: &MetadataStatResolver) -> bool {
        MetadataSkippingFilter::new(inner, resolver).eval_sql_where(predicate) != Some(false)
    }

    /// Retrieves a stat value for a column, applying the nullcount guard for non-partition columns.
    fn get_stat(
        &self,
        col: &ColumnName,
        stat_type: StatType,
        data_type: &DataType,
    ) -> Option<Scalar> {
        let detail = self.resolver.resolve(col, stat_type)?;
        if detail.is_partition {
            // Partition columns bypass the nullcount guard — their values are always present.
            let stat_col = &detail.stat_column_name;
            match stat_type {
                StatType::Min => self.inner.get_parquet_min_stat(stat_col, data_type),
                StatType::Max => self.inner.get_parquet_max_stat(stat_col, data_type),
            }
        } else {
            // Non-partition columns require nullcount == 0 to trust the stat.
            let stat_col = &detail.stat_column_name;
            match self.inner.get_parquet_nullcount_stat(stat_col) {
                Some(0) => match stat_type {
                    StatType::Min => self.inner.get_parquet_min_stat(stat_col, data_type),
                    StatType::Max => self.inner.get_parquet_max_stat(stat_col, data_type),
                },
                _ => None, // Can't prove the stat is present for all files
            }
        }
    }

    /// Returns whether the given column is a partition column according to the resolver.
    fn is_partition_column(&self, col: &ColumnName) -> bool {
        // A column is a partition column if either its min or max detail says so.
        self.resolver
            .resolve(col, StatType::Min)
            .is_some_and(|d| d.is_partition)
    }
}

impl<T: ParquetStatsProvider> DataSkippingPredicateEvaluator for MetadataSkippingFilter<T> {
    type Output = bool;
    type ColumnStat = Scalar;

    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.get_stat(col, StatType::Min, data_type)
    }

    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        self.get_stat(col, StatType::Max, data_type)
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Scalar> {
        if self.is_partition_column(col) {
            // Partition values are never null — always report zero nulls.
            return Some(Scalar::from(0i64));
        }
        // Delta nullcount semantics are not safely derivable from metadata footer stats
        // for data columns.
        None
    }

    // Delta rowcount semantics are not safely derivable from metadata footer stats.
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

    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        if self.is_partition_column(col) {
            // Partition values are never null: IS NULL → false, IS NOT NULL → true.
            return Some(inverted);
        }
        // Delta IS [NOT] NULL semantics rely on file-level nullcount/rowcount stats, which are
        // not safely derivable from metadata footer stats for data columns.
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
