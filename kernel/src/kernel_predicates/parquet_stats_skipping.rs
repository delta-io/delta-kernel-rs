//! An implementation of data skipping that leverages parquet stats from the file footer.
use crate::expressions::{
    column_name, BinaryPredicateOp, ColumnName, Expression, JunctionPredicateOp,
    OpaquePredicateOpRef, Predicate, Scalar,
};
use crate::kernel_predicates::{
    DataSkippingPredicateEvaluator, KernelPredicateEvaluator as _, KernelPredicateEvaluatorDefaults,
};
use crate::schema::DataType;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

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
    #[allow(dead_code)] // Used by CheckpointMetaSkippingFilter (requires default-engine-base)
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
/// Partition columns are supported: they resolve to `add.partitionValues_parsed.<col>` where
/// both min and max stats come from the same column (the actual partition value stored in the
/// checkpoint). Partition values are never null.
///
/// To prevent unsound pruning when nested stat entries are NULL/missing, min/max are only exposed
/// if parquet footer nullcount for the nested stat column exists and is exactly 0.
///
/// NOTE: Due to <https://github.com/apache/arrow-rs/issues/9451>, arrow-rs currently reports
/// `nullcount = 0` even when column statistics are entirely missing, making the nullcount
/// guard ineffective. Until the upstream fix lands, checkpoint row group skipping will
/// conservatively keep all row groups (no pruning) for nested stat columns.
#[allow(dead_code)] // Only constructed when default-engine-base is enabled
pub(crate) struct CheckpointMetaSkippingFilter<T: ParquetStatsProvider> {
    inner: T,
    min_stats: HashMap<ColumnName, ColumnName>,
    max_stats: HashMap<ColumnName, ColumnName>,
    /// The set of partition columns referenced by the predicate.
    partition_columns: HashSet<ColumnName>,
}

#[allow(dead_code)] // Only used when default-engine-base is enabled
impl<T: ParquetStatsProvider> CheckpointMetaSkippingFilter<T> {
    pub(crate) fn new(
        mut inner: T,
        predicate: &Predicate,
        partition_columns: &HashSet<ColumnName>,
    ) -> Self {
        let min_prefix = column_name!("add.stats_parsed.minValues");
        let max_prefix = column_name!("add.stats_parsed.maxValues");
        let partition_prefix = column_name!("add.partitionValues_parsed");

        let mut min_stats = HashMap::new();
        let mut max_stats = HashMap::new();
        let mut matched_partition_columns = HashSet::new();

        for col in predicate.references() {
            if partition_columns.contains(col) {
                // Partition column: min and max both map to the same partitionValues_parsed path.
                let partition_path = partition_prefix.join(col);
                min_stats.insert(col.clone(), partition_path.clone());
                max_stats.insert(col.clone(), partition_path);
                matched_partition_columns.insert(col.clone());
            } else {
                // Data column: map to stats_parsed paths.
                min_stats.insert(col.clone(), min_prefix.join(col));
                max_stats.insert(col.clone(), max_prefix.join(col));
            }
        }

        inner.prepare_stats(min_stats.values().chain(max_stats.values()));

        Self {
            inner,
            min_stats,
            max_stats,
            partition_columns: matched_partition_columns,
        }
    }

    pub(crate) fn apply(
        inner: T,
        predicate: &Predicate,
        partition_columns: &HashSet<ColumnName>,
    ) -> bool {
        CheckpointMetaSkippingFilter::new(inner, predicate, partition_columns)
            .eval_sql_where(predicate)
            != Some(false)
    }
}

impl<T: ParquetStatsProvider> DataSkippingPredicateEvaluator for CheckpointMetaSkippingFilter<T> {
    type Output = bool;
    type ColumnStat = Scalar;

    // NOTE: The nullcount guard is currently ineffective due to arrow-rs #9451
    // (see struct-level doc comment). Once the upstream fix lands, this will begin pruning.
    fn get_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        let nested_col = self.min_stats.get(col)?;
        match self.inner.get_parquet_nullcount_stat(nested_col) {
            Some(0) => self.inner.get_parquet_min_stat(nested_col, data_type),
            _ => None, // Can't prove the min-stat is present for all files
        }
    }

    // NOTE: See get_min_stat comment about arrow-rs #9451.
    fn get_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        let nested_col = self.max_stats.get(col)?;
        match self.inner.get_parquet_nullcount_stat(nested_col) {
            Some(0) => self.inner.get_parquet_max_stat(nested_col, data_type),
            _ => None, // Can't prove the max-stat is present for all files
        }
    }

    fn get_nullcount_stat(&self, col: &ColumnName) -> Option<Scalar> {
        if self.partition_columns.contains(col) {
            // Partition values are never null — always report zero nulls.
            return Some(Scalar::from(0i64));
        }
        // Delta nullcount semantics are not safely derivable from checkpoint footer stats
        // for data columns.
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

    fn eval_pred_is_null(&self, col: &ColumnName, inverted: bool) -> Option<bool> {
        if self.partition_columns.contains(col) {
            // Partition values are never null: IS NULL → false, IS NOT NULL → true.
            return Some(inverted);
        }
        // Delta IS [NOT] NULL semantics rely on file-level nullcount/rowcount stats, which are
        // not safely derivable from checkpoint footer stats for data columns.
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
