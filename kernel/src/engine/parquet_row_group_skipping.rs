//! An implementation of parquet row group skipping using data skipping predicates over footer stats.
use crate::expressions::{
    BinaryExpression, ColumnName, Expression, Scalar, UnaryExpression, VariadicExpression,
};
use crate::predicates::parquet_stats_skipping::ParquetStatsProvider;
use crate::schema::{DataType, PrimitiveType};
use chrono::{DateTime, Days};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescPtr;
use std::collections::{HashMap, HashSet};
use tracing::debug;

#[cfg(test)]
mod tests;

/// An extension trait for [`ArrowReaderBuilder`] that injects row group skipping capability.
pub(crate) trait ParquetRowGroupSkipping {
    /// Instructs the parquet reader to perform row group skipping, eliminating any row group whose
    /// stats prove that none of the group's rows can satisfy the given `predicate`.
    fn with_row_group_filter(self, predicate: &Expression) -> Self;
}
impl<T> ParquetRowGroupSkipping for ArrowReaderBuilder<T> {
    fn with_row_group_filter(self, predicate: &Expression) -> Self {
        let indices = self
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(index, row_group)| {
                // If the group survives the filter, return Some(index) so filter_map keeps it.
                RowGroupFilter::apply(row_group, predicate).then_some(index)
            })
            .collect();
        debug!("with_row_group_filter({predicate:#?}) = {indices:?})");
        self.with_row_groups(indices)
    }
}

/// A ParquetStatsSkippingFilter for row group skipping. It obtains stats from a parquet
/// [`RowGroupMetaData`] and pre-computes the mapping of each referenced column path to its
/// corresponding field index, for O(1) stats lookups.
struct RowGroupFilter<'a> {
    row_group: &'a RowGroupMetaData,
    field_indices: HashMap<ColumnName, usize>,
}

impl<'a> RowGroupFilter<'a> {
    /// Creates a new row group filter for the given row group and predicate.
    fn new(row_group: &'a RowGroupMetaData, predicate: &Expression) -> Self {
        Self {
            row_group,
            field_indices: compute_field_indices(row_group.schema_descr().columns(), predicate),
        }
    }

    /// Applies a filtering predicate to a row group. Return value false means to skip it.
    fn apply(row_group: &'a RowGroupMetaData, predicate: &Expression) -> bool {
        use crate::predicates::PredicateEvaluator as _;
        RowGroupFilter::new(row_group, predicate).eval_sql_where(predicate) != Some(false)
    }

    /// Returns `None` if the column doesn't exist and `Some(None)` if the column has no stats.
    fn get_stats(&self, col: &ColumnName) -> Option<Option<&Statistics>> {
        self.field_indices
            .get(col)
            .map(|&i| self.row_group.column(i).statistics())
    }

    fn decimal_from_bytes(bytes: Option<&[u8]>, precision: u8, scale: u8) -> Option<Scalar> {
        // WARNING: The bytes are stored in big-endian order; reverse and then 0-pad to 16 bytes.
        let bytes = bytes.filter(|b| b.len() <= 16)?;
        let mut bytes = Vec::from(bytes);
        bytes.reverse();
        bytes.resize(16, 0u8);
        let bytes: [u8; 16] = bytes.try_into().ok()?;
        Some(Scalar::Decimal(
            i128::from_le_bytes(bytes),
            precision,
            scale,
        ))
    }

    fn timestamp_from_date(days: Option<&i32>) -> Option<Scalar> {
        let days = u64::try_from(*days?).ok()?;
        let timestamp = DateTime::UNIX_EPOCH.checked_add_days(Days::new(days))?;
        let timestamp = timestamp.signed_duration_since(DateTime::UNIX_EPOCH);
        Some(Scalar::TimestampNtz(timestamp.num_microseconds()?))
    }
}

impl ParquetStatsProvider for RowGroupFilter<'_> {
    // Extracts a stat value, converting from its physical type to the requested logical type.
    //
    // NOTE: This code is highly redundant with [`get_max_stat_value`] below, but parquet
    // ValueStatistics<T> requires T to impl a private trait, so we can't factor out any kind of
    // helper method. And macros are hard enough to read that it's not worth defining one.
    fn get_parquet_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        use PrimitiveType::*;
        let value = match (data_type.as_primitive_opt()?, self.get_stats(col)??) {
            (String, Statistics::ByteArray(s)) => s.min_opt()?.as_utf8().ok()?.into(),
            (String, Statistics::FixedLenByteArray(s)) => s.min_opt()?.as_utf8().ok()?.into(),
            (String, _) => return None,
            (Long, Statistics::Int64(s)) => s.min_opt()?.into(),
            (Long, Statistics::Int32(s)) => (*s.min_opt()? as i64).into(),
            (Long, _) => return None,
            (Integer, Statistics::Int32(s)) => s.min_opt()?.into(),
            (Integer, _) => return None,
            (Short, Statistics::Int32(s)) => (*s.min_opt()? as i16).into(),
            (Short, _) => return None,
            (Byte, Statistics::Int32(s)) => (*s.min_opt()? as i8).into(),
            (Byte, _) => return None,
            (Float, Statistics::Float(s)) => s.min_opt()?.into(),
            (Float, _) => return None,
            (Double, Statistics::Double(s)) => s.min_opt()?.into(),
            (Double, Statistics::Float(s)) => (*s.min_opt()? as f64).into(),
            (Double, _) => return None,
            (Boolean, Statistics::Boolean(s)) => s.min_opt()?.into(),
            (Boolean, _) => return None,
            (Binary, Statistics::ByteArray(s)) => s.min_opt()?.data().into(),
            (Binary, Statistics::FixedLenByteArray(s)) => s.min_opt()?.data().into(),
            (Binary, _) => return None,
            (Date, Statistics::Int32(s)) => Scalar::Date(*s.min_opt()?),
            (Date, _) => return None,
            (Timestamp, Statistics::Int64(s)) => Scalar::Timestamp(*s.min_opt()?),
            (Timestamp, _) => return None, // TODO: Int96 timestamps
            (TimestampNtz, Statistics::Int64(s)) => Scalar::TimestampNtz(*s.min_opt()?),
            (TimestampNtz, Statistics::Int32(s)) => Self::timestamp_from_date(s.min_opt())?,
            (TimestampNtz, _) => return None, // TODO: Int96 timestamps
            (Decimal(p, s), Statistics::Int32(i)) => Scalar::Decimal(*i.min_opt()? as i128, *p, *s),
            (Decimal(p, s), Statistics::Int64(i)) => Scalar::Decimal(*i.min_opt()? as i128, *p, *s),
            (Decimal(p, s), Statistics::FixedLenByteArray(b)) => {
                Self::decimal_from_bytes(b.min_bytes_opt(), *p, *s)?
            }
            (Decimal(..), _) => return None,
        };
        Some(value)
    }

    fn get_parquet_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        use PrimitiveType::*;
        let value = match (data_type.as_primitive_opt()?, self.get_stats(col)??) {
            (String, Statistics::ByteArray(s)) => s.max_opt()?.as_utf8().ok()?.into(),
            (String, Statistics::FixedLenByteArray(s)) => s.max_opt()?.as_utf8().ok()?.into(),
            (String, _) => return None,
            (Long, Statistics::Int64(s)) => s.max_opt()?.into(),
            (Long, Statistics::Int32(s)) => (*s.max_opt()? as i64).into(),
            (Long, _) => return None,
            (Integer, Statistics::Int32(s)) => s.max_opt()?.into(),
            (Integer, _) => return None,
            (Short, Statistics::Int32(s)) => (*s.max_opt()? as i16).into(),
            (Short, _) => return None,
            (Byte, Statistics::Int32(s)) => (*s.max_opt()? as i8).into(),
            (Byte, _) => return None,
            (Float, Statistics::Float(s)) => s.max_opt()?.into(),
            (Float, _) => return None,
            (Double, Statistics::Double(s)) => s.max_opt()?.into(),
            (Double, Statistics::Float(s)) => (*s.max_opt()? as f64).into(),
            (Double, _) => return None,
            (Boolean, Statistics::Boolean(s)) => s.max_opt()?.into(),
            (Boolean, _) => return None,
            (Binary, Statistics::ByteArray(s)) => s.max_opt()?.data().into(),
            (Binary, Statistics::FixedLenByteArray(s)) => s.max_opt()?.data().into(),
            (Binary, _) => return None,
            (Date, Statistics::Int32(s)) => Scalar::Date(*s.max_opt()?),
            (Date, _) => return None,
            (Timestamp, Statistics::Int64(s)) => Scalar::Timestamp(*s.max_opt()?),
            (Timestamp, _) => return None, // TODO: Int96 timestamps
            (TimestampNtz, Statistics::Int64(s)) => Scalar::TimestampNtz(*s.max_opt()?),
            (TimestampNtz, Statistics::Int32(s)) => Self::timestamp_from_date(s.max_opt())?,
            (TimestampNtz, _) => return None, // TODO: Int96 timestamps
            (Decimal(p, s), Statistics::Int32(i)) => Scalar::Decimal(*i.max_opt()? as i128, *p, *s),
            (Decimal(p, s), Statistics::Int64(i)) => Scalar::Decimal(*i.max_opt()? as i128, *p, *s),
            (Decimal(p, s), Statistics::FixedLenByteArray(b)) => {
                Self::decimal_from_bytes(b.max_bytes_opt(), *p, *s)?
            }
            (Decimal(..), _) => return None,
        };
        Some(value)
    }

    fn get_parquet_nullcount_stat(&self, col: &ColumnName) -> Option<i64> {
        // NOTE: Stats for any given column are optional, which may produce a NULL nullcount. But if
        // the column itself is missing, then we know all values are implied to be NULL.
        //
        let Some(stats) = self.get_stats(col) else {
            // WARNING: This optimization is only sound if the caller has verified that the column
            // actually exists in the table's logical schema, and that any necessary logical to
            // physical name mapping has been performed. Because we currently lack both the
            // validation and the name mapping support, we must disable this optimization for the
            // time being. See https://github.com/delta-io/delta-kernel-rs/issues/434.
            return Some(self.get_parquet_rowcount_stat()).filter(|_| false);
        };

        // WARNING: [`Statistics::null_count_opt`] returns Some(0) when the underlying stat is
        // missing, causing an IS NULL predicate to wrongly skip the file if it contains any NULL
        // values. Manually drill into each arm's [`ValueStatistics`] for the stat's true.
        let nullcount = match stats? {
            Statistics::Boolean(s) => s.null_count_opt(),
            Statistics::Int32(s) => s.null_count_opt(),
            Statistics::Int64(s) => s.null_count_opt(),
            Statistics::Int96(s) => s.null_count_opt(),
            Statistics::Float(s) => s.null_count_opt(),
            Statistics::Double(s) => s.null_count_opt(),
            Statistics::ByteArray(s) => s.null_count_opt(),
            Statistics::FixedLenByteArray(s) => s.null_count_opt(),
        };

        // Parquet nullcount stats are always u64, so we can directly return the value instead of
        // wrapping it in a Scalar. We can safely cast it from u64 to i64 because the nullcount can
        // never be larger than the rowcount and the parquet rowcount stat is i64.
        Some(nullcount? as i64)
    }

    fn get_parquet_rowcount_stat(&self) -> i64 {
        self.row_group.num_rows()
    }
}

/// Given a filter expression of interest and a set of parquet column descriptors, build a column ->
/// index mapping for columns the expression references. This ensures O(1) lookup times, for an
/// overall O(n) cost to evaluate an expression tree with n nodes.
pub(crate) fn compute_field_indices(
    fields: &[ColumnDescPtr],
    expression: &Expression,
) -> HashMap<ColumnName, usize> {
    fn do_recurse(expression: &Expression, cols: &mut HashSet<ColumnName>) {
        use Expression::*;
        let mut recurse = |expr| do_recurse(expr, cols); // simplifies the call sites below
        match expression {
            Literal(_) => {}
            Column(name) => cols.extend([name.clone()]), // returns `()`, unlike `insert`
            Struct(fields) => fields.iter().for_each(recurse),
            Unary(UnaryExpression { expr, .. }) => recurse(expr),
            Binary(BinaryExpression { left, right, .. }) => {
                [left, right].iter().for_each(|e| recurse(e))
            }
            Variadic(VariadicExpression { exprs, .. }) => exprs.iter().for_each(recurse),
        }
    }

    // Build up a set of requested column paths, then take each found path as the corresponding map
    // key (avoids unnecessary cloning).
    //
    // NOTE: If a requested column was not available, it is silently ignored. These missing columns
    // are implied all-null, so we will infer their min/max stats as NULL and nullcount == rowcount.
    let mut requested_columns = HashSet::new();
    do_recurse(expression, &mut requested_columns);
    fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            requested_columns
                .take(f.path().parts())
                .map(|path| (path, i))
        })
        .collect()
}
