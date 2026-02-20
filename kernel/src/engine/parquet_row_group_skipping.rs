//! An implementation of parquet row group skipping using data skipping predicates over footer stats.
use crate::engine::arrow_utils::RowIndexBuilder;
use crate::expressions::{ColumnName, DecimalData, Predicate, Scalar};
use crate::kernel_predicates::parquet_stats_skipping::ParquetStatsProvider;
use crate::parquet::arrow::arrow_reader::ArrowReaderBuilder;
use crate::parquet::file::metadata::RowGroupMetaData;
use crate::parquet::file::statistics::Statistics;
use crate::parquet::schema::types::ColumnDescPtr;
use crate::schema::{DataType, DecimalType, PrimitiveType};
use chrono::{DateTime, Days};
use std::collections::HashMap;
use tracing::debug;

#[cfg(test)]
mod tests;

/// An extension trait for [`ArrowReaderBuilder`] that injects row group skipping capability.
pub(crate) trait ParquetRowGroupSkipping {
    /// Instructs the parquet reader to perform row group skipping, eliminating any row group whose
    /// stats prove that none of the group's rows can satisfy the given `predicate`.
    ///
    /// If a [`RowIndexBuilder`] is provided, it will be updated to only include row indices of the
    /// row groups that survived the filter.
    fn with_row_group_filter(
        self,
        predicate: &Predicate,
        row_indexes: Option<&mut RowIndexBuilder>,
    ) -> Self;
}
impl<T> ParquetRowGroupSkipping for ArrowReaderBuilder<T> {
    fn with_row_group_filter(
        self,
        predicate: &Predicate,
        row_indexes: Option<&mut RowIndexBuilder>,
    ) -> Self {
        let ordinals: Vec<_> = self
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, row_group)| {
                // If the group survives the filter, return Some(ordinal) so filter_map keeps it.
                RowGroupFilter::apply(row_group, predicate).then_some(ordinal)
            })
            .collect();
        debug!("with_row_group_filter({predicate:#?}) = {ordinals:?})");
        if let Some(row_indexes) = row_indexes {
            row_indexes.select_row_groups(&ordinals);
        }
        self.with_row_groups(ordinals)
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
    fn new(row_group: &'a RowGroupMetaData, predicate: &Predicate) -> Self {
        Self {
            row_group,
            field_indices: compute_field_indices(row_group.schema_descr().columns(), predicate),
        }
    }

    /// Applies a filtering predicate to a row group. Return value false means to skip it.
    fn apply(row_group: &'a RowGroupMetaData, predicate: &Predicate) -> bool {
        use crate::kernel_predicates::KernelPredicateEvaluator as _;
        RowGroupFilter::new(row_group, predicate).eval_sql_where(predicate) != Some(false)
    }

    /// Returns `None` if the column doesn't exist and `Some(None)` if the column has no stats.
    fn get_stats(&self, col: &ColumnName) -> Option<Option<&Statistics>> {
        self.field_indices
            .get(col)
            .map(|&i| self.row_group.column(i).statistics())
    }

    fn decimal_from_bytes(bytes: Option<&[u8]>, dtype: DecimalType) -> Option<Scalar> {
        // WARNING: The bytes are stored in big-endian order; reverse and then 0-pad to 16 bytes.
        let bytes = bytes.filter(|b| b.len() <= 16)?;
        let mut bytes = Vec::from(bytes);
        bytes.reverse();
        bytes.resize(16, 0u8);
        let bytes: [u8; 16] = bytes.try_into().ok()?;
        let value = DecimalData::try_new(i128::from_le_bytes(bytes), dtype).ok()?;
        Some(value.into())
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
            (Decimal(d), Statistics::Int32(i)) => {
                DecimalData::try_new(*i.min_opt()?, *d).ok()?.into()
            }
            (Decimal(d), Statistics::Int64(i)) => {
                DecimalData::try_new(*i.min_opt()?, *d).ok()?.into()
            }
            (Decimal(d), Statistics::FixedLenByteArray(b)) => {
                Self::decimal_from_bytes(b.min_bytes_opt(), *d)?
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
            (Decimal(d), Statistics::Int32(i)) => {
                DecimalData::try_new(*i.max_opt()?, *d).ok()?.into()
            }
            (Decimal(d), Statistics::Int64(i)) => {
                DecimalData::try_new(*i.max_opt()?, *d).ok()?.into()
            }
            (Decimal(d), Statistics::FixedLenByteArray(b)) => {
                Self::decimal_from_bytes(b.max_bytes_opt(), *d)?
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

/// Given a predicate of interest and a set of parquet column descriptors, build a column ->
/// index mapping for columns the predicate references. This ensures O(1) lookup times, for an
/// overall O(n) cost to evaluate a predicate tree with n nodes.
pub(crate) fn compute_field_indices(
    fields: &[ColumnDescPtr],
    predicate: &Predicate,
) -> HashMap<ColumnName, usize> {
    // Build up a set of requested column paths, then take each found path as the corresponding map
    // key (avoids unnecessary cloning).
    //
    // NOTE: If a requested column was not available, it is silently ignored. These missing columns
    // are implied all-null, so we will infer their min/max stats as NULL and nullcount == rowcount.
    let mut requested_columns = predicate.references();
    fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            requested_columns
                .take(f.path().parts())
                .map(|path| (path.clone(), i))
        })
        .collect()
}
