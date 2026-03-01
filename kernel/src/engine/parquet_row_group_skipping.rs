//! An implementation of parquet row group skipping using data skipping predicates over footer
//! stats.
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Days};
use delta_kernel_derive::internal_api;
use tracing::debug;

use crate::engine::arrow_utils::RowIndexBuilder;
use crate::expressions::{ColumnName, DecimalData, Predicate, Scalar};
use crate::kernel_predicates::parquet_stats_skipping::ParquetStatsProvider;
use crate::parquet::arrow::arrow_reader::ArrowReaderBuilder;
use crate::parquet::file::metadata::RowGroupMetaData;
use crate::parquet::file::statistics::Statistics;
use crate::parquet::schema::types::ColumnDescPtr;
use crate::schema::{DataType, DecimalType, PrimitiveType};

#[cfg(test)]
mod tests;

/// An extension trait for [`ArrowReaderBuilder`] that injects row group skipping capability.
#[internal_api]
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

    /// Like [`with_row_group_filter`](Self::with_row_group_filter), but for checkpoint and sidecar
    /// parquet files where statistics are nested under `add.stats_parsed.*` and partition values
    /// under `add.partitionValues_parsed.*`.
    ///
    /// The `predicate` uses physical column names (e.g. `x > 10`, or `col-abc-123 > 10` under
    /// column mapping), and the filter internally maps them to the checkpoint's nested stats
    /// schema layout.
    /// Statistics for data columns are null-guarded: if a stat column contains any null values
    /// in the row group (indicating some files lack that statistic), the stat is treated as
    /// unavailable to prevent false pruning.
    // TODO: remove #[allow(dead_code)] once production callers land
    #[allow(dead_code)]
    fn with_checkpoint_row_group_filter(
        self,
        predicate: &Predicate,
        partition_columns: &HashSet<String>,
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

    fn with_checkpoint_row_group_filter(
        self,
        predicate: &Predicate,
        partition_columns: &HashSet<String>,
        row_indexes: Option<&mut RowIndexBuilder>,
    ) -> Self {
        let ordinals: Vec<_> = self
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, row_group)| {
                CheckpointRowGroupFilter::apply(row_group, predicate, partition_columns)
                    .then_some(ordinal)
            })
            .collect();
        debug!("with_checkpoint_row_group_filter({predicate:#?}) = {ordinals:?})");
        if let Some(row_indexes) = row_indexes {
            row_indexes.select_row_groups(&ordinals);
        }
        self.with_row_groups(ordinals)
    }
}

/// A [`ParquetStatsProvider`] for data file row group skipping. It obtains stats from a parquet
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
}

impl ParquetStatsProvider for RowGroupFilter<'_> {
    fn get_parquet_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        extract_min_scalar(data_type, self.get_stats(col)??)
    }

    fn get_parquet_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        extract_max_scalar(data_type, self.get_stats(col)??)
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
            return self.get_parquet_rowcount_stat().filter(|_| false);
        };

        extract_nullcount(stats)
    }

    fn get_parquet_rowcount_stat(&self) -> Option<i64> {
        Some(self.row_group.num_rows())
    }
}

/// Extracts the minimum stat value from parquet footer statistics, converting from the physical
/// parquet type to the requested logical Delta type.
fn extract_min_scalar(data_type: &DataType, stats: &Statistics) -> Option<Scalar> {
    use PrimitiveType::*;
    let value = match (data_type.as_primitive_opt()?, stats) {
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
        (TimestampNtz, Statistics::Int32(s)) => timestamp_from_date(s.min_opt())?,
        (TimestampNtz, _) => return None, // TODO: Int96 timestamps
        (Decimal(d), Statistics::Int32(i)) => DecimalData::try_new(*i.min_opt()?, *d).ok()?.into(),
        (Decimal(d), Statistics::Int64(i)) => DecimalData::try_new(*i.min_opt()?, *d).ok()?.into(),
        (Decimal(d), Statistics::FixedLenByteArray(b)) => {
            decimal_from_bytes(b.min_bytes_opt(), *d)?
        }
        (Decimal(..), _) => return None,
        // Void columns have no Parquet representation, so no stats exist
        (Void, _) => return None,
    };
    Some(value)
}

/// Extracts the maximum stat value from parquet footer statistics, converting from the physical
/// parquet type to the requested logical Delta type.
fn extract_max_scalar(data_type: &DataType, stats: &Statistics) -> Option<Scalar> {
    use PrimitiveType::*;
    let value = match (data_type.as_primitive_opt()?, stats) {
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
        (TimestampNtz, Statistics::Int32(s)) => timestamp_from_date(s.max_opt())?,
        (TimestampNtz, _) => return None, // TODO: Int96 timestamps
        (Decimal(d), Statistics::Int32(i)) => DecimalData::try_new(*i.max_opt()?, *d).ok()?.into(),
        (Decimal(d), Statistics::Int64(i)) => DecimalData::try_new(*i.max_opt()?, *d).ok()?.into(),
        (Decimal(d), Statistics::FixedLenByteArray(b)) => {
            decimal_from_bytes(b.max_bytes_opt(), *d)?
        }
        (Decimal(..), _) => return None,
        // Void columns have no Parquet representation, so no stats exist
        (Void, _) => return None,
    };
    Some(value)
}

/// Extracts the null count from parquet footer statistics for a column.
fn extract_nullcount(stats: Option<&Statistics>) -> Option<i64> {
    // WARNING: The parquet footer decoding forces missing stats to Some(0), which would cause
    // an IS NULL predicate to wrongly skip the file if it contains any NULL values. To be safe,
    // we must never return Some(0). See https://github.com/apache/arrow-rs/issues/9451.
    let nullcount = stats?.null_count_opt().filter(|n| *n > 0);

    // Parquet nullcount stats are always u64, so we can directly return the value instead of
    // wrapping it in a Scalar. We can safely cast it from u64 to i64 because the nullcount can
    // never be larger than the rowcount and the parquet rowcount stat is i64.
    Some(nullcount? as i64)
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

/// Checks whether a parquet column has any null values in a row group, based on its footer stats.
/// Returns `true` if the column has nulls, or if nullcount stats are unavailable (conservative).
#[allow(dead_code)]
fn column_has_nulls(row_group: &RowGroupMetaData, col_index: usize) -> bool {
    row_group
        .column(col_index)
        .statistics()
        .and_then(|s| s.null_count_opt())
        .is_none_or(|n| n > 0)
}

/// Parquet field indices for a single column's Delta statistics within a checkpoint file.
/// Each index points to a leaf column in the checkpoint parquet schema.
#[derive(Default)]
#[allow(dead_code)]
struct StatsColumnIndices {
    /// Index of `add.stats_parsed.minValues.<col>` in the parquet schema.
    min_index: Option<usize>,
    /// Index of `add.stats_parsed.maxValues.<col>` in the parquet schema.
    max_index: Option<usize>,
    /// Index of `add.stats_parsed.nullCount.<col>` in the parquet schema.
    nullcount_index: Option<usize>,
}

/// A [`ParquetStatsProvider`] for row group skipping in checkpoint and sidecar parquet files.
///
/// Unlike [`RowGroupFilter`] which evaluates a predicate whose column references directly match
/// the parquet schema, this filter evaluates a predicate with physical column names (e.g.
/// `x > 10`, or `col-abc-123 > 10` under column mapping) against
/// a checkpoint file where statistics are nested under `add.stats_parsed.{minValues,maxValues,
/// nullCount}.<col>` and partition values under `add.partitionValues_parsed.<col>`.
///
/// Because checkpoint statistics are aggregates _of_ per-file statistics, a null value in a stat
/// column means the corresponding file was missing that statistic. The parquet footer min/max
/// ignores nulls, which can produce misleading aggregates. To prevent false pruning, this filter
/// returns `None` for any stat whose column contains null values in the row group (checked via
/// the column's own null count in the parquet footer).
///
/// Partition columns are handled separately: their footer min/max of
/// `add.partitionValues_parsed.<col>` can be used directly without null guarding, because parquet
/// footer stats ignore null values (which may appear for non-add action rows).
#[allow(dead_code)]
pub(crate) struct CheckpointRowGroupFilter<'a> {
    row_group: &'a RowGroupMetaData,
    /// Maps each predicate data column to its stats column indices in the checkpoint parquet file.
    stats_column_indices: HashMap<ColumnName, StatsColumnIndices>,
    /// Maps each predicate partition column to its `add.partitionValues_parsed.<col>` field index.
    partition_column_indices: HashMap<ColumnName, usize>,
    /// Physical names of table partition columns.
    partition_columns: &'a HashSet<String>,
}

#[allow(dead_code)]
impl<'a> CheckpointRowGroupFilter<'a> {
    /// Creates a new checkpoint row group filter. The `predicate` uses physical column names
    /// (e.g. `x > 10`, or `col-abc-123 > 10` under column mapping), and `partition_columns`
    /// are the physical names of the table's partition columns.
    pub(crate) fn new(
        row_group: &'a RowGroupMetaData,
        predicate: &Predicate,
        partition_columns: &'a HashSet<String>,
    ) -> Self {
        let (stats_column_indices, partition_column_indices) = compute_checkpoint_field_indices(
            row_group.schema_descr().columns(),
            predicate,
            partition_columns,
        );
        Self {
            row_group,
            stats_column_indices,
            partition_column_indices,
            partition_columns,
        }
    }

    /// Applies the predicate to a checkpoint row group. Returns `false` if the row group can be
    /// safely skipped (none of its add file rows can match the predicate).
    pub(crate) fn apply(
        row_group: &'a RowGroupMetaData,
        predicate: &Predicate,
        partition_columns: &'a HashSet<String>,
    ) -> bool {
        use crate::kernel_predicates::KernelPredicateEvaluator as _;
        CheckpointRowGroupFilter::new(row_group, predicate, partition_columns)
            .eval_sql_where(predicate)
            != Some(false)
    }

    /// Returns `true` if the column is a partition column.
    fn is_partition_column(&self, col: &ColumnName) -> bool {
        is_partition_column(col, self.partition_columns)
    }

    /// Returns the footer statistics for a parquet column at the given index.
    fn get_stats_at(&self, index: usize) -> Option<&Statistics> {
        self.row_group.column(index).statistics()
    }

    /// Retrieves a stat value for a data column, but only if the stat column has no null values
    /// in this row group. A null in the stat column means some file is missing that statistic,
    /// making the footer aggregate unreliable.
    fn get_guarded_stat(
        &self,
        col: &ColumnName,
        data_type: &DataType,
        get_index: impl FnOnce(&StatsColumnIndices) -> Option<usize>,
        extract: impl FnOnce(&DataType, &Statistics) -> Option<Scalar>,
    ) -> Option<Scalar> {
        let indices = self.stats_column_indices.get(col)?;
        let stat_index = get_index(indices)?;
        // If the stat column has any nulls, some files are missing this stat and the footer
        // aggregate is unreliable.
        if column_has_nulls(self.row_group, stat_index) {
            return None;
        }
        extract(data_type, self.get_stats_at(stat_index)?)
    }
}

impl ParquetStatsProvider for CheckpointRowGroupFilter<'_> {
    fn get_parquet_min_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        if self.is_partition_column(col) {
            let &idx = self.partition_column_indices.get(col)?;
            return extract_min_scalar(data_type, self.get_stats_at(idx)?);
        }
        self.get_guarded_stat(col, data_type, |i| i.min_index, extract_min_scalar)
    }

    fn get_parquet_max_stat(&self, col: &ColumnName, data_type: &DataType) -> Option<Scalar> {
        if self.is_partition_column(col) {
            let &idx = self.partition_column_indices.get(col)?;
            return extract_max_scalar(data_type, self.get_stats_at(idx)?);
        }
        let max = self.get_guarded_stat(col, data_type, |i| i.max_index, extract_max_scalar)?;
        Some(adjust_stats_for_truncation(max))
    }

    fn get_parquet_nullcount_stat(&self, col: &ColumnName) -> Option<i64> {
        if self.is_partition_column(col) {
            let &idx = self.partition_column_indices.get(col)?;
            return extract_nullcount(self.get_stats_at(idx));
        }
        let indices = self.stats_column_indices.get(col)?;
        let nullcount_index = indices.nullcount_index?;
        // If the nullCount stat column itself has nulls, some files are missing nullCount
        // stats, making the aggregate unreliable.
        if column_has_nulls(self.row_group, nullcount_index) {
            return None;
        }
        // Return the MAX of the nullCount column from the footer. If any file has null values,
        // max(nullCount) > 0, so IS NULL predicates won't falsely prune the row group.
        let stats = self.get_stats_at(nullcount_index)?;
        extract_max_i64(stats)
    }

    fn get_parquet_rowcount_stat(&self) -> Option<i64> {
        // The footer row count is the number of add file _rows_ in this row group, not the
        // sum of data file row counts. Returning `None` makes IS NOT NULL evaluate to `None`
        // (can't decide), which prevents it from ever pruning checkpoint row groups. This also
        // allows the IS NOT NULL guard in `eval_pred_sql_where` to pass through for
        // null-intolerant binary comparisons, letting the actual comparison decide.
        None
    }
}

/// Adjusts a max stat value to account for millisecond truncation in JSON-serialized stats.
/// `stats_parsed` inherits from JSON stats which truncate timestamps to millisecond precision:
/// `stored_max <= actual_max <= stored_max + 999us`. Adding 999us ensures we never falsely
/// prune files whose actual max exceeds the truncated value. Non-timestamp values pass through
/// unchanged.
///
/// See also [`DataSkippingPredicateCreator::adjust_scalar_for_max_stat_truncation`] in
/// `scan/data_skipping.rs`, which handles the same truncation issue from the predicate side
/// (subtracting 999us from the comparison value instead of adding to the stat).
#[allow(dead_code)]
fn adjust_stats_for_truncation(val: Scalar) -> Scalar {
    match val {
        Scalar::Timestamp(us) => Scalar::Timestamp(us.saturating_add(999)),
        Scalar::TimestampNtz(us) => Scalar::TimestampNtz(us.saturating_add(999)),
        other => other,
    }
}

/// Extracts the maximum value as i64 from parquet statistics. Used for reading checkpoint
/// nullCount stats where the footer max represents the largest per-file null count.
#[allow(dead_code)]
fn extract_max_i64(stats: &Statistics) -> Option<i64> {
    match stats {
        Statistics::Int64(s) => Some(*s.max_opt()?),
        Statistics::Int32(s) => Some(i64::from(*s.max_opt()?)),
        _ => None,
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

/// Returns `true` if the column is a top-level partition column.
/// Delta partition columns are always top-level (no nested partition columns).
#[allow(dead_code)]
fn is_partition_column(col: &ColumnName, partition_columns: &HashSet<String>) -> bool {
    let path = col.path();
    path.len() == 1 && partition_columns.contains(path[0].as_str())
}

/// Builds column index mappings for checkpoint row group skipping. Maps each predicate column to
/// its corresponding stats column indices
/// (`add.stats_parsed.{minValues,maxValues,nullCount}.<col>`) or partition column index
/// (`add.partitionValues_parsed.<col>`).
#[allow(dead_code)]
fn compute_checkpoint_field_indices(
    fields: &[ColumnDescPtr],
    predicate: &Predicate,
    partition_columns: &HashSet<String>,
) -> (
    HashMap<ColumnName, StatsColumnIndices>,
    HashMap<ColumnName, usize>,
) {
    let referenced_columns = predicate.references();
    let mut stats_indices: HashMap<ColumnName, StatsColumnIndices> = HashMap::new();
    let mut partition_indices: HashMap<ColumnName, usize> = HashMap::new();

    for (i, field) in fields.iter().enumerate() {
        let parts = field.path().parts();

        // Match add.partitionValues_parsed.<col>
        // Delta partition columns are always top-level (no nested partition columns).
        if parts.len() == 3 && parts[0] == "add" && parts[1] == "partitionValues_parsed" {
            let col_name = ColumnName::new([&parts[2]]);
            if referenced_columns.contains(&col_name)
                && is_partition_column(&col_name, partition_columns)
            {
                partition_indices.insert(col_name, i);
            }
            continue;
        }

        // Match add.stats_parsed.{minValues,maxValues,nullCount}.<col_path...>
        if parts.len() >= 4 && parts[0] == "add" && parts[1] == "stats_parsed" {
            let stat_type = parts[2].as_str();
            let col_name = ColumnName::new(&parts[3..]);
            // Skip partition columns (they use partitionValues_parsed, not stats_parsed)
            if !referenced_columns.contains(&col_name)
                || is_partition_column(&col_name, partition_columns)
            {
                continue;
            }
            let entry = stats_indices.entry(col_name).or_default();
            match stat_type {
                "minValues" => entry.min_index = Some(i),
                "maxValues" => entry.max_index = Some(i),
                "nullCount" => entry.nullcount_index = Some(i),
                _ => {}
            }
        }
    }

    (stats_indices, partition_indices)
}
