use std::collections::HashMap;
use std::sync::Arc;

use url::Url;

use crate::actions::deletion_vector::DeletionVectorPath;
use crate::expressions::{ColumnName, ExpressionRef};
use crate::partition::build_partition_path;
use crate::schema::SchemaRef;
use crate::table_features::ColumnMappingMode;

/// Table-wide write state shared across all [`WriteContext`] instances created by a
/// [`Transaction`]. Holds the target directory, schemas, column mapping mode, stats columns,
/// and logical partition column names.
///
/// [`Transaction`]: super::Transaction
#[derive(Debug)]
pub(super) struct SharedWriteState {
    pub(super) target_dir: Url,
    pub(super) logical_schema: SchemaRef,
    pub(super) physical_schema: SchemaRef,
    pub(super) logical_to_physical: ExpressionRef,
    pub(super) column_mapping_mode: ColumnMappingMode,
    pub(super) stats_columns: Vec<ColumnName>,
    /// Logical partition column names in metadata-defined order.
    pub(super) logical_partition_columns: Vec<String>,
}

/// A write context for a specific partition (or an unpartitioned table). Created by
/// [`Transaction::partitioned_write_context`] or [`Transaction::unpartitioned_write_context`].
///
/// Contains both table-wide state (shared cheaply via `Arc`) and per-partition state
/// (serialized partition values with physical column names as keys). Pass this to
/// [`DefaultEngine::write_parquet`] to write data files with correctly formatted partition
/// metadata.
///
/// For custom engines that bypass `DefaultEngine`, use [`partition_values`] to build the
/// `partitionValues` map in Add actions. A Hive-style target directory helper is also
/// available as an internal API.
///
/// [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet
/// [`partition_values`]: WriteContext::partition_values
#[derive(Debug)]
pub struct WriteContext {
    pub(super) shared: Arc<SharedWriteState>,
    /// Physical column name -> serialized value (`None` = null partition value).
    /// Empty for unpartitioned tables. Ordering for hive-style paths comes from
    /// `shared.logical_partition_columns`, not from this map.
    pub(super) physical_partition_values: HashMap<String, Option<String>>,
}

impl WriteContext {
    /// Returns the table root URL.
    pub fn target_dir(&self) -> &Url {
        &self.shared.target_dir
    }

    /// Returns the logical (user-facing) table schema. Connectors use this to determine
    /// the schema of data to write.
    pub fn logical_schema(&self) -> &SchemaRef {
        &self.shared.logical_schema
    }

    /// Returns the physical schema (partition columns removed, column mapping applied).
    pub fn physical_schema(&self) -> &SchemaRef {
        &self.shared.physical_schema
    }

    /// Returns the expression that transforms logical data to physical data for writing.
    pub fn logical_to_physical(&self) -> ExpressionRef {
        self.shared.logical_to_physical.clone()
    }

    /// The [`ColumnMappingMode`] for this table.
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.shared.column_mapping_mode
    }

    /// Returns the column names that should have statistics collected during writes.
    ///
    /// Based on table configuration (dataSkippingNumIndexedCols, dataSkippingStatsColumns).
    pub fn stats_columns(&self) -> &[ColumnName] {
        &self.shared.stats_columns
    }

    /// Returns the serialized partition values for this write context. Keys are physical
    /// column names; values are protocol-serialized strings (`None` = null).
    ///
    /// For unpartitioned tables, this is empty.
    pub fn partition_values(&self) -> &HashMap<String, Option<String>> {
        &self.physical_partition_values
    }

    /// Returns the target directory where data files for this partition should be written.
    ///
    /// The directory structure depends on the table's column mapping mode:
    ///
    /// | | Not Partitioned | Partitioned |
    /// |---|---|---|
    /// | **CM OFF** | `<table_root>/` | `<table_root>/col=val/.../` |
    /// | **CM ON** | `<table_root>/<2char>/` | `<table_root>/<2char>/` |
    ///
    /// With column mapping OFF and partitioned tables, the path uses **logical** column names
    /// and Hive-style encoding (matching Spark). With column mapping ON, a random 2-character
    /// alphanumeric prefix is used unconditionally (matching Spark's
    /// `getRandomPrefix`). Column mapping forces random prefixes to avoid S3 hotspots and
    /// to prevent leaking physical UUID column names into directory paths.
    ///
    /// The engine appends the filename (e.g., `<uuid>.parquet`) to this directory to form
    /// the full file path.
    ///
    /// # Examples
    ///
    /// ```text
    /// // CM OFF, partitioned: year=2024, region=US
    /// write_dir() -> "s3://bucket/table/year=2024/region=US/"
    ///
    /// // CM OFF, not partitioned
    /// write_dir() -> "s3://bucket/table/"
    ///
    /// // CM ON (partitioned or not)
    /// write_dir() -> "s3://bucket/table/3v/"
    /// ```
    pub fn write_dir(&self) -> Url {
        let mut url = self.shared.target_dir.clone();
        match self.shared.column_mapping_mode {
            ColumnMappingMode::None => {
                // No column mapping: use Hive-style partition directories for partitioned
                // tables, or just the table root for unpartitioned tables.
                if !self.shared.logical_partition_columns.is_empty() {
                    let path_suffix = self.hive_partition_path_suffix();
                    url.set_path(&format!("{}{}", url.path(), path_suffix));
                }
            }
            ColumnMappingMode::Id | ColumnMappingMode::Name => {
                // Column mapping ON: use a random 2-char alphanumeric prefix (matching
                // Spark's getRandomPrefix). This avoids S3 hotspots and prevents leaking
                // physical UUID column names into directory paths.
                let prefix = random_alphanumeric_prefix();
                url.set_path(&format!("{}{}/", url.path(), prefix));
            }
        }
        url
    }

    /// Builds the Hive-style partition path suffix (e.g., `year=2024/region=US/`).
    /// Only valid when column mapping is OFF. Uses logical column names for path segments
    /// and looks up serialized values by physical key.
    fn hive_partition_path_suffix(&self) -> String {
        let columns: Vec<(&str, Option<&str>)> = self
            .shared
            .logical_partition_columns
            .iter()
            .map(|logical_name| {
                // physical_partition_values is keyed by physical name (for
                // AddFile.partitionValues), but hive-style paths use logical column names.
                let field = self.shared.logical_schema.field(logical_name);
                let physical_name = field
                    .map(|f| f.physical_name(self.shared.column_mapping_mode))
                    .unwrap_or(logical_name.as_str());
                let value = self
                    .physical_partition_values
                    .get(physical_name)
                    .and_then(|v| v.as_deref());
                (logical_name.as_str(), value)
            })
            .collect();
        build_partition_path(&columns)
    }

    /// Generate a new unique absolute URL for a deletion vector file.
    ///
    /// This method generates a unique file name in the table directory.
    /// Each call to this method returns a new unique path.
    ///
    /// # Arguments
    ///
    /// * `random_prefix` - A random prefix to use for the deletion vector file name.
    ///   Making this non-empty can help distributed load on object storage when writing/reading
    ///   to avoid throttling.  Typically a random string of 2-4 characters is sufficient
    ///   for this purpose.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let write_context = transaction.unpartitioned_write_context()?;
    /// let dv_path = write_context.new_deletion_vector_path(String::from(rand_string()));
    /// ```
    pub fn new_deletion_vector_path(&self, random_prefix: String) -> DeletionVectorPath {
        DeletionVectorPath::new(self.shared.target_dir.clone(), random_prefix)
    }
}

/// Generates a random 2-character alphanumeric prefix for partition directory paths, matching
/// Spark's `Utils.getRandomPrefix` (`Random.alphanumeric.take(2)`). Used when column mapping
/// is enabled to avoid S3 hotspots and prevent leaking physical UUID column names into paths.
fn random_alphanumeric_prefix() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..2)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}
