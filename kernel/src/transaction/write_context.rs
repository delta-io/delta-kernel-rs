use std::collections::HashMap;
use std::sync::Arc;

use rand::Rng;
use url::Url;

use crate::actions::deletion_vector::DeletionVectorPath;
use crate::expressions::{ColumnName, ExpressionRef};
use crate::partition::hive::build_partition_path;
use crate::schema::SchemaRef;
use crate::table_features::ColumnMappingMode;

/// Table-wide write state shared across all [`WriteContext`] instances created by a
/// [`Transaction`]. Holds the target directory, schemas, column mapping mode, stats columns,
/// and resolved partition column name pairs (logical and physical).
///
/// [`Transaction`]: super::Transaction
#[derive(Debug)]
pub(super) struct SharedWriteState {
    pub(super) table_root: Url,
    pub(super) logical_schema: SchemaRef,
    pub(super) physical_schema: SchemaRef,
    pub(super) logical_to_physical: ExpressionRef,
    pub(super) column_mapping_mode: ColumnMappingMode,
    pub(super) stats_columns: Vec<ColumnName>,
    /// Partition columns in metadata-defined order: (logical_name, physical_name).
    pub(super) logical_and_physical_partition_columns: Vec<(String, String)>,
}

/// A write context for a specific partition or an unpartitioned table. Created by
/// [`Transaction::partitioned_write_context`] or [`Transaction::unpartitioned_write_context`].
///
/// Note: clustered tables are unpartitioned and use `unpartitioned_write_context`.
///
/// Contains both table-wide state (shared cheaply via `Arc`) and per-partition state
/// (serialized partition values with physical column names as keys). How you use a
/// `WriteContext` depends on your engine:
///
/// - **`DefaultEngine` consumers**: pass this to [`DefaultEngine::write_parquet`], which
///   handles everything (transform, write, partition metadata).
/// - **Arrow-based custom engines**: write parquet yourself, then call
///   [`build_add_file_metadata`] with the resulting `DataFileMetadata` and this
///   `WriteContext` to produce the Add action `EngineData` for [`Transaction::add_files`].
/// - **Fully custom (non-Arrow) engines**: use [`physical_partition_values`] to build the
///   `partitionValues` map in Add actions directly.
///
/// [`Transaction::partitioned_write_context`]: super::Transaction::partitioned_write_context
/// [`Transaction::unpartitioned_write_context`]: super::Transaction::unpartitioned_write_context
/// [`DefaultEngine::write_parquet`]: crate::engine::default::DefaultEngine::write_parquet
/// [`build_add_file_metadata`]: crate::engine::default::build_add_file_metadata
/// [`Transaction::add_files`]: super::Transaction::add_files
/// [`physical_partition_values`]: WriteContext::physical_partition_values
#[derive(Debug)]
pub struct WriteContext {
    pub(super) shared: Arc<SharedWriteState>,
    /// Physical column name -> serialized value (`None` = null partition value).
    /// Empty for unpartitioned tables. Used for building `add.partitionValues` in the
    /// Delta log (which requires physical column names as keys).
    pub(super) physical_partition_values: HashMap<String, Option<String>>,
    /// Hive-style partition path using logical column names in metadata-defined order
    /// (e.g., `"year=2024/region=US/"`). Empty string for unpartitioned tables.
    ///
    /// This is not necessarily the filesystem path that data files are written to. With
    /// column mapping enabled, [`write_dir`] uses a random prefix instead. Computed once
    /// at construction; exposed via [`partition_group_key`].
    ///
    /// [`partition_group_key`]: WriteContext::partition_group_key
    /// [`write_dir`]: WriteContext::write_dir
    logical_partition_path: String,
}

impl WriteContext {
    pub(super) fn new(
        shared: Arc<SharedWriteState>,
        physical_partition_values: HashMap<String, Option<String>>,
    ) -> Self {
        let logical_partition_path =
            Self::build_logical_partition_path(&shared, &physical_partition_values);
        Self {
            shared,
            physical_partition_values,
            logical_partition_path,
        }
    }

    /// Builds a Hive-style partition path using logical column names and serialized values
    /// (e.g., `year=2024/region=US/`).
    fn build_logical_partition_path(
        shared: &SharedWriteState,
        physical_values: &HashMap<String, Option<String>>,
    ) -> String {
        let columns: Vec<(&str, Option<&str>)> = shared
            .logical_and_physical_partition_columns
            .iter()
            .map(|(logical, physical)| {
                let value = physical_values
                    .get(physical.as_str())
                    .and_then(|v| v.as_deref());
                (logical.as_str(), value)
            })
            .collect();
        build_partition_path(&columns)
    }

    /// Returns the table root URL.
    pub fn table_root_dir(&self) -> &Url {
        &self.shared.table_root
    }

    /// Returns the recommended directory for writing Parquet data files. Connectors should
    /// write files as `<write_dir>/<uuid>.parquet`. Not strictly required (data files can
    /// live anywhere under the table root), but produces the conventional layout.
    ///
    /// ```text
    ///              | CM OFF                              | CM ON
    /// -------------|-------------------------------------|-------------------------------
    /// Unpartitioned| <table_root>/<uuid>.parquet         | <table_root>/<2char>/<uuid>.parquet
    /// Partitioned  | <table_root>/col=val/.../<uuid>.pq  | <table_root>/<2char>/<uuid>.parquet
    /// ```
    ///
    /// CM ON uses a random 2-char alphanumeric prefix (matching Delta-Spark's
    /// `getRandomPrefix`) to avoid S3 hotspots. Each call generates a fresh prefix,
    /// matching Delta-Spark's per-file behavior.
    ///
    /// # Warning
    ///
    /// **Not suitable as a group key.** With CM ON, the random prefix changes on every
    /// call, so two calls with the same partition values produce different URLs. Use
    /// [`partition_group_key`] instead when you need a stable, hashable identifier for
    /// grouping writes by partition.
    ///
    /// [`partition_group_key`]: WriteContext::partition_group_key
    // TODO(#2357): respect `delta.randomizeFilePrefixes` and `delta.randomPrefixLength`
    // table properties. Currently random prefixes are only used when column mapping is on.
    pub fn write_dir(&self) -> Url {
        let mut url = self.shared.table_root.clone();
        match self.shared.column_mapping_mode {
            ColumnMappingMode::None => {
                // No column mapping: use Hive-style partition directories for partitioned
                // tables, or just the table root for unpartitioned tables.
                if !self.logical_partition_path.is_empty() {
                    url.set_path(&format!("{}{}", url.path(), self.logical_partition_path));
                }
            }
            ColumnMappingMode::Id | ColumnMappingMode::Name => {
                // Column mapping ON: random 2-char prefix avoids S3 hotspots and avoids
                // exposing physical UUID column names in Hive-style directory paths.
                let prefix = random_alphanumeric_prefix();
                url.set_path(&format!("{}{}/", url.path(), prefix));
            }
        }
        url
    }

    /// Returns the logical (user-facing) table schema. Connectors use this to determine
    /// the schema of data to write.
    pub fn logical_schema(&self) -> &SchemaRef {
        &self.shared.logical_schema
    }

    /// Returns the physical schema (partition columns removed if applicable, column mapping
    /// applied). Partition columns are kept when `materializePartitionColumns` is enabled.
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
    pub fn physical_partition_values(&self) -> &HashMap<String, Option<String>> {
        &self.physical_partition_values
    }

    /// Returns a deterministic, hashable string that uniquely identifies this partition.
    ///
    /// Connectors that group rows by partition values need a stable key. This method provides
    /// one. The returned string uses Hive-style encoding with logical column names in
    /// metadata-defined order: `"year=2024/region=US/"`.
    ///
    /// The key uses **logical** column names regardless of the table's column mapping mode.
    /// This is intentional: the group key is a connector-side artifact that never reaches
    /// the Delta log, so physical names (which may be opaque UUIDs) offer no benefit.
    /// Logical names make the key readable and debuggable.
    ///
    /// Returns an empty string for unpartitioned tables (all rows belong to one group).
    ///
    /// # Warning
    ///
    /// **This is not the path you should write data files to.** Use [`write_dir`] for that.
    /// The two methods serve different purposes:
    ///
    /// - `partition_group_key()` is **stable**: same partition values always produce the
    ///   same string, regardless of column mapping mode. Safe to use as a `HashMap` key.
    /// - [`write_dir`] is a **filesystem URL**: with column mapping ON, it includes a
    ///   random prefix that changes on every call. Not suitable as a group key.
    ///
    /// [`write_dir`]: WriteContext::write_dir
    pub fn partition_group_key(&self) -> &str {
        &self.logical_partition_path
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
    // TODO(#2357): generate the random prefix internally based on table properties
    // (delta.randomizeFilePrefixes / delta.randomPrefixLength) instead of requiring the
    // caller to pass it. Connectors that need custom paths can use table_root_dir() directly.
    pub fn new_deletion_vector_path(&self, random_prefix: String) -> DeletionVectorPath {
        DeletionVectorPath::new(self.shared.table_root.clone(), random_prefix)
    }
}

/// Generates a random 2-character alphanumeric prefix for partition directory paths, matching
/// Delta-Spark's `Utils.getRandomPrefix` (`Random.alphanumeric.take(2)`). Used when column mapping
/// is enabled to avoid S3 hotspots and prevent leaking physical UUID column names into paths.
fn random_alphanumeric_prefix() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..2)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Expression;
    use std::collections::HashSet;
    use std::sync::Arc;

    use rstest::rstest;

    use crate::schema::{DataType, StructField, StructType};

    /// Prefix added to logical partition column names to produce fake physical names.
    /// Using distinct names exercises the logical-to-physical resolution path.
    const PHYS_PREFIX: &str = "phys_";

    /// Creates a test WriteContext with fake physical names (`phys_<logical>`).
    /// `partition_values` should be keyed by **logical** names; this helper translates
    /// them to physical keys internally.
    fn make_write_context(
        cm_mode: ColumnMappingMode,
        partition_columns: Vec<String>,
        partition_values: HashMap<String, Option<String>>,
    ) -> WriteContext {
        let schema = Arc::new(StructType::new_unchecked(vec![StructField::nullable(
            "value",
            DataType::INTEGER,
        )]));
        let logical_and_physical_cols: Vec<(String, String)> = partition_columns
            .iter()
            .map(|name| (name.clone(), format!("{PHYS_PREFIX}{name}")))
            .collect();
        let physical_partition_values: HashMap<String, Option<String>> = logical_and_physical_cols
            .iter()
            .filter_map(|(logical, physical)| {
                partition_values
                    .get(logical.as_str())
                    .map(|value| (physical.clone(), value.clone()))
            })
            .collect();
        let shared = Arc::new(SharedWriteState {
            table_root: Url::parse("s3://bucket/table/").unwrap(),
            logical_schema: schema.clone(),
            physical_schema: schema.clone(),
            logical_to_physical: Arc::new(Expression::literal(true)),
            column_mapping_mode: cm_mode,
            stats_columns: vec![],
            logical_and_physical_partition_columns: logical_and_physical_cols,
        });
        WriteContext::new(shared, physical_partition_values)
    }

    /// Tests the cross product of ColumnMappingMode x partitioned/unpartitioned.
    #[rstest]
    fn test_write_dir_structure(
        #[values(
            ColumnMappingMode::None,
            ColumnMappingMode::Name,
            ColumnMappingMode::Id
        )]
        cm_mode: ColumnMappingMode,
        #[values(true, false)] is_partitioned: bool,
    ) {
        let (cols, pvs) = if is_partitioned {
            (
                vec!["year".into(), "month".into()],
                HashMap::from([
                    ("year".into(), Some("2024".into())),
                    ("month".into(), Some("03".into())),
                ]),
            )
        } else {
            (vec![], HashMap::new())
        };
        let wc = make_write_context(cm_mode, cols, pvs);
        let path = wc.write_dir().path().to_string();

        match cm_mode {
            ColumnMappingMode::None if !is_partitioned => {
                assert_eq!(
                    path, "/table/",
                    "CM off, unpartitioned: should be table root"
                );
            }
            ColumnMappingMode::None => {
                assert_eq!(
                    path, "/table/year=2024/month=03/",
                    "CM off, partitioned: full Hive-style path"
                );
            }
            ColumnMappingMode::Name | ColumnMappingMode::Id => {
                assert!(
                    !path.contains("year="),
                    "CM on: should NOT contain Hive-style dirs, got: {path}"
                );
                // Path should be /table/<2-char-alphanumeric>/ regardless of partitioning.
                let prefix_dir = path
                    .strip_prefix("/table/")
                    .unwrap()
                    .strip_suffix('/')
                    .unwrap();
                assert_eq!(
                    prefix_dir.len(),
                    2,
                    "expected 2-char prefix, got: {prefix_dir}"
                );
                assert!(
                    prefix_dir.chars().all(|c| c.is_ascii_alphanumeric()),
                    "prefix should be alphanumeric, got: {prefix_dir}"
                );
            }
        }
    }

    #[test]
    fn test_write_dir_cm_on_generates_different_prefixes_per_call() {
        let wc = make_write_context(ColumnMappingMode::Name, vec![], HashMap::new());
        let dirs: Vec<String> = (0..20).map(|_| wc.write_dir().path().to_string()).collect();
        let unique: HashSet<_> = dirs.iter().collect();
        assert!(
            unique.len() > 1,
            "20 calls should produce at least 2 distinct prefixes"
        );
    }

    #[test]
    fn test_write_dir_cm_off_partitioned_null_value_uses_hive_default() {
        let wc = make_write_context(
            ColumnMappingMode::None,
            vec!["region".into()],
            HashMap::from([("region".into(), None)]),
        );
        let path = wc.write_dir().path().to_string();
        assert!(
            path.contains("__HIVE_DEFAULT_PARTITION__"),
            "null partition value should use HIVE_DEFAULT_PARTITION, got: {path}"
        );
    }

    #[test]
    fn test_random_alphanumeric_prefix_format() {
        for _ in 0..100 {
            let prefix = random_alphanumeric_prefix();
            assert_eq!(prefix.len(), 2, "prefix should be exactly 2 chars");
            assert!(
                prefix.chars().all(|c| c.is_ascii_alphanumeric()),
                "prefix should be alphanumeric, got: {prefix}"
            );
        }
    }

    // ============================================================================
    // partition_group_key
    // ============================================================================

    #[test]
    fn test_partition_group_key_unpartitioned_returns_empty() {
        let wc = make_write_context(ColumnMappingMode::None, vec![], HashMap::new());
        assert_eq!(wc.partition_group_key(), "");
    }

    #[test]
    fn test_partition_group_key_mixed_null_and_value_produces_correct_path() {
        let wc = make_write_context(
            ColumnMappingMode::Name,
            vec!["year".into(), "region".into()],
            HashMap::from([
                ("year".into(), Some("2024".into())),
                ("region".into(), None),
            ]),
        );
        assert_eq!(
            wc.partition_group_key(),
            "year=2024/region=__HIVE_DEFAULT_PARTITION__/"
        );
    }

    #[rstest]
    fn test_partition_group_key_preserves_partition_column_order_across_cm_modes(
        #[values(
            ColumnMappingMode::None,
            ColumnMappingMode::Name,
            ColumnMappingMode::Id
        )]
        cm_mode: ColumnMappingMode,
    ) {
        let wc = make_write_context(
            cm_mode,
            vec!["b".into(), "a".into()],
            HashMap::from([
                ("a".into(), Some("1".into())),
                ("b".into(), Some("2".into())),
            ]),
        );
        assert_eq!(wc.partition_group_key(), "b=2/a=1/");
    }
}
