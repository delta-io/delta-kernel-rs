use url::Url;

use crate::actions::deletion_vector::DeletionVectorPath;
use crate::expressions::{ColumnName, ExpressionRef};
use crate::schema::SchemaRef;
use crate::table_features::ColumnMappingMode;
use crate::transaction::PathMode;

/// WriteContext is data derived from a [`Transaction`] that can be provided to writers in order to
/// write table data.
///
/// [`Transaction`]: super::Transaction
pub struct WriteContext {
    target_dir: Url,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
    logical_to_physical: ExpressionRef,
    column_mapping_mode: ColumnMappingMode,
    /// Column names that should have statistics collected during writes.
    stats_columns: Vec<ColumnName>,
    /// How file paths should be stored in the Delta log.
    path_mode: PathMode,
}

impl WriteContext {
    pub(crate) fn new(
        target_dir: Url,
        logical_schema: SchemaRef,
        physical_schema: SchemaRef,
        logical_to_physical: ExpressionRef,
        column_mapping_mode: ColumnMappingMode,
        stats_columns: Vec<ColumnName>,
        path_mode: PathMode,
    ) -> Self {
        WriteContext {
            target_dir,
            logical_schema,
            physical_schema,
            logical_to_physical,
            column_mapping_mode,
            stats_columns,
            path_mode,
        }
    }

    pub fn target_dir(&self) -> &Url {
        &self.target_dir
    }

    pub fn logical_schema(&self) -> &SchemaRef {
        &self.logical_schema
    }

    pub fn physical_schema(&self) -> &SchemaRef {
        &self.physical_schema
    }

    pub fn logical_to_physical(&self) -> ExpressionRef {
        self.logical_to_physical.clone()
    }

    /// The [`ColumnMappingMode`] for this table.
    pub fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }

    /// Returns the column names that should have statistics collected during writes.
    ///
    /// Based on table configuration (dataSkippingNumIndexedCols, dataSkippingStatsColumns).
    pub fn stats_columns(&self) -> &[ColumnName] {
        &self.stats_columns
    }

    /// Returns the [`PathMode`] that controls how file paths should be stored in the Delta log.
    ///
    /// Connectors should use this to determine whether to store relative or absolute paths in the
    /// add file metadata passed to [`Transaction::add_files`].
    ///
    /// [`Transaction::add_files`]: super::Transaction::add_files
    pub fn path_mode(&self) -> PathMode {
        self.path_mode
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
    /// let write_context = transaction.get_write_context();
    /// let dv_path = write_context.new_deletion_vector_path(String::from(rand_string()));
    /// // dv_url might be: s3://bucket/table/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin
    /// ```
    pub fn new_deletion_vector_path(&self, random_prefix: String) -> DeletionVectorPath {
        DeletionVectorPath::new(self.target_dir.clone(), random_prefix)
    }
}
