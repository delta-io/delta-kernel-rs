//! Builder for overwriting the schema of an existing Delta table.
//!
//! This module contains [`OverwriteSchemaTransactionBuilder`], which validates and constructs an
//! [`OverwriteSchemaTransaction`] from user-provided schema and partition columns.
//!
//! Use [`overwrite_schema()`](super::super::overwrite_schema::overwrite_schema) as the entry
//! point rather than constructing the builder directly.

use std::sync::Arc;

use crate::committer::Committer;
use crate::engine_data::FilteredEngineData;
use crate::schema::SchemaRef;
use crate::snapshot::SnapshotRef;
use crate::table_features::{assign_column_mapping_metadata, ColumnMappingMode, Operation};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::overwrite_schema::OverwriteSchemaTransaction;
use crate::{DeltaResult, Engine, Error};

/// Builder for configuring a schema-overwrite transaction on an existing Delta table.
///
/// Use this to validate and build an [`OverwriteSchemaTransaction`]. The `build()` method
/// performs all validation and I/O:
/// - Validates that the operation is supported on the snapshot
/// - Validates the schema (non-empty, partition columns exist)
/// - Scans the entire table for files to remove
/// - Handles column mapping (reads `maxColumnId`, assigns metadata)
/// - Creates new `Metadata` via `try_with_new_schema()`
///
/// Created via [`overwrite_schema()`](super::super::overwrite_schema::overwrite_schema).
pub struct OverwriteSchemaTransactionBuilder {
    snapshot: SnapshotRef,
    new_schema: SchemaRef,
    partition_columns: Vec<String>,
}

impl OverwriteSchemaTransactionBuilder {
    /// Creates a new OverwriteSchemaTransactionBuilder.
    ///
    /// This is typically called via
    /// [`overwrite_schema()`](super::super::overwrite_schema::overwrite_schema) rather than
    /// directly.
    pub fn new(
        snapshot: SnapshotRef,
        new_schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            snapshot,
            new_schema,
            partition_columns,
        }
    }

    /// Builds an [`OverwriteSchemaTransaction`] that can be committed to overwrite the table
    /// schema.
    ///
    /// This method performs all validation and heavy I/O:
    /// 1. Validates the operation is supported on the snapshot
    /// 2. Validates the schema (non-empty, partition columns exist)
    /// 3. Scans the entire table for files to remove
    /// 4. Handles column mapping (reads `maxColumnId`, assigns metadata to new schema)
    /// 5. Creates new `Metadata` via `try_with_new_schema()`
    /// 6. Constructs the transaction
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine instance to use for validation and scanning
    /// * `committer` - The committer to use for the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table's protocol doesn't support write operations
    /// - The schema is empty or contains metadata columns
    /// - A partition column doesn't exist in the schema
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<OverwriteSchemaTransaction> {
        // Validate the operation is supported on the snapshot
        self.snapshot
            .table_configuration()
            .ensure_operation_supported(Operation::Write)?;

        // Validate schema is non-empty (fail early before scanning files)
        if self.new_schema.fields().len() == 0 {
            return Err(Error::generic("Schema cannot be empty"));
        }

        // Note: metadata column validation is handled by try_with_new_schema() below.

        // Validate partition columns exist in schema
        for col in &self.partition_columns {
            if self.new_schema.field(col).is_none() {
                return Err(Error::generic(format!(
                    "Partition column '{}' not found in schema",
                    col
                )));
            }
        }

        // Scan all existing files and collect them for removal
        let scan = self.snapshot.clone().scan_builder().build()?;
        let remove_files: Vec<FilteredEngineData> = scan
            .scan_metadata(engine)?
            .map(|m| Ok(m?.scan_files))
            .collect::<DeltaResult<_>>()?;

        // Handle column mapping: if the table uses column mapping, assign metadata to new schema
        let table_config = self.snapshot.table_configuration();
        let column_mapping_mode = table_config.column_mapping_mode();
        let mut configuration = table_config.metadata().configuration().clone();

        let effective_schema = match column_mapping_mode {
            ColumnMappingMode::Name | ColumnMappingMode::Id => {
                // Start from the existing maxColumnId to avoid collisions with historical files
                let mut max_id: i64 = configuration
                    .get(COLUMN_MAPPING_MAX_COLUMN_ID)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let transformed_schema =
                    assign_column_mapping_metadata(&self.new_schema, &mut max_id)?;
                configuration.insert(
                    COLUMN_MAPPING_MAX_COLUMN_ID.to_string(),
                    max_id.to_string(),
                );
                Arc::new(transformed_schema)
            }
            ColumnMappingMode::None => self.new_schema.clone(),
        };

        // Create new Metadata preserving table identity
        let new_metadata = table_config.metadata().try_with_new_schema(
            effective_schema,
            self.partition_columns,
            configuration,
        )?;

        // Read clustering columns from snapshot
        let clustering_columns = self.snapshot.get_clustering_columns(engine)?;

        // Build the transaction
        OverwriteSchemaTransaction::try_new_overwrite_schema(
            self.snapshot,
            committer,
            new_metadata,
            remove_files,
            clustering_columns,
        )
    }
}
