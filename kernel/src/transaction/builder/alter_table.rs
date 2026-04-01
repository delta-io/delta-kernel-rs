//! Builder for alter-table (schema evolution) transactions.
//!
//! This module contains [`AlterTableTransactionBuilder`], which validates and constructs an
//! [`AlterTableTransaction`] from user-provided schema operations.
//!
//! Use [`alter_table()`](super::super::alter_table::alter_table) as the entry point rather
//! than constructing the builder directly.

use std::marker::PhantomData;
use std::sync::Arc;

use uuid::Uuid;

use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::{ColumnMetadataKey, MetadataValue, SchemaRef, StructField, StructType};
use crate::snapshot::SnapshotRef;
use crate::table_features::{ColumnMappingMode, Operation};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::alter_table::AlterTableTransaction;
use crate::{DeltaResult, Engine, Error};

/// Operations that can be applied to evolve a table schema.
///
/// Operations are validated and applied in order during
/// [`AlterTableTransactionBuilder::build`]. Each operation sees the schema
/// state after all prior operations have been applied.
#[derive(Debug, Clone)]
pub(crate) enum SchemaOperation {
    /// Add a column. `parent` is empty for top-level, or a path for nested (future).
    AddColumn {
        parent: ColumnName,
        field: StructField,
    },
    /// Drop a column by path. Requires column mapping.
    DropColumn { path: ColumnName },
    /// Rename a column by path. Requires column mapping.
    RenameColumn { path: ColumnName, new_name: String },
    /// Widen a column's nullability. Only NOT NULL -> nullable is allowed.
    SetNullable { path: ColumnName },
}

/// Result of applying schema operations to a table schema.
#[derive(Debug)]
pub(crate) struct SchemaEvolutionResult {
    /// The evolved schema after all operations are applied.
    pub schema: SchemaRef,
    /// The new max column ID (if column mapping is active and columns were added).
    /// Used to update `delta.columnMapping.maxColumnId` in table properties.
    pub new_max_column_id: Option<i64>,
}

// =============================================================================
// Type-state markers for the builder
// =============================================================================

/// Initial builder state. All 4 operations are available but `build()` is not -- at least one
/// operation must be specified.
#[derive(Debug)]
pub struct Ready;

/// Builder state after `add_column`, `drop_column`, or `set_nullable`. These operations can be
/// freely chained with each other. `build()` is available.
#[derive(Debug)]
pub struct Modifying;

/// Builder state after `rename_column`. This is a terminal state -- no further operations can
/// be chained. Only `build()` is available.
#[derive(Debug)]
pub struct Renaming;

/// Builder for constructing an [`AlterTableTransaction`].
///
/// Created via [`Snapshot::alter_table`](crate::snapshot::Snapshot::alter_table). Schema
/// operations are buffered and validated when [`build`] is called.
///
/// The builder uses a type-state pattern to enforce chaining rules at compile time:
/// - `add_column`, `drop_column`, and `set_nullable` can be freely chained together.
/// - `rename_column` must be the only operation (cannot be combined with others).
/// - `build()` is only available after at least one operation has been specified.
///
/// # Example
///
/// ```rust,no_run
/// use delta_kernel::schema::{StructField, DataType};
/// use delta_kernel::committer::FileSystemCommitter;
/// use delta_kernel::snapshot::SnapshotRef;
/// # use delta_kernel::Engine;
/// # fn example(engine: &dyn Engine, snapshot: SnapshotRef) -> delta_kernel::DeltaResult<()> {
///
/// let result = snapshot
///     .alter_table()
///     .add_column(StructField::nullable("email", DataType::STRING))
///     .drop_column("old_field")
///     .set_nullable("strict_col")
///     .build(engine, Box::new(FileSystemCommitter::new()))?
///     .commit(engine)?;
/// # Ok(())
/// # }
/// ```
///
/// [`build`]: AlterTableTransactionBuilder::build
pub struct AlterTableTransactionBuilder<S = Ready> {
    /// The snapshot of the existing table to evolve.
    snapshot: SnapshotRef,
    /// Ordered list of schema operations to apply.
    operations: Vec<SchemaOperation>,
    _state: PhantomData<S>,
}

// --- Zero-cost state transition and shared build logic ---

impl<S> AlterTableTransactionBuilder<S> {
    /// Zero-cost state transition. Moves all data into a new builder with a different state.
    fn transition<T>(self) -> AlterTableTransactionBuilder<T> {
        AlterTableTransactionBuilder {
            snapshot: self.snapshot,
            operations: self.operations,
            _state: PhantomData,
        }
    }

    /// Core build logic shared by all buildable states.
    fn build_inner(
        self,
        _engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        let table_config = self.snapshot.table_configuration();

        // Validate the table supports writes
        table_config.ensure_operation_supported(Operation::Write)?;

        let schema = table_config.logical_schema();
        let column_mapping_mode = table_config.column_mapping_mode();
        let partition_columns: Vec<String> = table_config.partition_columns().to_vec();

        // Parse current max column ID from table properties
        let current_max_column_id: i64 = table_config
            .table_properties()
            .unknown_properties
            .get(COLUMN_MAPPING_MAX_COLUMN_ID)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        // Apply all schema operations
        let result = apply_schema_operations(
            &schema,
            &self.operations,
            column_mapping_mode,
            current_max_column_id,
            &partition_columns,
        )?;

        // Build evolved metadata
        let mut evolved_metadata = table_config.metadata().clone().with_schema(result.schema)?;

        // Update max column ID in configuration if it changed
        if let Some(new_max_id) = result.new_max_column_id {
            let mut config = table_config.metadata().configuration().clone();
            config.insert(
                COLUMN_MAPPING_MAX_COLUMN_ID.to_string(),
                new_max_id.to_string(),
            );
            evolved_metadata = evolved_metadata.with_configuration(config);
        }

        // Build effective table configuration with evolved metadata
        let effective_table_config = crate::table_configuration::TableConfiguration::try_new_from(
            table_config,
            Some(evolved_metadata),
            None, // no protocol change
            self.snapshot.version() + 1,
        )?;

        AlterTableTransaction::try_new_alter_table(self.snapshot, effective_table_config, committer)
    }
}

// --- Ready: all 4 operations available, build() NOT available ---

impl AlterTableTransactionBuilder<Ready> {
    /// Create a new builder for the given snapshot.
    pub(crate) fn new(snapshot: impl Into<SnapshotRef>) -> Self {
        Self {
            snapshot: snapshot.into(),
            operations: vec![],
            _state: PhantomData,
        }
    }

    /// Add a new top-level column to the table schema.
    ///
    /// The field must not already exist in the schema. If the table has existing data files,
    /// the field should be nullable -- existing files will read NULL for this column.
    ///
    /// If column mapping is enabled, the builder automatically assigns a new column ID and
    /// physical name. Otherwise the logical name is used as-is.
    ///
    /// # Arguments
    ///
    /// * `field` - The [`StructField`] to add (name, type, nullability, metadata)
    ///
    /// # Errors (at build time)
    ///
    /// - Column with the same name already exists at the top level
    /// - Field is non-nullable (no default value support)
    pub fn add_column(mut self, field: StructField) -> AlterTableTransactionBuilder<Modifying> {
        self.operations.push(SchemaOperation::AddColumn {
            parent: ColumnName::default(),
            field,
        });
        self.transition()
    }

    /// Drop a column from the table schema.
    ///
    /// Requires column mapping to be enabled (mode = name or id). The column is removed from
    /// the logical schema but physical data in existing Parquet files is untouched -- it simply
    /// becomes unreferenced.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the column to drop. Must match a field in the current schema.
    ///
    /// # Errors (at build time)
    ///
    /// - Column does not exist in the current schema
    /// - Column mapping is not enabled on the table
    /// - Column is a partition column
    pub fn drop_column(
        mut self,
        path: impl Into<ColumnName>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.operations
            .push(SchemaOperation::DropColumn { path: path.into() });
        self.transition()
    }

    /// Rename a column in the table schema.
    ///
    /// Requires column mapping to be enabled (mode = name or id). Only the logical name
    /// changes; the physical name and column ID remain the same, so existing data files
    /// are unaffected.
    ///
    /// This operation cannot be combined with other operations in the same transaction.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the column to rename
    /// * `new_name` - New name for the column (leaf name only)
    ///
    /// # Errors (at build time)
    ///
    /// - Column does not exist
    /// - Column mapping is not enabled
    /// - `new_name` conflicts with an existing column at the same level
    /// - Column is a partition column
    pub fn rename_column(
        mut self,
        path: impl Into<ColumnName>,
        new_name: impl Into<String>,
    ) -> AlterTableTransactionBuilder<Renaming> {
        self.operations.push(SchemaOperation::RenameColumn {
            path: path.into(),
            new_name: new_name.into(),
        });
        self.transition()
    }

    /// Widen a column's nullability from NOT NULL to nullable.
    ///
    /// Only the safe direction is allowed: NOT NULL -> nullable.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the column to make nullable
    ///
    /// # Errors (at build time)
    ///
    /// - Column does not exist
    pub fn set_nullable(
        mut self,
        path: impl Into<ColumnName>,
    ) -> AlterTableTransactionBuilder<Modifying> {
        self.operations
            .push(SchemaOperation::SetNullable { path: path.into() });
        self.transition()
    }
}

// --- Modifying: add/drop/set_nullable chainable, build() available ---

impl AlterTableTransactionBuilder<Modifying> {
    /// Add another column. See [`AlterTableTransactionBuilder::add_column`] on [`Ready`].
    pub fn add_column(mut self, field: StructField) -> Self {
        self.operations.push(SchemaOperation::AddColumn {
            parent: ColumnName::default(),
            field,
        });
        self
    }

    /// Drop another column. See [`AlterTableTransactionBuilder::drop_column`] on [`Ready`].
    pub fn drop_column(mut self, path: impl Into<ColumnName>) -> Self {
        self.operations
            .push(SchemaOperation::DropColumn { path: path.into() });
        self
    }

    /// Widen another column's nullability. See [`AlterTableTransactionBuilder::set_nullable`]
    /// on [`Ready`].
    pub fn set_nullable(mut self, path: impl Into<ColumnName>) -> Self {
        self.operations
            .push(SchemaOperation::SetNullable { path: path.into() });
        self
    }

    /// Builds an [`AlterTableTransaction`] that can be committed to evolve the schema.
    ///
    /// This method validates all buffered operations, applies them to the current schema,
    /// and constructs a metadata-only transaction.
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine instance for validation
    /// * `committer` - The committer to use for the transaction
    ///
    /// # Errors
    ///
    /// - Any individual operation fails validation (see per-method errors)
    /// - Table does not support writes (unsupported features)
    /// - `TableConfiguration` validation fails on the evolved state
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        self.build_inner(engine, committer)
    }
}

// --- Renaming: terminal state, only build() available ---

impl AlterTableTransactionBuilder<Renaming> {
    /// Builds an [`AlterTableTransaction`] that can be committed to evolve the schema.
    ///
    /// This method validates the rename operation and constructs a metadata-only transaction.
    ///
    /// # Arguments
    ///
    /// * `engine` - The engine instance for validation
    /// * `committer` - The committer to use for the transaction
    ///
    /// # Errors
    ///
    /// - The rename operation fails validation (see `rename_column` errors)
    /// - Table does not support writes (unsupported features)
    /// - `TableConfiguration` validation fails on the evolved state
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<AlterTableTransaction> {
        self.build_inner(engine, committer)
    }
}

/// Applies a sequence of schema operations to the given schema.
///
/// Each operation is validated against the current schema state (after prior operations have
/// been applied). Returns the final schema and any metadata changes needed.
///
/// # Arguments
///
/// * `schema` - The current table schema (logical schema from snapshot)
/// * `operations` - Ordered list of schema operations to apply
/// * `column_mapping_mode` - The table's column mapping mode
/// * `current_max_column_id` - Current `delta.columnMapping.maxColumnId` value
/// * `partition_columns` - Partition column names (some operations are restricted)
///
/// # Errors
///
/// Returns an error if any operation fails validation. The error message identifies which
/// operation failed and why.
pub(crate) fn apply_schema_operations(
    schema: &SchemaRef,
    operations: &[SchemaOperation],
    column_mapping_mode: ColumnMappingMode,
    current_max_column_id: i64,
    partition_columns: &[String],
) -> DeltaResult<SchemaEvolutionResult> {
    let mut evolving_schema: StructType = schema.as_ref().clone();
    let mut max_column_id = current_max_column_id;

    for op in operations {
        match op {
            SchemaOperation::AddColumn { parent: _, field } => {
                // TODO: Support nested columns via parent path. Currently top-level only.

                // Validate field doesn't already exist
                if evolving_schema.field(&field.name).is_some() {
                    return Err(Error::generic(format!(
                        "Cannot add column '{}': column already exists in the schema",
                        field.name
                    )));
                }

                // New columns must be nullable (existing data files will have NULL for this column)
                if !field.is_nullable() {
                    return Err(Error::generic(format!(
                        "Cannot add non-nullable column '{}': existing data files would have no \
                         value for this column. Use a nullable column instead.",
                        field.name
                    )));
                }

                let new_field = if column_mapping_mode != ColumnMappingMode::None {
                    assign_alter_column_mapping(field, &mut max_column_id)?
                } else {
                    field.clone()
                };

                // Append the new field at the end of the schema
                evolving_schema = evolving_schema.with_field_inserted_after(None, new_field)?;
            }
            SchemaOperation::DropColumn { path } => {
                // TODO: Support nested columns via path. Currently top-level only.
                let name = path
                    .path()
                    .last()
                    .ok_or_else(|| Error::generic("Cannot drop column: empty column path"))?;

                // Require column mapping for drop
                if column_mapping_mode == ColumnMappingMode::None {
                    return Err(Error::generic(format!(
                        "Cannot drop column '{}': column mapping must be enabled to drop columns",
                        name
                    )));
                }

                // Validate column exists
                if evolving_schema.field(name).is_none() {
                    return Err(Error::generic(format!(
                        "Cannot drop column '{}': column does not exist in the schema",
                        name
                    )));
                }

                // Partition columns cannot be dropped
                if partition_columns.iter().any(|p| p == name) {
                    return Err(Error::generic(format!(
                        "Cannot drop column '{}': partition columns cannot be dropped",
                        name
                    )));
                }

                evolving_schema = evolving_schema.with_field_removed(name);
            }
            SchemaOperation::RenameColumn { path, new_name } => {
                // TODO: Support nested columns via path. Currently top-level only.
                let old_name = path
                    .path()
                    .last()
                    .ok_or_else(|| Error::generic("Cannot rename column: empty column path"))?;

                // Require column mapping for rename
                if column_mapping_mode == ColumnMappingMode::None {
                    return Err(Error::generic(format!(
                        "Cannot rename column '{}': column mapping must be enabled to \
                         rename columns (current mode: {:?})",
                        old_name, column_mapping_mode
                    )));
                }

                // Partition columns cannot be renamed
                if partition_columns.iter().any(|p| p == old_name) {
                    return Err(Error::generic(format!(
                        "Cannot rename column '{}': partition columns cannot be renamed",
                        old_name
                    )));
                }

                // Validate old column exists and get it
                let old_field = evolving_schema.field(old_name).ok_or_else(|| {
                    Error::generic(format!(
                        "Cannot rename column '{}': column does not exist in the schema",
                        old_name
                    ))
                })?;

                // Validate new name doesn't conflict
                if evolving_schema.field(new_name).is_some() {
                    return Err(Error::generic(format!(
                        "Cannot rename column '{}' to '{}': a column with name '{}' already exists",
                        old_name, new_name, new_name
                    )));
                }

                let renamed_field = old_field.with_name(new_name);

                // Rebuild the schema to get the correct field name as IndexMap key.
                // with_field_replaced only updates the value but keeps the old key.
                let new_fields: Vec<StructField> = evolving_schema
                    .fields()
                    .map(|f| {
                        if f.name() == old_name {
                            renamed_field.clone()
                        } else {
                            f.clone()
                        }
                    })
                    .collect();
                evolving_schema = StructType::try_new(new_fields)?;
            }
            SchemaOperation::SetNullable { path } => {
                // TODO: Support nested columns via path. Currently top-level only.
                let name = path
                    .path()
                    .last()
                    .ok_or_else(|| Error::generic("Cannot set nullable: empty column path"))?;

                // Validate column exists
                let field = evolving_schema.field(name).ok_or_else(|| {
                    Error::generic(format!(
                        "Cannot set nullable on column '{}': column does not exist in the schema",
                        name
                    ))
                })?;

                // Only NOT NULL -> nullable is allowed; if already nullable, this is a no-op
                if field.is_nullable() {
                    continue;
                }

                let mut nullable_field = field.clone();
                nullable_field.nullable = true;
                evolving_schema = evolving_schema.with_field_replaced(name, nullable_field)?;
            }
        }
    }

    let new_max_column_id = (max_column_id != current_max_column_id).then_some(max_column_id);

    Ok(SchemaEvolutionResult {
        schema: Arc::new(evolving_schema),
        new_max_column_id,
    })
}

/// Assigns column mapping metadata (column ID and physical name) to a field for ALTER TABLE
/// ADD COLUMN. Unlike the CREATE TABLE variant, this does not reject pre-existing metadata
/// (it overwrites it).
fn assign_alter_column_mapping(field: &StructField, max_id: &mut i64) -> DeltaResult<StructField> {
    let mut new_field = field.clone();

    // Assign new column ID
    *max_id += 1;
    new_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
        MetadataValue::Number(*max_id),
    );

    // Assign physical name
    let physical_name = format!("col-{}", Uuid::new_v4());
    new_field.metadata.insert(
        ColumnMetadataKey::ColumnMappingPhysicalName
            .as_ref()
            .to_string(),
        MetadataValue::String(physical_name),
    );

    // Recursively process nested types
    new_field.data_type = process_nested_data_type_for_alter(&field.data_type, max_id)?;

    Ok(new_field)
}

/// Process nested data types to assign column mapping metadata to any nested struct fields
/// during ALTER TABLE operations.
fn process_nested_data_type_for_alter(
    data_type: &crate::schema::DataType,
    max_id: &mut i64,
) -> DeltaResult<crate::schema::DataType> {
    use crate::schema::{ArrayType, DataType, MapType};
    match data_type {
        DataType::Struct(inner) => {
            let new_fields: Vec<StructField> = inner
                .fields()
                .map(|field| assign_alter_column_mapping(field, max_id))
                .collect::<DeltaResult<_>>()?;
            let new_inner = StructType::try_new(new_fields)?;
            Ok(DataType::Struct(Box::new(new_inner)))
        }
        DataType::Array(array_type) => {
            let new_element_type =
                process_nested_data_type_for_alter(array_type.element_type(), max_id)?;
            Ok(DataType::Array(Box::new(ArrayType::new(
                new_element_type,
                array_type.contains_null(),
            ))))
        }
        DataType::Map(map_type) => {
            let new_key_type = process_nested_data_type_for_alter(map_type.key_type(), max_id)?;
            let new_value_type = process_nested_data_type_for_alter(map_type.value_type(), max_id)?;
            Ok(DataType::Map(Box::new(MapType::new(
                new_key_type,
                new_value_type,
                map_type.value_contains_null(),
            ))))
        }
        // Primitive and Variant types don't contain nested struct fields
        DataType::Primitive(_) | DataType::Variant(_) => Ok(data_type.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::ColumnMappingMode;

    use super::*;

    // Helper to create a SchemaRef for tests
    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ]))
    }

    #[test]
    fn add_column_no_column_mapping() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::default(),
            field: StructField::nullable("email", DataType::STRING),
        }];

        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect("add_column should succeed");

        assert_eq!(result.schema.num_fields(), 4);
        let email_field = result
            .schema
            .field("email")
            .expect("email field should exist");
        assert_eq!(email_field.data_type(), &DataType::STRING);
        assert!(email_field.is_nullable());
        assert!(result.new_max_column_id.is_none());
    }

    #[test]
    fn add_column_with_column_mapping() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::default(),
            field: StructField::nullable("email", DataType::STRING),
        }];

        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::Name, 3, &[])
            .expect("add_column should succeed");

        assert_eq!(result.schema.num_fields(), 4);
        let email_field = result
            .schema
            .field("email")
            .expect("email field should exist");

        // Check column mapping metadata was assigned
        let col_id = email_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingId)
            .expect("should have column ID");
        assert_eq!(col_id, &MetadataValue::Number(4));

        let phys_name = email_field
            .get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName)
            .expect("should have physical name");
        assert!(matches!(phys_name, MetadataValue::String(s) if s.starts_with("col-")));

        assert_eq!(result.new_max_column_id, Some(4));
    }

    #[test]
    fn add_duplicate_column_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::default(),
            field: StructField::nullable("name", DataType::STRING),
        }];

        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect_err("duplicate column should fail");
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn add_non_nullable_column_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::AddColumn {
            parent: ColumnName::default(),
            field: StructField::new("email", DataType::STRING, false),
        }];

        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect_err("non-nullable should fail");
        assert!(err.to_string().contains("non-nullable"));
    }

    #[test]
    fn drop_column_with_column_mapping() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: ColumnName::new(["age"]),
        }];
        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::Name, 3, &[])
            .expect("drop_column should succeed");
        assert_eq!(result.schema.num_fields(), 2);
        assert!(result.schema.field("age").is_none());
    }

    #[test]
    fn drop_column_without_column_mapping_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: ColumnName::new(["age"]),
        }];
        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect_err("drop without column mapping should fail");
        assert!(err.to_string().contains("column mapping must be enabled"));
    }

    #[test]
    fn drop_nonexistent_column_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: ColumnName::new(["nonexistent"]),
        }];
        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::Name, 3, &[])
            .expect_err("drop nonexistent should fail");
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn drop_partition_column_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::DropColumn {
            path: ColumnName::new(["name"]),
        }];
        let err = apply_schema_operations(
            &schema,
            &ops,
            ColumnMappingMode::Name,
            3,
            &["name".to_string()],
        )
        .expect_err("drop partition column should fail");
        assert!(err
            .to_string()
            .contains("partition columns cannot be dropped"));
    }

    #[test]
    fn rename_column_with_name_mode() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: ColumnName::new(["name"]),
            new_name: "full_name".to_string(),
        }];
        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::Name, 3, &[])
            .expect("rename should succeed");
        assert_eq!(result.schema.num_fields(), 3);
        assert!(result.schema.field("name").is_none());
        let renamed = result
            .schema
            .field("full_name")
            .expect("renamed field should exist");
        assert_eq!(renamed.data_type(), &DataType::STRING);
        assert!(renamed.is_nullable());
    }

    #[test]
    fn rename_column_with_id_mode_succeeds() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: ColumnName::new(["name"]),
            new_name: "full_name".to_string(),
        }];
        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::Id, 3, &[])
            .expect("rename with Id mode should succeed");
        assert!(result.schema.field("full_name").is_some());
        assert!(result.schema.field("name").is_none());
    }

    #[test]
    fn rename_column_without_column_mapping_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: ColumnName::new(["name"]),
            new_name: "full_name".to_string(),
        }];
        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect_err("rename without column mapping should fail");
        assert!(err.to_string().contains("column mapping must be enabled"));
    }

    #[test]
    fn rename_to_existing_name_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: ColumnName::new(["name"]),
            new_name: "age".to_string(),
        }];
        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::Name, 3, &[])
            .expect_err("rename to existing name should fail");
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn rename_partition_column_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::RenameColumn {
            path: ColumnName::new(["name"]),
            new_name: "full_name".to_string(),
        }];
        let err = apply_schema_operations(
            &schema,
            &ops,
            ColumnMappingMode::Name,
            3,
            &["name".to_string()],
        )
        .expect_err("rename partition column should fail");
        assert!(err
            .to_string()
            .contains("partition columns cannot be renamed"));
    }

    #[test]
    fn set_nullable_on_not_null_column() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::SetNullable {
            path: ColumnName::new(["id"]),
        }];
        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect("set_nullable should succeed");
        let id_field = result.schema.field("id").expect("id should exist");
        assert!(id_field.is_nullable());
    }

    #[test]
    fn set_nullable_on_already_nullable_is_noop() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::SetNullable {
            path: ColumnName::new(["name"]),
        }];
        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect("set_nullable on nullable should succeed (noop)");
        let name_field = result.schema.field("name").expect("name should exist");
        assert!(name_field.is_nullable());
    }

    #[test]
    fn set_nullable_nonexistent_column_fails() {
        let schema = test_schema();
        let ops = vec![SchemaOperation::SetNullable {
            path: ColumnName::new(["nonexistent"]),
        }];
        let err = apply_schema_operations(&schema, &ops, ColumnMappingMode::None, 0, &[])
            .expect_err("set_nullable on nonexistent should fail");
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn chained_add_drop_set_nullable() {
        let schema = test_schema();
        let ops = vec![
            SchemaOperation::AddColumn {
                parent: ColumnName::default(),
                field: StructField::nullable("email", DataType::STRING),
            },
            SchemaOperation::DropColumn {
                path: ColumnName::new(["age"]),
            },
            SchemaOperation::SetNullable {
                path: ColumnName::new(["id"]),
            },
        ];
        let result = apply_schema_operations(&schema, &ops, ColumnMappingMode::Name, 3, &[])
            .expect("chained add/drop/set_nullable should succeed");
        assert_eq!(result.schema.num_fields(), 3); // id, name, email
        assert!(result.schema.field("id").is_some());
        assert!(result.schema.field("name").is_some());
        assert!(result.schema.field("email").is_some());
        assert!(result.schema.field("age").is_none());
        assert!(result.schema.field("id").unwrap().is_nullable());
    }

    #[test]
    fn empty_operations_succeeds_at_schema_level() {
        let schema = test_schema();
        let result = apply_schema_operations(&schema, &[], ColumnMappingMode::None, 0, &[])
            .expect("empty ops should succeed at schema level");
        assert_eq!(result.schema.num_fields(), 3);
    }
}
