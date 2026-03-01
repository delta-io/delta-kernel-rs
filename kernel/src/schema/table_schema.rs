//! [`TableSchema`] bundles a logical schema, column mapping mode, partition columns,
//! and materialized row ID column together under a single named type.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{
    ArrayType, DataType, MapType, MetadataColumnSpec, PrimitiveType, SchemaRef, SchemaTransform,
    StructField, StructType,
};
use crate::expressions::{ColumnName, Scalar};
use crate::scan::field_classifiers::TransformFieldClassifier;
use crate::table_configuration::TableConfiguration;
use crate::table_features::ColumnMappingMode;
use crate::transforms::{parse_partition_value_raw, FieldTransformSpec, TransformSpec};
use crate::{DeltaResult, Error};

// Private helper for predicate-driven schema traversal.
// Walks the logical schema to find referenced leaf columns, tracking
// logical→physical name mappings and filtering out non-skippable types.
struct GetReferencedFields<'a> {
    unresolved_references: HashSet<&'a ColumnName>,
    column_mappings: HashMap<ColumnName, ColumnName>,
    logical_path: Vec<String>,
    physical_path: Vec<String>,
    column_mapping_mode: ColumnMappingMode,
}
impl<'a> SchemaTransform<'a> for GetReferencedFields<'a> {
    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        self.unresolved_references
            .remove(self.logical_path.as_slice())
            .then(|| {
                self.column_mappings.insert(
                    ColumnName::new(&self.logical_path),
                    ColumnName::new(&self.physical_path),
                );
                Cow::Borrowed(ptype)
            })
    }

    // Arrays and maps are not eligible for data skipping; filter them out.
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }

    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        let physical_name = field.physical_name(self.column_mapping_mode);
        self.logical_path.push(field.name.clone());
        self.physical_path.push(physical_name.to_string());
        let field = self.recurse_into_struct_field(field);
        self.logical_path.pop();
        self.physical_path.pop();
        Some(Cow::Owned(field?.with_name(physical_name)))
    }
}

/// Bundles a logical schema, column mapping mode, partition columns,
/// and materialized row ID column for read and write schema computation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TableSchema {
    schema: SchemaRef,
    column_mapping_mode: ColumnMappingMode,
    partition_columns: Vec<String>,
    materialized_row_id_col: Option<String>,
}

impl TableSchema {
    /// Create a new [`TableSchema`] from a logical schema and table configuration.
    pub(crate) fn new(schema: SchemaRef, table_config: &TableConfiguration) -> Self {
        let column_mapping_mode = table_config.column_mapping_mode();
        let partition_columns = table_config.partition_columns().to_vec();
        let materialized_row_id_col = table_config
            .table_properties()
            .enable_row_tracking
            .filter(|&b| b)
            .and_then(|_| {
                table_config
                    .table_properties()
                    .materialized_row_id_column_name
                    .clone()
            });
        Self {
            schema,
            column_mapping_mode,
            partition_columns,
            materialized_row_id_col,
        }
    }

    /// Create a new [`TableSchemaRef`] for testing, without table configuration.
    #[cfg(test)]
    pub(crate) fn new_for_test(
        schema: SchemaRef,
        column_mapping_mode: ColumnMappingMode,
    ) -> Arc<Self> {
        Arc::new(Self {
            schema,
            column_mapping_mode,
            partition_columns: vec![],
            materialized_row_id_col: None,
        })
    }

    /// Returns the logical schema.
    pub(crate) fn logical_schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the column mapping mode. Exposed for tests only.
    ///
    /// Production code should never need the raw mode — all logical-to-physical name translation
    /// should go through `TableSchema` methods (e.g. [`Self::logical_to_physical_name`],
    /// [`Self::compute_read_schema_and_transform`]) so that callers remain insulated from
    /// the column-mapping details.
    #[cfg(test)]
    pub(crate) fn column_mapping_mode(&self) -> ColumnMappingMode {
        self.column_mapping_mode
    }

    /// Returns the physical name for the given logical column name, or `None` if not found.
    pub(crate) fn logical_to_physical_name<'a>(&'a self, logical_name: &str) -> Option<&'a str> {
        self.schema
            .field(logical_name)
            .map(|f| f.physical_name(self.column_mapping_mode))
    }

    /// Compute the physical read schema and transform spec for this table schema.
    ///
    /// The `classifier` allows different scan types (regular, CDF) to customize field handling.
    pub(crate) fn compute_read_schema_and_transform<C: TransformFieldClassifier>(
        &self,
        classifier: &C,
    ) -> DeltaResult<(SchemaRef, Option<Arc<TransformSpec>>)> {
        // Pre-pass: collect metadata column info and validate
        let mut metadata_field_names: HashSet<String> = HashSet::new();
        let mut selected_row_index_col_name: Option<&String> = None;
        for metadata_column in self.schema.metadata_columns() {
            if self.partition_columns.contains(metadata_column.name()) {
                return Err(Error::Schema(format!(
                    "Metadata column names must not match partition columns: {}",
                    metadata_column.name()
                )));
            }
            if matches!(
                metadata_column.get_metadata_column_spec(),
                Some(MetadataColumnSpec::RowIndex)
            ) {
                selected_row_index_col_name = Some(metadata_column.name());
            }
            metadata_field_names.insert(metadata_column.name().clone());
        }

        // Main loop: build physical schema and transform spec
        let mut read_fields = Vec::with_capacity(self.schema.num_fields());
        let mut transform_spec = Vec::with_capacity(self.schema.num_fields());
        let mut last_physical_field: Option<String> = None;

        for (index, logical_field) in self.schema.fields().enumerate() {
            if let Some(spec) =
                classifier.classify_field(logical_field, index, &last_physical_field)
            {
                transform_spec.push(spec);
            } else if self.partition_columns.contains(logical_field.name()) {
                transform_spec.push(FieldTransformSpec::MetadataDerivedColumn {
                    field_index: index,
                    insert_after: last_physical_field.clone(),
                });
            } else {
                match logical_field.get_metadata_column_spec() {
                    Some(MetadataColumnSpec::RowId) => {
                        let index_column_name = match selected_row_index_col_name {
                            Some(index_column_name) => index_column_name.to_string(),
                            None => {
                                let index_column_name = (0..)
                                    .map(|i| format!("row_indexes_for_row_id_{}", i))
                                    .find(|name| self.schema.field(name).is_none())
                                    .ok_or(Error::generic(
                                        "Couldn't generate row index column name",
                                    ))?;
                                read_fields.push(StructField::create_metadata_column(
                                    &index_column_name,
                                    MetadataColumnSpec::RowIndex,
                                ));
                                transform_spec.push(FieldTransformSpec::StaticDrop {
                                    field_name: index_column_name.clone(),
                                });
                                index_column_name
                            }
                        };
                        let Some(row_id_col_name) = &self.materialized_row_id_col else {
                            return Err(Error::unsupported(
                                "Row IDs require row tracking to be enabled with a configured materialized column name",
                            ));
                        };
                        read_fields.push(StructField::nullable(row_id_col_name, DataType::LONG));
                        transform_spec.push(FieldTransformSpec::GenerateRowId {
                            field_name: row_id_col_name.to_string(),
                            row_index_field_name: index_column_name,
                        });
                    }
                    Some(MetadataColumnSpec::RowCommitVersion) => {
                        return Err(Error::unsupported("Row commit versions not supported"));
                    }
                    Some(MetadataColumnSpec::RowIndex)
                    | Some(MetadataColumnSpec::FilePath)
                    | None => {
                        let physical_field = logical_field.make_physical(self.column_mapping_mode);
                        debug!("\n\n{logical_field:#?}\nAfter mapping: {physical_field:#?}\n\n");
                        let physical_name = physical_field.name.clone();
                        if !logical_field.is_metadata_column()
                            && metadata_field_names.contains(&physical_name)
                        {
                            return Err(Error::Schema(format!(
                                "Metadata column names must not match physical columns, but logical column '{}' has physical name '{}'",
                                logical_field.name(), physical_name,
                            )));
                        }
                        last_physical_field = Some(physical_name);
                        read_fields.push(physical_field);
                    }
                }
            }
        }

        let physical_schema = Arc::new(StructType::try_new(read_fields)?);
        let transform_spec =
            if !transform_spec.is_empty() || self.column_mapping_mode != ColumnMappingMode::None {
                Some(Arc::new(transform_spec))
            } else {
                None
            };
        Ok((physical_schema, transform_spec))
    }

    /// Build a filtered physical schema and column name mappings for a set of column references.
    ///
    /// Returns `None` if none of the referenced columns are eligible for data skipping.
    /// Returns `Err` if any referenced column is not present in the schema.
    pub(crate) fn get_referenced_physical_schema(
        &self,
        references: HashSet<&ColumnName>,
    ) -> DeltaResult<Option<(StructType, HashMap<ColumnName, ColumnName>)>> {
        let mut visitor = GetReferencedFields {
            unresolved_references: references,
            column_mappings: HashMap::new(),
            logical_path: vec![],
            physical_path: vec![],
            column_mapping_mode: self.column_mapping_mode,
        };
        let schema_opt = visitor.transform_struct(&self.schema);
        let mut unresolved = visitor.unresolved_references.into_iter();
        if let Some(unresolved) = unresolved.next() {
            return Err(Error::missing_column(format!(
                "Predicate references unknown column: {unresolved}"
            )));
        }
        Ok(schema_opt.map(|s| (s.into_owned(), visitor.column_mappings)))
    }

    /// Parse a single partition value for the field at `field_idx` from the raw string map.
    pub(crate) fn parse_partition_value(
        &self,
        field_idx: usize,
        partition_values: &HashMap<String, String>,
    ) -> DeltaResult<(usize, (String, Scalar))> {
        let Some(field) = self.schema.field_at_index(field_idx) else {
            return Err(Error::InternalError(format!(
                "out of bounds partition column field index {field_idx}"
            )));
        };
        let name = field.physical_name(self.column_mapping_mode);
        let partition_value =
            parse_partition_value_raw(partition_values.get(name), field.data_type())?;
        Ok((field_idx, (name.to_string(), partition_value)))
    }

    /// Compute the physical write schema for this table schema.
    ///
    /// `include_partition_cols`: if false, partition columns are excluded from the result
    /// (they are stored in the file path, not in the data).
    pub(crate) fn compute_write_schema(&self, include_partition_cols: bool) -> SchemaRef {
        let fields = self
            .schema
            .fields()
            .filter(|f| {
                include_partition_cols || !self.partition_columns.contains(&f.name().to_string())
            })
            .map(|f| f.make_physical(self.column_mapping_mode));
        Arc::new(StructType::new_unchecked(fields))
    }
}
