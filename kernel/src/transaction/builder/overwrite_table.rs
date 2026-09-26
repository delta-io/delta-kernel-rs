use std::borrow::Cow;
use std::sync::Arc;

use super::create_table::validate_partition_columns;
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::void_utils::validate_schema_for_write;
use crate::schema::{
    schema_contains_non_null_fields, ColumnMetadataKey, DataType, SchemaRef, StructField,
    StructType,
};
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    assign_column_mapping_metadata, find_max_column_id_in_schema,
    strip_stray_column_mapping_metadata, validate_column_mapping_id, ColumnMappingMode,
    TableFeature,
};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::OverwriteTableTransaction;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Engine, Error};

/// Builds a full-table overwrite from a fixed source snapshot and replacement schema.
///
/// The table identity, properties, protocol, and column-mapping mode are preserved. Fields at
/// the same logical path retain their mapping IDs and physical names, even when their types
/// change. New fields receive fresh identities; renames and moves are not inferred.
#[derive(Debug)]
pub struct OverwriteTableTransactionBuilder {
    snapshot: SnapshotRef,
    schema: SchemaRef,
    partition_columns: Vec<String>,
}

impl OverwriteTableTransactionBuilder {
    pub(crate) fn new(
        snapshot: SnapshotRef,
        schema: SchemaRef,
        partition_columns: Vec<String>,
    ) -> Self {
        Self {
            snapshot,
            schema,
            partition_columns,
        }
    }

    /// Validates the replacement and stages removals for every active file in the source snapshot.
    ///
    /// Uses `engine` to read file metadata, without reading the old Parquet data. All removal
    /// metadata is retained in memory. `committer` must support atomic metadata and data updates.
    /// Returns a transaction whose write state describes the replacement schema and partitioning.
    ///
    /// # Errors
    ///
    /// Rejects invalid or empty schemas, invalid partition columns, conflicting mapping metadata,
    /// and schema features absent from the existing protocol. CDF and append-only must be disabled.
    /// Clustering, Iceberg compatibility, column defaults, and adaptive metadata are unsupported.
    /// Both the source and replacement must pass Kernel's write-support checks, including rejection
    /// of SQL-expression invariants. NOT NULL constraints remain the writer's responsibility.
    /// Log-read errors are propagated; no commit is attempted by this method.
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> DeltaResult<OverwriteTableTransaction> {
        let old_config = self.snapshot.table_configuration();
        old_config.ensure_read_write_supported()?;
        for feature in [
            TableFeature::ChangeDataFeed,
            TableFeature::AppendOnly,
            TableFeature::ClusteredTable,
            TableFeature::IcebergCompatV1,
            TableFeature::IcebergCompatV2,
            TableFeature::IcebergCompatV3,
            TableFeature::AllowColumnDefaults,
            TableFeature::AdaptiveMetadataPreview,
        ] {
            require!(
                !old_config.is_feature_enabled(&feature),
                Error::unsupported(format!(
                    "Full-table overwrite is not supported with {feature} enabled"
                ))
            );
        }
        let config = replacement_config(old_config, &self.schema, self.partition_columns)?;
        let scan = self
            .snapshot
            .clone()
            .scan_builder()
            .without_row_transforms()
            .build()?;
        let removals = scan
            .scan_metadata(engine)?
            .map(|batch| batch.map(|metadata| metadata.scan_files))
            .collect::<DeltaResult<Vec<_>>>()?;
        OverwriteTableTransaction::try_new_overwrite(self.snapshot, config, removals, committer)
    }
}

fn replacement_config(
    old_config: &TableConfiguration,
    schema: &SchemaRef,
    partition_columns: Vec<String>,
) -> DeltaResult<TableConfiguration> {
    let mode = old_config.column_mapping_mode();
    require!(
        schema.num_fields() > 0,
        Error::schema("Full-table overwrite requires a non-empty schema")
    );
    StructType::ensure_no_metadata_columns(&mut schema.fields())?;
    validate_schema(schema, mode)?;
    validate_schema_for_write(schema)?;
    require!(
        !schema_contains_non_null_fields(schema)
            || old_config.is_feature_supported(&TableFeature::Invariants),
        Error::unsupported("Non-null fields require existing invariants protocol support")
    );
    if !partition_columns.is_empty() {
        let columns = partition_columns
            .iter()
            .map(|s| ColumnName::new([s.clone()]))
            .collect::<Vec<_>>();
        validate_partition_columns(schema, &columns)?;
    }

    let mut metadata = old_config.metadata().clone();
    let old_type = DataType::from(old_config.logical_schema().as_ref().clone());
    let schema = TransferMappings {
        previous: Some(&old_type),
        mapping_enabled: mode != ColumnMappingMode::None,
    }
    .transform_struct(schema)?
    .into_owned();
    let schema = if mode == ColumnMappingMode::None {
        strip_stray_column_mapping_metadata(false, &schema).unwrap_or(schema)
    } else {
        let stored_max = old_config
            .table_properties()
            .column_mapping_max_column_id
            .unwrap_or(0);
        validate_column_mapping_id(stored_max)?;
        let mut max_id =
            stored_max.max(find_max_column_id_in_schema(&old_config.logical_schema()).unwrap_or(0));
        let mapped = assign_column_mapping_metadata(&schema, &mut max_id, false)?;
        metadata =
            metadata.with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, max_id.to_string());
        mapped
    };
    let schema = Arc::new(schema);
    let metadata = metadata
        .with_schema(schema.clone())?
        .with_partition_columns(partition_columns);
    let config = TableConfiguration::try_new_with_schema(old_config, metadata, schema)?;
    config.ensure_read_write_supported()?;
    let properties = config.table_properties();
    for reserved_name in [
        properties.materialized_row_id_column_name.as_deref(),
        properties
            .materialized_row_commit_version_column_name
            .as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        require!(
            config.physical_schema().field(reserved_name).is_none(),
            Error::schema(format!(
                "Overwrite column conflicts with reserved row-tracking column '{reserved_name}'"
            ))
        );
    }
    Ok(config)
}

struct TransferMappings<'s> {
    previous: Option<&'s DataType>,
    mapping_enabled: bool,
}

impl<'a> SchemaTransform<'a> for TransferMappings<'_> {
    transform_output_type!(|'a, T| DeltaResult<Cow<'a, T>>);

    fn transform_struct(&mut self, schema: &'a StructType) -> DeltaResult<Cow<'a, StructType>> {
        let old_struct = match self.previous {
            Some(DataType::Struct(schema)) => Some(schema.as_ref()),
            _ => None,
        };
        let fields = schema
            .fields()
            .map(|field| {
                let previous = old_struct.and_then(|schema| schema.field(field.name()));
                for key in field.metadata.keys() {
                    require!(
                        key != ColumnMetadataKey::GenerationExpression.as_ref()
                            && !key.starts_with("delta.identity.")
                            && key != "CURRENT_DEFAULT"
                            && key != "EXISTS_DEFAULT",
                        Error::unsupported(format!(
                            "Full-table overwrite does not support column metadata '{key}'"
                        ))
                    );
                }
                let mut result = field.clone();
                if self.mapping_enabled {
                    for key in [
                        ColumnMetadataKey::ColumnMappingId,
                        ColumnMetadataKey::ColumnMappingPhysicalName,
                    ] {
                        let existing = previous.and_then(|old| old.get_config_value(&key));
                        if let Some(supplied) = field.get_config_value(&key) {
                            require!(
                                Some(supplied) == existing,
                                Error::schema(format!(
                                    "Conflicting {} for overwrite column '{}'",
                                    key.as_ref(),
                                    field.name()
                                ))
                            );
                        }
                        if let Some(value) = existing {
                            result
                                .metadata
                                .insert(key.as_ref().to_string(), value.clone());
                        }
                    }
                }
                result.data_type = Self {
                    previous: previous.map(StructField::data_type),
                    mapping_enabled: self.mapping_enabled,
                }
                .transform(field.data_type())?
                .into_owned();
                Ok(result)
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        Ok(Cow::Owned(StructType::try_new(fields)?))
    }

    fn transform_array_element(&mut self, element: &'a DataType) -> DeltaResult<Cow<'a, DataType>> {
        let previous = match self.previous {
            Some(DataType::Array(a)) => Some(a.element_type()),
            _ => None,
        };
        Self {
            previous,
            mapping_enabled: self.mapping_enabled,
        }
        .transform(element)
    }

    fn transform_map_key(&mut self, key: &'a DataType) -> DeltaResult<Cow<'a, DataType>> {
        let previous = match self.previous {
            Some(DataType::Map(m)) => Some(m.key_type()),
            _ => None,
        };
        Self {
            previous,
            mapping_enabled: self.mapping_enabled,
        }
        .transform(key)
    }

    fn transform_map_value(&mut self, value: &'a DataType) -> DeltaResult<Cow<'a, DataType>> {
        let previous = match self.previous {
            Some(DataType::Map(m)) => Some(m.value_type()),
            _ => None,
        };
        Self {
            previous,
            mapping_enabled: self.mapping_enabled,
        }
        .transform(value)
    }

    fn transform_variant(&mut self, variant: &'a StructType) -> DeltaResult<Cow<'a, StructType>> {
        Ok(Cow::Borrowed(variant))
    }
}
