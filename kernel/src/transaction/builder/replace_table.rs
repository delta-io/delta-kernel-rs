use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use super::create_table::validate_partition_columns;
use super::TransactionBuilderState;
use crate::committer::Committer;
use crate::expressions::ColumnName;
use crate::schema::validation::validate_schema;
use crate::schema::void_utils::validate_schema_for_write;
use crate::schema::{
    normalize_column_names_to_schema_casing, schema_contains_non_null_fields, ColumnMetadataKey,
    DataType, SchemaRef, StructField, StructType,
};
use crate::snapshot::SnapshotRef;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{
    assign_column_mapping_metadata, find_max_column_id_in_schema,
    strip_stray_column_mapping_metadata, validate_column_mapping_id, ColumnMappingMode,
    TableFeature,
};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::Transaction;
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{DeltaResult, Engine, EngineData, KernelError};

/// Builds a full-table replacement from a fixed source snapshot and replacement schema.
///
/// The table identity, properties, protocol, and column-mapping mode are preserved. Fields at
/// the same logical path retain their mapping IDs and physical names, even when their types
/// change. New fields receive fresh identities; renames and moves are not inferred.
#[derive(Debug)]
pub struct ReplaceTableTransactionBuilder {
    snapshot: SnapshotRef,
    schema: SchemaRef,
    data_layout: Option<DataLayout>,
    state: TransactionBuilderState,
    transaction_ids: Vec<(String, i64)>,
}

impl ReplaceTableTransactionBuilder {
    pub(crate) fn new(snapshot: SnapshotRef, schema: SchemaRef) -> Self {
        Self {
            snapshot,
            schema,
            data_layout: None,
            state: TransactionBuilderState::new(),
            transaction_ids: Vec::new(),
        }
    }

    /// Sets the replacement partition layout. Omission preserves the existing layout.
    pub fn with_data_layout(mut self, data_layout: DataLayout) -> Self {
        self.data_layout = Some(data_layout);
        self
    }

    /// Records the connector name and version in commit info.
    pub fn with_engine_info(mut self, engine_info: impl Into<String>) -> Self {
        self.state = self.state.with_engine_info(engine_info);
        self
    }

    /// Adds an application transaction identifier to the replacement commit.
    pub fn with_transaction_id(mut self, app_id: impl Into<String>, version: i64) -> Self {
        self.transaction_ids.push((app_id.into(), version));
        self
    }

    /// Adds user-controlled domain metadata to the replacement commit.
    pub fn with_domain_metadata(
        mut self,
        domain: impl Into<String>,
        configuration: impl Into<String>,
    ) -> Self {
        self.state = self.state.with_domain_metadata(domain, configuration);
        self
    }

    /// Attaches an opaque identifier to this transaction's metric events.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<Arc<str>>) -> Self {
        self.state = self.state.with_correlation_id(correlation_id);
        self
    }

    /// Replaces the operation parameters recorded in commit info.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate or empty keys.
    pub fn with_operation_parameters<I, K, V>(mut self, parameters: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state = self.state.with_operation_parameters(parameters)?;
        Ok(self)
    }

    /// Replaces the operation metrics recorded in commit info.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate or empty keys.
    pub fn with_operation_metrics<I, K, V>(mut self, metrics: I) -> DeltaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.state = self.state.with_operation_metrics(metrics)?;
        Ok(self)
    }

    /// Supplies connector-defined commit info.
    pub fn with_commit_info(
        mut self,
        commit_info: Box<dyn EngineData>,
        commit_info_schema: SchemaRef,
    ) -> Self {
        self.state = self.state.with_commit_info(commit_info, commit_info_schema);
        self
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
    ) -> DeltaResult<Transaction> {
        let mut ids = HashSet::new();
        if let Some((app_id, _)) = self
            .transaction_ids
            .iter()
            .find(|(app_id, _)| !ids.insert(app_id.as_str()))
        {
            return Err(KernelError::invalid_transaction_state(format!(
                "app_id {app_id} appears more than once"
            )));
        }
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
                KernelError::unsupported(format!(
                    "REPLACE TABLE is not supported with {feature} enabled"
                ))
            );
        }
        let partition_columns = match self.data_layout {
            None => normalize_column_names_to_schema_casing(
                &self.schema,
                &old_config
                    .logical_partition_columns()
                    .iter()
                    .cloned()
                    .map(|column| ColumnName::new([column]))
                    .collect::<Vec<_>>(),
            )
            .into_iter()
            .map(|column| column.into_inner().remove(0))
            .collect(),
            Some(DataLayout::None) => Vec::new(),
            Some(DataLayout::Partitioned { columns }) => {
                let columns = normalize_column_names_to_schema_casing(&self.schema, &columns);
                validate_partition_columns(&self.schema, &columns)?;
                columns
                    .into_iter()
                    .map(|column| column.into_inner().remove(0))
                    .collect()
            }
            Some(DataLayout::Clustered { .. }) => {
                return Err(KernelError::unsupported(
                    "REPLACE TABLE does not support clustered layout",
                ));
            }
        };
        let config = replacement_config(old_config, &self.schema, partition_columns)?;
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
        let mut state = self.state;
        state.data_change = Some(true);
        let transaction = Transaction::try_new_existing_table(self.snapshot, committer, engine)?
            .with_builder_state(state)?
            .with_transaction_ids(self.transaction_ids)
            .with_table_replacement(config, removals);
        transaction.validate_domain_metadata_operations()?;
        Ok(transaction)
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
        KernelError::schema("REPLACE TABLE requires a non-empty schema")
    );
    StructType::ensure_no_metadata_columns(&mut schema.fields())?;
    validate_schema(schema, mode, false)?;
    validate_schema_for_write(schema)?;
    require!(
        !schema_contains_non_null_fields(schema)
            || old_config.is_feature_supported(&TableFeature::Invariants),
        KernelError::unsupported("Non-null fields require existing invariants protocol support")
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
            KernelError::schema(format!(
                "Replacement column conflicts with reserved row-tracking column '{reserved_name}'"
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
                let previous = old_struct.and_then(|schema| {
                    schema
                        .fields()
                        .find(|old| old.name().eq_ignore_ascii_case(field.name()))
                });
                for key in field.metadata.keys() {
                    require!(
                        key != ColumnMetadataKey::GenerationExpression.as_ref()
                            && !key.starts_with("delta.identity.")
                            && key != "CURRENT_DEFAULT"
                            && key != "EXISTS_DEFAULT",
                        KernelError::unsupported(format!(
                            "REPLACE TABLE does not support column metadata '{key}'"
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
                                KernelError::schema(format!(
                                    "Conflicting {} for replacement column '{}'",
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
