use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel_derive::internal_api;

use super::create_table::validate_partition_columns;
use super::state::TransactionBuilderState;
use super::TransactionBuilder;
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
use crate::transaction::{ReplaceTable, Transaction, UpdateTableOperation};
use crate::transforms::{transform_output_type, SchemaTransform};
use crate::utils::require;
use crate::{Engine, FilteredEngineData, KernelError, KernelResult};

/// Metadata changes to apply while replacing all data files.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct MetadataPatch {
    schema: Option<SchemaRef>,
}

impl MetadataPatch {
    /// Replaces the schema while retaining unspecified table metadata.
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Selects how a full-table replacement constructs its target metadata.
#[derive(Debug)]
#[non_exhaustive]
pub enum MetadataPolicy {
    /// Retains the existing table identity, properties, protocol, and column-mapping mode while
    /// applying the specified changes. An empty patch leaves metadata unchanged. Retained fields
    /// keep their mapping IDs and physical names even for incompatible type changes, unlike Spark
    /// overwrite. Historical files may therefore have different physical types under the same
    /// path; readers must use the schema of each table version. New fields get unique paths.
    Patch(MetadataPatch),
}

impl MetadataPolicy {
    /// Patches the schema while preserving other metadata and replacing all data files.
    pub fn patch_schema(schema: SchemaRef) -> Self {
        Self::Patch(MetadataPatch::default().with_schema(schema))
    }
}

/// Builds a full-table replacement from a fixed source snapshot and metadata policy.
///
/// All replacement modes remove the active data files. The supported metadata policy is
/// [`MetadataPolicy::Patch`]; retained mapping identities do not imply type compatibility across
/// table versions. New fields receive fresh identities; renames and moves are not inferred.
///
/// cbindgen:ignore
pub type ReplaceTableTransactionBuilder = TransactionBuilder<ReplaceTable>;

/// Opaque state collected by replacement-specific builder setters.
#[doc(hidden)]
pub struct ReplaceTableBuilderState {
    snapshot: SnapshotRef,
    metadata_policy: MetadataPolicy,
    data_layout: Option<DataLayout>,
}

impl std::fmt::Debug for TransactionBuilder<ReplaceTable> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaceTableTransactionBuilder")
            .field("snapshot_version", &self.local_state.snapshot.version())
            .finish()
    }
}

impl TransactionBuilder<ReplaceTable> {
    pub(crate) fn new(
        snapshot: SnapshotRef,
        metadata_policy: MetadataPolicy,
        engine_info: String,
    ) -> Self {
        Self {
            common_state: TransactionBuilderState {
                engine_info,
                data_change: Some(true),
                ..TransactionBuilderState::default()
            },
            local_state: ReplaceTableBuilderState {
                snapshot,
                metadata_policy,
                data_layout: None,
            },
        }
    }

    /// Sets the replacement partition layout. Omission preserves the existing layout.
    #[internal_api]
    pub(crate) fn with_data_layout(mut self, data_layout: DataLayout) -> Self {
        self.local_state.data_layout = Some(data_layout);
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
    /// Rejects `dataChange = false`, invalid or empty schemas, invalid partition columns,
    /// conflicting mapping metadata, and schema features absent from the existing protocol;
    /// replacement does not upgrade it.
    /// CDF and append-only must be disabled.
    /// Clustering, Iceberg compatibility, column defaults, and adaptive metadata are unsupported.
    /// Both the source and replacement must pass Kernel's write-support checks, including rejection
    /// of SQL-expression invariants. NOT NULL constraints remain the writer's responsibility.
    /// Log-read errors are propagated; no commit is attempted by this method.
    pub fn build(
        self,
        engine: &dyn Engine,
        committer: Box<dyn Committer>,
    ) -> KernelResult<Transaction> {
        self.common_state.validate()?;
        require!(
            self.common_state.data_change != Some(false),
            KernelError::invalid_transaction_state("REPLACE TABLE requires data_change to be true")
        );
        let ReplaceTableBuilderState {
            snapshot,
            metadata_policy,
            data_layout,
        } = self.local_state;
        let old_config = snapshot.table_configuration();
        old_config.ensure_read_write_supported()?;
        let (config, emit_metadata) = match metadata_policy {
            MetadataPolicy::Patch(patch) => patch_existing_config(old_config, patch, data_layout)?,
        };
        let scan = snapshot
            .clone()
            .scan_builder()
            .without_row_transforms()
            .build()?;
        let removals = scan
            .scan_metadata(engine)?
            .map(|batch| {
                let selected = batch?.scan_files.apply_selection_vector()?;
                Ok(FilteredEngineData::with_all_rows_selected(selected))
            })
            .collect::<KernelResult<Vec<_>>>()?;
        // Reuse Write's source validation and commit setup, then replace its operation and config.
        let mut builder = snapshot.update_table(
            UpdateTableOperation::Write,
            self.common_state.engine_info.clone(),
        );
        builder.common_state = self.common_state;
        let transaction = builder.build(engine, committer)?.with_table_replacement(
            config,
            removals,
            emit_metadata,
        );
        transaction.validate_domain_metadata_operations()?;
        Ok(transaction)
    }
}

fn patch_existing_config(
    old_config: &TableConfiguration,
    patch: MetadataPatch,
    data_layout: Option<DataLayout>,
) -> KernelResult<(TableConfiguration, bool)> {
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
    let emit_metadata = patch.schema.is_some() || data_layout.is_some();
    if !emit_metadata {
        return Ok((old_config.clone(), false));
    }
    let schema = patch.schema.unwrap_or_else(|| old_config.logical_schema());
    let partition_columns = match data_layout {
        None => normalize_column_names_to_schema_casing(
            &schema,
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
            let columns = normalize_column_names_to_schema_casing(&schema, &columns);
            validate_partition_columns(&schema, &columns)?;
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
    Ok((
        replacement_config(old_config, &schema, partition_columns)?,
        true,
    ))
}

fn replacement_config(
    old_config: &TableConfiguration,
    schema: &SchemaRef,
    partition_columns: Vec<String>,
) -> KernelResult<TableConfiguration> {
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
    transform_output_type!(|'a, T| KernelResult<Cow<'a, T>>);

    fn transform_struct(&mut self, schema: &'a StructType) -> KernelResult<Cow<'a, StructType>> {
        let old_struct = match self.previous {
            Some(DataType::Struct(schema)) => Some(schema.as_ref()),
            _ => None,
        };
        let previous_fields: HashMap<_, _> = old_struct
            .map(|schema| {
                schema
                    .fields()
                    .map(|field| (field.name().to_lowercase(), field))
                    .collect()
            })
            .unwrap_or_default();
        let fields = schema
            .fields()
            .map(|field| {
                let previous = previous_fields.get(&field.name().to_lowercase()).copied();
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
            .collect::<KernelResult<Vec<_>>>()?;
        Ok(Cow::Owned(StructType::try_new(fields)?))
    }

    fn transform_array_element(
        &mut self,
        element: &'a DataType,
    ) -> KernelResult<Cow<'a, DataType>> {
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

    fn transform_map_key(&mut self, key: &'a DataType) -> KernelResult<Cow<'a, DataType>> {
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

    fn transform_map_value(&mut self, value: &'a DataType) -> KernelResult<Cow<'a, DataType>> {
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

    fn transform_variant(&mut self, variant: &'a StructType) -> KernelResult<Cow<'a, StructType>> {
        require!(
            DataType::Variant(Box::new(variant.clone())) == DataType::unshredded_variant(),
            KernelError::unsupported("REPLACE TABLE requires canonical unshredded variant columns")
        );
        Ok(Cow::Borrowed(variant))
    }
}

#[cfg(test)]
mod tests {
    use test_utils::{actions_to_string, add_commit, engine_store_setup, TestAction};

    use super::MetadataPolicy;
    use crate::committer::FileSystemCommitter;
    use crate::engine::sync::SyncEngine;
    use crate::snapshot::Snapshot;

    #[tokio::test]
    async fn replace_table_compacts_unselected_scan_files_before_retaining_removals(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (store, _unused_engine, table_url) = engine_store_setup("sparse_removals", None);
        let engine = SyncEngine::new_with_store(store.clone());
        add_commit(
            table_url.as_str(),
            store.as_ref(),
            0,
            actions_to_string(vec![
                TestAction::Metadata,
                TestAction::Add("stale.parquet".into()),
                TestAction::Add("live.parquet".into()),
            ]),
        )
        .await?;
        add_commit(
            table_url.as_str(),
            store.as_ref(),
            1,
            actions_to_string(vec![TestAction::Remove("stale.parquet".into())]),
        )
        .await?;
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
        let scan = snapshot
            .clone()
            .scan_builder()
            .without_row_transforms()
            .build()?;
        let batches = scan
            .scan_metadata(&engine)?
            .collect::<crate::Result<Vec<_>>>()?;
        assert!(batches
            .iter()
            .any(|batch| batch.scan_files.selection_vector().contains(&false)));

        let txn = snapshot
            .clone()
            .replace_table(MetadataPolicy::patch_schema(snapshot.schema()), "test")
            .build(&engine, Box::new(FileSystemCommitter::new()))?;
        assert_eq!(
            txn.staged_data_changes
                .remove_files_metadata
                .iter()
                .map(|batch| batch.data().len())
                .sum::<usize>(),
            1
        );
        assert!(txn
            .staged_data_changes
            .remove_files_metadata
            .iter()
            .all(|batch| batch.selection_vector().is_empty()));
        Ok(())
    }
}
