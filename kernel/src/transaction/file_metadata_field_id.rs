//! Assigns the file metadata field ID of `add.clusteringProvider` on adaptiveMetadata tables.
//!
//! Column mapping IDs and file metadata field IDs share the `delta.columnMapping.maxColumnId`
//! counter, so the new ID is chosen above every ID the table already uses, and the counter is
//! raised to it in the same commit.

use serde_json::{Map, Value};

use crate::actions::DomainMetadata;
use crate::content_tree::file_metadata::{
    ADAPTIVE_METADATA_DOMAIN_NAME, FILE_METADATA_FIELD_IDS, FileMetadataFieldIds,
    configuration_object,
};
use crate::content_tree::stats::{MAX_DATA_FIELD_ID, is_data_field_id};
use crate::snapshot::Snapshot;
use crate::table_configuration::TableConfiguration;
use crate::table_features::{TableFeature, effective_max_column_id};
use crate::table_properties::COLUMN_MAPPING_MAX_COLUMN_ID;
use crate::transaction::CLUSTERING_PROVIDER_NAME;
use crate::{Engine, KernelError, KernelResult, Result};

/// A file metadata field ID assigned to `add.clusteringProvider`, and the domain that records it.
pub(crate) struct ClusteringProviderFieldId {
    /// The new ID. It is also the new `delta.columnMapping.maxColumnId`.
    pub(crate) id: i32,
    /// The `delta.adaptiveMetadata` domain to commit: the existing configuration with the ID
    /// added, and every other key kept.
    pub(crate) domain: DomainMetadata,
}

/// Returns a new ID for `add.clusteringProvider` when `table_config` needs one: the table supports
/// both adaptiveMetadata and clustering, and its mapping has no ID yet. Returns `None` otherwise.
///
/// `current_domain` returns the table's `delta.adaptiveMetadata` configuration. Only called for
/// tables that can need an ID.
pub(crate) fn assign_clustering_provider_field_id(
    table_config: &TableConfiguration,
    current_domain: impl FnOnce() -> KernelResult<Option<String>>,
) -> KernelResult<Option<ClusteringProviderFieldId>> {
    let supports_both = table_config.is_feature_supported(&TableFeature::AdaptiveMetadataPreview)
        && table_config.is_feature_supported(&TableFeature::ClusteredTable);
    if !supports_both {
        return Ok(None);
    }
    let current = current_domain()?;
    let mut configuration = match current.as_deref() {
        Some(current) => {
            let mapping = FileMetadataFieldIds::try_from_configuration(current)
                .map_err(KernelError::generic)?;
            if mapping.clustering_provider.is_some() {
                return Ok(None);
            }
            configuration_object(current).map_err(KernelError::generic)?
        }
        None => Map::new(),
    };
    // The typed parse above rejects a mapping that is not an object, so the empty default only
    // applies when the domain is absent.
    let mut field_ids = match configuration.remove(FILE_METADATA_FIELD_IDS) {
        Some(Value::Object(field_ids)) => field_ids,
        _ => Map::new(),
    };

    // Unrecognized keys in the mapping may hold IDs too, so they count toward the largest in use.
    let persisted = table_config.table_properties().column_mapping_max_column_id;
    let seed = effective_max_column_id(
        &table_config.logical_schema(),
        table_config.column_mapping_mode(),
        persisted,
    )?
    .ok_or_else(|| {
        KernelError::invalid_protocol(format!(
            "Cannot assign a file metadata field ID to {CLUSTERING_PROVIDER_NAME} without \
             {COLUMN_MAPPING_MAX_COLUMN_ID} in table properties"
        ))
    })?;
    let largest_in_use = field_ids
        .values()
        .filter_map(Value::as_i64)
        .fold(seed, i64::max);
    let id = largest_in_use
        .checked_add(1)
        .and_then(|next| i32::try_from(next).ok())
        .filter(|id| is_data_field_id(*id))
        .ok_or_else(|| {
            KernelError::generic(format!(
                "Cannot assign file metadata field ID to {CLUSTERING_PROVIDER_NAME} after \
                 {largest_in_use}: data field IDs go up to {MAX_DATA_FIELD_ID}"
            ))
        })?;

    field_ids.insert(CLUSTERING_PROVIDER_NAME.to_string(), Value::from(id));
    configuration.insert(
        FILE_METADATA_FIELD_IDS.to_string(),
        Value::Object(field_ids),
    );
    let configuration = serde_json::to_string(&configuration).map_err(|err| {
        KernelError::generic(format!(
            "Cannot serialize {ADAPTIVE_METADATA_DOMAIN_NAME} for {CLUSTERING_PROVIDER_NAME}: {err}"
        ))
    })?;
    Ok(Some(ClusteringProviderFieldId {
        id,
        domain: DomainMetadata::new(ADAPTIVE_METADATA_DOMAIN_NAME.to_string(), configuration),
    }))
}

/// Assigns the `clusteringProvider` ID for an update transaction on `read_snapshot`'s table,
/// unless the transaction is a blind append.
pub(crate) fn assign_for_update(
    table_config: TableConfiguration,
    read_snapshot: &Snapshot,
    is_blind_append: bool,
    engine: &dyn Engine,
) -> Result<(TableConfiguration, Option<DomainMetadata>)> {
    if is_blind_append {
        return Ok((table_config, None));
    }
    assign_and_raise_max_column_id(table_config, || {
        read_snapshot
            .reader()
            .get_domain_metadata_internal(ADAPTIVE_METADATA_DOMAIN_NAME, engine)
    })
}

/// Assigns the `clusteringProvider` ID for a table being created. The new table has no domain
/// yet, so the mapping is never read.
pub(crate) fn assign_for_create(
    table_config: TableConfiguration,
) -> Result<(TableConfiguration, Option<DomainMetadata>)> {
    assign_and_raise_max_column_id(table_config, || Ok(None))
}

/// Raises `delta.columnMapping.maxColumnId` to the new ID and returns the domain to commit with
/// it. The counter and the mapping change in one commit, so a later writer cannot pick the same
/// ID. When no ID is assigned, the configuration is returned unchanged with a `None` domain.
fn assign_and_raise_max_column_id(
    table_config: TableConfiguration,
    current_domain: impl FnOnce() -> KernelResult<Option<String>>,
) -> Result<(TableConfiguration, Option<DomainMetadata>)> {
    let assignment = assign_clustering_provider_field_id(&table_config, current_domain)?;
    let Some(ClusteringProviderFieldId { id, domain }) = assignment else {
        return Ok((table_config, None));
    };
    let metadata = table_config
        .metadata()
        .clone()
        .with_configuration_entry(COLUMN_MAPPING_MAX_COLUMN_ID, id.to_string());
    let updated = TableConfiguration::try_new_with_schema(
        &table_config,
        metadata,
        table_config.logical_schema(),
    )?;
    Ok((updated, Some(domain)))
}
