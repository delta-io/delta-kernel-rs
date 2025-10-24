//! This module defines visitors that can be used to extract the various delta actions from
//! [`crate::engine_data::EngineData`] types.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use delta_kernel_derive::internal_api;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType, Schema, StructField};
use crate::utils::require;
use crate::{DeltaResult, Error};

use super::deletion_vector::DeletionVectorDescriptor;
use super::domain_metadata::DomainMetadataMap;
use super::*;

#[derive(Default)]
#[internal_api]
pub(crate) struct MetadataVisitor {
    pub(crate) metadata: Option<Metadata>,
}

impl RowVisitor for MetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Metadata::to_schema().leaves(METADATA_NAME));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if let Some(metadata) = visit_metadata_at(i, getters)? {
                self.metadata = Some(metadata);
                break;
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct SelectionVectorVisitor {
    pub(crate) selection_vector: Vec<bool>,
}

/// A single non-nullable BOOL column
impl RowVisitor for SelectionVectorVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![column_name!("output")], vec![DataType::BOOLEAN]).into());
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of SelectionVectorVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            self.selection_vector
                .push(getters[0].get(i, "selectionvector.output")?);
        }
        Ok(())
    }
}

#[derive(Default)]
#[internal_api]
pub(crate) struct ProtocolVisitor {
    pub(crate) protocol: Option<Protocol>,
}

impl RowVisitor for ProtocolVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Protocol::to_schema().leaves(PROTOCOL_NAME));
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            if let Some(protocol) = visit_protocol_at(i, getters)? {
                self.protocol = Some(protocol);
                break;
            }
        }
        Ok(())
    }
}

#[allow(unused)]
#[derive(Default)]
#[internal_api]
pub(crate) struct AddVisitor {
    pub(crate) adds: Vec<Add>,
}

impl AddVisitor {
    #[internal_api]
    fn visit_add<'a>(
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Add> {
        require!(
            getters.len() == 15,
            Error::InternalError(format!(
                "Wrong number of AddVisitor getters: {}",
                getters.len()
            ))
        );
        let partition_values: HashMap<_, _> = getters[1].get(row_index, "add.partitionValues")?;
        let size: i64 = getters[2].get(row_index, "add.size")?;
        let modification_time: i64 = getters[3].get(row_index, "add.modificationTime")?;
        let data_change: bool = getters[4].get(row_index, "add.dataChange")?;
        let stats: Option<String> = getters[5].get_opt(row_index, "add.stats")?;

        // TODO(nick) extract tags if we ever need them at getters[6]

        let deletion_vector = visit_deletion_vector_at(row_index, &getters[7..])?;

        let base_row_id: Option<i64> = getters[12].get_opt(row_index, "add.base_row_id")?;
        let default_row_commit_version: Option<i64> =
            getters[13].get_opt(row_index, "add.default_row_commit")?;
        let clustering_provider: Option<String> =
            getters[14].get_opt(row_index, "add.clustering_provider")?;

        Ok(Add {
            path,
            partition_values,
            size,
            modification_time,
            data_change,
            stats,
            tags: None,
            deletion_vector,
            base_row_id,
            default_row_commit_version,
            clustering_provider,
        })
    }
    pub(crate) fn names_and_types() -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Add::to_schema().leaves(ADD_NAME));
        NAMES_AND_TYPES.as_ref()
    }
}

impl RowVisitor for AddVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        Self::names_and_types()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                self.adds.push(Self::visit_add(i, path, getters)?);
            }
        }
        Ok(())
    }
}

#[allow(unused)]
#[derive(Default)]
#[internal_api]
pub(crate) struct RemoveVisitor {
    pub(crate) removes: Vec<Remove>,
}

impl RemoveVisitor {
    #[internal_api]
    pub(crate) fn visit_remove<'a>(
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Remove> {
        require!(
            getters.len() == 14,
            Error::InternalError(format!(
                "Wrong number of RemoveVisitor getters: {}",
                getters.len()
            ))
        );
        let deletion_timestamp: Option<i64> =
            getters[1].get_opt(row_index, "remove.deletionTimestamp")?;
        let data_change: bool = getters[2].get(row_index, "remove.dataChange")?;
        let extended_file_metadata: Option<bool> =
            getters[3].get_opt(row_index, "remove.extendedFileMetadata")?;

        let partition_values: Option<HashMap<_, _>> =
            getters[4].get_opt(row_index, "remove.partitionValues")?;

        let size: Option<i64> = getters[5].get_opt(row_index, "remove.size")?;

        // TODO(nick) tags are skipped in getters[6]

        let deletion_vector = visit_deletion_vector_at(row_index, &getters[7..])?;

        let base_row_id: Option<i64> = getters[12].get_opt(row_index, "remove.baseRowId")?;
        let default_row_commit_version: Option<i64> =
            getters[13].get_opt(row_index, "remove.defaultRowCommitVersion")?;

        Ok(Remove {
            path,
            data_change,
            deletion_timestamp,
            extended_file_metadata,
            partition_values,
            size,
            tags: None,
            deletion_vector,
            base_row_id,
            default_row_commit_version,
        })
    }
    pub(crate) fn names_and_types() -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Remove::to_schema().leaves(REMOVE_NAME));
        NAMES_AND_TYPES.as_ref()
    }
}

impl RowVisitor for RemoveVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        Self::names_and_types()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of a Remove action
            if let Some(path) = getters[0].get_opt(i, "remove.path")? {
                self.removes.push(Self::visit_remove(i, path, getters)?);
            }
        }
        Ok(())
    }
}

#[allow(unused)]
#[derive(Default)]
#[internal_api]
pub(crate) struct CdcVisitor {
    pub(crate) cdcs: Vec<Cdc>,
}

impl CdcVisitor {
    #[internal_api]
    pub(crate) fn visit_cdc<'a>(
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Cdc> {
        Ok(Cdc {
            path,
            partition_values: getters[1].get(row_index, "cdc.partitionValues")?,
            size: getters[2].get(row_index, "cdc.size")?,
            data_change: getters[3].get(row_index, "cdc.dataChange")?,
            tags: getters[4].get_opt(row_index, "cdc.tags")?,
        })
    }
}

impl RowVisitor for CdcVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Cdc::to_schema().leaves(CDC_NAME));
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 5,
            Error::InternalError(format!(
                "Wrong number of CdcVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of a Cdc action
            if let Some(path) = getters[0].get_opt(i, "cdc.path")? {
                self.cdcs.push(Self::visit_cdc(i, path, getters)?);
            }
        }
        Ok(())
    }
}

pub(crate) type SetTransactionMap = HashMap<String, SetTransaction>;

/// Extract application transaction actions from the log into a map
///
/// This visitor maintains the first entry for each application id it
/// encounters.  When a specific application id is required then
/// `application_id` can be set. This bounds the memory required for the
/// visitor to at most one entry and reduces the amount of processing
/// required.
///
#[derive(Default, Debug)]
#[internal_api]
pub(crate) struct SetTransactionVisitor {
    pub(crate) set_transactions: SetTransactionMap,
    pub(crate) application_id: Option<String>,
    /// Minimum timestamp for transaction retention. Transactions with last_updated
    /// older than or equal to this timestamp will be filtered out. None means no filtering.
    expiration_timestamp: Option<i64>,
}

impl SetTransactionVisitor {
    /// Create a new visitor. When application_id is set then bookkeeping is only for that id only
    pub(crate) fn new(application_id: Option<String>, expiration_timestamp: Option<i64>) -> Self {
        SetTransactionVisitor {
            set_transactions: HashMap::default(),
            application_id,
            expiration_timestamp,
        }
    }

    #[internal_api]
    pub(crate) fn visit_txn<'a>(
        row_index: usize,
        app_id: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<SetTransaction> {
        require!(
            getters.len() == 3,
            Error::InternalError(format!(
                "Wrong number of SetTransactionVisitor getters: {}",
                getters.len()
            ))
        );
        let version: i64 = getters[1].get(row_index, "txn.version")?;
        let last_updated: Option<i64> = getters[2].get_opt(row_index, "txn.lastUpdated")?;
        Ok(SetTransaction {
            app_id,
            version,
            last_updated,
        })
    }
}

impl RowVisitor for SetTransactionVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| SetTransaction::to_schema().leaves(SET_TRANSACTION_NAME));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // Assumes batches are visited in reverse order relative to the log
        for i in 0..row_count {
            if let Some(app_id) = getters[0].get_opt(i, "txn.appId")? {
                // if caller requested a specific id then only visit matches
                if self
                    .application_id
                    .as_ref()
                    .is_none_or(|requested| requested.eq(&app_id))
                {
                    let txn = SetTransactionVisitor::visit_txn(i, app_id, getters)?;
                    // Check retention: filter out transactions that are old
                    // If last_updated is None, the transaction never expires
                    match self.expiration_timestamp.zip(txn.last_updated) {
                        Some((expiration_ts, last_updated)) if last_updated <= expiration_ts => {
                            continue
                        }
                        _ => (),
                    }
                    if !self.set_transactions.contains_key(&txn.app_id) {
                        self.set_transactions.insert(txn.app_id.clone(), txn);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
#[internal_api]
pub(crate) struct SidecarVisitor {
    pub(crate) sidecars: Vec<Sidecar>,
}

impl SidecarVisitor {
    fn visit_sidecar<'a>(
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Sidecar> {
        Ok(Sidecar {
            path,
            size_in_bytes: getters[1].get(row_index, "sidecar.sizeInBytes")?,
            modification_time: getters[2].get(row_index, "sidecar.modificationTime")?,
            tags: getters[3].get_opt(row_index, "sidecar.tags")?,
        })
    }
}

impl RowVisitor for SidecarVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Sidecar::to_schema().leaves(SIDECAR_NAME));
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 4,
            Error::InternalError(format!(
                "Wrong number of SidecarVisitor getters: {}",
                getters.len()
            ))
        );
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of a Sidecar action
            if let Some(path) = getters[0].get_opt(i, "sidecar.path")? {
                self.sidecars.push(Self::visit_sidecar(i, path, getters)?);
            }
        }
        Ok(())
    }
}

/// Visit data batches of actions to extract the latest domain metadata for each domain. Note that
/// this will return all domains including 'removed' domains. The caller is responsible for either
/// using or throwing away these tombstones.
///
/// Note that this visitor requires that the log (each actions batch) is replayed in reverse order.
///
/// This visitor maintains the first entry for each domain it encounters. A domain_filter may be
/// included to only retain the domain metadata for a specific domain (in order to bound memory
/// requirements).
#[derive(Debug, Default)]
pub(crate) struct DomainMetadataVisitor {
    domain_metadatas: DomainMetadataMap,
    domain_filter: Option<String>,
}

impl DomainMetadataVisitor {
    /// Create a new visitor. When domain_filter is set then we only retain
    pub(crate) fn new(domain_filter: Option<String>) -> Self {
        DomainMetadataVisitor {
            domain_filter,
            ..Default::default()
        }
    }

    pub(crate) fn visit_domain_metadata<'a>(
        row_index: usize,
        domain: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<DomainMetadata> {
        require!(
            getters.len() == 3,
            Error::InternalError(format!(
                "Wrong number of DomainMetadataVisitor getters: {}",
                getters.len()
            ))
        );
        let configuration: String = getters[1].get(row_index, "domainMetadata.configuration")?;
        let removed: bool = getters[2].get(row_index, "domainMetadata.removed")?;
        Ok(DomainMetadata {
            domain,
            configuration,
            removed,
        })
    }

    pub(crate) fn filter_found(&self) -> bool {
        self.domain_filter.is_some() && !self.domain_metadatas.is_empty()
    }

    pub(crate) fn into_domain_metadatas(mut self) -> DomainMetadataMap {
        // note that the resulting visitor.domain_metadatas includes removed domains, so we need to filter
        self.domain_metadatas.retain(|_, dm| !dm.removed);
        self.domain_metadatas
    }
}

impl RowVisitor for DomainMetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| DomainMetadata::to_schema().leaves(DOMAIN_METADATA_NAME));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // Requires that batches are visited in reverse order relative to the log
        for i in 0..row_count {
            let domain: Option<String> = getters[0].get_opt(i, "domainMetadata.domain")?;
            if let Some(domain) = domain {
                // if caller requested a specific domain then only visit matches
                let filter = self.domain_filter.as_ref();
                if filter.is_none_or(|requested| requested == &domain) {
                    let domain_metadata =
                        DomainMetadataVisitor::visit_domain_metadata(i, domain.clone(), getters)?;
                    self.domain_metadatas
                        .entry(domain)
                        .or_insert(domain_metadata);
                }
            }
        }
        Ok(())
    }
}

/// Get a DV out of some engine data. The caller is responsible for slicing the `getters` slice such
/// that the first element contains the `storageType` element of the deletion vector.
pub(crate) fn visit_deletion_vector_at<'a>(
    row_index: usize,
    getters: &[&'a dyn GetData<'a>],
) -> DeltaResult<Option<DeletionVectorDescriptor>> {
    let storage_type_opt: Option<String> =
        getters[0].get_opt(row_index, "remove.deletionVector.storageType")?;
    if let Some(storage_type_str) = storage_type_opt {
        let storage_type = storage_type_str.parse()?;
        let path_or_inline_dv: String =
            getters[1].get(row_index, "deletionVector.pathOrInlineDv")?;
        let offset: Option<i32> = getters[2].get_opt(row_index, "deletionVector.offset")?;
        let size_in_bytes: i32 = getters[3].get(row_index, "deletionVector.sizeInBytes")?;
        let cardinality: i64 = getters[4].get(row_index, "deletionVector.cardinality")?;
        Ok(Some(DeletionVectorDescriptor {
            storage_type,
            path_or_inline_dv,
            offset,
            size_in_bytes,
            cardinality,
        }))
    } else {
        Ok(None)
    }
}

/// Get a Metadata out of some engine data. Note that Ok(None) is returned if there is no Metadata
/// found. The caller is responsible for slicing the `getters` slice such that the first element
/// contains the `id` element of the metadata.
#[internal_api]
pub(crate) fn visit_metadata_at<'a>(
    row_index: usize,
    getters: &[&'a dyn GetData<'a>],
) -> DeltaResult<Option<Metadata>> {
    require!(
        getters.len() == 9,
        Error::InternalError(format!(
            "Wrong number of MetadataVisitor getters: {}",
            getters.len()
        ))
    );

    // Since id column is required, use it to detect presence of a metadata action
    let Some(id) = getters[0].get_opt(row_index, "metadata.id")? else {
        return Ok(None);
    };

    let name: Option<String> = getters[1].get_opt(row_index, "metadata.name")?;
    let description: Option<String> = getters[2].get_opt(row_index, "metadata.description")?;
    // get format out of primitives
    let format_provider: String = getters[3].get(row_index, "metadata.format.provider")?;
    // options for format is always empty, so skip getters[4]
    let schema_string: String = getters[5].get(row_index, "metadata.schema_string")?;
    let partition_columns: Vec<_> = getters[6].get(row_index, "metadata.partition_list")?;
    let created_time: Option<i64> = getters[7].get_opt(row_index, "metadata.created_time")?;
    let configuration_map_opt: Option<HashMap<_, _>> =
        getters[8].get_opt(row_index, "metadata.configuration")?;
    let configuration = configuration_map_opt.unwrap_or_else(HashMap::new);

    Ok(Some(Metadata {
        id,
        name,
        description,
        format: Format {
            provider: format_provider,
            options: HashMap::new(),
        },
        schema_string,
        partition_columns,
        created_time,
        configuration,
    }))
}

/// Get a Protocol out of some engine data. Note that Ok(None) is returned if there is no Protocol
/// found. The caller is responsible for slicing the `getters` slice such that the first element
/// contains the `min_reader_version` element of the protocol.
#[internal_api]
pub(crate) fn visit_protocol_at<'a>(
    row_index: usize,
    getters: &[&'a dyn GetData<'a>],
) -> DeltaResult<Option<Protocol>> {
    require!(
        getters.len() == 4,
        Error::InternalError(format!(
            "Wrong number of ProtocolVisitor getters: {}",
            getters.len()
        ))
    );
    // Since minReaderVersion column is required, use it to detect presence of a Protocol action
    let Some(min_reader_version) = getters[0].get_opt(row_index, "protocol.min_reader_version")?
    else {
        return Ok(None);
    };
    let min_writer_version: i32 = getters[1].get(row_index, "protocol.min_writer_version")?;
    let reader_features: Option<Vec<_>> =
        getters[2].get_opt(row_index, "protocol.reader_features")?;
    let writer_features: Option<Vec<_>> =
        getters[3].get_opt(row_index, "protocol.writer_features")?;

    let protocol = Protocol::try_new(
        min_reader_version,
        min_writer_version,
        reader_features,
        writer_features,
    )?;
    Ok(Some(protocol))
}

/// This visitor extracts the in-commit timestamp (ICT) from a CommitInfo action in the log it is
/// present. The [`EngineData`] being visited must have the schema defined in
/// [`InCommitTimestampVisitor::schema`].
///
/// Only the a single row of the engine data is checked (the first row). This is because in-commit
/// timestamps requires that the CommitInfo containing the ICT be the first action in the log.
#[allow(unused)]
#[derive(Default)]
pub(crate) struct InCommitTimestampVisitor {
    pub(crate) in_commit_timestamp: Option<i64>,
}

impl InCommitTimestampVisitor {
    #[allow(unused)]
    /// Get the schema that the visitor expects the data to have.
    pub(crate) fn schema() -> Arc<Schema> {
        static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
            let ict_type = StructField::new("inCommitTimestamp", DataType::LONG, true);
            Arc::new(StructType::new_unchecked(vec![StructField::new(
                COMMIT_INFO_NAME,
                StructType::new_unchecked([ict_type]),
                true,
            )]))
        });
        SCHEMA.clone()
    }
}
impl RowVisitor for InCommitTimestampVisitor {
    fn selected_column_names_and_types(
        &self,
    ) -> (&'static [crate::schema::ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names = vec![column_name!("commitInfo.inCommitTimestamp")];
            let types = vec![DataType::LONG];

            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(
        &mut self,
        row_count: usize,
        getters: &[&'a dyn crate::engine_data::GetData<'a>],
    ) -> DeltaResult<()> {
        require!(
            getters.len() == 1,
            Error::InternalError(format!(
                "Wrong number of InCommitTimestampVisitor getters: {}",
                getters.len()
            ))
        );

        // If the batch is empty, return
        if row_count == 0 {
            return Ok(());
        }
        // CommitInfo must be the first action in a commit
        if let Some(in_commit_timestamp) = getters[0].get_long(0, "commitInfo.inCommitTimestamp")? {
            self.in_commit_timestamp = Some(in_commit_timestamp);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::arrow::array::StringArray;

    use crate::engine::sync::SyncEngine;
    use crate::expressions::{column_expr_ref, Expression};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{action_batch, parse_json_batch};
    use crate::Engine;

    #[test]
    fn test_parse_protocol() -> DeltaResult<()> {
        let data = action_batch();
        let parsed = Protocol::try_new_from_data(data.as_ref())?.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![TableFeature::DeletionVectors]),
            writer_features: Some(vec![TableFeature::DeletionVectors]),
        };
        assert_eq!(parsed, expected);
        Ok(())
    }

    #[test]
    fn test_parse_cdc() -> DeltaResult<()> {
        let data = action_batch();
        let mut visitor = CdcVisitor::default();
        visitor.visit_rows_of(data.as_ref())?;
        let expected = Cdc {
            path: "_change_data/age=21/cdc-00000-93f7fceb-281a-446a-b221-07b88132d203.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("age".to_string(), "21".to_string()),
            ]),
            size: 1033,
            data_change: false,
            tags: None
        };

        assert_eq!(&visitor.cdcs, &[expected]);
        Ok(())
    }

    #[test]
    fn test_parse_sidecar() -> DeltaResult<()> {
        let data = action_batch();

        let mut visitor = SidecarVisitor::default();
        visitor.visit_rows_of(data.as_ref())?;

        let sidecar1 = Sidecar {
            path: "016ae953-37a9-438e-8683-9a9a4a79a395.parquet".into(),
            size_in_bytes: 9268,
            modification_time: 1714496113961,
            tags: Some(HashMap::from([(
                "tag_foo".to_string(),
                "tag_bar".to_string(),
            )])),
        };

        assert_eq!(visitor.sidecars.len(), 1);
        assert_eq!(visitor.sidecars[0], sidecar1);

        Ok(())
    }

    #[test]
    fn test_parse_metadata() -> DeltaResult<()> {
        let data = action_batch();
        let parsed = Metadata::try_new_from_data(data.as_ref())?.unwrap();

        let configuration = HashMap::from_iter([
            (
                "delta.enableDeletionVectors".to_string(),
                "true".to_string(),
            ),
            ("delta.columnMapping.mode".to_string(), "none".to_string()),
            ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
        ]);
        let expected = Metadata {
            id: "testId".into(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".into(),
                options: Default::default(),
            },
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            partition_columns: Vec::new(),
            created_time: Some(1677811175819),
            configuration,
        };
        assert_eq!(parsed, expected);
        Ok(())
    }

    #[test]
    fn test_parse_add_partitioned() {
        let json_strings: StringArray = vec![
            r#"{"commitInfo":{"timestamp":1670892998177,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[\"c1\",\"c2\"]"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"1356"},"engineInfo":"Apache-Spark/3.3.1 Delta-Lake/2.2.0","txnId":"046a258f-45e3-4657-b0bf-abfb0f76681c"}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet","partitionValues":{"c1":"5","c2":"b"},"size":452,"modificationTime":1670892998136,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}"}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);
        let mut add_visitor = AddVisitor::default();
        add_visitor.visit_rows_of(batch.as_ref()).unwrap();
        let add1 = Add {
            path: "c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("c1".to_string(), "4".to_string()),
                ("c2".to_string(), "c".to_string()),
            ]),
            size: 452,
            modification_time: 1670892998135,
            data_change: true,
            stats: Some("{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}".into()),
            ..Default::default()
        };
        let add2 = Add {
            path: "c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("c1".to_string(), "5".to_string()),
                ("c2".to_string(), "b".to_string()),
            ]),
            modification_time: 1670892998136,
            stats: Some("{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}".into()),
            ..add1.clone()
        };
        let add3 = Add {
            path: "c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("c1".to_string(), "6".to_string()),
                ("c2".to_string(), "a".to_string()),
            ]),
            modification_time: 1670892998137,
            stats: Some("{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}".into()),
            ..add1.clone()
        };
        let expected = vec![add1, add2, add3];
        assert_eq!(add_visitor.adds.len(), expected.len());
        for (add, expected) in add_visitor.adds.into_iter().zip(expected.into_iter()) {
            assert_eq!(add, expected);
        }
    }

    #[test]
    fn test_parse_remove_partitioned() {
        let json_strings: StringArray = vec![
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"remove":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","deletionTimestamp":1670892998135,"dataChange":true,"partitionValues":{"c1":"4","c2":"c"},"size":452}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);
        let mut remove_visitor = RemoveVisitor::default();
        remove_visitor.visit_rows_of(batch.as_ref()).unwrap();
        let expected_remove = Remove {
            path: "c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet"
                .into(),
            deletion_timestamp: Some(1670892998135),
            data_change: true,
            partition_values: Some(HashMap::from([
                ("c1".to_string(), "4".to_string()),
                ("c2".to_string(), "c".to_string()),
            ])),
            size: Some(452),
            ..Default::default()
        };
        assert_eq!(
            remove_visitor.removes.len(),
            1,
            "Unexpected number of remove actions"
        );
        assert_eq!(
            remove_visitor.removes[0], expected_remove,
            "Unexpected remove action"
        );
    }

    #[test]
    fn test_parse_txn() {
        let json_strings: StringArray = vec![
            r#"{"commitInfo":{"timestamp":1670892998177,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[\"c1\",\"c2\"]"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"1356"},"engineInfo":"Apache-Spark/3.3.1 Delta-Lake/2.2.0","txnId":"046a258f-45e3-4657-b0bf-abfb0f76681c"}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"txn":{"appId":"myApp","version": 3}}"#,
            r#"{"txn":{"appId":"myApp2","version": 4, "lastUpdated": 1670892998177}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);
        let mut txn_visitor = SetTransactionVisitor::default();
        txn_visitor.visit_rows_of(batch.as_ref()).unwrap();
        let mut actual = txn_visitor.set_transactions;
        assert_eq!(
            actual.remove("myApp2"),
            Some(SetTransaction {
                app_id: "myApp2".to_string(),
                version: 4,
                last_updated: Some(1670892998177),
            })
        );
        assert_eq!(
            actual.remove("myApp"),
            Some(SetTransaction {
                app_id: "myApp".to_string(),
                version: 3,
                last_updated: None,
            })
        );
    }

    #[test]
    fn test_parse_domain_metadata() {
        // note: we process commit_1, commit_0 since the visitor expects things in reverse order.
        // these come from the 'more recent' commit
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"domainMetadata":{"domain": "zach1","configuration":"cfg1","removed": true}}"#,
            r#"{"domainMetadata":{"domain": "zach2","configuration":"cfg2","removed": false}}"#,
            r#"{"domainMetadata":{"domain": "zach3","configuration":"cfg3","removed": true}}"#,
            r#"{"domainMetadata":{"domain": "zach4","configuration":"cfg4","removed": false}}"#,
            r#"{"domainMetadata":{"domain": "zach5","configuration":"cfg5","removed": true}}"#,
            r#"{"domainMetadata":{"domain": "zach6","configuration":"cfg6","removed": false}}"#,
        ]
        .into();
        let commit_1 = parse_json_batch(json_strings);
        // these come from the 'older' commit
        let json_strings: StringArray = vec![
            r#"{"domainMetadata":{"domain": "zach1","configuration":"old_cfg1","removed": true}}"#,
            r#"{"domainMetadata":{"domain": "zach2","configuration":"old_cfg2","removed": false}}"#,
            r#"{"domainMetadata":{"domain": "zach3","configuration":"old_cfg3","removed": false}}"#,
            r#"{"domainMetadata":{"domain": "zach4","configuration":"old_cfg4","removed": true}}"#,
            r#"{"domainMetadata":{"domain": "zach7","configuration":"cfg7","removed": true}}"#,
            r#"{"domainMetadata":{"domain": "zach8","configuration":"cfg8","removed": false}}"#,
        ]
        .into();
        let commit_0 = parse_json_batch(json_strings);
        let mut domain_metadata_visitor = DomainMetadataVisitor::default();
        // visit commit 1 then 0
        domain_metadata_visitor
            .visit_rows_of(commit_1.as_ref())
            .unwrap();
        domain_metadata_visitor
            .visit_rows_of(commit_0.as_ref())
            .unwrap();
        let actual = domain_metadata_visitor.domain_metadatas.clone();
        let expected = DomainMetadataMap::from([
            (
                "zach1".to_string(),
                DomainMetadata {
                    domain: "zach1".to_string(),
                    configuration: "cfg1".to_string(),
                    removed: true,
                },
            ),
            (
                "zach2".to_string(),
                DomainMetadata {
                    domain: "zach2".to_string(),
                    configuration: "cfg2".to_string(),
                    removed: false,
                },
            ),
            (
                "zach3".to_string(),
                DomainMetadata {
                    domain: "zach3".to_string(),
                    configuration: "cfg3".to_string(),
                    removed: true,
                },
            ),
            (
                "zach4".to_string(),
                DomainMetadata {
                    domain: "zach4".to_string(),
                    configuration: "cfg4".to_string(),
                    removed: false,
                },
            ),
            (
                "zach5".to_string(),
                DomainMetadata {
                    domain: "zach5".to_string(),
                    configuration: "cfg5".to_string(),
                    removed: true,
                },
            ),
            (
                "zach6".to_string(),
                DomainMetadata {
                    domain: "zach6".to_string(),
                    configuration: "cfg6".to_string(),
                    removed: false,
                },
            ),
            (
                "zach7".to_string(),
                DomainMetadata {
                    domain: "zach7".to_string(),
                    configuration: "cfg7".to_string(),
                    removed: true,
                },
            ),
            (
                "zach8".to_string(),
                DomainMetadata {
                    domain: "zach8".to_string(),
                    configuration: "cfg8".to_string(),
                    removed: false,
                },
            ),
        ]);
        assert_eq!(actual, expected);

        let expected = DomainMetadataMap::from([
            (
                "zach2".to_string(),
                DomainMetadata {
                    domain: "zach2".to_string(),
                    configuration: "cfg2".to_string(),
                    removed: false,
                },
            ),
            (
                "zach4".to_string(),
                DomainMetadata {
                    domain: "zach4".to_string(),
                    configuration: "cfg4".to_string(),
                    removed: false,
                },
            ),
            (
                "zach6".to_string(),
                DomainMetadata {
                    domain: "zach6".to_string(),
                    configuration: "cfg6".to_string(),
                    removed: false,
                },
            ),
            (
                "zach8".to_string(),
                DomainMetadata {
                    domain: "zach8".to_string(),
                    configuration: "cfg8".to_string(),
                    removed: false,
                },
            ),
        ]);
        assert_eq!(domain_metadata_visitor.into_domain_metadatas(), expected);

        // test filtering
        let mut domain_metadata_visitor = DomainMetadataVisitor::new(Some("zach3".to_string()));
        domain_metadata_visitor
            .visit_rows_of(commit_1.as_ref())
            .unwrap();
        domain_metadata_visitor
            .visit_rows_of(commit_0.as_ref())
            .unwrap();
        let actual = domain_metadata_visitor.domain_metadatas.clone();
        let expected = DomainMetadataMap::from([(
            "zach3".to_string(),
            DomainMetadata {
                domain: "zach3".to_string(),
                configuration: "cfg3".to_string(),
                removed: true,
            },
        )]);
        assert_eq!(actual, expected);
        let expected = DomainMetadataMap::from([]);
        assert_eq!(domain_metadata_visitor.into_domain_metadatas(), expected);

        // test filtering for a domain that is not present
        let mut domain_metadata_visitor = DomainMetadataVisitor::new(Some("notexist".to_string()));
        domain_metadata_visitor
            .visit_rows_of(commit_1.as_ref())
            .unwrap();
        domain_metadata_visitor
            .visit_rows_of(commit_0.as_ref())
            .unwrap();
        assert!(domain_metadata_visitor.domain_metadatas.is_empty());
    }

    /*************************************
     *  In-commit timestamp visitor tests *
     **************************************/

    fn add_action() -> &'static str {
        r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#
    }
    fn commit_info_action() -> &'static str {
        r#"{"commitInfo":{"inCommitTimestamp":1677811178585, "timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#
    }

    fn transform_batch(batch: Box<dyn EngineData>) -> Box<dyn EngineData> {
        let engine = SyncEngine::new();
        let expression =
            Expression::Struct(vec![Arc::new(Expression::Struct(vec![column_expr_ref!(
                "commitInfo.inCommitTimestamp"
            )]))]);
        engine
            .evaluation_handler()
            .new_expression_evaluator(
                get_commit_schema().clone(),
                expression.into(),
                InCommitTimestampVisitor::schema().into(),
            )
            .evaluate(batch.as_ref())
            .unwrap()
    }

    // Helper function to reduce duplication in tests
    fn run_timestamp_visitor_test(json_strings: Vec<&str>, expected_timestamp: Option<i64>) {
        let json_strings: StringArray = json_strings.into();
        let batch = parse_json_batch(json_strings);
        let batch = transform_batch(batch);
        let mut visitor = InCommitTimestampVisitor::default();
        visitor.visit_rows_of(batch.as_ref()).unwrap();
        assert_eq!(visitor.in_commit_timestamp, expected_timestamp);
    }

    #[test]
    fn commit_info_not_first() {
        run_timestamp_visitor_test(vec![add_action(), commit_info_action()], None);
    }

    #[test]
    fn commit_info_not_present() {
        run_timestamp_visitor_test(vec![add_action()], None);
    }

    #[test]
    fn commit_info_get() {
        run_timestamp_visitor_test(
            vec![commit_info_action(), add_action()],
            Some(1677811178585), // Retrieved ICT
        );
    }
}
