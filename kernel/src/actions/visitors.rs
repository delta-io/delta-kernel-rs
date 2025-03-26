//! This module defines visitors that can be used to extract the various delta actions from
//! [`crate::engine_data::EngineData`] types.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use tracing::debug;

use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::scan::log_replay::FileActionKey;
use crate::schema::{column_name, ColumnName, ColumnNamesAndTypes, DataType};
use crate::utils::require;
use crate::{DeltaResult, Error};

use super::deletion_vector::DeletionVectorDescriptor;
use super::schemas::ToSchema as _;
use super::{
    Add, Cdc, Format, Metadata, Protocol, Remove, SetTransaction, Sidecar, ADD_NAME, CDC_NAME,
    METADATA_NAME, PROTOCOL_NAME, REMOVE_NAME, SET_TRANSACTION_NAME, SIDECAR_NAME,
};

#[derive(Default)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct MetadataVisitor {
    pub(crate) metadata: Option<Metadata>,
}

impl MetadataVisitor {
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    fn visit_metadata<'a>(
        row_index: usize,
        id: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Metadata> {
        require!(
            getters.len() == 9,
            Error::InternalError(format!(
                "Wrong number of MetadataVisitor getters: {}",
                getters.len()
            ))
        );
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

        Ok(Metadata {
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
        })
    }
}

impl RowVisitor for MetadataVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Metadata::to_schema().leaves(METADATA_NAME));
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since id column is required, use it to detect presence of a metadata action
            if let Some(id) = getters[0].get_opt(i, "metadata.id")? {
                self.metadata = Some(Self::visit_metadata(i, id, getters)?);
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
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct ProtocolVisitor {
    pub(crate) protocol: Option<Protocol>,
}

impl ProtocolVisitor {
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    pub(crate) fn visit_protocol<'a>(
        row_index: usize,
        min_reader_version: i32,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Protocol> {
        require!(
            getters.len() == 4,
            Error::InternalError(format!(
                "Wrong number of ProtocolVisitor getters: {}",
                getters.len()
            ))
        );
        let min_writer_version: i32 = getters[1].get(row_index, "protocol.min_writer_version")?;
        let reader_features: Option<Vec<_>> =
            getters[2].get_opt(row_index, "protocol.reader_features")?;
        let writer_features: Option<Vec<_>> =
            getters[3].get_opt(row_index, "protocol.writer_features")?;

        Protocol::try_new(
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        )
    }
}

impl RowVisitor for ProtocolVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| Protocol::to_schema().leaves(PROTOCOL_NAME));
        NAMES_AND_TYPES.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since minReaderVersion column is required, use it to detect presence of a Protocol action
            if let Some(mrv) = getters[0].get_opt(i, "protocol.min_reader_version")? {
                self.protocol = Some(Self::visit_protocol(i, mrv, getters)?);
                break;
            }
        }
        Ok(())
    }
}

#[allow(unused)]
#[derive(Default)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct AddVisitor {
    pub(crate) adds: Vec<Add>,
}

impl AddVisitor {
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
    #[cfg_attr(not(feature = "developer-visibility"), visibility::make(pub(crate)))]
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
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct RemoveVisitor {
    pub(crate) removes: Vec<Remove>,
}

impl RemoveVisitor {
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct CdcVisitor {
    pub(crate) cdcs: Vec<Cdc>,
}

impl CdcVisitor {
    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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

pub type SetTransactionMap = HashMap<String, SetTransaction>;

/// Extract application transaction actions from the log into a map
///
/// This visitor maintains the first entry for each application id it
/// encounters.  When a specific application id is required then
/// `application_id` can be set. This bounds the memory required for the
/// visitor to at most one entry and reduces the amount of processing
/// required.
///
#[derive(Default, Debug)]
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct SetTransactionVisitor {
    pub(crate) set_transactions: SetTransactionMap,
    pub(crate) application_id: Option<String>,
}

impl SetTransactionVisitor {
    /// Create a new visitor. When application_id is set then bookkeeping is only for that id only
    pub(crate) fn new(application_id: Option<String>) -> Self {
        SetTransactionVisitor {
            set_transactions: HashMap::default(),
            application_id,
        }
    }

    #[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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
                if !self
                    .application_id
                    .as_ref()
                    .is_some_and(|requested| !requested.eq(&app_id))
                {
                    let txn = SetTransactionVisitor::visit_txn(i, app_id, getters)?;
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
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
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
/// A visitor that filters actions for inclusion in a checkpoint file.
///
/// This visitor processes actions in newest-to-oldest order (as they appear in log
/// replay) and applies deduplication logic for both file and non-file actions.
///
/// # File Action Filtering
/// - Keeps only the first occurrence of each unique (path, dvId) pair
/// - Excludes expired tombstone remove actions (where deletionTimestamp ≤ minimumFileRetentionTimestamp)
///
/// # Non-File Action Filtering
/// - Keeps only the first protocol action
/// - Keeps only the first metadata action
/// - Keeps only the first transaction action for each unique app ID
///
/// This filtered set of actions represents the minimal set needed to reconstruct
/// the latest valid state of the table.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct CheckpointVisitor<'seen> {
    // File actions deduplication state
    deduplicator: FileActionDeduplicator<'seen>,
    total_file_actions: usize,
    total_add_actions: usize,
    minimum_file_retention_timestamp: i64,

    // Non-file actions deduplication state
    seen_protocol: bool,
    seen_metadata: bool,
    seen_txns: &'seen mut HashSet<String>,
    total_non_file_actions: usize,
}

#[allow(unused)]
impl CheckpointVisitor<'_> {
    /// Create a new CheckpointVisitor
    fn new<'seen>(
        seen_file_keys: &'seen mut HashSet<FileActionKey>,
        seen_txns: &'seen mut HashSet<String>,
        selection_vector: Vec<bool>,
        is_log_batch: bool,
        minimum_file_retention_timestamp: i64,
    ) -> CheckpointVisitor<'seen> {
        CheckpointVisitor {
            deduplicator: FileActionDeduplicator::new(
                seen_file_keys,
                selection_vector,
                is_log_batch,
            ),
            total_file_actions: 0,
            total_add_actions: 0,
            minimum_file_retention_timestamp,

            seen_protocol: false,
            seen_metadata: false,
            seen_txns,
            total_non_file_actions: 0,
        }
    }

    /// A remove action includes a timestamp indicating when the deletion occurred. Physical files  
    /// are deleted lazily after a user-defined expiration time, allowing concurrent readers to  
    /// access stale snapshots. A remove action remains as a tombstone in a checkpoint file until
    /// it expires, which happens when the current time exceeds the removal timestamp plus the
    /// expiration threshold.
    fn is_expired_tombstone<'a>(&self, i: usize, getter: &'a dyn GetData<'a>) -> DeltaResult<bool> {
        // Ideally this should never be zero, but we are following the same behavior as Delta
        // Spark and the Java Kernel.
        let mut deletion_timestamp: i64 = 0;
        if let Some(ts) = getter.get_opt(i, "remove.deletionTimestamp")? {
            deletion_timestamp = ts;
        }

        Ok(deletion_timestamp <= self.minimum_file_retention_timestamp)
    }

    /// Returns true if the row contains a valid file action to be included in the checkpoint.
    fn is_valid_file_action<'a>(
        &mut self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<bool> {
        // Extract file action key and determine if it's an add operation
        let Some((file_key, is_add)) = self.deduplicator.extract_file_action(
            i,
            getters,
            // Do not skip remove actions (even if we're processing a log batch)
            FileActionExtractConfig::new(0, 4, 1, 6, false),
        )?
        else {
            return Ok(false);
        };

        if self.deduplicator.check_and_record_seen(file_key) {
            return Ok(false);
        }

        // Ignore expired tombstones. The getter at the fifth index is the remove action's deletionTimestamp.
        if !is_add && self.is_expired_tombstone(i, getters[5])? {
            return Ok(false);
        }

        if is_add {
            self.total_add_actions += 1;
        }

        self.total_file_actions += 1;
        Ok(true)
    }

    /// Returns true if the row contains a protocol action, and we haven't seen one yet.
    fn is_valid_protocol_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        if getter.get_int(i, "protocol.minReaderVersion")?.is_some() && !self.seen_protocol {
            self.seen_protocol = true;
            self.total_non_file_actions += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if the row contains a metadata action, and we haven't seen one yet.
    fn is_valid_metadata_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        if getter.get_str(i, "metaData.id")?.is_some() && !self.seen_metadata {
            self.seen_metadata = true;
            self.total_non_file_actions += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if the row contains a txn action with an appId that we haven't seen yet.
    fn is_valid_txn_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        let app_id = match getter.get_str(i, "txn.appId")? {
            Some(id) => id,
            None => return Ok(false),
        };

        // Attempting to insert the app_id into the set. If it's already present, the insert will
        // return false, indicating that we've already seen this app_id.
        if self.seen_txns.insert(app_id.to_string()) {
            self.total_non_file_actions += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl RowVisitor for CheckpointVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // The data columns visited must be in the following order:
        // 1. ADD
        // 2. REMOVE
        // 3. METADATA
        // 4. PROTOCOL
        // 5. TXN
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            const STRING: DataType = DataType::STRING;
            const INTEGER: DataType = DataType::INTEGER;
            let types_and_names = vec![
                // File action columns
                (STRING, column_name!("add.path")),
                (STRING, column_name!("add.deletionVector.storageType")),
                (STRING, column_name!("add.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("add.deletionVector.offset")),
                (STRING, column_name!("remove.path")),
                (DataType::LONG, column_name!("remove.deletionTimestamp")),
                (STRING, column_name!("remove.deletionVector.storageType")),
                (STRING, column_name!("remove.deletionVector.pathOrInlineDv")),
                (INTEGER, column_name!("remove.deletionVector.offset")),
                // Non-file action columns
                (STRING, column_name!("metaData.id")),
                (INTEGER, column_name!("protocol.minReaderVersion")),
                (STRING, column_name!("txn.appId")),
            ];
            let (types, names) = types_and_names.into_iter().unzip();
            (names, types).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 12,
            Error::InternalError(format!(
                "Wrong number of visitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            // Check for non-file actions (metadata, protocol, txn)
            let is_non_file_action = self.is_valid_metadata_action(i, getters[9])?
                || self.is_valid_protocol_action(i, getters[10])?
                || self.is_valid_txn_action(i, getters[11])?;

            // Check for file actions (add, remove)
            let is_file_action = self.is_valid_file_action(i, getters)?;

            // Mark the row for selection if it's either a valid non-file or file action
            if is_non_file_action || is_file_action {
                self.deduplicator.selection_vector_mut()[i] = true;
            }
        }
        Ok(())
    }
}

/// This struct contains indices and configuration options needed to
/// extract file actions from action batches in the Delta log.
pub(crate) struct FileActionExtractConfig {
    /// Index of the getter containing the add.path column
    pub add_path_index: usize,
    /// Index of the getter containing the remove.path column
    pub remove_path_index: usize,
    /// Starting index for add action deletion vector columns
    pub add_dv_start_index: usize,
    /// Starting index for remove action deletion vector columns
    pub remove_dv_start_index: usize,
    /// Whether to skip remove actions when extracting file actions
    pub skip_removes: bool,
}

impl FileActionExtractConfig {
    pub(crate) fn new(
        add_path_index: usize,
        remove_path_index: usize,
        add_dv_start_index: usize,
        remove_dv_start_index: usize,
        skip_removes: bool,
    ) -> Self {
        Self {
            add_path_index,
            remove_path_index,
            add_dv_start_index,
            remove_dv_start_index,
            skip_removes,
        }
    }
}

/// Core implementation for deduplicating file actions in Delta log replay
/// This struct extracts the common functionality from the CheckpointVisitor
/// and the ScanDataVisitor.
pub(crate) struct FileActionDeduplicator<'seen> {
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log for deduplication
    seen_file_keys: &'seen mut HashSet<FileActionKey>,
    /// Selection vector to track which rows should be included
    selection_vector: Vec<bool>,
    /// Whether we're processing a log batch (as opposed to a checkpoint)
    is_log_batch: bool,
}

impl<'seen> FileActionDeduplicator<'seen> {
    pub(crate) fn new(
        seen_file_keys: &'seen mut HashSet<FileActionKey>,
        selection_vector: Vec<bool>,
        is_log_batch: bool,
    ) -> Self {
        Self {
            seen_file_keys,
            selection_vector,
            is_log_batch,
        }
    }

    /// Checks if log replay already processed this logical file (in which case the current action
    /// should be ignored). If not already seen, register it so we can recognize future duplicates.
    /// Returns `true` if we have seen the file and should ignore it, `false` if we have not seen it
    /// and should process it.
    pub(crate) fn check_and_record_seen(&mut self, key: FileActionKey) -> bool {
        // Note: each (add.path + add.dv_unique_id()) pair has a
        // unique Add + Remove pair in the log. For example:
        // https://github.com/delta-io/delta/blob/master/spark/src/test/resources/delta/table-with-dv-large/_delta_log/00000000000000000001.json

        if self.seen_file_keys.contains(&key) {
            debug!(
                "Ignoring duplicate ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            true
        } else {
            debug!(
                "Including ({}, {:?}) in scan, is log {}",
                key.path, key.dv_unique_id, self.is_log_batch
            );
            if self.is_log_batch {
                // Remember file actions from this batch so we can ignore duplicates as we process
                // batches from older commit and/or checkpoint files. We don't track checkpoint
                // batches because they are already the oldest actions and never replace anything.
                self.seen_file_keys.insert(key);
            }
            false
        }
    }

    /// Extract deletion vector unique ID
    fn extract_dv_unique_id<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        add_dv_start_index: Option<usize>,
        remove_dv_start_index: Option<usize>,
    ) -> DeltaResult<Option<String>> {
        // Get the starting index based on action type
        let start_idx = add_dv_start_index
            .or(remove_dv_start_index)
            .ok_or_else(|| Error::GenericError {
                source: "starting indices for add/remove DVs should have been passed".into(),
            })?;

        // Extract the DV unique ID
        match getters[start_idx].get_opt(i, "deletionVector.storageType")? {
            Some(storage_type) => Ok(Some(DeletionVectorDescriptor::unique_id_from_parts(
                storage_type,
                getters[start_idx + 1].get(i, "deletionVector.pathOrInlineDv")?,
                getters[start_idx + 2].get_opt(i, "deletionVector.offset")?,
            ))),
            None => Ok(None),
        }
    }

    /// Extracts a file action key and determines if it's an add operation.
    ///
    /// This method examines the data at the given index using the provided getters and config
    /// to identify whether a file action exists and what type it is.
    ///
    /// # Arguments
    ///
    /// * `i` - Index position in the data structure to examine
    /// * `getters` - Collection of data getter implementations used to access the data
    /// * `config` - Configuration specifying where to find add/remove operations
    ///
    /// # Returns
    ///
    /// * `Ok(Some((key, is_add)))` - When a file action is found, returns the key and whether it's an add operation
    /// * `Ok(None)` - When no file action is found
    /// * `Err(...)` - On any error during extraction
    pub(crate) fn extract_file_action<'a>(
        &self,
        i: usize,
        getters: &[&'a dyn GetData<'a>],
        config: FileActionExtractConfig,
    ) -> DeltaResult<Option<(FileActionKey, bool)>> {
        // Try to extract an add action path
        if let Some(path) = getters[config.add_path_index].get_str(i, "add.path")? {
            let dv_unique_id =
                self.extract_dv_unique_id(i, getters, Some(config.add_dv_start_index), None)?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), true)));
        }

        // The AddRemoveDedupVisitor skips remove actions when extracting file actions from a checkpoint file.
        if config.skip_removes {
            return Ok(None);
        }

        // Try to extract a remove action path
        if let Some(path) = getters[config.remove_path_index].get_str(i, "remove.path")? {
            let dv_unique_id =
                self.extract_dv_unique_id(i, getters, None, Some(config.remove_dv_start_index))?;
            return Ok(Some((FileActionKey::new(path, dv_unique_id), false)));
        }

        // No file action found
        Ok(None)
    }

    pub(crate) fn selection_vector(self) -> Vec<bool> {
        self.selection_vector
    }

    pub(crate) fn selection_vector_ref(&self) -> &Vec<bool> {
        &self.selection_vector
    }

    pub(crate) fn selection_vector_mut(&mut self) -> &mut Vec<bool> {
        &mut self.selection_vector
    }

    /// Returns whether we are currently processing a log batch.
    pub(crate) fn is_log_batch(&self) -> bool {
        self.is_log_batch
    }
}

/// A visitor that selects non-file actions for a checkpoint file. Since log replay visits actions
/// in newest-first order, we only keep the first occurrence of:
/// - a protocol action,
/// - a metadata action,
/// - a transaction (txn) action for a given app ID.
///
/// Any subsequent (older) actions of the same type are ignored. This visitor tracks which actions
/// have been seen and includes only the first occurrence of each in the selection vector.
#[cfg_attr(feature = "developer-visibility", visibility::make(pub))]
pub(crate) struct CheckpointNonFileActionsVisitor<'seen> {
    // Non-file actions state
    pub(crate) seen_protocol: bool,
    pub(crate) seen_metadata: bool,
    pub(crate) seen_txns: &'seen mut HashSet<String>,
    pub(crate) selection_vector: Vec<bool>,
    pub(crate) total_actions: usize,
}

#[allow(unused)] // TODO: Remove flag once used for checkpoint writing
impl CheckpointNonFileActionsVisitor<'_> {
    /// Returns true if the row contains a protocol action, and we haven’t seen one yet.
    fn is_valid_protocol_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        if getter.get_int(i, "protocol.minReaderVersion")?.is_some() && !self.seen_protocol {
            self.seen_protocol = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if the row contains a metadata action, and we haven’t seen one yet.
    fn is_valid_metadata_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        if getter.get_str(i, "metaData.id")?.is_some() && !self.seen_metadata {
            self.seen_metadata = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Returns true if the row contains a txn action with an appId that we haven’t seen yet.
    fn is_valid_txn_action<'a>(
        &mut self,
        i: usize,
        getter: &'a dyn GetData<'a>,
    ) -> DeltaResult<bool> {
        let app_id = match getter.get_str(i, "txn.appId")? {
            Some(id) => id,
            None => return Ok(false),
        };

        Ok(self.seen_txns.insert(app_id.to_string()))
    }
}

impl RowVisitor for CheckpointNonFileActionsVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        // The data columns visited must be in the following order:
        // 1. METADATA
        // 2. PROTOCOL
        // 3. TXN
        static CHECKPOINT_NON_FILE_ACTION_COLUMNS: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| {
                const STRING: DataType = DataType::STRING;
                const INTEGER: DataType = DataType::INTEGER;
                let types_and_names = vec![
                    (STRING, column_name!("metaData.id")),
                    (INTEGER, column_name!("protocol.minReaderVersion")),
                    (STRING, column_name!("txn.appId")),
                ];
                let (types, names) = types_and_names.into_iter().unzip();
                (names, types).into()
            });
        CHECKPOINT_NON_FILE_ACTION_COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require!(
            getters.len() == 3,
            Error::InternalError(format!(
                "Wrong number of visitor getters: {}",
                getters.len()
            ))
        );

        for i in 0..row_count {
            let should_select = self.is_valid_metadata_action(i, getters[0])?
                || self.is_valid_protocol_action(i, getters[1])?
                || self.is_valid_txn_action(i, getters[2])?;

            if should_select {
                self.selection_vector[i] = true;
                self.total_actions += 1;
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
    if let Some(storage_type) =
        getters[0].get_opt(row_index, "remove.deletionVector.storageType")?
    {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::{
        actions::get_log_schema, engine::arrow_data::ArrowEngineData, engine::sync::SyncEngine,
        Engine, EngineData,
    };

    // TODO(nick): Merge all copies of this into one "test utils" thing
    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    fn action_batch() -> Box<dyn EngineData> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"remove":{"path":"part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","deletionTimestamp":1670892998135,"dataChange":true,"partitionValues":{"c1":"4","c2":"c"},"size":452}}"#, 
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none", "delta.enableChangeDataFeed":"true"},"createdTime":1677811175819}}"#,
            r#"{"cdc":{"path":"_change_data/age=21/cdc-00000-93f7fceb-281a-446a-b221-07b88132d203.c000.snappy.parquet","partitionValues":{"age":"21"},"size":1033,"dataChange":false}}"#,
            r#"{"sidecar":{"path":"016ae953-37a9-438e-8683-9a9a4a79a395.parquet","sizeInBytes":9268,"modificationTime":1714496113961,"tags":{"tag_foo":"tag_bar"}}}"#,
            r#"{"txn":{"appId":"myApp","version": 3}}"#,
        ]
        .into();
        parse_json_batch(json_strings)
    }

    fn parse_json_batch(json_strings: StringArray) -> Box<dyn EngineData> {
        let engine = SyncEngine::new();
        let json_handler = engine.get_json_handler();
        let output_schema = get_log_schema().clone();
        json_handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap()
    }

    #[test]
    fn test_parse_protocol() -> DeltaResult<()> {
        let data = action_batch();
        let parsed = Protocol::try_new_from_data(data.as_ref())?.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
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
    fn test_checkpoint_visitor() -> DeltaResult<()> {
        let data = action_batch();
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            &mut seen_txns,
            vec![false; 8],
            true,
            0, // minimum_file_retention_timestamp (no expired tombstones)
        );

        visitor.visit_rows_of(data.as_ref())?;

        // Combined results from both file and non-file actions
        // Row 0 is an add action
        // Row 1 is a remove action
        // Row 3 is a protocol action
        // Row 4 is a metadata action
        // Row 7 is a txn action
        let expected = vec![true, true, false, true, true, false, false, true];

        // Verify file action results
        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 1);

        // Verify non-file action results
        assert!(visitor.seen_protocol);
        assert!(visitor.seen_metadata);
        assert_eq!(visitor.seen_txns.len(), 1);
        assert_eq!(visitor.total_non_file_actions, 3);

        assert_eq!(visitor.deduplicator.selection_vector, expected);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_boundary_cases_for_tombstone_expiration() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"remove":{"path":"exactly_at_threshold","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"remove":{"path":"one_below_threshold","deletionTimestamp":99,"dataChange":true,"partitionValues":{}}}"#,
            r#"{"remove":{"path":"one_above_threshold","deletionTimestamp":101,"dataChange":true,"partitionValues":{}}}"#,
            // Missing timestamp defaults to 0
            r#"{"remove":{"path":"missing_timestamp","dataChange":true,"partitionValues":{}}}"#, 
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            &mut seen_txns,
            vec![false; 4],
            true,
            100, // minimum_file_retention_timestamp (threshold set to 100)
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Only "one_above_threshold" should be kept
        let expected = vec![false, false, true, false];
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 0);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_conflicting_file_actions_in_log_batch() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
             // Duplicate path
            r#"{"remove":{"path":"file1","deletionTimestamp":100,"dataChange":true,"partitionValues":{}}}"#,
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor =
            CheckpointVisitor::new(&mut seen_file_keys, &mut seen_txns, vec![false; 2], true, 0);

        visitor.visit_rows_of(batch.as_ref())?;

        // First one should be included, second one skipped as a duplicate
        let expected = vec![true, false];
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 1);
        assert_eq!(visitor.total_add_actions, 1);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_duplicate_file_actions_in_checkpoint_batch() -> DeltaResult<()> {
        // Note: this is NOT a valid checkpoint batch since it contains duplicate file actions!
        // However, we should still be able to parse it without errors, and the duplicates should be included.
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#,
            // Duplicate path
            r#"{"add":{"path":"file1","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true}}"#, 
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            &mut seen_txns,
            vec![false; 2],
            false, // is_log_batch = false (checkpoint batch)
            0,
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // Both should be included since we don't track duplicates in checkpoint batches
        let expected = vec![true, true];
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 2);
        assert_eq!(visitor.total_non_file_actions, 0);
        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_with_deletion_vectors() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#,
            // Same path but different DV
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"two","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#, 
            // Duplicate of first entry
            r#"{"add":{"path":"file1","partitionValues":{},"size":635,"modificationTime":100,"dataChange":true,"deletionVector":{"storageType":"one","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}}}"#, 
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor =
            CheckpointVisitor::new(&mut seen_file_keys, &mut seen_txns, vec![false; 3], true, 0);

        visitor.visit_rows_of(batch.as_ref())?;

        let expected = vec![true, true, false]; // Third one is a duplicate
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert_eq!(visitor.total_file_actions, 2);
        assert_eq!(visitor.total_add_actions, 2);
        assert_eq!(visitor.total_non_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
        ].into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor =
            CheckpointVisitor::new(&mut seen_file_keys, &mut seen_txns, vec![false; 3], true, 0);

        visitor.visit_rows_of(batch.as_ref())?;

        let expected = vec![true, true, true];
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert!(visitor.seen_protocol);
        assert!(visitor.seen_metadata);
        assert_eq!(visitor.seen_txns.len(), 1);
        assert_eq!(visitor.total_non_file_actions, 3);
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_already_seen_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
        ].into();
        let batch = parse_json_batch(json_strings);

        // Pre-populate with txn app1
        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        seen_txns.insert("app1".to_string());

        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            &mut seen_txns, // Pre-populated transaction
            vec![false; 3],
            true,
            0,
        );

        // Mark these as already seen
        visitor.seen_protocol = true;
        visitor.seen_metadata = true;

        visitor.visit_rows_of(batch.as_ref())?;

        // All actions should be skipped as they have already been seen
        let expected = vec![false, false, false];
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert_eq!(visitor.total_non_file_actions, 0);
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }

    #[test]
    fn test_checkpoint_visitor_duplicate_non_file_actions() -> DeltaResult<()> {
        let json_strings: StringArray = vec![
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#,
            r#"{"txn":{"appId":"app1","version":1,"lastUpdated":123456789}}"#, // Duplicate txn
            r#"{"txn":{"appId":"app2","version":1,"lastUpdated":123456789}}"#, // Different app ID
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7}}"#, // Duplicate protocol
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#,
            // Duplicate metadata
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1677811175819}}"#, 
        ]
        .into();
        let batch = parse_json_batch(json_strings);

        let mut seen_file_keys = HashSet::new();
        let mut seen_txns = HashSet::new();
        let mut visitor = CheckpointVisitor::new(
            &mut seen_file_keys,
            &mut seen_txns,
            vec![false; 7],
            true, // is_log_batch
            0,    // minimum_file_retention_timestamp
        );

        visitor.visit_rows_of(batch.as_ref())?;

        // First occurrence of each type should be included
        let expected = vec![true, false, true, true, false, true, false];
        assert_eq!(visitor.deduplicator.selection_vector, expected);
        assert_eq!(visitor.seen_txns.len(), 2); // Two different app IDs
        assert_eq!(visitor.total_non_file_actions, 4); // 2 txns + 1 protocol + 1 metadata
        assert_eq!(visitor.total_file_actions, 0);

        Ok(())
    }
}
