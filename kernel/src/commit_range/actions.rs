//! [`DeltaAction`]: selects which Delta log action to read.
//! [`CommitAction`]: per-commit handle exposing version, timestamp,
//! and a re-buildable iterator over the commit's action batches.

use std::slice;
use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::{Metadata, Protocol, METADATA_NAME, PROTOCOL_NAME};
use crate::path::ParsedLogPath;
use crate::schema::{SchemaRef, StructField, StructType, ToSchema as _};
use crate::table_configuration::TableConfiguration;
use crate::table_features::{ensure_table_can_be_read, Operation};
use crate::{DeltaResult, Engine, FileDataReadResultIterator, Version};

/// A Delta log action kind.
///
/// Callers that need to read multiple action types pass a slice
/// (e.g. `&[DeltaAction::Add, DeltaAction::Remove]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeltaAction {
    Add,
    Remove,
    Metadata,
    Protocol,
    CommitInfo,
    Cdc,
}

/// Read schema for extracting protocol+metadata from a single commit JSON.
static PM_READ_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
    ]))
});

/// Per-commit handle returned by [`super::CommitRange::commits`].
///
/// Carries the commit's version, timestamp, and the effective (extracted from this
/// commit overlaid onto the iterator's accumulated state) `Protocol` / `Metadata`.
/// Reading the commit's action batches is lazy and re-buildable via
/// [`Self::get_actions`], which issues a fresh JSON read on every call.
pub struct CommitAction {
    engine: Arc<dyn Engine>,
    table_root: Url,
    log_path: ParsedLogPath,
    read_schema: SchemaRef,
    protocol: Option<Protocol>,
    metadata: Option<Metadata>,
}

impl CommitAction {
    /// Construct a [`CommitAction`] for one commit JSON file. The `protocol` /
    /// `metadata` arguments seed the effective state inherited from prior commits
    /// (or from a start snapshot); they are overlaid by `get_protocol_and_metadata`
    /// when this commit read its own Protocol/Metadata actions.
    pub(crate) fn new(
        engine: Arc<dyn Engine>,
        table_root: Url,
        log_path: ParsedLogPath,
        read_schema: SchemaRef,
        protocol: Option<Protocol>,
        metadata: Option<Metadata>,
    ) -> Self {
        Self {
            engine,
            table_root,
            log_path,
            read_schema,
            protocol,
            metadata,
        }
    }

    /// Commit version of this commit.
    pub fn version(&self) -> Version {
        self.log_path.version
    }

    /// Commit timestamp in milliseconds since epoch -- currently the commit file's
    /// `last_modified` time; ICT-enabled tables are not yet supported.
    pub fn timestamp(&self) -> i64 {
        self.log_path.location.last_modified
    }

    /// Reads the commit JSON projected to `[protocol, metadata]`, extracts both
    /// actions (if present), and overlays them onto `self`. Overlay semantics: a
    /// `None` extraction does NOT clear the inherited value, so callers can rely
    /// on `self.protocol` / `self.metadata` reflecting the effective state at
    /// this commit after this call returns.
    ///
    /// Returns the extracted (not effective) values, so the outer iterator can
    /// advance its own accumulated `latest_protocol` / `latest_metadata`.
    pub(crate) fn get_protocol_and_metadata(
        &mut self,
    ) -> DeltaResult<(Option<Protocol>, Option<Metadata>)> {
        let json_iter = self.engine.json_handler().read_json_files(
            slice::from_ref(&self.log_path.location),
            PM_READ_SCHEMA.clone(),
            None,
        )?;

        let mut extracted_protocol: Option<Protocol> = None;
        let mut extracted_metadata: Option<Metadata> = None;
        for batch_res in json_iter {
            let batch = batch_res?;
            if extracted_protocol.is_none() {
                extracted_protocol = Protocol::try_new_from_data(batch.as_ref())?;
            }
            if extracted_metadata.is_none() {
                extracted_metadata = Metadata::try_new_from_data(batch.as_ref())?;
            }
            if extracted_protocol.is_some() && extracted_metadata.is_some() {
                break;
            }
        }

        if let Some(p) = &extracted_protocol {
            self.protocol = Some(p.clone());
        }
        if let Some(m) = &extracted_metadata {
            self.metadata = Some(m.clone());
        }
        Ok((extracted_protocol, extracted_metadata))
    }

    /// Validate that the kernel can read the given commit
    /// based on the extracted protocol and metadata.
    pub(crate) fn protocol_validation(&self) -> DeltaResult<()> {
        match (&self.protocol, &self.metadata) {
            (Some(p), Some(m)) => {
                let table_config = TableConfiguration::try_new(
                    m.clone(),
                    p.clone(),
                    self.table_root.clone(),
                    self.log_path.version,
                )?;
                table_config.ensure_operation_supported(Operation::Scan)
            }
            (Some(p), None) => ensure_table_can_be_read(p),
            (None, None) => Ok(()),
            (None, Some(_)) => Ok(()),
        }
    }

    /// Return an iterator over the commit's action batches projected to the
    /// caller-requested `read_schema`.
    pub fn get_actions(&self) -> DeltaResult<FileDataReadResultIterator> {
        self.engine.json_handler().read_json_files(
            slice::from_ref(&self.log_path.location),
            self.read_schema.clone(),
            None,
        )
    }
}
