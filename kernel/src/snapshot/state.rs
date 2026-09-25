//! Component access for immutable snapshot state.
//!
//! An FFI connector may keep large components outside Rust. Accessors return owned values so
//! no borrow from connector memory can outlive one FFI call. Log paths are delivered in batches
//! to avoid a callback for each file.

use std::sync::Arc;

use delta_kernel_derive::internal_api;
use url::Url;

use crate::actions::{Metadata, Protocol};
use crate::crc::Crc;
use crate::error::SnapshotHintError;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_path::LogPath;
use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::utils::require;
use crate::{DeltaResult, Snapshot, Version};

/// Immutable components of a snapshot, independently backed by Rust or a connector.
///
/// Implementations must return the same state for every call. In particular, the version,
/// protocol, metadata, schema, and log paths must describe one validated snapshot generation.
pub trait SnapshotState {
    fn table_root(&self) -> &Url;
    fn version(&self) -> Version;
    fn is_latest(&self) -> bool;
    fn protocol(&self) -> DeltaResult<Protocol>;
    fn metadata(&self) -> DeltaResult<Metadata>;
    fn logical_schema(&self) -> DeltaResult<SchemaRef>;
    fn last_checkpoint(&self) -> DeltaResult<Option<LastCheckpointHint>>;
    fn crc(&self) -> DeltaResult<Option<Arc<Crc>>>;
    fn visit_log_paths(
        &self,
        visitor: &mut dyn FnMut(&[LogPath]) -> DeltaResult<()>,
    ) -> DeltaResult<()>;
}

impl SnapshotState for Snapshot {
    fn table_root(&self) -> &Url {
        self.table_root()
    }

    fn version(&self) -> Version {
        self.version()
    }

    fn is_latest(&self) -> bool {
        self.is_built_as_latest()
    }

    fn protocol(&self) -> DeltaResult<Protocol> {
        Ok(self.table_configuration().protocol().clone())
    }

    fn metadata(&self) -> DeltaResult<Metadata> {
        Ok(self.table_configuration().metadata().clone())
    }

    fn logical_schema(&self) -> DeltaResult<SchemaRef> {
        Ok(self.schema())
    }

    fn last_checkpoint(&self) -> DeltaResult<Option<LastCheckpointHint>> {
        Ok(self.log_segment().last_checkpoint_metadata.clone())
    }

    fn crc(&self) -> DeltaResult<Option<Arc<Crc>>> {
        Ok(self.base_crc().cloned())
    }

    fn visit_log_paths(
        &self,
        visitor: &mut dyn FnMut(&[LogPath]) -> DeltaResult<()>,
    ) -> DeltaResult<()> {
        let mut batch = Vec::with_capacity(256);
        for path in self.log_segment().listed.iter_all_paths() {
            batch.push(LogPath::from(path.clone()));
            if batch.len() == 256 {
                visitor(&batch)?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            visitor(&batch)?;
        }
        Ok(())
    }
}

impl Snapshot {
    /// Validate connector state against this snapshot without constructing another snapshot.
    /// Each component is read and released before the next is requested. Log paths are grouped
    /// using the same rules as snapshot-hint construction before comparing log segments.
    #[internal_api]
    pub(crate) fn matches_state(&self, state: &dyn SnapshotState) -> DeltaResult<bool> {
        if self.table_root() != state.table_root()
            || self.version() != state.version()
            || self.is_built_as_latest() != state.is_latest()
            || *self.table_configuration().protocol() != state.protocol()?
            || *self.table_configuration().metadata() != state.metadata()?
        {
            return Ok(false);
        }

        let crc = state.crc()?;
        if self.base_crc().map(Arc::as_ref) != crc.as_deref() {
            return Ok(false);
        }

        let segment = log_segment_from_state(state)?;
        Ok(segment == *self.log_segment())
    }
}

/// Resolve borrowed log paths into the segment needed by scan planning and handoff validation.
pub(crate) fn log_segment_from_state(state: &dyn SnapshotState) -> DeltaResult<LogSegment> {
    let mut paths: Vec<ParsedLogPath> = Vec::new();
    state.visit_log_paths(&mut |batch| {
        paths.extend(batch.iter().cloned().map(Into::into));
        Ok(())
    })?;
    require!(
        !paths
            .iter()
            .any(|path| matches!(path.file_type, LogPathFileType::CompactedCommit { .. })),
        SnapshotHintError::LogCompaction.into()
    );
    paths.sort_unstable_by(|a, b| (a.version, &a.filename).cmp(&(b.version, &b.filename)));
    let files = LogSegmentFiles::build_log_segment_files(
        paths.into_iter().map(Ok),
        Vec::new(),
        0,
        None,
        CheckpointHandling::Adopt,
    )?;
    Ok(LogSegment::try_new(
        files,
        state.table_root().join("_delta_log/")?,
        Some(state.version()),
        state.last_checkpoint()?,
    )
    .map_err(|source| SnapshotHintError::LogSegment {
        source: Box::new(source),
    })?)
}
