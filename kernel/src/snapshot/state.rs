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
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::log_path::LogPath;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Snapshot, Version};

/// Immutable components of a snapshot, independently backed by Rust or a connector.
///
/// Implementations must return the same state for every call. In particular, the version,
/// protocol, metadata, schema, and log paths must describe one validated snapshot generation.
#[internal_api]
pub(crate) trait SnapshotState {
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
