//! A scoped connector implementation of kernel snapshot component access.

use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::crc::Crc;
use delta_kernel::last_checkpoint_hint::LastCheckpointHint;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::SnapshotState;
use delta_kernel::{DeltaResult, LogPath, Version};
use url::Url;

use super::{invalid_crc, invalid_with_source, FfiSnapshotHint};

/// The connector owns every pointer reachable from `hint` for this call only.
pub(super) struct BorrowedSnapshotState<'a> {
    pub hint: &'a FfiSnapshotHint,
    pub table_root: &'a Url,
}

impl SnapshotState for BorrowedSnapshotState<'_> {
    fn table_root(&self) -> &Url {
        self.table_root
    }

    fn version(&self) -> Version {
        self.hint.version
    }

    fn is_latest(&self) -> bool {
        matches!(self.hint.freshness, super::FfiSnapshotHintFreshness::Latest)
    }

    fn protocol(&self) -> DeltaResult<Protocol> {
        unsafe { self.hint.protocol.try_to_kernel() }
            .map_err(|source| invalid_with_source("supplied protocol is invalid", source))
    }

    fn metadata(&self) -> DeltaResult<Metadata> {
        unsafe { self.hint.metadata.try_to_kernel() }
            .map_err(|source| invalid_with_source("supplied metadata is invalid", source))
    }

    fn logical_schema(&self) -> DeltaResult<SchemaRef> {
        let metadata = self.metadata()?;
        metadata.parse_schema().map(Arc::new)
    }

    fn last_checkpoint(&self) -> DeltaResult<Option<LastCheckpointHint>> {
        unsafe { self.hint.last_checkpoint.as_ref() }
            .map(|checkpoint| unsafe { checkpoint.try_to_kernel() })
            .transpose()
            .map_err(|source| invalid_with_source("supplied _last_checkpoint is invalid", source))
    }

    fn crc(&self) -> DeltaResult<Option<Arc<Crc>>> {
        unsafe { self.hint.crc.as_ref() }
            .map(|crc| unsafe { crc.try_to_kernel() })
            .map(|result| result.map_err(invalid_crc))
            .transpose()
            .map(|crc| crc.map(Arc::new))
    }

    fn visit_log_paths(
        &self,
        visitor: &mut dyn FnMut(&[LogPath]) -> DeltaResult<()>,
    ) -> DeltaResult<()> {
        let paths = unsafe { self.hint.log_paths.log_paths() }
            .map_err(|source| invalid_with_source("supplied log paths are invalid", source))?;
        for batch in paths.chunks(256) {
            visitor(batch)?;
        }
        Ok(())
    }
}
