//! A scoped connector implementation of kernel snapshot component access.

use std::sync::Arc;

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::crc::Crc;
use delta_kernel::last_checkpoint_hint::LastCheckpointHint;
use delta_kernel::schema::SchemaRef;
use delta_kernel::snapshot::{SnapshotLogState, SnapshotScanState, SnapshotState};
use delta_kernel::{DeltaResult, LogPath, Version};
use url::Url;

use super::{invalid_crc, invalid_with_source, FfiSnapshotHint, FfiSnapshotScanState};

/// The connector owns every pointer reachable from `hint` for this call only.
pub(super) struct BorrowedSnapshotState<'a> {
    pub hint: &'a FfiSnapshotHint,
    pub table_root: &'a Url,
}

impl SnapshotLogState for BorrowedSnapshotState<'_> {
    fn table_root(&self) -> &Url {
        self.table_root
    }

    fn version(&self) -> Version {
        self.hint.version
    }

    fn is_latest(&self) -> bool {
        matches!(self.hint.freshness, super::FfiSnapshotHintFreshness::Latest)
    }

    fn last_checkpoint(&self) -> DeltaResult<Option<LastCheckpointHint>> {
        unsafe { self.hint.last_checkpoint.as_ref() }
            .map(|checkpoint| unsafe { checkpoint.try_to_kernel() })
            .transpose()
            .map_err(|source| invalid_with_source("supplied _last_checkpoint is invalid", source))
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

impl SnapshotScanState for BorrowedSnapshotState<'_> {
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
}

impl SnapshotState for BorrowedSnapshotState<'_> {
    fn crc(&self) -> DeltaResult<Option<Arc<Crc>>> {
        unsafe { self.hint.crc.as_ref() }
            .map(|crc| unsafe { crc.try_to_kernel() })
            .map(|result| result.map_err(invalid_crc))
            .transpose()
            .map(|crc| crc.map(Arc::new))
    }
}

/// The connector owns every pointer reachable from `value` for this scan-planning call only.
pub(super) struct BorrowedSnapshotScanState<'a> {
    pub value: &'a FfiSnapshotScanState,
    pub table_root: &'a Url,
}

impl SnapshotLogState for BorrowedSnapshotScanState<'_> {
    fn ordered_log_paths(
        &self,
    ) -> DeltaResult<Option<delta_kernel::snapshot::SnapshotLogPathIterator<'_>>> {
        let Some(source) = (unsafe { self.value.log_path_source.as_ref() }) else {
            return Ok(None);
        };
        if self.value.log_paths.len != 0 {
            return Err(super::invalid(
                "Supply either a log path array or a batch source",
            ));
        }
        let mut offset = 0usize;
        let mut batch = Vec::new().into_iter();
        let mut done = false;
        Ok(Some(Box::new(std::iter::from_fn(move || {
            if let Some(path) = batch.next() {
                return Some(Ok(path));
            }
            if done {
                return None;
            }
            let mut output = crate::log_path::LogPathArray::empty();
            let result = (|| {
                if !unsafe { (source.read_batch)(source.context, offset, 256, 65536, &mut output) }
                {
                    return Err(super::invalid("Connector log path batch failed"));
                }
                if output.len > 256 {
                    return Err(super::invalid(
                        "Connector exceeded the log path batch entry limit",
                    ));
                }
                let paths = unsafe { output.log_paths() }?;
                offset = offset
                    .checked_add(paths.len())
                    .ok_or_else(|| super::invalid("Log path count overflow"))?;
                Ok(paths)
            })();
            match result {
                Ok(paths) => {
                    batch = paths.into_iter();
                    let next = batch.next();
                    done = next.is_none();
                    next.map(Ok)
                }
                Err(error) => {
                    done = true;
                    Some(Err(error))
                }
            }
        }))))
    }

    fn table_root(&self) -> &Url {
        self.table_root
    }

    fn version(&self) -> Version {
        self.value.version
    }

    fn is_latest(&self) -> bool {
        matches!(
            self.value.freshness,
            super::FfiSnapshotHintFreshness::Latest
        )
    }

    fn last_checkpoint(&self) -> DeltaResult<Option<LastCheckpointHint>> {
        unsafe { self.value.last_checkpoint.as_ref() }
            .map(|checkpoint| unsafe { checkpoint.try_to_kernel() })
            .transpose()
            .map_err(|source| invalid_with_source("supplied _last_checkpoint is invalid", source))
    }

    fn visit_log_paths(
        &self,
        visitor: &mut dyn FnMut(&[LogPath]) -> DeltaResult<()>,
    ) -> DeltaResult<()> {
        let paths = unsafe { self.value.log_paths.log_paths() }
            .map_err(|source| invalid_with_source("supplied log paths are invalid", source))?;
        for batch in paths.chunks(256) {
            visitor(batch)?;
        }
        Ok(())
    }
}

impl SnapshotScanState for BorrowedSnapshotScanState<'_> {
    fn protocol(&self) -> DeltaResult<Protocol> {
        unsafe { self.value.protocol.try_to_kernel() }
            .map_err(|source| invalid_with_source("supplied protocol is invalid", source))
    }

    fn metadata(&self) -> DeltaResult<Metadata> {
        unsafe { self.value.metadata.try_to_kernel() }
            .map_err(|source| invalid_with_source("supplied metadata is invalid", source))
    }

    fn logical_schema(&self) -> DeltaResult<SchemaRef> {
        let metadata = self.metadata()?;
        metadata.parse_schema().map(Arc::new)
    }
}
