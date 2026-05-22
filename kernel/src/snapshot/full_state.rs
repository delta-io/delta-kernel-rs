//! Declarative full snapshot read (FSR) entry points on [`Snapshot`].

use std::sync::Arc;

use crate::plans::operations::scan::{FullState, FullStateBuilder};
use crate::scan::ScanBuilder as KernelScanBuilder;
use crate::snapshot::SnapshotRef;
use crate::Snapshot;

impl Snapshot {
    /// Create a canonical FSR plan builder rooted at this snapshot.
    pub fn full_state_builder(self: &SnapshotRef) -> FullStateBuilder {
        FullState::for_table(Arc::clone(self))
    }

    /// Create a split-phase scan replay plan builder rooted at this snapshot.
    pub fn scan_replay_builder(self: &SnapshotRef) -> KernelScanBuilder {
        KernelScanBuilder::new(Arc::clone(self))
    }
}
