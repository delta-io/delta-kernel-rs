//! Layout and metadata for the checkpoint selected by a snapshot.

use std::sync::Arc;

use crate::FileMeta;
use crate::action_reconciliation::checkpoint_shape::CheckpointType;
use crate::last_checkpoint_hint::LastCheckpointHint;
use crate::schema::SchemaRef;

/// Where a checkpoint stores its Add and Remove actions, independently of its naming scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointKind {
    /// File actions are stored in the checkpoint parts themselves.
    Inline,
    /// File actions are stored in sidecars referenced by the checkpoint.
    Manifest,
}

/// Checkpoint layout and portable metadata used by scans and snapshot metadata access.
///
/// Scan discovery fills only the layout facts it needs. A successful
/// [`crate::snapshot::SnapshotReader::get_checkpoint_info`] resolves the complete sidecar list,
/// leaf schema, and portable hint. This type is not serialized; [`LastCheckpointHint`] describes
/// the portable fields. Layout discovery does not validate every required checkpoint action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointShape {
    pub(crate) checkpoint_type: CheckpointType,
    pub(crate) leaf_checkpoint_schema: Option<SchemaRef>,
    /// Only complete lists may replace manifest discovery during scan execution.
    pub(crate) sidecars: Option<Arc<[FileMeta]>>,
    pub(crate) last_checkpoint: Option<Box<LastCheckpointHint>>,
}

impl CheckpointShape {
    /// Returns where file actions are stored, or `None` when no checkpoint is selected.
    pub fn kind(&self) -> Option<CheckpointKind> {
        match self.checkpoint_type {
            CheckpointType::None => None,
            CheckpointType::Leaf => Some(CheckpointKind::Inline),
            CheckpointType::Manifest => Some(CheckpointKind::Manifest),
        }
    }

    /// Returns the leaf schema when discovery requested it.
    ///
    /// This is the sidecar schema for a manifest and the root schema for an inline checkpoint.
    /// JSON leaves use the canonical action read schema.
    pub fn leaf_schema(&self) -> Option<&SchemaRef> {
        self.leaf_checkpoint_schema.as_ref()
    }

    /// Returns the resolved sidecar count, or part count for an inline checkpoint.
    pub fn leaf_file_count(&self) -> Option<usize> {
        let sidecars = self.sidecars.as_ref()?;
        Some(if sidecars.is_empty() {
            self.last_checkpoint.as_ref()?.parts.unwrap_or(1)
        } else {
            sidecars.len()
        })
    }

    /// Returns the total bytes in the checkpoint parts and all referenced sidecars, when resolved.
    pub fn size_in_bytes(&self) -> Option<i64> {
        self.last_checkpoint.as_ref()?.size_in_bytes
    }

    /// Returns the bounded portable hint produced by complete resolution.
    pub fn last_checkpoint(&self) -> Option<&LastCheckpointHint> {
        self.last_checkpoint.as_deref()
    }
}
