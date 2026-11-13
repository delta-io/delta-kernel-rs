//! Layout and metadata for the checkpoint selected by a snapshot.

use std::sync::Arc;

use super::SelectedCheckpointType;
use crate::FileMeta;
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

/// Complete layout and portable metadata for the checkpoint selected by a snapshot.
///
/// Scan planning and [`crate::snapshot::SnapshotReader::get_checkpoint_info`] share discovery of
/// the checkpoint type, complete sidecar list, leaf schema, and portable hint. This type is not
/// serialized; [`LastCheckpointHint`] describes the portable fields. Layout discovery does not
/// validate every required checkpoint action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub(crate) checkpoint_type: SelectedCheckpointType,
    pub(crate) leaf_checkpoint_schema: SchemaRef,
    pub(crate) sidecars: Arc<[FileMeta]>,
    pub(crate) size_in_bytes: u64,
    pub(crate) last_checkpoint: Box<LastCheckpointHint>,
}

impl CheckpointInfo {
    /// Returns whether the selected checkpoint follows the V1 or V2 specification.
    pub fn checkpoint_type(&self) -> SelectedCheckpointType {
        self.checkpoint_type
    }

    /// Returns where the checkpoint stores its file actions.
    pub fn kind(&self) -> CheckpointKind {
        if self.sidecars.is_empty() {
            CheckpointKind::Inline
        } else {
            CheckpointKind::Manifest
        }
    }

    /// Returns the schema of the files containing file actions.
    ///
    /// This is the sidecar schema for a manifest and the root schema for an inline checkpoint.
    /// JSON leaves use the canonical action read schema.
    pub fn leaf_schema(&self) -> &SchemaRef {
        &self.leaf_checkpoint_schema
    }

    /// Returns the resolved sidecar count, or part count for an inline checkpoint.
    pub fn leaf_file_count(&self) -> usize {
        if self.sidecars.is_empty() {
            self.last_checkpoint.parts.unwrap_or(1)
        } else {
            self.sidecars.len()
        }
    }

    /// Returns the total bytes in the checkpoint parts and all referenced sidecars.
    pub fn size_in_bytes(&self) -> u64 {
        self.size_in_bytes
    }

    /// Returns the bounded portable hint produced by complete resolution.
    pub fn last_checkpoint(&self) -> &LastCheckpointHint {
        &self.last_checkpoint
    }
}
