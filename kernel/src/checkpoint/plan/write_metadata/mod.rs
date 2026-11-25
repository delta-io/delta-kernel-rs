//! Complete `_last_checkpoint` metadata for classic single-part checkpoints.

use std::sync::LazyLock;

mod write;

use super::super::{CheckpointWriter, LastCheckpointHintStats, V2CheckpointConfig};
use crate::last_checkpoint_hint::{CheckpointFileStatus, LastCheckpointHint};
use crate::path::{CheckpointInstance, ParsedLogPath};
use crate::schema::{SchemaRef, lazy_schema_ref};
use crate::{FileMeta, KernelError, KernelResult};

const NUM_ACTIONS: &str = "num_actions";
const NUM_ADDS: &str = "num_of_add_files";
const NUM_BYTES: &str = "size_in_bytes";
// Delta-Spark's checkpointSchema.writeThresholdLength default, measured in UTF-16 code units.
const CHECKPOINT_SCHEMA_THRESHOLD: usize = 20_000;

static STATISTICS_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    not_null NUM_ACTIONS: LONG,
    not_null NUM_ADDS: LONG,
    not_null NUM_BYTES: LONG,
};

impl CheckpointWriter {
    fn serialize_checkpoint_hint(
        &self,
        stats: &LastCheckpointHintStats,
        root_file: &FileMeta,
        checkpoint_schema: SchemaRef,
    ) -> KernelResult<Vec<u8>> {
        if stats.num_actions < stats.num_of_add_files
            || stats.num_of_add_files < 0
            || stats.size_in_bytes < 0
        {
            return Err(KernelError::checkpoint_write(
                "Invalid checkpoint statistics",
            ));
        }
        let root = ParsedLogPath::new(root_file.clone())
            .ok_or_else(|| KernelError::checkpoint_write("Invalid checkpoint root file"))?;
        if !matches!(
            CheckpointInstance::of(&root),
            Some(CheckpointInstance::Classic)
        ) || root.version != self.snapshot.version()
            || root_file.location != self.snapshot.log_segment().log_root.join(&root.filename)?
        {
            return Err(KernelError::checkpoint_write(
                "Checkpoint root does not match this writer",
            ));
        }
        let root_size = i64::try_from(root_file.size)
            .map_err(|_| KernelError::checkpoint_write("Checkpoint root size exceeds i64"))?;
        let has_sidecars = self.schema_context.is_v2
            && matches!(
                &self.v2_checkpoint_config,
                V2CheckpointConfig::WithSidecar { .. }
            );
        if stats.size_in_bytes < root_size || (!has_sidecars && stats.size_in_bytes != root_size) {
            return Err(KernelError::checkpoint_write(
                "Checkpoint byte count does not match root file",
            ));
        }
        let hint = LastCheckpointHint {
            version: self.snapshot.version(),
            size: stats.num_actions,
            parts: None,
            size_in_bytes: Some(stats.size_in_bytes),
            num_of_add_files: Some(stats.num_of_add_files),
            checkpoint_schema: checkpoint_schema_json(&checkpoint_schema)?
                .map(|_| checkpoint_schema),
            checkpoint_files: Some(vec![CheckpointFileStatus {
                path: root.filename,
                length: root_size,
                is_dir: false,
                modification_time: root_file.last_modified,
            }]),
            checksum: None,
            tags: None,
            // Classic filenames do not use the UUID checkpoint locator.
            v2_checkpoint: None,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            checkpoint_type: None,
            #[cfg(feature = "adaptive-metadata-in-dev")]
            amt_checkpoint: None,
        };
        Ok(serde_json::to_vec(&hint)?)
    }
}

fn checkpoint_schema_json(schema: &SchemaRef) -> KernelResult<Option<String>> {
    let json = serde_json::to_string(schema)?;
    Ok((json.encode_utf16().count() <= CHECKPOINT_SCHEMA_THRESHOLD).then_some(json))
}

#[cfg(test)]
mod tests;
