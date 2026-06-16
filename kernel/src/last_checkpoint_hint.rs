//! Utities for reading the `_last_checkpoint` file. Maybe this file should instead go under
//! log_segment module since it should only really be used there? as hint for listing?

use std::collections::HashMap;

use delta_kernel_derive::internal_api;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};
use url::Url;

use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, StorageHandler, Version};

/// Name of the _last_checkpoint file that provides metadata about the last checkpoint
/// created for the table. This file is used as a hint for the engine to quickly locate
/// the latest checkpoint without a full directory listing.
const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

/// A reduced view of [`LastCheckpointHint`] that [`crate::log_segment::LogSegment`] retains --
/// just enough to validate and apply the hint without holding every metadata field. If this grows
/// to be similar in size to `LastCheckpointHint`, we should switch to using it directly.
#[derive(Debug, Clone, PartialEq, Eq)]
#[internal_api]
pub(crate) struct LastCheckpointHintSummary {
    /// Version of the latest known checkpoint, at the time the hint file was read.
    pub version: Version,

    /// Schema of the checkpoint file(s), as read from the `_last_checkpoint` hint.
    /// Useful for determining if `stats_parsed` is available for data skipping.
    /// `None` when the hint file did not include a `checkpointSchema` field.
    pub schema: Option<SchemaRef>,

    /// File name of the V2 checkpoint the hint describes (`v2Checkpoint.path`); see
    /// [`LastCheckpointHint::v2_checkpoint`]. `None` for V1 / classic checkpoints.
    pub v2_checkpoint_path: Option<String>,
}

// Note: Schema can not be derived because the checkpoint schema is only known at runtime.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) struct LastCheckpointHint {
    /// The version of the table when the last checkpoint was made.
    #[allow(unreachable_pub)] // used by acceptance tests (TODO make an fn accessor?)
    pub version: Version,
    /// The number of actions that are stored in the checkpoint.
    pub(crate) size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub(crate) parts: Option<usize>,
    /// The number of bytes of the checkpoint.
    pub(crate) size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint.
    pub(crate) num_of_add_files: Option<i64>,
    /// The schema of the checkpoint file.
    pub(crate) checkpoint_schema: Option<SchemaRef>,
    /// The checksum of the last checkpoint JSON.
    pub(crate) checksum: Option<String>,
    /// Additional metadata about the last checkpoint.
    pub(crate) tags: Option<HashMap<String, String>>,
    /// For a V2 checkpoint, the embedded V2 checkpoint info. Identifies the specific checkpoint
    /// file the hint describes. Absent for V1 / classic checkpoints.
    pub(crate) v2_checkpoint: Option<LastCheckpointV2>,
}

/// The `v2Checkpoint` object embedded in a `_last_checkpoint` hint for a V2 checkpoint.
///
/// Only the `path` (the checkpoint file's name) is parsed; it identifies which V2 checkpoint the
/// hint's fields describe, since several V2 checkpoints can share a version.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) struct LastCheckpointV2 {
    /// Bare file name of the V2 checkpoint this hint describes (a `path.getName()`-style value,
    /// not a full path), matched against the selected checkpoint part's file name.
    pub(crate) path: String,
}

impl LastCheckpointHint {
    /// Returns the path of the `_last_checkpoint` file given the log root of a table.
    #[internal_api]
    pub(crate) fn path(log_root: &Url) -> DeltaResult<Url> {
        Ok(log_root.join(LAST_CHECKPOINT_FILE_NAME)?)
    }

    /// Try reading the `_last_checkpoint` file.
    ///
    /// Note that we typically want to ignore a missing/invalid `_last_checkpoint` file without
    /// failing the read. Thus, the semantics of this function are to return `None` if the file is
    /// not found or is invalid JSON. Unexpected/unrecoverable errors are returned as `Err` case and
    /// are assumed to cause failure.
    // TODO(#1047): weird that we propagate FileNotFound as part of the iterator instead of top-
    // level result coming from storage.read_files
    #[instrument(name = "last_checkpoint.read", skip_all, err)]
    pub(crate) fn try_read(
        storage: &dyn StorageHandler,
        log_root: &Url,
    ) -> DeltaResult<Option<LastCheckpointHint>> {
        let file_path = Self::path(log_root)?;
        match storage.read_files(vec![(file_path, None)])?.next() {
            Some(Ok(data)) => {
                let result: Option<LastCheckpointHint> = serde_json::from_slice(&data)
                    .inspect_err(|e| warn!("invalid _last_checkpoint JSON: {e}"))
                    .ok();
                info!(hint = result.as_ref().map(|h| h.summary()));
                Ok(result)
            }
            Some(Err(Error::FileNotFound(_))) => {
                info!("_last_checkpoint file not found");
                Ok(None)
            }
            Some(Err(err)) => Err(err),
            None => {
                warn!("empty _last_checkpoint file");
                Ok(None)
            }
        }
    }

    /// Succinct summary string for logging purposes.
    fn summary(&self) -> String {
        format!(
            "{{v={}, size={}, parts={:?}}}",
            self.version, self.size, self.parts
        )
    }

    /// Convert the LastCheckpointHint to JSON bytes
    #[cfg(test)]
    pub(crate) fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to convert LastCheckpointHint to JSON bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real `_last_checkpoint` for a V2 checkpoint carries a `v2Checkpoint` object; we parse its
    /// `path`. Extra `v2Checkpoint` fields a writer emits (e.g. `sizeInBytes`, `sidecarFiles`) are
    /// ignored. Guards the `camelCase` wire key -- a rename would otherwise silently parse to
    /// `None` (errors are swallowed in `try_read`) and disable the identity filter.
    #[test]
    fn parses_v2_checkpoint_path_from_wire_json() {
        let json = br#"{
            "version": 5,
            "size": 10,
            "v2Checkpoint": {
                "path": "00000000000000000005.checkpoint.0190e8f5-uuid.parquet",
                "sizeInBytes": 1234,
                "sidecarFiles": []
            }
        }"#;
        let hint: LastCheckpointHint = serde_json::from_slice(json).unwrap();
        assert_eq!(
            hint.v2_checkpoint,
            Some(LastCheckpointV2 {
                path: "00000000000000000005.checkpoint.0190e8f5-uuid.parquet".to_string(),
            })
        );
    }

    /// A `_last_checkpoint` without a `v2Checkpoint` object (V1 / classic) parses to `None`.
    #[test]
    fn v2_checkpoint_absent_parses_to_none() {
        let json = br#"{"version": 5, "size": 10}"#;
        let hint: LastCheckpointHint = serde_json::from_slice(json).unwrap();
        assert!(hint.v2_checkpoint.is_none());
    }
}
