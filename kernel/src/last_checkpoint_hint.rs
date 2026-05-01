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

/// A more minimal version of LastCheckpointHint, storing only the version and schema.
///
/// This is primarily used by LogSegment to store necessary information about the latest
/// checkpoint without retaining all of the metadata fields. If this struct grows
/// to be similar in size to LastCheckpointHint, we should just switch to using LastCheckpointHint
/// directly.
#[derive(Debug, Clone, PartialEq, Eq)]
#[internal_api]
pub(crate) struct LastCheckpointHintSummary {
    /// Version of the latest known checkpoint, at the time the hint file was read.
    pub version: Version,

    /// Schema of the checkpoint file(s), as read from the `_last_checkpoint` hint.
    /// Useful for determining if `stats_parsed` is available for data skipping.
    /// `None` when the hint file did not include a `checkpointSchema` field.
    pub schema: Option<SchemaRef>,
}

// Note: Schema can not be derived because the checkpoint schema is only known at runtime.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) struct LastCheckpointHint {
    /// The version of the table when the last checkpoint was made.
    pub version: Version,
    /// The number of actions that are stored in the checkpoint.
    pub size: i64,
    /// The number of fragments if the last checkpoint was written in multiple parts.
    pub parts: Option<usize>,
    /// The number of bytes of the checkpoint.
    pub size_in_bytes: Option<i64>,
    /// The number of AddFile actions in the checkpoint.
    pub num_of_add_files: Option<i64>,
    /// The schema of the checkpoint file.
    pub checkpoint_schema: Option<SchemaRef>,
    /// The checksum of the last checkpoint JSON.
    pub checksum: Option<String>,
    /// Additional metadata about the last checkpoint.
    pub tags: Option<HashMap<String, String>>,
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
    #[internal_api]
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
