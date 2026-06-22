//! Utilities for reading the `_last_checkpoint` file. Maybe this file should instead go under
//! log_segment module since it should only really be used there? as hint for listing?

use std::collections::HashMap;

use delta_kernel_derive::internal_api;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};
use url::Url;

use crate::actions::{
    CheckpointMetadata, DomainMetadata, Metadata, Protocol, SetTransaction, Sidecar,
};
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

    /// Number of parts the hint's checkpoint was written in (`parts`). `None` for a single-part /
    /// classic checkpoint. Part of the checkpoint identity for multi-part V1 checkpoints.
    pub parts: Option<usize>,

    /// Schema of the checkpoint file(s), as read from the `_last_checkpoint` hint.
    /// Useful for determining if `stats_parsed` is available for data skipping.
    /// `None` when the hint file did not include a `checkpointSchema` field.
    pub schema: Option<SchemaRef>,

    /// File name of the V2 checkpoint the hint describes (`v2Checkpoint.path`). `None` for V1 /
    /// classic checkpoints.
    pub v2_checkpoint_path: Option<String>,

    /// Sidecar references carried by the hint's `v2Checkpoint`, if any.
    pub sidecar_files: Option<Vec<Sidecar>>,
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
/// Carries the V2 checkpoint file's identity and metadata plus the actions a reader would otherwise
/// read from the checkpoint itself -- its sidecar references and its non-file actions. Absent for
/// V1 / classic checkpoints.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) struct LastCheckpointV2 {
    /// Bare file name of the V2 checkpoint this hint describes, matched against the selected
    /// checkpoint part's file name. Several V2 checkpoints can share a version, so this identifies
    /// which one the hint's fields describe.
    pub(crate) path: String,

    /// Size in bytes of the V2 checkpoint file named by `path`.
    pub(crate) size_in_bytes: Option<i64>,

    /// Modification time of the V2 checkpoint file named by `path`, in milliseconds since the Unix
    /// epoch.
    pub(crate) modification_time: Option<i64>,

    /// The sidecar files this checkpoint references, for a manifest (non-leaf) V2 checkpoint.
    /// Empty/absent for a leaf checkpoint that inlines its file actions.
    pub(crate) sidecar_files: Option<Vec<Sidecar>>,

    /// The checkpoint's non-file actions (see [`HintAction`]), letting a reader obtain them
    /// without reading the checkpoint file.
    pub(crate) non_file_actions: Option<Vec<HintAction>>,
}

/// One element of [`LastCheckpointV2`]'s `non_file_actions`, reusing kernel's action structs so the
/// hint yields the same types as log replay. Mirrors the log's single-action layout; an
/// unrecognized action key parses to an empty element rather than failing, so the hint survives new
/// action types.
#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) struct HintAction {
    #[serde(rename = "metaData")]
    pub(crate) metadata: Option<Metadata>,
    pub(crate) protocol: Option<Protocol>,
    pub(crate) txn: Option<SetTransaction>,
    pub(crate) domain_metadata: Option<DomainMetadata>,
    pub(crate) checkpoint_metadata: Option<CheckpointMetadata>,
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
    /// `path` and file metadata. An empty `sidecarFiles` (a leaf checkpoint) parses to `Some([])`,
    /// distinct from an absent field. Guards the `camelCase` wire keys -- a rename would otherwise
    /// silently parse to `None` (errors are swallowed in `try_read`) and disable the identity
    /// filter.
    #[test]
    fn parses_v2_checkpoint_path_from_wire_json() {
        let json = br#"{
            "version": 5,
            "size": 10,
            "v2Checkpoint": {
                "path": "00000000000000000005.checkpoint.0190e8f5-uuid.parquet",
                "sizeInBytes": 1234,
                "modificationTime": 1700000000000,
                "sidecarFiles": []
            }
        }"#;
        let hint: LastCheckpointHint = serde_json::from_slice(json).unwrap();
        let v2 = hint.v2_checkpoint.expect("v2Checkpoint present");
        assert_eq!(
            v2.path,
            "00000000000000000005.checkpoint.0190e8f5-uuid.parquet"
        );
        assert_eq!(v2.size_in_bytes, Some(1234));
        assert_eq!(v2.modification_time, Some(1700000000000));
        assert_eq!(v2.sidecar_files, Some(vec![]));
        assert_eq!(v2.non_file_actions, None);
    }

    /// A manifest V2 checkpoint hint carries its sidecar references and non-file actions. The
    /// non-file actions reuse kernel's action structs, and each element sets exactly one action;
    /// unknown action keys (e.g. a future `commitInfo`) are ignored rather than failing the parse.
    #[test]
    fn parses_v2_checkpoint_sidecars_and_non_file_actions() {
        let json = br#"{
            "version": 5,
            "size": 10,
            "v2Checkpoint": {
                "path": "00000000000000000005.checkpoint.0190e8f5-uuid.parquet",
                "sidecarFiles": [
                    {"path": "sidecar-1.parquet", "sizeInBytes": 42, "modificationTime": 1700000000000}
                ],
                "nonFileActions": [
                    {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7}},
                    {"metaData": {"id": "table-id", "format": {"provider": "parquet", "options": {}},
                        "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
                        "partitionColumns": [], "configuration": {}}},
                    {"txn": {"appId": "app", "version": 1}},
                    {"domainMetadata": {"domain": "d", "configuration": "c", "removed": false}},
                    {"checkpointMetadata": {"version": 5}},
                    {"commitInfo": {"timestamp": 123}}
                ]
            }
        }"#;
        let hint: LastCheckpointHint = serde_json::from_slice(json).unwrap();
        let v2 = hint.v2_checkpoint.expect("v2Checkpoint present");

        let sidecars = v2.sidecar_files.expect("sidecarFiles present");
        assert_eq!(sidecars.len(), 1);
        assert_eq!(sidecars[0].path, "sidecar-1.parquet");
        assert_eq!(sidecars[0].size_in_bytes, 42);

        let actions = v2.non_file_actions.expect("nonFileActions present");
        // The unknown `commitInfo` element parses to an all-`None` HintAction rather than erroring.
        assert_eq!(actions.len(), 6);
        assert_eq!(
            actions[0].protocol.as_ref().unwrap().min_reader_version(),
            3
        );
        assert_eq!(actions[1].metadata.as_ref().unwrap().id(), "table-id");
        assert_eq!(actions[2].txn.as_ref().unwrap().app_id, "app");
        assert_eq!(actions[4].checkpoint_metadata.as_ref().unwrap().version, 5);
        assert_eq!(actions[5], HintAction::default());
    }

    /// A `_last_checkpoint` without a `v2Checkpoint` object (V1 / classic) parses to `None`.
    #[test]
    fn v2_checkpoint_absent_parses_to_none() {
        let json = br#"{"version": 5, "size": 10}"#;
        let hint: LastCheckpointHint = serde_json::from_slice(json).unwrap();
        assert!(hint.v2_checkpoint.is_none());
    }

    /// A malformed `v2Checkpoint` -- missing the required `path`, or a type-mismatched field --
    /// fails the whole-hint parse. `try_read` swallows that to `None`, so the reader falls back to
    /// a footer read rather than trusting a partially-parsed hint.
    #[test]
    fn malformed_v2_checkpoint_fails_whole_hint_parse() {
        let missing_path = br#"{"version": 5, "size": 10, "v2Checkpoint": {"sizeInBytes": 1234}}"#;
        assert!(serde_json::from_slice::<LastCheckpointHint>(missing_path).is_err());

        let bad_type = br#"{"version": 5, "size": 10,
            "v2Checkpoint": {"path": "c.parquet", "sizeInBytes": "not-a-number"}}"#;
        assert!(serde_json::from_slice::<LastCheckpointHint>(bad_type).is_err());
    }
}
