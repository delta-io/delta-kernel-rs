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
use crate::path::ParsedLogPath;
use crate::schema::SchemaRef;
use crate::{DeltaResult, Error, FileMeta, StorageHandler, Version};

/// Name of the _last_checkpoint file that provides metadata about the last checkpoint
/// created for the table. This file is used as a hint for the engine to quickly locate
/// the latest checkpoint without a full directory listing.
const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";

// Note: Schema can not be derived because the checkpoint schema is only known at runtime.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(test, derive(Default))]
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
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(test, derive(Default))]
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

/// One element of [`LastCheckpointV2`]'s `non_file_actions`. A log action is exactly one action
/// type, so this is an externally-tagged enum keyed by the action name, reusing kernel's action
/// structs to yield the same types as log replay. An unrecognized action key fails the whole-hint
/// parse; `try_read` swallows that, so the reader falls back to reading the checkpoint.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) enum HintAction {
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    Protocol(Protocol),
    Txn(SetTransaction),
    DomainMetadata(DomainMetadata),
    CheckpointMetadata(CheckpointMetadata),
}

impl LastCheckpointHint {
    /// Whether this hint describes the checkpoint a log segment selected -- the `checkpoint_parts`
    /// at `version`. A checkpoint's identity is more than its version (several can share one, e.g.
    /// concurrent writers), so the rest must also match before the hint's fields can be trusted:
    ///
    /// - Part count: a multi-part V1 checkpoint is keyed by `(version, numParts)` and its parts
    ///   share a deterministic per-version name, so a matching part count pins the identity.
    ///   `parts` is absent for a single-part / classic checkpoint (hence one part).
    /// - `v2Checkpoint.path`: a V2 checkpoint's file name carries a UUID, so the name must match
    ///   the selected part -- `checkpoint_parts.first()`, the file the hint's fields describe.
    pub(crate) fn applies_to(
        &self,
        version: Version,
        checkpoint_parts: &[ParsedLogPath<FileMeta>],
    ) -> bool {
        self.version == version
            && self.parts.unwrap_or(1) == checkpoint_parts.len()
            && match &self.v2_checkpoint {
                Some(v2) => {
                    checkpoint_parts.first().map(|p| p.filename.as_str()) == Some(v2.path.as_str())
                }
                None => true,
            }
    }

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
    use rstest::rstest;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::create_log_path;

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

    /// A manifest V2 checkpoint hint carries its sidecar references and non-file actions. Each
    /// non-file action decodes to exactly one [`HintAction`] variant via its action key.
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
                    {"checkpointMetadata": {"version": 5}}
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
        assert_eq!(actions.len(), 5);
        assert!(matches!(&actions[0], HintAction::Protocol(p) if p.min_reader_version() == 3));
        assert!(matches!(&actions[1], HintAction::Metadata(m) if m.id() == "table-id"));
        assert!(matches!(&actions[2], HintAction::Txn(t) if t.app_id == "app"));
        assert!(matches!(&actions[3], HintAction::DomainMetadata(_)));
        assert!(matches!(&actions[4], HintAction::CheckpointMetadata(c) if c.version == 5));
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

    /// `applies_to` accepts the hint only for the checkpoint a segment selected: the version and
    /// part count must match, and for a V2 checkpoint the first part's file name must match too.
    #[test]
    fn applies_to_matches_only_the_selected_checkpoint() {
        let root = Url::parse("memory:///_delta_log/").unwrap();
        let part = |name: &str| create_log_path(root.join(name).unwrap().as_str());

        let selected =
            "00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.parquet";
        let other = "00000000000000000001.checkpoint.22222222-2222-2222-2222-222222222222.parquet";

        // V2 single-part: applies only when the version and the file name both match.
        let v2 = LastCheckpointHint {
            version: 1,
            v2_checkpoint: Some(LastCheckpointV2 {
                path: selected.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(v2.applies_to(1, &[part(selected)]));
        assert!(!v2.applies_to(2, &[part(selected)]), "wrong version");
        assert!(!v2.applies_to(1, &[part(other)]), "wrong v2 path");

        // V1 multi-part: keyed by (version, numParts); the file name is not consulted.
        let mp1 = "00000000000000000001.checkpoint.0000000001.0000000002.parquet";
        let mp2 = "00000000000000000001.checkpoint.0000000002.0000000002.parquet";
        let v1_multi = LastCheckpointHint {
            version: 1,
            parts: Some(2),
            ..Default::default()
        };
        assert!(v1_multi.applies_to(1, &[part(mp1), part(mp2)]));
        assert!(!v1_multi.applies_to(1, &[part(mp1)]), "wrong part count");

        // V1 single-part: an absent `parts` means one part.
        let v1_single = LastCheckpointHint {
            version: 1,
            ..Default::default()
        };
        assert!(v1_single.applies_to(1, &[part("00000000000000000001.checkpoint.parquet")]));
    }

    /// Returns the single `actions` element matching `extract`, asserting there is exactly one.
    fn one_action<'a, T: 'a>(
        actions: &'a [HintAction],
        extract: impl Fn(&'a HintAction) -> Option<&'a T>,
    ) -> &'a T {
        let mut matching = actions.iter().filter_map(extract);
        let found = matching.next().expect("expected a matching action");
        assert!(
            matching.next().is_none(),
            "expected exactly one matching action"
        );
        found
    }

    /// The `v2Checkpoint` hint parses from V2 checkpoint tables to its exact contents. Pins, per
    /// table, the checkpoint version, file path, sidecar paths, metadata id, created time, and
    /// configuration; and -- shared across these fixtures -- a `(3, 7)` protocol with
    /// V2Checkpoint/AppendOnly/ Invariants features and an unpartitioned parquet `id: long`
    /// schema. The non-file actions are exactly protocol + metadata + checkpointMetadata (no
    /// txn, no domainMetadata). Also checks the identity gate exposes a matched hint's sidecars
    /// but suppresses a mismatched one (`v2-classic-checkpoint-parquet`, whose hint names a
    /// UUID checkpoint while the segment selects the classic-named one).
    #[rstest]
    #[case::parquet_sidecars(
    "v2-checkpoints-parquet-with-sidecars",
    6,
    "00000000000000000006.checkpoint.f15b9025-707a-4c73-aac0-31dfcbd29aa6.parquet",
    &[
    "00000000000000000006.checkpoint.0000000001.0000000002.76931b15-ead3-480d-b86c-afe55a577fc3.parquet",
    "00000000000000000006.checkpoint.0000000002.0000000002.4367b29c-0e87-447f-8e81-9814cc01ad1f.parquet",
    ],
    "5a5afdfe-7d40-4109-bb92-29b051257e4c",
    1739329708855,
    &[("delta.checkpointInterval", "1"), ("delta.checkpointPolicy", "v2")]
    )]
    #[case::json_sidecars(
    "v2-checkpoints-json-with-sidecars",
    6,
    "00000000000000000006.checkpoint.2a15d0c6-8b11-4a98-bab4-957905d62f7f.json",
    &[
    "00000000000000000006.checkpoint.0000000001.0000000002.19af1366-a425-47f4-8fa6-8d6865625573.parquet",
    "00000000000000000006.checkpoint.0000000002.0000000002.5008b69f-aa8a-4a66-9299-0733a56a7e63.parquet",
    ],
    "f571bf08-452e-4155-9f52-f793e630c55c",
    1739329697356,
    &[("delta.checkpointInterval", "1"), ("delta.checkpointPolicy", "v2")]
    )]
    #[case::parquet_last_checkpoint(
    "v2-checkpoints-parquet-with-last-checkpoint",
    0,
    "00000000000000000000.checkpoint.8516aa94-7099-4e71-92a0-d6d7e7bb3b2c.parquet",
    &["00000000000000000000.checkpoint.0000000001.0000000001.c561300b-ad5f-49d4-a28d-9b3f4bb0331c.parquet"],
    "d3b78022-27fc-470d-a4ea-1b8b47fcc143",
    1739329764309,
    &[("delta.checkpointPolicy", "v2")]
    )]
    #[case::json_last_checkpoint(
    "v2-checkpoints-json-with-last-checkpoint",
    0,
    "00000000000000000000.checkpoint.0e42c15b-17cc-4918-990d-2ff76e918e4d.json",
    &["00000000000000000000.checkpoint.0000000001.0000000001.9167a758-dd93-4e52-8636-7cf5776eb10f.parquet"],
    "f03ce383-0d09-4e1c-9446-8d80e1a59daa",
    1739329763101,
    &[("delta.checkpointPolicy", "v2")]
    )]
    #[case::classic_parquet(
    "v2-classic-checkpoint-parquet",
    1,
    "00000000000000000001.checkpoint.bfe7499d-715e-4d64-82a4-e6cdd2fc37af.parquet",
    &["00000000000000000001.checkpoint.0000000001.0000000001.e2eb56f9-1c54-4a82-b122-de108e317c20.parquet"],
    "541a194a-df83-4f46-9adf-032a1275e82b",
    1739329759409,
    &[("delta.checkpointPolicy", "v2")]
    )]
    #[case::classic_json(
    "v2-classic-checkpoint-json",
    1,
    "00000000000000000001.checkpoint.6c750e24-bbc4-4618-8feb-7cd7d5b9e084.json",
    &["00000000000000000001.checkpoint.0000000001.0000000001.c1bacf45-f3a9-4846-bd44-87cdacd4620f.parquet"],
    "29ef2045-59c5-4cf7-9d5d-2ba47e971d32",
    1739313200623,
    &[("delta.checkpointPolicy", "v2")]
    )]
    fn v2_last_checkpoint_hint_contents(
        #[case] table: &str,
        #[case] expected_version: u64,
        #[case] expected_path: &str,
        #[case] expected_sidecars: &[&str],
        #[case] expected_metadata_id: &str,
        #[case] expected_created_time: i64,
        #[case] expected_config: &[(&str, &str)],
    ) -> DeltaResult<()> {
        use crate::utils::test_utils::load_test_table;

        let (engine, snapshot, _tempdir) = load_test_table(table)?;
        let seg = snapshot.log_segment();
        let hint = LastCheckpointHint::try_read(engine.storage_handler().as_ref(), &seg.log_root)?
            .expect("table has a _last_checkpoint");
        let v2 = hint.v2_checkpoint.as_ref().expect("V2 checkpoint hint");

        // Version, checkpoint file path, and sidecar paths are this table's exact identity.
        assert_eq!(hint.version, expected_version, "{table}: version");
        assert_eq!(v2.path, expected_path, "{table}: v2 path");
        let sidecar_paths: Vec<&str> = v2
            .sidecar_files
            .as_ref()
            .expect("sidecarFiles present")
            .iter()
            .map(|s| s.path.as_str())
            .collect();
        assert_eq!(
            sidecar_paths.as_slice(),
            expected_sidecars,
            "{table}: sidecar paths"
        );

        // The non-file actions are exactly one protocol, one metadata, and one checkpointMetadata
        // -- no txn or domainMetadata.
        let actions = v2
            .non_file_actions
            .as_ref()
            .expect("nonFileActions present");
        assert!(
            !actions.iter().any(|a| matches!(a, HintAction::Txn(_))),
            "{table}: no txn"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, HintAction::DomainMetadata(_))),
            "{table}: no domain metadata"
        );

        // A (3, 7) protocol gated on the V2Checkpoint reader feature, with V2Checkpoint/AppendOnly/
        // Invariants on the writer side.
        let protocol = one_action(actions, |a| match a {
            HintAction::Protocol(p) => Some(p),
            _ => None,
        });
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            (3, 7),
            "{table}: protocol version"
        );
        assert_eq!(
            protocol.reader_features(),
            Some([TableFeature::V2Checkpoint].as_slice()),
            "{table}: reader features"
        );
        assert_eq!(
            protocol.writer_features(),
            Some(
                [
                    TableFeature::V2Checkpoint,
                    TableFeature::AppendOnly,
                    TableFeature::Invariants
                ]
                .as_slice()
            ),
            "{table}: writer features"
        );

        // Metadata for an unnamed, unpartitioned parquet table with a single `id: long` column.
        let metadata = one_action(actions, |a| match a {
            HintAction::Metadata(m) => Some(m),
            _ => None,
        });
        assert_eq!(metadata.id(), expected_metadata_id, "{table}: metadata id");
        assert_eq!(metadata.name(), None, "{table}: metadata name");
        assert_eq!(
            metadata.description(),
            None,
            "{table}: metadata description"
        );
        assert_eq!(metadata.format_provider(), "parquet", "{table}: format");
        assert_eq!(
            metadata.parse_schema()?,
            StructType::new_unchecked([StructField::nullable("id", DataType::LONG)]),
            "{table}: metadata schema"
        );
        assert!(
            metadata.partition_columns().is_empty(),
            "{table}: unpartitioned"
        );
        assert_eq!(
            metadata.created_time(),
            Some(expected_created_time),
            "{table}: created time"
        );
        let expected_config: HashMap<String, String> = expected_config
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(
            metadata.configuration(),
            &expected_config,
            "{table}: configuration"
        );

        // checkpointMetadata records this checkpoint's version.
        let checkpoint_metadata = one_action(actions, |a| match a {
            HintAction::CheckpointMetadata(c) => Some(c),
            _ => None,
        });
        assert_eq!(
            checkpoint_metadata.version as u64, expected_version,
            "{table}: checkpointMetadata.version"
        );
        assert_eq!(
            checkpoint_metadata.tags, None,
            "{table}: checkpointMetadata.tags"
        );

        // Identity gate: a hint naming the selected checkpoint exposes its sidecars through the
        // accessor; one naming a different same-version checkpoint is fully suppressed.
        let selected = &seg
            .listed
            .checkpoint_parts
            .first()
            .expect("checkpoint present")
            .filename;
        if &v2.path == selected {
            assert_eq!(
                seg.checkpoint_sidecars(),
                v2.sidecar_files.as_deref(),
                "{table}: matched hint exposes its sidecars"
            );
        } else {
            assert!(
                seg.checkpoint_schema().is_none() && seg.checkpoint_sidecars().is_none(),
                "{table}: mismatched hint ({}) must be suppressed",
                v2.path
            );
        }
        Ok(())
    }
}
