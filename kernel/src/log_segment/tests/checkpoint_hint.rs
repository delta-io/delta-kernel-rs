//! Tests for the `_last_checkpoint` hint: identity gating of the retained hint and the
//! contents parsed from real V2 checkpoint tables.

use super::*;

fn synthetic_sidecar() -> Sidecar {
    Sidecar {
        path: "sidecar-1.parquet".to_string(),
        size_in_bytes: 42,
        modification_time: 1,
        tags: None,
    }
}

/// A V2 hint's fields are trusted only when the hint names the checkpoint file the segment
/// selected -- several V2 checkpoints can share a version, so a version match alone is not
/// enough. `checkpoint_schema` and `checkpoint_sidecars` share that identity gate: on a
/// match both surface the hint's values verbatim (an explicit empty sidecar list as
/// `Some([])`, distinct from absent -- the contract the scan-shape fast path relies on); on
/// a mismatch both are suppressed.
#[rstest]
#[case::matched(true, vec![synthetic_sidecar()], Some(vec![synthetic_sidecar()]))]
#[case::matched_empty_sidecars(true, vec![], Some(vec![]))]
#[case::mismatched(false, vec![synthetic_sidecar()], None)]
fn test_v2_hint_accessors_identity_filter(
    #[case] hint_names_selected: bool,
    #[case] hint_sidecars: Vec<Sidecar>,
    #[case] expected_sidecars: Option<Vec<Sidecar>>,
) -> DeltaResult<()> {
    let (_store, log_root) = new_in_memory_store();
    let selected = "00000000000000000001.checkpoint.11111111-1111-1111-1111-111111111111.parquet";
    let other = "00000000000000000001.checkpoint.22222222-2222-2222-2222-222222222222.parquet";
    let checkpoint_file = log_root.join(selected)?.to_string();
    let hint_name = if hint_names_selected { selected } else { other };

    let hint_schema: SchemaRef = Arc::new(StructType::new_unchecked([StructField::nullable(
        "metadata",
        StructType::new_unchecked([]),
    )]));

    let commit = create_log_path(log_root.join("00000000000000000002.json")?.as_str());
    let log_segment = LogSegment::try_new(
        LogSegmentFiles {
            checkpoint_parts: vec![create_log_path_with_size(&checkpoint_file, 1)],
            ascending_commit_files: vec![commit.clone()],
            latest_commit_file: Some(commit),
            ..Default::default()
        },
        log_root,
        None,
        Some(LastCheckpointHint {
            version: 1,
            checkpoint_schema: Some(hint_schema.clone()),
            v2_checkpoint: Some(LastCheckpointV2 {
                path: hint_name.to_string(),
                sidecar_files: Some(hint_sidecars),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )?;

    assert_eq!(log_segment.checkpoint_version, Some(1));
    assert_eq!(
        log_segment.checkpoint_schema(),
        hint_names_selected.then(|| hint_schema.clone())
    );
    assert_eq!(
        log_segment.checkpoint_sidecars(),
        expected_sidecars.as_deref()
    );
    Ok(())
}

/// A multi-part V1 checkpoint is identified by `(version, numParts)`. The hint applies only
/// when its part count matches the checkpoint the segment selected; a different `parts` (a
/// same-version checkpoint with a different part count) must be ignored.
#[rstest]
#[case::matches(Some(2), true)]
#[case::wrong_count(Some(1), false)]
#[case::hint_single_segment_multi(None, false)]
fn test_v1_multipart_numparts_identity(
    #[case] hint_parts: Option<usize>,
    #[case] hint_matches: bool,
) -> DeltaResult<()> {
    let (_store, log_root) = new_in_memory_store();

    let part1 = "00000000000000000001.checkpoint.0000000001.0000000002.parquet";
    let part2 = "00000000000000000001.checkpoint.0000000002.0000000002.parquet";
    let cp1 = log_root.join(part1)?.to_string();
    let cp2 = log_root.join(part2)?.to_string();

    let commit = create_log_path(log_root.join("00000000000000000002.json")?.as_str());
    let log_segment = LogSegment::try_new(
        LogSegmentFiles {
            checkpoint_parts: vec![
                create_log_path_with_size(&cp1, 1),
                create_log_path_with_size(&cp2, 1),
            ],
            ascending_commit_files: vec![commit.clone()],
            latest_commit_file: Some(commit),
            ..Default::default()
        },
        log_root,
        None,
        Some(LastCheckpointHint {
            version: 1,
            parts: hint_parts,
            ..Default::default()
        }),
    )?;

    assert_eq!(log_segment.checkpoint_version, Some(1));
    assert_eq!(
        log_segment.checkpoint_hint().is_some(),
        hint_matches,
        "hint with parts={hint_parts:?} against a 2-part checkpoint"
    );
    Ok(())
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
