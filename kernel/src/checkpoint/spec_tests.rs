//! Checkpoint type classification against real checkpoint files.
//!
//! Each case writes a table at version 2 to an in-memory store: two commits plus a checkpoint
//! whose naming, contents, and table protocol vary.

use std::sync::Arc;

use rstest::rstest;

use crate::arrow::array::StringArray;
use crate::checkpoint::SelectedCheckpointType::{self, V1, V2};
use crate::engine::PanicEngine;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::json::SyncJsonHandler;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::parquet::arrow::ArrowWriter;
use crate::snapshot::SnapshotRef;
use crate::unit_test_utils::string_array_to_engine_data;
use crate::{JsonHandler as _, KernelError, Snapshot};

const ROOT: &str = "memory:///";
const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const PROTOCOL_V1: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const PROTOCOL_V2: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint"]}}"#;
const METADATA: &str = r#"{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":0}}"#;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Naming {
    Classic,
    MultiPart,
    UuidParquet,
    UuidJson,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Contents {
    /// File actions only.
    V1,
    /// File actions only, with null `checkpointMetadata` and `sidecar` columns.
    V1WithNullV2Columns,
    /// `checkpointMetadata` and inline file actions.
    V2Inline,
    /// `checkpointMetadata` and a sidecar holding the file actions.
    V2Sidecars,
    /// A sidecar but no `checkpointMetadata`.
    SidecarsWithoutMetadata,
    /// `checkpointMetadata` for a different version.
    MetadataVersionMismatch,
    /// Two `checkpointMetadata` actions.
    DuplicateMetadata,
}

fn add(path: &str) -> String {
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{}},"size":1,"modificationTime":0,"dataChange":false}}}}"#
    )
}

fn checkpoint_metadata(version: u64) -> String {
    format!(r#"{{"checkpointMetadata":{{"version":{version}}}}}"#)
}

fn put(store: &InMemory, path: &str, bytes: Vec<u8>) {
    futures::executor::block_on(store.put(&Path::from(path), bytes.into())).unwrap();
}

/// Writes JSON action lines as Parquet with the all-actions schema projected to `fields`.
fn write_parquet(store: &InMemory, path: &str, lines: &[String], fields: &[&str]) -> u64 {
    let schema = crate::actions::get_all_actions_schema()
        .project(fields)
        .unwrap();
    let data = SyncJsonHandler::new(None)
        .parse_json(
            string_array_to_engine_data(StringArray::from(lines.to_vec())),
            schema,
        )
        .unwrap();
    let batch = ArrowEngineData::try_from_engine_data(data).unwrap();
    let mut buffer = vec![];
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.record_batch().schema(), None).unwrap();
    writer.write(batch.record_batch()).unwrap();
    writer.close().unwrap();
    let size = buffer.len() as u64;
    put(store, path, buffer);
    size
}

/// Writes the table and returns its store. `stale_last_checkpoint` writes a `_last_checkpoint`
/// whose schema omits `checkpointMetadata` and `sidecar`, as an earlier V1 file would have had.
fn write_table(
    v2_feature: bool,
    naming: Naming,
    contents: Contents,
    stale_last_checkpoint: bool,
) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let protocol = if v2_feature { PROTOCOL_V2 } else { PROTOCOL_V1 };
    put(
        &store,
        "_delta_log/00000000000000000000.json",
        format!("{protocol}\n{METADATA}\n").into_bytes(),
    );
    for (version, file) in [(1, "a.parquet"), (2, "b.parquet")] {
        put(
            &store,
            &format!("_delta_log/{version:020}.json"),
            format!("{}\n", add(file)).into_bytes(),
        );
    }

    let adds = vec![add("a.parquet"), add("b.parquet")];
    let mut lines = vec![protocol.to_string(), METADATA.to_string()];
    match contents {
        Contents::V1 | Contents::V1WithNullV2Columns => lines.extend(adds.clone()),
        Contents::V2Inline => {
            lines.push(checkpoint_metadata(2));
            lines.extend(adds.clone());
        }
        Contents::MetadataVersionMismatch => {
            lines.push(checkpoint_metadata(1));
            lines.extend(adds.clone());
        }
        Contents::DuplicateMetadata => {
            lines.push(checkpoint_metadata(2));
            lines.push(checkpoint_metadata(2));
            lines.extend(adds.clone());
        }
        Contents::V2Sidecars | Contents::SidecarsWithoutMetadata => {
            let size = write_parquet(&store, "_delta_log/_sidecars/s1.parquet", &adds, &["add"]);
            if contents == Contents::V2Sidecars {
                lines.push(checkpoint_metadata(2));
            }
            lines.push(format!(
                r#"{{"sidecar":{{"path":"s1.parquet","sizeInBytes":{size},"modificationTime":0}}}}"#
            ));
        }
    }
    let mut fields = vec!["protocol", "metaData", "add"];
    let all_columns = contents == Contents::V1WithNullV2Columns;
    if all_columns || lines.iter().any(|line| line.contains("checkpointMetadata")) {
        fields.push("checkpointMetadata");
    }
    if all_columns || lines.iter().any(|line| line.contains("\"sidecar\"")) {
        fields.push("sidecar");
    }

    let mut last_checkpoint = serde_json::json!({ "version": 2, "size": lines.len() });
    match naming {
        Naming::Classic => {
            write_parquet(
                &store,
                "_delta_log/00000000000000000002.checkpoint.parquet",
                &lines,
                &fields,
            );
        }
        Naming::MultiPart => {
            let (first, second) = lines.split_at(lines.len() - 1);
            for (part, part_lines) in [(1, first), (2, second)] {
                write_parquet(
                    &store,
                    &format!(
                        "_delta_log/00000000000000000002.checkpoint.{part:010}.0000000002.parquet"
                    ),
                    part_lines,
                    &fields,
                );
            }
            last_checkpoint["parts"] = 2.into();
        }
        Naming::UuidParquet | Naming::UuidJson => {
            let extension = if naming == Naming::UuidJson {
                "json"
            } else {
                "parquet"
            };
            let name = format!("00000000000000000002.checkpoint.{UUID}.{extension}");
            if naming == Naming::UuidJson {
                put(
                    &store,
                    &format!("_delta_log/{name}"),
                    (lines.join("\n") + "\n").into_bytes(),
                );
            } else {
                write_parquet(&store, &format!("_delta_log/{name}"), &lines, &fields);
            }
            last_checkpoint["v2Checkpoint"] = serde_json::json!({ "path": name });
        }
    }
    if stale_last_checkpoint {
        let stale = crate::actions::get_all_actions_schema()
            .project(&["protocol", "metaData", "add"])
            .unwrap();
        last_checkpoint["checkpointSchema"] = serde_json::to_value(stale.as_ref()).unwrap();
    }
    put(
        &store,
        "_delta_log/_last_checkpoint",
        last_checkpoint.to_string().into_bytes(),
    );
    store
}

fn load(store: &Arc<InMemory>) -> (SyncEngine, SnapshotRef) {
    let engine = SyncEngine::new_with_store(store.clone());
    let snapshot = Snapshot::builder_for(ROOT).build(&engine).unwrap();
    (engine, snapshot)
}

fn scanned_files(snapshot: &SnapshotRef, engine: &SyncEngine) -> usize {
    let scan = snapshot.clone().scan_builder().build().unwrap();
    scan.scan_metadata(engine)
        .unwrap()
        .map(|data| {
            let data = data.unwrap();
            data.scan_files
                .selection_vector()
                .iter()
                .filter(|s| **s)
                .count()
        })
        .sum()
}

#[rstest]
#[case::classic_v1(false, Naming::Classic, Contents::V1, V1)]
#[case::classic_v1_on_v2_table(true, Naming::Classic, Contents::V1, V1)]
#[case::classic_v1_with_null_v2_columns(true, Naming::Classic, Contents::V1WithNullV2Columns, V1)]
#[case::classic_v2_inline(true, Naming::Classic, Contents::V2Inline, V2)]
#[case::classic_v2_sidecars(true, Naming::Classic, Contents::V2Sidecars, V2)]
#[case::multi_part_v1(false, Naming::MultiPart, Contents::V1, V1)]
#[case::uuid_parquet_inline(true, Naming::UuidParquet, Contents::V2Inline, V2)]
#[case::uuid_parquet_sidecars(true, Naming::UuidParquet, Contents::V2Sidecars, V2)]
#[case::uuid_json_inline(true, Naming::UuidJson, Contents::V2Inline, V2)]
#[case::uuid_json_sidecars(true, Naming::UuidJson, Contents::V2Sidecars, V2)]
fn valid_checkpoint_is_classified_by_its_contents(
    #[case] v2_feature: bool,
    #[case] naming: Naming,
    #[case] contents: Contents,
    #[case] expected: SelectedCheckpointType,
) {
    // Given a valid checkpoint.
    let store = write_table(v2_feature, naming, contents, false);
    let (engine, snapshot) = load(&store);
    // When discovery reads it.
    let info = snapshot
        .reader()
        .get_checkpoint_info(&engine)
        .unwrap()
        .unwrap();
    // Then it is classified by its contents, and scans still see every file.
    assert_eq!(info.checkpoint_type(), expected);
    assert_eq!(
        snapshot.reader().get_checkpoint_type(&PanicEngine).unwrap(),
        Some(expected)
    );
    assert_eq!(scanned_files(&snapshot, &engine), 2);
}

#[rstest]
#[case::uuid_parquet_without_metadata(true, Naming::UuidParquet, Contents::V1, Some(V2), V2)]
#[case::uuid_json_without_metadata(true, Naming::UuidJson, Contents::V1, Some(V2), V2)]
#[case::multi_part_with_metadata(true, Naming::MultiPart, Contents::V2Inline, Some(V1), V1)]
#[case::classic_v2_without_feature(false, Naming::Classic, Contents::V2Inline, Some(V1), V2)]
#[case::uuid_v2_without_feature(false, Naming::UuidParquet, Contents::V2Inline, Some(V2), V2)]
#[case::sidecars_without_metadata(
    true,
    Naming::Classic,
    Contents::SidecarsWithoutMetadata,
    None,
    V1
)]
fn checkpoint_breaking_a_writer_rule_is_read_without_error(
    #[case] v2_feature: bool,
    #[case] naming: Naming,
    #[case] contents: Contents,
    #[case] known_without_io: Option<SelectedCheckpointType>,
    #[case] discovered: SelectedCheckpointType,
) {
    // Given a checkpoint that breaks a writer requirement but can still be read correctly.
    let store = write_table(v2_feature, naming, contents, false);
    let (engine, snapshot) = load(&store);
    // When the type is looked up without I/O, naming and protocol decide where they can.
    assert_eq!(snapshot.known_checkpoint_type(), known_without_io);
    // When discovery reads it, there is no error, and the contents decide a classic-named type.
    let info = snapshot
        .reader()
        .get_checkpoint_info(&engine)
        .unwrap()
        .unwrap();
    assert_eq!(info.checkpoint_type(), discovered);
    assert_eq!(
        snapshot.reader().get_checkpoint_type(&PanicEngine).unwrap(),
        Some(discovered)
    );
    assert_eq!(scanned_files(&snapshot, &engine), 2);
}

#[rstest]
#[case::metadata_version_mismatch(Contents::MetadataVersionMismatch, "version mismatch")]
#[case::duplicate_metadata(Contents::DuplicateMetadata, "Duplicate checkpointMetadata")]
fn checkpoint_kernel_cannot_read_correctly_fails_discovery(
    #[case] contents: Contents,
    #[case] message: &str,
) {
    // Given a checkpoint whose checkpointMetadata cannot describe it unambiguously.
    let store = write_table(true, Naming::Classic, contents, false);
    let (engine, snapshot) = load(&store);
    // When discovery reads it.
    let error = snapshot.reader().get_checkpoint_info(&engine).unwrap_err();
    // Then discovery reports the error, and the snapshot caches nothing.
    assert!(
        matches!(error.without_backtrace(), KernelError::InvalidCheckpoint(m) if m.contains(message)),
        "expected {message:?} in {error}"
    );
    assert!(snapshot.cached_checkpoint_info().is_none());
}

// TODO(#4048): Kernel trusts a stale `_last_checkpoint` schema for a replaced classic-named
// checkpoint, so it hides the sidecars and reports V1. Remove the `ignore` once that is fixed.
#[rstest]
#[case::classic_v2_sidecars(Naming::Classic, Contents::V2Sidecars, V2, 2)]
#[case::classic_v2_inline(Naming::Classic, Contents::V2Inline, V2, 2)]
#[ignore = "TODO(#4048): stale `_last_checkpoint` schema hides a replaced checkpoint's sidecars"]
fn stale_last_checkpoint_schema_is_ignored(
    #[case] naming: Naming,
    #[case] contents: Contents,
    #[case] expected: SelectedCheckpointType,
    #[case] files: usize,
) {
    // Given a `_last_checkpoint` schema that omits checkpointMetadata and sidecar, as an earlier
    // V1 file with the same name would have had.
    let v2_feature = naming != Naming::MultiPart;
    let store = write_table(v2_feature, naming, contents, true);
    let (engine, snapshot) = load(&store);
    // When the type is looked up and the table is scanned.
    let checkpoint_type = snapshot.reader().get_checkpoint_type(&engine).unwrap();
    // Then the checkpoint's own contents decide, and no sidecar files are skipped.
    assert_eq!(checkpoint_type, Some(expected));
    assert_eq!(scanned_files(&snapshot, &engine), files);
}

#[test]
fn snapshot_hint_type_is_used_until_the_checkpoint_is_read() {
    // Given a classic-named V2 checkpoint, rebuilt from a snapshot hint that claims V1.
    let store = write_table(true, Naming::Classic, Contents::V2Inline, false);
    let (engine, snapshot) = load(&store);
    let hint = snapshot
        .to_snapshot_hint()
        .unwrap()
        .with_checkpoint_type(Some(V1));
    let hinted = Snapshot::builder_for(ROOT)
        .with_snapshot_hint(hint)
        .build(&PanicEngine)
        .unwrap();
    // When the type is looked up before anything reads the checkpoint, the snapshot hint's
    // type is reported without I/O.
    assert_eq!(
        hinted.reader().get_checkpoint_type(&PanicEngine).unwrap(),
        Some(V1)
    );
    // When discovery reads the checkpoint, its contents win without an error.
    let info = hinted
        .reader()
        .get_checkpoint_info(&engine)
        .unwrap()
        .unwrap();
    assert_eq!(info.checkpoint_type(), V2);
    assert_eq!(
        hinted.reader().get_checkpoint_type(&PanicEngine).unwrap(),
        Some(V2)
    );
}

#[rstest]
#[case::multi_part_ignores_hint(Naming::MultiPart, Contents::V1, false, V2, V1)]
#[case::uuid_ignores_hint(Naming::UuidParquet, Contents::V2Inline, true, V1, V2)]
#[case::protocol_ignores_hint(Naming::Classic, Contents::V1, false, V2, V1)]
fn snapshot_hint_type_is_ignored_when_naming_or_protocol_decide(
    #[case] naming: Naming,
    #[case] contents: Contents,
    #[case] v2_feature: bool,
    #[case] hinted_type: SelectedCheckpointType,
    #[case] expected: SelectedCheckpointType,
) {
    // Given a snapshot hint whose type disagrees with the checkpoint's naming or table protocol.
    let store = write_table(v2_feature, naming, contents, false);
    let (_engine, snapshot) = load(&store);
    let hint = snapshot
        .to_snapshot_hint()
        .unwrap()
        .with_checkpoint_type(Some(hinted_type));
    // When the snapshot is built from it.
    let hinted = Snapshot::builder_for(ROOT)
        .with_snapshot_hint(hint)
        .build(&PanicEngine)
        .unwrap();
    // Then naming and protocol decide, without I/O or an error.
    assert_eq!(
        hinted.reader().get_checkpoint_type(&PanicEngine).unwrap(),
        Some(expected)
    );
}
