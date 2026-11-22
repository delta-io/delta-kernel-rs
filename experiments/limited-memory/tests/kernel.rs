#[path = "../examples/support/mod.rs"]
mod support;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::memory::{MemoryBudget, MemoryError};
use delta_kernel::schema::{MetadataValue, StructType};
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{Error, KernelError, Snapshot};

fn is_memory(error: &KernelError) -> bool {
    matches!(error.without_backtrace(), KernelError::Memory(_))
}

#[test]
fn actual_schema_decode_uses_budgeted_metadata_and_preserves_wire_semantics() {
    let input = r#"{"type":"struct","fields":[{"name":"col","type":"long","nullable":true,"metadata":{"comment":"payload","delta.columnMapping.physicalName":"physical","delta.columnMapping.id":1}}]}"#;
    let ordinary: StructType = serde_json::from_str(input).unwrap();
    let budget = MemoryBudget::new(4096);
    let parsed: StructType = budget.with(|| serde_json::from_str(input)).unwrap();
    assert_eq!(parsed, ordinary);
    let metadata = &parsed.field("col").unwrap().metadata;
    assert!(matches!(
        metadata["comment"],
        MetadataValue::LimitedString(_)
    ));
    assert_eq!(metadata["comment"].as_string(), Some("payload"));
    assert!(budget.usage().reserved > 0);
    let cloned = parsed.clone();
    drop(parsed);
    assert!(budget.usage().reserved > 0);
    assert_eq!(
        serde_json::to_value(&cloned).unwrap(),
        serde_json::from_str::<serde_json::Value>(input).unwrap()
    );
    drop(cloned);
    assert_eq!(budget.usage().reserved, 0);
    let small = MemoryBudget::new(0);
    assert!(
        small
            .with(|| serde_json::from_str::<StructType>(input))
            .is_err()
    );
    assert_eq!(small.usage().reserved, 0);
}

#[test]
fn real_snapshot_and_scan_complete_and_free_budgeted_owners() {
    let fixture = support::Fixture::create(10, 10, 20, Some(16_384)).unwrap();
    assert!(fixture.directory.path().is_dir());
    assert_eq!(
        (
            fixture.width,
            fixture.initial_files,
            fixture.tail_commits,
            fixture.schema_bytes
        ),
        (10, 10, 20, 16_384)
    );
    let engine = SyncEngine::new();
    let budget = MemoryBudget::new(8 * 1024 * 1024);
    let snapshot = budget
        .with(|| Snapshot::builder_for(&fixture.table_root).build(&engine))
        .unwrap();
    assert!(budget.usage().reserved > 0);
    let count = budget
        .with(|| -> delta_kernel::Result<_> {
            let scan = snapshot.clone().scan_builder().build()?;
            let mut count = 0;
            for batch in scan.scan_metadata(&engine)? {
                count = batch?.visit_scan_files(count, |count, _| *count += 1)?;
            }
            Ok(count)
        })
        .unwrap();
    assert_eq!(count, 30);
    assert!(budget.usage().peak <= budget.usage().limit);
    drop(snapshot);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn snapshot_exhaustion_does_not_substitute_an_older_snapshot() {
    let fixture = support::Fixture::create(2, 2, 3, None).unwrap();
    let engine = SyncEngine::new();
    let budget = MemoryBudget::new(0);
    let error = budget
        .with(|| Snapshot::builder_for(&fixture.table_root).build(&engine))
        .unwrap_err();
    assert!(
        matches!(error, Error::Kernel(ref inner) if is_memory(inner)),
        "{error}"
    );
    assert_eq!(budget.usage().reserved, 0);
    assert_eq!(
        Snapshot::builder_for(&fixture.table_root)
            .build(&engine)
            .unwrap()
            .version(),
        4
    );
}

#[test]
fn actual_scan_budget_failures_release_partial_replay_state() {
    let fixture = support::Fixture::create(2, 2, 100, None).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(&fixture.table_root)
        .build(&engine)
        .unwrap();
    let mut failures = 0;
    for cap in [0, 256, 1024, 4096, 16_384, 65_536, 262_144] {
        let budget = MemoryBudget::new(cap);
        let result = budget.with(|| -> delta_kernel::Result<_> {
            let scan = snapshot.clone().scan_builder().build()?;
            let mut count = 0;
            for batch in scan.scan_metadata(&engine)? {
                count = batch?.visit_scan_files(count, |count, _| *count += 1)?;
            }
            Ok(count)
        });
        match result {
            Ok(count) => assert_eq!(count, 102),
            Err(error) => {
                assert!(
                    matches!(error, Error::Kernel(ref inner) if is_memory(inner)),
                    "{error}"
                );
                failures += 1;
            }
        }
        assert!(budget.usage().peak <= cap);
        assert_eq!(budget.usage().reserved, 0, "cap={cap}");
    }
    assert!(failures >= 3);
}

#[test]
fn actual_commit_exhaustion_publishes_no_commit_and_retry_can_succeed() {
    let fixture = support::Fixture::create(2, 2, 3, None).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(&fixture.table_root)
        .build(&engine)
        .unwrap();
    let transaction = snapshot
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "poc", false)
        .with_table_properties_added([("poc.large", "x".repeat(32_768))])
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap();
    let budget = MemoryBudget::new(0);
    let result = budget.with(|| transaction.commit(&engine));
    assert!(result.is_err());
    assert_eq!(budget.usage().reserved, 0);
    assert!(budget.usage().rejected > 0);
    assert_eq!(
        Snapshot::builder_for(&fixture.table_root)
            .build(&engine)
            .unwrap()
            .version(),
        snapshot.version()
    );
    let transaction = snapshot
        .clone()
        .update_table(UpdateTableOperation::AlterTable, "poc", false)
        .with_table_properties_added([("poc.retry", "ok")])
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap();
    let budget = MemoryBudget::new(1024 * 1024);
    let committed = budget
        .with(|| transaction.commit(&engine))
        .unwrap()
        .unwrap_committed();
    assert_eq!(committed.commit_version(), snapshot.version() + 1);
    drop(committed);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn actual_write_state_output_is_fallible_and_retains_its_charge() {
    let fixture = support::Fixture::create(2, 2, 1, None).unwrap();
    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder_for(&fixture.table_root)
        .build(&engine)
        .unwrap();
    let transaction = snapshot
        .update_table(UpdateTableOperation::AlterTable, "poc", false)
        .with_table_properties_added([("poc.write-state", "true")])
        .build(&engine, Box::new(FileSystemCommitter::new()))
        .unwrap();
    let state = transaction.write_state().unwrap();
    let empty = MemoryBudget::new(0);
    assert!(matches!(
        state.encode_with_budget(&empty),
        Err(KernelError::Memory(MemoryError::AllocationFailed { .. }))
    ));
    assert_eq!(empty.usage().reserved, 0);
    let budget = MemoryBudget::new(1024 * 1024);
    let encoded = state.encode_with_budget(&budget).unwrap();
    assert_eq!(&encoded[..], state.encode().unwrap());
    assert_eq!(encoded.capacity(), budget.usage().reserved);
    let _decoded = delta_kernel::transaction::WriteState::decode(&encoded).unwrap();
    let error = empty
        .with(|| delta_kernel::transaction::WriteState::decode(&encoded))
        .unwrap_err();
    assert!(is_memory(&error), "{error}");
    drop(encoded);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn real_concurrent_queries_share_one_limit_and_release_escaped_snapshots() {
    let fixture = support::Fixture::create(10, 10, 10, Some(65_536)).unwrap();
    let probe = MemoryBudget::new(8 * 1024 * 1024);
    let snapshot = probe
        .with(|| Snapshot::builder_for(&fixture.table_root).build(&SyncEngine::new()))
        .unwrap();
    let retained = probe.usage().reserved;
    assert!(retained > 32_768);
    drop(snapshot);
    let budget = MemoryBudget::new(probe.usage().peak * 2);
    assert!(budget.usage().limit < retained * 4);
    // Hold one successful query's snapshot while three workers contend for the remainder.
    let retained_snapshot = budget
        .with(|| Snapshot::builder_for(&fixture.table_root).build(&SyncEngine::new()))
        .unwrap();
    let start = std::sync::Barrier::new(3);
    let finish = std::sync::Barrier::new(3);
    let successes = 1 + std::thread::scope(|scope| {
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let budget = &budget;
                let table_root = &fixture.table_root;
                let start = &start;
                let finish = &finish;
                scope.spawn(move || {
                    start.wait();
                    let result =
                        budget.with(|| Snapshot::builder_for(table_root).build(&SyncEngine::new()));
                    finish.wait();
                    match result {
                        Ok(snapshot) => {
                            drop(snapshot);
                            1
                        }
                        Err(error) => {
                            assert!(
                                matches!(error, Error::Kernel(ref inner) if is_memory(inner)),
                                "{error}"
                            );
                            0
                        }
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum::<usize>()
    });
    drop(retained_snapshot);
    assert!((1..4).contains(&successes));
    assert!(budget.usage().peak <= budget.usage().limit);
    assert!(budget.usage().rejected > 0);
    assert_eq!(budget.usage().reserved, 0);
}

#[test]
fn commit_cap_sweep_never_reports_exhaustion_after_publication() {
    for cap in [0, 256, 4096, 32_768, 65_536, 131_072, 262_144, 1_048_576] {
        let fixture = support::Fixture::create(2, 2, 1, Some(16_384)).unwrap();
        let engine = SyncEngine::new();
        let snapshot = Snapshot::builder_for(&fixture.table_root)
            .build(&engine)
            .unwrap();
        let transaction = snapshot
            .clone()
            .update_table(UpdateTableOperation::AlterTable, "poc", false)
            .with_table_properties_added([("poc.sweep", "true")])
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap();
        let budget = MemoryBudget::new(cap);
        let result = budget.with(|| transaction.commit(&engine));
        let actual = Snapshot::builder_for(&fixture.table_root)
            .build(&engine)
            .unwrap();
        match result {
            Ok(committed) => {
                let committed = committed.unwrap_committed();
                assert_eq!(committed.commit_version(), snapshot.version() + 1);
                assert_eq!(actual.version(), committed.commit_version());
                assert_eq!(
                    actual.schema(),
                    committed.post_commit_snapshot().unwrap().schema()
                );
                assert_eq!(
                    actual.table_properties(),
                    committed.post_commit_snapshot().unwrap().table_properties()
                );
                drop(committed);
            }
            Err(error) => {
                assert!(
                    matches!(error, Error::Kernel(ref inner) if is_memory(inner)),
                    "{error}"
                );
                assert_eq!(
                    actual.version(),
                    snapshot.version(),
                    "cap={cap}: error after publish"
                );
            }
        }
        assert!(budget.usage().peak <= cap);
        assert_eq!(budget.usage().reserved, 0);
    }
}

#[test]
fn serialized_replay_keys_keep_budgeted_ownership_and_identity() {
    let budget = MemoryBudget::new(16_384);
    let mut keys = delta_kernel::memory::FileKeySet::new(&budget);
    keys.try_insert("path", Some("dv-1")).unwrap();
    keys.try_insert("path", Some("dv-2")).unwrap();
    keys.try_insert("other", None).unwrap();
    let encoded = serde_json::to_vec(&keys).unwrap();
    drop(keys);
    assert_eq!(budget.usage().reserved, 0);
    let restored: delta_kernel::memory::FileKeySet =
        budget.with(|| serde_json::from_slice(&encoded)).unwrap();
    assert_eq!(restored.len(), 3);
    assert!(restored.contains("path", Some("dv-1")));
    assert!(restored.contains("path", Some("dv-2")));
    assert!(restored.contains("other", None));
    assert!(budget.usage().reserved > 0);
    drop(restored);
    assert_eq!(budget.usage().reserved, 0);
    assert!(
        MemoryBudget::new(0)
            .with(|| serde_json::from_slice::<delta_kernel::memory::FileKeySet>(&encoded))
            .is_err()
    );
}

#[test]
fn limited_commit_json_matches_arrow_for_nested_values_and_selection() {
    use std::sync::Arc;

    use delta_kernel::arrow::array::{
        ArrayRef, Int64Array, ListArray, MapBuilder, StringArray, StringBuilder,
    };
    use delta_kernel::arrow::datatypes::Int64Type;
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::{Engine, FilteredEngineData};

    let directory = tempfile::tempdir().unwrap();
    let root = delta_kernel::try_parse_uri(directory.path().to_str().unwrap()).unwrap();
    let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    map.keys().append_value("null-value");
    map.values().append_null();
    map.append(true).unwrap();
    map.append(false).unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "name",
            Arc::new(StringArray::from(vec![Some("a\"\\\n"), None])) as ArrayRef,
        ),
        ("count", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        (
            "list",
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(1), None]),
                Some(vec![]),
            ])) as ArrayRef,
        ),
        ("map", Arc::new(map.finish()) as ArrayRef),
    ])
    .unwrap();
    let engine = SyncEngine::new();
    for selection in [
        vec![],
        vec![true, false],
        vec![false, true],
        vec![false, false],
    ] {
        let normal = root.join("normal.json").unwrap();
        let limited = root.join("limited.json").unwrap();
        let make_data = || {
            Box::new(std::iter::once(Ok(FilteredEngineData::try_new(
                Box::new(ArrowEngineData::new(batch.clone())),
                selection.clone(),
            )
            .unwrap())))
                as Box<dyn delta_kernel::KernelResultIterator<FilteredEngineData>>
        };
        engine
            .json_handler()
            .write_json_file(&normal, make_data(), true)
            .unwrap();
        let budget = MemoryBudget::new(16_384);
        budget
            .with(|| {
                engine
                    .json_handler()
                    .write_json_file(&limited, make_data(), true)
            })
            .unwrap();
        let read = |path: &std::path::Path| {
            std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            read(&normal.to_file_path().unwrap()),
            read(&limited.to_file_path().unwrap())
        );
        assert!(budget.usage().peak <= budget.usage().limit);
        assert_eq!(budget.usage().reserved, 0);
    }
}

#[test]
fn scoped_snapshot_scan_and_commit_preserve_name_and_id_column_mapping() {
    use std::sync::Arc;

    use delta_kernel::schema::{DataType, StructField};
    use delta_kernel::transaction::create_table::create_table;

    for mode in ["name", "id"] {
        let directory = tempfile::tempdir().unwrap();
        let root = delta_kernel::try_parse_uri(directory.path().to_str().unwrap())
            .unwrap()
            .to_string();
        let engine = SyncEngine::new();
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("logical", DataType::LONG)]).unwrap(),
        );
        create_table(&root, schema, "poc", true)
            .with_table_properties([("delta.columnMapping.mode", mode)])
            .build(&engine, Box::new(FileSystemCommitter::new()))
            .unwrap()
            .commit(&engine)
            .unwrap()
            .unwrap_committed();
        let ordinary = Snapshot::builder_for(&root).build(&engine).unwrap();
        let budget = MemoryBudget::new(1024 * 1024);
        budget.with(|| {
            let snapshot = Snapshot::builder_for(&root).build(&engine).unwrap();
            assert_eq!(snapshot.schema(), ordinary.schema());
            let scan = snapshot.clone().scan_builder().build().unwrap();
            for batch in scan.scan_metadata(&engine).unwrap() {
                batch.unwrap();
            }
            let committed = snapshot
                .update_table(UpdateTableOperation::AlterTable, "poc", false)
                .with_table_properties_added([("poc.mapped", "true")])
                .build(&engine, Box::new(FileSystemCommitter::new()))
                .unwrap()
                .commit(&engine)
                .unwrap()
                .unwrap_committed();
            assert_eq!(
                committed.post_commit_snapshot().unwrap().schema(),
                ordinary.schema()
            );
        });
        assert_eq!(budget.usage().reserved, 0);
    }
}
