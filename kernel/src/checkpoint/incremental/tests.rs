use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::actions::{Add, DomainMetadata, Protocol, Remove, SetTransaction};
use crate::arrow::array::{AsArray, StringArray};
use crate::arrow::json::writer::{JsonArray, WriterBuilder};
use crate::checkpoint::metrics::CheckpointMetrics;
use crate::checkpoint::tests::{
    create_metadata_action, create_v2_checkpoint_protocol_action, new_in_memory_store,
    write_commit_to_store,
};
use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use crate::engine::DelegatingEngine;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::object_store::{ObjectStore, ObjectStoreExt};
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::unit_test_utils::{
    Action, create_add_action, create_remove_action, serialize_actions, string_array_to_engine_data,
};
use crate::{
    Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize, KernelResult,
    ParquetFooter, ParquetHandler, PredicateRef, SchemaRef, Snapshot,
};

type BeforeSidecars = Box<dyn FnOnce() -> KernelResult<()> + Send>;

struct ReadCall {
    files: Vec<Url>,
    schema: SchemaRef,
    batches: Arc<AtomicUsize>,
}

struct ReadProbe {
    inner: Arc<dyn ParquetHandler>,
    before_sidecars: Mutex<Option<BeforeSidecars>>,
    reads: Mutex<Vec<ReadCall>>,
    path_only_after: Option<usize>,
}

impl ParquetHandler for ReadProbe {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        let batches = Arc::new(AtomicUsize::new(0));
        self.reads.lock().unwrap().push(ReadCall {
            files: files.iter().map(|f| f.location.clone()).collect(),
            schema: schema.clone(),
            batches: batches.clone(),
        });
        if files
            .iter()
            .any(|f| f.location.path().contains("/_sidecars/"))
            && let Some(hook) = self.before_sidecars.lock().unwrap().take()
        {
            hook()?;
        }
        let path_only_after = self
            .path_only_after
            .filter(|_| schema.contains_col(["_file"]));
        let input = self.inner.read_parquet_files(files, schema, predicate)?;
        Ok(Box::new(input.enumerate().map(move |(index, data)| {
            batches.fetch_add(1, Ordering::Relaxed);
            let data = data?;
            if path_only_after.is_none_or(|start| index < start) {
                return Ok(data);
            }
            let arrow = ArrowEngineData::try_from_engine_data(data)?;
            let batch = arrow.record_batch();
            let index = batch.schema().index_of("_file")?;
            let paths = batch.column(index).as_string::<i32>();
            let paths = StringArray::from_iter_values(
                paths
                    .iter()
                    .map(|path| Url::parse(path.unwrap()).unwrap().path().to_owned()),
            );
            let mut columns = batch.columns().to_vec();
            columns[index] = Arc::new(paths);
            let batch = crate::arrow::array::RecordBatch::try_new(batch.schema(), columns)?;
            Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
        })))
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }

    fn write_parquet_file(
        &self,
        path: Url,
        data: FileDataReadResultIterator,
    ) -> KernelResult<FileSize> {
        self.inner.write_parquet_file(path, data)
    }
}

fn checkpoint_spec(reuse_eligible_sidecars: bool) -> CheckpointSpec {
    CheckpointSpec::V2(if reuse_eligible_sidecars {
        V2CheckpointConfig::ReuseEligibleSidecars {
            file_actions_per_sidecar_hint: Some(1),
        }
    } else {
        V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }
    })
}

fn add_with_dv(path: &str) -> Action {
    Action::Add(Add {
        path: path.into(),
        data_change: true,
        deletion_vector: Some(DeletionVectorDescriptor {
            storage_type: DeletionVectorStorageType::PersistedAbsolute,
            path_or_inline_dv: "file:///unused-dv.bin".into(),
            offset: Some(1),
            size_in_bytes: 1,
            cardinality: 1,
        }),
        stats: Some(r#"{"numRecords":2,"tightBounds":false}"#.into()),
        ..Default::default()
    })
}

async fn parquet_rows(store: &InMemory, path: &str) -> Vec<Value> {
    parquet_rows_with_nulls(store, path, false).await
}

async fn parquet_rows_with_nulls(store: &InMemory, path: &str, explicit_nulls: bool) -> Vec<Value> {
    let bytes = store
        .get(&Path::from(path))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(explicit_nulls)
        .build::<_, JsonArray>(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    serde_json::from_slice(&writer.into_inner()).unwrap()
}

async fn checkpoint_rows(store: &InMemory, version: u64) -> (Vec<Value>, Vec<Value>) {
    let root = parquet_rows(
        store,
        &format!("_delta_log/{version:020}.checkpoint.parquet"),
    )
    .await;
    let mut leaves = Vec::new();
    for path in sidecars(&root) {
        leaves.extend(parquet_rows(store, &format!("_delta_log/_sidecars/{path}")).await);
    }
    (root, leaves)
}

fn sidecars(root: &[Value]) -> HashSet<String> {
    root.iter()
        .filter_map(|row| row["sidecar"]["path"].as_str().map(str::to_owned))
        .collect()
}

async fn assert_counts(store: &InMemory, version: u64) {
    let (root, leaves) = checkpoint_rows(store, version).await;
    let hint: Value = serde_json::from_slice(
        &store
            .get(&Path::from("_delta_log/_last_checkpoint"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    let mut bytes = store
        .head(&Path::from(format!(
            "_delta_log/{version:020}.checkpoint.parquet"
        )))
        .await
        .unwrap()
        .size;
    for path in sidecars(&root) {
        bytes += store
            .head(&Path::from(format!("_delta_log/_sidecars/{path}")))
            .await
            .unwrap()
            .size;
    }
    let adds = leaves.iter().filter(|row| row.get("add").is_some()).count();
    assert_eq!(hint["size"], json!(root.len() + leaves.len()));
    assert_eq!(hint["sizeInBytes"], json!(bytes));
    assert_eq!(hint["numOfAddFiles"], json!(adds));
    let metadata: Vec<_> = root
        .iter()
        .filter_map(|row| row.get("checkpointMetadata"))
        .collect();
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0]["version"], json!(version));
    let mut keys = HashSet::new();
    for row in &leaves {
        let action = row.get("add").or_else(|| row.get("remove")).unwrap();
        assert!(
            keys.insert((
                action["path"].to_string(),
                action["deletionVector"].to_string()
            )),
            "duplicate logical file: {row}"
        );
    }
}

#[rstest]
#[case::append(vec![vec![create_add_action("D")]], true)]
#[case::tail_compaction(vec![vec![create_add_action("D")],
    vec![create_remove_action("D"), create_add_action("E")]], true)]
#[case::inherited_remove(vec![vec![create_remove_action("A"), create_add_action("D")]], false)]
#[case::stats_readd(vec![vec![Action::Add(Add { path: "A".into(),
    data_change: false, stats: Some(r#"{"numRecords":7}"#.into()), ..Default::default() })]], false)]
#[case::remove_then_readd(vec![vec![create_remove_action("A")],
    vec![create_add_action("A")]], false)]
#[case::readd_then_remove(vec![vec![create_add_action("A")],
    vec![create_remove_action("A")]], false)]
#[case::metadata_change(vec![vec![create_metadata_action()]], false)]
#[case::protocol_change(vec![vec![create_v2_checkpoint_protocol_action()]], false)]
#[case::restore_tombstone(vec![vec![create_add_action("old")]], false)]
#[case::tail_dv_replacement(vec![vec![create_add_action("D")],
    vec![add_with_dv("D"), create_remove_action("D")]], true)]
#[case::inherited_dv_replacement(vec![vec![add_with_dv("A"), create_remove_action("A")]], false)]
#[case::inherited_dv_replacement_with_expired_remove(vec![vec![add_with_dv("A"),
    Action::Remove(Remove { path: "A".into(), deletion_timestamp: Some(0),
        data_change: true, ..Default::default() })]], false)]
#[case::non_file_updates(vec![vec![
    Action::Txn(SetTransaction { app_id: "app".into(), version: 2, last_updated: Some(i64::MAX) }),
    Action::DomainMetadata(DomainMetadata::remove("test.domain".into(), "{}".into())),
]], true)]
#[tokio::test]
async fn reuse_matches_full_checkpoint(
    #[case] commits: Vec<Vec<Action>>,
    #[case] expect_reuse: bool,
    #[values(false, true)] json_root: bool,
    #[values(false, true)] separate_sidecar: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let base_version = u64::from(separate_sidecar);
    let configuration_changed = commits
        .iter()
        .flatten()
        .any(|action| matches!(action, Action::Metadata(_) | Action::Protocol(_)));
    let mut base_actions = vec![
        create_metadata_action(),
        Action::Protocol(Protocol::try_new_modern(
            ["v2Checkpoint", "deletionVectors"],
            ["v2Checkpoint", "deletionVectors", "domainMetadata"],
        )?),
        create_add_action("A"),
        create_remove_action("old"),
        Action::Txn(SetTransaction {
            app_id: "app".into(),
            version: 1,
            last_updated: Some(i64::MAX),
        }),
        Action::DomainMetadata(DomainMetadata::new("test.domain".into(), "{}".into())),
    ];
    if !separate_sidecar {
        base_actions.push(create_add_action("B"));
    }
    write_commit_to_store(&store, base_actions, 0).await?;
    if separate_sidecar {
        write_commit_to_store(&store, vec![create_add_action("B")], 1).await?;
    }
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    snapshot.checkpoint(&engine, Some(&checkpoint_spec(false)))?;
    let (base, _) = checkpoint_rows(&store, base_version).await;
    let base_sidecars = sidecars(&base);
    let parquet_root = Path::from(format!("_delta_log/{base_version:020}.checkpoint.parquet"));
    let base_path = if json_root {
        let json_path = Path::from(format!(
            "_delta_log/{base_version:020}.checkpoint.{}.json",
            uuid::Uuid::new_v4()
        ));
        let contents = base
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        store.put(&json_path, contents.into()).await?;
        store.delete(&parquet_root).await?;
        store
            .delete(&Path::from("_delta_log/_last_checkpoint"))
            .await?;
        json_path
    } else {
        parquet_root
    };
    let version = base_version + commits.len() as u64;
    for (index, actions) in commits.into_iter().enumerate() {
        write_commit_to_store(&store, actions, base_version + index as u64 + 1).await?;
    }

    // Independent object namespaces prevent the full writer from overwriting the candidate root.
    let reference = Arc::new(InMemory::new());
    for file in store.list(None).try_collect::<Vec<_>>().await? {
        let data = store.get(&file.location).await?.bytes().await?;
        reference.put(&file.location, data.into()).await?;
    }
    let reference_engine = SyncEngine::new_with_store(reference.clone());
    Snapshot::builder_for("memory:///")
        .build(&reference_engine)?
        .checkpoint(&reference_engine, Some(&checkpoint_spec(false)))?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    let (_, updated) = target.checkpoint(&engine, Some(&checkpoint_spec(true)))?;
    assert_eq!(updated.version(), version);
    let (root, mut leaves) = checkpoint_rows(&store, version).await;
    if expect_reuse {
        assert!(base_sidecars.is_subset(&sidecars(&root)));
    } else {
        assert_eq!(
            base_sidecars.intersection(&sidecars(&root)).count(),
            usize::from(separate_sidecar && !configuration_changed)
        );
    }
    let (reference_root, mut reference_leaves) = checkpoint_rows(&reference, version).await;
    leaves.sort_by_key(Value::to_string);
    reference_leaves.sort_by_key(Value::to_string);
    assert_eq!(leaves, reference_leaves);
    let non_files = |rows: Vec<Value>| {
        let mut rows: Vec<_> = rows
            .into_iter()
            .filter(|row| row.get("sidecar").is_none() && row.get("checkpointMetadata").is_none())
            .collect();
        rows.sort_by_key(Value::to_string);
        rows
    };
    assert_eq!(non_files(root), non_files(reference_root));
    assert_counts(&store, version).await;

    // Force a fresh reader to depend on the new root and its sidecars, with no old root or JSONs.
    for old_version in 0..=version {
        store
            .delete(&Path::from(format!("_delta_log/{old_version:020}.json")))
            .await?;
    }
    store.delete(&base_path).await?;
    let fresh = Snapshot::builder_for("memory:///").build(&engine)?;
    assert_eq!(fresh.log_segment().checkpoint_version, Some(version));
    let paths =
        |snapshot: &crate::SnapshotRef, engine: &SyncEngine| -> crate::Result<Vec<String>> {
            let mut paths = Vec::new();
            for batch in snapshot
                .clone()
                .scan_builder()
                .build()?
                .scan_metadata(engine)?
            {
                paths = batch?.visit_scan_files(paths, |paths, file| paths.push(file.path))?;
            }
            paths.sort();
            Ok(paths)
        };
    let reference_snapshot = Snapshot::builder_for("memory:///").build(&reference_engine)?;
    assert_eq!(
        paths(&fresh, &engine)?,
        paths(&reference_snapshot, &reference_engine)?
    );
    Ok(())
}

#[tokio::test]
async fn reused_tombstones_expire_and_force_full_checkpoint()
-> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
            Action::Remove(Remove {
                path: "old".into(),
                deletion_timestamp: Some(100),
                data_change: true,
                ..Default::default()
            }),
        ],
        0,
    )
    .await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let mut writer = snapshot.create_checkpoint_writer(&engine)?;
    writer.minimum_file_retention_timestamp = 0;
    let info =
        writer.write_v2_checkpoint_with_sidecars(&engine, 1, CheckpointMetrics::disabled())?;
    writer.finalize(&engine, &info.last_checkpoint_stats)?;
    let (base, _) = checkpoint_rows(&store, 0).await;
    write_commit_to_store(&store, vec![create_add_action("D")], 1).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let mut writer = snapshot.create_checkpoint_writer(&engine)?;
    writer.minimum_file_retention_timestamp = 100;
    let info = writer.write_v2_checkpoint_reusing_eligible_sidecars(
        &engine,
        1,
        CheckpointMetrics::disabled(),
    )?;
    writer.finalize(&engine, &info.last_checkpoint_stats)?;
    let (root, leaves) = checkpoint_rows(&store, 1).await;
    assert!(sidecars(&base).is_disjoint(&sidecars(&root)));
    assert!(leaves.iter().all(|row| row.get("remove").is_none()));
    assert_counts(&store, 1).await;
    Ok(())
}

#[tokio::test]
async fn repeated_reuse_with_cached_snapshot_and_no_hint_counts()
-> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
        ],
        0,
    )
    .await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let (_, mut snapshot) = snapshot.checkpoint(&engine, Some(&checkpoint_spec(true)))?;
    let (root, _) = checkpoint_rows(&store, 0).await;
    let first_sidecars = sidecars(&root);
    for version in 1..=3 {
        let action = serialize_actions([create_add_action(&format!("file-{version}"))]);
        store
            .put(
                &Path::from(format!("_delta_log/{version:020}.json")),
                action.into(),
            )
            .await?;
        snapshot = Snapshot::builder_from(snapshot).build(&engine)?;
        (_, snapshot) = snapshot.checkpoint(&engine, Some(&checkpoint_spec(true)))?;
        assert!(snapshot.log_segment().checkpoint_hint().is_none());
        let (root, _) = checkpoint_rows(&store, version).await;
        assert!(first_sidecars.is_subset(&sidecars(&root)));
        assert_counts(&store, version).await;
    }
    Ok(())
}

#[rstest]
#[case::reuse(false)]
#[case::fallback(true)]
#[tokio::test]
async fn captured_root_and_target_survive_changes_during_validation(
    #[case] remove_inherited: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
            Action::Txn(SetTransaction {
                app_id: "retained".into(),
                version: 1,
                last_updated: Some(100),
            }),
        ],
        0,
    )
    .await?;
    let base = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    base.checkpoint(engine.as_ref(), Some(&checkpoint_spec(false)))?;
    let root_path = Path::from("_delta_log/00000000000000000000.checkpoint.parquet");
    let original = store.get(&root_path).await?.bytes().await?;
    // Prepare a replacement of the same root with a later transaction-retention cutoff.
    let mut later_writer = base.create_checkpoint_writer(engine.as_ref())?;
    later_writer.txn_expiration_timestamp = Some(200);
    later_writer.write_v2_checkpoint_with_sidecars(
        engine.as_ref(),
        1,
        CheckpointMetrics::disabled(),
    )?;
    let replacement = store.get(&root_path).await?.bytes().await?;
    store.put(&root_path, original.into()).await?;
    write_commit_to_store(
        &store,
        vec![if remove_inherited {
            create_remove_action("A")
        } else {
            create_add_action("D")
        }],
        1,
    )
    .await?;
    let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let mut writer = target.create_checkpoint_writer(engine.as_ref())?;
    writer.txn_expiration_timestamp = Some(50);
    let storage = engine.storage_handler();
    let probe = Arc::new(ReadProbe {
        inner: engine.parquet_handler(),
        reads: Mutex::default(),
        path_only_after: None,
        before_sidecars: Mutex::new(Some(Box::new(move || {
            storage.put(
                &Url::parse("memory:///_delta_log/00000000000000000000.checkpoint.parquet")?,
                replacement,
                true,
            )?;
            storage.put(
                &Url::parse("memory:///_delta_log/00000000000000000002.json")?,
                serialize_actions([create_add_action("future")]).into(),
                false,
            )
        }))),
    });
    let observed = DelegatingEngine::new(engine.clone()).with_parquet_handler(probe.clone());
    let info = writer.write_v2_checkpoint_reusing_eligible_sidecars(
        &observed,
        1,
        CheckpointMetrics::disabled(),
    )?;
    writer.finalize(&observed, &info.last_checkpoint_stats)?;
    let (root, leaves) = checkpoint_rows(&store, 1).await;
    assert!(root.iter().any(|row| row["txn"]["appId"] == "retained"));
    assert!(leaves.iter().all(|row| row["add"]["path"] != "future"));
    assert_counts(&store, 1).await;
    let reads = probe.reads.lock().unwrap();
    assert_eq!(
        reads
            .iter()
            .flat_map(|call| &call.files)
            .filter(|path| path
                .path()
                .ends_with("00000000000000000000.checkpoint.parquet"))
            .count(),
        1
    );
    let sidecar_reads: Vec<_> = reads
        .iter()
        .filter(|call| call.files.iter().any(|p| p.path().contains("/_sidecars/")))
        .collect();
    assert_eq!(sidecar_reads.len(), if remove_inherited { 2 } else { 1 });
    assert!(!sidecar_reads[0].schema.contains_col(["add", "stats"]));
    Ok(())
}

#[tokio::test]
async fn validation_read_error_does_not_publish_checkpoint_or_hint()
-> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
        ],
        0,
    )
    .await?;
    Snapshot::builder_for("memory:///")
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), Some(&checkpoint_spec(false)))?;
    write_commit_to_store(&store, vec![create_add_action("D")], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let probe = Arc::new(ReadProbe {
        inner: engine.parquet_handler(),
        reads: Mutex::default(),
        path_only_after: None,
        before_sidecars: Mutex::new(Some(Box::new(|| {
            Err(crate::KernelError::generic("injected read error"))
        }))),
    });
    let observed = DelegatingEngine::new(engine).with_parquet_handler(probe);
    let result = target.checkpoint(&observed, Some(&checkpoint_spec(true)));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected read error")
    );
    assert!(
        store
            .head(&Path::from(
                "_delta_log/00000000000000000001.checkpoint.parquet"
            ))
            .await
            .is_err()
    );
    let hint: Value = serde_json::from_slice(
        &store
            .get(&Path::from("_delta_log/_last_checkpoint"))
            .await?
            .bytes()
            .await?,
    )?;
    assert_eq!(hint["version"], 0);
    Ok(())
}

#[tokio::test]
async fn inline_base_falls_back_after_first_file_batch()
-> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let mut actions = vec![
        create_metadata_action(),
        create_v2_checkpoint_protocol_action(),
    ];
    actions.extend((0..3000).map(|i| create_add_action(&format!("file-{i}"))));
    write_commit_to_store(&store, actions, 0).await?;
    Snapshot::builder_for("memory:///")
        .build(engine.as_ref())?
        .checkpoint(
            engine.as_ref(),
            Some(&CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)),
        )?;
    write_commit_to_store(&store, vec![create_add_action("tail")], 1).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let probe = Arc::new(ReadProbe {
        inner: engine.parquet_handler(),
        before_sidecars: Mutex::default(),
        reads: Mutex::default(),
        path_only_after: None,
    });
    let observed = DelegatingEngine::new(engine.clone()).with_parquet_handler(probe.clone());
    snapshot.checkpoint(&observed, Some(&checkpoint_spec(true)))?;
    let (_, leaves) = checkpoint_rows(&store, 1).await;
    assert_eq!(leaves.len(), 3001);
    assert_counts(&store, 1).await;
    let reads = probe.reads.lock().unwrap();
    let root_reads: Vec<_> = reads
        .iter()
        .filter(|call| call.schema.contains_col(["add", "path"]))
        .filter(|call| {
            call.files.iter().any(|path| {
                path.path()
                    .ends_with("00000000000000000000.checkpoint.parquet")
            })
        })
        .collect();
    assert_eq!(root_reads.len(), 2);
    assert_eq!(root_reads[0].batches.load(Ordering::Relaxed), 1);
    assert!(root_reads[1].batches.load(Ordering::Relaxed) > 1);
    Ok(())
}

#[rstest]
#[case::overlap(false)]
#[case::expired_tombstone(true)]
#[tokio::test]
async fn late_rejection_preserves_other_sidecars_and_their_counts(
    #[case] expire: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let oldest = if expire {
        Action::Remove(Remove {
            path: "A".into(),
            deletion_timestamp: Some(100),
            data_change: true,
            ..Default::default()
        })
    } else {
        create_add_action("A")
    };
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            oldest,
        ],
        0,
    )
    .await?;
    write_commit_to_store(&store, vec![create_add_action("B")], 1).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let mut writer = snapshot.create_checkpoint_writer(&engine)?;
    writer.minimum_file_retention_timestamp = 0;
    let info =
        writer.write_v2_checkpoint_with_sidecars(&engine, 1, CheckpointMetrics::disabled())?;
    writer.finalize(&engine, &info.last_checkpoint_stats)?;
    let (base_root, _) = checkpoint_rows(&store, 1).await;
    let ordered_refs: Vec<_> = base_root
        .iter()
        .filter_map(|r| r["sidecar"]["path"].as_str())
        .collect();
    assert_eq!(ordered_refs.len(), 2);
    let first_rows =
        parquet_rows(&store, &format!("_delta_log/_sidecars/{}", ordered_refs[0])).await;
    let last_rows =
        parquet_rows(&store, &format!("_delta_log/_sidecars/{}", ordered_refs[1])).await;
    assert_eq!(first_rows[0]["add"]["path"], "B");
    assert_eq!(
        last_rows[0][if expire { "remove" } else { "add" }]["path"],
        "A"
    );
    let mut tail = vec![create_add_action("C")];
    if !expire {
        tail.push(create_remove_action("A"));
    }
    write_commit_to_store(&store, tail, 2).await?;
    let snapshot = Snapshot::builder_for("memory:///").build(&engine)?;
    let mut writer = snapshot.create_checkpoint_writer(&engine)?;
    writer.minimum_file_retention_timestamp = 100;
    let info = writer.write_v2_checkpoint_reusing_eligible_sidecars(
        &engine,
        1,
        CheckpointMetrics::disabled(),
    )?;
    assert_eq!(info.last_checkpoint_stats.num_of_add_files, 2);
    assert_eq!(info.num_remove_files, if expire { 0 } else { 1 });
    writer.finalize(&engine, &info.last_checkpoint_stats)?;
    let (root, leaves) = checkpoint_rows(&store, 2).await;
    assert!(sidecars(&root).contains(ordered_refs[0]));
    assert!(!sidecars(&root).contains(ordered_refs[1]));
    assert_eq!(info.num_sidecars, sidecars(&root).len() as u64);
    let mut adds: Vec<_> = leaves
        .iter()
        .filter_map(|r| r["add"]["path"].as_str())
        .collect();
    adds.sort();
    assert_eq!(adds, ["B", "C"]);
    let removes: Vec<_> = leaves
        .iter()
        .filter_map(|r| r["remove"]["path"].as_str())
        .collect();
    assert_eq!(removes, if expire { vec![] } else { vec!["A"] });
    assert_counts(&store, 2).await;
    Ok(())
}

#[rstest]
#[case::early_conflict(0)]
#[case::late_conflict(2999)]
#[tokio::test]
async fn batch_conflict_rewrites_whole_sidecar_and_preserves_later_clean_file(
    #[case] removed_index: usize,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    let mut actions = vec![
        create_metadata_action(),
        create_v2_checkpoint_protocol_action(),
    ];
    actions.extend((0..3000).map(|i| create_add_action(&format!("base-{i}"))));
    write_commit_to_store(&store, actions, 0).await?;
    Snapshot::builder_for("memory:///")
        .build(engine.as_ref())?
        .checkpoint(
            engine.as_ref(),
            Some(&CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
                file_actions_per_sidecar_hint: Some(10_000),
            })),
        )?;
    let (root, _) = checkpoint_rows(&store, 0).await;
    assert_eq!(sidecars(&root).len(), 1);
    let dirty = sidecars(&root).into_iter().next().unwrap();
    write_commit_to_store(&store, vec![create_add_action("clean")], 1).await?;
    Snapshot::builder_for("memory:///")
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), Some(&checkpoint_spec(true)))?;
    let (base, _) = checkpoint_rows(&store, 1).await;
    let clean = sidecars(&base).into_iter().find(|p| p != &dirty).unwrap();
    let ordered: Vec<_> = base
        .iter()
        .filter_map(|r| r["sidecar"]["path"].as_str())
        .collect();
    assert_eq!(ordered, [dirty.as_str(), clean.as_str()]);
    write_commit_to_store(
        &store,
        vec![
            create_remove_action(&format!("base-{removed_index}")),
            create_add_action("tail"),
        ],
        2,
    )
    .await?;
    let reference = Arc::new(InMemory::new());
    for file in store.list(None).try_collect::<Vec<_>>().await? {
        let bytes = store.get(&file.location).await?.bytes().await?;
        reference.put(&file.location, bytes.into()).await?;
    }
    let reference_engine = SyncEngine::new_with_store(reference.clone());
    let snapshot = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let probe = Arc::new(ReadProbe {
        inner: engine.parquet_handler(),
        reads: Mutex::default(),
        path_only_after: None,
        before_sidecars: Mutex::default(),
    });
    let observed = DelegatingEngine::new(engine.clone()).with_parquet_handler(probe.clone());
    let (_, updated) = snapshot.checkpoint(&observed, Some(&checkpoint_spec(true)))?;
    let (root, leaves) = checkpoint_rows(&store, 2).await;
    assert!(sidecars(&root).contains(&clean));
    assert!(!sidecars(&root).contains(&dirty));
    assert_eq!(
        leaves.iter().filter(|r| r.get("add").is_some()).count(),
        3001
    );
    assert_eq!(
        leaves.iter().filter(|r| r.get("remove").is_some()).count(),
        1
    );
    assert_counts(&store, 2).await;
    {
        let reads = probe.reads.lock().unwrap();
        let validation = reads
            .iter()
            .find(|r| {
                !r.schema.contains_col(["add", "partitionValues"])
                    && r.files.iter().any(|p| p.path().ends_with(&dirty))
            })
            .unwrap();
        assert!(validation.batches.load(Ordering::Relaxed) > 2);
        assert_eq!(validation.files.len(), 2);
        assert!(validation.schema.contains_col(["_file"]));
        let full_files: Vec<_> = reads
            .iter()
            .filter(|r| r.schema.contains_col(["add", "partitionValues"]))
            .flat_map(|r| &r.files)
            .filter(|p| p.path().contains("/_sidecars/"))
            .collect();
        assert_eq!(full_files.len(), 1);
        assert!(full_files[0].path().ends_with(&dirty));
    }
    Snapshot::builder_for("memory:///")
        .build(&reference_engine)?
        .checkpoint(&reference_engine, Some(&checkpoint_spec(false)))?;
    let (_, mut full) = checkpoint_rows(&reference, 2).await;
    let mut selective = leaves;
    full.sort_by_key(Value::to_string);
    selective.sort_by_key(Value::to_string);
    assert_eq!(selective, full);

    // The returned snapshot retains the selective root's descriptors after a classic-root
    // overwrite.
    write_commit_to_store(
        &store,
        vec![create_remove_action("clean"), create_add_action("next")],
        3,
    )
    .await?;
    let next = Snapshot::builder_from(updated).build(engine.as_ref())?;
    next.checkpoint(engine.as_ref(), Some(&checkpoint_spec(true)))?;
    let (next_root, _) = checkpoint_rows(&store, 3).await;
    let mut still_clean = sidecars(&root);
    still_clean.remove(&clean);
    assert!(still_clean.is_subset(&sidecars(&next_root)));
    assert!(!sidecars(&next_root).contains(&clean));
    assert_counts(&store, 3).await;
    Ok(())
}

#[rstest]
#[tokio::test]
async fn path_only_provenance_rewrites_all_sidecars_without_guessing(
    #[values(0, 1)] path_only_after: usize,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
        ],
        0,
    )
    .await?;
    write_commit_to_store(&store, vec![create_add_action("B")], 1).await?;
    Snapshot::builder_for("memory:///")
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), Some(&checkpoint_spec(false)))?;
    let (base, _) = checkpoint_rows(&store, 1).await;
    assert_eq!(sidecars(&base).len(), 2);
    write_commit_to_store(&store, vec![create_add_action("C")], 2).await?;
    let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let probe = Arc::new(ReadProbe {
        inner: engine.parquet_handler(),
        before_sidecars: Mutex::default(),
        reads: Mutex::default(),
        path_only_after: Some(path_only_after),
    });
    let observed = DelegatingEngine::new(engine.clone()).with_parquet_handler(probe.clone());
    target.checkpoint(&observed, Some(&checkpoint_spec(true)))?;
    let (root, leaves) = checkpoint_rows(&store, 2).await;
    assert!(sidecars(&base).is_disjoint(&sidecars(&root)));
    let mut paths: Vec<_> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    paths.sort();
    assert_eq!(paths, ["A", "B", "C"]);
    assert_counts(&store, 2).await;
    let reads = probe.reads.lock().unwrap();
    let proof = reads
        .iter()
        .find(|call| call.schema.contains_col(["_file"]))
        .unwrap();
    assert_eq!(proof.batches.load(Ordering::Relaxed), path_only_after + 1);
    let full = reads
        .iter()
        .find(|call| {
            call.schema.contains_col(["add", "partitionValues"])
                && call.files.iter().any(|p| p.path().contains("/_sidecars/"))
        })
        .unwrap();
    assert_eq!(full.files.len(), 2);
    Ok(())
}

fn encoded_rows(
    engine: &dyn Engine,
    schema: SchemaRef,
    rows: &[Value],
) -> KernelResult<Box<dyn EngineData>> {
    let strings = StringArray::from_iter_values(rows.iter().map(Value::to_string));
    engine
        .json_handler()
        .parse_json(string_array_to_engine_data(strings), schema)
}

#[rstest]
#[case::matching(false, None, true)]
#[case::empty_tags(false, Some(json!({})), true)]
#[case::tagged(false, Some(json!({"otherWriter": "v1"})), true)]
#[case::null_tags(false, Some(Value::Null), true)]
#[case::null_value(false, Some(json!({"note": null})), true)]
#[case::mixed_values(false, Some(json!({"otherWriter": "v1", "note": null, "empty": ""})), true)]
#[case::compatible_schema_mismatch(true, None, false)]
#[case::tagged_schema_mismatch(true, Some(json!({"otherWriter": "v1", "note": null})), false)]
#[tokio::test]
async fn foreign_sidecar_eligibility(
    #[case] add_only_sidecar_schema: bool,
    #[case] tags: Option<Value>,
    #[case] expect_reuse: bool,
    #[values(false, true)] json_root: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
        ],
        0,
    )
    .await?;
    write_commit_to_store(&store, vec![create_add_action("B")], 1).await?;
    let (_, base) = Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&checkpoint_spec(false)))?;
    let base_file = &base.log_segment().listed.checkpoint_parts[0].location;
    let root_schema = engine
        .parquet_handler()
        .read_parquet_footer(base_file)?
        .schema;
    let (mut root, _) = checkpoint_rows(&store, 1).await;
    assert_eq!(sidecars(&root).len(), 2);
    let reference = root.iter_mut().find_map(|r| r.get_mut("sidecar")).unwrap();
    let leaves = parquet_rows(
        &store,
        &format!(
            "_delta_log/_sidecars/{}",
            reference["path"].as_str().unwrap()
        ),
    )
    .await;
    if add_only_sidecar_schema {
        let old_url = base.log_segment().log_root.join(&format!(
            "_sidecars/{}",
            reference["path"].as_str().unwrap()
        ))?;
        let old = engine.storage_handler().head(&old_url)?;
        let schema = engine
            .parquet_handler()
            .read_parquet_footer(&old)?
            .schema
            .project(&["add"])?;
        let new_url = base
            .log_segment()
            .log_root
            .join("_sidecars/foreign-add-only.parquet")?;
        let data = encoded_rows(&engine, schema, &leaves)?;
        engine
            .parquet_handler()
            .write_parquet_file(new_url.clone(), Box::new(std::iter::once(Ok(data))))?;
        let meta = engine.storage_handler().head(&new_url)?;
        reference["path"] = json!("foreign-add-only.parquet");
        reference["sizeInBytes"] = json!(meta.size);
        reference["modificationTime"] = json!(meta.last_modified);
    }
    if let Some(tags) = tags {
        reference["tags"] = tags;
    }
    let mut expected_reference = reference.clone();
    // An omitted optional map is encoded as null; an empty map remains a distinct value.
    if expected_reference.get("tags").is_none() {
        expected_reference["tags"] = Value::Null;
    }
    if json_root {
        let path = Path::from(format!(
            "_delta_log/{:020}.checkpoint.{}.json",
            1,
            uuid::Uuid::new_v4()
        ));
        let contents = root
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        store.put(&path, contents.into()).await?;
        store.delete(&Path::from(base_file.location.path())).await?;
        store
            .delete(&Path::from("_delta_log/_last_checkpoint"))
            .await?;
    } else {
        let data = encoded_rows(&engine, root_schema, &root)?;
        engine.parquet_handler().write_parquet_file(
            base_file.location.clone(),
            Box::new(std::iter::once(Ok(data))),
        )?;
    }
    let inherited = sidecars(&root);
    let mut expected_paths = vec!["A", "B"];
    for (version, added) in [(2, "C"), (3, "D")] {
        write_commit_to_store(&store, vec![create_add_action(added)], version).await?;
        Snapshot::builder_for("memory:///")
            .build(&engine)?
            .checkpoint(&engine, Some(&checkpoint_spec(true)))?;
        let (root, leaves) = checkpoint_rows(&store, version).await;
        if expect_reuse {
            assert!(inherited.is_subset(&sidecars(&root)));
        } else {
            assert_eq!(inherited.intersection(&sidecars(&root)).count(), 1);
        }
        // Read raw Parquet with explicit nulls: the default JSON test reader drops null map
        // entries too, which would conceal the exact preservation this test is checking.
        let root = parquet_rows_with_nulls(
            &store,
            &format!("_delta_log/{version:020}.checkpoint.parquet"),
            true,
        )
        .await;
        let references: Vec<_> = root
            .iter()
            .filter_map(|r| r["sidecar"].as_object())
            .collect();
        let reused = references
            .iter()
            .find(|r| r["path"] == expected_reference["path"]);
        if expect_reuse {
            assert_eq!(reused.copied(), expected_reference.as_object());
        } else {
            assert!(reused.is_none());
        }
        for reference in references {
            if !inherited.contains(reference["path"].as_str().unwrap()) {
                assert_eq!(reference["tags"], Value::Null);
            }
        }
        expected_paths.push(added);
        let mut paths: Vec<_> = leaves
            .iter()
            .filter_map(|r| r["add"]["path"].as_str())
            .collect();
        paths.sort();
        assert_eq!(paths, expected_paths);
        assert_counts(&store, version).await;
    }
    Ok(())
}
