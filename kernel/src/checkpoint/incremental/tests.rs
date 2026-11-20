use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use crate::actions::{Add, SetTransaction, Sidecar};
use crate::arrow::array::{Array, MapArray, StringArray, StructArray};
use crate::arrow::json::writer::{JsonArray, WriterBuilder};
use crate::checkpoint::tests::{
    create_metadata_action, create_v2_checkpoint_protocol_action, new_in_memory_store,
    write_commit_to_store,
};
use crate::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use crate::engine::DelegatingEngine;
use crate::engine::sync::SyncEngine;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::schema::SchemaRef;
use crate::unit_test_utils::{Action, create_add_action, create_remove_action};
use crate::{
    Engine, EngineData, FileDataReadResultIterator, FileMeta, FileSize, FileSlice, KernelError,
    KernelResult, KernelResultIterator, ParquetFooter, ParquetHandler, PredicateRef, Snapshot,
    StorageHandler,
};

fn spec(reuse: bool) -> CheckpointSpec {
    let file_actions_per_sidecar_hint = Some(1);
    CheckpointSpec::V2(if reuse {
        V2CheckpointConfig::ReuseSidecarsAllOrNothing {
            file_actions_per_sidecar_hint,
        }
    } else {
        V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint,
        }
    })
}

fn benchmark_spec() -> CheckpointSpec {
    CheckpointSpec::V2(
        V2CheckpointConfig::ReuseSidecarsAssumeFreshAddsForBenchmark {
            file_actions_per_sidecar_hint: Some(1),
        },
    )
}

fn txn(version: i64) -> Action {
    Action::Txn(SetTransaction::new("checkpoint-test".into(), version, None))
}

fn forbid_inherited_sidecar_input(path: &Url, inherited: &HashSet<String>) {
    assert!(
        !inherited.contains(path.as_str()),
        "inherited sidecar I/O during no-read reuse: {path}"
    );
}

struct NoSidecarReadsParquet {
    inner: Arc<dyn ParquetHandler>,
    inherited: Arc<HashSet<String>>,
}

struct FailBaseRootParquet {
    inner: Arc<dyn ParquetHandler>,
}

impl ParquetHandler for FailBaseRootParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        if files.iter().any(|file| {
            file.location
                .path()
                .ends_with("00000000000000000000.checkpoint.parquet")
        }) {
            return Err(KernelError::generic("injected base-root read failure"));
        }
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
    ) -> KernelResult<FileSize> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }
}

impl ParquetHandler for NoSidecarReadsParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        for file in files {
            forbid_inherited_sidecar_input(&file.location, &self.inherited);
        }
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
    ) -> KernelResult<FileSize> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        forbid_inherited_sidecar_input(&file.location, &self.inherited);
        self.inner.read_parquet_footer(file)
    }
}

struct NoSidecarReadsStorage {
    inner: Arc<dyn StorageHandler>,
    inherited: Arc<HashSet<String>>,
}

impl StorageHandler for NoSidecarReadsStorage {
    fn list_from(
        &self,
        path: &Url,
        end: Option<&Url>,
    ) -> KernelResult<Box<dyn KernelResultIterator<FileMeta>>> {
        self.inner.list_from(path, end)
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> KernelResult<Box<dyn KernelResultIterator<Bytes>>> {
        for (path, _) in &files {
            forbid_inherited_sidecar_input(path, &self.inherited);
        }
        self.inner.read_files(files)
    }

    fn copy_atomic(&self, src: &Url, dest: &Url) -> KernelResult<()> {
        self.inner.copy_atomic(src, dest)
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> KernelResult<()> {
        self.inner.put(path, data, overwrite)
    }

    fn head(&self, path: &Url) -> KernelResult<FileMeta> {
        forbid_inherited_sidecar_input(path, &self.inherited);
        self.inner.head(path)
    }

    fn delete(&self, path: &Url) -> KernelResult<()> {
        self.inner.delete(path)
    }
}

async fn parquet_rows(store: &InMemory, path: &str) -> Vec<Value> {
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
    let mut writer = WriterBuilder::new().build::<_, JsonArray>(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    serde_json::from_slice(&writer.into_inner()).unwrap()
}

async fn sidecar_tags_in_root(
    store: &InMemory,
    version: u64,
) -> Vec<HashMap<String, Option<String>>> {
    let bytes = store
        .get(&Path::from(format!(
            "_delta_log/{version:020}.checkpoint.parquet"
        )))
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
    let mut result = Vec::new();
    for batch in batches {
        let sidecar = batch
            .column_by_name("sidecar")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let tags = sidecar
            .column_by_name("tags")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if !sidecar.is_valid(row) {
                continue;
            }
            let entries = tags.value(row);
            let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
            let keys = entries
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = entries
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            result.push(
                (0..entries.len())
                    .map(|index| {
                        (
                            keys.value(index).to_owned(),
                            values
                                .is_valid(index)
                                .then(|| values.value(index).to_owned()),
                        )
                    })
                    .collect(),
            );
        }
    }
    result
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

async fn replace_base_root_with_json(store: &InMemory, root: &[Value]) {
    let path = Path::from(
        "_delta_log/00000000000000000000.checkpoint.00000000-0000-0000-0000-000000000001.json",
    );
    let lines = root
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");
    store
        .put(&path, Bytes::from(format!("{lines}\n")).into())
        .await
        .unwrap();
    store
        .delete(&Path::from(
            "_delta_log/00000000000000000000.checkpoint.parquet",
        ))
        .await
        .unwrap();
}

fn guarded_engine(
    store: Arc<InMemory>,
    inherited: &HashSet<String>,
) -> KernelResult<DelegatingEngine> {
    let engine = SyncEngine::new_with_store(store.clone());
    let log_root = Url::parse("memory:///_delta_log/")?;
    let inherited_urls: Arc<HashSet<String>> = Arc::new(
        inherited
            .iter()
            .map(|path| {
                Sidecar {
                    path: path.clone(),
                    size_in_bytes: 0,
                    modification_time: 0,
                    tags: None,
                }
                .to_filemeta(&log_root)
                .map(|meta| meta.location.to_string())
            })
            .collect::<KernelResult<_>>()?,
    );
    Ok(DelegatingEngine::new(Arc::new(engine))
        .with_parquet_handler(Arc::new(NoSidecarReadsParquet {
            inner: SyncEngine::new_with_store(store.clone()).parquet_handler(),
            inherited: inherited_urls.clone(),
        }))
        .with_storage_handler(Arc::new(NoSidecarReadsStorage {
            inner: SyncEngine::new_with_store(store).storage_handler(),
            inherited: inherited_urls,
        })))
}

async fn assert_hint_counts(store: &InMemory, version: u64) {
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
    let mut total_bytes = store
        .head(&Path::from(format!(
            "_delta_log/{version:020}.checkpoint.parquet"
        )))
        .await
        .unwrap()
        .size;
    for path in sidecars(&root) {
        total_bytes += store
            .head(&Path::from(format!("_delta_log/_sidecars/{path}")))
            .await
            .unwrap()
            .size;
    }
    assert_eq!(hint["size"], json!(root.len() + leaves.len()));
    assert_eq!(hint["sizeInBytes"], json!(total_bytes));
    assert_eq!(
        hint["numOfAddFiles"],
        json!(leaves.iter().filter(|row| row.get("add").is_some()).count())
    );
}

#[rstest]
#[case::append(vec![vec![create_add_action("D")]], false, false, vec!["A", "B", "D"])]
#[case::tail_compaction(
    vec![vec![create_add_action("D")],
         vec![create_remove_action("D"), create_add_action("E")]],
    false,
    false,
    vec!["A", "B", "E"]
)]
#[case::inherited_remove(
    vec![vec![create_remove_action("A"), create_add_action("D")]],
    false,
    false,
    vec!["B", "D"]
)]
#[case::metadata_readd(
    vec![vec![Action::Add(Add {
        path: "A".into(),
        data_change: false,
        stats: Some(r#"{"numRecords":7}"#.into()),
        ..Default::default()
    })]],
    false,
    false,
    vec!["A", "B"]
)]
#[case::inherited_tombstone_readd(
    vec![vec![create_add_action("old")]],
    false,
    true,
    vec!["A", "B", "old"]
)]
#[case::metadata_change(vec![vec![create_metadata_action()]], false, false, vec!["A", "B"])]
#[case::txn_only(vec![vec![txn(1)]], true, false, vec!["A", "B"])]
#[case::base_tombstone(vec![vec![txn(1)]], false, true, vec!["A", "B"])]
#[tokio::test]
async fn all_or_nothing_reuses_only_unchanged_base(
    #[case] commits: Vec<Vec<Action>>,
    #[case] expect_reuse: bool,
    #[case] base_tombstone: bool,
    #[case] expected_active: Vec<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    let mut base_actions = vec![
        create_metadata_action(),
        create_v2_checkpoint_protocol_action(),
        create_add_action("A"),
        create_add_action("B"),
    ];
    if base_tombstone {
        base_actions.push(create_remove_action("old"));
    }
    write_commit_to_store(&store, base_actions, 0).await?;
    let base = Snapshot::builder_for("memory:///").build(&engine)?;
    base.checkpoint(&engine, Some(&spec(false)))?;
    let (root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&root);
    let version = commits.len() as u64;
    for (index, actions) in commits.into_iter().enumerate() {
        write_commit_to_store(&store, actions, index as u64 + 1).await?;
    }
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    target.checkpoint(&engine, Some(&spec(true)))?;
    let (root, leaves) = checkpoint_rows(&store, version).await;
    let reused = inherited.intersection(&sidecars(&root)).count();
    assert_eq!(reused, if expect_reuse { inherited.len() } else { 0 });
    let mut active: Vec<&str> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    active.sort_unstable();
    assert_eq!(active, expected_active);
    assert_hint_counts(&store, version).await;
    let fresh = Snapshot::builder_for("memory:///").build(&engine)?;
    assert_eq!(fresh.log_segment().checkpoint_version, Some(version));
    Ok(())
}

#[tokio::test]
async fn repeated_reuse_keeps_all_references_and_exact_hint_counts()
-> Result<(), Box<dyn std::error::Error>> {
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
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(false)))?;
    for version in [1, 2] {
        let (previous_root, _) = checkpoint_rows(&store, version - 1).await;
        let previous = sidecars(&previous_root);
        write_commit_to_store(&store, vec![txn(version as i64)], version).await?;
        Snapshot::builder_for("memory:///")
            .build(&engine)?
            .checkpoint(&engine, Some(&spec(true)))?;
        let (root, _) = checkpoint_rows(&store, version).await;
        assert!(previous.is_subset(&sidecars(&root)));
        assert_hint_counts(&store, version).await;
    }
    let (_, leaves) = checkpoint_rows(&store, 2).await;
    assert_eq!(
        leaves.iter().filter(|row| row.get("add").is_some()).count(),
        1
    );
    Ok(())
}

#[rstest]
#[case::missing_action_count("sidecarNumActions", None)]
#[case::inconsistent_add_count("numOfAddFiles", Some("999"))]
#[case::malformed_action_count("sidecarNumActions", Some("not-a-number"))]
#[case::inconsistent_size("sidecarSizeInBytes", Some("999"))]
#[case::incompatible_schema("sidecarFileSchema", Some("{}"))]
#[tokio::test]
async fn invalid_base_proof_falls_back_to_full_checkpoint(
    #[case] tag: &str,
    #[case] replacement: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(false)))?;
    let (mut root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&root);
    let tags = root
        .iter_mut()
        .find_map(|row| row.get_mut("checkpointMetadata"))
        .unwrap()["tags"]
        .as_object_mut()
        .unwrap();
    if let Some(value) = replacement {
        tags.insert(tag.into(), json!(value));
    } else {
        tags.remove(tag);
    }
    replace_base_root_with_json(&store, &root).await;
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(true)))?;
    let (new_root, leaves) = checkpoint_rows(&store, 1).await;
    assert_eq!(inherited.intersection(&sidecars(&new_root)).count(), 0);
    assert_eq!(
        leaves
            .iter()
            .filter(|row| row["add"]["path"] == "A")
            .count(),
        1
    );
    assert_hint_counts(&store, 1).await;
    Ok(())
}

#[tokio::test]
async fn ordinary_reuse_preserves_nullable_sidecar_tags_without_reading_sidecars()
-> Result<(), Box<dyn std::error::Error>> {
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
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(false)))?;
    let (mut root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&root);
    for row in &mut root {
        if row["sidecar"]["path"].is_string() {
            row["sidecar"]["tags"] = json!({"keep": "value", "nullable": null});
        }
    }
    replace_base_root_with_json(&store, &root).await;
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    target.checkpoint(
        &guarded_engine(store.clone(), &inherited)?,
        Some(&spec(true)),
    )?;
    let (new_root, _) = checkpoint_rows(&store, 1).await;
    assert_eq!(sidecars(&new_root), inherited);
    assert_eq!(
        sidecar_tags_in_root(&store, 1).await,
        vec![HashMap::from([
            ("keep".into(), Some("value".into())),
            ("nullable".into(), None)
        ])]
    );
    assert_hint_counts(&store, 1).await;
    Ok(())
}

#[tokio::test]
async fn base_root_read_error_does_not_publish_a_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
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
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(false)))?;
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    let guarded = DelegatingEngine::new(Arc::new(SyncEngine::new_with_store(store.clone())))
        .with_parquet_handler(Arc::new(FailBaseRootParquet {
            inner: engine.parquet_handler(),
        }));
    let result = target.checkpoint(&guarded, Some(&spec(true)));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected base-root read failure")
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
async fn benchmark_fresh_append_reuses_without_inherited_sidecar_io()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = SyncEngine::new_with_store(store.clone());
    write_commit_to_store(
        &store,
        vec![
            create_metadata_action(),
            create_v2_checkpoint_protocol_action(),
            create_add_action("A"),
            create_add_action("B"),
        ],
        0,
    )
    .await?;
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(false)))?;
    let (base_root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&base_root);
    write_commit_to_store(&store, vec![create_add_action("fresh-D")], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    let guarded = guarded_engine(store.clone(), &inherited)?;
    target.checkpoint(&guarded, Some(&benchmark_spec()))?;
    let (root, leaves) = checkpoint_rows(&store, 1).await;
    assert!(inherited.is_subset(&sidecars(&root)));
    assert_eq!(sidecars(&root).len(), inherited.len() + 1);
    let mut active: Vec<_> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    active.sort_unstable();
    assert_eq!(active, ["A", "B", "fresh-D"]);
    assert_hint_counts(&store, 1).await;
    Ok(())
}

#[rstest]
#[case::inherited_remove(vec![create_remove_action("A")])]
#[case::analyze_readd(vec![Action::Add(Add {
    path: "A".into(),
    data_change: false,
    stats: Some(r#"{"numRecords":7}"#.into()),
    ..Default::default()
})])]
#[tokio::test]
async fn benchmark_assumption_rejects_unsafe_tail_actions(
    #[case] tail: Vec<Action>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(false)))?;
    let (base_root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&base_root);
    write_commit_to_store(&store, tail, 1).await?;
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&benchmark_spec()))?;
    let (root, _) = checkpoint_rows(&store, 1).await;
    assert_eq!(inherited.intersection(&sidecars(&root)).count(), 0);
    Ok(())
}
