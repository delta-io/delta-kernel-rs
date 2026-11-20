use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

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
use crate::engine_data::FilteredEngineData;
use crate::metrics::{
    CheckpointWriteMode, CheckpointWriteOutcome, CheckpointWriteSuccess, MetricEvent,
};
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::object_store::path::Path;
use crate::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::schema::SchemaRef;
use crate::unit_test_utils::{
    Action, CapturingReporter, create_add_action, create_remove_action,
    install_thread_local_metrics_reporter,
};
use crate::{
    Engine, EngineData, Error, FileDataReadResultIterator, FileMeta, FileSize, FileSlice,
    JsonHandler, KernelError, KernelResult, KernelResultIterator, ParquetFooter, ParquetHandler,
    PredicateRef, Snapshot, StorageHandler,
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

fn stats_only_add_a() -> Action {
    Action::Add(Add {
        path: "A".into(),
        data_change: false,
        stats: Some(r#"{"numRecords":7}"#.into()),
        ..Default::default()
    })
}

#[derive(Clone, Default)]
struct TailReadAndSidecarWrites {
    tail_requests: HashMap<String, usize>,
    tail_batches: HashMap<String, usize>,
    written_sidecars: Vec<String>,
}

struct TrackTailJson {
    inner: Arc<dyn JsonHandler>,
    state: Arc<Mutex<TailReadAndSidecarWrites>>,
    fail_on_older_batch: bool,
}

impl JsonHandler for TrackTailJson {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> KernelResult<Box<dyn EngineData>> {
        self.inner.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        assert_eq!(
            files.len(),
            2,
            "expected the two tail commits in one ordered read"
        );
        assert!(
            files[0]
                .location
                .path()
                .ends_with("00000000000000000002.json")
        );
        assert!(
            files[1]
                .location
                .path()
                .ends_with("00000000000000000001.json")
        );
        let filenames: Vec<_> = files
            .iter()
            .map(|file| file.location.path().rsplit('/').next().unwrap().to_owned())
            .collect();
        {
            let mut state = self.state.lock().unwrap();
            for filename in &filenames {
                *state.tail_requests.entry(filename.clone()).or_default() += 1;
            }
        }
        let mut batches = self
            .inner
            .read_json_files(files, physical_schema, predicate)?;
        let state = self.state.clone();
        let fail_on_older_batch = self.fail_on_older_batch;
        let mut emitted = 0;
        Ok(Box::new(std::iter::from_fn(move || {
            // SyncJsonHandler yields one batch for each of these one-action commits.
            // With hint=1, Add D's sidecar must finish before the older file is pulled.
            if emitted == 1 {
                assert!(
                    !state.lock().unwrap().written_sidecars.is_empty(),
                    "newest Add D was not flushed before the older tail batch"
                );
                if fail_on_older_batch {
                    return Some(Err(KernelError::generic("injected older-tail JSON error")));
                }
            }
            let batch = batches.next();
            if matches!(&batch, Some(Ok(_))) {
                assert!(emitted < filenames.len(), "unexpected extra JSON batch");
                *state
                    .lock()
                    .unwrap()
                    .tail_batches
                    .entry(filenames[emitted].clone())
                    .or_default() += 1;
            }
            if batch.is_some() {
                emitted += 1;
            }
            batch
        })))
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn KernelResultIterator<FilteredEngineData> + '_>,
        overwrite: bool,
    ) -> KernelResult<FileSize> {
        self.inner.write_json_file(path, data, overwrite)
    }
}

struct TrackSidecarWrites {
    inner: Arc<dyn ParquetHandler>,
    state: Arc<Mutex<TailReadAndSidecarWrites>>,
}

impl ParquetHandler for TrackSidecarWrites {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        self.inner
            .read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
    ) -> KernelResult<FileSize> {
        let size = self.inner.write_parquet_file(location.clone(), data)?;
        if location.path().contains("/_sidecars/") {
            self.state
                .lock()
                .unwrap()
                .written_sidecars
                .push(location.to_string());
        }
        Ok(size)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }
}

fn tracked_tail_engine(
    engine: Arc<dyn Engine>,
    state: Arc<Mutex<TailReadAndSidecarWrites>>,
    fail_on_older_batch: bool,
) -> DelegatingEngine {
    DelegatingEngine::new(engine.clone())
        .with_json_handler(Arc::new(TrackTailJson {
            inner: engine.json_handler(),
            state: state.clone(),
            fail_on_older_batch,
        }))
        .with_parquet_handler(Arc::new(TrackSidecarWrites {
            inner: engine.parquet_handler(),
            state,
        }))
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

struct FailInheritedSidecarParquet {
    inner: Arc<dyn ParquetHandler>,
    inherited: HashSet<String>,
    state: Arc<Mutex<TailReadAndSidecarWrites>>,
}

impl ParquetHandler for FailInheritedSidecarParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> KernelResult<FileDataReadResultIterator> {
        if files.iter().any(|file| {
            file.location.path().contains("/_sidecars/")
                && file
                    .location
                    .path()
                    .rsplit('/')
                    .next()
                    .is_some_and(|name| self.inherited.contains(name))
        }) {
            assert!(
                !self.state.lock().unwrap().written_sidecars.is_empty(),
                "inherited sidecar read began before Add D sidecar was written"
            );
            return Err(KernelError::generic(
                "injected inherited-sidecar read failure",
            ));
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

fn single_checkpoint_success(reporter: &CapturingReporter) -> CheckpointWriteSuccess {
    let events: Vec<_> = reporter
        .events()
        .into_iter()
        .filter_map(|event| match event {
            MetricEvent::CheckpointWriteSuccess(event) => Some(event),
            _ => None,
        })
        .collect();
    let [event] = events.as_slice() else {
        panic!("expected one checkpoint success event, got {events:?}");
    };
    event.clone()
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

#[rstest]
#[case::reuse(false)]
#[case::missing_summary_fallback(true)]
#[tokio::test(flavor = "multi_thread")]
async fn reuse_checkpoint_metrics_count_input_work_and_published_rows(
    #[case] missing_summary: bool,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let (mut base_root, base_leaves) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&base_root);
    assert!(!inherited.is_empty());
    assert_eq!(base_leaves.len(), 2);
    let root_rows = base_root.len() as u64;
    let sidecar_rows = base_leaves.len() as u64;
    if missing_summary {
        base_root
            .iter_mut()
            .find_map(|row| row.get_mut("checkpointMetadata"))
            .unwrap()["tags"]
            .as_object_mut()
            .unwrap()
            .remove("sidecarNumActions");
        replace_base_root_with_json(&store, &base_root).await;
    }
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    let reporter = Arc::new(CapturingReporter::default());
    let event = {
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let target = Snapshot::builder_for("memory:///").build(&engine)?;
        target.checkpoint(&engine, Some(&spec(true)))?;
        single_checkpoint_success(&reporter)
    };
    let (root, leaves) = checkpoint_rows(&store, 1).await;
    let final_sidecars = sidecars(&root);
    let reused = inherited.intersection(&final_sidecars).count() as u64;
    assert_eq!(
        reused,
        if missing_summary {
            0
        } else {
            inherited.len() as u64
        }
    );
    assert_eq!(
        event.mode,
        CheckpointWriteMode::V2ReusingSidecarsAllOrNothing
    );
    assert_eq!(event.version, 1);
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected a written checkpoint");
    };
    assert_eq!(stats.num_reused_sidecars, reused);
    assert_eq!(stats.num_sidecars, final_sidecars.len() as u64);
    assert_eq!(stats.num_add_files, 2);
    assert_eq!(stats.num_remove_files, 0);
    assert_eq!(stats.num_actions, (root.len() + leaves.len()) as u64);
    assert_eq!(event.processing_stats.num_actions_from_log_tail, 1);
    // Fallback reads the root during preflight, then reads it and its sidecars again.
    assert_eq!(
        event.processing_stats.num_actions_from_checkpoint,
        if missing_summary {
            2 * root_rows + sidecar_rows
        } else {
            root_rows
        }
    );
    assert_eq!(
        event.output_stats.num_sidecars_written,
        if missing_summary {
            final_sidecars.len() as u64
        } else {
            0
        }
    );
    assert_hint_counts(&store, 1).await;
    Ok(())
}

#[rstest]
#[case::first_checkpoint(false)]
#[case::inline_no_sidecar_base(true)]
#[tokio::test(flavor = "multi_thread")]
async fn missing_or_inline_base_writes_full_sidecar_checkpoint(
    #[case] inline_base: bool,
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
    if inline_base {
        Snapshot::builder_for("memory:///")
            .build(&engine)?
            .checkpoint(
                &engine,
                Some(&CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)),
            )?;
        let (base_root, _) = checkpoint_rows(&store, 0).await;
        assert!(sidecars(&base_root).is_empty());
        assert!(base_root.iter().any(|row| row["add"]["path"] == "A"));
        write_commit_to_store(&store, vec![txn(1)], 1).await?;
    }
    let version = u64::from(inline_base);
    Snapshot::builder_for("memory:///")
        .build(&engine)?
        .checkpoint(&engine, Some(&spec(true)))?;
    let (root, leaves) = checkpoint_rows(&store, version).await;
    assert_eq!(sidecars(&root).len(), 1);
    assert!(root.iter().all(|row| row["add"]["path"].is_null()));
    assert_eq!(
        leaves
            .iter()
            .filter(|row| row["add"]["path"] == "A")
            .count(),
        1
    );
    assert_hint_counts(&store, version).await;
    Ok(())
}

#[rstest]
#[case::all_or_nothing(V2CheckpointConfig::ReuseSidecarsAllOrNothing {
    file_actions_per_sidecar_hint: Some(0),
})]
#[case::benchmark(V2CheckpointConfig::ReuseSidecarsAssumeFreshAddsForBenchmark {
    file_actions_per_sidecar_hint: Some(0),
})]
#[tokio::test(flavor = "multi_thread")]
async fn reuse_specs_reject_zero_sidecar_hint_without_publication(
    #[case] config: V2CheckpointConfig,
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
    let hint_before = store
        .get(&Path::from("_delta_log/_last_checkpoint"))
        .await?
        .bytes()
        .await?;
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    let no_base_reads = DelegatingEngine::new(Arc::new(SyncEngine::new_with_store(store.clone())))
        .with_parquet_handler(Arc::new(FailBaseRootParquet {
            inner: engine.parquet_handler(),
        }));
    assert!(matches!(
        target.checkpoint(&no_base_reads, Some(&CheckpointSpec::V2(config))),
        Err(Error::Kernel(KernelError::CheckpointWrite(_)))
    ));
    assert!(
        store
            .head(&Path::from(
                "_delta_log/00000000000000000001.checkpoint.parquet"
            ))
            .await
            .is_err()
    );
    let hint_after = store
        .get(&Path::from("_delta_log/_last_checkpoint"))
        .await?
        .bytes()
        .await?;
    assert_eq!(hint_after, hint_before);
    Ok(())
}

#[derive(Clone, Copy)]
enum RootEdit {
    NullTags,
    MissingCheckpointMetadata,
    WrongCheckpointVersion,
    DuplicateSidecar,
}

#[rstest]
#[case::null_tags(RootEdit::NullTags, false)]
#[case::missing_checkpoint_metadata(RootEdit::MissingCheckpointMetadata, true)]
#[case::wrong_checkpoint_version(RootEdit::WrongCheckpointVersion, true)]
#[case::duplicate_sidecar(RootEdit::DuplicateSidecar, true)]
#[tokio::test(flavor = "multi_thread")]
async fn malformed_base_root_falls_back_or_errors_before_publication(
    #[case] edit: RootEdit,
    #[case] expect_error: bool,
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
    let hint_before = store
        .get(&Path::from("_delta_log/_last_checkpoint"))
        .await?
        .bytes()
        .await?;
    match edit {
        RootEdit::NullTags => {
            root.iter_mut()
                .find_map(|row| row.get_mut("checkpointMetadata"))
                .unwrap()["tags"] = Value::Null;
        }
        RootEdit::MissingCheckpointMetadata => {
            root.retain(|row| row["checkpointMetadata"].is_null());
        }
        RootEdit::WrongCheckpointVersion => {
            root.iter_mut()
                .find_map(|row| row.get_mut("checkpointMetadata"))
                .unwrap()["version"] = json!(99);
        }
        RootEdit::DuplicateSidecar => {
            root.push(
                root.iter()
                    .find(|row| row["sidecar"]["path"].is_string())
                    .unwrap()
                    .clone(),
            );
        }
    }
    replace_base_root_with_json(&store, &root).await;
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    let result = target.checkpoint(&engine, Some(&spec(true)));
    if expect_error {
        assert!(matches!(
            result,
            Err(Error::Kernel(KernelError::InvalidCheckpoint(_)))
        ));
        assert!(
            store
                .head(&Path::from(
                    "_delta_log/00000000000000000001.checkpoint.parquet"
                ))
                .await
                .is_err()
        );
        let hint_after = store
            .get(&Path::from("_delta_log/_last_checkpoint"))
            .await?
            .bytes()
            .await?;
        assert_eq!(hint_after, hint_before);
    } else {
        result?;
        let (new_root, leaves) = checkpoint_rows(&store, 1).await;
        assert!(inherited.is_disjoint(&sidecars(&new_root)));
        assert_eq!(
            leaves
                .iter()
                .filter(|row| row["add"]["path"] == "A")
                .count(),
            1
        );
        assert_hint_counts(&store, 1).await;
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn inherited_sidecar_guard_rejects_full_checkpoint_reads()
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
    let (root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&root);
    assert_eq!(inherited.len(), 1);
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    let target = Snapshot::builder_for("memory:///").build(&engine)?;
    let guarded = guarded_engine(store, &inherited)?;
    let panic = match catch_unwind(AssertUnwindSafe(|| {
        target.checkpoint(&guarded, Some(&spec(false)))
    })) {
        Err(panic) => panic,
        Ok(_) => panic!("full checkpoint must read an inherited sidecar"),
    };
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("inherited sidecar I/O"), "{message}");
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
    let guarded = guarded_engine(store.clone(), &inherited)?;
    let reporter = Arc::new(CapturingReporter::default());
    let event = {
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let target = Snapshot::builder_for("memory:///").build(&engine)?;
        target.checkpoint(&guarded, Some(&benchmark_spec()))?;
        single_checkpoint_success(&reporter)
    };
    let (root, leaves) = checkpoint_rows(&store, 1).await;
    assert!(inherited.is_subset(&sidecars(&root)));
    assert_eq!(sidecars(&root).len(), inherited.len() + 1);
    assert_eq!(
        event.mode,
        CheckpointWriteMode::V2ReusingSidecarsAssumeFreshAddsForBenchmark
    );
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected a written checkpoint");
    };
    assert_eq!(stats.num_reused_sidecars, inherited.len() as u64);
    assert_eq!(stats.num_sidecars, sidecars(&root).len() as u64);
    assert_eq!(event.output_stats.num_sidecars_written, 1);
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
#[case::inherited_remove(vec![create_remove_action("A")], vec![], None)]
#[case::analyze_readd(
    vec![stats_only_add_a()],
    vec!["A"],
    Some(r#"{"numRecords":7}"#)
)]
#[case::metadata_with_fresh_add(
    vec![create_metadata_action(), create_add_action("D")],
    vec!["A", "D"],
    None
)]
#[case::protocol_with_fresh_add(
    vec![create_v2_checkpoint_protocol_action(), create_add_action("D")],
    vec!["A", "D"],
    None
)]
#[tokio::test]
async fn benchmark_assumption_rejects_unsafe_tail_actions(
    #[case] tail: Vec<Action>,
    #[case] expected_active: Vec<&str>,
    #[case] expected_a_stats: Option<&str>,
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
    let (root, leaves) = checkpoint_rows(&store, 1).await;
    assert_eq!(inherited.intersection(&sidecars(&root)).count(), 0);
    let mut active: Vec<_> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    active.sort_unstable();
    assert_eq!(active, expected_active);
    let actual_a_stats = leaves
        .iter()
        .find(|row| row["add"]["path"] == "A")
        .and_then(|row| row["add"]["stats"].as_str());
    assert_eq!(actual_a_stats, expected_a_stats);
    assert_hint_counts(&store, 1).await;
    Ok(())
}

#[rstest]
#[case::older_remove_a(create_remove_action("A"), vec!["B", "D"], None)]
#[case::older_stats_only_add_a(
    stats_only_add_a(),
    vec!["A", "B", "D"],
    Some(r#"{"numRecords":7}"#)
)]
#[tokio::test(flavor = "multi_thread")]
async fn late_tail_fallback_keeps_already_written_add_sidecar(
    #[case] older_action: Action,
    #[case] expected_active: Vec<&str>,
    #[case] expected_a_stats: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
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
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), Some(&spec(false)))?;
    let (base_root, base_leaves) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&base_root);
    write_commit_to_store(&store, vec![older_action], 1).await?;
    write_commit_to_store(&store, vec![create_add_action("D")], 2).await?;
    let state = Arc::new(Mutex::new(TailReadAndSidecarWrites::default()));
    let tracked = tracked_tail_engine(engine.clone(), state.clone(), false);
    let reporter = Arc::new(CapturingReporter::default());
    let event = {
        let _guard = install_thread_local_metrics_reporter(reporter.clone());
        let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
        target.checkpoint(&tracked, Some(&benchmark_spec()))?;
        single_checkpoint_success(&reporter)
    };

    let observed = state.lock().unwrap().clone();
    let expected_reads = HashMap::from([
        ("00000000000000000002.json".to_owned(), 1),
        ("00000000000000000001.json".to_owned(), 1),
    ]);
    assert_eq!(observed.tail_requests, expected_reads);
    assert_eq!(observed.tail_batches, expected_reads);
    let (root, leaves) = checkpoint_rows(&store, 2).await;
    let final_sidecars = sidecars(&root);
    assert!(inherited.is_disjoint(&final_sidecars));
    assert_eq!(
        event.mode,
        CheckpointWriteMode::V2ReusingSidecarsAssumeFreshAddsForBenchmark
    );
    let CheckpointWriteOutcome::Written { stats } = event.outcome else {
        panic!("expected a written checkpoint");
    };
    assert_eq!(stats.num_reused_sidecars, 0);
    assert_eq!(stats.num_sidecars, final_sidecars.len() as u64);
    assert_eq!(
        event.output_stats.num_sidecars_written,
        final_sidecars.len() as u64
    );
    assert_eq!(
        event.processing_stats.num_actions_from_checkpoint,
        (base_root.len() + base_leaves.len()) as u64
    );
    let mut written_names = HashSet::new();
    let mut d_sidecars = Vec::new();
    for path in &observed.written_sidecars {
        let url = Url::parse(path)?;
        let filename = url.path().rsplit('/').next().unwrap().to_owned();
        written_names.insert(filename.clone());
        let rows = parquet_rows(&store, url.path().trim_start_matches('/')).await;
        if rows.iter().any(|row| row["add"]["path"] == "D") {
            d_sidecars.push(filename);
        }
    }
    assert_eq!(d_sidecars.len(), 1, "Add D must be written to one sidecar");
    assert!(final_sidecars.contains(&d_sidecars[0]));
    assert_eq!(
        final_sidecars, written_names,
        "no written sidecar is abandoned"
    );
    assert_eq!(
        leaves
            .iter()
            .filter(|row| row["add"]["path"] == "D")
            .count(),
        1,
        "Add D must not be replayed into another sidecar"
    );
    let mut active: Vec<_> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    active.sort_unstable();
    assert_eq!(active, expected_active);
    let actual_a_stats = leaves
        .iter()
        .find(|row| row["add"]["path"] == "A")
        .and_then(|row| row["add"]["stats"].as_str());
    assert_eq!(actual_a_stats, expected_a_stats);
    assert_hint_counts(&store, 2).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reuse_reads_each_tail_json_once_without_inherited_sidecar_io()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
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
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), Some(&spec(false)))?;
    let (base_root, base_leaves) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&base_root);
    assert!(base_leaves.iter().all(|row| row["add"]["path"] != "D"));
    write_commit_to_store(&store, vec![txn(1)], 1).await?;
    write_commit_to_store(&store, vec![create_add_action("D")], 2).await?;
    let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let state = Arc::new(Mutex::new(TailReadAndSidecarWrites::default()));
    let guarded = guarded_engine(store.clone(), &inherited)?;
    let tracked = tracked_tail_engine(Arc::new(guarded), state.clone(), false);
    target.checkpoint(&tracked, Some(&benchmark_spec()))?;

    let observed = state.lock().unwrap().clone();
    let expected_reads = HashMap::from([
        ("00000000000000000002.json".to_owned(), 1),
        ("00000000000000000001.json".to_owned(), 1),
    ]);
    assert_eq!(observed.tail_requests, expected_reads);
    assert_eq!(observed.tail_batches, expected_reads);
    let (root, leaves) = checkpoint_rows(&store, 2).await;
    let final_sidecars = sidecars(&root);
    assert!(inherited.is_subset(&final_sidecars));
    assert_eq!(final_sidecars.len(), inherited.len() + 1);
    assert_eq!(observed.written_sidecars.len(), 1);
    let written = Url::parse(&observed.written_sidecars[0])?;
    assert!(final_sidecars.contains(written.path().rsplit('/').next().unwrap()));
    let mut active: Vec<_> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    active.sort_unstable();
    assert_eq!(active, ["A", "B", "D"]);
    assert_hint_counts(&store, 2).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn late_tail_json_error_after_sidecar_flush_does_not_publish_root_or_hint()
-> Result<(), Box<dyn std::error::Error>> {
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
        .checkpoint(engine.as_ref(), Some(&spec(false)))?;
    write_commit_to_store(&store, vec![create_remove_action("A")], 1).await?;
    write_commit_to_store(&store, vec![create_add_action("D")], 2).await?;
    let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let state = Arc::new(Mutex::new(TailReadAndSidecarWrites::default()));
    let tracked = tracked_tail_engine(engine, state.clone(), true);
    let result = target.checkpoint(&tracked, Some(&benchmark_spec()));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected older-tail JSON error")
    );
    assert!(
        !state.lock().unwrap().written_sidecars.is_empty(),
        "error must follow a completed Add D sidecar write"
    );
    assert!(
        store
            .head(&Path::from(
                "_delta_log/00000000000000000002.checkpoint.parquet"
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

#[tokio::test(flavor = "multi_thread")]
async fn inherited_sidecar_read_error_after_tail_flush_preserves_base_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, _) = new_in_memory_store();
    let engine = Arc::new(SyncEngine::new_with_store(store.clone()));
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
        .build(engine.as_ref())?
        .checkpoint(engine.as_ref(), Some(&spec(false)))?;
    let (base_root, _) = checkpoint_rows(&store, 0).await;
    let inherited = sidecars(&base_root);
    let hint_before = store
        .get(&Path::from("_delta_log/_last_checkpoint"))
        .await?
        .bytes()
        .await?;
    write_commit_to_store(&store, vec![create_remove_action("A")], 1).await?;
    write_commit_to_store(&store, vec![create_add_action("D")], 2).await?;
    let target = Snapshot::builder_for("memory:///").build(engine.as_ref())?;
    let state = Arc::new(Mutex::new(TailReadAndSidecarWrites::default()));
    let tracked: Arc<dyn Engine> =
        Arc::new(tracked_tail_engine(engine.clone(), state.clone(), false));
    let failing = DelegatingEngine::new(tracked.clone()).with_parquet_handler(Arc::new(
        FailInheritedSidecarParquet {
            inner: tracked.parquet_handler(),
            inherited: inherited.clone(),
            state: state.clone(),
        },
    ));
    let error = target
        .checkpoint(&failing, Some(&benchmark_spec()))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected inherited-sidecar read failure")
    );
    let observed = state.lock().unwrap().clone();
    assert!(
        !observed.written_sidecars.is_empty(),
        "late failure must follow a completed Add D sidecar write"
    );
    let expected_reads = HashMap::from([
        ("00000000000000000002.json".to_owned(), 1),
        ("00000000000000000001.json".to_owned(), 1),
    ]);
    assert_eq!(observed.tail_requests, expected_reads);
    assert_eq!(observed.tail_batches, expected_reads);
    assert!(
        store
            .head(&Path::from(
                "_delta_log/00000000000000000002.checkpoint.parquet"
            ))
            .await
            .is_err()
    );
    let hint_after = store
        .get(&Path::from("_delta_log/_last_checkpoint"))
        .await?
        .bytes()
        .await?;
    assert_eq!(hint_after, hint_before);

    let prior = Snapshot::builder_for("memory:///")
        .at_version(0)
        .build(engine.as_ref())?;
    assert_eq!(prior.log_segment().checkpoint_version, Some(0));
    let (root, leaves) = checkpoint_rows(&store, 0).await;
    assert_eq!(sidecars(&root), inherited);
    let mut active: Vec<_> = leaves
        .iter()
        .filter_map(|row| row["add"]["path"].as_str())
        .collect();
    active.sort_unstable();
    assert_eq!(active, ["A", "B"]);
    Ok(())
}
