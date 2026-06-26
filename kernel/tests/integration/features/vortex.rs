//! Characterization tests for the Vortex alternate file-format POC against Delta features that
//! the unit/integration round-trip tests don't cover: deletion vectors, partitioned writes,
//! change data feed, and type widening.
//!
//! Each test drives the full kernel path with `DefaultEngine::write_vortex` as the writer and the
//! per-file `.vortex` dispatch as the reader, so it tells us whether the feature actually works
//! end-to-end over Vortex data files (vs. the Parquet path it mirrors).

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::arrow::array::{AsArray, Int32Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{Int32Type, Int64Type, Schema as ArrowSchema};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::expressions::Scalar;
use delta_kernel::object_store::ObjectStoreExt as _;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_changes::TableChanges;
use delta_kernel::transaction::create_table::create_table as kernel_create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::transaction::WriteContext;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use tempfile::tempdir;
use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use test_utils::delta_kernel_default_engine::DefaultEngine;
use test_utils::{
    add_commit, create_table as json_create_table, engine_store_setup, load_and_begin_transaction,
    read_actions_from_commit, read_scan,
};
use url::Url;

type VortexEngine = Arc<DefaultEngine<TokioBackgroundExecutor>>;

// ============================================================================
// Shared helpers
// ============================================================================

/// Local store + concrete engine + table URL in a fresh temp dir. A `file://` URL is required so
/// `read_actions_from_commit` (which uses `to_file_path`) works.
fn setup(
    name: &str,
) -> (
    tempfile::TempDir,
    Arc<delta_kernel::object_store::DynObjectStore>,
    VortexEngine,
    Url,
) {
    let tmp = tempdir().unwrap();
    let base = Url::from_directory_path(tmp.path()).unwrap();
    let (store, engine, table_url) = engine_store_setup(name, Some(&base));
    // The kernel `create_table` builder lists the table dir to confirm no table exists yet, which
    // errors on a non-existent dir under LocalFileSystem; create the (empty) table dir up front.
    std::fs::create_dir_all(table_url.to_file_path().unwrap()).unwrap();
    (tmp, store, Arc::new(engine), table_url)
}

/// Appends one Vortex data file built from `batch` (no partitioning) and commits it.
async fn append_vortex_batch(
    table_url: &Url,
    engine: &VortexEngine,
    batch: RecordBatch,
) -> DeltaResult<()> {
    let mut txn = load_and_begin_transaction(table_url.clone(), engine.as_ref())?
        .with_engine_info("vortex-poc")
        .with_data_change(true);
    let write_context = txn.unpartitioned_write_context()?;
    let add = engine
        .write_vortex(&ArrowEngineData::new(batch), &write_context)
        .await?;
    txn.add_files(add);
    assert!(txn.commit(engine.as_ref())?.is_committed());
    Ok(())
}

/// Collects all scan rows for a snapshot into `RecordBatch`es.
fn scan_batches(snapshot: &Arc<Snapshot>, engine: VortexEngine) -> DeltaResult<Vec<RecordBatch>> {
    let scan = snapshot.clone().scan_builder().build()?;
    read_scan(&scan, engine as Arc<dyn Engine>)
}

/// Writes a deletion vector to the store and returns its descriptor (copied from the DV feature
/// test; kept local so this module is self-contained).
async fn write_deletion_vector_to_store(
    store: &Arc<dyn delta_kernel::object_store::ObjectStore>,
    write_context: &WriteContext,
    dv: KernelDeletionVector,
) -> DeltaResult<DeletionVectorDescriptor> {
    use delta_kernel::object_store::path::Path as ObjectStorePath;

    let dv_path = write_context.new_deletion_vector_path(String::new());
    let dv_absolute_path = dv_path.absolute_path()?;
    let dv_object_path = ObjectStorePath::parse(dv_absolute_path.path())?;

    let mut dv_buffer = Vec::new();
    let mut dv_writer = StreamingDeletionVectorWriter::new(&mut dv_buffer);
    let dv_write_result = dv_writer.write_deletion_vector(dv)?;
    dv_writer.finalize()?;

    store.put(&dv_object_path, dv_buffer.into()).await?;
    Ok(dv_write_result.to_descriptor(&dv_path))
}

/// Extracts scan files (file metadata + selection vector) for DV updates.
fn get_scan_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
) -> DeltaResult<Vec<FilteredEngineData>> {
    let scan = snapshot.scan_builder().build()?;
    let all: Vec<_> = scan.scan_metadata(engine)?.collect::<Result<Vec<_>, _>>()?;
    Ok(all.into_iter().map(|sm| sm.scan_files).collect())
}

/// Sorted `i32` values from column 0 across all batches.
fn sorted_i32_col0(batches: &[RecordBatch]) -> Vec<i32> {
    let mut v: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let c = b.column(0).as_primitive::<Int32Type>();
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect();
    v.sort_unstable();
    v
}

// ============================================================================
// 1. Partitioned Vortex writes
// ============================================================================

/// Writes two partitions as Vortex files (data files contain only the non-partition column) and
/// verifies the scan reconstructs the partition column from `partitionValues`.
#[tokio::test(flavor = "multi_thread")]
async fn vortex_partitioned_write_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (_tmp, _store, engine, table_url) = setup("vortex_partitioned");

    let schema = Arc::new(StructType::try_new(vec![
        StructField::nullable("number", DataType::INTEGER),
        StructField::nullable("part", DataType::STRING),
    ])?);
    let _ = kernel_create_table(table_url.as_str(), schema, "vortex-poc")
        .with_data_layout(DataLayout::partitioned(["part"]))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // Data batches exclude the partition column.
    let data_schema =
        StructType::try_new(vec![StructField::nullable("number", DataType::INTEGER)])?;
    let arrow_data_schema: Arc<ArrowSchema> = Arc::new((&data_schema).try_into_arrow()?);

    let mut txn = load_and_begin_transaction(table_url.clone(), engine.as_ref())?
        .with_engine_info("vortex-poc")
        .with_data_change(true);
    for (values, part) in [(vec![1, 2, 3], "a"), (vec![4, 5], "b")] {
        let batch = RecordBatch::try_new(
            arrow_data_schema.clone(),
            vec![Arc::new(Int32Array::from(values))],
        )?;
        let wc = txn.partitioned_write_context(HashMap::from([(
            "part".to_string(),
            Scalar::String(part.into()),
        )]))?;
        let add = engine
            .write_vortex(&ArrowEngineData::new(batch), &wc)
            .await?;
        txn.add_files(add);
    }
    let snapshot = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

    let batches = scan_batches(&snapshot, engine.clone())?;
    // Verify (number, part) pairs round-trip, ignoring scan order.
    let mut pairs: Vec<(i32, String)> = batches
        .iter()
        .flat_map(|b| {
            let number = b
                .column_by_name("number")
                .unwrap()
                .as_primitive::<Int32Type>();
            let part = b
                .column_by_name("part")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| (number.value(i), part.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            (1, "a".to_string()),
            (2, "a".to_string()),
            (3, "a".to_string()),
            (4, "b".to_string()),
            (5, "b".to_string()),
        ]
    );
    Ok(())
}

// ============================================================================
// 2. Deletion-vector reads over a Vortex file
// ============================================================================

/// Writes a Vortex file with 6 rows, applies a deletion vector deleting positions 1 and 4, and
/// verifies the scan returns only the surviving rows -- exercising kernel's positional
/// selection-vector alignment over the Vortex reader's row order.
#[tokio::test(flavor = "multi_thread")]
async fn vortex_deletion_vector_read() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (_tmp, store, engine, table_url) = setup("vortex_dv");

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "id",
        DataType::INTEGER,
    )])?);
    json_create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true,
        vec!["deletionVectors"],
        vec!["deletionVectors"],
    )
    .await?;

    // v1: append a Vortex file with ids 0..6.
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5]))],
    )?;
    append_vortex_batch(&table_url, &engine, batch).await?;

    let file_path = {
        let adds = read_actions_from_commit(&table_url, 1, "add")?;
        assert_eq!(adds.len(), 1);
        adds[0]["path"].as_str().unwrap().to_string()
    };

    // Sanity: all 6 rows visible before deletion.
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(
        sorted_i32_col0(&scan_batches(&snapshot, engine.clone())?),
        vec![0, 1, 2, 3, 4, 5]
    );

    // v2: delete positions 1 and 4 (ids 1 and 4).
    let mut dv = KernelDeletionVector::new();
    dv.add_deleted_row_indexes([1u64, 4]);
    let write_context = load_and_begin_transaction(table_url.clone(), engine.as_ref())?
        .unpartitioned_write_context()?;
    let descriptor = write_deletion_vector_to_store(&store, &write_context, dv).await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = load_and_begin_transaction(table_url.clone(), engine.as_ref())?
        .with_engine_info("vortex-poc")
        .with_operation("DELETE".to_string());
    let scan_files = get_scan_files(snapshot, engine.as_ref())?;
    let dv_map = HashMap::from([(file_path, descriptor)]);
    txn.update_deletion_vectors(dv_map, scan_files.into_iter().map(Ok))?;
    assert!(txn.commit(engine.as_ref())?.is_committed());

    // Survivors: ids 0, 2, 3, 5.
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(
        sorted_i32_col0(&scan_batches(&snapshot, engine.clone())?),
        vec![0, 2, 3, 5]
    );
    Ok(())
}

// ============================================================================
// 3. Change Data Feed over Vortex files
// ============================================================================

/// Enables CDF, appends a Vortex file, and reads the table changes -- verifying the CDF read path
/// (which also calls `read_parquet_files` one file at a time) dispatches to the Vortex reader and
/// synthesizes the expected `insert` rows.
#[tokio::test(flavor = "multi_thread")]
async fn vortex_cdf_read() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (_tmp, _store, engine, table_url) = setup("vortex_cdf");

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "number",
        DataType::INTEGER,
    )])?);
    // The kernel builder auto-enables the changeDataFeed feature from the enablement property.
    let _ = kernel_create_table(table_url.as_str(), schema.clone(), "vortex-poc")
        .with_table_properties([("delta.enableChangeDataFeed", "true")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // v1: append a Vortex file with numbers 1, 2, 3.
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )?;
    append_vortex_batch(&table_url, &engine, batch).await?;

    // Read changes 0..=1. Project out _commit_timestamp (non-deterministic).
    let table_changes = TableChanges::try_new(table_url.clone(), engine.as_ref(), 0, Some(1))?;
    let names: Vec<_> = table_changes
        .schema()
        .fields()
        .map(|f| f.name())
        .filter(|n| *n != "_commit_timestamp")
        .collect();
    let schema = table_changes.schema().project(&names)?;
    let scan = table_changes
        .into_scan_builder()
        .with_schema(schema)
        .build()?;
    let batches: Vec<RecordBatch> = scan
        .execute(engine.clone() as Arc<dyn Engine>)?
        .map(|d| d?.try_into_record_batch())
        .collect::<Result<Vec<_>, _>>()?;

    // Three insert rows, numbers 1/2/3, all _change_type = "insert".
    assert_eq!(sorted_i32_col0(&batches), vec![1, 2, 3]);
    for b in &batches {
        let change_type = b
            .column_by_name("_change_type")
            .expect("_change_type column")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(change_type.value(i), "insert");
        }
    }
    Ok(())
}

// ============================================================================
// 4. Type widening over a Vortex file
// ============================================================================

/// Writes a Vortex file with an `integer` column, then widens the column to `long` via a
/// hand-written commit that adds the `typeWidening` feature and `delta.typeChanges` metadata. A
/// scan at the widened version reads the narrow Vortex file into the wide logical schema, so the
/// reader must cast int32 -> int64.
///
/// The widening commit is written as raw JSON because kernel rejects `typeWidening` as an
/// unsupported *write* feature (it only supports reading widened tables). The narrow data file is
/// written first into a plain (3,7) table, so the append itself uses no unsupported feature.
#[tokio::test(flavor = "multi_thread")]
async fn vortex_type_widening_read() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (_tmp, store, engine, table_url) = setup("vortex_type_widening");

    let schema = Arc::new(StructType::try_new(vec![StructField::nullable(
        "n",
        DataType::INTEGER,
    )])?);
    // Plain (3,7) table with no features, so the v1 Vortex append is accepted.
    json_create_table(
        store.clone(),
        table_url.clone(),
        schema.clone(),
        &[],
        true,
        vec![],
        vec![],
    )
    .await?;

    // v1: append a Vortex file with int values (physically int32 in the file).
    let arrow_schema: Arc<ArrowSchema> = Arc::new(schema.as_ref().try_into_arrow()?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int32Array::from(vec![100, 200, 300]))],
    )?;
    append_vortex_batch(&table_url, &engine, batch).await?;

    // v2: add the typeWidening feature (protocol) and widen `n` integer -> long (metaData). Reuse
    // v0's metaData (table id, format, createdTime) and only swap the schemaString, adding the
    // required `delta.typeChanges` metadata and the enablement config.
    let mut meta = read_actions_from_commit(&table_url, 0, "metaData")?.remove(0);
    let widened_schema = serde_json::json!({
        "type": "struct",
        "fields": [{
            "name": "n",
            "type": "long",
            "nullable": true,
            "metadata": {
                "delta.typeChanges": [{"toType": "long", "fromType": "integer", "tableVersion": 2}]
            }
        }]
    })
    .to_string();
    meta["schemaString"] = serde_json::Value::String(widened_schema);
    meta["configuration"]["delta.enableTypeWidening"] = serde_json::Value::String("true".into());
    let protocol = serde_json::json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["typeWidening"],
            "writerFeatures": ["typeWidening"],
        }
    });
    let commit = format!("{}\n{}", protocol, serde_json::json!({ "metaData": meta }));
    add_commit(table_url.as_str(), store.as_ref(), 2, commit).await?;

    // Scan at the widened version: the int32 Vortex file is read into the int64 logical schema.
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    assert_eq!(
        snapshot.schema().field("n").unwrap().data_type(),
        &DataType::LONG
    );

    let batches = scan_batches(&snapshot, engine.clone())?;
    let mut values: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let c = b.column(0).as_primitive::<Int64Type>();
            (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![100, 200, 300]);
    Ok(())
}
