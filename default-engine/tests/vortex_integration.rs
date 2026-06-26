//! End-to-end integration tests for the Vortex alternate file-format POC.
//!
//! These exercise the full kernel path -- `create_table` -> write data file -> `add_files` ->
//! `commit` -> `Snapshot` -> `Scan` -> `execute` -- using the [`DefaultEngine`]'s `write_vortex`
//! writer and the per-file extension dispatch in the read path. The headline test is
//! [`test_mixed_parquet_vortex_scan`]: a single table whose data files are a mix of `.parquet` and
//! `.vortex`, read back correctly by one scan.

use std::sync::Arc;

use delta_kernel::arrow::array::{Int32Array, RecordBatch};
use delta_kernel::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::DefaultEngine;
use test_utils::{engine_store_setup, read_scan, test_read};
use url::Url;

type TestEngine = Arc<DefaultEngine<TokioBackgroundExecutor>>;

/// Single nullable `number: int` column (matches the existing append-test fixture).
fn number_schema() -> SchemaRef {
    Arc::new(StructType::try_new(vec![StructField::nullable("number", DataType::INTEGER)]).unwrap())
}

fn number_batch(schema: &SchemaRef, values: Vec<i32>) -> ArrowEngineData {
    let arrow_schema: ArrowSchema = schema.as_ref().try_into_arrow().unwrap();
    ArrowEngineData::new(
        RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap(),
    )
}

/// Creates an empty table with the given schema (column mapping None, no DVs, no row tracking).
fn create_empty_table(url: &Url, schema: SchemaRef, engine: &dyn Engine) -> DeltaResult<()> {
    let result = create_table(url.as_str(), schema, "vortex-poc")
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?;
    assert!(result.is_committed());
    Ok(())
}

/// Appends a single Vortex-encoded data file containing `values` and commits it.
async fn append_vortex(url: &Url, engine: TestEngine, values: Vec<i32>) -> DeltaResult<()> {
    let schema = number_schema();
    let mut txn = test_utils::load_and_begin_transaction(url.clone(), engine.as_ref())?
        .with_data_change(true);
    let write_context = txn.unpartitioned_write_context()?;
    let data = number_batch(&schema, values);
    let add = engine.write_vortex(&data, &write_context).await?;
    txn.add_files(add);
    assert!(txn.commit(engine.as_ref())?.is_committed());
    Ok(())
}

/// Appends a single Parquet-encoded data file containing `values` and commits it.
async fn append_parquet(url: &Url, engine: TestEngine, values: Vec<i32>) -> DeltaResult<()> {
    let schema = number_schema();
    let mut txn = test_utils::load_and_begin_transaction(url.clone(), engine.as_ref())?
        .with_data_change(true);
    let write_context = txn.unpartitioned_write_context()?;
    let data = number_batch(&schema, values);
    let add = engine.write_parquet(&data, &write_context).await?;
    txn.add_files(add);
    assert!(txn.commit(engine.as_ref())?.is_committed());
    Ok(())
}

/// Scans the table and returns every `number` value, sorted (so the assertion does not depend on
/// cross-file / cross-commit scan ordering).
fn scan_numbers_sorted(url: &Url, engine: TestEngine) -> DeltaResult<Vec<i32>> {
    use delta_kernel::arrow::array::AsArray as _;
    use delta_kernel::arrow::datatypes::Int32Type;

    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;
    let batches = read_scan(&scan, engine)?;
    let mut values: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_primitive::<Int32Type>();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    Ok(values)
}

#[tokio::test]
async fn test_vortex_write_commit_read_roundtrip() -> DeltaResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = number_schema();
    let (_store, engine, url) = engine_store_setup("vortex_roundtrip", None);
    let engine = Arc::new(engine);

    create_empty_table(&url, schema.clone(), engine.as_ref())?;
    append_vortex(&url, engine.clone(), vec![1, 2, 3]).await?;

    // Read back through a full scan and assert the rows equal what was written.
    let expected = number_batch(&schema, vec![1, 2, 3]);
    test_read(&expected, &url, engine)?;
    Ok(())
}

#[tokio::test]
async fn test_mixed_parquet_vortex_scan() -> DeltaResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let schema = number_schema();
    let (_store, engine, url) = engine_store_setup("vortex_mixed", None);
    let engine = Arc::new(engine);

    create_empty_table(&url, schema, engine.as_ref())?;
    // One Parquet append and one Vortex append, in separate commits. A single scan must read both.
    append_parquet(&url, engine.clone(), vec![1, 2, 3]).await?;
    append_vortex(&url, engine.clone(), vec![4, 5, 6]).await?;

    let values = scan_numbers_sorted(&url, engine)?;
    assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
    Ok(())
}
