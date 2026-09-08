//! Integration tests for Concurrent Identity Columns (CIC).
//!
//! Covers both ends of the CIC write flow through the public API:
//! - CREATE TABLE orchestration: mint sequence ids, register them via
//!   [`create_identity_sequences`], stamp them into the schema via [`identity_column_cic`], and
//!   commit a table with the `identityColumnsCic` writer feature auto-enabled.
//! - Reserve + fill: an [`IdentityColumnManager`] built from the table schema reserves ranges and
//!   fills batches with generated identity values.

use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::identity_columns::{
    detect_identity_columns, identity_column_cic, IdentityColumnInfo,
};
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::EngineData;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::storage::store_from_url;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_unity_catalog::{create_identity_sequences, IdentityColumnManager};
use unity_catalog_delta_client_api::InMemorySequenceClient;
use uuid::Uuid;

type TestError = Box<dyn std::error::Error + Send + Sync>;

/// Table id the tests seed / create sequences under.
const SEQ_TABLE: &str = "tbl-seq";

fn column(name: &str, start: i64, step: i64) -> IdentityColumnInfo {
    IdentityColumnInfo {
        column_name: name.to_string(),
        sequence_id: Uuid::new_v4().to_string(),
        start,
        step,
        allow_explicit_insert: false,
    }
}

/// Schema with two identity columns around a payload column.
fn schema_for(cols: &[IdentityColumnInfo]) -> SchemaRef {
    Arc::new(
        StructType::try_new(vec![
            identity_column_cic(
                &cols[0].column_name,
                &cols[0].sequence_id,
                cols[0].start,
                cols[0].step,
            ),
            StructField::new("payload", DataType::STRING, true),
            identity_column_cic(
                &cols[1].column_name,
                &cols[1].sequence_id,
                cols[1].start,
                cols[1].step,
            ),
        ])
        .unwrap(),
    )
}

fn payload_batch(rows: usize) -> ArrowEngineData {
    let payload: ArrayRef = Arc::new(StringArray::from(
        (0..rows).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
    ));
    let arrow_schema = Arc::new(Schema::new(vec![ArrowField::new(
        "payload",
        ArrowDataType::Utf8,
        true,
    )]));
    ArrowEngineData::new(RecordBatch::try_new(arrow_schema, vec![payload]).unwrap())
}

fn i64_col(filled: Box<dyn EngineData>, name: &str) -> Vec<i64> {
    let batch = ArrowEngineData::try_from_engine_data(filled)
        .unwrap()
        .record_batch()
        .clone();
    let idx = batch.schema().index_of(name).unwrap();
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn build_engine(
    path: &str,
) -> Result<Arc<delta_kernel_default_engine::DefaultEngine<TokioMultiThreadExecutor>>, TestError> {
    let table_url = url::Url::from_directory_path(path).map_err(|_| "invalid path")?;
    let store = store_from_url(&table_url)?;
    Ok(Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            )))
            .build(),
    ))
}

// ============================================================================
// Reserve + fill via IdentityColumnManager
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn manager_reserve_and_fill_multiple_columns() {
    let cols = [column("id", 1, 1), column("row_id", 100, 10)];
    let client = Arc::new(InMemorySequenceClient::new());
    create_identity_sequences(client.as_ref(), SEQ_TABLE, &cols)
        .await
        .unwrap();
    let schema = schema_for(&cols);

    let manager = IdentityColumnManager::new(&schema, client, SEQ_TABLE).unwrap();
    manager.reserve(5).await.unwrap();

    let filled = manager
        .fill_engine_batch(&payload_batch(3), &schema)
        .unwrap();
    assert_eq!(i64_col(filled, "id").as_slice(), &[1, 2, 3]);

    // The second column advanced in lockstep from the same batched reserve.
    let filled = manager
        .fill_engine_batch(&payload_batch(2), &schema)
        .unwrap();
    assert_eq!(i64_col(filled, "row_id").as_slice(), &[130, 140]);
}

#[tokio::test(flavor = "multi_thread")]
async fn manager_ensure_available_then_fill() {
    let cols = [column("id", 1, 1), column("row_id", 1000, 10)];
    let client = Arc::new(InMemorySequenceClient::new());
    create_identity_sequences(client.as_ref(), SEQ_TABLE, &cols)
        .await
        .unwrap();
    let schema = schema_for(&cols);

    let manager = IdentityColumnManager::new(&schema, client, SEQ_TABLE).unwrap();
    // Nothing prefetched: ensure_available reserves the deficit itself.
    manager.ensure_available(3).await.unwrap();
    let filled = manager
        .fill_engine_batch(&payload_batch(3), &schema)
        .unwrap();
    assert_eq!(i64_col(filled, "row_id").as_slice(), &[1000, 1010, 1020]);
}

#[tokio::test(flavor = "multi_thread")]
async fn manager_reports_step_mismatch() {
    // Seed a sequence whose stored step (1) disagrees with the schema (2).
    let client = Arc::new(InMemorySequenceClient::new());
    client.seed_sequence(SEQ_TABLE, "seq-id", 1, 1).unwrap();
    client.seed_sequence(SEQ_TABLE, "seq-row", 1, 1).unwrap();
    let schema = Arc::new(
        StructType::try_new(vec![
            identity_column_cic("id", "seq-id", 1, 2), // step 2, but seeded step is 1
            StructField::new("payload", DataType::STRING, true),
            identity_column_cic("row_id", "seq-row", 1, 1),
        ])
        .unwrap(),
    );

    let manager = IdentityColumnManager::new(&schema, client, SEQ_TABLE).unwrap();
    let err = manager.reserve(5).await.unwrap_err();
    assert!(
        err.to_string().contains("step") && err.to_string().contains("seq-id"),
        "unexpected error: {err}"
    );
}

#[test]
fn table_without_identity_columns_returns_empty() {
    let schema = Arc::new(
        StructType::try_new(vec![
            StructField::new("id", DataType::LONG, false),
            StructField::new("name", DataType::STRING, true),
        ])
        .unwrap(),
    );

    let cols = detect_identity_columns(&schema).unwrap();
    assert!(cols.is_empty());
}

// ============================================================================
// CREATE TABLE end-to-end
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn create_table_allocates_uc_sequence_and_enables_feature() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_path = tmp.path().to_str().ok_or("bad tmp path")?.to_string();
    let engine = build_engine(&table_path)?;

    let table_id = "tbl-cic-demo";
    let client = Arc::new(InMemorySequenceClient::new());

    // 1. Mint ids (each column gets a distinct sequence_id).
    let cols = [column("id", 1, 1), column("row_id", 1000, 10)];
    assert!(
        cols[0].sequence_id != cols[1].sequence_id,
        "each column should get a distinct minted id"
    );

    // 2. Stamp sequence_ids into the schema and commit the CREATE-table transaction first
    //    (auto-enables identityColumnsCic).
    let schema = schema_for(&cols);
    let _commit = create_table(&table_path, schema, "UCCatalogTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // 3. Only after the table exists, register the sequences in UC.
    create_identity_sequences(client.as_ref(), table_id, &cols).await?;

    // 4. Reload and assert protocol + schema.
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert!(
        snapshot
            .table_configuration()
            .is_feature_supported(&TableFeature::IdentityColumnsCic),
        "identityColumnsCic should be supported after CREATE TABLE"
    );

    let read_schema = snapshot.schema();
    let read_cols = detect_identity_columns(&read_schema)?;
    assert_eq!(read_cols.len(), 2);
    assert_eq!(read_cols[0].sequence_id, cols[0].sequence_id);
    assert_eq!(read_cols[1].sequence_id, cols[1].sequence_id);

    // 5. The sequences really exist in UC: a manager built from the reloaded schema can reserve and
    //    fill.
    let manager = IdentityColumnManager::new(&read_schema, client, table_id)?;
    manager.ensure_available(3).await?;
    let filled = manager.fill_engine_batch(&payload_batch(3), &read_schema)?;
    assert_eq!(i64_col(filled, "id").as_slice(), &[1, 2, 3]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_rejects_non_long_identity_column() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_path = tmp.path().to_str().ok_or("bad tmp path")?.to_string();
    let engine = build_engine(&table_path)?;

    let bad_field = StructField::new("id", DataType::INTEGER, false).with_metadata(vec![
        (
            ColumnMetadataKey::IdentityCicSequenceId
                .as_ref()
                .to_string(),
            MetadataValue::String("seq-bogus".to_string()),
        ),
        (
            ColumnMetadataKey::IdentityCicStart.as_ref().to_string(),
            MetadataValue::Number(1),
        ),
        (
            ColumnMetadataKey::IdentityCicStep.as_ref().to_string(),
            MetadataValue::Number(1),
        ),
    ]);
    let schema = Arc::new(StructType::try_new(vec![bad_field])?);

    let err = create_table(&table_path, schema, "UCCatalogTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap_err();
    assert!(
        err.to_string().contains("must be of type LONG"),
        "unexpected error: {err}"
    );

    Ok(())
}
