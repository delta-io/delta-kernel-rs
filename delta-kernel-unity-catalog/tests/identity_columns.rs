//! Integration tests for Concurrent Identity Columns (CIC).
//!
//! CIC is restricted to catalog-managed tables, so these tables enable `catalogManaged` and commit
//! through a catalog committer ([`TestCatalogCommitter`], which writes to the published path so a
//! plain `Snapshot::builder_for` reads them back).
//!
//! Covers both ends of the CIC write flow through the public API:
//! - CREATE TABLE orchestration: mint sequence ids, stamp them into the schema via [`cic_column`],
//!   commit a catalog-managed table with `concurrentIdentityColumns` auto-enabled, then register
//!   the sequences with the service.
//! - Write: the connector discovers the columns to fill via
//!   [`Transaction::concurrent_identity_columns`], reserves ranges through a `SequenceClient`,
//!   generates values itself (`range_start + step * i`), fills the batch, and acknowledges via
//!   [`Transaction::ack_concurrent_identity_columns`]. Kernel neither reserves, generates, nor
//!   inserts values, but gates `write_state` on the acknowledgement.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use delta_kernel::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::identity_columns::{cic_column, ConcurrentIdentityColumn, IdentityColumnInfo};
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{Engine, Error};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::storage::store_from_url;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use test_utils::TestCatalogCommitter;
use unity_catalog_delta_client_api::{
    CreateIdentitySequences, IdentityReservation, IdentitySequenceSpec, InMemorySequenceClient,
    ReserveIdentityRanges, SequenceClient,
};
use uuid::Uuid;

type TestEngine = DefaultEngine<TokioMultiThreadExecutor>;
type TestError = Box<dyn std::error::Error + Send + Sync>;

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
            cic_column(
                &cols[0].column_name,
                &cols[0].sequence_id,
                cols[0].start,
                cols[0].step,
            ),
            StructField::new("payload", DataType::STRING, true),
            cic_column(
                &cols[1].column_name,
                &cols[1].sequence_id,
                cols[1].start,
                cols[1].step,
            ),
        ])
        .unwrap(),
    )
}

/// Creates and commits (v0) a catalog-managed table carrying the given CIC columns. CIC requires
/// `catalogManaged`, so the create enables it (which auto-enables `inCommitTimestamp`) and commits
/// through a catalog committer.
fn create_cic_table(
    engine: &TestEngine,
    table_path: &str,
    table_id: &str,
    cols: &[IdentityColumnInfo],
) -> Result<(), TestError> {
    let _ = create_table(table_path, schema_for(cols), "cic-test/1.0")
        .with_table_properties([
            ("delta.feature.catalogManaged", "supported"),
            ("io.unitycatalog.tableId", table_id),
        ])
        .build(engine, Box::new(TestCatalogCommitter))?
        .commit(engine)?;
    Ok(())
}

fn build_engine(path: &str) -> Result<Arc<TestEngine>, TestError> {
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

/// Connector-side: reserve `n` values for every CIC in one batched RPC and generate them by
/// enumerating each reserved range (`range_start + step * i`), keyed by logical column name. This
/// is orchestration a real connector owns -- kernel provides only the report
/// ([`ConcurrentIdentityColumn`]).
async fn reserve_and_generate<C: SequenceClient>(
    client: &C,
    table_id: &str,
    cics: &[ConcurrentIdentityColumn<'_>],
    n: i64,
) -> HashMap<String, Vec<i64>> {
    let response = client
        .reserve_identity_ranges(ReserveIdentityRanges {
            table_id: table_id.to_string(),
            reservations: cics
                .iter()
                .map(|c| IdentityReservation {
                    sequence_id: c.sequence_id().to_string(),
                    count: n,
                    step: Some(c.step()),
                })
                .collect(),
        })
        .await
        .unwrap();
    // The response ranges carry their sequence id; match each back to its column and enumerate.
    let ranges: HashMap<&str, &_> = response
        .ranges
        .iter()
        .map(|r| (r.sequence_id.as_str(), r))
        .collect();
    cics.iter()
        .map(|c| {
            let r = ranges[c.sequence_id()];
            let values = (0..n).map(|i| r.range_start + r.step * i).collect();
            (c.column_name().to_string(), values)
        })
        .collect()
}

/// Build the full batch (id, payload, row_id in schema order) the connector hands to the writer.
fn build_filled_batch(id: &[i64], payload: &[&str], row_id: &[i64]) -> ArrowEngineData {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("payload", ArrowDataType::Utf8, true),
        ArrowField::new("row_id", ArrowDataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(id.to_vec())),
        Arc::new(StringArray::from(payload.to_vec())),
        Arc::new(Int64Array::from(row_id.to_vec())),
    ];
    ArrowEngineData::new(RecordBatch::try_new(arrow_schema, columns).unwrap())
}

fn i64_col(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let idx = batch.schema().index_of(name).unwrap();
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

// ============================================================================
// Report + ack surface
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_identity_columns_reports_and_gates_write_state() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_path = tmp.path().to_str().ok_or("bad tmp path")?.to_string();
    let engine = build_engine(&table_path)?;

    let cols = [column("id", 1, 1), column("row_id", 1000, 10)];
    create_cic_table(engine.as_ref(), &table_path, "tbl-cic-report", &cols)?;

    // Catalog-managed tables load against a catalog watermark; create committed v0.
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url)
        .with_max_catalog_version(0)
        .build(engine.as_ref())?;

    // The report surfaces both CIC columns with their sequence parameters.
    let mut txn = snapshot
        .transaction(Box::new(TestCatalogCommitter), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_data_change(true);
    let reported = txn.concurrent_identity_columns()?;
    let names: Vec<&str> = reported.iter().map(|c| c.column_name()).collect();
    assert_eq!(names, vec!["id", "row_id"]);
    assert_eq!(reported[1].sequence_id(), cols[1].sequence_id);
    assert_eq!(reported[1].step(), 10);

    // write_state is gated until the connector acknowledges filling the columns.
    let err = txn.write_state().err().unwrap();
    assert!(
        matches!(err, Error::InvalidTransactionState(_))
            && err.to_string().contains("ack_concurrent_identity_columns"),
        "unexpected error: {err}"
    );

    txn.ack_concurrent_identity_columns();
    assert!(txn.write_state().is_ok(), "ack should unblock write_state");
    Ok(())
}

// ============================================================================
// CREATE TABLE + end-to-end write
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn create_write_and_read_back_generates_identity_values() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_path = tmp.path().to_str().ok_or("bad tmp path")?.to_string();
    let engine = build_engine(&table_path)?;

    let table_id = "tbl-cic-demo";
    let client = Arc::new(InMemorySequenceClient::new());
    let cols = [column("id", 1, 1), column("row_id", 1000, 10)];

    // CREATE the catalog-managed table first (auto-enables concurrentIdentityColumns), then
    // register the sequences in UC -- the connector builds the batch from the columns it minted.
    create_cic_table(engine.as_ref(), &table_path, table_id, &cols)?;
    client
        .create_identity_sequences(CreateIdentitySequences {
            table_id: table_id.to_string(),
            sequences: cols
                .iter()
                .map(|c| IdentitySequenceSpec {
                    sequence_id: c.sequence_id.clone(),
                    start: c.start,
                    step: c.step,
                })
                .collect(),
        })
        .await?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url.clone())
        .with_max_catalog_version(0)
        .build(engine.as_ref())?;
    assert!(snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::ConcurrentIdentityColumns));

    // Connector: discover CIC columns, reserve + generate, fill the batch, ack, write.
    let mut txn = snapshot
        .clone()
        .transaction(Box::new(TestCatalogCommitter), engine.as_ref())?
        .with_operation("WRITE".to_string())
        .with_data_change(true);

    let cics = txn.concurrent_identity_columns()?;
    let generated = reserve_and_generate(client.as_ref(), table_id, &cics, 3).await;
    let filled = build_filled_batch(&generated["id"], &["a", "b", "c"], &generated["row_id"]);
    drop(cics); // release the borrow of txn before mutating it
    txn.ack_concurrent_identity_columns();

    let write_context = txn.write_state()?.write_context_builder().build()?;
    let add_files = engine.write_parquet(&filled, &write_context).await?;
    txn.add_files(add_files);
    let CommitResult::CommittedTransaction(_) = txn.commit(engine.as_ref())? else {
        return Err("commit did not succeed".into());
    };

    // Read back and assert the generated identity values landed (write committed v1).
    let snapshot_v1 = Snapshot::builder_for(table_url)
        .with_max_catalog_version(1)
        .build(engine.as_ref())?;
    let scan = snapshot_v1.scan_builder().build()?;
    let engine_dyn: Arc<dyn Engine> = engine.clone();
    let mut ids = Vec::new();
    let mut row_ids = Vec::new();
    for batch in scan.execute(engine_dyn)? {
        let rb = ArrowEngineData::try_from_engine_data(batch?)?
            .record_batch()
            .clone();
        ids.extend(i64_col(&rb, "id"));
        row_ids.extend(i64_col(&rb, "row_id"));
    }
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(row_ids, vec![1000, 1010, 1020]);
    Ok(())
}

/// ALTER TABLE cannot introduce a valid CIC column. A non-nullable CIC column is blocked by the
/// "added columns must be nullable" rule; a *nullable* field carrying CIC metadata reaches the
/// ALTER-path CIC validation, which rejects it (CIC columns must be non-nullable). Either way, the
/// ALTER hook prevents an inconsistent CIC column from landing.
#[tokio::test(flavor = "multi_thread")]
async fn alter_table_rejects_adding_cic_column() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_path = tmp.path().to_str().ok_or("bad tmp path")?.to_string();
    let engine = build_engine(&table_path)?;

    // A plain table with no CIC columns / feature.
    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("a", DataType::LONG, false),
        StructField::new("b", DataType::STRING, true),
    ])?);
    let _ = create_table(&table_path, schema, "cic-test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    // A nullable field carrying CIC metadata: passes the "added columns must be nullable" rule,
    // then the ALTER-path CIC validation rejects it because CIC columns must be non-nullable.
    let nullable_cic = StructField::new("id", DataType::LONG, true).with_metadata(vec![
        (
            ColumnMetadataKey::IdentityConcurrentSequenceId
                .as_ref()
                .to_string(),
            MetadataValue::String("seq-x".to_string()),
        ),
        (
            ColumnMetadataKey::IdentityStart.as_ref().to_string(),
            MetadataValue::Number(1),
        ),
        (
            ColumnMetadataKey::IdentityStep.as_ref().to_string(),
            MetadataValue::Number(1),
        ),
    ]);
    let err = snapshot
        .alter_table()
        .add_column(nullable_cic)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap_err();
    assert!(
        err.to_string().contains("non-nullable"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_rejects_non_long_identity_column() -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let table_path = tmp.path().to_str().ok_or("bad tmp path")?.to_string();
    let engine = build_engine(&table_path)?;

    let bad_field = StructField::new("id", DataType::INTEGER, false).with_metadata(vec![
        (
            ColumnMetadataKey::IdentityConcurrentSequenceId
                .as_ref()
                .to_string(),
            MetadataValue::String("seq-bogus".to_string()),
        ),
        (
            ColumnMetadataKey::IdentityStart.as_ref().to_string(),
            MetadataValue::Number(1),
        ),
        (
            ColumnMetadataKey::IdentityStep.as_ref().to_string(),
            MetadataValue::Number(1),
        ),
    ]);
    let schema = Arc::new(StructType::try_new(vec![bad_field])?);

    let err = create_table(&table_path, schema, "cic-test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap_err();
    assert!(
        err.to_string().contains("must be of type LONG"),
        "unexpected error: {err}"
    );
    Ok(())
}
