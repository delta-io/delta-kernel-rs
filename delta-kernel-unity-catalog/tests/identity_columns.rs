//! Integration tests for Concurrent Identity Columns (CIC).
//!
//! Covers both ends of the CIC write flow:
//! - Reservation: kernel's `SequenceReserver` backed by a UC sequence client bridged via
//!   [`UCSequenceReserver`], including the step-mismatch enforcement.
//! - CREATE TABLE orchestration: the engine allocates UC sequences via
//!   [`create_identity_sequences`], stamps the resulting IDs into the schema via
//!   [`identity_column_cic`], and commits a table with the `identityColumnsCic` writer feature
//!   auto-enabled.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::identity_columns::{
    detect_identity_columns, identity_column_cic, IdentityColumnInfo, SequenceReserver,
};
use delta_kernel::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::storage::store_from_url;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_unity_catalog::{
    create_identity_sequences, IdentityColumnSpec, UCSequenceReserver,
};
use unity_catalog_delta_client_api::InMemorySequenceClient;

type TestError = Box<dyn std::error::Error + Send + Sync>;

/// Table id the reservation tests seed sequences under and reserve from.
const SEQ_TABLE: &str = "tbl-seq";

fn seeded_client(sequences: &[(&str, i64, i64)]) -> Arc<InMemorySequenceClient> {
    let client = Arc::new(InMemorySequenceClient::new());
    for &(id, start, step) in sequences {
        client.seed_sequence(SEQ_TABLE, id, start, step).unwrap();
    }
    client
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
// Reservation tests (schema already carries sequence_id)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn reserve_single_identity_column() {
    let client = seeded_client(&[("seq-1", 1, 1)]);
    let reserver = UCSequenceReserver::new(client, SEQ_TABLE);

    let schema = Arc::new(
        StructType::try_new(vec![
            identity_column_cic("id", "seq-1", 1, 1),
            StructField::new("value", DataType::STRING, true),
        ])
        .unwrap(),
    );

    let cols = detect_identity_columns(&schema).unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(
        cols[0],
        IdentityColumnInfo {
            column_name: "id".to_string(),
            sequence_id: "seq-1".to_string(),
            start: 1,
            step: 1,
            allow_explicit_insert: false,
        }
    );

    let reservation = reserver.reserve_ids("seq-1", 1, 10).unwrap();
    assert_eq!(reservation.range_start, 1);
    assert_eq!(reservation.range_end, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn reserve_multiple_identity_columns() {
    let client = seeded_client(&[("seq-a", 0, 2), ("seq-b", 100, 10)]);
    let reserver = UCSequenceReserver::new(client, SEQ_TABLE);

    let schema = Arc::new(
        StructType::try_new(vec![
            identity_column_cic("col_a", "seq-a", 0, 2),
            StructField::new("data", DataType::STRING, true),
            identity_column_cic("col_b", "seq-b", 100, 10),
        ])
        .unwrap(),
    );

    let cols = detect_identity_columns(&schema).unwrap();
    assert_eq!(cols.len(), 2);

    let r_a = reserver.reserve_ids("seq-a", 2, 5).unwrap();
    assert_eq!(r_a.range_start, 0);
    assert_eq!(r_a.range_end, 8); // 0, 2, 4, 6, 8

    let r_b = reserver.reserve_ids("seq-b", 10, 3).unwrap();
    assert_eq!(r_b.range_start, 100);
    assert_eq!(r_b.range_end, 120); // 100, 110, 120
}

#[tokio::test(flavor = "multi_thread")]
async fn successive_reservations_non_overlapping() {
    let client = seeded_client(&[("seq-1", 1, 1)]);
    let reserver = UCSequenceReserver::new(client, SEQ_TABLE);

    let r1 = reserver.reserve_ids("seq-1", 1, 100).unwrap();
    let r2 = reserver.reserve_ids("seq-1", 1, 100).unwrap();
    let r3 = reserver.reserve_ids("seq-1", 1, 50).unwrap();

    assert!(r1.range_end < r2.range_start);
    assert!(r2.range_end < r3.range_start);
}

#[tokio::test(flavor = "multi_thread")]
async fn reserve_ids_rejects_step_mismatch() {
    let client = seeded_client(&[("seq-1", 1, 1)]);
    let reserver = UCSequenceReserver::new(client, SEQ_TABLE);

    // Service thinks step=1, caller (via schema) claims step=2.
    let err = reserver.reserve_ids("seq-1", 2, 10).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("step") && msg.contains("seq-1"),
        "unexpected error message: {msg}"
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

    // 1. Allocate sequences in UC for each identity column the engine wants.
    let infos = create_identity_sequences(
        client.as_ref(),
        table_id,
        &[
            IdentityColumnSpec::new("id", 1, 1),
            IdentityColumnSpec::new("row_id", 1000, 10),
        ],
    )
    .await?;
    assert_eq!(infos.len(), 2);
    assert_eq!(infos[0].column_name, "id");
    assert_eq!(infos[1].column_name, "row_id");
    assert!(
        infos[0].sequence_id != infos[1].sequence_id,
        "every CreateIdentitySequence call should mint a fresh id"
    );

    // 2. Stamp sequence_ids into the schema.
    let schema = Arc::new(StructType::try_new(vec![
        identity_column_cic(
            &infos[0].column_name,
            &infos[0].sequence_id,
            infos[0].start,
            infos[0].step,
        ),
        StructField::new("payload", DataType::STRING, true),
        identity_column_cic(
            &infos[1].column_name,
            &infos[1].sequence_id,
            infos[1].start,
            infos[1].step,
        ),
    ])?);

    // 3. Create the table. Kernel auto-enables identityColumnsCic.
    let _commit = create_table(&table_path, schema, "UCCatalogTest/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    // 4. Reload and assert protocol + schema.
    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

    let table_config = snapshot.table_configuration();
    assert!(
        table_config.is_feature_supported(&TableFeature::IdentityColumnsCic),
        "identityColumnsCic should be supported after CREATE TABLE"
    );

    let read_schema = snapshot.schema();
    let read_cols = detect_identity_columns(&read_schema)?;
    assert_eq!(read_cols.len(), 2);
    assert_eq!(read_cols[0].sequence_id, infos[0].sequence_id);
    assert_eq!(read_cols[0].start, 1);
    assert_eq!(read_cols[0].step, 1);
    assert_eq!(read_cols[1].sequence_id, infos[1].sequence_id);
    assert_eq!(read_cols[1].start, 1000);
    assert_eq!(read_cols[1].step, 10);

    // And the sequences really exist in UC -- reserve via the same client, scoped to the table
    // they were created under.
    let reserver = UCSequenceReserver::new(client.clone(), table_id);
    let range = reserver.reserve_ids(&read_cols[0].sequence_id, read_cols[0].step, 3)?;
    assert_eq!(range.range_start, 1);
    assert_eq!(range.range_end, 3);

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
