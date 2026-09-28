//! Integration tests for the `concurrentIdentityColumns` writer feature (CIC), exercised through
//! public APIs: create-table enablement, the connector-facing report/acknowledge write gate, and
//! the ALTER-table rejection.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, SchemaRef, StructField, StructType,
};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{DeltaResult, Error, Snapshot};
use rstest::rstest;
use test_utils::{test_table_setup, TestCatalogCommitter};

// The `catalogManaged` (+ its `vacuumProtocolCheck`) properties CIC requires, plus a UC table id.
const CATALOG_MANAGED_PROPERTIES: [(&str, &str); 3] = [
    ("delta.feature.catalogManaged", "supported"),
    ("delta.feature.vacuumProtocolCheck", "supported"),
    ("io.unitycatalog.tableId", "cic-integration-test"),
];

/// A non-nullable LONG field carrying the three CIC metadata keys, built through the public
/// `ColumnMetadataKey` + `add_metadata` path (the kernel stamper is test-internal).
fn cic_field(name: &str, sequence_id: &str, start: i64, step: i64) -> StructField {
    StructField::not_null(name, DataType::LONG).add_metadata([
        (
            ColumnMetadataKey::IdentityConcurrentSequenceId
                .as_ref()
                .to_string(),
            MetadataValue::String(sequence_id.to_string()),
        ),
        (
            ColumnMetadataKey::IdentityStart.as_ref().to_string(),
            MetadataValue::Number(start),
        ),
        (
            ColumnMetadataKey::IdentityStep.as_ref().to_string(),
            MetadataValue::Number(step),
        ),
    ])
}

fn plain_schema() -> SchemaRef {
    Arc::new(
        StructType::try_new(vec![
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

fn cic_schema() -> SchemaRef {
    Arc::new(
        StructType::try_new(vec![
            cic_field("id", "seq-abc", 10, 2),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

/// `write_state` reports CIC columns and refuses to produce write state until the connector
/// acknowledges them. A table with no CIC reaches write state with no acknowledgement.
#[rstest]
#[case::with_cic(true)]
#[case::without_cic(false)]
#[tokio::test]
async fn write_state_ack_gate_for_concurrent_identity_columns(
    #[case] has_cic: bool,
) -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let engine = engine.as_ref();
    let schema = if has_cic {
        cic_schema()
    } else {
        plain_schema()
    };

    // A CIC schema auto-enables the identityColumns + concurrentIdentityColumns writer features
    // (create fails without `catalogManaged`, covered separately below).
    create_table(&table_path, schema, "Test/1.0")
        .with_table_properties(CATALOG_MANAGED_PROPERTIES)
        .build(engine, Box::new(TestCatalogCommitter))?
        .commit(engine)?
        .unwrap_committed();

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url)
        .with_max_catalog_version(0)
        .build(engine)?;
    let mut txn = snapshot.transaction(Box::new(TestCatalogCommitter), engine)?;

    let cics = txn.concurrent_identity_columns()?;
    if has_cic {
        assert_eq!(cics.len(), 1);
        assert_eq!(cics[0].column_name(), "id");
        assert_eq!(cics[0].sequence_id(), "seq-abc");
        assert_eq!(cics[0].start(), 10);
        assert_eq!(cics[0].step(), 2);
        drop(cics);

        let err = txn
            .write_state()
            .expect_err("write_state must require a CIC acknowledgement");
        assert!(
            matches!(err, Error::InvalidTransactionState(_)),
            "unexpected error: {err:?}"
        );

        txn.ack_concurrent_identity_columns();
        txn.write_state()?;
    } else {
        assert!(cics.is_empty());
        drop(cics);
        // No CIC column: write state is produced without any acknowledgement.
        txn.write_state()?;
    }
    Ok(())
}

/// Creating a table from a CIC schema (with `catalogManaged`) auto-enables both the
/// `identityColumns` and `concurrentIdentityColumns` writer features and round-trips the CIC
/// metadata onto the persisted schema; neither writer-only feature leaks into the reader features.
#[tokio::test]
async fn create_table_with_cic_schema_enables_identity_features() -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let engine = engine.as_ref();

    create_table(&table_path, cic_schema(), "Test/1.0")
        .with_table_properties(CATALOG_MANAGED_PROPERTIES)
        .build(engine, Box::new(TestCatalogCommitter))?
        .commit(engine)?
        .unwrap_committed();

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url)
        .with_max_catalog_version(0)
        .build(engine)?;

    let protocol = snapshot.table_configuration().protocol();
    let writer_features = protocol
        .writer_features()
        .expect("writer features must be present");
    let reader_features = protocol
        .reader_features()
        .expect("reader features must be present");

    assert!(writer_features.contains(&TableFeature::IdentityColumns));
    assert!(writer_features.contains(&TableFeature::ConcurrentIdentityColumns));
    // Both are writer-only and must not leak into the reader features.
    assert!(!reader_features.contains(&TableFeature::IdentityColumns));
    assert!(!reader_features.contains(&TableFeature::ConcurrentIdentityColumns));

    // The CIC metadata survives the create round-trip on the persisted schema.
    let id = snapshot
        .schema()
        .field("id")
        .expect("id column present")
        .clone();
    assert_eq!(
        id.get_config_value(&ColumnMetadataKey::IdentityConcurrentSequenceId),
        Some(&MetadataValue::String("seq-abc".to_string()))
    );

    Ok(())
}

/// A CIC schema without `catalogManaged` is rejected by the real create-table builder.
#[tokio::test]
async fn create_table_rejects_cic_without_catalog_managed() -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let result = create_table(&table_path, cic_schema(), "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    let err = result.expect_err("CIC without catalogManaged must be rejected");
    assert!(err.to_string().contains("catalog-managed"), "got: {err}");
    Ok(())
}

/// Introducing a CIC column via ALTER on a table lacking the feature is rejected (kernel does not
/// support enabling `concurrentIdentityColumns` through ALTER).
#[tokio::test]
async fn alter_table_rejects_adding_concurrent_identity_column() -> DeltaResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let engine = engine.as_ref();
    create_table(&table_path, plain_schema(), "Test/1.0")
        .build(engine, Box::new(FileSystemCommitter::new()))?
        .commit(engine)?
        .unwrap_committed();

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine)?;

    let result = snapshot
        .alter_table()
        .add_column(cic_field("new_id", "seq-1", 1, 1))
        .build(engine, Box::new(FileSystemCommitter::new()));
    assert!(
        result.is_err(),
        "adding a CIC column via ALTER must be rejected"
    );
    Ok(())
}
