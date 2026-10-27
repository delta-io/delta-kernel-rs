//! Integration tests for the `generatedColumns` writer feature: generated-column discovery on
//! `Snapshot` and `Transaction`, and the `ack_generated_columns` gate on commits and `write_state`.

use std::sync::Arc;

use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, GeneratedColumn, MetadataValue, SchemaRef, StructField, StructType,
};
use delta_kernel::transaction::{Transaction, UpdateTableOperation};
use delta_kernel::{KernelError, KernelResult, Snapshot};
use rstest::rstest;
use serde_json::json;
use test_utils::{
    add_commit, begin_transaction, begin_transaction_with, create_add_files_metadata, create_table,
    test_table_setup, test_table_setup_mt,
};
use url::Url;

use crate::common::write_utils::get_scan_files;

const GENERATED_COLUMNS_FEATURE: &[&str] = &["generatedColumns"];
const EVENT_YEAR: &[(&str, &str)] = &[("event_year", "YEAR(event_time)")];
const TESTING_GENERATED_COLUMNS: &[(&str, &str)] = &[
    // Expressions a user would expect.
    ("event_year", "YEAR(event_time)"),
    ("double_id", "id * 2"),
    ("next_age", "age + 1"),
    ("age_decade", "CAST(age / 10 AS INT)"),
    ("id_or_zero", "COALESCE(id, 0)"),
    // Unusual expressions, returned verbatim. The connector must parse these expressions and
    // reject invalid ones.
    ("empty", ""),
    ("blank", "   "),
    ("broken", "YEAR("),
];

/// The operation whose generated-column acknowledgement gate is under test.
#[derive(Clone, Copy, PartialEq)]
enum GatedOp {
    CommitWithData,
    RemoveOnlyCommit,
    MetadataOnlyCommit,
    WriteState,
}

/// A schema with `id`, `age`, and `event_time` columns followed by one INTEGER column per
/// `(name, raw_sql)` pair in `generated`, each carrying its SQL as `delta.generationExpression`
/// metadata.
fn schema_with_generated_columns(generated: &[(&str, &str)]) -> SchemaRef {
    let generated_fields = generated.iter().map(|(name, raw_sql)| {
        StructField::nullable(*name, DataType::INTEGER).add_metadata([(
            ColumnMetadataKey::GenerationExpression.as_ref(),
            MetadataValue::String(raw_sql.to_string()),
        )])
    });
    let fields = [
        StructField::nullable("id", DataType::INTEGER),
        StructField::nullable("age", DataType::INTEGER),
        StructField::nullable("event_time", DataType::TIMESTAMP),
    ]
    .into_iter()
    .chain(generated_fields);
    Arc::new(StructType::try_new(fields).expect("test schema must be valid"))
}

/// Writes version 0 of an unpartitioned table at `table_path` with a table-features protocol
/// listing `writer_features` and a schema declaring the `generated` columns. This builds a table in
/// any generated-column and feature state without going through CREATE TABLE.
async fn write_table(
    table_path: &str,
    writer_features: &[&str],
    generated: &[(&str, &str)],
) -> KernelResult<Url> {
    let table_url = Url::from_directory_path(table_path).expect("table path must be a URL");
    create_table(
        Arc::new(LocalFileSystem::new()),
        table_url,
        schema_with_generated_columns(generated),
        &[],
        true,
        vec![],
        writer_features.to_vec(),
    )
    .await
}

fn stage_one_file(txn: &mut Transaction) -> KernelResult<()> {
    let add = create_add_files_metadata(
        txn.add_files_schema(),
        vec![("part-00000.parquet", 1024, 1_000_000, Some(1))],
    )?;
    txn.add_files(add);
    Ok(())
}

/// Asserts `result` failed with the acknowledgement gate error.
fn assert_gate_error<T: std::fmt::Debug>(result: KernelResult<T>) {
    let err = result.expect_err("acknowledgement gate must reject this operation");
    let is_gate_error = matches!(
        &err,
        KernelError::InvalidTransactionState(msg) if msg.contains("ack_generated_columns")
    );
    assert!(
        is_gate_error,
        "expected the acknowledgement gate error, got: {err:?}"
    );
}

/// The `(name, raw_sql)` pairs of `generated_columns`, in order.
fn generated_column_pairs(generated_columns: &[GeneratedColumn]) -> Vec<(&str, &str)> {
    generated_columns
        .iter()
        .map(|column| (column.name(), column.raw_sql()))
        .collect()
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_and_transaction_return_stored_generated_columns_in_schema_order(
    #[values(false, true)] checkpoint: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = write_table(
        &table_path,
        GENERATED_COLUMNS_FEATURE,
        TESTING_GENERATED_COLUMNS,
    )
    .await?;

    let mut snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    if checkpoint {
        snapshot.checkpoint(engine.as_ref(), None)?;
        // A reloaded snapshot reads Metadata from the checkpoint instead of the commit.
        snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    }
    let snapshot_columns = snapshot.generated_columns()?;
    let from_snapshot = generated_column_pairs(&snapshot_columns);
    assert_eq!(from_snapshot, TESTING_GENERATED_COLUMNS);

    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    let txn_columns = txn.generated_columns()?;
    let from_txn = generated_column_pairs(&txn_columns);
    assert_eq!(from_txn, TESTING_GENERATED_COLUMNS);

    stage_one_file(&mut txn)?;
    assert_gate_error(txn.commit(engine.as_ref()));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn mapped_and_partitioned_tables_return_logical_names_and_require_ack_for_write_state(
    #[values(true, false)] column_mapping: bool,
    #[values(true, false)] partitioned: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let mut reader_features = vec![];
    let mut writer_features = GENERATED_COLUMNS_FEATURE.to_vec();
    if column_mapping {
        reader_features.push("columnMapping");
        writer_features.push("columnMapping");
    }
    let partition_columns: &[&str] = if partitioned { &["event_year"] } else { &[] };
    let table_url = create_table(
        Arc::new(LocalFileSystem::new()),
        Url::from_directory_path(&table_path).expect("table path must be a URL"),
        schema_with_generated_columns(EVENT_YEAR),
        partition_columns,
        true,
        reader_features,
        writer_features,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let generated_columns = snapshot.generated_columns()?;
    let from_snapshot = generated_column_pairs(&generated_columns);
    assert_eq!(from_snapshot, EVENT_YEAR);

    let txn = begin_transaction(snapshot, engine.as_ref())?;
    assert_gate_error(txn.write_state());
    Ok(())
}

#[rstest]
#[tokio::test]
async fn write_state_and_commits_require_ack_only_on_tables_with_generated_columns(
    #[values(
        GatedOp::CommitWithData,
        GatedOp::RemoveOnlyCommit,
        GatedOp::MetadataOnlyCommit,
        GatedOp::WriteState
    )]
    op: GatedOp,
    #[values(true, false)] declares_generated_columns: bool,
    #[values(true, false)] acknowledge: bool,
    #[values(true, false)] discover_first: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let generated = if declares_generated_columns {
        EVENT_YEAR
    } else {
        &[]
    };
    let table_url = write_table(&table_path, GENERATED_COLUMNS_FEATURE, generated).await?;

    let mut snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    if op == GatedOp::RemoveOnlyCommit {
        let mut seed_txn = begin_transaction(snapshot, engine.as_ref())?;
        seed_txn.ack_generated_columns();
        stage_one_file(&mut seed_txn)?;
        snapshot = seed_txn
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
    }

    let mut txn = begin_transaction(snapshot.clone(), engine.as_ref())?;
    // Discovery alone must not count as an acknowledgement.
    if discover_first {
        let _generated_columns = txn.generated_columns()?;
    }
    if acknowledge {
        txn.ack_generated_columns();
    }
    let mut txn = txn
        .with_transaction_id("app_id".to_string(), 1)
        .with_operation_metrics([("numFiles", Some("1"))]);

    match op {
        GatedOp::CommitWithData => stage_one_file(&mut txn)?,
        GatedOp::RemoveOnlyCommit => {
            let scan_files = get_scan_files(snapshot, engine.as_ref())?;
            let found_files = !scan_files.is_empty();
            assert!(found_files);
            for scan_file in scan_files {
                txn.remove_files(scan_file);
            }
        }
        GatedOp::MetadataOnlyCommit | GatedOp::WriteState => {}
    }
    let result = if op == GatedOp::WriteState {
        txn.write_state().map(|_write_state| ())
    } else {
        txn.commit(engine.as_ref()).map(|committed| {
            let _committed = committed.unwrap_committed();
        })
    };
    if acknowledge || !declares_generated_columns {
        result?;
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[tokio::test]
async fn expressions_without_feature_define_no_generated_columns_and_txn_build_fails(
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, &[], EVENT_YEAR).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let generated_columns = snapshot.generated_columns()?;
    assert!(generated_columns.is_empty());

    let result = begin_transaction(snapshot, engine.as_ref());
    let rejected_as_malformed = matches!(result, Err(KernelError::InvalidProtocol(_)));
    assert!(rejected_as_malformed);
    Ok(())
}

#[tokio::test]
async fn protocol_upgrade_turns_existing_expressions_into_generated_columns_that_require_ack(
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, &[], EVENT_YEAR).await?;
    let before_upgrade = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let protocol = json!({"protocol": {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": [],
        "writerFeatures": GENERATED_COLUMNS_FEATURE,
    }});
    add_commit(
        table_url.as_str(),
        &LocalFileSystem::new(),
        1,
        format!("{protocol}\n"),
    )
    .await?;

    let incremental = Snapshot::builder_from(before_upgrade).build(engine.as_ref())?;
    let incremental_columns = incremental.generated_columns()?;
    let from_incremental = generated_column_pairs(&incremental_columns);
    assert_eq!(from_incremental, EVENT_YEAR);

    let time_travel = Snapshot::builder_for(table_url)
        .at_version(0)
        .build(engine.as_ref())?;
    let time_travel_columns = time_travel.generated_columns()?;
    assert!(time_travel_columns.is_empty());

    let mut txn = begin_transaction(incremental, engine.as_ref())?;
    stage_one_file(&mut txn)?;
    assert_gate_error(txn.commit(engine.as_ref()));
    Ok(())
}

#[tokio::test]
async fn alter_table_build_fails_on_tables_with_generated_columns() -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, GENERATED_COLUMNS_FEATURE, EVENT_YEAR).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let result = begin_transaction_with(
        snapshot,
        engine.as_ref(),
        UpdateTableOperation::AlterTable,
        "Test/1.0",
        false,
        |builder| builder.add_column(StructField::nullable("note", DataType::STRING)),
    );

    let rejected = matches!(result, Err(KernelError::Unsupported(_)));
    assert!(rejected);
    Ok(())
}
