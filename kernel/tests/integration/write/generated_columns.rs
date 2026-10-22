//! Integration tests for the `generatedColumns` writer feature: generated-column discovery on
//! `Snapshot` and `Transaction`, and the `ack_generated_columns` gate on commits and `write_state`.

use std::sync::Arc;

use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{
    ColumnMetadataKey, DataType, GeneratedColumn, MetadataValue, SchemaRef, StructField, StructType,
};
use delta_kernel::transaction::{Transaction, UpdateTableOperation};
use delta_kernel::{Engine, KernelError, KernelResult, Snapshot};
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
#[derive(Clone, Copy)]
enum GatedOp {
    CommitWithData,
    MetadataOnlyCommit,
    WriteState,
}

/// A schema with `id`, `age`, and `event_time` columns followed by one INTEGER column per
/// `(name, raw_sql)` pair in `generated`, each carrying its SQL as `delta.generationExpression`
/// metadata.
fn test_schema(generated: &[(&str, &str)]) -> SchemaRef {
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

/// Writes version 0 of an unpartitioned table at `table_path` whose protocol lists
/// `writer_features` and whose schema declares the `generated` columns. Writing the commit directly
/// controls the protocol's writer features.
async fn write_table(
    table_path: &str,
    writer_features: &[&str],
    generated: &[(&str, &str)],
) -> KernelResult<Url> {
    let table_url = Url::from_directory_path(table_path).expect("table path must be a URL");
    create_table(
        Arc::new(LocalFileSystem::new()),
        table_url,
        test_schema(generated),
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

/// Performs `op` on `txn` and returns the result of the step the acknowledgement gate guards.
fn run_gated_op(op: GatedOp, mut txn: Transaction, engine: &dyn Engine) -> KernelResult<()> {
    match op {
        GatedOp::CommitWithData => {
            stage_one_file(&mut txn)?;
            txn.commit(engine).map(|committed| {
                let _committed = committed.unwrap_committed();
            })
        }
        GatedOp::MetadataOnlyCommit => txn
            .with_transaction_id("app_id".to_string(), 1)
            .commit(engine)
            .map(|committed| {
                let _committed = committed.unwrap_committed();
            }),
        GatedOp::WriteState => txn.write_state().map(|_write_state| ()),
    }
}

/// Asserts `result` failed with the acknowledgement gate error.
fn assert_gate_error<T: std::fmt::Debug>(result: KernelResult<T>) {
    let err = result.expect_err("acknowledgement gate must reject this operation");
    let is_gate_error = matches!(err, KernelError::InvalidTransactionState(_));
    assert!(
        is_gate_error,
        "expected the acknowledgement gate error, got: {err:?}"
    );
}

/// The `(name, raw_sql)` pairs of `generated_columns`, in order.
fn discovered(generated_columns: &[GeneratedColumn]) -> Vec<(&str, &str)> {
    generated_columns
        .iter()
        .map(|column| (column.name(), column.raw_sql()))
        .collect()
}

#[tokio::test]
async fn discovers_generated_columns_from_snapshot_and_transaction(
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(
        &table_path,
        GENERATED_COLUMNS_FEATURE,
        TESTING_GENERATED_COLUMNS,
    )
    .await?;

    // Discovery keeps schema order and needs no acknowledgement.
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let snapshot_columns = snapshot.generated_columns()?;
    let from_snapshot = discovered(&snapshot_columns);
    assert_eq!(from_snapshot, TESTING_GENERATED_COLUMNS);

    let txn = begin_transaction(snapshot, engine.as_ref())?;
    let txn_columns = txn.generated_columns()?;
    let from_txn = discovered(&txn_columns);
    assert_eq!(from_txn, TESTING_GENERATED_COLUMNS);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn discovers_logical_names_of_mapped_and_partition_generated_columns(
    #[values(true, false)] column_mapping: bool,
    #[values(true, false)] partitioned: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
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
        test_schema(EVENT_YEAR),
        partition_columns,
        true,
        reader_features,
        writer_features,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let generated_columns = snapshot.generated_columns()?;
    let from_snapshot = discovered(&generated_columns);
    assert_eq!(from_snapshot, EVENT_YEAR);

    let txn = begin_transaction(snapshot, engine.as_ref())?;
    assert_gate_error(txn.write_state());
    Ok(())
}

#[rstest]
#[tokio::test]
async fn generated_column_table_write_requires_acknowledgement(
    #[values(
        GatedOp::CommitWithData,
        GatedOp::MetadataOnlyCommit,
        GatedOp::WriteState
    )]
    op: GatedOp,
    #[values(true, false)] acknowledge: bool,
    #[values(true, false)] discover_first: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, GENERATED_COLUMNS_FEATURE, EVENT_YEAR).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    // Discovery alone must not count as an acknowledgement.
    if discover_first {
        let _discovered = txn.generated_columns()?;
    }
    if acknowledge {
        txn.ack_generated_columns();
    }

    let result = run_gated_op(op, txn, engine.as_ref());
    if acknowledge {
        result?;
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[rstest]
#[case::without_feature(&[])]
#[case::with_feature(GENERATED_COLUMNS_FEATURE)]
#[tokio::test]
async fn table_without_generated_columns_needs_no_acknowledgement(
    #[case] writer_features: &[&str],
    #[values(
        GatedOp::CommitWithData,
        GatedOp::MetadataOnlyCommit,
        GatedOp::WriteState
    )]
    op: GatedOp,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, writer_features, &[]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let snapshot_columns = snapshot.generated_columns()?;
    assert!(snapshot_columns.is_empty());

    let txn = begin_transaction(snapshot, engine.as_ref())?;
    let txn_columns = txn.generated_columns()?;
    assert!(txn_columns.is_empty());
    run_gated_op(op, txn, engine.as_ref())?;
    Ok(())
}

#[tokio::test]
async fn protocol_upgrade_turns_existing_generation_expressions_into_generated_columns(
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, &[], EVENT_YEAR).await?;
    let before_upgrade = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let write_before_upgrade = begin_transaction(before_upgrade.clone(), engine.as_ref());
    let rejected = matches!(write_before_upgrade, Err(KernelError::InvalidProtocol(_)));
    assert!(
        rejected,
        "expected an invalid protocol error before the upgrade"
    );
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
    let from_incremental = discovered(&incremental_columns);
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

#[rstest]
#[tokio::test]
async fn remove_only_commit_on_generated_column_table_requires_acknowledgement(
    #[values(true, false)] acknowledge: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, GENERATED_COLUMNS_FEATURE, EVENT_YEAR).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut add_txn = begin_transaction(snapshot, engine.as_ref())?;
    add_txn.ack_generated_columns();
    stage_one_file(&mut add_txn)?;
    let snapshot = add_txn
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;
    let mut remove_txn = begin_transaction(snapshot, engine.as_ref())?;
    for scan_file in scan_files {
        remove_txn.remove_files(scan_file);
    }
    if acknowledge {
        remove_txn.ack_generated_columns();
    }

    let result = remove_txn.commit(engine.as_ref());
    if acknowledge {
        result?.unwrap_committed();
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn discovers_generated_columns_after_checkpoint(
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = write_table(&table_path, GENERATED_COLUMNS_FEATURE, EVENT_YEAR).await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref(), None)?;

    // A fresh snapshot reads Metadata from the checkpoint (via the `_last_checkpoint` hint), so
    // discovery and the ack gate must still see the generated column.
    let reloaded = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let reloaded_columns = reloaded.generated_columns()?;
    let from_reloaded = discovered(&reloaded_columns);
    assert_eq!(from_reloaded, EVENT_YEAR);

    let mut txn = begin_transaction(reloaded, engine.as_ref())?;
    stage_one_file(&mut txn)?;
    assert_gate_error(txn.commit(engine.as_ref()));
    Ok(())
}

#[tokio::test]
async fn acknowledgement_survives_later_builder_calls(
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, GENERATED_COLUMNS_FEATURE, EVENT_YEAR).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = begin_transaction(snapshot, engine.as_ref())?;
    txn.ack_generated_columns();
    let mut txn = txn
        .with_transaction_id("app_id".to_string(), 1)
        .with_operation_metrics([("numFiles", Some("1"))]);
    stage_one_file(&mut txn)?;
    txn.commit(engine.as_ref())?.unwrap_committed();
    Ok(())
}

#[rstest]
#[tokio::test]
async fn alter_table_on_generated_column_table_requires_acknowledgement(
    #[values(true, false)] acknowledge: bool,
) -> KernelResult<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(
        &table_path,
        GENERATED_COLUMNS_FEATURE,
        TESTING_GENERATED_COLUMNS,
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = begin_transaction_with(
        snapshot,
        engine.as_ref(),
        UpdateTableOperation::AlterTable,
        "Test/1.0",
        false,
        |builder| builder.add_column(StructField::nullable("note", DataType::STRING)),
    )?;
    if !acknowledge {
        assert_gate_error(txn.commit(engine.as_ref()));
        return Ok(());
    }

    txn.ack_generated_columns();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let altered_columns = altered.generated_columns()?;
    let from_altered = discovered(&altered_columns);
    assert_eq!(from_altered, TESTING_GENERATED_COLUMNS);

    // The ALTER commit rewrites the schema, so a fresh load reads the expressions kernel wrote.
    let reloaded = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let reloaded_columns = reloaded.generated_columns()?;
    let from_reloaded = discovered(&reloaded_columns);
    assert_eq!(from_reloaded, TESTING_GENERATED_COLUMNS);
    Ok(())
}
