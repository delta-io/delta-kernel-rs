//! Integration tests for the `checkConstraints` writer feature: constraint discovery via
//! [`TableWriteExpressions`] and the `ack_check_constraints` gate on commits and `write_state`.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{schema_ref, DataType, SchemaRef, StructField};
use delta_kernel::transaction::Transaction;
use delta_kernel::write_expressions::TableWriteExpressions;
use delta_kernel::{KernelError, Result, Snapshot};
use rstest::rstest;
use serde_json::json;
use test_utils::{add_commit, create_add_files_metadata, test_table_setup, test_table_setup_mt};
use url::Url;

use crate::common::write_utils::get_scan_files;

const CHECK_CONSTRAINTS_FEATURE: &[&str] = &["checkConstraints"];
const POSITIVE_AMOUNT: &[(&str, &str)] = &[("positive_amount", "amount > 0")];

/// The operation whose CHECK-constraint acknowledgement gate is under test.
#[derive(Clone, Copy)]
enum GatedOp {
    CommitWithData,
    MetadataOnlyCommit,
    WriteState,
}

fn test_schema() -> SchemaRef {
    schema_ref! {
        nullable "amount": LONG,
        nullable "name": STRING,
    }
}

fn stage_one_file(txn: &mut Transaction) -> Result<(), Box<dyn std::error::Error>> {
    let add = create_add_files_metadata(
        txn.add_files_schema(),
        vec![("part-00000.parquet", 1024, 1_000_000, Some(1))],
    )?;
    txn.add_files(add);
    Ok(())
}

/// Asserts `result` failed with the acknowledgement gate error.
fn assert_gate_error<T: std::fmt::Debug>(result: Result<T>) {
    let err = result.expect_err("acknowledgement gate must reject this operation");
    let is_gate_error = matches!(err, KernelError::InvalidTransactionState(_));
    assert!(
        is_gate_error,
        "expected the acknowledgement gate error, got: {err:?}"
    );
}

/// The `(name, raw_sql)` pairs `source` discovers, in the order it returns them.
fn discovered(source: &impl TableWriteExpressions) -> Vec<(&str, &str)> {
    source
        .check_constraints()
        .iter()
        .map(|constraint| (constraint.name(), constraint.raw_sql()))
        .collect()
}

/// Writes version 0 of the table at `table_path` by hand: a table-features protocol listing
/// `writer_features`, and metadata declaring `constraints` as `delta.constraints.<name>` keys. This
/// builds a table in any constraint and feature state without going through CREATE TABLE.
async fn write_table(
    table_path: &str,
    writer_features: &[&str],
    constraints: &[(&str, &str)],
) -> Result<Url, Box<dyn std::error::Error>> {
    let table_url = Url::from_directory_path(table_path).expect("table path must be a URL");
    let configuration: serde_json::Map<String, serde_json::Value> = constraints
        .iter()
        .map(|(name, sql)| (format!("delta.constraints.{name}"), json!(sql)))
        .collect();
    let protocol = json!({"protocol": {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": [],
        "writerFeatures": writer_features,
    }});
    let metadata = json!({"metaData": {
        "id": "test-id",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": serde_json::to_string(test_schema().as_ref())?,
        "partitionColumns": [],
        "configuration": configuration,
        "createdTime": 1_700_000_000_000_i64,
    }});
    add_commit(
        table_url.as_str(),
        &LocalFileSystem::new(),
        0,
        format!("{protocol}\n{metadata}\n"),
    )
    .await?;
    Ok(table_url)
}

#[tokio::test]
async fn discovers_constraints_from_snapshot_and_transaction(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let constraints = [
        ("positive_amount", "amount > 0"),
        ("NonEmptyName", "name != ''"),
    ];
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, &constraints).await?;

    // Discovery keeps the stored name case, sorts by name, and needs no acknowledgement.
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let from_snapshot = discovered(snapshot.as_ref());
    let expected = [
        ("NonEmptyName", "name != ''"),
        ("positive_amount", "amount > 0"),
    ];
    assert_eq!(from_snapshot, expected);

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let from_txn = discovered(&txn);
    assert_eq!(from_txn, expected);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn constrained_table_write_requires_acknowledgement(
    #[values(
        GatedOp::CommitWithData,
        GatedOp::MetadataOnlyCommit,
        GatedOp::WriteState
    )]
    op: GatedOp,
    #[values(true, false)] acknowledge: bool,
    #[values(true, false)] discover_first: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    // Discovery alone must not count as an acknowledgement.
    if discover_first {
        let _discovered = txn.check_constraints();
    }
    if acknowledge {
        txn.ack_check_constraints();
    }

    let result = match op {
        GatedOp::CommitWithData => {
            stage_one_file(&mut txn)?;
            txn.commit(engine.as_ref()).map(|committed| {
                let _committed = committed.unwrap_committed();
            })
        }
        GatedOp::MetadataOnlyCommit => txn
            .with_transaction_id("app_id".to_string(), 1)
            .commit(engine.as_ref())
            .map(|committed| {
                let _committed = committed.unwrap_committed();
            }),
        GatedOp::WriteState => txn.write_state().map(|_write_state| ()),
    };
    if acknowledge {
        result?;
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn data_commit_to_table_without_constraints_needs_no_acknowledgement(
    #[values(&[][..], CHECK_CONSTRAINTS_FEATURE)] writer_features: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, writer_features, &[]).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let from_snapshot = discovered(snapshot.as_ref());
    assert!(from_snapshot.is_empty());

    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    stage_one_file(&mut txn)?;
    txn.commit(engine.as_ref())?.unwrap_committed();
    Ok(())
}

#[rstest]
#[tokio::test]
async fn remove_only_commit_on_constrained_table_requires_acknowledgement(
    #[values(true, false)] acknowledge: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut add_txn =
        snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    add_txn.ack_check_constraints();
    stage_one_file(&mut add_txn)?;
    let snapshot = add_txn
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;
    let mut remove_txn =
        snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    for scan_file in scan_files {
        remove_txn.remove_files(scan_file);
    }
    if acknowledge {
        remove_txn.ack_check_constraints();
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
async fn discovers_constraints_after_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref(), None)?;

    // A fresh snapshot reads Metadata from the checkpoint (via the `_last_checkpoint` hint), so
    // discovery and the ack gate must still see the constraint.
    let reloaded = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let from_reloaded = discovered(reloaded.as_ref());
    assert_eq!(from_reloaded, POSITIVE_AMOUNT);

    let mut txn = reloaded.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    stage_one_file(&mut txn)?;
    assert_gate_error(txn.commit(engine.as_ref()));
    Ok(())
}

#[tokio::test]
async fn acknowledgement_survives_later_builder_calls() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    txn.ack_check_constraints();
    let mut txn = txn
        .with_operation("WRITE".to_string())
        .with_engine_info("Test/1.0");
    stage_one_file(&mut txn)?;
    txn.commit(engine.as_ref())?.unwrap_committed();
    Ok(())
}

#[tokio::test]
async fn table_with_constraints_but_without_feature_is_readable_but_not_writable(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, &[], POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let from_snapshot = discovered(snapshot.as_ref());
    assert_eq!(from_snapshot, POSITIVE_AMOUNT);

    let result = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref());
    let rejected_as_malformed = matches!(result, Err(KernelError::InvalidProtocol(_)));
    assert!(rejected_as_malformed);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn alter_table_on_constrained_table_requires_acknowledgement(
    #[values(true, false)] acknowledge: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot
        .alter_table()
        .add_column(StructField::nullable("note", DataType::STRING))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if !acknowledge {
        assert_gate_error(txn.commit(engine.as_ref()));
        return Ok(());
    }

    txn.ack_check_constraints();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let from_altered = discovered(altered.as_ref());
    assert_eq!(from_altered, POSITIVE_AMOUNT);
    Ok(())
}
