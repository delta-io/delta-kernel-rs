//! Integration tests for the `checkConstraints` writer feature: constraint discovery on `Snapshot`
//! and `Transaction`, and the `ack_check_constraints` gate on commits and `write_state`.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{schema_ref, DataType, SchemaRef, StructField};
use delta_kernel::table_properties::CheckConstraint;
use delta_kernel::transaction::create_table::{
    create_table as kernel_create_table, CreateTableTransaction,
};
use delta_kernel::transaction::{Transaction, UpdateTableOperation};
use delta_kernel::{Engine, KernelError, KernelResult, Snapshot, SnapshotRef};
use rstest::rstest;
use serde_json::json;
use test_utils::{
    add_commit, create_add_files_metadata, read_actions_from_commit, test_table_setup,
    test_table_setup_mt,
};
use url::Url;

use crate::common::write_utils::get_scan_files;

const CHECK_CONSTRAINTS_FEATURE: &[&str] = &["checkConstraints"];
const POSITIVE_AMOUNT: &[(&str, &str)] = &[("positive_amount", "amount > 0")];

/// The operation whose CHECK-constraint acknowledgement gate is under test.
#[derive(Clone, Copy, PartialEq)]
enum GatedOp {
    CommitWithData,
    RemoveOnlyCommit,
    MetadataOnlyCommit,
    WriteState,
}

fn test_schema() -> SchemaRef {
    schema_ref! {
        nullable "amount": LONG,
        nullable "name": STRING,
    }
}

/// Builds a data-changing write transaction on `snapshot`.
fn build_write_txn(snapshot: &SnapshotRef, engine: &dyn Engine) -> KernelResult<Transaction> {
    snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "Test/1.0", true)
        .build(engine, Box::new(FileSystemCommitter::new()))
}

/// Builds (but does not commit) a create-table transaction declaring `constraints`.
fn build_create_txn(
    engine: &dyn Engine,
    table_path: &str,
    constraints: &[(&str, &str)],
) -> KernelResult<CreateTableTransaction> {
    let properties = constraints
        .iter()
        .map(|(name, sql)| (format!("delta.constraints.{name}"), sql.to_string()));
    kernel_create_table(table_path, test_schema(), "Test/1.0", true)
        .with_table_properties(properties)
        .build(engine, Box::new(FileSystemCommitter::new()))
}

/// Creates a table with the given constraints (possibly none), acknowledging them on the create
/// commit, and returns its URL.
fn create_constrained_table(
    engine: &dyn Engine,
    table_path: &str,
    constraints: &[(&str, &str)],
) -> KernelResult<Url, Box<dyn std::error::Error>> {
    let mut txn = build_create_txn(engine, table_path, constraints)?;
    txn.ack_check_constraints();
    txn.commit(engine)?.unwrap_committed();
    Ok(Url::from_directory_path(table_path).expect("table path must be a URL"))
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
        KernelError::InvalidTransactionState(msg) if msg.contains("ack_check_constraints")
    );
    assert!(
        is_gate_error,
        "expected the acknowledgement gate error, got: {err:?}"
    );
}

/// The `(name, raw_sql)` pairs of `constraints`, in order.
fn constraint_pairs(constraints: &[CheckConstraint]) -> Vec<(&str, &str)> {
    constraints
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
) -> KernelResult<Url> {
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

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_and_transaction_return_stored_constraints_in_name_order(
    #[values(false, true)] checkpoint: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let constraints = [
        ("NonEmptyName", "name != ''"),
        ("amount_below_limit", "amount < 1000"),
        ("positive_amount", "amount > 0"),
        ("short_name", "length(name) < 100"),
    ];
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, &constraints).await?;

    let mut snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    if checkpoint {
        snapshot.checkpoint(engine.as_ref(), None)?;
        // A reloaded snapshot reads Metadata from the checkpoint instead of the commit.
        snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    }
    let from_snapshot = constraint_pairs(snapshot.check_constraints());
    assert_eq!(from_snapshot, constraints);

    let mut txn = build_write_txn(&snapshot, engine.as_ref())?;
    let from_txn = constraint_pairs(txn.check_constraints());
    assert_eq!(from_txn, constraints);

    stage_one_file(&mut txn)?;
    assert_gate_error(txn.commit(engine.as_ref()));
    Ok(())
}

#[rstest]
#[case::single(
    &[("positive_amount", "amount > 0")],
    &[("positive_amount", "amount > 0")]
)]
#[case::multiple_with_mixed_case_name(
    &[("PositiveAmount", "amount > 0"), ("nonempty_name", "name != ''")],
    &[("nonempty_name", "name != ''"), ("positiveamount", "amount > 0")]
)]
#[tokio::test]
async fn discovers_constraints_and_auto_enables_feature_on_create(
    #[case] declared: &[(&str, &str)],
    #[case] expected: &[(&str, &str)],
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let mut create_txn = build_create_txn(engine.as_ref(), &table_path, declared)?;

    // The uncommitted create-table transaction already discovers the declared constraints.
    let from_create_txn = constraint_pairs(create_txn.check_constraints());
    assert_eq!(from_create_txn, expected);

    create_txn.ack_check_constraints();
    create_txn.commit(engine.as_ref())?.unwrap_committed();
    let table_url = Url::from_directory_path(&table_path).expect("table path must be a URL");

    // CREATE TABLE auto-enabled the writer feature from the declared constraints.
    let protocol = read_actions_from_commit(&table_url, 0, "protocol")?;
    let writer_features = protocol[0]["writerFeatures"]
        .as_array()
        .expect("writer v7 protocol must list writer features");
    let lists_check_constraints = writer_features
        .iter()
        .any(|f| f.as_str() == Some("checkConstraints"));
    assert!(lists_check_constraints);

    // Discovery works from both a snapshot and a transaction, with no ack.
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let from_snapshot = constraint_pairs(snapshot.check_constraints());
    assert_eq!(from_snapshot, expected);

    let txn = build_write_txn(&snapshot, engine.as_ref())?;
    let from_txn = constraint_pairs(txn.check_constraints());
    assert_eq!(from_txn, expected);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn write_state_and_commits_require_ack_only_on_tables_with_constraints(
    #[values(
        GatedOp::CommitWithData,
        GatedOp::RemoveOnlyCommit,
        GatedOp::MetadataOnlyCommit,
        GatedOp::WriteState
    )]
    op: GatedOp,
    #[values(true, false)] declares_constraints: bool,
    #[values(true, false)] acknowledge: bool,
    #[values(true, false)] discover_first: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let constraints = if declares_constraints {
        POSITIVE_AMOUNT
    } else {
        &[]
    };
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, constraints).await?;

    let mut snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    if op == GatedOp::RemoveOnlyCommit {
        let mut seed_txn = build_write_txn(&snapshot, engine.as_ref())?;
        seed_txn.ack_check_constraints();
        stage_one_file(&mut seed_txn)?;
        snapshot = seed_txn
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
    }

    let mut txn = build_write_txn(&snapshot, engine.as_ref())?;
    // Discovery alone must not count as an acknowledgement.
    if discover_first {
        let _constraints = txn.check_constraints();
    }
    if acknowledge {
        txn.ack_check_constraints();
    }
    let mut txn = txn
        .with_transaction_id("app_id".to_string(), 1)
        .with_operation_metrics([("numFiles", Some("1"))]);

    match op {
        GatedOp::CommitWithData => stage_one_file(&mut txn)?,
        GatedOp::RemoveOnlyCommit => {
            let scan_files = get_scan_files(snapshot.clone(), engine.as_ref())?;
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
    if acknowledge || !declares_constraints {
        result?;
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn creating_a_constrained_table_requires_acknowledgement(
    #[values(GatedOp::MetadataOnlyCommit, GatedOp::WriteState)] op: GatedOp,
    #[values(true, false)] acknowledge: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let mut txn = build_create_txn(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;
    if acknowledge {
        txn.ack_check_constraints();
    }

    let result = match op {
        GatedOp::MetadataOnlyCommit => txn.commit(engine.as_ref()).map(|committed| {
            let _committed = committed.unwrap_committed();
        }),
        GatedOp::WriteState => txn.write_state().map(|_write_state| ()),
        GatedOp::CommitWithData | GatedOp::RemoveOnlyCommit => {
            unreachable!("not a case of this test")
        }
    };
    if acknowledge {
        result?;
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[tokio::test]
async fn constraints_without_feature_are_discoverable_but_txn_build_fails() -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, &[], POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let from_snapshot = constraint_pairs(snapshot.check_constraints());
    assert_eq!(from_snapshot, POSITIVE_AMOUNT);

    let result = build_write_txn(&snapshot, engine.as_ref());
    let rejected_as_malformed = matches!(result, Err(KernelError::InvalidProtocol(_)));
    assert!(rejected_as_malformed);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn alter_table_commit_succeeds_only_with_ack_and_keeps_constraints(
    #[values(true, false)] acknowledge: bool,
) -> KernelResult<()> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, POSITIVE_AMOUNT).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot
        .update_table(UpdateTableOperation::AlterTable, "Test/1.0", false)
        .add_column(StructField::nullable("note", DataType::STRING))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if !acknowledge {
        assert_gate_error(txn.commit(engine.as_ref()));
        return Ok(());
    }

    txn.ack_check_constraints();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let from_altered = constraint_pairs(altered.check_constraints());
    assert_eq!(from_altered, POSITIVE_AMOUNT);
    Ok(())
}
