//! Integration tests for the `checkConstraints` writer feature: discovery via the
//! [`TableWriteExpressions`] trait, the separate `ack_check_constraints` acknowledgement, and the
//! commit-time gate.

use delta_kernel::check_constraints::TableWriteExpressions;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{schema_ref, SchemaRef};
use delta_kernel::transaction::create_table::{
    create_table as kernel_create_table, CreateTableTransaction,
};
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
use rstest::rstest;
use test_utils::{
    create_add_files_metadata, read_actions_from_commit, test_table_setup, test_table_setup_mt,
};
use url::Url;

use crate::common::write_utils::get_scan_files;

/// The operation whose CHECK-constraint acknowledgement gate is under test.
#[derive(Clone, Copy)]
enum GatedOp {
    CommitWithData,
    WriteState,
}

fn test_schema() -> SchemaRef {
    schema_ref! {
        nullable "amount": LONG,
        nullable "name": STRING,
    }
}

/// Builds (but does not commit) a create-table transaction declaring `constraints`.
fn build_create_txn(
    engine: &dyn Engine,
    table_path: &str,
    constraints: &[(&str, &str)],
) -> DeltaResult<CreateTableTransaction> {
    let properties = constraints
        .iter()
        .map(|(name, sql)| (format!("delta.constraints.{name}"), sql.to_string()));
    kernel_create_table(table_path, test_schema(), "Test/1.0")
        .with_table_properties(properties)
        .build(engine, Box::new(FileSystemCommitter::new()))
}

/// Creates a table with the given constraints (possibly none), acknowledging them on the create
/// commit, and returns its URL.
fn create_constrained_table(
    engine: &dyn Engine,
    table_path: &str,
    constraints: &[(&str, &str)],
) -> Result<Url, Box<dyn std::error::Error>> {
    let mut txn = build_create_txn(engine, table_path, constraints)?;
    txn.ack_check_constraints();
    txn.commit(engine)?.unwrap_committed();
    Ok(Url::from_directory_path(table_path).expect("table path must be a URL"))
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
fn assert_gate_error<T: std::fmt::Debug>(result: DeltaResult<T>) {
    let err = result.expect_err("acknowledgement gate must reject this operation");
    let is_gate_error = matches!(err, Error::InvalidTransactionState(_));
    assert!(
        is_gate_error,
        "expected the acknowledgement gate error, got: {err:?}"
    );
}

#[rstest]
#[case::single(&[("positive_amount", "amount > 0")])]
#[case::multiple(&[("positive_amount", "amount > 0"), ("nonempty_name", "name != ''")])]
#[tokio::test]
async fn discovers_constraints_and_auto_enables_feature_on_create(
    #[case] constraints: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(engine.as_ref(), &table_path, constraints)?;

    // CREATE TABLE auto-enabled the writer feature from the declared constraints.
    let protocol = read_actions_from_commit(&table_url, 0, "protocol")?;
    let writer_features = protocol[0]["writerFeatures"]
        .as_array()
        .expect("writer v7 protocol must list writer features");
    let lists_check_constraints = writer_features
        .iter()
        .any(|f| f.as_str() == Some("checkConstraints"));
    assert!(lists_check_constraints);

    let mut expected: Vec<_> = constraints
        .iter()
        .map(|(name, sql)| (name.to_string(), sql.to_string()))
        .collect();
    expected.sort();

    // Discovery works from both a snapshot and a transaction, with no ack.
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut from_snapshot: Vec<_> = snapshot
        .check_constraints()
        .iter()
        .map(|c| (c.name().to_string(), c.raw_sql().to_string()))
        .collect();
    from_snapshot.sort();
    assert_eq!(from_snapshot, expected);

    let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    let mut from_txn: Vec<_> = txn
        .check_constraints()
        .iter()
        .map(|c| (c.name().to_string(), c.raw_sql().to_string()))
        .collect();
    from_txn.sort();
    assert_eq!(from_txn, expected);
    Ok(())
}

/// A data-adding commit and `write_state` both gate on acknowledgement on a constrained table. The
/// `commit_discovery_is_not_acknowledgement` case first calls `check_constraints()` to prove
/// discovery is read-only and does not arm the gate.
#[rstest]
#[case::commit_acknowledged(GatedOp::CommitWithData, true, false)]
#[case::commit_not_acknowledged(GatedOp::CommitWithData, false, false)]
#[case::commit_discovery_is_not_acknowledgement(GatedOp::CommitWithData, false, true)]
#[case::write_state_acknowledged(GatedOp::WriteState, true, false)]
#[case::write_state_not_acknowledged(GatedOp::WriteState, false, false)]
#[tokio::test]
async fn constrained_table_write_requires_acknowledgement(
    #[case] op: GatedOp,
    #[case] acknowledge: bool,
    #[case] discover_first: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    if discover_first {
        txn.check_constraints();
    }
    if acknowledge {
        txn.ack_check_constraints();
    }

    match op {
        GatedOp::CommitWithData => {
            stage_one_file(&mut txn)?;
            let result = txn.commit(engine.as_ref());
            if acknowledge {
                result?.unwrap_committed();
            } else {
                assert_gate_error(result);
            }
        }
        GatedOp::WriteState => {
            let result = txn.write_state();
            if acknowledge {
                result?;
            } else {
                assert_gate_error(result);
            }
        }
    }
    Ok(())
}

#[rstest]
#[case::acknowledged(true)]
#[case::not_acknowledged(false)]
#[tokio::test]
async fn creating_a_constrained_table_requires_acknowledgement(
    #[case] acknowledge: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let mut txn = build_create_txn(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;
    if acknowledge {
        txn.ack_check_constraints();
    }

    let result = txn.commit(engine.as_ref());
    if acknowledge {
        result?.unwrap_committed();
    } else {
        assert_gate_error(result);
    }
    Ok(())
}

#[rstest]
#[case::metadata_only_on_constrained_table(&[("positive_amount", "amount > 0")], false)]
#[case::data_append_to_unconstrained_table(&[], true)]
#[tokio::test]
async fn commit_needs_no_acknowledgement_when_no_constraints_apply(
    #[case] constraints: &[(&str, &str)],
    #[case] add_data: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(engine.as_ref(), &table_path, constraints)?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    if add_data {
        stage_one_file(&mut txn)?;
    } else {
        txn = txn.with_transaction_id("app_id".to_string(), 1);
    }
    txn.commit(engine.as_ref())?.unwrap_committed();
    Ok(())
}

#[tokio::test]
async fn remove_only_commit_needs_no_acknowledgement_on_constrained_table(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    // Add a data file (acknowledged), then remove it in a fresh transaction without acknowledging:
    // a remove-only commit deletes rows rather than adding them, so the gate does not fire.
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
    remove_txn.commit(engine.as_ref())?.unwrap_committed();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn discovers_constraints_after_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup_mt()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    snapshot.checkpoint(engine.as_ref(), None)?;

    // A fresh snapshot reads Metadata from the checkpoint (via the `_last_checkpoint` hint), so
    // discovery and the ack gate must still see the constraint.
    let reloaded = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let discovered: Vec<_> = reloaded
        .check_constraints()
        .iter()
        .map(|c| (c.name().to_string(), c.raw_sql().to_string()))
        .collect();
    let expected = [("positive_amount".to_string(), "amount > 0".to_string())];
    assert_eq!(discovered, expected);

    let mut txn = reloaded.transaction(Box::new(FileSystemCommitter::new()), engine.as_ref())?;
    stage_one_file(&mut txn)?;
    assert_gate_error(txn.commit(engine.as_ref()));
    Ok(())
}
