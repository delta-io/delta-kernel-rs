//! Integration tests for the `checkConstraints` writer feature: discovery via the
//! [`TableWriteExpressions`] trait, the separate `ack_check_constraints` acknowledgement, and the
//! commit-time gate.

use delta_kernel::check_constraints::TableWriteExpressions;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{schema_ref, DataType, SchemaRef, StructField};
use delta_kernel::transaction::create_table::{
    create_table as kernel_create_table, CreateTableTransaction,
};
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Engine, Error, Snapshot};
use rstest::rstest;
use serde_json::json;
use test_utils::{
    add_commit, create_add_files_metadata, engine_store_setup, read_actions_from_commit,
    test_table_setup, test_table_setup_mt,
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

/// Writes version 0 of an in-memory table by hand, listing `writer_features` in the protocol and
/// declaring `constraints`, for combinations CREATE TABLE cannot produce.
async fn write_table_with_protocol(
    table_name: &str,
    writer_features: &[&str],
    constraints: &[(&str, &str)],
) -> Result<(Url, impl Engine), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup(table_name, None);
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
        store.as_ref(),
        0,
        format!("{protocol}\n{metadata}\n"),
    )
    .await?;
    Ok((table_url, engine))
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

#[tokio::test]
async fn acknowledgement_survives_later_builder_calls() -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

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
async fn data_commit_needs_no_acknowledgement_when_feature_listed_without_constraints(
) -> Result<(), Box<dyn std::error::Error>> {
    let (table_url, engine) =
        write_table_with_protocol("feature_without_constraints", &["checkConstraints"], &[])
            .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let discovered = snapshot.check_constraints();
    assert!(discovered.is_empty());

    let mut txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
    stage_one_file(&mut txn)?;
    txn.commit(&engine)?.unwrap_committed();
    Ok(())
}

#[tokio::test]
async fn table_with_constraints_but_without_feature_is_readable_but_not_writable(
) -> Result<(), Box<dyn std::error::Error>> {
    let (table_url, engine) = write_table_with_protocol(
        "constraints_without_feature",
        &[],
        &[("positive_amount", "amount > 0")],
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let discovered: Vec<_> = snapshot
        .check_constraints()
        .iter()
        .map(|c| c.raw_sql().to_string())
        .collect();
    assert_eq!(discovered, ["amount > 0".to_string()]);

    let result = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine);
    let rejected_as_malformed = matches!(result, Err(Error::InvalidProtocol(_)));
    assert!(rejected_as_malformed);
    Ok(())
}

#[tokio::test]
async fn create_table_rejects_constraint_with_empty_name() -> Result<(), Box<dyn std::error::Error>>
{
    let (_tmp, table_path, engine) = test_table_setup()?;
    let result = build_create_txn(engine.as_ref(), &table_path, &[("", "amount > 0")]);
    let rejected = matches!(result, Err(Error::Generic(_)));
    assert!(rejected);
    Ok(())
}

#[tokio::test]
async fn alter_table_on_constrained_table_needs_no_acknowledgement(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let altered = snapshot
        .alter_table()
        .add_column(StructField::nullable("note", DataType::STRING))
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let discovered: Vec<_> = altered
        .check_constraints()
        .iter()
        .map(|c| c.raw_sql().to_string())
        .collect();
    assert_eq!(discovered, ["amount > 0".to_string()]);
    Ok(())
}

#[rstest]
#[case::acknowledged(true)]
#[case::not_acknowledged(false)]
#[tokio::test]
async fn alter_table_add_constraint_requires_acknowledgement(
    #[case] acknowledge: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(engine.as_ref(), &table_path, &[])?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
    let mut txn = snapshot
        .alter_table()
        .add_check_constraint("Positive_Amount", "amount > 0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if acknowledge {
        txn.ack_check_constraints();
    }
    let result = txn.commit(engine.as_ref());
    if !acknowledge {
        assert_gate_error(result);
        return Ok(());
    }

    let altered = result?.unwrap_post_commit_snapshot();
    let discovered: Vec<_> = altered
        .check_constraints()
        .iter()
        .map(|c| (c.name().to_string(), c.raw_sql().to_string()))
        .collect();
    let expected = [("positive_amount".to_string(), "amount > 0".to_string())];
    assert_eq!(discovered, expected);

    let protocol = read_actions_from_commit(&table_url, 1, "protocol")?;
    let writer_features = protocol[0]["writerFeatures"]
        .as_array()
        .expect("writer v7 protocol must list writer features");
    let lists_check_constraints = writer_features
        .iter()
        .any(|f| f.as_str() == Some("checkConstraints"));
    assert!(lists_check_constraints);
    Ok(())
}

#[tokio::test]
async fn alter_table_adds_column_and_constraint_in_one_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(engine.as_ref(), &table_path, &[])?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot
        .alter_table()
        .add_column(StructField::nullable("discount", DataType::LONG))
        .add_check_constraint("nonnegative_discount", "discount >= 0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    txn.ack_check_constraints();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

    let has_column = altered.schema().contains("discount");
    assert!(has_column);
    let discovered: Vec<_> = altered
        .check_constraints()
        .iter()
        .map(|c| c.raw_sql().to_string())
        .collect();
    assert_eq!(discovered, ["discount >= 0".to_string()]);
    Ok(())
}

#[rstest]
#[case::duplicate_name_in_other_case("POSITIVE_AMOUNT", "amount > 1")]
#[case::empty_name("", "amount > 0")]
#[case::empty_expression("nonempty_name", "  ")]
#[tokio::test]
async fn alter_table_rejects_invalid_constraint_addition(
    #[case] name: &str,
    #[case] raw_sql: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let result = snapshot
        .alter_table()
        .add_check_constraint(name, raw_sql)
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));
    let rejected = matches!(result, Err(Error::Generic(_)));
    assert!(rejected);
    Ok(())
}

#[rstest]
#[case::same_case("positive_amount")]
#[case::other_case("POSITIVE_AMOUNT")]
#[tokio::test]
async fn alter_table_drops_constraint_without_acknowledgement(
    #[case] name: &str,
    #[values(false, true)] if_exists: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let builder = snapshot.alter_table();
    let builder = if if_exists {
        builder.drop_check_constraint_if_exists(name)
    } else {
        builder.drop_check_constraint(name)
    };
    let altered = builder
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();

    let remaining = altered.check_constraints();
    assert!(remaining.is_empty());
    Ok(())
}

#[rstest]
#[case::rejected_without_if_exists(false)]
#[case::skipped_with_if_exists(true)]
#[tokio::test]
async fn alter_table_drop_of_missing_constraint(
    #[case] if_exists: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(
        engine.as_ref(),
        &table_path,
        &[("positive_amount", "amount > 0")],
    )?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let builder = snapshot.alter_table();
    let result = if if_exists {
        builder.drop_check_constraint_if_exists("missing")
    } else {
        builder.drop_check_constraint("missing")
    }
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    if !if_exists {
        let rejected = matches!(result, Err(Error::Generic(_)));
        assert!(rejected);
        return Ok(());
    }
    let altered = result?
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    let discovered: Vec<_> = altered
        .check_constraints()
        .iter()
        .map(|c| c.raw_sql().to_string())
        .collect();
    assert_eq!(discovered, ["amount > 0".to_string()]);
    Ok(())
}
