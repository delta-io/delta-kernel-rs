//! Integration tests for the `checkConstraints` writer feature: constraint discovery via
//! [`TableWriteExpressions`] and the `ack_check_constraints` gate on commits and `write_state`.

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{schema_ref, DataType, SchemaRef, StructField};
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::{
    create_table as kernel_create_table, CreateTableTransaction,
};
use delta_kernel::transaction::Transaction;
use delta_kernel::write_expressions::TableWriteExpressions;
use delta_kernel::{Engine, KernelError, Result, Snapshot};
use rstest::rstest;
use serde_json::json;
use test_utils::{
    add_commit, create_add_files_metadata, create_table as create_test_table, engine_store_setup,
    read_actions_from_commit, test_table_setup, test_table_setup_mt,
};
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

/// Builds (but does not commit) a create-table transaction declaring `constraints`.
fn build_create_txn(
    engine: &dyn Engine,
    table_path: &str,
    constraints: &[(&str, &str)],
) -> Result<CreateTableTransaction> {
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
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let mut create_txn = build_create_txn(engine.as_ref(), &table_path, declared)?;

    // The uncommitted create-table transaction already discovers the declared constraints.
    let from_create_txn = discovered(&create_txn);
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
    let from_snapshot = discovered(snapshot.as_ref());
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
async fn creating_a_constrained_table_requires_acknowledgement(
    #[values(GatedOp::MetadataOnlyCommit, GatedOp::WriteState)] op: GatedOp,
    #[values(true, false)] acknowledge: bool,
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

    let result = match op {
        GatedOp::MetadataOnlyCommit => txn.commit(engine.as_ref()).map(|committed| {
            let _committed = committed.unwrap_committed();
        }),
        GatedOp::WriteState => txn.write_state().map(|_write_state| ()),
        GatedOp::CommitWithData => unreachable!("not a case of this test"),
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
    #[values(false, true)] drops_one_of_two_constraints: bool,
    #[values(true, false)] acknowledge: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let constraints = [
        ("nonempty_name", "name != ''"),
        ("positive_amount", "amount > 0"),
    ];
    let table_url = write_table(&table_path, CHECK_CONSTRAINTS_FEATURE, &constraints).await?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let builder = snapshot.alter_table();
    let builder = if drops_one_of_two_constraints {
        builder.drop_check_constraint("nonempty_name")
    } else {
        builder.add_column(StructField::nullable("note", DataType::STRING))
    };
    let mut txn = builder.build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if !acknowledge {
        assert_gate_error(txn.commit(engine.as_ref()));
        return Ok(());
    }

    txn.ack_check_constraints();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let from_altered = discovered(altered.as_ref());
    let expected = if drops_one_of_two_constraints {
        &constraints[1..]
    } else {
        &constraints[..]
    };
    assert_eq!(from_altered, expected);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn alter_table_add_constraint_requires_acknowledgement(
    #[values(true, false)] acknowledge: bool,
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
    let from_altered = discovered(altered.as_ref());
    let expected = [("positive_amount", "amount > 0")];
    assert_eq!(from_altered, expected);

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
    let from_altered = discovered(altered.as_ref());
    let expected = [("nonnegative_discount", "discount >= 0")];
    assert_eq!(from_altered, expected);
    Ok(())
}

#[tokio::test]
async fn alter_table_applies_constraint_operations_in_order(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_tmp, table_path, engine) = test_table_setup()?;
    let table_url = create_constrained_table(engine.as_ref(), &table_path, &[])?;

    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    let mut txn = snapshot
        .alter_table()
        .add_check_constraint("positive_amount", "amount > 0")
        .add_check_constraint("named", "name IS NOT NULL")
        .drop_check_constraint("POSITIVE_AMOUNT")
        .add_check_constraint("positive_amount", "amount > 1")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    txn.ack_check_constraints();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();

    let from_altered = discovered(altered.as_ref());
    let expected = [
        ("named", "name IS NOT NULL"),
        ("positive_amount", "amount > 1"),
    ];
    assert_eq!(from_altered, expected);
    Ok(())
}

#[tokio::test]
async fn alter_table_add_constraint_upgrades_legacy_protocol(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, engine, table_url) = engine_store_setup("legacy_protocol", None);
    let table_url =
        create_test_table(store, table_url, test_schema(), &[], false, vec![], vec![]).await?;

    let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
    let mut txn = snapshot
        .alter_table()
        .add_check_constraint("positive_amount", "amount > 0")
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    txn.ack_check_constraints();
    txn.commit(&engine)?.unwrap_committed();

    let altered = Snapshot::builder_for(table_url).build(&engine)?;
    let protocol = altered.table_configuration().protocol();
    let versions = (protocol.min_reader_version(), protocol.min_writer_version());
    assert_eq!(versions, (1, 3));
    let writer_features = protocol.writer_features();
    assert_eq!(writer_features, None);
    let from_altered = discovered(altered.as_ref());
    let expected = [("positive_amount", "amount > 0")];
    assert_eq!(from_altered, expected);
    Ok(())
}

#[rstest]
#[case::duplicate_name_in_other_case("POSITIVE_AMOUNT", "amount > 1")]
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
    let rejected = matches!(result, Err(KernelError::Generic(_)));
    assert!(rejected);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn alter_table_drops_last_constraint_without_acknowledgement_and_keeps_feature(
    #[values("positive_amount", "POSITIVE_AMOUNT")] name: &str,
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
    let keeps_feature = altered
        .table_configuration()
        .is_feature_supported(&TableFeature::CheckConstraints);
    assert!(keeps_feature);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn alter_table_drop_of_missing_constraint(
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
    let result = if if_exists {
        builder.drop_check_constraint_if_exists("missing")
    } else {
        builder.drop_check_constraint("missing")
    }
    .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    if !if_exists {
        let rejected = matches!(result, Err(KernelError::Generic(_)));
        assert!(rejected);
        return Ok(());
    }
    let mut txn = result?;
    txn.ack_check_constraints();
    let altered = txn.commit(engine.as_ref())?.unwrap_post_commit_snapshot();
    let from_altered = discovered(altered.as_ref());
    let expected = [("positive_amount", "amount > 0")];
    assert_eq!(from_altered, expected);
    Ok(())
}
