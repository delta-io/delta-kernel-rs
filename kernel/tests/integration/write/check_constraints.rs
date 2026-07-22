//! Integration tests for the `checkConstraints` writer feature: discovery, acknowledgment, and
//! the commit-time gate (in development, gated by the `check-constraints-in-dev` cargo feature).
//!
//! Enforcement (running a validator over each batch) lands in later work; these tests cover the
//! contract that guards a constrained table -- a data-adding commit must acknowledge the
//! constraints (by calling `check_constraints()`) or fail closed.

use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::Snapshot;
use test_utils::engine_store_setup;

fn test_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new([
            StructField::nullable("amount", DataType::LONG),
            StructField::nullable("name", DataType::STRING),
        ])
        .unwrap(),
    )
}

#[tokio::test]
#[cfg(not(feature = "check-constraints-in-dev"))]
async fn write_blocked_when_cargo_feature_off() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;

    use test_utils::create_table_with_configuration;

    let (store, engine, table_location) = engine_store_setup("test_cc_off", None);
    let table_url = create_table_with_configuration(
        store,
        table_location,
        test_schema(),
        &[],
        true,
        vec![],
        vec!["checkConstraints"],
        HashMap::from([(
            "delta.constraints.positive_amount".to_string(),
            "amount > 0".to_string(),
        )]),
    )
    .await?;

    let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
    let err = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)
        .expect_err("write must be blocked when checkConstraints is unsupported");
    assert!(
        err.to_string().contains("checkConstraints"),
        "error must name the unsupported feature; got: {err}",
    );
    Ok(())
}

#[cfg(feature = "check-constraints-in-dev")]
mod enabled {
    use std::collections::HashMap;

    use delta_kernel::arrow::array::{ArrayRef, Int64Array, StringArray};
    use delta_kernel::object_store::DynObjectStore;
    use delta_kernel::transaction::Transaction;
    use delta_kernel::SnapshotRef;
    use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
    use test_utils::delta_kernel_default_engine::DefaultEngine;
    use test_utils::{create_table_with_configuration, insert_data};
    use url::Url;

    use super::*;

    /// Creates an unpartitioned table listing `checkConstraints` with the given
    /// `delta.constraints.<name>` entries and returns `(table_url, engine)`.
    async fn setup_constrained_table(
        test_name: &str,
        constraints: &[(&str, &str)],
    ) -> Result<(Url, DefaultEngine<TokioBackgroundExecutor>), Box<dyn std::error::Error>> {
        let (store, engine, table_location): (Arc<DynObjectStore>, _, _) =
            engine_store_setup(test_name, None);
        let configuration = constraints
            .iter()
            .map(|(name, sql)| (format!("delta.constraints.{name}"), sql.to_string()))
            .collect();
        let table_url = create_table_with_configuration(
            store,
            table_location,
            test_schema(),
            &[],
            true,
            vec![],
            vec!["checkConstraints"],
            configuration,
        )
        .await?;
        Ok((table_url, engine))
    }

    fn begin_txn(
        table_url: &Url,
        engine: &DefaultEngine<TokioBackgroundExecutor>,
    ) -> Result<Transaction, Box<dyn std::error::Error>> {
        let snapshot: SnapshotRef = Snapshot::builder_for(table_url.clone()).build(engine)?;
        Ok(snapshot
            .transaction(Box::new(FileSystemCommitter::new()), engine)?
            .with_operation("WRITE".to_string()))
    }

    /// Stages one fabricated add-file on `txn` (bypassing kernel write contexts), so the commit is
    /// a data-adding commit that the acknowledgment gate applies to.
    fn add_one_file(txn: &mut Transaction) -> Result<(), Box<dyn std::error::Error>> {
        let fabricated = test_utils::create_add_files_metadata(
            txn.add_files_schema(),
            vec![("part-00000.parquet", 1024, 1_000_000, Some(1))],
        )?;
        txn.add_files(fabricated);
        Ok(())
    }

    /// Asserts an error is the fail-closed acknowledgment-gate error. Matches the error *variant*
    /// (plus a stable keyword) rather than the exact message, which is expected to be refined.
    fn assert_ack_gate_error(err: delta_kernel::Error) {
        assert!(
            matches!(err, delta_kernel::Error::Unsupported(_)),
            "ack gate must return Error::Unsupported; got: {err:?}",
        );
        assert!(
            err.to_string().contains("CHECK constraints"),
            "ack-gate error must name CHECK constraints; got: {err}",
        );
    }

    #[tokio::test]
    async fn creating_a_transaction_on_a_constrained_table_does_not_require_acknowledgment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) =
            setup_constrained_table("test_cc_txn_create", &[("positive_amount", "amount > 0")])
                .await?;
        // With the feature on, checkConstraints is Supported, so opening a transaction (which
        // checks write support) no longer blocks. Acknowledgment happens later, by calling
        // check_constraints(), and is only required at commit (see the gate tests below).
        begin_txn(&table_url, &engine)?;
        Ok(())
    }

    /// A data-adding commit to a constrained table fails closed unless the transaction
    /// acknowledged. The `after_snapshot_discovery` case first calls
    /// `Snapshot::check_constraints()` to prove that discovery is read-only -- it does not arm
    /// the transaction's commit gate.
    #[rstest::rstest]
    #[case::plain("test_cc_gate_unacked", false)]
    #[case::after_snapshot_discovery("test_cc_snapshot_discovery_unacked", true)]
    #[tokio::test]
    async fn data_adding_commit_without_acknowledgment_fails_closed(
        #[case] test_name: &str,
        #[case] discover_via_snapshot: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) =
            setup_constrained_table(test_name, &[("positive_amount", "amount > 0")]).await?;

        if discover_via_snapshot {
            // Snapshot discovery is read-only: it neither acknowledges nor arms any gate, so a
            // transaction built afterwards must still acknowledge on the transaction itself.
            let snapshot = Snapshot::builder_for(table_url.clone()).build(&engine)?;
            assert!(snapshot.check_constraints().is_kernel_parsable());
        }

        // A connector that registers add files directly and never acknowledges is stopped at
        // commit.
        let mut txn = begin_txn(&table_url, &engine)?;
        add_one_file(&mut txn)?;
        let err = txn
            .commit(&engine)
            .expect_err("data-adding commit without acknowledgment must fail closed");
        assert_ack_gate_error(err);
        Ok(())
    }

    #[tokio::test]
    async fn commit_adding_data_after_acknowledgment_succeeds(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) =
            setup_constrained_table("test_cc_gate_acked", &[("positive_amount", "amount > 0")])
                .await?;

        let mut txn = begin_txn(&table_url, &engine)?;
        // Calling check_constraints() IS the acknowledgment. (Enforcement -- validating the batch
        // against the predicate -- is later work; this test only exercises the gate.)
        let constraints = txn.check_constraints();
        assert!(constraints.is_kernel_parsable());
        assert_eq!(constraints.len(), 1);

        add_one_file(&mut txn)?;
        txn.commit(&engine)?.unwrap_committed();
        Ok(())
    }

    #[tokio::test]
    async fn acknowledgment_survives_builder_chaining_after_ack(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) = setup_constrained_table(
            "test_cc_ack_then_builder",
            &[("positive_amount", "amount > 0")],
        )
        .await?;

        // Acknowledgment is recorded through `&self`, but the `with_*` builders consume `self` by
        // value. Acknowledging BEFORE chaining a builder must still satisfy the commit gate -- the
        // acknowledgment moves with the transaction.
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
        let txn = snapshot.transaction(Box::new(FileSystemCommitter::new()), &engine)?;
        txn.check_constraints(); // acknowledge FIRST
        let mut txn = txn
            .with_operation("WRITE".to_string()) // moves self by value AFTER ack
            .with_engine_info("connector/1.0");
        add_one_file(&mut txn)?;
        txn.commit(&engine)?.unwrap_committed();
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_constraint_commits_after_acknowledgment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // The gate keys on acknowledgment + whether the table has constraints -- NOT on whether
        // kernel can parse them. A table with a kernel-unsupported constraint (a conjunction is
        // outside the single-comparison parser subset) is still committable once acknowledged; the
        // connector self-enforces the raw SQL.
        let (table_url, engine) = setup_constrained_table(
            "test_cc_unsupported_ack",
            &[("ranged", "amount > 0 AND amount < 100")],
        )
        .await?;

        let mut txn = begin_txn(&table_url, &engine)?;
        let constraints = txn.check_constraints();
        assert!(
            !constraints.is_kernel_parsable(),
            "conjunction is unparsable, so the set is not fully kernel-parsable",
        );
        // Kernel exposes the parse result per constraint rather than a connector-owned subset: the
        // conjunction has no predicate, only its raw SQL.
        let raw_only: Vec<_> = constraints
            .iter()
            .filter(|c| c.predicate().is_none())
            .map(|c| c.raw_sql())
            .collect();
        assert_eq!(raw_only, ["amount > 0 AND amount < 100"]);

        add_one_file(&mut txn)?;
        txn.commit(&engine)?.unwrap_committed();
        Ok(())
    }

    /// A commit that adds no rows needs no acknowledgment, whether it is metadata/commit-info-only
    /// or set-transaction-only. Both collapse to the "no add files" branch of the gate.
    #[rstest::rstest]
    #[case::metadata_only("test_cc_gate_metadata", false)]
    #[case::set_transaction_only("test_cc_gate_set_txn", true)]
    #[tokio::test]
    async fn non_data_adding_commit_needs_no_acknowledgment(
        #[case] test_name: &str,
        #[case] set_transaction: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) =
            setup_constrained_table(test_name, &[("positive_amount", "amount > 0")]).await?;
        let mut txn = begin_txn(&table_url, &engine)?;
        if set_transaction {
            txn = txn.with_transaction_id("app_id".to_string(), 1);
        }
        txn.commit(&engine)?.unwrap_committed();
        Ok(())
    }

    #[tokio::test]
    async fn feature_listed_but_no_constraints_needs_no_acknowledgment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // A table whose protocol lists checkConstraints but that declares no constraints has
        // nothing to enforce, so a data-adding commit needs no acknowledgment.
        let (table_url, engine) = setup_constrained_table("test_cc_feature_only", &[]).await?;
        let engine = Arc::new(engine);
        let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;

        assert!(snapshot.check_constraints().is_empty());

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![-1])), // no constraints: any value is fine
            Arc::new(StringArray::from(vec!["a"])),
        ];
        assert!(insert_data(snapshot, &engine, columns)
            .await?
            .is_committed());
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_check_constraints_is_deterministic_and_cached(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) = setup_constrained_table(
            "test_cc_snapshot_discovery",
            &[("positive_amount", "amount > 0"), ("named", "name = 'a'")],
        )
        .await?;
        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;

        // Discovery is deterministically ordered by name, and the cache returns an equivalent set
        // on repeated calls (parses at most once, but that is an implementation detail --
        // assert on the observable result, not the cache).
        assert!(snapshot.check_constraints().is_kernel_parsable());
        let first: Vec<_> = snapshot
            .check_constraints()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        assert_eq!(first, ["named", "positive_amount"]);
        let second: Vec<_> = snapshot
            .check_constraints()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        assert_eq!(second, first);
        Ok(())
    }

    #[tokio::test]
    async fn constraint_resolves_against_logical_names_under_column_mapping(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use test_utils::column_mapping_fixtures::cm_field;

        // Column-mapped table: physical names differ from logical names. The constraint references
        // the LOGICAL name `amount`, and kernel resolves constraints against the logical schema, so
        // it must parse regardless of column mapping. A regression resolving against the physical
        // schema would silently downgrade the constraint to connector-enforced.
        let schema = Arc::new(
            StructType::try_new([
                cm_field("amount", 1, "col-amount", DataType::LONG),
                cm_field("name", 2, "col-name", DataType::STRING),
            ])
            .unwrap(),
        );
        let (store, engine, table_location): (Arc<DynObjectStore>, _, _) =
            engine_store_setup("test_cc_column_mapping", None);
        let table_url = create_table_with_configuration(
            store,
            table_location,
            schema,
            &[],
            true,
            // columnMapping is a reader+writer feature: it must appear in BOTH lists.
            vec!["columnMapping"],
            vec!["columnMapping", "checkConstraints"],
            HashMap::from([
                (
                    "delta.constraints.positive_amount".to_string(),
                    "amount > 0".to_string(),
                ),
                (
                    "delta.columnMapping.maxColumnId".to_string(),
                    "2".to_string(),
                ),
            ]),
        )
        .await?;

        let snapshot = Snapshot::builder_for(table_url).build(&engine)?;
        let constraints = snapshot.check_constraints();
        assert!(
            constraints.is_kernel_parsable(),
            "logical name `amount` must resolve under column mapping",
        );
        assert_eq!(constraints.len(), 1);
        Ok(())
    }
}
