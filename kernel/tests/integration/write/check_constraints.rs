//! Integration tests for the `checkConstraints` writer feature (in development, gated by the
//! `check-constraints-in-dev` cargo feature): discovery, the acknowledgment/commit gate, and
//! connector-driven enforcement.
//!
//! Coverage spans the contract that guards a constrained table (a data-adding commit must
//! acknowledge by calling `check_constraints()` or fail closed), the validator a connector runs
//! over the full logical batch before partitioning, and interactions with other table features
//! (column mapping, partitioning).

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

    use delta_kernel::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::expressions::Scalar;
    use delta_kernel::object_store::DynObjectStore;
    use delta_kernel::transaction::Transaction;
    use delta_kernel::{Engine as _, SnapshotRef};
    use itertools::Itertools as _;
    use test_utils::delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
    use test_utils::delta_kernel_default_engine::DefaultEngine;
    use test_utils::{create_table_with_configuration, insert_data, test_read};
    use url::Url;

    use super::*;

    /// Creates an unpartitioned table listing `checkConstraints` with the given
    /// `delta.constraints.<name>` entries and returns `(table_url, engine)`.
    async fn setup_constrained_table(
        test_name: &str,
        constraints: &[(&str, &str)],
    ) -> Result<(Url, DefaultEngine<TokioBackgroundExecutor>), Box<dyn std::error::Error>> {
        setup_constrained_table_partitioned(test_name, constraints, &[]).await
    }

    /// Like [`setup_constrained_table`] but partitions the table by `partition_columns`, so tests
    /// can exercise constraints that reference a partition column.
    async fn setup_constrained_table_partitioned(
        test_name: &str,
        constraints: &[(&str, &str)],
        partition_columns: &[&str],
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
            partition_columns,
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

    /// Builds a logical batch of the table's schema (`amount: LONG`, `name: STRING`) for validator
    /// tests. `amounts` carries the nullable values the constraints are checked against.
    fn batch(
        amounts: Vec<Option<i64>>,
        names: Vec<&str>,
    ) -> Result<ArrowEngineData, Box<dyn std::error::Error>> {
        let arrow_schema = Arc::new(test_schema().as_ref().try_into_arrow()?);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(amounts)),
            Arc::new(StringArray::from(names)),
        ];
        Ok(ArrowEngineData::new(RecordBatch::try_new(
            arrow_schema,
            columns,
        )?))
    }

    fn assert_err_contains(err: delta_kernel::Error, needle: &str) {
        let msg = err.to_string();
        assert!(msg.contains(needle), "expected '{needle}' in error: {msg}");
    }

    /// Builds the data-only batch (`amount`) written to a table partitioned by `name`: the physical
    /// parquet excludes the partition column, which is supplied to the write context as a Scalar.
    fn amount_only_batch(
        amounts: Vec<Option<i64>>,
    ) -> Result<ArrowEngineData, Box<dyn std::error::Error>> {
        let schema = StructType::try_new([StructField::nullable("amount", DataType::LONG)])?;
        let arrow_schema = Arc::new((&schema).try_into_arrow()?);
        Ok(ArrowEngineData::new(RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int64Array::from(amounts)) as ArrayRef],
        )?))
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

    /// A connector runs the kernel-built validator over its data before writing. A satisfying batch
    /// passes; a `false` row and a `NULL` row (the protocol counts both as violations) each raise a
    /// [`CheckConstraintViolation`] naming the constraint. `write_parquet` itself validates
    /// nothing.
    #[tokio::test]
    async fn validator_catches_false_and_null_violations() -> Result<(), Box<dyn std::error::Error>>
    {
        let (table_url, engine) = setup_constrained_table(
            "test_cc_validator_violations",
            &[("positive_amount", "amount > 0")],
        )
        .await?;
        // Calling check_constraints() is itself the acknowledgment; it also reports the set is
        // fully kernel-parsable, so a validator binds without failing closed.
        let txn = begin_txn(&table_url, &engine)?;
        let constraints = txn.check_constraints();
        assert!(constraints.is_kernel_parsable());
        let constraint = constraints
            .iter()
            .exactly_one()
            .expect("table has exactly one constraint");
        assert!(constraint.predicate().is_some());

        let handler = engine.evaluation_handler();

        // All rows satisfy the constraint.
        constraint.validate(
            &batch(vec![Some(1), Some(5)], vec!["a", "b"])?,
            handler.as_ref(),
        )?;

        // A `false` row violates, and the error names the constraint.
        let err = constraint
            .validate(
                &batch(vec![Some(1), Some(-5)], vec!["a", "b"])?,
                handler.as_ref(),
            )
            .expect_err("a negative amount must violate");
        assert_err_contains(err, "positive_amount");

        // A `NULL` predicate result also violates (only `true` passes).
        let err = constraint
            .validate(
                &batch(vec![Some(1), None], vec!["a", "b"])?,
                handler.as_ref(),
            )
            .expect_err("a NULL amount must violate");
        assert_err_contains(err, "NULL");
        Ok(())
    }

    /// Building a validator over a constraint kernel cannot parse fails closed (before any data is
    /// written); a connector with no SQL engine of its own must refuse the write. The validator is
    /// bound once from the acknowledged set and reused, and a typed violation carries the offending
    /// value.
    #[tokio::test]
    async fn validator_binds_once_and_fails_closed_on_unparsable(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) = setup_constrained_table(
            "test_cc_validator_fail_closed",
            &[("amount_range", "amount > 0 AND amount < 100")],
        )
        .await?;
        let txn = begin_txn(&table_url, &engine)?;
        let constraints = txn.check_constraints();
        // The junction is outside the single-comparison grammar, so the set is not fully parsable
        // and binding a validator over it fails closed with the connector-enforced error.
        assert!(!constraints.is_kernel_parsable());
        let handler = engine.evaluation_handler();
        let err = constraints
            .validator(handler.as_ref())
            .map(|_| ())
            .expect_err("an unparsable constraint must fail closed");
        assert_err_contains(err, "must enforce");

        // A parsable set binds once and reports a typed violation with the offending value.
        let (table_url, engine) = setup_constrained_table(
            "test_cc_validator_typed_violation",
            &[("positive_amount", "amount > 0")],
        )
        .await?;
        let txn = begin_txn(&table_url, &engine)?;
        let handler = engine.evaluation_handler();
        let validator = txn.check_constraints().validator(handler.as_ref())?;
        validator.validate(&batch(vec![Some(1), Some(2)], vec!["a", "b"])?)?;
        let err = validator
            .validate(&batch(vec![Some(1), Some(-7)], vec!["a", "b"])?)
            .expect_err("a negative amount must violate");
        match err {
            delta_kernel::Error::CheckConstraintViolation {
                name, expression, ..
            } => {
                assert_eq!(name, "positive_amount");
                assert_eq!(expression, "amount > 0");
            }
            other => panic!("expected CheckConstraintViolation, got: {other:?}"),
        }
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

    // === Pre-partition enforcement flow ===
    //
    // A connector enforces CHECK constraints itself: it acknowledges via `check_constraints()`,
    // builds one validator, and validates the full logical batch *before* partitioning or writing.
    // Kernel wires nothing into `write_parquet`. Validating before partitioning is sound by
    // construction -- the batch still carries the partition column as ordinary per-row data, and
    // the partition a file lands in is derived from that same column, so the validated value is
    // exactly what a reader reconstructs from `add.partitionValues`. Because validation
    // completes before the first file is written, a violation writes no parquet and the
    // connector never reaches `commit()` (Delta's atomic commit then leaves the table
    // unchanged).

    /// The documented flow: validate the whole batch, and only if it passes write + commit. A
    /// violating batch is caught before any file is written (no parquet, no commit); a satisfying
    /// batch commits and round-trips. `write_parquet` validates nothing itself.
    #[tokio::test]
    async fn connector_validates_batch_before_writing() -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) = setup_constrained_table(
            "test_cc_validate_before_write",
            &[("positive_amount", "amount > 0")],
        )
        .await?;
        let mut txn = begin_txn(&table_url, &engine)?;
        // Acknowledge, then bind the validator once, before building a write context.
        let validator = txn
            .check_constraints()
            .validator(engine.evaluation_handler().as_ref())?;
        let write_context = txn.unpartitioned_write_context()?;

        // A violating batch is rejected by the connector's own validation -- nothing is written.
        let err = validator
            .validate(&batch(vec![Some(1), Some(-5)], vec!["a", "b"])?)
            .expect_err("a violating batch must be rejected before writing");
        assert_err_contains(err, "positive_amount");

        // A satisfying batch: validate, then write + commit + read back.
        let good = batch(vec![Some(1), Some(5)], vec!["a", "b"])?;
        validator.validate(&good)?;
        let add_files_metadata = engine.write_parquet(&good, &write_context).await?;
        txn.add_files(add_files_metadata);
        txn.commit(&engine)?.unwrap_committed();

        test_read(
            &batch(vec![Some(1), Some(5)], vec!["a", "b"])?,
            &table_url,
            Arc::new(engine),
        )?;
        Ok(())
    }

    /// A constraint on a *partition* column is just a predicate over the pre-partition batch (which
    /// carries the partition column as data), enforced by the same validator as a data-column
    /// constraint -- no special handling. Both a partition-column and a data-column violation are
    /// caught; a satisfying batch writes to its partition, commits, and round-trips.
    #[tokio::test]
    async fn partition_column_constraint_validates_over_full_batch(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) = setup_constrained_table_partitioned(
            "test_cc_partition_col",
            &[
                ("name_check", "name = 'a'"),
                ("positive_amount", "amount > 0"),
            ],
            &["name"],
        )
        .await?;
        let mut txn = begin_txn(&table_url, &engine)?;

        // Both constraints parse -- the partition-column one (`name = 'a'`) is just a predicate.
        let constraints = txn.check_constraints();
        assert!(constraints.is_kernel_parsable());
        let validator = constraints.validator(engine.evaluation_handler().as_ref())?;

        // A row violating the partition-column constraint (name != 'a') is caught.
        let err = validator
            .validate(&batch(vec![Some(1)], vec!["b"])?)
            .expect_err("name 'b' must violate name_check");
        assert_err_contains(err, "name_check");

        // A row violating the data-column constraint (amount <= 0) is caught too.
        let err = validator
            .validate(&batch(vec![Some(-5)], vec!["a"])?)
            .expect_err("a negative amount must violate positive_amount");
        assert_err_contains(err, "positive_amount");

        // A satisfying batch validates over the FULL batch (partition column `name` present as
        // data). The write then takes the data-only batch (`amount`) plus the partition value as a
        // Scalar -- the partition column is not part of the physical parquet.
        validator.validate(&batch(vec![Some(5)], vec!["a"])?)?;
        let write_context = txn
            .partitioned_write_context(HashMap::from([("name".to_string(), Scalar::from("a"))]))?;
        let add_files_metadata = engine
            .write_parquet(&amount_only_batch(vec![Some(5)])?, &write_context)
            .await?;
        txn.add_files(add_files_metadata);
        txn.commit(&engine)?.unwrap_committed();

        test_read(
            &batch(vec![Some(5)], vec!["a"])?,
            &table_url,
            Arc::new(engine),
        )?;
        Ok(())
    }

    /// A constraint referencing BOTH a partition column and a data column (`region != label`) is
    /// just a predicate over the full pre-partition batch, which carries both columns as data -- no
    /// connector SQL engine and no partition overlay needed. The typed violation locates the row; a
    /// satisfying write commits and round-trips, with `region` reconstructed from
    /// `add.partitionValues`.
    #[tokio::test]
    async fn mixed_partition_and_data_constraint_validates_over_full_batch(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Two STRING columns so a partition-vs-data comparison is well-typed; `region` partitions.
        let schema = Arc::new(
            StructType::try_new([
                StructField::nullable("label", DataType::STRING),
                StructField::nullable("region", DataType::STRING),
            ])
            .unwrap(),
        );
        // Pre-partition batches carry both columns with their real per-row values (validation
        // sees the whole batch).
        let mk_batch = |labels: Vec<&str>,
                        regions: Vec<&str>|
         -> Result<ArrowEngineData, Box<dyn std::error::Error>> {
            let arrow_schema = Arc::new(schema.as_ref().try_into_arrow()?);
            Ok(ArrowEngineData::new(RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(labels)) as ArrayRef,
                    Arc::new(StringArray::from(regions)) as ArrayRef,
                ],
            )?))
        };
        // The physical write excludes the `region` partition column (supplied as a Scalar).
        let data_schema = Arc::new(StructType::try_new([StructField::nullable(
            "label",
            DataType::STRING,
        )])?);
        let mk_label_batch =
            |labels: Vec<&str>| -> Result<ArrowEngineData, Box<dyn std::error::Error>> {
                let arrow_schema = Arc::new(data_schema.as_ref().try_into_arrow()?);
                Ok(ArrowEngineData::new(RecordBatch::try_new(
                    arrow_schema,
                    vec![Arc::new(StringArray::from(labels)) as ArrayRef],
                )?))
            };

        let (store, engine, table_location): (Arc<DynObjectStore>, _, _) =
            engine_store_setup("test_cc_mixed_partition", None);
        let table_url = create_table_with_configuration(
            store,
            table_location,
            schema.clone(),
            &["region"],
            true,
            vec![],
            vec!["checkConstraints"],
            HashMap::from([(
                "delta.constraints.region_ne_label".to_string(),
                "region != label".to_string(),
            )]),
        )
        .await?;

        let mut txn = begin_txn(&table_url, &engine)?;

        // The mixed constraint is kernel-parsable: a plain comparison over two batch columns.
        let constraints = txn.check_constraints();
        assert!(constraints.is_kernel_parsable());
        let validator = constraints.validator(engine.evaluation_handler().as_ref())?;

        // Satisfying: no row has region == label.
        validator.validate(&mk_batch(vec!["EU", "ASIA"], vec!["US", "US"])?)?;

        // Violating: row 1 has region == label ("US" == "US") -> `region != label` is false.
        let err = validator
            .validate(&mk_batch(vec!["EU", "US"], vec!["US", "US"])?)
            .expect_err("a row whose region equals its label must violate");
        match err {
            delta_kernel::Error::CheckConstraintViolation {
                name,
                expression,
                details,
            } => {
                assert_eq!(name, "region_ne_label");
                assert_eq!(expression, "region != label");
                assert!(details.contains("row 1"), "locates the row: {details}");
            }
            other => panic!("expected CheckConstraintViolation, got: {other:?}"),
        }

        // A satisfying write commits and round-trips, with `region` reconstructed from
        // `add.partitionValues`. Validation sees the full batch; the write takes the data-only
        // (`label`) batch plus the partition value.
        validator.validate(&mk_batch(vec!["EU", "ASIA"], vec!["US", "US"])?)?;
        let write_context = txn.partitioned_write_context(HashMap::from([(
            "region".to_string(),
            Scalar::from("US"),
        )]))?;
        let add_files = engine
            .write_parquet(&mk_label_batch(vec!["EU", "ASIA"])?, &write_context)
            .await?;
        txn.add_files(add_files);
        txn.commit(&engine)?.unwrap_committed();

        test_read(
            &mk_batch(vec!["EU", "ASIA"], vec!["US", "US"])?,
            &table_url,
            Arc::new(engine),
        )?;
        Ok(())
    }

    // === Cross-feature coverage ===

    /// A table with multiple constraints, partitioned, written across multiple partition values in
    /// one transaction. One validator (bound once) enforces every constraint over each full batch
    /// before it is partitioned; each partition's data-only batch is then written and the whole
    /// write commits as one version. Exercises the realistic connector loop end to end.
    #[tokio::test]
    async fn multiple_constraints_across_partitions_validate_and_commit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (table_url, engine) = setup_constrained_table_partitioned(
            "test_cc_matrix",
            &[
                ("positive_amount", "amount > 0"),
                ("named_a_or_b", "name != 'c'"),
            ],
            &["name"],
        )
        .await?;
        let mut txn = begin_txn(&table_url, &engine)?;

        // Acknowledge and bind one validator for the whole write.
        let constraints = txn.check_constraints();
        assert!(constraints.is_kernel_parsable());
        assert_eq!(constraints.len(), 2);
        let validator = constraints.validator(engine.evaluation_handler().as_ref())?;

        // Two partitions ("a" and "b"), each a full batch validated before partitioning. The write
        // context takes the partition value as a Scalar and the data-only (`amount`) batch.
        for (partition, amounts) in [("a", vec![Some(1), Some(2)]), ("b", vec![Some(3)])] {
            let names = vec![partition; amounts.len()];
            validator.validate(&batch(amounts.clone(), names)?)?;
            let write_context = txn.partitioned_write_context(HashMap::from([(
                "name".to_string(),
                Scalar::from(partition),
            )]))?;
            let add_files = engine
                .write_parquet(&amount_only_batch(amounts)?, &write_context)
                .await?;
            txn.add_files(add_files);
        }
        txn.commit(&engine)?.unwrap_committed();

        // A later violating batch (name == 'c') is caught by the same validator, unaffected by the
        // committed data.
        let err = validator
            .validate(&batch(vec![Some(9)], vec!["c"])?)
            .expect_err("name 'c' must violate named_a_or_b");
        assert_err_contains(err, "named_a_or_b");
        Ok(())
    }
}
