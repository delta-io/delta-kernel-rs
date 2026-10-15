//! Batch pruning contract tests with a synthetic ASCII comparator, without ICU conformance claims.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, BooleanArray, RecordBatch, StringArray};
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_expression;
use delta_kernel::engine::arrow_expression::opaque::{
    ArrowOpaquePredicate, ArrowOpaquePredicateOp,
};
use delta_kernel::expressions::{
    col, column_name, lit, BinaryPredicateOp, ColumnName, Expression, Predicate, Scalar,
    ScalarExpressionEvaluator,
};
use delta_kernel::kernel_predicates::{
    DirectDataSkippingPredicateEvaluator, DirectPredicateEvaluator,
    IndirectDataSkippingPredicateEvaluator,
};
use delta_kernel::scan::StatsOptions;
use delta_kernel::schema::DataType;
use delta_kernel::transaction::UpdateTableOperation;
use delta_kernel::{KernelError, Result, Snapshot};
use rstest::rstest;
use test_utils::{begin_transaction_with, into_record_batch, read_scan, test_table_setup_mt};

use super::{rows, simple_batch, synthetic_fixture, CollationStats, Operation, SYNTHETIC_ID};
use crate::features::collations::{create_fixture, generated_table, physical_path};

#[rstest]
#[case::matching(Some(SYNTHETIC_ID), true)]
#[case::missing(None, false)]
#[case::version_mismatch(Some("test.ASCII_CI.2"), false)]
#[case::provider_mismatch(Some("other.ASCII_CI.1"), false)]
#[case::name_mismatch(Some("test.ASCII_CS.1"), false)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opaque_batch_pruning_matches_unpruned_execution(
    #[case] stats_id: Option<&'static str>,
    #[case] compatible: bool,
    #[values(Operation::Equal, Operation::Greater)] operation: Operation,
    #[values(false, true)] inverted: bool,
    #[values("none", "name", "id")] mapping: &str,
    #[values(
        None,
        Some(CheckpointSpec::V1),
        Some(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)),
        Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar {
            file_actions_per_sidecar_hint: Some(1),
        }))
    )]
    checkpoint: Option<CheckpointSpec>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let fixture = synthetic_fixture(stats_id, operation);
    let mut properties = vec![
        ("delta.columnMapping.mode", mapping),
        ("delta.dataSkippingNumIndexedCols", "0"),
        ("delta.checkpoint.writeStatsAsStruct", "true"),
        ("delta.checkpoint.writeStatsAsJson", "false"),
    ];
    if matches!(checkpoint, Some(CheckpointSpec::V2(_))) {
        properties.push(("delta.feature.v2Checkpoint", "supported"));
    }
    let snapshot = create_fixture(&path, engine.as_ref(), &fixture, &properties).await?;
    if let Some(checkpoint) = checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(&checkpoint))?;
    }
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut predicate = Predicate::arrow_opaque(
        AsciiCaseInsensitiveOp {
            operation,
            stats_inverted: None,
            calls: calls.clone(),
        },
        [col!("name"), lit("alice")],
    );
    if inverted {
        predicate = Predicate::not(predicate);
    }
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_stats(
            StatsOptions::struct_columns(vec![column_name!("id")])
                .with_collation_stats(SYNTHETIC_ID, [column_name!("name")]),
        )
        .with_predicate(Arc::new(predicate))
        .build()?;
    let candidates = rows(&read_scan(&scan, engine.clone())?);
    assert!(calls.load(Ordering::Relaxed) > 0);
    assert_eq!(candidates.len(), if compatible { 2 } else { 4 });
    let actual: Vec<_> = candidates
        .into_iter()
        .filter(|(_, name)| operation.matches(name, "alice") != inverted)
        .collect();
    let oracle: Vec<_> = rows(&read_scan(
        &snapshot.scan_builder().build()?,
        engine.clone(),
    )?)
    .into_iter()
    .filter(|(_, name)| operation.matches(name, "alice") != inverted)
    .collect();
    assert_eq!(actual, oracle);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opaque_batch_pruning_retains_files_with_missing_required_bounds(
    #[values("minValues", "maxValues")] missing_bound: &str,
    #[values(false, true)] inverted: bool,
    #[values(false, true)] checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let mut fixture = synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal);
    for (_, stats) in &mut fixture.files {
        stats
            .as_mut()
            .unwrap()
            .bounds
            .as_object_mut()
            .unwrap()
            .remove(missing_bound);
    }
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &fixture,
        &[
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.checkpoint.writeStatsAsJson", "false"),
        ],
    )
    .await?;
    if checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(&CheckpointSpec::V1))?;
    }
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let mut predicate = Predicate::arrow_opaque(
        AsciiCaseInsensitiveOp {
            operation: Operation::Equal,
            stats_inverted: None,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        [col!("name"), lit("alice")],
    );
    if inverted {
        predicate = Predicate::not(predicate);
    }
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::none().with_collation_stats(SYNTHETIC_ID, [column_name!("name")]))
        .with_predicate(Arc::new(predicate))
        .build()?;
    assert_eq!(rows(&read_scan(&scan, engine)?).len(), 4);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opaque_batch_pruning_does_not_resurrect_removed_matching_files(
    #[values(false, true)] checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal),
        &[],
    )
    .await?;
    let remove_scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(Arc::new(col!("id").eq(lit(1))))
        .build()?;
    let mut transaction = begin_transaction_with(snapshot, engine.as_ref(), |builder| {
        builder.with_operation(UpdateTableOperation::Delete)
    })?;
    for metadata in remove_scan.scan_metadata(engine.as_ref())? {
        transaction.remove_files(metadata?.scan_files);
    }
    let snapshot = transaction
        .commit(engine.as_ref())?
        .unwrap_post_commit_snapshot();
    if checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(&CheckpointSpec::V1))?;
    }
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::none().with_collation_stats(SYNTHETIC_ID, [column_name!("name")]))
        .with_predicate(Arc::new(Predicate::arrow_opaque(
            AsciiCaseInsensitiveOp {
                operation: Operation::Equal,
                stats_inverted: None,
                calls: Arc::new(AtomicUsize::new(0)),
            },
            [col!("name"), lit("alice")],
        )))
        .build()?;
    assert_eq!(rows(&read_scan(&scan, engine)?), [(3, "alice".to_string())]);
    Ok(())
}

#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_mapped_collation_bounds_are_projected_by_exact_identifier(
    #[values("none", "name", "id")] mapping: &str,
    #[values(false, true)] checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let fixture = generated_table(true, &["first", "last", "email"]);
    let collation_id = fixture.files[0].1.as_ref().unwrap().identifier;
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &fixture,
        &[
            ("delta.columnMapping.mode", mapping),
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.checkpoint.writeStatsAsJson", "false"),
        ],
    )
    .await?;
    if checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(&CheckpointSpec::V1))?;
    }
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let selected_path = physical_path(&snapshot, &column_name!("name.first"));
    let unselected_path = physical_path(&snapshot, &column_name!("name.last"));
    let builder = snapshot.scan_builder().with_stats(
        StatsOptions::none()
            .with_collation_stats(collation_id, [column_name!("name.first")])
            .with_collation_stats("spark.UTF8_LCASE.99.0", [column_name!("name.first")]),
    );
    let schemas = builder.stats_output_schemas()?.unwrap();
    let expected = ColumnName::new(["statsWithCollation", collation_id, "minValues"])
        .join(&ColumnName::new(selected_path.clone()));
    assert!(schemas.physical.field_at(&expected).is_ok());
    assert!(schemas.logical.contains_col([
        "statsWithCollation",
        collation_id,
        "minValues",
        "name",
        "first"
    ]));
    let unselected = ColumnName::new(["statsWithCollation", collation_id, "minValues"])
        .join(&ColumnName::new(unselected_path));
    assert!(schemas.physical.field_at(&unselected).is_err());
    let missing = ColumnName::new([
        "stats_parsed",
        "statsWithCollation",
        "spark.UTF8_LCASE.99.0",
        "minValues",
    ])
    .join(&ColumnName::new(selected_path));
    let read = ColumnName::new(["stats_parsed"]).join(&expected);
    let scan = builder.build()?;
    let mut names = Vec::new();
    for metadata in scan.scan_metadata(engine.as_ref())? {
        let batch = into_record_batch(metadata?.scan_files.apply_selection_vector()?);
        let values = evaluate_expression(&Expression::Column(read.clone()), &batch, None)?;
        names.extend(
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .flatten()
                .map(str::to_string),
        );
        let missing = evaluate_expression(&Expression::Column(missing.clone()), &batch, None)?;
        assert_eq!(missing.null_count(), batch.num_rows());
    }
    names.sort();
    assert_eq!(names, ["Alice", "alice"]);
    Ok(())
}

#[rstest]
#[case::and(false, false, 1)]
#[case::or(true, false, 4)]
#[case::not_and(false, true, 4)]
#[case::not_or(true, true, 3)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_collated_comparisons_defer_without_losing_boolean_structure(
    #[case] disjunction: bool,
    #[case] inverted: bool,
    #[case] candidates: usize,
    #[values(false, true)] opaque: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &synthetic_fixture((!opaque).then_some(SYNTHETIC_ID), Operation::Equal),
        &[],
    )
    .await?;
    let collated = if opaque {
        Predicate::arrow_opaque(
            AsciiCaseInsensitiveOp {
                operation: Operation::Equal,
                stats_inverted: None,
                calls: Arc::new(AtomicUsize::new(0)),
            },
            [col!("name"), lit("alice")],
        )
    } else {
        Predicate::binary_with_collation(
            BinaryPredicateOp::Equal,
            col!("name"),
            lit("alice"),
            SYNTHETIC_ID,
        )
    };
    let numeric = col!("id").ge(lit(4));
    let mut predicate = if disjunction {
        Predicate::or(collated, numeric)
    } else {
        Predicate::and(collated, numeric)
    };
    if inverted {
        predicate = Predicate::not(predicate);
    }
    let scan = snapshot
        .scan_builder()
        .with_stats(
            StatsOptions::struct_columns(vec![column_name!("id")])
                .with_collation_stats(SYNTHETIC_ID, [column_name!("name")]),
        )
        .with_predicate(Arc::new(predicate))
        .build()?;
    assert_eq!(rows(&read_scan(&scan, engine)?).len(), candidates);
    Ok(())
}

#[rstest]
#[case::and(false, false, &[])]
#[case::or(true, false, &[1, 3, 4])]
#[case::not_and(false, true, &[1, 2, 3, 4])]
#[case::not_or(true, true, &[2])]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supported_collated_boolean_pruning_preserves_rows_in_mixed_intervals(
    #[case] disjunction: bool,
    #[case] inverted: bool,
    #[case] expected_ids: &[i32],
    #[values(None, Some("minValues"), Some("maxValues"))] missing_bound: Option<&str>,
    #[values(false, true)] checkpoint: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let mut fixture = synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal);
    let mut bounds = serde_json::json!({
        "minValues": { "name": "Alice" },
        "maxValues": { "name": "Zoe" },
    });
    if let Some(missing_bound) = missing_bound {
        bounds.as_object_mut().unwrap().remove(missing_bound);
    }
    fixture.files[0] = (
        simple_batch(vec![1, 2], vec!["Alice", "Zoe"]),
        Some(CollationStats {
            identifier: SYNTHETIC_ID,
            bounds,
        }),
    );
    fixture.files.remove(1);
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &fixture,
        &[
            ("delta.checkpoint.writeStatsAsStruct", "true"),
            ("delta.checkpoint.writeStatsAsJson", "false"),
        ],
    )
    .await?;
    if checkpoint {
        snapshot.checkpoint(engine.as_ref(), Some(&CheckpointSpec::V1))?;
    }
    let snapshot = Snapshot::builder_for(&path).build(engine.as_ref())?;
    let collated = Predicate::arrow_opaque(
        AsciiCaseInsensitiveOp {
            operation: Operation::Equal,
            stats_inverted: None,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        [col!("name"), lit("alice")],
    );
    let numeric = col!("id").ge(lit(4));
    let mut predicate = if disjunction {
        Predicate::or(collated, numeric)
    } else {
        Predicate::and(collated, numeric)
    };
    if inverted {
        predicate = Predicate::not(predicate);
    }
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_stats(
            StatsOptions::struct_columns(vec![column_name!("id")])
                .with_collation_stats(SYNTHETIC_ID, [column_name!("name")]),
        )
        .with_predicate(Arc::new(predicate))
        .build()?;
    let full_filter = |(id, name): &(i32, String)| {
        let collated = Operation::Equal.matches(name, "alice");
        let numeric = *id >= 4;
        let matches = if disjunction {
            collated || numeric
        } else {
            collated && numeric
        };
        matches != inverted
    };
    let actual: Vec<_> = rows(&read_scan(&scan, engine.clone())?)
        .into_iter()
        .filter(full_filter)
        .collect();
    let oracle: Vec<_> = rows(&read_scan(
        &snapshot
            .scan_builder()
            .with_stats(StatsOptions::none())
            .build()?,
        engine,
    )?)
    .into_iter()
    .filter(full_filter)
    .collect();
    assert_eq!(
        oracle.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        expected_ids
    );
    assert_eq!(actual, oracle);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_columnar_collation_bounds_support_multiple_predicates_without_log_replay(
) -> Result<(), Box<dyn std::error::Error>> {
    let (_temp, path, engine) = test_table_setup_mt()?;
    let snapshot = create_fixture(
        &path,
        engine.as_ref(),
        &synthetic_fixture(Some(SYNTHETIC_ID), Operation::Equal),
        &[],
    )
    .await?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::none().with_collation_stats(SYNTHETIC_ID, [column_name!("name")]))
        .build()?;
    let mut cached = Vec::new();
    for metadata in scan.scan_metadata(engine.as_ref())? {
        cached.push(into_record_batch(
            metadata?.scan_files.apply_selection_vector()?,
        ));
    }
    let calls = Arc::new(AtomicUsize::new(0));
    for (operation, inverted, literal, expected) in [
        (Operation::Equal, false, "alice", 2),
        (Operation::Equal, false, "bob", 1),
        (Operation::Greater, false, "alice", 2),
        (Operation::Greater, true, "bob", 3),
    ] {
        let evaluator = AsciiCaseInsensitiveOp {
            operation,
            stats_inverted: Some(inverted),
            calls: calls.clone(),
        };
        let args = AsciiCaseInsensitiveOp::bounds(&column_name!("name"), lit(literal));
        let mut retained = 0;
        for batch in &cached {
            retained += evaluator
                .eval_pred(&args, batch, false)?
                .iter()
                .filter(|verdict| *verdict != Some(false))
                .count();
        }
        assert_eq!(retained, expected);
    }
    assert_eq!(calls.load(Ordering::Relaxed), cached.len() * 4);
    Ok(())
}

#[derive(Clone, Debug)]
struct AsciiCaseInsensitiveOp {
    operation: Operation,
    stats_inverted: Option<bool>,
    calls: Arc<AtomicUsize>,
}

impl AsciiCaseInsensitiveOp {
    fn bounds(column: &ColumnName, literal: Expression) -> [Expression; 3] {
        let bound = |name| {
            Expression::Column(
                ColumnName::new(["stats_parsed", "statsWithCollation", SYNTHETIC_ID, name])
                    .join(column),
            )
        };
        [bound("minValues"), bound("maxValues"), literal]
    }
}

impl PartialEq for AsciiCaseInsensitiveOp {
    fn eq(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.stats_inverted == other.stats_inverted
            && Arc::ptr_eq(&self.calls, &other.calls)
    }
}

impl ArrowOpaquePredicateOp for AsciiCaseInsensitiveOp {
    fn name(&self) -> &str {
        "synthetic_ascii_case_insensitive"
    }

    fn eval_pred(
        &self,
        args: &[Expression],
        batch: &RecordBatch,
        inverted: bool,
    ) -> Result<BooleanArray> {
        let Some(stats_inverted) = self.stats_inverted else {
            return Err(KernelError::unsupported(
                "Row comparisons are connector-owned",
            ));
        };
        assert!(!inverted, "A may-match mask must never be negated");
        self.calls.fetch_add(1, Ordering::Relaxed);
        let arrays = args
            .iter()
            .map(|arg| evaluate_expression(arg, batch, Some(&DataType::STRING)))
            .collect::<Result<Vec<_>>>()?;
        let strings: Vec<_> = arrays
            .iter()
            .map(|array| array.as_any().downcast_ref::<StringArray>().unwrap())
            .collect();
        let [min, max, literal] = strings.as_slice() else {
            panic!("Expected min, max, and literal arguments");
        };
        Ok((0..batch.num_rows())
            .map(|row| {
                let literal = literal.value(row).to_ascii_lowercase();
                match (self.operation, stats_inverted) {
                    (Operation::Greater, false) => {
                        (!max.is_null(row)).then(|| max.value(row).to_ascii_lowercase() > literal)
                    }
                    (Operation::Greater, true) => {
                        (!min.is_null(row)).then(|| min.value(row).to_ascii_lowercase() <= literal)
                    }
                    (Operation::Equal, false) => {
                        (!min.is_null(row) && !max.is_null(row)).then(|| {
                            min.value(row).to_ascii_lowercase() <= literal
                                && literal <= max.value(row).to_ascii_lowercase()
                        })
                    }
                    (Operation::Equal, true) => {
                        (!min.is_null(row) && !max.is_null(row)).then(|| {
                            min.value(row).to_ascii_lowercase() != literal
                                || max.value(row).to_ascii_lowercase() != literal
                        })
                    }
                }
            })
            .collect())
    }

    fn eval_pred_scalar(
        &self,
        _eval_expr: &ScalarExpressionEvaluator<'_>,
        _eval_pred: &DirectPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Result<Option<bool>> {
        Err(KernelError::unsupported("No scalar collation evaluator"))
    }

    fn eval_as_data_skipping_predicate(
        &self,
        _evaluator: &DirectDataSkippingPredicateEvaluator<'_>,
        _exprs: &[Expression],
        _inverted: bool,
    ) -> Option<bool> {
        None
    }

    fn as_data_skipping_predicate(
        &self,
        _evaluator: &IndirectDataSkippingPredicateEvaluator<'_>,
        exprs: &[Expression],
        inverted: bool,
    ) -> Option<Predicate> {
        let [Expression::Column(column), literal @ Expression::Literal(Scalar::String(_))] = exprs
        else {
            return None;
        };
        Some(Predicate::arrow_opaque(
            Self {
                stats_inverted: Some(inverted),
                ..self.clone()
            },
            Self::bounds(column, literal.clone()),
        ))
    }
}
