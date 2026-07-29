use std::sync::Arc;

use ::test_utils::table_builder::{DataLayoutConfig, FeatureSet, LogState, TestTableBuilder};
use rstest::rstest;

use super::*;
use crate::arrow::array::{Array, ArrayRef, BooleanArray, StructArray};
use crate::arrow::compute::filter_record_batch;
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty::pretty_format_batches;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::expressions::{column_expr, column_name, Expression as Expr, Predicate as Pred};
use crate::plans::ir::nodes::Operator;
use crate::plans::Operation as PlanOperation;
use crate::scan::{PartitionValuesOptions, Scan, StatsOptions, StructStats};
use crate::{DeltaResult, Engine, Snapshot};

fn comparable_metadata_batch(
    field: impl Fn(&str) -> ArrayRef,
    stats_parsed: Option<ArrayRef>,
    partitions_parsed: Option<ArrayRef>,
) -> DeltaResult<RecordBatch> {
    let mut columns = vec![
        ("path", field("path")),
        ("size", field("size")),
        ("modificationTime", field("modificationTime")),
        ("stats", field("stats")),
        ("partitionValues", field("partitionValues")),
        ("deletionVector", field("deletionVector")),
        ("baseRowId", field("baseRowId")),
        ("defaultRowCommitVersion", field("defaultRowCommitVersion")),
        ("tags", field("tags")),
        ("clusteringProvider", field("clusteringProvider")),
    ];
    if let Some(stats) = stats_parsed {
        columns.push(("stats_parsed", stats));
    }
    if let Some(partitions) = partitions_parsed {
        columns.push(("partitionValues_parsed", partitions));
    }
    Ok(RecordBatch::try_from_iter(columns)?)
}

fn imperative_metadata(scan: Scan, engine: &dyn Engine) -> DeltaResult<Vec<RecordBatch>> {
    let mut batches = vec![];
    for metadata in scan.scan_metadata(engine)? {
        let (data, selection) = metadata?.scan_files.into_parts();
        let batch = filter_record_batch(
            &data.try_into_record_batch()?,
            &BooleanArray::from(selection),
        )?;
        if batch.num_rows() == 0 {
            continue;
        }
        let constants = batch
            .column_by_name("fileConstantValues")
            .expect("file constants")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("file constants struct");
        batches.push(comparable_metadata_batch(
            |name| {
                batch
                    .column_by_name(name)
                    .or_else(|| constants.column_by_name(name))
                    .unwrap_or_else(|| panic!("metadata field {name}"))
                    .clone()
            },
            batch.column_by_name("stats_parsed").cloned(),
            batch.column_by_name("partitionValues_parsed").cloned(),
        )?);
    }
    Ok(batches)
}

fn declarative_metadata(scan: &Scan, engine: &dyn Engine) -> DeltaResult<Vec<RecordBatch>> {
    let Some(plan) = scan.declarative_metadata_scan_plan(engine)? else {
        return Ok(vec![]);
    };
    let batches = engine
        .plan_executor()
        .execute_op(PlanOperation::QueryPlan(plan))?
        .into_data()?;

    let mut projected = vec![];
    for batch in batches {
        let batch = batch?.try_into_record_batch()?;
        if batch.num_rows() == 0 {
            continue;
        }
        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        projected.push(comparable_metadata_batch(
            |name| {
                add.column_by_name(name)
                    .unwrap_or_else(|| panic!("add.{name}"))
                    .clone()
            },
            add.column_by_name(STATS_PARSED).cloned(),
            add.column_by_name(PARTITION_VALUES_PARSED).cloned(),
        )?);
    }
    Ok(projected)
}

fn metadata_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn sorted_pretty_lines(batches: &[RecordBatch]) -> DeltaResult<Vec<String>> {
    let formatted = pretty_format_batches(batches)?.to_string();
    let mut lines: Vec<_> = formatted.lines().map(str::to_string).collect();
    let len = lines.len();
    if len > 3 {
        lines[2..len - 1].sort_unstable();
    }
    Ok(lines)
}

fn assert_metadata_eq(
    actual: &[RecordBatch],
    expected: &[RecordBatch],
    context: &str,
) -> DeltaResult<()> {
    if let (Some(actual), Some(expected)) = (actual.first(), expected.first()) {
        assert_eq!(actual.schema(), expected.schema(), "{context}");
    }
    let actual = sorted_pretty_lines(actual)?;
    let expected = sorted_pretty_lines(expected)?;
    assert_eq!(actual, expected, "{context}");
    Ok(())
}

fn without_stats(batches: &[RecordBatch], keep_json: bool) -> DeltaResult<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            RecordBatch::try_from_iter(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .filter(|(field, _)| {
                        field.name() != STATS_PARSED && (keep_json || field.name() != STATS)
                    })
                    .map(|(field, column)| (field.name(), column.clone())),
            )
            .map_err(Into::into)
        })
        .collect()
}

fn assert_stats_output_schema(batches: &[RecordBatch], stats: &StructStats) {
    for batch in batches {
        let schema = batch.schema();
        let stats_field = schema.field_with_name(STATS_PARSED).ok();
        match stats {
            StructStats::None => assert!(stats_field.is_none()),
            StructStats::All => assert!(stats_field.is_some()),
            StructStats::Columns(columns) if columns.is_empty() => assert!(stats_field.is_none()),
            StructStats::Columns(columns) => {
                let ArrowDataType::Struct(stats_fields) =
                    stats_field.expect("stats_parsed output").data_type()
                else {
                    panic!("stats_parsed must be a struct");
                };
                let null_counts = stats_fields
                    .iter()
                    .find(|field| field.name() == "nullCount")
                    .expect("nullCount output");
                let ArrowDataType::Struct(data_fields) = null_counts.data_type() else {
                    panic!("nullCount must be a struct");
                };
                let actual: Vec<_> = data_fields
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect();
                let expected: Vec<_> = columns
                    .iter()
                    .map(|column| column.path()[0].as_str())
                    .collect();
                assert_eq!(actual, expected);
            }
        }
    }
}

fn assert_json_only_output_schema(batches: &[RecordBatch]) {
    for batch in batches {
        assert_eq!(
            batch
                .schema()
                .field_with_name(STATS)
                .expect("JSON stats output")
                .data_type(),
            &ArrowDataType::Utf8,
        );
    }
}

#[rstest]
#[case::v2_parquet_manifest("v2-checkpoints-parquet-with-sidecars")]
#[case::v2_json_manifest("v2-checkpoints-json-with-sidecars")]
#[case::v2_parquet_leaf("v2-checkpoints-parquet-without-sidecars")]
#[case::v2_json_leaf("v2-checkpoints-json-without-sidecars")]
#[case::v1_single_part_struct_stats("v1-single-part-struct-stats-only")]
#[case::v1_multi_part_struct_stats("v1-multi-part-struct-stats-only")]
#[case::v1_multi_part_partitioned_struct_stats("v1-multi-part-partitioned-struct-stats-only")]
fn declarative_metadata_matches_imperative_scan(
    #[case] table: &str,
    #[values(
        None,
        Some(column_expr!("id").gt(Expr::literal(3i64))),
        Some(column_expr!("id").eq(Expr::literal(2i64))),
        Some(column_expr!("id").le(Expr::literal(0i64))),
        Some(column_expr!("id").is_not_null())
    )]
    predicate: Option<Pred>,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = crate::utils::test_utils::load_test_table(table)?;
    let predicate = predicate.map(Arc::new);

    let imperative_builder = snapshot
        .clone()
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct());
    let imperative_builder = match &predicate {
        Some(predicate) => imperative_builder.with_predicate(predicate.clone()),
        None => imperative_builder,
    };
    let expected = imperative_metadata(imperative_builder.build()?, engine.as_ref())?;

    let declarative_builder = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct());
    let declarative_builder = match predicate {
        Some(predicate) => declarative_builder.with_predicate(predicate),
        None => declarative_builder,
    };
    let scan = declarative_builder.build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;

    assert_metadata_eq(&actual, &expected, &format!("table {table}"))
}

#[rstest]
#[case::parquet_manifest("v2-checkpoints-parquet-with-sidecars")]
#[case::json_manifest("v2-checkpoints-json-with-sidecars")]
fn declarative_metadata_scans_sidecars_from_checkpoint_hint(
    #[case] table: &str,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = crate::utils::test_utils::load_test_table(table)?;
    let plan = snapshot
        .scan_builder()
        .build()?
        .declarative_metadata_scan_plan(engine.as_ref())?
        .expect("metadata plan");

    assert!(plan
        .nodes
        .iter()
        .all(|node| !matches!(&node.op, Operator::Load(_))));
    assert!(plan.nodes.iter().any(|node| {
        let Operator::ScanParquet(scan) = &node.op else {
            return false;
        };
        scan.files
            .iter()
            .any(|file| file.meta.location.path().contains("/_sidecars/"))
    }));
    Ok(())
}

#[rstest]
fn declarative_metadata_respects_output_options(
    #[values(
        StatsOptions::json_only(),
        StatsOptions::all_struct(),
        StatsOptions::struct_columns(vec![column_name!("id")]),
        StatsOptions::all(),
        StatsOptions::none()
    )]
    stats: StatsOptions,
    #[values(
        PartitionValuesOptions::string_map_only(),
        PartitionValuesOptions::with_struct()
    )]
    partitions: PartitionValuesOptions,
    #[values(None, Some(column_expr!("value").gt(Expr::literal("value_2"))))] predicate: Option<
        Pred,
    >,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) =
        crate::utils::test_utils::load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
    let struct_stats = stats.struct_stats.clone();
    let json_only = stats.synthesize_json && matches!(&struct_stats, StructStats::None);
    let no_stats = !stats.synthesize_json && matches!(&struct_stats, StructStats::None);
    // Imperative metadata includes structured stats needed internally by its predicate. Those
    // fields are not caller-requested output, so compare common fields and validate the
    // declarative stats schema separately.
    let compare_without_stats = predicate.is_some()
        && (matches!(stats.struct_stats, StructStats::Columns(_))
            || matches!(stats.struct_stats, StructStats::None));
    let expected_stats = if no_stats && predicate.is_some() {
        StatsOptions::json_only()
    } else {
        stats.clone()
    };
    let expected_builder = snapshot
        .clone()
        .scan_builder()
        .with_stats(expected_stats)
        .with_partition_values(partitions.clone());
    let expected_builder = match &predicate {
        Some(predicate) => expected_builder.with_predicate(Arc::new(predicate.clone())),
        None => expected_builder,
    };
    let expected = imperative_metadata(expected_builder.build()?, engine.as_ref())?;
    let builder = snapshot
        .scan_builder()
        .with_stats(stats)
        .with_partition_values(partitions);
    let builder = match predicate {
        Some(predicate) => builder.with_predicate(Arc::new(predicate)),
        None => builder,
    };
    let scan = builder.build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;
    assert_stats_output_schema(&actual, &struct_stats);
    if json_only {
        assert_json_only_output_schema(&actual);
    }
    if compare_without_stats {
        assert_metadata_eq(
            &without_stats(&actual, !no_stats)?,
            &without_stats(&expected, !no_stats)?,
            "metadata output options",
        )
    } else {
        assert_metadata_eq(&actual, &expected, "metadata output options")
    }
}

#[rstest]
#[case::gt_three(column_expr!("id").gt(Expr::literal(3i64)), 2)]
#[case::eq_two(column_expr!("id").eq(Expr::literal(2i64)), 1)]
#[case::le_zero(column_expr!("id").le(Expr::literal(0i64)), 0)]
fn declarative_metadata_data_skipping(
    #[values(
        "v1-multi-part-struct-stats-only",
        "v2-parquet-sidecars-struct-stats-only",
        "v2-json-sidecars-struct-stats-only"
    )]
    table: &str,
    #[case] predicate: Pred,
    #[case] expected_count: usize,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = crate::utils::test_utils::load_test_table(table)?;
    let predicate = Arc::new(predicate);
    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        engine.as_ref(),
    )?;
    assert_eq!(metadata_row_count(&expected), expected_count);

    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;

    assert_metadata_eq(&actual, &expected, &format!("table {table}"))
}

#[rstest]
#[case::part_zero(column_expr!("part").eq(Expr::literal(0i32)), 1)]
#[case::part_one(column_expr!("part").eq(Expr::literal(1i32)), 2)]
#[case::missing_part(column_expr!("part").eq(Expr::literal(4i32)), 0)]
fn declarative_metadata_reconstructs_partition_values_for_pruning(
    #[case] predicate: Pred,
    #[case] expected_count: usize,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) =
        crate::utils::test_utils::load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
    let predicate = Arc::new(predicate);
    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        engine.as_ref(),
    )?;
    assert_eq!(metadata_row_count(&expected), expected_count);

    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;

    assert_metadata_eq(&actual, &expected, "partition pruning")
}

#[test]
fn declarative_metadata_reconstructs_well_formed_stats_and_partitions() -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) =
        crate::utils::test_utils::load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let plan = scan
        .declarative_metadata_scan_plan(engine.as_ref())?
        .expect("metadata plan");
    let batches = engine
        .plan_executor()
        .execute_op(PlanOperation::QueryPlan(plan))?
        .into_data()?;

    let mut projected = vec![];
    for batch in batches {
        let batch = batch?.try_into_record_batch()?;
        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        projected.push(RecordBatch::try_from_iter([
            (
                "stats",
                add.column_by_name(STATS_PARSED)
                    .expect("add.stats_parsed")
                    .clone(),
            ),
            (
                "partitionValues",
                add.column_by_name(PARTITION_VALUES_PARSED)
                    .expect("add.partitionValues_parsed")
                    .clone(),
            ),
        ])?);
    }

    let formatted = pretty_format_batches(&projected)?.to_string();
    let mut actual_rows: Vec<_> = formatted
        .lines()
        .filter(|line| line.starts_with("| {numRecords:"))
        .collect();
    actual_rows.sort_unstable();
    let expected_rows = [
        expected_stats_row(1, 1),
        expected_stats_row(2, 2),
        expected_stats_row(3, 0),
        expected_stats_row(4, 1),
        expected_stats_row(5, 2),
    ];
    assert_eq!(actual_rows, expected_rows, "{formatted}");
    assert!(formatted.contains("| stats"));
    assert!(formatted.contains("| partitionValues |"));
    Ok(())
}

fn expected_stats_row(id: i64, partition: i32) -> String {
    format!(
        "| {{numRecords: 1, nullCount: {{id: 0, value: 0}}, minValues: \
         {{id: {id}, value: value_{id}}}, maxValues: {{id: {id}, value: value_{id}}}, \
         tightBounds: true}} | {{part: {partition}}}       |"
    )
}

#[test]
fn declarative_metadata_reconciles_checkpoint_with_later_commits() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(4).with_checkpoint_at([2]))
        .build()
        .expect("build checkpoint-plus-commits table");
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;

    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        &engine,
    )?;
    assert_eq!(metadata_row_count(&expected), 4);

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, &engine)?;

    assert_metadata_eq(&actual, &expected, "checkpoint with later commits")
}

#[rstest]
fn declarative_metadata_prunes_across_v1_log_states(
    #[values(
        LogState::with_latest_version(4),
        LogState::with_latest_version(4).with_checkpoint_at([4]),
        LogState::with_latest_version(4).with_checkpoint_at([2])
    )]
    log_state: LogState,
    #[values(
        (column_expr!("value").gt(Expr::literal(2500i32)), 2),
        (
            column_expr!("part_string").eq(Expr::literal("part_2000")),
            1
        )
    )]
    pruning: (Pred, usize),
) -> DeltaResult<()> {
    assert_declarative_metadata_matches_imperative(
        log_state,
        FeatureSet::new(),
        pruning.0,
        pruning.1,
    )
}

#[rstest]
fn declarative_metadata_partition_prunes_v2_checkpoints(
    #[values(2, 4)] checkpoint_version: u64,
) -> DeltaResult<()> {
    let log_state = LogState::with_latest_version(4)
        .with_checkpoint_at([checkpoint_version])
        .with_sidecars_if_enabled(None);
    assert_declarative_metadata_matches_imperative(
        log_state,
        FeatureSet::new().v2_checkpoint(),
        column_expr!("part_string").eq(Expr::literal("part_2000")),
        1,
    )
}

fn assert_declarative_metadata_matches_imperative(
    log_state: LogState,
    features: FeatureSet,
    predicate: Pred,
    expected_count: usize,
) -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(log_state)
        .with_features(features)
        .with_data_layout(DataLayoutConfig::PartitionedAllTypes)
        .build()
        .expect("build partitioned table");
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let predicate = Arc::new(predicate);

    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        &engine,
    )?;
    assert_eq!(
        metadata_row_count(&expected),
        expected_count,
        "{}",
        table.description()
    );
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, &engine)?;

    assert_metadata_eq(&actual, &expected, table.description())
}
