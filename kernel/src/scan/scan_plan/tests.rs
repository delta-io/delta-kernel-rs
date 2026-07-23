use std::sync::Arc;

use ::test_utils::table_builder::{LogState, TestTableBuilder};
use rstest::rstest;

use super::*;
use crate::arrow::array::{Array, StringArray, StructArray};
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty::pretty_format_batches;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::expressions::{column_expr, Expression as Expr, Predicate as Pred};
use crate::plans::Operation as PlanOperation;
use crate::scan::state::ScanFile;
use crate::scan::{PartitionValuesOptions, Scan, StatsOptions};
use crate::{DeltaResult, Engine, Snapshot};

fn imperative_metadata_paths(scan: Scan, engine: &dyn Engine) -> DeltaResult<Vec<String>> {
    fn collect_path(paths: &mut Vec<String>, scan_file: ScanFile) {
        paths.push(scan_file.path.to_string());
    }

    let mut paths = vec![];
    for scan_metadata in scan.scan_metadata(engine)? {
        paths = scan_metadata?.visit_scan_files(paths, collect_path)?;
    }
    paths.sort();
    Ok(paths)
}

fn declarative_metadata_paths(scan: &Scan, engine: &dyn Engine) -> DeltaResult<Vec<String>> {
    let Some(plan) = scan.declarative_metadata_scan_plan(engine)? else {
        return Ok(vec![]);
    };
    let batches = engine
        .plan_executor()
        .execute_op(PlanOperation::QueryPlan(plan))?
        .into_data()?;

    let mut paths = vec![];
    for batch in batches {
        let batch = batch?.try_into_record_batch()?;
        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        let path_col = add
            .column_by_name("path")
            .expect("add.path")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path string");
        paths.extend((0..path_col.len()).map(|i| path_col.value(i).to_string()));
    }
    paths.sort();
    Ok(paths)
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

    let imperative_builder = snapshot.clone().scan_builder();
    let imperative_builder = match &predicate {
        Some(predicate) => imperative_builder.with_predicate(predicate.clone()),
        None => imperative_builder,
    };
    let expected = imperative_metadata_paths(imperative_builder.build()?, engine.as_ref())?;

    let declarative_builder = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct());
    let declarative_builder = match predicate {
        Some(predicate) => declarative_builder.with_predicate(predicate),
        None => declarative_builder,
    };
    let scan = declarative_builder.build()?;
    let actual = declarative_metadata_paths(&scan, engine.as_ref())?;

    assert_eq!(actual, expected, "table {table}");
    Ok(())
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
    let expected = imperative_metadata_paths(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .build()?,
        engine.as_ref(),
    )?;
    assert_eq!(expected.len(), expected_count);

    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .build()?;
    let actual = declarative_metadata_paths(&scan, engine.as_ref())?;

    assert_eq!(actual, expected, "table {table}");
    Ok(())
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
    let expected = imperative_metadata_paths(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        engine.as_ref(),
    )?;
    assert_eq!(expected.len(), expected_count);

    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata_paths(&scan, engine.as_ref())?;

    assert_eq!(actual, expected);
    Ok(())
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
                add.column_by_name("stats").expect("add.stats").clone(),
            ),
            (
                "partitionValues",
                add.column_by_name("partitionValues")
                    .expect("add.partitionValues")
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

    let expected = imperative_metadata_paths(snapshot.clone().scan_builder().build()?, &engine)?;
    assert_eq!(expected.len(), 4);

    let scan = snapshot.scan_builder().build()?;
    let actual = declarative_metadata_paths(&scan, &engine)?;

    assert_eq!(actual, expected);
    Ok(())
}
