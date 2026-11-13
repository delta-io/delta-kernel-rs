use StagedAction::{Add, AddWithStats, Remove};
use rstest::rstest;

use super::*;
use crate::arrow::compute::concat_batches;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::plans::{Operation, PlanExecutor, ScopedPlanExecutor};
use crate::transaction::declarative::test_utils::{StagedAction, Stats, fields_input, make_ref};

fn run_checks(checks: CheckList, actions: &[StagedAction]) -> KernelResult<()> {
    let executor = SyncPlanExecutor::default().get_scoped();
    let mut adds = Vec::new();
    let mut removes = Vec::new();
    for action in actions {
        let relation = make_ref(&executor, action)?;
        match action {
            Add(..) | AddWithStats(..) => adds.push(relation),
            Remove(..) => removes.push(relation),
        }
    }
    let plan = build_plan(&adds, &removes, &checks)?;
    let results = executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?;
    validate_result(results, &checks)
}

fn assert_result(result: KernelResult<()>, expected_error: Option<&str>) -> KernelResult<()> {
    if let Some(message) = expected_error {
        let error = result.unwrap_err();
        assert!(
            matches!(error, KernelError::InvalidTransactionState(_)),
            "{error}"
        );
        assert!(error.to_string().contains(message), "{error}");
    } else {
        result?;
    }
    Ok(())
}

#[rstest]
#[case::empty(&[], None)]
#[case::valid(&[Add(Some("part"), Some(0), Some(-1))], None)]
#[case::missing_path(&[Add(None, Some(1), Some(1))], Some("missing required field 'path'"))]
#[case::all_null(&[Add(None, None, None)], Some("missing required field 'path'"))]
#[case::empty_path(&[Add(Some(""), Some(1), Some(1))], Some("empty path"))]
#[case::missing_size(&[Add(Some("part"), None, Some(1))], Some("missing required field 'size'"))]
#[case::negative_size(&[Add(Some("part"), Some(-1), Some(1))], Some("negative size -1"))]
#[case::missing_modified(&[Add(Some("part"), Some(1), None)],
    Some("missing required field 'modificationTime'"))]
#[case::ignores_removes(&[Remove(None, None)], None)]
#[case::mixed(&[Remove(Some("removed"), Some(1)), Add(Some("part"), None, Some(1))],
    Some("missing required field 'size'"))]
#[case::first_failure(&[
    Add(Some("good"), Some(1), Some(1)), Add(Some("missing-size"), None, Some(1)),
    Add(Some("negative-size"), Some(-7), Some(1)),
], Some("AddFile for 'missing-size' is missing required field 'size'"))]
#[case::first_failure_all_null(&[
    Add(Some("good"), Some(1), Some(1)), Add(None, None, None),
    Add(Some("negative-size"), Some(-7), Some(1)),
], Some("AddFile with missing path is missing required field 'path'"))]
fn add_file_fields(
    #[case] actions: &[StagedAction],
    #[case] error: Option<&str>,
) -> KernelResult<()> {
    assert_result(
        run_checks(vec![Box::new(AddFileFieldsCheck)], actions),
        error,
    )
}

#[rstest]
#[case::empty(&[], None)]
#[case::valid(&[Remove(Some("part"), Some(0))], None)]
#[case::missing_path(&[Remove(None, Some(1))], Some("missing required field 'path'"))]
#[case::all_null(&[Remove(None, None)], Some("missing required field 'path'"))]
#[case::empty_path(&[Remove(Some(""), Some(1))], Some("empty path"))]
#[case::missing_size(&[Remove(Some("part"), None)], Some("missing required field 'size'"))]
#[case::negative_size(&[Remove(Some("part"), Some(-1))], Some("negative size -1"))]
#[case::ignores_adds(&[Add(None, None, None)], None)]
#[case::mixed(&[Add(Some("added"), Some(1), Some(1)), Remove(Some("part"), None)],
    Some("missing required field 'size'"))]
fn remove_file_fields(
    #[case] actions: &[StagedAction],
    #[case] error: Option<&str>,
) -> KernelResult<()> {
    assert_result(
        run_checks(vec![Box::new(RemoveFileFieldsCheck)], actions),
        error,
    )
}

#[rstest]
#[case::empty(&[], None)]
#[case::adds(&[Add(Some("part"), Some(1), Some(1))], None)]
#[case::removes(&[Remove(Some("part"), Some(0))], Some("cannot remove file for 'part'"))]
#[case::missing_path(&[Remove(None, None)], Some("cannot remove file with missing path"))]
#[case::mixed(&[Add(Some("added"), Some(1), Some(1)), Remove(Some("part"), Some(1))],
    Some("cannot remove file for 'part'"))]
fn append_only(#[case] actions: &[StagedAction], #[case] error: Option<&str>) -> KernelResult<()> {
    assert_result(run_checks(vec![Box::new(AppendOnlyCheck)], actions), error)
}

const fn stats(nr: Option<i64>, nc: Option<i64>, min: Option<i64>, max: Option<i64>) -> Stats {
    Stats {
        num_records: nr,
        null_count: nc,
        min,
        max,
        null_parent: false,
    }
}

#[rstest]
#[case::empty(&[], None)]
#[case::missing(&[AddWithStats("part", stats(None, None, None, None))], Some("numRecords"))]
#[case::negative(&[AddWithStats("part", stats(Some(-1), None, None, None))], None)]
#[case::zero(&[AddWithStats("part", stats(Some(0), None, None, None))], None)]
#[case::null_parent(&[AddWithStats("part", Stats {
    null_parent: true, ..stats(Some(1), Some(0), Some(1), Some(2))
})], Some("numRecords"))]
#[case::ignores_removes(&[Remove(Some("part"), Some(1))], None)]
#[case::mixed(&[Remove(Some("removed"), Some(1)),
    AddWithStats("part", stats(None, None, None, None))], Some("numRecords"))]
fn record_count_presence(
    #[case] actions: &[StagedAction],
    #[case] error: Option<&str>,
) -> KernelResult<()> {
    assert_result(run_checks(vec![Box::new(NumRecordsCheck)], actions), error)
}

#[rstest]
#[case::empty(&[], None)]
#[case::missing(&[AddWithStats("part", stats(None, None, None, None))],
    Some("[nullCount, minValues, maxValues]"))]
#[case::empty_file(&[AddWithStats("part", stats(Some(0), Some(0), None, None))], None)]
#[case::all_null(&[AddWithStats("part", stats(Some(3), Some(3), None, None))], None)]
#[case::no_record_count(&[AddWithStats("part", stats(None, Some(1), Some(2), Some(4)))], None)]
#[case::no_null_count(&[AddWithStats("part", stats(Some(3), None, Some(2), Some(4)))],
    Some("[nullCount]"))]
#[case::no_min(&[AddWithStats("part", stats(Some(3), Some(1), None, Some(4)))],
    Some("[minValues]"))]
#[case::no_max(&[AddWithStats("part", stats(Some(3), Some(1), Some(2), None))],
    Some("[maxValues]"))]
#[case::present(&[AddWithStats("part", stats(Some(3), Some(1), Some(2), Some(5)))], None)]
#[case::null_parent(&[AddWithStats("part", Stats {
    null_parent: true, ..stats(Some(1), Some(0), Some(1), Some(2))
})], Some("[nullCount, minValues, maxValues]"))]
#[case::ignores_removes(&[Remove(Some("part"), Some(1))], None)]
#[case::mixed(&[Remove(Some("removed"), Some(1)),
    AddWithStats("part", stats(Some(3), Some(1), None, Some(4)))], Some("[minValues]"))]
fn clustering_statistics(
    #[case] actions: &[StagedAction],
    #[case] error: Option<&str>,
) -> KernelResult<()> {
    let column = ColumnName::new(["physical.id"]);
    let check = ClusteringStatsCheck::new(column, DataType::LONG)?;
    assert_eq!(check.name(), "add.clustering_stats:[\"physical.id\"]");
    assert_result(run_checks(vec![Box::new(check)], actions), error)
}

#[rstest]
fn empty_inputs(#[values(false, true)] with_source: bool) -> KernelResult<()> {
    let executor = SyncPlanExecutor::default().get_scoped();
    let checks: CheckList = vec![
        Box::new(AddFileFieldsCheck),
        Box::new(RemoveFileFieldsCheck),
    ];
    let sources = if with_source {
        let schema = fields_input(None, None, None)?.build()?.schema;
        vec![executor.execute_and_retain("empty", PlanBuilder::values(schema, vec![])?.build()?)?]
    } else {
        vec![]
    };
    let plan = build_plan(&sources, &[], &checks)?;
    let results = executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?;
    validate_result(results, &checks)
}

#[rstest]
#[case::no_batches(&[], false, Some("Expected exactly one pre-commit validation row, but got 0"))]
#[case::empty_batch(&[0], false, Some("Expected exactly one pre-commit validation row, but got 0"))]
#[case::one_row(&[1], false, None)]
#[case::leading_empty_batch(&[0, 1], false, None)]
#[case::trailing_empty_batch(&[1, 0], false, None)]
#[case::surrounding_empty_batches(&[0, 1, 0], false, None)]
#[case::multiple_rows_in_batch(&[2], false,
    Some("Expected exactly one pre-commit validation row, but got 2"))]
#[case::multiple_rows_across_batches(&[1, 1], false,
    Some("Expected exactly one pre-commit validation row, but got 2"))]
#[case::late_stream_error(&[1], true, Some("late stream error"))]
#[case::late_stream_error_after_empty(&[1, 0], true, Some("late stream error"))]
#[case::late_stream_error_after_multiple_rows(&[1, 1], true, Some("late stream error"))]
#[case::first_stream_error(&[], true, Some("late stream error"))]
fn summary_stream_exhaustion(
    #[case] batch_sizes: &[usize],
    #[case] late_error: bool,
    #[case] expected_error: Option<&str>,
    #[values(false, true)] invalid_size: bool,
) -> KernelResult<()> {
    let checks: CheckList = vec![Box::new(AddFileFieldsCheck)];
    let executor = SyncPlanExecutor::default().get_scoped();
    let source = executor.execute_and_retain(
        "adds",
        fields_input(
            Some("part"),
            Some(if invalid_size { -1 } else { 1 }),
            Some(1),
        )?
        .build()?,
    )?;
    let plan = build_plan(&[source], &[], &checks)?;
    let template = executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?
        .next()
        .unwrap()?
        .try_into_record_batch()?;
    let mut stream: Vec<KernelResult<Box<dyn EngineData>>> = batch_sizes
        .iter()
        .map(|size| {
            let batch = concat_batches(&template.schema(), std::iter::repeat_n(&template, *size))?;
            Ok(Box::new(ArrowEngineData::from(batch)) as Box<dyn EngineData>)
        })
        .collect();
    if late_error {
        stream.push(Err(KernelError::generic("late stream error")));
    }
    let result = validate_result(stream.into_iter(), &checks);
    let expected_error = expected_error.or(invalid_size.then_some("negative size -1"));
    if let Some(message) = expected_error {
        let error = result.unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    } else {
        result?;
    }
    Ok(())
}
