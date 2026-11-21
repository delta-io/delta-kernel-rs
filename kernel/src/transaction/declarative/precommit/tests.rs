use StagedAction::{Add, AddWithStats, Remove};
use rstest::rstest;

use super::*;
use crate::EngineData;
use crate::arrow::compute::concat_batches;
use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::expressions::{Scalar, col, lit};
use crate::plans::{Operation, PlanExecutor, PlanExecutorExt as _, PlanResult, ScopedPlanExecutor};
use crate::schema::{SchemaStructPatchBuilder, schema_ref};
use crate::transaction::declarative::test_utils::{
    STATS_SCHEMA, StagedAction, Stats, fields_input, make_ref,
};

// Control batch boundaries and late errors while exercising typed execution and validation.
struct StreamPlanExecutor(std::sync::Mutex<Option<PlanResult>>);

impl PlanExecutor for StreamPlanExecutor {
    fn execute_op(&self, _op: Operation) -> KernelResult<PlanResult> {
        Ok(self.0.lock().unwrap().take().unwrap())
    }
}

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
    let plan = build_plan(&adds, &removes, &checks, &STATS_SCHEMA)?;
    let summary = executor.execute_plan_into(plan)?;
    validate_result(summary, &checks)
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
#[case::ignores_invalid_adds(&[Add(None, None, None)], None)]
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
#[case::negative(&[AddWithStats("part", stats(Some(-1), None, None, None))],
    Some("AddFile for 'part' has negative numRecords -1"))]
#[case::zero(&[AddWithStats("part", stats(Some(0), None, None, None))], None)]
#[case::positive(&[AddWithStats("part", stats(Some(1), None, None, None))], None)]
#[case::null_parent(&[AddWithStats("part", Stats {
    null_parent: true, ..stats(Some(1), Some(0), Some(1), Some(2))
})], Some("numRecords"))]
#[case::ignores_removes(&[Remove(Some("part"), Some(1))], None)]
#[case::mixed(&[Remove(Some("removed"), Some(1)),
    AddWithStats("part", stats(None, None, None, None))], Some("numRecords"))]
fn record_count_is_present_and_non_negative(
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
    let check = ClusteringStatsCheck::new([(column, DataType::LONG)])?;
    assert_eq!(check.name(), "add_clustering_stats");
    assert_result(run_checks(vec![Box::new(check)], actions), error)
}

#[rstest]
#[case::present(false, false, None)]
#[case::first_column_missing(true, false, Some("physical.id"))]
#[case::nested_column_missing(false, true, Some("physical.tag"))]
#[case::both_missing(true, true, Some("physical.id"))]
fn clustering_statistics_identify_failing_physical_column(
    #[case] missing_id_min: bool,
    #[case] missing_tag_min: bool,
    #[case] expected_column: Option<&str>,
    #[values(false, true)] extra_and_reordered_fields: bool,
) -> KernelResult<()> {
    let stats_schema = schema_ref! {
        nullable "numRecords": LONG,
        nullable "nullCount": {
            nullable "physical.id": LONG,
            nullable "parent": { nullable "physical.tag": LONG },
        },
        nullable "minValues": {
            nullable "physical.id": LONG,
            nullable "parent": { nullable "physical.tag": STRING },
        },
        nullable "maxValues": {
            nullable "physical.id": LONG,
            nullable "parent": { nullable "physical.tag": STRING },
        },
    };
    let source_schema = schema_ref! {
        nullable "path": STRING,
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "stats": (stats_schema.clone()),
    };
    let input = PlanBuilder::values(
        schema_ref! { nullable "id_min": LONG, nullable "tag_min": STRING },
        vec![vec![
            if missing_id_min {
                Scalar::Null(DataType::LONG)
            } else {
                1i64.into()
            },
            if missing_tag_min {
                Scalar::Null(DataType::STRING)
            } else {
                "a".into()
            },
        ]],
    )?
    .project(
        Expression::struct_from([
            lit("part"),
            lit(1i64),
            lit(1i64),
            Expression::struct_from([
                lit(3i64),
                Expression::struct_from([lit(0i64), Expression::struct_from([lit(0i64)])]),
                Expression::struct_from([
                    col!("id_min"),
                    Expression::struct_from([col!("tag_min")]),
                ]),
                Expression::struct_from([lit(4i64), Expression::struct_from([lit("z")])]),
            ]),
        ]),
        source_schema,
    )?;
    let input = if extra_and_reordered_fields {
        input.project_patch(|patch| {
            patch
                .drop("path")
                .append(
                    StructField::nullable("path", DataType::STRING),
                    col!("path"),
                )
                .prepend(
                    StructField::nullable("unused", DataType::STRING),
                    lit("ignored"),
                )
        })?
    } else {
        input
    };
    let executor = SyncPlanExecutor::default().get_scoped();
    let source = executor.execute_and_retain("adds", input.build()?)?;
    let checks: CheckList = vec![
        Box::new(AddFileFieldsCheck),
        Box::new(ClusteringStatsCheck::new([
            (ColumnName::new(["physical.id"]), DataType::LONG),
            (
                ColumnName::new(["parent", "physical.tag"]),
                DataType::STRING,
            ),
        ])?),
    ];
    let plan = build_plan(&[source], &[], &checks, &stats_schema)?;
    assert_eq!(plan.schema, *CheckResults::schema_ref());
    let summary = executor.execute_plan_into(plan)?;
    let result = validate_result(summary, &checks);
    if let Some(column) = expected_column {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("AddFile for 'part'"), "{error}");
        assert!(error.to_string().contains(column), "{error}");
        assert!(error.to_string().contains("[minValues]"), "{error}");
    } else {
        result?;
    }
    Ok(())
}

#[test]
fn common_input_schema_keeps_table_statistics_independently_of_checks() -> KernelResult<()> {
    let stats_schema = schema_ref! {
        nullable "numRecords": LONG,
        nullable "minValues": { nullable "unused_by_checks": DOUBLE },
    };
    let input = build_input(&[], &[], &stats_schema)?.build()?;
    assert_eq!(
        input
            .schema
            .field_at(&ColumnName::new(["add", "stats"]))?
            .data_type(),
        &DataType::from(stats_schema.clone()),
    );
    for check in [
        Box::new(AddFileFieldsCheck) as Box<dyn DynStagedDataCheck>,
        Box::new(AppendOnlyCheck),
    ] {
        let plan = build_plan(&[], &[], &[check], &stats_schema)?;
        assert_eq!(plan.schema, *CheckResults::schema_ref());
    }
    Ok(())
}

#[rstest]
#[case::add_missing_path(true, SchemaStructPatchBuilder::new().drop("path"), "path")]
#[case::add_missing_size(true, SchemaStructPatchBuilder::new().drop("size"), "size")]
#[case::add_missing_time(true, SchemaStructPatchBuilder::new().drop("modificationTime"),
    "modificationTime")]
#[case::add_missing_stats(true, SchemaStructPatchBuilder::new().drop("stats"), "stats")]
#[case::add_missing_record_count(true,
    SchemaStructPatchBuilder::new().drop_at(["stats"], "numRecords"), "numRecords")]
#[case::add_missing_bound(true,
    SchemaStructPatchBuilder::new().drop_at(["stats", "minValues"], "physical.id"), "physical.id")]
#[case::add_wrong_size_type(true, SchemaStructPatchBuilder::new()
    .replace("size", StructField::nullable("size", DataType::STRING)), "Unexpected input type at size")]
#[case::add_wrong_stat_type(true, SchemaStructPatchBuilder::new()
    .replace_at(["stats"], "numRecords", StructField::nullable("numRecords", DataType::STRING)),
    "Unexpected input type at stats.numRecords")]
#[case::remove_missing_path(false, SchemaStructPatchBuilder::new().drop("path"), "path")]
#[case::remove_missing_size(false, SchemaStructPatchBuilder::new().drop("size"), "size")]
#[case::remove_wrong_size_type(false, SchemaStructPatchBuilder::new()
    .replace("size", StructField::nullable("size", DataType::STRING)), "Unexpected input type at size")]
fn rejects_missing_or_mistyped_input_columns(
    #[case] is_add: bool,
    #[case] patch: SchemaStructPatchBuilder,
    #[case] expected_error: &str,
) -> KernelResult<()> {
    let schema = if is_add {
        fields_input(Some("part"), Some(1), Some(1))?
            .build()?
            .schema
    } else {
        schema_ref! { nullable "path": STRING, nullable "size": LONG }
    };
    let source = RelationRef::new("invalid", Arc::new(patch.build(&schema)?));
    let checks: CheckList = vec![
        Box::new(AddFileFieldsCheck),
        Box::new(RemoveFileFieldsCheck),
    ];
    let (adds, removes) = if is_add {
        (vec![source], vec![])
    } else {
        (vec![], vec![source])
    };
    let error = build_plan(&adds, &removes, &checks, &STATS_SCHEMA).unwrap_err();
    assert!(error.to_string().contains(expected_error), "{error}");
    Ok(())
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
    let plan = build_plan(&sources, &[], &checks, &STATS_SCHEMA)?;
    let summary = executor.execute_plan_into(plan)?;
    validate_result(summary, &checks)
}

#[rstest]
#[case::no_batches(&[], false, Some("expected exactly one row, found 0 rows"))]
#[case::empty_batch(&[0], false, Some("expected exactly one row, found 0 rows"))]
#[case::one_row(&[1], false, None)]
#[case::leading_empty_batch(&[0, 1], false, None)]
#[case::trailing_empty_batch(&[1, 0], false, None)]
#[case::surrounding_empty_batches(&[0, 1, 0], false, None)]
#[case::multiple_rows_in_batch(&[2], false,
    Some("expected exactly one row, found at least 2 rows"))]
#[case::multiple_rows_across_batches(&[1, 1], false,
    Some("expected exactly one row, found at least 2 rows"))]
#[case::late_stream_error(&[1], true, Some("late stream error"))]
#[case::late_stream_error_after_empty(&[1, 0], true, Some("late stream error"))]
#[case::late_stream_error_after_multiple_rows(&[1, 1], true,
    Some("expected exactly one row, found at least 2 rows"))]
#[case::first_stream_error(&[], true, Some("late stream error"))]
fn summary_stream_cardinality_and_errors(
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
    let plan = build_plan(&[source], &[], &checks, &STATS_SCHEMA)?;
    let template = executor
        .execute_plan(plan.clone())?
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
    let executor = StreamPlanExecutor(std::sync::Mutex::new(Some(PlanResult::Data(Box::new(
        stream.into_iter(),
    )))));
    let result = executor
        .execute_plan_into::<CheckResults>(plan)
        .and_then(|summary| validate_result(summary, &checks));
    let expected_error = expected_error.or(invalid_size.then_some("negative size -1"));
    if let Some(message) = expected_error {
        let error = result.unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    } else {
        result?;
    }
    Ok(())
}

#[rstest]
#[case::add_first(false, "AddFile for 'added' has negative size -1")]
#[case::remove_first(true, "RemoveFile for 'removed' has negative size -1")]
fn check_priority_follows_check_list(
    #[case] reverse: bool,
    #[case] expected_error: &str,
) -> KernelResult<()> {
    let mut checks: CheckList = vec![
        Box::new(AddFileFieldsCheck),
        Box::new(RemoveFileFieldsCheck),
    ];
    if reverse {
        checks.reverse();
    }
    assert_result(
        run_checks(
            checks,
            &[
                Add(Some("added"), Some(-1), Some(1)),
                Remove(Some("removed"), Some(-1)),
            ],
        ),
        Some(expected_error),
    )
}
