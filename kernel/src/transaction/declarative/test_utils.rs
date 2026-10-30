use crate::KernelResult;
use crate::arrow::array::RecordBatch;
use crate::engine::arrow_data::{EngineDataArrowExt, fix_nested_null_masks};
use crate::engine::sync::plan::ScopedSyncPlanExecutor;
use crate::expressions::{Expression, Scalar, StructData, col, lit};
use crate::plans::{Operation, PlanBuilder, PlanExecutor, RelationRef};
use crate::schema::{DataType, StructField, schema_ref};

/// Metadata actions accepted by the shared staged-relation fixtures.
#[derive(Debug)]
pub(super) enum StagedAction {
    Add(Option<&'static str>, Option<i64>, Option<i64>),
    Remove(Option<&'static str>, Option<i64>),
    AddWithStats(&'static str, Stats),
}

/// Statistics for the physical column named `physical.id`; `null_parent` masks the stats struct.
#[derive(Debug, Default)]
pub(super) struct Stats {
    pub num_records: Option<i64>,
    pub null_count: Option<i64>,
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub null_parent: bool,
}

/// Materializes `action` in `executor` and returns its retained relation.
pub(super) fn make_ref(
    executor: &ScopedSyncPlanExecutor,
    action: &StagedAction,
) -> KernelResult<RelationRef> {
    let (name, input) = match action {
        StagedAction::Add(path, size, modified) => ("adds", fields_input(*path, *size, *modified)?),
        StagedAction::Remove(path, size) => (
            "removes",
            PlanBuilder::values(
                schema_ref! { nullable "path": STRING, nullable "size": LONG },
                vec![vec![string(*path), long(*size)]],
            )?,
        ),
        StagedAction::AddWithStats(path, stats) => {
            let stats_schema = schema_ref! {
                nullable "numRecords": LONG,
                nullable "nullCount": { nullable "physical.id": LONG },
                nullable "minValues": { nullable "physical.id": LONG },
                nullable "maxValues": { nullable "physical.id": LONG },
            };
            let leaf = |v| -> KernelResult<Scalar> {
                Ok(Scalar::Struct(StructData::try_new(
                    vec![StructField::nullable("physical.id", DataType::LONG)],
                    vec![long(v)],
                )?))
            };
            let values = Scalar::Struct(StructData::try_new(
                stats_schema.fields().cloned().collect(),
                vec![
                    long(stats.num_records),
                    leaf(stats.null_count)?,
                    leaf(stats.min)?,
                    leaf(stats.max)?,
                ],
            )?);
            let schema = schema_ref! {
                nullable "path": STRING, nullable "size": LONG, nullable "modificationTime": LONG,
                nullable "stats": (stats_schema),
            };
            let mut input = PlanBuilder::values(
                schema.clone(),
                vec![vec![(*path).into(), 1i64.into(), 1i64.into(), values]],
            )?;
            if stats.null_parent {
                // Exercise populated child buffers under a null parent.
                input = input.project(
                    Expression::struct_from([
                        col!("path"),
                        col!("size"),
                        col!("modificationTime"),
                        Expression::struct_with_nullability_from(
                            [
                                col!("stats.numRecords"),
                                col!("stats.nullCount"),
                                col!("stats.minValues"),
                                col!("stats.maxValues"),
                            ],
                            lit(false),
                        ),
                    ]),
                    schema,
                )?;
            }
            ("adds", input)
        }
    };
    let plan = input.build()?;
    let schema = plan.schema.clone();
    let mut batches = executor
        .execute_op(Operation::QueryPlan(plan))?
        .into_data()?;
    let batch = batches.next().unwrap()?;
    assert!(batches.next().is_none());
    // Arrow leaf visitors require parent validity to be propagated to child buffers.
    let batch = RecordBatch::from(fix_nested_null_masks(batch.try_into_record_batch()?.into()));
    executor.create_ref(name, schema, batch)
}

/// Builds a single metadata row with the supplied optional field values.
pub(super) fn fields_input(
    path: Option<&str>,
    size: Option<i64>,
    modified: Option<i64>,
) -> KernelResult<PlanBuilder> {
    PlanBuilder::values(
        schema_ref! {
            nullable "path": STRING, nullable "size": LONG, nullable "modificationTime": LONG,
        },
        vec![vec![string(path), long(size), long(modified)]],
    )
}

fn string(value: Option<&str>) -> Scalar {
    value
        .map(|v| Scalar::String(v.into()))
        .unwrap_or(Scalar::Null(DataType::STRING))
}

fn long(value: Option<i64>) -> Scalar {
    value
        .map(Scalar::Long)
        .unwrap_or(Scalar::Null(DataType::LONG))
}
