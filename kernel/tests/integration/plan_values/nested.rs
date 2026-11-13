use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use delta_kernel::arrow::datatypes::Field;
use delta_kernel::delta_kernel_derive::{
    IntoStructData, ToSchema, TryFromPlanResult, TryFromStructData,
};
use delta_kernel::engine::sync::plan::SyncPlanExecutor;
use delta_kernel::expressions::ColumnName;
use delta_kernel::plans::PlanResultRow;
use delta_kernel::plans::ir::nodes::Values;
use delta_kernel::{KernelError, KernelResult, PlanExecutor, PlanResult};
use rstest::rstest;

use super::{Containers, TestRow as FlatRow, convert_rows, data, plan, row_batch};

#[derive(Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData)]
struct NestedType {
    row: FlatRow,
    containers: Containers,
}

#[derive(
    Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData, TryFromPlanResult,
)]
struct TestRow {
    before: i32,
    nested: NestedType,
    after: Option<String>,
}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct SimpleNestedRow {
    nested: FlatRow,
}

#[derive(
    Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData, TryFromPlanResult,
)]
struct ContainerRow {
    nested: Containers,
}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct EmptyRow {}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct EmptyNested {
    nested: EmptyRow,
}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct NullableChild {
    child: Option<FlatRow>,
}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct NullableDescendant {
    nested: NullableChild,
}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct UnsupportedDescendant {
    nested: super::IntegerList,
}

fn nested_array(columns: Vec<(&str, ArrayRef)>) -> ArrayRef {
    Arc::new(StructArray::from(
        columns
            .into_iter()
            .map(|(name, array)| {
                (
                    Arc::new(Field::new(name, array.data_type().clone(), true)),
                    array,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn nested_batch(ids: Vec<Option<i32>>) -> RecordBatch {
    let labels = vec![None::<&str>; ids.len()];
    let nested = nested_array(vec![
        ("label", Arc::new(StringArray::from(labels)) as ArrayRef),
        (
            "ignored",
            Arc::new(Int32Array::from(vec![99; ids.len()])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
    ]);
    RecordBatch::try_from_iter([("nested", nested)]).unwrap()
}

#[rstest]
fn nested_structs_round_trip_through_executor(
    #[values(false, true)] populated: bool,
    #[values(false, true)] singleton: bool,
) -> KernelResult<()> {
    let expected: Vec<_> = (0..if singleton { 1 } else { 2 })
        .map(|id| TestRow {
            before: id,
            nested: NestedType {
                row: FlatRow {
                    id: id + 10,
                    label: populated.then(|| "nested".to_owned()),
                },
                containers: Containers {
                    tags: populated.then(|| vec!["tag".to_owned()]),
                    attrs: populated
                        .then(|| HashMap::from([("key".to_owned(), "value".to_owned())])),
                },
            },
            after: populated.then(|| "after".to_owned()),
        })
        .collect();
    let result = SyncPlanExecutor::default().execute_plan(plan(expected.clone())?)?;
    let actual = convert_rows::<TestRow>(result, singleton)?;
    assert_eq!(actual, expected);
    let (_, columns) = TestRow::schema_and_columns();
    assert_eq!(
        columns.as_ref().0,
        &[
            ColumnName::new(["before"]),
            ColumnName::new(["nested", "row", "id"]),
            ColumnName::new(["nested", "row", "label"]),
            ColumnName::new(["nested", "containers", "tags"]),
            ColumnName::new(["nested", "containers", "attrs"]),
            ColumnName::new(["after"]),
        ]
    );
    Ok(())
}

#[test]
fn nested_fields_match_names_across_batches_and_empty_results() -> KernelResult<()> {
    let actual: Vec<SimpleNestedRow> = data(vec![
        Ok(nested_batch(vec![])),
        Ok(nested_batch(vec![Some(7)])),
        Ok(nested_batch(vec![])),
        Ok(nested_batch(vec![Some(8)])),
    ])
    .try_into()?;
    let expected = [7, 8].map(|id| SimpleNestedRow {
        nested: FlatRow { id, label: None },
    });
    assert_eq!(actual, expected);
    let empty: Vec<SimpleNestedRow> = data(vec![]).try_into()?;
    assert!(empty.is_empty());
    Ok(())
}

#[rstest]
#[case::missing_child(vec![("nested", nested_array(vec![("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef)]))])]
#[case::wrong_child_type(vec![("nested", nested_array(vec![("id", Arc::new(StringArray::from(vec!["one"])) as ArrayRef), ("label", Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef)]))])]
#[case::required_child_null(vec![("nested", nested_array(vec![("id", Arc::new(Int32Array::from(vec![None::<i32>])) as ArrayRef), ("label", Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef)]))])]
#[case::missing_parent(vec![("other", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])]
#[case::wrong_parent_type(vec![("nested", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])]
fn invalid_nested_fields_are_rejected(#[case] columns: Vec<(&str, ArrayRef)>) -> KernelResult<()> {
    let actual: KernelResult<Vec<SimpleNestedRow>> =
        data(vec![Ok(RecordBatch::try_from_iter(columns)?)]).try_into();
    assert!(actual.is_err());
    Ok(())
}

#[test]
fn required_nested_struct_with_all_null_children_remains_present() -> KernelResult<()> {
    let expected = ContainerRow {
        nested: Containers {
            tags: None,
            attrs: None,
        },
    };
    let actual: ContainerRow = SyncPlanExecutor::default()
        .execute_plan(plan(vec![expected.clone()])?)?
        .try_into()?;
    assert_eq!(actual, expected);
    Ok(())
}

#[rstest]
#[case::empty_struct(PlanResult::into_values::<EmptyNested>)]
#[case::nullable_descendant(PlanResult::into_values::<NullableDescendant>)]
#[case::unsupported_descendant(PlanResult::into_values::<UnsupportedDescendant>)]
fn unsupported_nested_schemas_fail_before_polling_batches(
    #[case] convert: fn(PlanResult) -> KernelResult<Values>,
    #[values(0, 1, 2)] source: usize,
) {
    let batches = match source {
        0 => vec![],
        1 => vec![Ok(row_batch(vec![]))],
        _ => vec![Err(KernelError::generic("batch error must not be read"))],
    };
    let error = convert(data(batches)).unwrap_err();
    assert!(matches!(
        error.without_backtrace(),
        KernelError::Unsupported(_)
    ));
}

#[test]
fn empty_top_level_rows_remain_supported() -> KernelResult<()> {
    let rows: Vec<EmptyRow> = data(vec![Ok(row_batch(vec![Some(1), Some(2)]))]).try_into()?;
    assert_eq!(rows, vec![EmptyRow {}, EmptyRow {}]);
    Ok(())
}

#[rstest]
fn nested_rows_propagate_late_stream_errors(#[values(false, true)] singleton: bool) {
    let result = data(vec![
        Ok(nested_batch(vec![Some(1)])),
        Err(KernelError::generic("late nested error")),
    ]);
    let error = convert_rows::<SimpleNestedRow>(result, singleton).unwrap_err();
    assert!(
        matches!(error.without_backtrace(), KernelError::Generic(message) if message == "late nested error")
    );
}
