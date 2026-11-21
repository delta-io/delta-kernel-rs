use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use delta_kernel::arrow::array::{ArrayRef, Int32Array, RecordBatch, StructArray};
use delta_kernel::arrow::buffer::NullBuffer;
use delta_kernel::delta_kernel_derive::{
    IntoStructData, ToSchema as DeriveToSchema, TryFromStructData,
};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::StructData;
use delta_kernel::schema::ToSchema;
use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;
use test_utils::IntoArray;

use super::{Containers, TestRow as FlatRow, data, execute_rows, row_batch};

result_row! { struct NestedType { row: FlatRow, containers: Containers } }

result_row! { pub(super) struct TestRow {
    before: i32, nested: NestedType, after: Option<String>,
} }
result_row! { pub(super) struct SimpleNestedRow { pub(super) nested: FlatRow } }
result_row! { struct RequiredParent { before: i32, nested: NullableChild, after: i32 } }
result_row! { struct OptionalParent { before: i32, nested: Option<NullableChild>, after: i32 } }
result_row! { #[derive(Default)] pub(super) struct EmptyRow {} }
result_row! { #[derive(Default)] pub(super) struct EmptyNested { nested: EmptyRow } }
result_row! { struct NullableChild { child: Option<FlatRow> } }
result_row! { pub(super) struct NullableDescendant { nested: NullableChild } }
#[derive(Debug, PartialEq, DeriveToSchema, TryFromStructData)]
pub(super) struct UnsupportedDescendant {
    nested: super::IntegerList,
}

pub(super) fn nested_array(
    columns: Vec<(&str, ArrayRef)>,
    validity: Option<Vec<bool>>,
) -> ArrayRef {
    let batch = RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .unwrap();
    Arc::new(StructArray::new(
        batch.schema().fields().clone(),
        batch.columns().to_vec(),
        validity.map(NullBuffer::from),
    ))
}

pub(super) fn rows(populated: bool) -> Vec<TestRow> {
    (0..2)
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
        .collect()
}

#[rstest]
#[case::missing_parent("other")]
#[case::wrong_parent_type("nested")]
fn invalid_nested_parents_are_rejected(#[case] name: &str) -> KernelResult<()> {
    let batch = RecordBatch::try_from_iter([(name, vec![1].into_arrow_array())])?;
    let actual = super::execute_result::<Vec<SimpleNestedRow>>(
        data(vec![Ok(batch)]),
        SimpleNestedRow::schema_ref(),
    );
    assert!(actual.is_err());
    Ok(())
}

#[rstest]
#[case::required(|nested: Option<NullableChild>| nested.map(|nested| RequiredParent { before: 3, nested, after: 5 }))]
#[case::optional(|nested| Some(OptionalParent { before: 3, nested, after: 5 }))]
fn nested_structs_follow_parent_validity<T>(
    #[case] expected: fn(Option<NullableChild>) -> Option<T>,
    #[values(false, true)] present: bool,
    #[values(false, true)] child_present: bool,
    #[values(Some(41), None)] id: Option<i32>,
    #[values(false, true)] label_present: bool,
) -> KernelResult<()>
where
    T: ToSchema + TryFrom<StructData, Error = KernelError> + Debug + PartialEq,
{
    let child = nested_array(
        vec![
            (
                "label",
                super::hidden_strings(vec!["hidden"], vec![label_present])?,
            ),
            ("ignored", vec![99].into_arrow_array()),
            ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        ],
        Some(vec![child_present]),
    );
    let nested = nested_array(
        vec![("ignored", vec![99].into_arrow_array()), ("child", child)],
        Some(vec![present]),
    );
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("after", vec![5].into_arrow_array(), false),
        ("nested", nested, true),
        ("before", vec![3].into_arrow_array(), false),
    ])?;
    let actual = super::execute_result::<Vec<T>>(data(vec![Ok(batch)]), T::schema_ref());
    let expected = expected(present.then(|| NullableChild {
        child: child_present.then(|| FlatRow {
            id: id.unwrap_or_default(),
            label: label_present.then(|| "hidden".to_owned()),
        }),
    }));
    if expected.is_none() || (present && child_present && id.is_none()) {
        let field = if expected.is_none() { "nested" } else { "id" };
        assert!(matches!(
            actual.unwrap_err().without_backtrace(),
            KernelError::InvalidStructData(message) if message.contains(field)
        ));
    } else {
        assert_eq!(actual?, vec![expected.unwrap()]);
    }
    Ok(())
}
#[rstest]
fn empty_top_level_rows_remain_supported(#[values(0, 1, 2)] count: usize) -> KernelResult<()> {
    let batch = row_batch(vec![Some(1); count]);
    let rows: Vec<EmptyRow> = super::execute_result(data(vec![Ok(batch)]), EmptyRow::schema_ref())?;
    assert_eq!(rows, vec![EmptyRow {}; count]);
    Ok(())
}

result_row! { struct OptionalEmptyNested { nested: Option<EmptyRow> } }

#[rstest]
#[case::required(|nested: Option<EmptyRow>| nested.map(|nested| EmptyNested { nested }))]
#[case::optional(|nested| Some(OptionalEmptyNested { nested }))]
fn empty_nested_structs_follow_parent_validity<T>(
    #[case] expected: fn(Option<EmptyRow>) -> Option<T>,
    #[values(false, true)] present: bool,
) -> KernelResult<()>
where
    T: ToSchema + TryFrom<StructData, Error = KernelError> + Debug + PartialEq,
{
    let nested = StructArray::new_empty_fields(1, Some(NullBuffer::from(vec![present])));
    let batch =
        RecordBatch::try_from_iter_with_nullable([("nested", Arc::new(nested) as ArrayRef, true)])?;
    let actual = super::execute_result::<Vec<T>>(data(vec![Ok(batch)]), T::schema_ref());
    match expected(present.then_some(EmptyRow {})) {
        Some(expected) => assert_eq!(actual?, vec![expected]),
        None => assert!(matches!(
            actual.unwrap_err().without_backtrace(),
            KernelError::InvalidStructData(message) if message.contains("nested")
        )),
    }
    Ok(())
}

#[rstest]
#[case::optional_parent(execute_rows::<Vec<OptionalParent>>, OptionalParent::to_schema)]
#[case::empty_parent(execute_rows::<Vec<EmptyNested>>, EmptyNested::to_schema)]
#[case::optional_empty_parent(
    execute_rows::<Vec<OptionalEmptyNested>>, OptionalEmptyNested::to_schema,
)]
#[case::nullable_descendant(
    execute_rows::<Vec<NullableDescendant>>, NullableDescendant::to_schema,
)]
fn supported_nested_schemas_accept_empty_sources_and_propagate_stream_errors<T: Debug>(
    #[case] convert: fn(Vec<KernelResult<RecordBatch>>) -> KernelResult<Vec<T>>,
    #[case] schema: fn() -> delta_kernel::schema::StructType,
    #[values(0, 1, 2)] source: usize,
) -> KernelResult<()> {
    let batches = match source {
        0 => vec![],
        1 => vec![Ok(RecordBatch::new_empty(Arc::new(
            (&schema()).try_into_arrow()?,
        )))],
        _ => vec![Err(KernelError::generic("late nested error"))],
    };
    let actual = convert(batches);
    if source == 2 {
        assert!(matches!(
            actual.unwrap_err().without_backtrace(),
            KernelError::Generic(message) if message == "late nested error"
        ));
    } else {
        assert!(actual?.is_empty());
    }
    Ok(())
}
