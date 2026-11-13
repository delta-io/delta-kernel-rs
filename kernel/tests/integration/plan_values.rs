//! Typed plan result conversion through the existing row visitor interface.

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::arrow::array::{
    ArrayRef, Int32Array, MapBuilder, RecordBatch, StringArray, StringBuilder,
};
use delta_kernel::delta_kernel_derive::{
    IntoStructData, ToSchema, TryFromPlanResult, TryFromStructData,
};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::sync::plan::SyncPlanExecutor;
use delta_kernel::expressions::{ColumnName, Scalar, StructData};
use delta_kernel::plans::ir::nodes::Values;
use delta_kernel::plans::{Plan, PlanResultRow};
use delta_kernel::schema::DataType;
use delta_kernel::{EngineData, KernelError, KernelResult, PlanBuilder, PlanExecutor, PlanResult};
use rstest::rstest;

#[derive(
    Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData, TryFromPlanResult,
)]
struct TestRow {
    id: i32,
    label: Option<String>,
}

#[derive(
    Clone, Debug, PartialEq, ToSchema, IntoStructData, TryFromStructData, TryFromPlanResult,
)]
struct Containers {
    tags: Option<Vec<String>>,
    attrs: Option<HashMap<String, String>>,
}

macro_rules! unsupported_row {
    ($name:ident, $($field_type:tt)+) => {
        #[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
        struct $name {
            value: $($field_type)+,
        }
    };
}
unsupported_row!(IntegerList, Vec<i32>);
unsupported_row!(NestedStruct, Option<TestRow>);
unsupported_row!(NestedList, Vec<Vec<String>>);
unsupported_row!(NestedMap, HashMap<String, TestRow>);
unsupported_row!(NullableList, Vec<Option<String>>);
unsupported_row!(NullableMap, HashMap<String, Option<String>>);

fn data(batches: Vec<KernelResult<RecordBatch>>) -> PlanResult {
    PlanResult::Data(Box::new(batches.into_iter().map(|batch| {
        batch.map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
    })))
}

fn row_batch(ids: Vec<Option<i32>>) -> RecordBatch {
    let labels = vec![None::<&str>; ids.len()];
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("label", Arc::new(StringArray::from(labels)) as ArrayRef),
    ])
    .unwrap()
}

fn plan<T: delta_kernel::schema::ToSchema + Into<StructData>>(rows: Vec<T>) -> KernelResult<Plan> {
    let Values { schema, rows } = rows.into_iter().collect();
    PlanBuilder::values(schema, rows)?.build()
}

#[test]
fn executor_results_convert_directly_to_singleton_and_vector() -> KernelResult<()> {
    let executor = SyncPlanExecutor::default();
    let expected = TestRow {
        id: 4,
        label: Some("four".to_owned()),
    };
    let row: TestRow = executor
        .execute_plan(plan(vec![expected.clone()])?)?
        .try_into()?;
    assert_eq!(row, expected);
    let expected = vec![expected, TestRow { id: 5, label: None }];
    let rows: Vec<TestRow> = executor.execute_plan(plan(expected.clone())?)?.try_into()?;
    assert_eq!(rows, expected);
    Ok(())
}

#[test]
fn batches_preserve_order_and_empty_batches_do_not_add_rows() -> KernelResult<()> {
    let rows: Vec<TestRow> = data(vec![
        Ok(row_batch(vec![])),
        Ok(row_batch(vec![Some(7)])),
        Ok(row_batch(vec![])),
        Ok(row_batch(vec![Some(8)])),
    ])
    .try_into()?;
    assert_eq!(
        rows,
        vec![
            TestRow { id: 7, label: None },
            TestRow { id: 8, label: None }
        ]
    );
    let singleton: TestRow = data(vec![
        Ok(row_batch(vec![])),
        Ok(row_batch(vec![Some(9)])),
        Ok(row_batch(vec![])),
    ])
    .try_into()?;
    assert_eq!(singleton, TestRow { id: 9, label: None });
    let empty: Vec<TestRow> = data(vec![]).try_into()?;
    assert!(empty.is_empty());
    Ok(())
}

#[rstest]
#[case::empty_stream(vec![])]
#[case::empty_batch(vec![vec![]])]
#[case::one_batch(vec![vec![Some(1), Some(2)]])]
#[case::separate_batches(vec![vec![Some(1)], vec![Some(2)]])]
fn singleton_requires_one_total_row(#[case] batches: Vec<Vec<Option<i32>>>) {
    let result = data(batches.into_iter().map(|ids| Ok(row_batch(ids))).collect());
    let error = TestRow::try_from(result).unwrap_err();
    assert!(matches!(
        error.without_backtrace(),
        KernelError::ScalarConversion(_)
    ));
}

#[rstest]
fn late_stream_errors_win_over_cardinality(
    #[values(0, 1, 2)] count: usize,
    #[values(false, true)] singleton: bool,
) {
    let result = data(vec![
        Ok(row_batch(vec![Some(1); count])),
        Err(KernelError::generic("late")),
    ]);
    let error = if singleton {
        TestRow::try_from(result).map(|_| ()).unwrap_err()
    } else {
        Vec::<TestRow>::try_from(result).map(|_| ()).unwrap_err()
    };
    assert!(
        matches!(error.without_backtrace(), KernelError::Generic(message) if message == "late")
    );
}

#[rstest]
#[case::unit(|| PlanResult::Unit)]
#[case::bytes(|| PlanResult::Bytes(Box::new(std::iter::empty())))]
#[case::files(|| PlanResult::FileMeta(Box::new(std::iter::empty())))]
fn non_data_variants_are_rejected(
    #[case] result: fn() -> PlanResult,
    #[values(false, true)] singleton: bool,
) {
    let error = if singleton {
        TestRow::try_from(result()).map(|_| ()).unwrap_err()
    } else {
        Vec::<TestRow>::try_from(result()).map(|_| ()).unwrap_err()
    };
    assert!(matches!(
        error.without_backtrace(),
        KernelError::PlanResultTypeMismatch {
            expected: "Data",
            ..
        }
    ));
}

#[test]
fn materialization_matches_field_names_and_uses_cached_schema() -> KernelResult<()> {
    let reordered = RecordBatch::try_from_iter([
        (
            "label",
            Arc::new(StringArray::from(vec![Some("second")])) as ArrayRef,
        ),
        ("ignored", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
        ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
    ])?;
    let values =
        data(vec![Ok(row_batch(vec![Some(1)])), Ok(reordered)]).into_values::<TestRow>()?;
    assert!(Arc::ptr_eq(&values.schema, TestRow::schema_and_columns().0));
    assert_eq!(
        values.rows,
        vec![
            vec![Scalar::Integer(1), Scalar::Null(DataType::STRING)],
            vec![Scalar::Integer(2), Scalar::String("second".to_owned())],
        ]
    );
    Ok(())
}

#[rstest]
#[case::missing(vec![("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])]
#[case::wrong_type(vec![("id", Arc::new(StringArray::from(vec!["one"])) as ArrayRef), ("label", Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef)])]
#[case::required_null(vec![("id", Arc::new(Int32Array::from(vec![None::<i32>])) as ArrayRef), ("label", Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef)])]
fn invalid_fields_are_rejected(#[case] columns: Vec<(&str, ArrayRef)>) -> KernelResult<()> {
    let result: KernelResult<Vec<TestRow>> =
        data(vec![Ok(RecordBatch::try_from_iter(columns)?)]).try_into();
    assert!(result.is_err());
    Ok(())
}

#[test]
fn string_containers_preserve_missing_empty_and_populated_values() -> KernelResult<()> {
    let expected = vec![
        Containers {
            tags: None,
            attrs: None,
        },
        Containers {
            tags: Some(vec![]),
            attrs: Some(HashMap::new()),
        },
        Containers {
            tags: Some(vec!["tag".to_owned()]),
            attrs: Some(HashMap::from([("key".to_owned(), "value".to_owned())])),
        },
    ];
    let rows: Vec<Containers> = SyncPlanExecutor::default()
        .execute_plan(plan(expected.clone())?)?
        .try_into()?;
    assert_eq!(rows, expected);
    Ok(())
}

#[derive(Debug, PartialEq, ToSchema, TryFromStructData, TryFromPlanResult)]
struct StringMap {
    attrs: HashMap<String, String>,
}

#[rstest]
fn maps_reject_null_values_and_duplicate_keys(
    #[values(false, true)] duplicate: bool,
) -> KernelResult<()> {
    let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    map.keys().append_value("key");
    if duplicate {
        map.values().append_value("first");
    } else {
        map.values().append_null();
    }
    if duplicate {
        map.keys().append_value("key");
        map.values().append_value("second");
    }
    map.append(true)?;
    let batch = RecordBatch::try_from_iter([("attrs", Arc::new(map.finish()) as ArrayRef)])?;
    let rows: KernelResult<Vec<StringMap>> = data(vec![Ok(batch)]).try_into();
    assert!(rows.is_err());
    Ok(())
}

#[rstest]
#[case::integer_list(PlanResult::into_values::<IntegerList>)]
#[case::nested_struct(PlanResult::into_values::<NestedStruct>)]
#[case::nested_list(PlanResult::into_values::<NestedList>)]
#[case::nested_map(PlanResult::into_values::<NestedMap>)]
#[case::nullable_list_elements(PlanResult::into_values::<NullableList>)]
#[case::nullable_map_values(PlanResult::into_values::<NullableMap>)]
fn unsupported_schemas_fail_without_rows(
    #[case] convert: fn(PlanResult) -> KernelResult<Values>,
    #[values(false, true)] empty_batch: bool,
) {
    let batches = if empty_batch {
        vec![Ok(row_batch(vec![]))]
    } else {
        vec![]
    };
    let error = convert(data(batches)).unwrap_err();
    assert!(matches!(
        error.without_backtrace(),
        KernelError::Unsupported(_)
    ));
}

#[test]
fn row_metadata_is_static_and_reused_after_conversion() -> KernelResult<()> {
    let (schema, columns) = TestRow::schema_and_columns();
    let _: TestRow = data(vec![Ok(row_batch(vec![Some(1)]))]).try_into()?;
    let (again_schema, again_columns) = TestRow::schema_and_columns();
    assert!(std::ptr::eq(schema, again_schema));
    assert!(Arc::ptr_eq(schema, again_schema));
    assert!(std::ptr::eq(columns, again_columns));
    let (names, types) = columns.as_ref();
    assert_eq!(
        names,
        &[ColumnName::new(["id"]), ColumnName::new(["label"])]
    );
    assert_eq!(types, &[DataType::INTEGER, DataType::STRING]);
    assert!(!schema.field("id").unwrap().is_nullable());
    assert!(schema.field("label").unwrap().is_nullable());
    Ok(())
}
