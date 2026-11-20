//! Typed plan result conversion through the existing row visitor interface.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

use delta_kernel::arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, MapBuilder, MapFieldNames, RecordBatch, StringArray,
    StringBuilder, new_null_array,
};
use delta_kernel::arrow::buffer::{NullBuffer, OffsetBuffer};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field};
use delta_kernel::delta_kernel_derive::{
    IntoStructData, ToSchema as DeriveToSchema, TryFromStructData,
};
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::sync::plan::SyncPlanExecutor;
use delta_kernel::expressions::{StructData, TryFromStructRows};
use delta_kernel::plans::Operation;
use delta_kernel::plans::ir::nodes::Values;
use delta_kernel::schema::{
    DataType, SchemaRef, StructField, StructType, ToSchema, lazy_schema_ref, schema,
};
use delta_kernel::{
    EngineData, KernelError, KernelResult, PlanBuilder, PlanExecutor, PlanExecutorExt, PlanResult,
};
use rstest::rstest;
use test_utils::{IntoArray, generate_batch};

macro_rules! result_row {
    ($(#[$attr:meta])* $vis:vis struct $name:ident { $($fields:tt)* }) => {
        #[derive(Clone, Debug, PartialEq, IntoStructData, DeriveToSchema, TryFromStructData)]
        $(#[$attr])*
        $vis struct $name { $($fields)* }
    };
}

result_row! { struct TestRow { id: i32, label: Option<String> } }
result_row! { struct Containers {
    tags: Option<Vec<String>>, attrs: Option<HashMap<String, String>>,
} }
result_row! { struct IntegerList { value: Vec<i32> } }
result_row! { struct NestedStruct { value: Option<TestRow> } }
result_row! { struct NestedList { value: Vec<Vec<String>> } }
result_row! { struct NestedMap { value: HashMap<String, TestRow> } }
result_row! { struct NullableList { value: Vec<Option<String>> } }
result_row! { struct NullableMap { value: HashMap<String, Option<String>> } }
result_row! { struct StringList { tags: Vec<String> } }

#[derive(Debug, PartialEq)]
struct CustomRow {
    adjusted_id: i32,
}

impl ToSchema for CustomRow {
    fn schema_ref() -> &'static SchemaRef {
        static SCHEMA_INFO: LazyLock<SchemaRef> = lazy_schema_ref! { not_null "id": INTEGER };
        &SCHEMA_INFO
    }
}

impl TryFrom<StructData> for CustomRow {
    type Error = KernelError;

    fn try_from(value: StructData) -> KernelResult<Self> {
        let id = i32::try_from(value.values()[0].clone())?;
        if id < 0 {
            return Err(KernelError::generic("decoder rejected row"));
        }
        Ok(Self {
            adjusted_id: id + 100,
        })
    }
}

#[derive(Debug, PartialEq, DeriveToSchema, TryFromStructData)]
struct SeparatelyDerivedRow {
    id: i32,
}

#[rstest]
#[case::separate_derives(SeparatelyDerivedRow { id: 7 })]
#[case::existing_schema_and_decoder(CustomRow { adjusted_id: 107 })]
fn wrappers_need_only_schema_and_struct_decoder<T>(
    #[case] expected: T,
    #[values(false, true)] singleton: bool,
) -> KernelResult<()>
where
    T: ToSchema + TryFrom<StructData, Error = KernelError> + Debug + PartialEq,
{
    let batch = generate_batch([("id", vec![7].into_arrow_array())])?;
    assert_eq!(
        execute_rows::<Option<T>>(vec![Ok(batch.clone())])?.as_ref(),
        Some(&expected)
    );
    assert_eq!(
        convert_rows::<T>(vec![Ok(batch)], singleton)?,
        vec![expected]
    );
    Ok(())
}

fn data(batches: Vec<KernelResult<RecordBatch>>) -> PlanResult {
    PlanResult::Data(Box::new(batches.into_iter().map(|batch| {
        batch.map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
    })))
}

struct ResultExecutor {
    result: std::sync::Mutex<Option<PlanResult>>,
}

impl PlanExecutor for ResultExecutor {
    fn execute_op(&self, _: Operation) -> KernelResult<PlanResult> {
        Ok(self.result.lock().unwrap().take().unwrap())
    }
}

fn execute_rows<T: TryFromStructRows>(batches: Vec<KernelResult<RecordBatch>>) -> KernelResult<T> {
    let schema = batches
        .iter()
        .find_map(|batch| batch.as_ref().ok())
        .map(|batch| {
            let schema: StructType = batch.schema().as_ref().try_into_kernel()?;
            Ok(Arc::new(schema))
        })
        .unwrap_or_else(|| Ok(<T::Row as ToSchema>::schema_ref().clone()))?;
    execute_result(data(batches), &schema)
}

fn execute_result<T: TryFromStructRows>(result: PlanResult, schema: &SchemaRef) -> KernelResult<T> {
    let plan = PlanBuilder::values(schema.clone(), vec![])?.build()?;
    ResultExecutor {
        result: std::sync::Mutex::new(Some(result)),
    }
    .execute_plan_into(plan)
}

fn empty_batch<T: delta_kernel::schema::ToSchema>() -> KernelResult<RecordBatch> {
    let schema = (&T::to_schema()).try_into_arrow()?;
    Ok(RecordBatch::new_empty(Arc::new(schema)))
}

fn row_batch(ids: Vec<Option<i32>>) -> RecordBatch {
    let labels = vec![None::<&str>; ids.len()];
    RecordBatch::try_from_iter_with_nullable([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef, false),
        (
            "label",
            Arc::new(StringArray::from(labels)) as ArrayRef,
            true,
        ),
    ])
    .unwrap()
}

fn convert_rows<T>(batches: Vec<KernelResult<RecordBatch>>, singleton: bool) -> KernelResult<Vec<T>>
where
    T: ToSchema + TryFrom<StructData, Error = KernelError>,
{
    if singleton {
        execute_rows::<T>(batches).map(|row| vec![row])
    } else {
        execute_rows(batches)
    }
}

fn singleton<T>(rows: Vec<T>) -> Option<T> {
    let [row]: [T; 1] = rows.try_into().ok()?;
    Some(row)
}

fn optional<T>(mut rows: Vec<T>) -> Option<Option<T>> {
    (rows.len() <= 1).then(|| rows.pop())
}

#[rstest]
#[case::required(singleton::<TestRow>, "exactly one row")]
#[case::optional(optional::<TestRow>, "at most one row")]
#[case::vector(Some::<Vec<TestRow>>, "")]
fn results_preserve_batch_order_and_enforce_cardinality<T>(
    #[case] expected: fn(Vec<TestRow>) -> Option<T>,
    #[case] count_error: &str,
    #[values(vec![], vec![0], vec![1], vec![2], vec![0, 1, 0, 1])] counts: Vec<usize>,
) -> KernelResult<()>
where
    T: TryFromStructRows + Debug + PartialEq,
{
    let count = counts.iter().sum::<usize>();
    let rows = (0..count as i32)
        .map(|id| TestRow { id, label: None })
        .collect();
    let mut ids = (0..count as i32).map(Some);
    let batches = counts
        .into_iter()
        .map(|count| Ok(row_batch(ids.by_ref().take(count).collect())))
        .collect();
    let actual = execute_rows::<T>(batches);
    match expected(rows) {
        Some(expected) => assert_eq!(actual?, expected),
        None => {
            let error = actual.unwrap_err();
            assert!(matches!(
                error.without_backtrace(),
                KernelError::ScalarConversion(_)
            ));
            assert!(
                error
                    .to_string()
                    .contains(&format!("expected {count_error}, found {count} rows"))
            );
        }
    }
    Ok(())
}

#[rstest]
#[case::required(|r: Vec<KernelResult<RecordBatch>>| execute_rows::<CustomRow>(r).map(drop), "exactly one row")]
#[case::optional(|r: Vec<KernelResult<RecordBatch>>| execute_rows::<Option<CustomRow>>(r).map(drop), "at most one row")]
#[case::vector(|r: Vec<KernelResult<RecordBatch>>| execute_rows::<Vec<CustomRow>>(r).map(drop), "")]
fn cardinality_precedes_custom_decoding(
    #[case] decode: fn(Vec<KernelResult<RecordBatch>>) -> KernelResult<()>,
    #[case] count_error: &str,
    #[values(1, 2)] count: usize,
    #[values(false, true)] split: bool,
) -> KernelResult<()> {
    let batches = if split { vec![1; count] } else { vec![count] }
        .into_iter()
        .map(|count| {
            Ok(generate_batch([(
                "id",
                vec![-1; count].into_arrow_array(),
            )])?)
        })
        .collect();
    let error = decode(batches).unwrap_err();
    if count == 2 && !count_error.is_empty() {
        assert!(matches!(
            error.without_backtrace(),
            KernelError::ScalarConversion(_)
        ));
        assert!(
            error
                .to_string()
                .contains(&format!("expected {count_error}, found 2 rows"))
        );
    } else {
        assert!(
            matches!(error.without_backtrace(), KernelError::Generic(message) if message == "decoder rejected row")
        );
    }
    Ok(())
}

#[rstest]
#[case::flat(vec![TestRow { id: 4, label: Some("four".to_owned()) }, TestRow { id: 5, label: None }])]
#[case::containers(vec![
    Containers { tags: None, attrs: None },
    Containers { tags: Some(vec![]), attrs: Some(HashMap::new()) },
    Containers {
        tags: Some(vec!["tag".to_owned()]),
        attrs: Some(HashMap::from([("key".to_owned(), "value".to_owned())])),
    },
])]
fn rows_round_trip_through_values_and_executor<T>(#[case] expected: Vec<T>) -> KernelResult<()>
where
    T: ToSchema
        + TryFrom<StructData, Error = KernelError>
        + Into<StructData>
        + Clone
        + Debug
        + PartialEq,
{
    let values: Values = expected.clone().into_iter().collect();
    assert_eq!(Vec::<T>::try_from(values.clone())?, expected);
    let plan = PlanBuilder::values(values.schema, values.rows)?.build()?;
    let actual: Vec<T> = SyncPlanExecutor::default().execute_plan_into(plan)?;
    assert_eq!(actual, expected);
    Ok(())
}
#[rstest]
#[case::required(|r: Vec<KernelResult<RecordBatch>>| execute_rows::<TestRow>(r).map(drop))]
#[case::optional(|r: Vec<KernelResult<RecordBatch>>| execute_rows::<Option<TestRow>>(r).map(drop))]
#[case::vector(|r: Vec<KernelResult<RecordBatch>>| execute_rows::<Vec<TestRow>>(r).map(drop))]
fn late_stream_errors_win_over_cardinality(
    #[case] convert: fn(Vec<KernelResult<RecordBatch>>) -> KernelResult<()>,
    #[values(0, 1, 2)] count: usize,
) {
    let error = convert(vec![
        Ok(row_batch(vec![Some(1); count])),
        Err(KernelError::generic("late")),
    ])
    .unwrap_err();
    assert!(
        matches!(error.without_backtrace(), KernelError::Generic(message) if message == "late")
    );
}
#[rstest]
#[case::unit(|| PlanResult::Unit)]
#[case::bytes(|| PlanResult::Bytes(Box::new(std::iter::empty())))]
#[case::files(|| PlanResult::FileMeta(Box::new(std::iter::empty())))]
fn non_data_variants_are_rejected(#[case] result: fn() -> PlanResult) {
    let error = execute_result::<Vec<TestRow>>(result(), TestRow::schema_ref()).unwrap_err();
    assert!(matches!(
        error.without_backtrace(),
        KernelError::PlanResultTypeMismatch {
            expected: "Data",
            ..
        }
    ));
}

#[rstest]
#[case::missing(schema! { not_null "id": INTEGER })]
#[case::extra(schema! { not_null "id": INTEGER, nullable "label": STRING, not_null "extra": INTEGER })]
#[case::wrong_type(schema! { not_null "id": LONG, nullable "label": STRING })]
#[case::nullability(schema! { nullable "id": INTEGER, nullable "label": STRING })]
#[case::metadata(schema! { (StructField::not_null("id", DataType::INTEGER).with_metadata([("key", "value")])), nullable "label": STRING })]
fn schema_mismatch_is_rejected_before_execution(#[case] actual: StructType) -> KernelResult<()> {
    let expected = TestRow::schema_ref();
    let message = format!("Expected:\n{expected}\nActual:\n{actual}");
    let plan = PlanBuilder::values(actual, vec![])?.build()?;
    let error = ResultExecutor {
        result: std::sync::Mutex::new(None),
    }
    .execute_plan_into::<Vec<TestRow>>(plan)
    .unwrap_err();
    assert!(matches!(
        error.without_backtrace(),
        KernelError::InvalidStructData(_)
    ));
    assert!(error.to_string().contains(&message));
    Ok(())
}

#[rstest]
#[case::missing(vec![("id", vec![1].into_arrow_array())])]
#[case::wrong_type(vec![("id", vec!["one"].into_arrow_array()), ("label", new_null_array(&ArrowDataType::Utf8, 1))])]
#[case::required_null(vec![("id", new_null_array(&ArrowDataType::Int32, 1)), ("label", new_null_array(&ArrowDataType::Utf8, 1))])]
fn invalid_batch_fields_are_rejected(#[case] columns: Vec<(&str, ArrayRef)>) -> KernelResult<()> {
    let batch = generate_batch(columns)?;
    let result = execute_result::<Vec<TestRow>>(data(vec![Ok(batch)]), TestRow::schema_ref());
    assert!(result.is_err());
    Ok(())
}

#[derive(Debug, PartialEq, DeriveToSchema, TryFromStructData)]
struct StringMap {
    #[field_id = 7]
    attrs: HashMap<String, String>,
}

fn string_map(entries: &[(&str, Option<&str>)]) -> KernelResult<ArrayRef> {
    let names = MapFieldNames {
        entry: "key_value".into(),
        key: "key".into(),
        value: "value".into(),
    };
    let mut map = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
    for (key, value) in entries {
        map.keys().append_value(key);
        map.values().append_option(*value);
    }
    map.append(true)?;
    Ok(Arc::new(map.finish()))
}

#[rstest]
#[case::required(|attrs| StringMap { attrs })]
fn maps_ignore_null_values<T>(
    #[case] expected_row: fn(HashMap<String, String>) -> T,
    #[values(vec![], vec![("key", Some("value"))], vec![("null", None)],
        vec![("key", Some("value")), ("null", None)])]
    entries: Vec<(&str, Option<&str>)>,
) -> KernelResult<()>
where
    T: ToSchema + TryFrom<StructData, Error = KernelError> + TryFromStructRows + Debug + PartialEq,
{
    let name = T::to_schema().fields().next().unwrap().name().clone();
    let batch = generate_batch([(name, string_map(&entries)?)])?;
    let rows: Vec<T> = execute_result(data(vec![Ok(batch)]), T::schema_ref())?;
    let attrs = entries
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key.to_owned(), value.to_owned())))
        .collect();
    assert_eq!(rows, vec![expected_row(attrs)]);
    Ok(())
}

fn hidden_strings(values: Vec<&str>, validity: Vec<bool>) -> KernelResult<ArrayRef> {
    let array = StringArray::from(values)
        .to_data()
        .into_builder()
        .nulls(Some(NullBuffer::from(validity)))
        .build()?;
    Ok(Arc::new(StringArray::from(array)))
}

#[rstest]
#[case::empty(3..3, None)]
#[case::empty_string(1..2, None)]
#[case::present_strings(1..3, None)]
#[case::nulls_outside_range(4..5, None)]
#[case::null_first(0..3, Some(0))]
#[case::null_middle(2..5, Some(1))]
#[case::null_last(4..6, Some(1))]
fn string_lists_reject_null_elements_with_hidden_bytes(
    #[case] offsets: std::ops::Range<usize>,
    #[case] null_index: Option<usize>,
    #[values(false, true)] singleton: bool,
) -> KernelResult<()> {
    let values = vec!["hidden", "", "first", "hidden", "last", "hidden"];
    let expected = values[offsets.clone()]
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
    let values = hidden_strings(values, vec![false, true, true, false, true, false])?;
    let list = ListArray::new(
        Arc::new(Field::new("item", ArrowDataType::Utf8, true)),
        OffsetBuffer::new(vec![offsets.start as i32, offsets.end as i32].into()),
        values,
        None,
    );
    let batch = generate_batch([("tags", Arc::new(list) as ArrayRef)])?;
    // The logical list contract is non-null; Arrow also stores nulls outside the selected range.
    let result = data(vec![Ok(batch)]);
    let schema = StringList::schema_ref();
    let actual = if singleton {
        execute_result::<StringList>(result, schema).map(|row| vec![row])
    } else {
        execute_result::<Vec<StringList>>(result, schema)
    };
    if let Some(index) = null_index {
        let error = actual.unwrap_err();
        assert!(matches!(
            error.without_backtrace(),
            KernelError::InvalidStructData(message)
                if message.contains(&format!("element at index {index} cannot be null"))
        ));
    } else {
        assert_eq!(actual?, vec![StringList { tags: expected }]);
    }
    Ok(())
}

#[rstest]
#[case::flat(execute_rows::<Vec<TestRow>>, Some(empty_batch::<TestRow> as fn() -> _))]
#[case::integer_list(execute_rows::<Vec<IntegerList>>, None)]
#[case::nested_list(execute_rows::<Vec<NestedList>>, None)]
#[case::nested_map(execute_rows::<Vec<NestedMap>>, None)]
#[case::nullable_list_elements(execute_rows::<Vec<NullableList>>, None)]
#[case::nullable_struct(execute_rows::<Vec<NestedStruct>>, None)]
#[case::nullable_map_values(execute_rows::<Vec<NullableMap>>, None)]
fn schema_support_controls_empty_results_and_stream_errors<T: Debug>(
    #[case] convert: fn(Vec<KernelResult<RecordBatch>>) -> KernelResult<Vec<T>>,
    #[case] batch: Option<fn() -> KernelResult<RecordBatch>>,
    #[values(0, 1, 2)] source: usize,
) -> KernelResult<()> {
    let batches = match source {
        0 => vec![],
        1 => vec![
            batch
                .map(|batch| batch())
                .unwrap_or_else(|| Ok(row_batch(vec![]))),
        ],
        _ => vec![Err(KernelError::generic("schema stream error"))],
    };
    let actual = convert(batches);
    if batch.is_some() && source != 2 {
        assert!(actual?.is_empty());
    } else {
        let error = actual.unwrap_err();
        match (batch.is_some(), error.without_backtrace()) {
            (false, KernelError::Unsupported(_)) => (),
            (true, KernelError::Generic(message)) => assert_eq!(message, "schema stream error"),
            _ => panic!("unexpected schema error: {error}"),
        }
    }
    Ok(())
}
