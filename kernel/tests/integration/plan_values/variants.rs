use std::fmt::Debug;

use delta_kernel::arrow::array::{ArrayRef, RecordBatch, new_null_array};
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::delta_kernel_derive::{
    IntoStructData, ToSchema as DeriveToSchema, TryFromStructData,
    TryFromStructRows as DeriveStructRows,
};
use delta_kernel::expressions::{Scalar, StructData, TryFromStructRows};
use delta_kernel::schema::{SchemaRef, ToSchema, schema};
use delta_kernel::{KernelError, KernelResult};
use rstest::rstest;
use test_utils::{IntoArray, assert_result_error_with_message};

use super::nested::{EmptyRow, nested_array};
use super::{CustomRow, data, execute_result, optional, singleton};

type Batches = Vec<KernelResult<RecordBatch>>;

fn execute_rows<T: TryFromStructRows>(batches: Batches) -> KernelResult<T> {
    execute_result(data(batches), <T::Row as ToSchema>::schema_ref())
}

result_row! { pub(super) struct Add { pub(super) id: i32 } }
result_row! { pub(super) struct Remove { pub(super) id: i32 } }
result_row! { struct NullableRow { id: Option<i32> } }

impl TryFrom<Scalar> for CustomRow {
    type Error = KernelError;

    fn try_from(value: Scalar) -> KernelResult<Self> {
        Self::try_from(StructData::try_from(value)?)
    }
}

macro_rules! action_collectors {
    ($($name:ident($add:ty, $remove:ty)),* $(,)?) => {$(
        #[derive(Debug, PartialEq, DeriveStructRows)]
        struct $name {
            add: $add,
            remove: $remove,
        }
    )*};
}

action_collectors! {
    Actions(Vec<Add>, Vec<Remove>),
    RequiredActions(Add, Remove),
    OptionalActions(Option<Add>, Option<Remove>),
    AddAndRemoves(Add, Vec<Remove>),
    OptionalAddAndRemoves(Option<Add>, Vec<Remove>),
    CustomAddAndRemoves(CustomRow, Vec<Remove>),
    OptionalCustomAddAndRemoves(Option<CustomRow>, Vec<Remove>),
    CustomActions(Vec<CustomRow>, Vec<Remove>),
    ReusedPayload(Add, Option<Add>),
}

#[derive(Debug, PartialEq, DeriveStructRows)]
struct NullableRows {
    nullable_row: Option<NullableRow>,
    empty_row: Option<EmptyRow>,
}

#[derive(Clone, Debug, PartialEq, IntoStructData, DeriveToSchema, TryFromStructData)]
pub(super) enum Action {
    Add(Add),
    Remove(Remove),
}

#[derive(Debug, PartialEq, IntoStructData, DeriveToSchema, TryFromStructData)]
enum RenamedAction {
    #[schema_rename = "add"]
    Created(Add),
    #[schema_rename = "remove"]
    Deleted(Remove),
}

#[derive(Debug, PartialEq, DeriveToSchema, TryFromStructData)]
enum NullableAction {
    NullableRow(NullableRow),
    EmptyRow(EmptyRow),
}

fn payload(ids: &[Option<i32>]) -> ArrayRef {
    let values = ids.iter().map(|id| id.unwrap_or(-999)).collect::<Vec<_>>();
    nested_array(
        vec![
            ("ignored", vec![0; ids.len()].into_arrow_array()),
            ("id", values.into_arrow_array()),
        ],
        Some(ids.iter().map(Option::is_some).collect()),
    )
}

fn batch(adds: &[Option<i32>], removes: &[Option<i32>]) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([
        ("remove", payload(removes), true),
        ("ignored", vec![0; adds.len()].into_arrow_array(), false),
        ("add", payload(adds), true),
    ])
    .unwrap()
}

fn variant_batches(adds: usize, removes: usize) -> Batches {
    let (adds, removes): (Vec<_>, Vec<_>) = (0..adds)
        .map(|id| (Some(id as i32), None))
        .chain((0..removes).map(|id| (None, Some(100 + id as i32))))
        .unzip();
    vec![Ok(batch(&adds, &removes))]
}

#[test]
fn variants_preserve_row_and_group_order_and_hidden_children() -> KernelResult<()> {
    let batches = [
        batch(&[], &[]),
        batch(&[Some(2), None], &[None, Some(9)]),
        batch(&[], &[]),
        batch(&[None, Some(4)], &[Some(8), None]),
    ];
    let rows: Vec<Action> = execute_rows(batches.iter().cloned().map(Ok).collect())?;
    assert_eq!(
        rows,
        vec![
            Action::Add(Add { id: 2 }),
            Action::Remove(Remove { id: 9 }),
            Action::Remove(Remove { id: 8 }),
            Action::Add(Add { id: 4 })
        ]
    );
    let groups: Actions = execute_rows(batches.into_iter().map(Ok).collect())?;
    assert_eq!(
        groups,
        Actions {
            add: vec![Add { id: 2 }, Add { id: 4 }],
            remove: vec![Remove { id: 9 }, Remove { id: 8 }],
        }
    );
    assert_eq!(
        Action::to_schema(),
        schema! {
            nullable "add": { not_null "id": INTEGER },
            nullable "remove": { not_null "id": INTEGER },
        }
    );
    Ok(())
}

#[rstest]
#[case::required(singleton::<Add>, singleton::<Remove>, |rows| execute_rows::<RequiredActions>(rows).map(|rows| (rows.add, rows.remove)))]
#[case::optional(optional::<Add>, optional::<Remove>, |rows| execute_rows::<OptionalActions>(rows).map(|rows| (rows.add, rows.remove)))]
#[case::vector(Some::<Vec<Add>>, Some::<Vec<Remove>>, |rows| execute_rows::<Actions>(rows).map(|rows| (rows.add, rows.remove)))]
#[case::required_vector(singleton::<Add>, Some::<Vec<Remove>>, |rows| execute_rows::<AddAndRemoves>(rows).map(|rows| (rows.add, rows.remove)))]
#[case::optional_vector(optional::<Add>, Some::<Vec<Remove>>, |rows| execute_rows::<OptionalAddAndRemoves>(rows).map(|rows| (rows.add, rows.remove)))]
fn named_components_enforce_their_own_cardinality<A, B>(
    #[case] add: fn(Vec<Add>) -> Option<A>,
    #[case] remove: fn(Vec<Remove>) -> Option<B>,
    #[case] collect: fn(Batches) -> KernelResult<(A, B)>,
    #[values(0, 1, 2)] adds: usize,
    #[values(0, 1, 2)] removes: usize,
) -> KernelResult<()>
where
    A: TryFromStructRows + Debug + PartialEq,
    B: TryFromStructRows + Debug + PartialEq,
{
    let add_rows = (0..adds as i32).map(|id| Add { id }).collect();
    let remove_rows = (100..100 + removes as i32)
        .map(|id| Remove { id })
        .collect();
    let expected = add(add_rows).zip(remove(remove_rows));
    let actual = collect(variant_batches(adds, removes));
    match expected {
        Some(expected) => assert_eq!(actual?, expected),
        None => assert!(matches!(
            actual.unwrap_err().without_backtrace(),
            KernelError::ScalarConversion(_)
        )),
    }
    Ok(())
}

#[rstest]
#[case::required(|rows| execute_rows::<CustomAddAndRemoves>(rows).map(drop))]
#[case::optional(|rows| execute_rows::<OptionalCustomAddAndRemoves>(rows).map(drop))]
#[case::vector(|rows| execute_rows::<CustomActions>(rows).map(drop))]
fn named_collections_use_manual_decoders(
    #[case] collect: fn(Batches) -> KernelResult<()>,
    #[values(1, 2)] count: usize,
    #[values(false, true)] split: bool,
) {
    let mut batches: Batches = if split { vec![1; count] } else { vec![count] }
        .into_iter()
        .map(|count| Ok(batch(&vec![Some(-1); count], &vec![None; count])))
        .collect();
    batches.push(Err(KernelError::generic("late decoder error")));
    let error = collect(batches).unwrap_err();
    assert!(matches!(
        error.without_backtrace(), KernelError::Generic(message)
            if message == "decoder rejected row"
    ));
}

#[rstest]
#[case::reused_payload(
    variant_batches(1, 1), ReusedPayload { add: Add { id: 0 }, remove: Some(Add { id: 100 }) }
)]
#[case::renamed_variant(
    vec![Ok(batch(&[None], &[Some(8)]))], RenamedAction::Deleted(Remove { id: 8 })
)]
fn named_collections_use_declared_fields_and_payload_decoders<T>(
    #[case] batches: Batches,
    #[case] expected: T,
) -> KernelResult<()>
where
    T: TryFromStructRows + Debug + PartialEq,
{
    assert_eq!(execute_rows::<T>(batches)?, expected);
    Ok(())
}

#[rstest]
#[case::named_collection(execute_rows::<RequiredActions>)]
#[case::named_enum(execute_rows::<Action>)]
fn variant_collection_errors_follow_consumption_order<T: Debug>(
    #[case] convert: fn(Batches) -> KernelResult<T>,
    #[values(false, true)] invalid_variant: bool,
    #[values(false, true)] late_malformed_variant: bool,
) {
    let first_batch = Ok(if invalid_variant {
        batch(&[None], &[None])
    } else {
        batch(&[Some(1), Some(2)], &[None, None])
    });
    let late_batch = if late_malformed_variant {
        Ok(batch(&[Some(3)], &[Some(4)]))
    } else {
        Err(KernelError::generic("late variant error"))
    };
    let batches = vec![first_batch, late_batch];
    let error = convert(batches).unwrap_err();
    if invalid_variant {
        assert!(matches!(
            error.without_backtrace(),
            KernelError::InvalidStructData(_)
        ));
    } else {
        assert!(matches!(
            error.without_backtrace(),
            KernelError::ScalarConversion(_)
        ));
    }
}

#[rstest]
#[case::required_payloads(Action::schema_ref().clone(), Action::try_from,
    vec![Scalar::from(Add { id: 1 }), Scalar::from(Remove { id: 2 })])]
#[case::nullable_payloads(NullableAction::schema_ref().clone(), NullableAction::try_from,
    vec![Scalar::from(NullableRow { id: None }), Scalar::from(EmptyRow {})])]
#[case::named_collection(Action::schema_ref().clone(), |row| RequiredActions::collect_into([Ok(row)]),
    vec![Scalar::from(Add { id: 1 }), Scalar::from(Remove { id: 2 })])]
fn variant_decoders_report_neither_and_both_fields<T: Debug>(
    #[case] schema: SchemaRef,
    #[case] decode: fn(StructData) -> KernelResult<T>,
    #[case] payloads: Vec<Scalar>,
    #[values(false, true)] both: bool,
) -> KernelResult<()> {
    let expected: Vec<_> = schema.fields().map(|field| field.name().as_str()).collect();
    let actual = if both { expected.clone() } else { vec![] };
    let message =
        format!("Expected exactly one non-null variant among {expected:?}; found {actual:?}");
    let values = if both {
        payloads
    } else {
        schema
            .fields()
            .map(|field| Scalar::Null(field.data_type().clone()))
            .collect()
    };
    let row = StructData::try_new(schema, values)?;
    assert_result_error_with_message(decode(row), &message);
    Ok(())
}

#[rstest]
#[case::enum_row(|nullable| if nullable {
    NullableAction::NullableRow(NullableRow { id: None })
} else {
    NullableAction::EmptyRow(EmptyRow {})
})]
#[case::named_collection(|nullable: bool| NullableRows {
    nullable_row: nullable.then_some(NullableRow { id: None }),
    empty_row: (!nullable).then_some(EmptyRow {}),
})]
fn present_payload_with_all_null_children_remains_present<T>(
    #[case] expected: fn(bool) -> T,
    #[values(false, true)] nullable: bool,
    #[values(false, true)] empty: bool,
    #[values(false, true)] split: bool,
) -> KernelResult<()>
where
    T: TryFromStructRows + Debug + PartialEq,
{
    // An invalid second row must win over result cardinality errors.
    let (nullable_validity, empty_validity) = if nullable == empty {
        (vec![true, nullable], vec![false, empty])
    } else {
        (vec![nullable], vec![empty])
    };
    let count = nullable_validity.len();
    let batch = RecordBatch::try_from_iter_with_nullable([
        (
            "emptyRow",
            nested_array(
                vec![("id", vec![42; count].into_arrow_array())],
                Some(empty_validity),
            ),
            true,
        ),
        (
            "nullableRow",
            nested_array(
                vec![("id", new_null_array(&ArrowDataType::Int32, count))],
                Some(nullable_validity),
            ),
            true,
        ),
    ])?;
    let batches = if split {
        (0..count).map(|index| Ok(batch.slice(index, 1))).collect()
    } else {
        vec![Ok(batch)]
    };
    let actual = execute_rows::<T>(batches);
    if nullable == empty {
        assert!(matches!(
            actual.unwrap_err().without_backtrace(),
            KernelError::InvalidStructData(message)
                if message.contains("Expected exactly one non-null variant")
        ));
    } else {
        assert_eq!(actual?, expected(nullable));
    }
    Ok(())
}

#[rstest]
#[case::missing_parent(false, payload(&[Some(1)]))]
#[case::wrong_parent_type(true, vec![1].into_arrow_array())]
#[case::missing_child(true, nested_array(vec![("other", vec![1].into_arrow_array())], None))]
#[case::wrong_child_type(true, nested_array(vec![("id", vec!["one"].into_arrow_array())], None))]
#[case::null_required_child(
    true, nested_array(vec![("id", new_null_array(&ArrowDataType::Int32, 1))], None)
)]
fn invalid_variant_sources_are_rejected(
    #[case] has_remove: bool,
    #[case] add: ArrayRef,
) -> KernelResult<()> {
    let mut columns = vec![("add", add)];
    if has_remove {
        columns.push(("remove", payload(&[None])));
    }
    let rows: KernelResult<Vec<Action>> =
        execute_rows(vec![Ok(RecordBatch::try_from_iter(columns)?)]);
    assert!(rows.is_err());
    Ok(())
}
