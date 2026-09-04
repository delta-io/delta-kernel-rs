//! Shared fixtures for the `arrow_data` / `arrow_get_data` struct-list tests.

use std::sync::{Arc, LazyLock};

use crate::arrow::array::{
    ArrayRef, GenericListArray, GenericListViewArray, Int32Array, LargeListArray, ListArray,
    OffsetSizeTrait, StructArray,
};
use crate::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
use crate::engine_data::{GetData, RowVisitor};
use crate::schema::{ColumnName, ColumnNamesAndTypes, DataType};
use crate::DeltaResult;

/// The list encodings that `ListLikeArray` unifies. The view flavors resolve a row's elements from
/// a separate sizes buffer rather than from adjacent offsets, so they exercise a structurally
/// different formula.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ListFlavor {
    List,
    LargeList,
    ListView,
    LargeListView,
}

/// A [`RowVisitor`] that collects the `n` (int) field of each visited element struct, preserving
/// nulls so a test can tell a dropped element from a null one.
#[derive(Default)]
pub(crate) struct CollectNVisitor {
    pub(crate) values: Vec<Option<i32>>,
}

impl RowVisitor for CollectNVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NT: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![ColumnName::new(["n"])], vec![DataType::INTEGER]).into());
        NT.as_ref()
    }
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            self.values.push(getters[0].get_int(i, "n")?);
        }
        Ok(())
    }
}

/// The `struct<n: int>` element type shared by every fixture. `nullable` applies to the element
/// struct itself, so the list's item field must declare it too.
fn element_fields(nullable: bool) -> Fields {
    vec![ArrowField::new("n", ArrowDataType::Int32, nullable)].into()
}

fn item_field(nullable: bool) -> Arc<ArrowField> {
    Arc::new(ArrowField::new(
        "item",
        ArrowDataType::Struct(element_fields(nullable)),
        nullable,
    ))
}

/// The `struct<n: int>` elements for `values`, where a `None` is a null element struct.
fn elements_of(values: &[Option<i32>]) -> StructArray {
    let nullable = values.iter().any(Option::is_none);
    let n = Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
    // A null element is a null on the struct itself, not merely on its `n` field.
    let nulls = nullable.then(|| NullBuffer::from_iter(values.iter().map(Option::is_some)));
    StructArray::new(element_fields(nullable), vec![n], nulls)
}

/// Build an `array<struct<n: int>>` from per-row element values (e.g. `[[10, 20], [30]]`).
pub(crate) fn struct_list_fixture(rows: &[&[i32]]) -> ListArray {
    let rows: Vec<_> = rows
        .iter()
        .map(|r| Some(r.iter().copied().map(Some).collect()))
        .collect();
    struct_list_fixture_opt(&rows)
}

/// Build an `array<struct<n: int>>` where an outer `None` row is a null list and an element `None`
/// is a null element struct.
pub(crate) fn struct_list_fixture_opt(rows: &[Option<Vec<Option<i32>>>]) -> ListArray {
    let flat: Vec<Option<i32>> = rows.iter().flatten().flatten().copied().collect();
    let nullable = flat.iter().any(Option::is_none);
    let offsets =
        OffsetBuffer::<i32>::from_lengths(rows.iter().map(|r| r.as_ref().map_or(0, Vec::len)));
    let row_nulls = rows
        .iter()
        .any(Option::is_none)
        .then(|| NullBuffer::from_iter(rows.iter().map(Option::is_some)));
    GenericListArray::new(
        item_field(nullable),
        offsets,
        Arc::new(elements_of(&flat)),
        row_nulls,
    )
}

/// Build an `array<struct<n: int>>` in the requested list encoding.
///
/// The view flavors lay their rows out back to front, so a test passing for them cannot be relying
/// on rows being contiguous or ascending within the values array. Only the sizes buffer keeps the
/// rows apart.
pub(crate) fn struct_list_fixture_as(rows: &[&[i32]], flavor: ListFlavor) -> ArrayRef {
    match flavor {
        ListFlavor::List => Arc::new(struct_list_fixture(rows)),
        ListFlavor::LargeList => {
            let flat: Vec<Option<i32>> = rows
                .iter()
                .flat_map(|r| r.iter().copied().map(Some))
                .collect();
            Arc::new(LargeListArray::new(
                item_field(false),
                OffsetBuffer::<i64>::from_lengths(rows.iter().map(|r| r.len())),
                Arc::new(elements_of(&flat)),
                None,
            ))
        }
        ListFlavor::ListView => Arc::new(back_to_front_list_view::<i32>(rows)),
        ListFlavor::LargeListView => Arc::new(back_to_front_list_view::<i64>(rows)),
    }
}

/// A list-view holding `rows` in reverse layout order: the last row's elements come first in the
/// values array, so the offsets buffer descends while each row's own elements stay in order.
fn back_to_front_list_view<O: OffsetSizeTrait>(rows: &[&[i32]]) -> GenericListViewArray<O> {
    let flat: Vec<Option<i32>> = rows
        .iter()
        .rev()
        .flat_map(|r| r.iter().copied().map(Some))
        .collect();
    let mut offsets = Vec::with_capacity(rows.len());
    let mut end = flat.len();
    for row in rows {
        end -= row.len();
        offsets.push(O::from_usize(end).expect("offset fits"));
    }
    let sizes: Vec<O> = rows
        .iter()
        .map(|r| O::from_usize(r.len()).expect("size fits"))
        .collect();
    GenericListViewArray::new(
        item_field(false),
        ScalarBuffer::from(offsets),
        ScalarBuffer::from(sizes),
        Arc::new(elements_of(&flat)),
        None,
    )
}
