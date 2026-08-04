use std::ops::Range;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, GenericBinaryType, GenericStringType,
    Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
};
use crate::arrow::array::{
    Array, BinaryViewArray, BooleanArray, GenericByteArray, GenericListArray, GenericListViewArray,
    MapArray, OffsetSizeTrait, PrimitiveArray, RunArray, StringViewArray,
};
use crate::engine::arrow_data::{as_string_accessor, ArrowEngineData};
use crate::engine_data::{
    EngineData, GetData, ListItem, MapItem, RowVisitor, StructList, StructListAccessor,
};
use crate::schema::ColumnName;
use crate::{DeltaResult, Error};

// actual impls (todo: could macro these)

impl GetData<'_> for BooleanArray {
    fn get_bool(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<bool>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Int8Type> {
    fn get_byte(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i8>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Int16Type> {
    fn get_short(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i16>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Int32Type> {
    fn get_int(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i32>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Int64Type> {
    fn get_long(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i64>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Float32Type> {
    fn get_float(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<f32>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Float64Type> {
    fn get_double(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<f64>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Date32Type> {
    fn get_date(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i32>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<TimestampMicrosecondType> {
    fn get_timestamp(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i64>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl GetData<'_> for PrimitiveArray<Decimal128Type> {
    fn get_decimal(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i128>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericStringType<i32>> {
    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericStringType<i64>> {
    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl<'a> GetData<'a> for StringViewArray {
    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericBinaryType<i32>> {
    fn get_binary(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericBinaryType<i64>> {
    fn get_binary(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

impl<'a> GetData<'a> for BinaryViewArray {
    fn get_binary(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
    }
}

/// Uniform access to list-like Arrow arrays (List, LargeList, ListView, LargeListView),
/// abstracting away differences in how each type computes per-row offsets.
trait ListLikeArray: Array {
    fn list_values(&self) -> &dyn Array;
    fn row_offsets(&self, row: usize) -> Range<usize>;
}

impl<O: OffsetSizeTrait> ListLikeArray for GenericListArray<O> {
    fn list_values(&self) -> &dyn Array {
        self.values().as_ref()
    }
    fn row_offsets(&self, row: usize) -> Range<usize> {
        self.offsets()[row].as_usize()..self.offsets()[row + 1].as_usize()
    }
}

impl<O: OffsetSizeTrait> ListLikeArray for GenericListViewArray<O> {
    fn list_values(&self) -> &dyn Array {
        self.values().as_ref()
    }
    fn row_offsets(&self, row: usize) -> Range<usize> {
        let start = self.offsets()[row].as_usize();
        start..start + self.sizes()[row].as_usize()
    }
}

fn get_list_item<'a>(
    list: &'a impl ListLikeArray,
    row_index: usize,
    field_name: &str,
) -> DeltaResult<Option<ListItem<'a>>> {
    if !list.is_valid(row_index) {
        return Ok(None);
    }
    let values = as_string_accessor(list.list_values()).ok_or_else(|| {
        Error::unexpected_column_type(format!(
            "{field_name}: list values are not a supported string type"
        ))
    })?;
    Ok(Some(ListItem::new(values, list.row_offsets(row_index))))
}

impl<T: ListLikeArray> StructListAccessor for T {
    fn visit_row_elements(
        &self,
        row_index: usize,
        column_names: &[ColumnName],
        visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()> {
        let values = self.list_values().as_struct_opt().ok_or_else(|| {
            Error::unexpected_column_type("struct list values are not structs".to_string())
        })?;
        // Slice to this row's elements and reuse the standard visit machinery.
        let offsets = self.row_offsets(row_index);
        let sliced = values.slice(offsets.start, offsets.len());
        // A RecordBatch cannot represent a null struct row, so `ArrowEngineData::from` would panic
        // on one; reject nulls here instead.
        if sliced.null_count() > 0 {
            return Err(Error::unexpected_column_type(
                "struct-list elements contain nulls; cannot visit".to_string(),
            ));
        }
        ArrowEngineData::from(sliced).visit_rows(column_names, visitor)
    }
}

/// Shared implementation of [`GetData::get_struct_list`] for the list array flavors.
fn get_struct_list_item<'a>(
    list: &'a impl ListLikeArray,
    row_index: usize,
    field_name: &str,
) -> DeltaResult<Option<StructList<'a>>> {
    // Check the element type before the null check: a non-struct list is a type error for every
    // row, even a null one. `visit_with` would also catch it, but too late to name `field_name`.
    if list.list_values().as_struct_opt().is_none() {
        return Err(Error::unexpected_column_type(format!(
            "{field_name}: list values are not structs"
        )));
    }
    if !list.is_valid(row_index) {
        return Ok(None);
    }
    Ok(Some(StructList::new(list, row_index)))
}

impl<'a, OffsetSize: OffsetSizeTrait> GetData<'a> for GenericListArray<OffsetSize> {
    fn get_list(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<ListItem<'a>>> {
        get_list_item(self, row_index, field_name)
    }
    fn get_struct_list(
        &'a self,
        row_index: usize,
        field_name: &str,
    ) -> DeltaResult<Option<StructList<'a>>> {
        get_struct_list_item(self, row_index, field_name)
    }
}

impl<'a, OffsetSize: OffsetSizeTrait> GetData<'a> for GenericListViewArray<OffsetSize> {
    fn get_list(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<ListItem<'a>>> {
        get_list_item(self, row_index, field_name)
    }
    fn get_struct_list(
        &'a self,
        row_index: usize,
        field_name: &str,
    ) -> DeltaResult<Option<StructList<'a>>> {
        get_struct_list_item(self, row_index, field_name)
    }
}

impl<'a> GetData<'a> for MapArray {
    fn get_map(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<MapItem<'a>>> {
        if !self.is_valid(row_index) {
            return Ok(None);
        }
        let keys = as_string_accessor(self.keys().as_ref()).ok_or_else(|| {
            Error::unexpected_column_type(format!(
                "{field_name}: map keys are not a supported string type"
            ))
        })?;
        let values = as_string_accessor(self.values().as_ref()).ok_or_else(|| {
            Error::unexpected_column_type(format!(
                "{field_name}: map values are not a supported string type"
            ))
        })?;
        let start = self.offsets()[row_index] as usize;
        let end = self.offsets()[row_index + 1] as usize;
        Ok(Some(MapItem::new(keys, values, start..end)))
    }
}

/// Validates row index and returns physical index into the values array.
///
/// Per Arrow spec, REE parent array has no validity bitmap (null_count = 0).
/// Nulls are encoded in the values child array, so null checking must be done
/// on the values array in each get_* method, not here on the parent array.
fn validate_and_get_physical_index(
    run_array: &RunArray<Int64Type>,
    row_index: usize,
    field_name: &str,
) -> DeltaResult<usize> {
    if row_index >= run_array.len() {
        return Err(Error::generic(format!(
            "Row index {row_index} out of bounds for field '{field_name}'"
        )));
    }

    let physical_idx = run_array.run_ends().get_physical_index(row_index);
    Ok(physical_idx)
}

/// Implement GetData for RunArray directly, so we can return it as a trait object
/// without needing a wrapper struct or Box::leak.
///
/// This implementation supports multiple value types (strings, integers, booleans, etc.)
/// by runtime downcasting of the values array.
impl<'a> GetData<'a> for RunArray<Int64Type> {
    fn get_str(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<&'a str>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_any()
            .downcast_ref::<GenericByteArray<GenericStringType<i32>>>()
            .ok_or_else(|| {
                Error::generic(format!(
                    "Expected StringArray values in RunArray, got {:?}",
                    self.values().data_type()
                ))
            })?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_int(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<i32>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| {
                Error::generic(format!(
                    "Expected Int32Array values in RunArray, got {:?}",
                    self.values().data_type()
                ))
            })?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_long(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<i64>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| {
                Error::generic(format!(
                    "Expected Int64Array values in RunArray, got {:?}",
                    self.values().data_type()
                ))
            })?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_bool(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<bool>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self.values().as_boolean_opt().ok_or_else(|| {
            Error::generic(format!(
                "Expected BooleanArray values in RunArray, got {:?}",
                self.values().data_type()
            ))
        })?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_binary(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_any()
            .downcast_ref::<GenericByteArray<GenericBinaryType<i32>>>()
            .ok_or_else(|| {
                Error::generic(format!(
                    "Expected BinaryArray values in RunArray, got {:?}",
                    self.values().data_type()
                ))
            })?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;

    use super::*;
    use crate::arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeBinaryArray, LargeStringArray, ListArray, ListBuilder, PrimitiveArray,
        StringBuilder, StructArray,
    };
    use crate::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
    use crate::engine::struct_list_test_support::{struct_list_fixture, CollectNVisitor};
    use crate::engine_data::GetData;

    // =========================================================================
    // Scalar type tests
    // =========================================================================

    #[test]
    fn test_get_byte() {
        let array = Int8Array::from(vec![Some(i8::MAX), Some(i8::MIN), None]);
        assert_eq!(array.get_byte(0, "f").unwrap(), Some(i8::MAX));
        assert_eq!(array.get_byte(1, "f").unwrap(), Some(i8::MIN));
        assert_eq!(array.get_byte(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_short() {
        let array = Int16Array::from(vec![Some(i16::MAX), Some(i16::MIN), None]);
        assert_eq!(array.get_short(0, "f").unwrap(), Some(i16::MAX));
        assert_eq!(array.get_short(1, "f").unwrap(), Some(i16::MIN));
        assert_eq!(array.get_short(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_bool() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(array.get_bool(0, "f").unwrap(), Some(true));
        assert_eq!(array.get_bool(1, "f").unwrap(), Some(false));
        assert_eq!(array.get_bool(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_int() {
        let array = Int32Array::from(vec![Some(42), Some(-1), None]);
        assert_eq!(array.get_int(0, "f").unwrap(), Some(42));
        assert_eq!(array.get_int(1, "f").unwrap(), Some(-1));
        assert_eq!(array.get_int(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_long() {
        let array = Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN), None]);
        assert_eq!(array.get_long(0, "f").unwrap(), Some(i64::MAX));
        assert_eq!(array.get_long(1, "f").unwrap(), Some(i64::MIN));
        assert_eq!(array.get_long(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_float() {
        let array = Float32Array::from(vec![Some(1.5f32), Some(-0.0), None]);
        assert_eq!(array.get_float(0, "f").unwrap(), Some(1.5f32));
        assert_eq!(array.get_float(1, "f").unwrap(), Some(-0.0f32));
        assert_eq!(array.get_float(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_float_special_values() {
        let array = Float32Array::from(vec![
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
        ]);
        assert!(array.get_float(0, "f").unwrap().unwrap().is_nan());
        assert_eq!(array.get_float(1, "f").unwrap(), Some(f32::INFINITY));
        assert_eq!(array.get_float(2, "f").unwrap(), Some(f32::NEG_INFINITY));
    }

    #[test]
    fn test_get_double() {
        let array = Float64Array::from(vec![Some(1.23), Some(-4.56), None]);
        assert_eq!(array.get_double(0, "f").unwrap(), Some(1.23));
        assert_eq!(array.get_double(1, "f").unwrap(), Some(-4.56));
        assert_eq!(array.get_double(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_double_special_values() {
        let array = Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]);
        assert!(array.get_double(0, "f").unwrap().unwrap().is_nan());
        assert_eq!(array.get_double(1, "f").unwrap(), Some(f64::INFINITY));
        assert_eq!(array.get_double(2, "f").unwrap(), Some(f64::NEG_INFINITY));
    }

    #[test]
    fn test_get_date() {
        // Date32 stores days since epoch
        let array = PrimitiveArray::<Date32Type>::from(vec![Some(0), Some(19000), None]);
        assert_eq!(array.get_date(0, "f").unwrap(), Some(0));
        assert_eq!(array.get_date(1, "f").unwrap(), Some(19000));
        assert_eq!(array.get_date(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_timestamp() {
        // TimestampMicrosecond stores microseconds since epoch
        let array = PrimitiveArray::<TimestampMicrosecondType>::from(vec![
            Some(1_000_000),
            Some(-1_000_000),
            None,
        ]);
        assert_eq!(array.get_timestamp(0, "f").unwrap(), Some(1_000_000));
        assert_eq!(array.get_timestamp(1, "f").unwrap(), Some(-1_000_000));
        assert_eq!(array.get_timestamp(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_decimal() {
        // Decimal128 stores as i128
        let array =
            PrimitiveArray::<Decimal128Type>::from(vec![Some(12345_i128), Some(-99999_i128), None]);
        assert_eq!(array.get_decimal(0, "f").unwrap(), Some(12345));
        assert_eq!(array.get_decimal(1, "f").unwrap(), Some(-99999));
        assert_eq!(array.get_decimal(2, "f").unwrap(), None);
    }

    // =========================================================================
    // Alternative Arrow representations for STRING and BINARY:
    //   STRING -> LargeUtf8 (LargeStringArray), Utf8View (StringViewArray)
    //   BINARY -> LargeBinary (LargeBinaryArray), BinaryView (BinaryViewArray)
    // =========================================================================

    #[test]
    fn test_get_str_large_string() {
        let array = LargeStringArray::from(vec![Some("hello"), Some("world"), None]);
        assert_eq!(array.get_str(0, "f").unwrap(), Some("hello"));
        assert_eq!(array.get_str(1, "f").unwrap(), Some("world"));
        assert_eq!(array.get_str(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_str_string_view() {
        let array = StringViewArray::from(vec![Some("hello"), Some("world"), None]);
        assert_eq!(array.get_str(0, "f").unwrap(), Some("hello"));
        assert_eq!(array.get_str(1, "f").unwrap(), Some("world"));
        assert_eq!(array.get_str(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_binary_large_binary() {
        let array = LargeBinaryArray::from(vec![Some(b"abc" as &[u8]), Some(b"xyz"), None]);
        assert_eq!(array.get_binary(0, "f").unwrap(), Some(b"abc" as &[u8]));
        assert_eq!(array.get_binary(1, "f").unwrap(), Some(b"xyz" as &[u8]));
        assert_eq!(array.get_binary(2, "f").unwrap(), None);
    }

    #[test]
    fn test_get_binary_binary_view() {
        let array = BinaryViewArray::from(vec![Some(b"abc" as &[u8]), Some(b"xyz"), None]);
        assert_eq!(array.get_binary(0, "f").unwrap(), Some(b"abc" as &[u8]));
        assert_eq!(array.get_binary(1, "f").unwrap(), Some(b"xyz" as &[u8]));
        assert_eq!(array.get_binary(2, "f").unwrap(), None);
    }

    // =========================================================================
    // Wrong-type error tests: calling the wrong getter returns an error
    // =========================================================================

    // =========================================================================
    // Array-of-structs: get_struct_list drives a nested RowVisitor
    // =========================================================================

    #[test]
    fn test_get_struct_list_visits_element_structs() {
        // Two outer rows: row 0 has elements [10, 20], row 1 has [30].
        let list = struct_list_fixture(&[&[10, 20], &[30]]);

        let mut row0 = CollectNVisitor::default();
        list.get_struct_list(0, "arr")
            .unwrap()
            .unwrap()
            .visit_with(&mut row0)
            .unwrap();
        assert_eq!(row0.values, vec![10, 20]);

        let mut row1 = CollectNVisitor::default();
        list.get_struct_list(1, "arr")
            .unwrap()
            .unwrap()
            .visit_with(&mut row1)
            .unwrap();
        assert_eq!(row1.values, vec![30]);
    }

    #[test]
    fn test_get_struct_list_on_string_list_errors() {
        let array = LargeStringArray::from(vec![Some("hello")]);
        assert!(array.get_struct_list(0, "f").is_err());
    }

    /// A non-struct element type is a type error for every row, including a null one -- the
    /// element-type check must not sit behind the nullity short-circuit.
    #[rstest]
    #[case::null_row(0)]
    #[case::present_row(1)]
    fn test_get_struct_list_on_list_of_strings_errors(#[case] row: usize) {
        // A `List<Utf8>` whose row 0 is null and row 1 is present.
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_null();
        builder.append_value([Some("a")]);
        let list: ListArray = builder.finish();

        assert!(list.get_struct_list(row, "arr").is_err());
    }

    #[test]
    fn test_get_struct_list_null_element_struct_errors_not_panics() {
        // Element structs [present, null] in a single outer row [0..2].
        let n = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let element_fields: Fields = vec![ArrowField::new("n", ArrowDataType::Int32, false)].into();
        let elements = StructArray::new(
            element_fields.clone(),
            vec![n],
            Some(NullBuffer::from(vec![true, false])),
        );
        let item_field = Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Struct(element_fields),
            true,
        ));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2]));
        let list = GenericListArray::<i32>::new(item_field, offsets, Arc::new(elements), None);

        let mut visitor = CollectNVisitor::default();
        let result = list
            .get_struct_list(0, "arr")
            .unwrap()
            .unwrap()
            .visit_with(&mut visitor);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_struct_list_null_outer_row_is_none_and_empty_slice_visits_nothing() {
        // Two outer rows: row 0 null, row 1 present but empty (offsets [0, 0, 0]).
        let n = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let element_fields: Fields = vec![ArrowField::new("n", ArrowDataType::Int32, false)].into();
        let elements = StructArray::new(element_fields.clone(), vec![n], None);
        let item_field = Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Struct(element_fields),
            false,
        ));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0, 0]));
        let list = GenericListArray::<i32>::new(
            item_field,
            offsets,
            Arc::new(elements),
            Some(NullBuffer::from(vec![false, true])),
        );

        // Null outer row -> Ok(None).
        assert!(list.get_struct_list(0, "arr").unwrap().is_none());

        // Present-but-empty row -> visits zero rows.
        let mut visitor = CollectNVisitor::default();
        list.get_struct_list(1, "arr")
            .unwrap()
            .unwrap()
            .visit_with(&mut visitor)
            .unwrap();
        assert!(visitor.values.is_empty());
    }

    #[test]
    fn test_wrong_type_returns_error() {
        let int_array = Int32Array::from(vec![Some(42)]);

        // Calling the wrong getter on an Int32Array should error
        assert!(int_array.get_byte(0, "f").is_err());
        assert!(int_array.get_short(0, "f").is_err());
        assert!(int_array.get_float(0, "f").is_err());
        assert!(int_array.get_double(0, "f").is_err());
        assert!(int_array.get_long(0, "f").is_err());
        assert!(int_array.get_decimal(0, "f").is_err());

        let float_array = Float32Array::from(vec![Some(1.0f32)]);
        assert!(float_array.get_int(0, "f").is_err());
        assert!(float_array.get_double(0, "f").is_err());
    }
}
