use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{
    types::{GenericBinaryType, GenericStringType, Int32Type, Int64Type},
    Array, BooleanArray, GenericByteArray, GenericListArray, MapArray, OffsetSizeTrait,
    PrimitiveArray, RunArray,
};

use crate::{
    engine_data::{GetData, ListItem, MapItem},
    DeltaResult, Error,
};

// actual impls (todo: could macro these)

impl GetData<'_> for BooleanArray {
    fn get_bool(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<bool>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl GetData<'_> for PrimitiveArray<Int32Type> {
    fn get_int(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i32>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl GetData<'_> for PrimitiveArray<Int64Type> {
    fn get_long(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i64>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericStringType<i32>> {
    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericBinaryType<i32>> {
    fn get_binary(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a, OffsetSize> GetData<'a> for GenericListArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn get_list(
        &'a self,
        row_index: usize,
        _field_name: &str,
    ) -> DeltaResult<Option<ListItem<'a>>> {
        if self.is_valid(row_index) {
            Ok(Some(ListItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for MapArray {
    fn get_map(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<MapItem<'a>>> {
        if self.is_valid(row_index) {
            Ok(Some(MapItem::new(self, row_index)))
        } else {
            Ok(None)
        }
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
            "Row index {} out of bounds for field '{}'",
            row_index, field_name
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
