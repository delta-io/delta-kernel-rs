use crate::arrow::array::cast::AsArray;
use crate::arrow::array::{
    types::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, GenericBinaryType, GenericStringType,
        Int32Type, Int64Type, TimestampMicrosecondType,
    },
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

impl<'a> GetData<'a> for GenericByteArray<GenericBinaryType<i32>> {
    fn get_binary(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        Ok(self.is_valid(row_index).then(|| self.value(row_index)))
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
        Ok(self
            .is_valid(row_index)
            .then(|| ListItem::new(self, row_index)))
    }
}

impl<'a> GetData<'a> for MapArray {
    fn get_map(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<MapItem<'a>>> {
        Ok(self
            .is_valid(row_index)
            .then(|| MapItem::new(self, row_index)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
    };
    use crate::engine_data::GetData;

    // =========================================================================
    // Existing type tests (bool, int, long, str)
    // =========================================================================

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

    // =========================================================================
    // New type tests (float, double, date, timestamp, decimal)
    // =========================================================================

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
    // Wrong-type error tests: calling the wrong getter returns an error
    // =========================================================================

    #[test]
    fn test_wrong_type_returns_error() {
        let int_array = Int32Array::from(vec![Some(42)]);

        // Calling get_float on an Int32Array should error
        assert!(int_array.get_float(0, "f").is_err());
        assert!(int_array.get_double(0, "f").is_err());
        assert!(int_array.get_long(0, "f").is_err());
        assert!(int_array.get_decimal(0, "f").is_err());

        let float_array = Float32Array::from(vec![Some(1.0f32)]);
        assert!(float_array.get_int(0, "f").is_err());
        assert!(float_array.get_double(0, "f").is_err());
    }
}
