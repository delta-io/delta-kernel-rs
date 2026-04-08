//! Extract a kernel [`Scalar`] from a single row of an Arrow array.
//!
//! This utility is primarily useful for connectors that partition data using Arrow arrays
//! and need to pass typed partition values to [`Transaction::partitioned_write_context`].
//!
//! # Example
//!
//! ```rust,ignore
//! use delta_kernel::engine::default::scalar_from_arrow::extract_scalar;
//! use delta_kernel::expressions::Scalar;
//! use arrow::array::Int32Array;
//!
//! let array = Int32Array::from(vec![Some(42), None, Some(7)]);
//! assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Integer(42));
//! assert_eq!(extract_scalar(&array, 1).unwrap(), Scalar::Null(DataType::INTEGER));
//! ```
//!
//! # Supported types
//!
//! All Delta primitive partition column types are supported:
//! - Integer, Long, Short, Byte, Float, Double, Boolean, String, Date,
//!   Timestamp (with timezone), TimestampNtz (without timezone), Decimal, Binary.
//!
//! Struct, Array, and Map types return an error (they cannot be partition columns).
//!
//! [`Scalar`]: crate::expressions::Scalar
//! [`Transaction::partitioned_write_context`]: crate::transaction::Transaction::partitioned_write_context

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimestampMicrosecondType,
};
use crate::arrow::array::Array;
use crate::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use crate::expressions::Scalar;
use crate::schema::DataType;
use crate::{DeltaResult, Error};

/// Extracts a kernel [`Scalar`] from the given row of an Arrow array.
///
/// Returns `Scalar::Null(data_type)` if the value at `row_idx` is null. Returns an error
/// for unsupported Arrow data types (e.g., nested types that cannot be partition columns).
///
/// This is useful for connectors that partition data using Arrow arrays and need typed
/// partition values for [`Transaction::partitioned_write_context`].
///
/// [`Transaction::partitioned_write_context`]: crate::transaction::Transaction::partitioned_write_context
pub fn extract_scalar(array: &dyn Array, row_idx: usize) -> DeltaResult<Scalar> {
    if array.is_null(row_idx) {
        return Ok(Scalar::Null(arrow_type_to_kernel_type(array.data_type())?));
    }
    match array.data_type() {
        ArrowDataType::Int8 => Ok(Scalar::Byte(
            array.as_primitive::<Int8Type>().value(row_idx),
        )),
        ArrowDataType::Int16 => Ok(Scalar::Short(
            array.as_primitive::<Int16Type>().value(row_idx),
        )),
        ArrowDataType::Int32 => Ok(Scalar::Integer(
            array.as_primitive::<Int32Type>().value(row_idx),
        )),
        ArrowDataType::Int64 => Ok(Scalar::Long(
            array.as_primitive::<Int64Type>().value(row_idx),
        )),
        ArrowDataType::Float32 => Ok(Scalar::Float(
            array.as_primitive::<Float32Type>().value(row_idx),
        )),
        ArrowDataType::Float64 => Ok(Scalar::Double(
            array.as_primitive::<Float64Type>().value(row_idx),
        )),
        ArrowDataType::Boolean => Ok(Scalar::Boolean(array.as_boolean().value(row_idx))),
        ArrowDataType::Utf8 => Ok(Scalar::String(
            array.as_string::<i32>().value(row_idx).to_string(),
        )),
        ArrowDataType::LargeUtf8 => Ok(Scalar::String(
            array.as_string::<i64>().value(row_idx).to_string(),
        )),
        ArrowDataType::Date32 => Ok(Scalar::Date(
            array.as_primitive::<Date32Type>().value(row_idx),
        )),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_tz)) => Ok(Scalar::Timestamp(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .value(row_idx),
        )),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Scalar::TimestampNtz(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .value(row_idx),
        )),
        ArrowDataType::Decimal128(precision, scale) => {
            let value = array.as_primitive::<Decimal128Type>().value(row_idx);
            Scalar::decimal(value, *precision, *scale as u8)
        }
        ArrowDataType::Binary => Ok(Scalar::Binary(
            array.as_binary::<i32>().value(row_idx).to_vec(),
        )),
        ArrowDataType::LargeBinary => Ok(Scalar::Binary(
            array.as_binary::<i64>().value(row_idx).to_vec(),
        )),
        other => Err(Error::generic(format!(
            "unsupported Arrow type for partition column: {other:?}"
        ))),
    }
}

/// Converts an Arrow [`ArrowDataType`] to a kernel [`DataType`] for null value construction.
/// Only supports primitive types valid for partition columns.
fn arrow_type_to_kernel_type(arrow_type: &ArrowDataType) -> DeltaResult<DataType> {
    match arrow_type {
        ArrowDataType::Int8 => Ok(DataType::BYTE),
        ArrowDataType::Int16 => Ok(DataType::SHORT),
        ArrowDataType::Int32 => Ok(DataType::INTEGER),
        ArrowDataType::Int64 => Ok(DataType::LONG),
        ArrowDataType::Float32 => Ok(DataType::FLOAT),
        ArrowDataType::Float64 => Ok(DataType::DOUBLE),
        ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(DataType::STRING),
        ArrowDataType::Date32 => Ok(DataType::DATE),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(DataType::TIMESTAMP),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
        ArrowDataType::Decimal128(p, s) => DataType::decimal(*p, *s as u8),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(DataType::BINARY),
        other => Err(Error::generic(format!(
            "unsupported Arrow type for partition column: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    };

    #[test]
    fn test_extract_scalar_integer() {
        let array = Int32Array::from(vec![42]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Integer(42));
    }

    #[test]
    fn test_extract_scalar_null_integer() {
        let array = Int32Array::from(vec![None::<i32>]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Null(DataType::INTEGER)
        );
    }

    #[test]
    fn test_extract_scalar_string() {
        let array = StringArray::from(vec!["hello"]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::String("hello".into())
        );
    }

    #[test]
    fn test_extract_scalar_boolean() {
        let array = BooleanArray::from(vec![true]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Boolean(true));
    }

    #[test]
    fn test_extract_scalar_long() {
        let array = Int64Array::from(vec![9_876_543_210i64]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Long(9_876_543_210)
        );
    }

    #[test]
    fn test_extract_scalar_short() {
        let array = Int16Array::from(vec![7i16]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Short(7));
    }

    #[test]
    fn test_extract_scalar_byte() {
        let array = Int8Array::from(vec![3i8]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Byte(3));
    }

    #[test]
    fn test_extract_scalar_float() {
        let array = Float32Array::from(vec![1.25f32]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Float(1.25));
    }

    #[test]
    fn test_extract_scalar_double() {
        let array = Float64Array::from(vec![99.99f64]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Double(99.99));
    }

    #[test]
    fn test_extract_scalar_date() {
        let array = Date32Array::from(vec![20178]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Date(20178));
    }

    #[test]
    fn test_extract_scalar_timestamp_with_tz() {
        let array = TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone("UTC");
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Timestamp(1_000_000)
        );
    }

    #[test]
    fn test_extract_scalar_timestamp_ntz() {
        let array = TimestampMicrosecondArray::from(vec![1_000_000i64]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::TimestampNtz(1_000_000)
        );
    }

    #[test]
    fn test_extract_scalar_decimal() {
        let array = Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::decimal(12345, 10, 2).unwrap()
        );
    }

    #[test]
    fn test_extract_scalar_binary() {
        let array = BinaryArray::from_vec(vec![b"Hello"]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Binary(b"Hello".to_vec())
        );
    }

    #[test]
    fn test_extract_scalar_null_string() {
        let array = StringArray::from(vec![None::<&str>]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Null(DataType::STRING)
        );
    }

    #[test]
    fn test_extract_scalar_multiple_rows() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Integer(1));
        assert_eq!(
            extract_scalar(&array, 1).unwrap(),
            Scalar::Null(DataType::INTEGER)
        );
        assert_eq!(extract_scalar(&array, 2).unwrap(), Scalar::Integer(3));
    }
}
