//! Extract a kernel [`Scalar`] from a single row of an Arrow array.
//!
//! This utility is primarily useful for connectors that partition data using Arrow arrays
//! and need to pass typed partition values to the write path (e.g., partitioned writes).
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
/// partition values for the write path (e.g., partitioned writes).
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
        new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeStringArray, StringArray, StructArray, TimestampMicrosecondArray,
    };
    use crate::arrow::datatypes::{Field, Fields};

    use rstest::rstest;
    use std::sync::Arc;

    // ============================================================================
    // extract_scalar: non-null values for each supported type
    // ============================================================================

    #[rstest]
    #[case::byte(
        Arc::new(Int8Array::from(vec![3i8])) as ArrayRef,
        Scalar::Byte(3)
    )]
    #[case::short(
        Arc::new(Int16Array::from(vec![7i16])) as ArrayRef,
        Scalar::Short(7)
    )]
    #[case::integer(
        Arc::new(Int32Array::from(vec![42])) as ArrayRef,
        Scalar::Integer(42)
    )]
    #[case::long(
        Arc::new(Int64Array::from(vec![9_876_543_210i64])) as ArrayRef,
        Scalar::Long(9_876_543_210)
    )]
    #[case::float(
        Arc::new(Float32Array::from(vec![1.25f32])) as ArrayRef,
        Scalar::Float(1.25)
    )]
    #[case::double(
        Arc::new(Float64Array::from(vec![99.99f64])) as ArrayRef,
        Scalar::Double(99.99)
    )]
    #[case::boolean_true(
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        Scalar::Boolean(true)
    )]
    #[case::boolean_false(
        Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        Scalar::Boolean(false)
    )]
    #[case::string(
        Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
        Scalar::String("hello".into())
    )]
    #[case::date(
        Arc::new(Date32Array::from(vec![20178])) as ArrayRef,
        Scalar::Date(20178)
    )]
    #[case::binary(
        Arc::new(BinaryArray::from_vec(vec![b"Hello"])) as ArrayRef,
        Scalar::Binary(b"Hello".to_vec())
    )]
    fn test_extract_scalar_non_null_returns_typed_value(
        #[case] array: ArrayRef,
        #[case] expected: Scalar,
    ) {
        assert_eq!(extract_scalar(array.as_ref(), 0).unwrap(), expected);
    }

    // ============================================================================
    // extract_scalar: integer boundary values
    // ============================================================================

    #[rstest]
    #[case::byte_min(
        Arc::new(Int8Array::from(vec![i8::MIN])) as ArrayRef,
        Scalar::Byte(i8::MIN)
    )]
    #[case::byte_max(
        Arc::new(Int8Array::from(vec![i8::MAX])) as ArrayRef,
        Scalar::Byte(i8::MAX)
    )]
    #[case::short_min(
        Arc::new(Int16Array::from(vec![i16::MIN])) as ArrayRef,
        Scalar::Short(i16::MIN)
    )]
    #[case::short_max(
        Arc::new(Int16Array::from(vec![i16::MAX])) as ArrayRef,
        Scalar::Short(i16::MAX)
    )]
    #[case::int_min(
        Arc::new(Int32Array::from(vec![i32::MIN])) as ArrayRef,
        Scalar::Integer(i32::MIN)
    )]
    #[case::int_max(
        Arc::new(Int32Array::from(vec![i32::MAX])) as ArrayRef,
        Scalar::Integer(i32::MAX)
    )]
    #[case::long_min(
        Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef,
        Scalar::Long(i64::MIN)
    )]
    #[case::long_max(
        Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef,
        Scalar::Long(i64::MAX)
    )]
    fn test_extract_scalar_integer_boundary_returns_correct_value(
        #[case] array: ArrayRef,
        #[case] expected: Scalar,
    ) {
        assert_eq!(extract_scalar(array.as_ref(), 0).unwrap(), expected);
    }

    // ============================================================================
    // extract_scalar: floating point edge cases
    // ============================================================================

    #[rstest]
    #[case::float_neg_zero(Arc::new(Float32Array::from(vec![-0.0f32])) as ArrayRef)]
    #[case::double_neg_zero(Arc::new(Float64Array::from(vec![-0.0f64])) as ArrayRef)]
    fn test_extract_scalar_negative_zero_returns_scalar(#[case] array: ArrayRef) {
        let result = extract_scalar(array.as_ref(), 0).unwrap();
        assert!(!result.is_null());
    }

    #[test]
    fn test_extract_scalar_float_nan_returns_nan() {
        let array = Float32Array::from(vec![f32::NAN]);
        match extract_scalar(&array, 0).unwrap() {
            Scalar::Float(v) => assert!(v.is_nan()),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_scalar_double_nan_returns_nan() {
        let array = Float64Array::from(vec![f64::NAN]);
        match extract_scalar(&array, 0).unwrap() {
            Scalar::Double(v) => assert!(v.is_nan()),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[rstest]
    #[case::float_inf(
        Arc::new(Float32Array::from(vec![f32::INFINITY])) as ArrayRef,
        Scalar::Float(f32::INFINITY)
    )]
    #[case::float_neg_inf(
        Arc::new(Float32Array::from(vec![f32::NEG_INFINITY])) as ArrayRef,
        Scalar::Float(f32::NEG_INFINITY)
    )]
    #[case::double_inf(
        Arc::new(Float64Array::from(vec![f64::INFINITY])) as ArrayRef,
        Scalar::Double(f64::INFINITY)
    )]
    #[case::double_neg_inf(
        Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])) as ArrayRef,
        Scalar::Double(f64::NEG_INFINITY)
    )]
    fn test_extract_scalar_infinity_returns_correct_value(
        #[case] array: ArrayRef,
        #[case] expected: Scalar,
    ) {
        assert_eq!(extract_scalar(array.as_ref(), 0).unwrap(), expected);
    }

    // ============================================================================
    // extract_scalar: timestamp variants
    // ============================================================================

    #[test]
    fn test_extract_scalar_timestamp_with_tz_returns_timestamp() {
        let array = TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone("UTC");
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Timestamp(1_000_000)
        );
    }

    #[test]
    fn test_extract_scalar_timestamp_without_tz_returns_timestamp_ntz() {
        let array = TimestampMicrosecondArray::from(vec![1_000_000i64]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::TimestampNtz(1_000_000)
        );
    }

    #[test]
    fn test_extract_scalar_timestamp_non_utc_tz_returns_timestamp() {
        let array =
            TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone("America/New_York");
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Timestamp(1_000_000)
        );
    }

    // ============================================================================
    // extract_scalar: decimal
    // ============================================================================

    #[test]
    fn test_extract_scalar_decimal_returns_correct_precision_and_scale() {
        let array = Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::decimal(12345, 10, 2).unwrap()
        );
    }

    #[test]
    fn test_extract_scalar_decimal_zero_scale_returns_whole_number() {
        let array = Decimal128Array::from(vec![42i128])
            .with_precision_and_scale(5, 0)
            .unwrap();
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::decimal(42, 5, 0).unwrap()
        );
    }

    #[test]
    fn test_extract_scalar_decimal_negative_returns_negative_value() {
        let array = Decimal128Array::from(vec![-5i128])
            .with_precision_and_scale(3, 2)
            .unwrap();
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::decimal(-5, 3, 2).unwrap()
        );
    }

    // ============================================================================
    // extract_scalar: Large variants (LargeUtf8, LargeBinary)
    // ============================================================================

    #[test]
    fn test_extract_scalar_large_utf8_returns_string() {
        let array = LargeStringArray::from(vec!["large string"]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::String("large string".into())
        );
    }

    #[test]
    fn test_extract_scalar_large_binary_returns_binary() {
        let array = LargeBinaryArray::from(vec![b"large bytes".as_ref()]);
        assert_eq!(
            extract_scalar(&array, 0).unwrap(),
            Scalar::Binary(b"large bytes".to_vec())
        );
    }

    // ============================================================================
    // extract_scalar: null values across all supported types
    // ============================================================================

    #[rstest]
    #[case::int8(ArrowDataType::Int8, DataType::BYTE)]
    #[case::int16(ArrowDataType::Int16, DataType::SHORT)]
    #[case::int32(ArrowDataType::Int32, DataType::INTEGER)]
    #[case::int64(ArrowDataType::Int64, DataType::LONG)]
    #[case::float32(ArrowDataType::Float32, DataType::FLOAT)]
    #[case::float64(ArrowDataType::Float64, DataType::DOUBLE)]
    #[case::boolean(ArrowDataType::Boolean, DataType::BOOLEAN)]
    #[case::utf8(ArrowDataType::Utf8, DataType::STRING)]
    #[case::large_utf8(ArrowDataType::LargeUtf8, DataType::STRING)]
    #[case::date32(ArrowDataType::Date32, DataType::DATE)]
    #[case::timestamp_tz(
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::TIMESTAMP
    )]
    #[case::timestamp_ntz(
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::TIMESTAMP_NTZ
    )]
    #[case::binary(ArrowDataType::Binary, DataType::BINARY)]
    #[case::large_binary(ArrowDataType::LargeBinary, DataType::BINARY)]
    fn test_extract_scalar_null_returns_typed_null(
        #[case] arrow_type: ArrowDataType,
        #[case] expected_kernel_type: DataType,
    ) {
        let array = new_null_array(&arrow_type, 1);
        assert_eq!(
            extract_scalar(array.as_ref(), 0).unwrap(),
            Scalar::Null(expected_kernel_type)
        );
    }

    #[test]
    fn test_extract_scalar_null_decimal_returns_typed_null() {
        let array = new_null_array(&ArrowDataType::Decimal128(10, 2), 1);
        assert_eq!(
            extract_scalar(array.as_ref(), 0).unwrap(),
            Scalar::Null(DataType::decimal(10, 2).unwrap())
        );
    }

    // ============================================================================
    // extract_scalar: multi-row arrays
    // ============================================================================

    #[test]
    fn test_extract_scalar_multi_row_selects_correct_index() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        assert_eq!(extract_scalar(&array, 0).unwrap(), Scalar::Integer(1));
        assert_eq!(
            extract_scalar(&array, 1).unwrap(),
            Scalar::Null(DataType::INTEGER)
        );
        assert_eq!(extract_scalar(&array, 2).unwrap(), Scalar::Integer(3));
    }

    // ============================================================================
    // extract_scalar: unsupported types return error
    // ============================================================================

    #[test]
    fn test_extract_scalar_struct_type_returns_error() {
        let fields = Fields::from(vec![Field::new("a", ArrowDataType::Int32, false)]);
        let array = StructArray::new_null(fields, 1);
        let result = extract_scalar(&array, 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported Arrow type"), "got: {msg}");
    }

    #[test]
    fn test_extract_scalar_list_type_returns_error() {
        let list_type =
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true)));
        let array = new_null_array(&list_type, 1);
        let result = extract_scalar(array.as_ref(), 0);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported Arrow type"), "got: {msg}");
    }

    // ============================================================================
    // arrow_type_to_kernel_type: verify mapping for all supported types
    // ============================================================================

    #[rstest]
    #[case::int8(ArrowDataType::Int8, DataType::BYTE)]
    #[case::int16(ArrowDataType::Int16, DataType::SHORT)]
    #[case::int32(ArrowDataType::Int32, DataType::INTEGER)]
    #[case::int64(ArrowDataType::Int64, DataType::LONG)]
    #[case::float32(ArrowDataType::Float32, DataType::FLOAT)]
    #[case::float64(ArrowDataType::Float64, DataType::DOUBLE)]
    #[case::boolean(ArrowDataType::Boolean, DataType::BOOLEAN)]
    #[case::utf8(ArrowDataType::Utf8, DataType::STRING)]
    #[case::large_utf8(ArrowDataType::LargeUtf8, DataType::STRING)]
    #[case::date32(ArrowDataType::Date32, DataType::DATE)]
    #[case::timestamp_tz(
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::TIMESTAMP
    )]
    #[case::timestamp_ntz(
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::TIMESTAMP_NTZ
    )]
    #[case::binary(ArrowDataType::Binary, DataType::BINARY)]
    #[case::large_binary(ArrowDataType::LargeBinary, DataType::BINARY)]
    fn test_arrow_type_to_kernel_type_returns_correct_mapping(
        #[case] arrow_type: ArrowDataType,
        #[case] expected: DataType,
    ) {
        assert_eq!(arrow_type_to_kernel_type(&arrow_type).unwrap(), expected);
    }

    #[test]
    fn test_arrow_type_to_kernel_type_decimal_preserves_precision_and_scale() {
        assert_eq!(
            arrow_type_to_kernel_type(&ArrowDataType::Decimal128(18, 5)).unwrap(),
            DataType::decimal(18, 5).unwrap()
        );
    }

    #[rstest]
    #[case::struct_type(ArrowDataType::Struct(Fields::empty()))]
    #[case::list_type(ArrowDataType::List(Arc::new(Field::new(
        "item",
        ArrowDataType::Int32,
        true
    ))))]
    #[case::timestamp_seconds(ArrowDataType::Timestamp(TimeUnit::Second, None))]
    #[case::timestamp_millis(ArrowDataType::Timestamp(TimeUnit::Millisecond, None))]
    #[case::timestamp_nanos(ArrowDataType::Timestamp(TimeUnit::Nanosecond, None))]
    fn test_arrow_type_to_kernel_type_unsupported_returns_error(#[case] arrow_type: ArrowDataType) {
        let result = arrow_type_to_kernel_type(&arrow_type);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported Arrow type"), "got: {msg}");
    }

    // ============================================================================
    // Roundtrip: Arrow -> extract_scalar -> serialize -> parse_scalar -> compare
    // ============================================================================
    //
    // Validates the full connector pipeline: Arrow array value is extracted as a Scalar,
    // serialized to a partition value string, then parsed back. The result must match
    // the originally extracted Scalar.

    use crate::partition::serialization::serialize_partition_value;
    use crate::schema::PrimitiveType;

    /// Maps an Arrow data type to the corresponding kernel PrimitiveType for parse_scalar.
    fn arrow_to_primitive_type(arrow_type: &ArrowDataType) -> PrimitiveType {
        match arrow_type {
            ArrowDataType::Int8 => PrimitiveType::Byte,
            ArrowDataType::Int16 => PrimitiveType::Short,
            ArrowDataType::Int32 => PrimitiveType::Integer,
            ArrowDataType::Int64 => PrimitiveType::Long,
            ArrowDataType::Float32 => PrimitiveType::Float,
            ArrowDataType::Float64 => PrimitiveType::Double,
            ArrowDataType::Boolean => PrimitiveType::Boolean,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => PrimitiveType::String,
            ArrowDataType::Date32 => PrimitiveType::Date,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => PrimitiveType::Timestamp,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => PrimitiveType::TimestampNtz,
            ArrowDataType::Decimal128(p, s) => {
                PrimitiveType::decimal(*p, *s as u8).expect("valid decimal")
            }
            ArrowDataType::Binary | ArrowDataType::LargeBinary => PrimitiveType::Binary,
            other => panic!("unsupported Arrow type in test helper: {other:?}"),
        }
    }

    #[rstest]
    #[case::byte(Arc::new(Int8Array::from(vec![42i8])) as ArrayRef)]
    #[case::byte_min(Arc::new(Int8Array::from(vec![i8::MIN])) as ArrayRef)]
    #[case::short(Arc::new(Int16Array::from(vec![1234i16])) as ArrayRef)]
    #[case::integer(Arc::new(Int32Array::from(vec![42])) as ArrayRef)]
    #[case::integer_min(Arc::new(Int32Array::from(vec![i32::MIN])) as ArrayRef)]
    #[case::long(Arc::new(Int64Array::from(vec![9_876_543_210i64])) as ArrayRef)]
    #[case::long_max(Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef)]
    #[case::boolean_true(Arc::new(BooleanArray::from(vec![true])) as ArrayRef)]
    #[case::boolean_false(Arc::new(BooleanArray::from(vec![false])) as ArrayRef)]
    #[case::string(Arc::new(StringArray::from(vec!["hello world"])) as ArrayRef)]
    #[case::string_special_chars(Arc::new(StringArray::from(vec!["US/East"])) as ArrayRef)]
    #[case::date(Arc::new(Date32Array::from(vec![20178])) as ArrayRef)]
    #[case::date_epoch(Arc::new(Date32Array::from(vec![0])) as ArrayRef)]
    #[case::date_pre_epoch(Arc::new(Date32Array::from(vec![-1])) as ArrayRef)]
    #[case::timestamp_tz(
        Arc::new(TimestampMicrosecondArray::from(vec![1_743_436_200_000_000i64]).with_timezone("UTC")) as ArrayRef
    )]
    #[case::timestamp_ntz(Arc::new(TimestampMicrosecondArray::from(vec![1_743_436_200_123_456i64])) as ArrayRef)]
    #[case::decimal(
        Arc::new(Decimal128Array::from(vec![12345i128]).with_precision_and_scale(10, 2).unwrap()) as ArrayRef
    )]
    #[case::decimal_negative(
        Arc::new(Decimal128Array::from(vec![-5i128]).with_precision_and_scale(3, 2).unwrap()) as ArrayRef
    )]
    #[case::binary_utf8(Arc::new(BinaryArray::from_vec(vec![b"Hello"])) as ArrayRef)]
    #[case::large_utf8(Arc::new(LargeStringArray::from(vec!["large"])) as ArrayRef)]
    #[case::float_normal(Arc::new(Float32Array::from(vec![1.25f32])) as ArrayRef)]
    #[case::float_inf(Arc::new(Float32Array::from(vec![f32::INFINITY])) as ArrayRef)]
    #[case::float_neg_inf(Arc::new(Float32Array::from(vec![f32::NEG_INFINITY])) as ArrayRef)]
    #[case::double_normal(Arc::new(Float64Array::from(vec![99.99f64])) as ArrayRef)]
    #[case::double_inf(Arc::new(Float64Array::from(vec![f64::INFINITY])) as ArrayRef)]
    #[case::double_neg_inf(Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])) as ArrayRef)]
    fn test_roundtrip_extract_serialize_parse_returns_original_scalar(#[case] array: ArrayRef) {
        let scalar = extract_scalar(array.as_ref(), 0).unwrap();
        let serialized = serialize_partition_value(&scalar)
            .unwrap()
            .expect("non-null value should serialize to Some");
        let ptype = arrow_to_primitive_type(array.data_type());
        let parsed = ptype.parse_scalar(&serialized).unwrap();
        assert_eq!(
            scalar, parsed,
            "roundtrip failed: serialized as '{serialized}'"
        );
    }

    // NaN does not equal itself, so we verify the roundtrip preserves NaN-ness separately.
    #[rstest]
    #[case::float_nan(Arc::new(Float32Array::from(vec![f32::NAN])) as ArrayRef)]
    #[case::double_nan(Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef)]
    fn test_roundtrip_nan_serialize_parse_returns_nan(#[case] array: ArrayRef) {
        let scalar = extract_scalar(array.as_ref(), 0).unwrap();
        let serialized = serialize_partition_value(&scalar)
            .unwrap()
            .expect("NaN should serialize to Some");
        let ptype = arrow_to_primitive_type(array.data_type());
        let parsed = ptype.parse_scalar(&serialized).unwrap();
        match parsed {
            Scalar::Float(v) => assert!(v.is_nan(), "expected NaN float"),
            Scalar::Double(v) => assert!(v.is_nan(), "expected NaN double"),
            other => panic!("expected float/double NaN, got {other:?}"),
        }
    }
}
