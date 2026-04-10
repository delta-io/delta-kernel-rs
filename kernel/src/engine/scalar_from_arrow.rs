//! Extract a kernel [`Scalar`] from a single row of an Arrow array.
//!
//! # Supported types
//!
//! All Delta primitive types are supported: Integer, Long, Short, Byte, Float, Double,
//! Boolean, String, Date, Timestamp, TimestampNtz, Decimal, Binary (including `LargeUtf8`
//! and `LargeBinary` Arrow variants).
//!
//! Complex types (Struct, Array, Map) are not supported and return an error.
//!
//! [`Scalar`]: crate::expressions::Scalar

// TODO: add `extract_scalar` that handles complex types (Struct, Array, Map) via recursive
// extraction into StructData/ArrayData/MapData when there is a concrete use case.

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

/// Extracts a primitive kernel [`Scalar`] from the given row of an Arrow array.
///
/// This is useful for connectors that partition data using Arrow arrays and need typed
/// partition values for the write path.
///
/// Returns `Scalar::Null(data_type)` if the value at `row_idx` is null. Any timezone
/// annotation on a microsecond `Timestamp` is treated as UTC-adjusted per the Delta
/// protocol; the raw microsecond value is extracted without conversion.
///
/// # Errors
///
/// Returns an error if:
/// - `row_idx` is out of bounds for the array
/// - The Arrow data type is not a supported primitive type (e.g., Struct, List, Map)
/// - The Arrow data type is a `Timestamp` with a non-microsecond time unit
/// - The decimal precision/scale is invalid
pub fn extract_primitive_scalar(array: &dyn Array, row_idx: usize) -> DeltaResult<Scalar> {
    if row_idx >= array.len() {
        return Err(Error::generic(format!(
            "row index {row_idx} out of bounds for array of length {}",
            array.len()
        )));
    }
    if array.is_null(row_idx) {
        return Ok(Scalar::Null(arrow_primitive_to_kernel_type(
            array.data_type(),
        )?));
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
        // The Delta protocol defines Timestamp as microseconds since epoch in UTC. Arrow
        // arrays may carry any timezone label, but the raw microsecond value is always the
        // same physical representation. We treat any timezone annotation as "this data is
        // UTC-adjusted" and extract the raw value without conversion.
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
            if *scale < 0 {
                return Err(Error::generic(format!(
                    "negative decimal scale ({scale}) is not supported"
                )));
            }
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
            "unsupported Arrow type for primitive scalar extraction: {other:?}"
        ))),
    }
}

/// Maps an Arrow data type to a kernel [`DataType`] for primitive types only.
///
/// Used by the null path to construct `Scalar::Null(data_type)`. Rejects complex types
/// (struct, list, map) and non-microsecond timestamps so that the null and non-null paths
/// accept exactly the same set of Arrow types.
///
/// This intentionally does not delegate to `TryFromArrow<&ArrowDataType>` from
/// `arrow_conversion` because it has different semantics: it accepts any timezone
/// annotation (not just UTC) and rejects types like UInt*, Utf8View, Date64 that are
/// not valid for direct scalar extraction.
fn arrow_primitive_to_kernel_type(arrow_type: &ArrowDataType) -> DeltaResult<DataType> {
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
        // Any timezone annotation means "UTC-adjusted". See comment in extract_primitive_scalar.
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(DataType::TIMESTAMP),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
        ArrowDataType::Decimal128(p, s) => {
            if *s < 0 {
                return Err(Error::generic(format!(
                    "negative decimal scale ({s}) is not supported"
                )));
            }
            DataType::decimal(*p, *s as u8)
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(DataType::BINARY),
        other => Err(Error::generic(format!(
            "unsupported Arrow type for primitive scalar extraction: {other:?}"
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
    use crate::engine::arrow_conversion::TryFromArrow as _;
    use crate::partition::serialization::serialize_partition_value;
    use crate::schema::PrimitiveType;

    use rstest::rstest;
    use std::sync::Arc;

    // ============================================================================
    // extract_primitive_scalar: non-null values for each supported type
    // ============================================================================

    #[rstest]
    #[case::byte(Arc::new(Int8Array::from(vec![3i8])) as ArrayRef, Scalar::Byte(3))]
    #[case::short(Arc::new(Int16Array::from(vec![7i16])) as ArrayRef, Scalar::Short(7))]
    #[case::integer(Arc::new(Int32Array::from(vec![42])) as ArrayRef, Scalar::Integer(42))]
    #[case::long(
        Arc::new(Int64Array::from(vec![9_876_543_210i64])) as ArrayRef,
        Scalar::Long(9_876_543_210)
    )]
    #[case::float(Arc::new(Float32Array::from(vec![1.25f32])) as ArrayRef, Scalar::Float(1.25))]
    #[case::double(
        Arc::new(Float64Array::from(vec![99.99f64])) as ArrayRef,
        Scalar::Double(99.99)
    )]
    #[case::boolean_true(Arc::new(BooleanArray::from(vec![true])) as ArrayRef, Scalar::Boolean(true))]
    #[case::boolean_false(Arc::new(BooleanArray::from(vec![false])) as ArrayRef, Scalar::Boolean(false))]
    #[case::string(
        Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
        Scalar::String("hello".into())
    )]
    #[case::date(Arc::new(Date32Array::from(vec![20178])) as ArrayRef, Scalar::Date(20178))]
    #[case::binary(
        Arc::new(BinaryArray::from_vec(vec![b"Hello"])) as ArrayRef,
        Scalar::Binary(b"Hello".to_vec())
    )]
    #[case::large_utf8(
        Arc::new(LargeStringArray::from(vec!["large string"])) as ArrayRef,
        Scalar::String("large string".into())
    )]
    #[case::large_binary(
        Arc::new(LargeBinaryArray::from(vec![b"large bytes".as_ref()])) as ArrayRef,
        Scalar::Binary(b"large bytes".to_vec())
    )]
    #[case::byte_min(Arc::new(Int8Array::from(vec![i8::MIN])) as ArrayRef, Scalar::Byte(i8::MIN))]
    #[case::byte_max(Arc::new(Int8Array::from(vec![i8::MAX])) as ArrayRef, Scalar::Byte(i8::MAX))]
    #[case::short_min(Arc::new(Int16Array::from(vec![i16::MIN])) as ArrayRef, Scalar::Short(i16::MIN))]
    #[case::short_max(Arc::new(Int16Array::from(vec![i16::MAX])) as ArrayRef, Scalar::Short(i16::MAX))]
    #[case::int_min(Arc::new(Int32Array::from(vec![i32::MIN])) as ArrayRef, Scalar::Integer(i32::MIN))]
    #[case::int_max(Arc::new(Int32Array::from(vec![i32::MAX])) as ArrayRef, Scalar::Integer(i32::MAX))]
    #[case::long_min(Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef, Scalar::Long(i64::MIN))]
    #[case::long_max(Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef, Scalar::Long(i64::MAX))]
    fn test_extract_primitive_scalar_non_null_returns_typed_value(
        #[case] array: ArrayRef,
        #[case] expected: Scalar,
    ) {
        assert_eq!(
            extract_primitive_scalar(array.as_ref(), 0).unwrap(),
            expected
        );
    }

    // ============================================================================
    // extract_primitive_scalar: floating point edge cases
    // ============================================================================

    #[rstest]
    #[case::float_neg_zero(Arc::new(Float32Array::from(vec![-0.0f32])) as ArrayRef)]
    #[case::double_neg_zero(Arc::new(Float64Array::from(vec![-0.0f64])) as ArrayRef)]
    fn test_extract_primitive_scalar_negative_zero_preserves_sign(#[case] array: ArrayRef) {
        match extract_primitive_scalar(array.as_ref(), 0).unwrap() {
            Scalar::Float(v) => assert!(v.is_sign_negative() && v == 0.0),
            Scalar::Double(v) => assert!(v.is_sign_negative() && v == 0.0),
            other => panic!("expected Float or Double, got {other:?}"),
        }
    }

    #[rstest]
    #[case::float_nan(Arc::new(Float32Array::from(vec![f32::NAN])) as ArrayRef)]
    #[case::double_nan(Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef)]
    fn test_extract_primitive_scalar_nan_returns_nan(#[case] array: ArrayRef) {
        match extract_primitive_scalar(array.as_ref(), 0).unwrap() {
            Scalar::Float(v) => assert!(v.is_nan()),
            Scalar::Double(v) => assert!(v.is_nan()),
            other => panic!("expected Float or Double NaN, got {other:?}"),
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
    fn test_extract_primitive_scalar_infinity_returns_correct_value(
        #[case] array: ArrayRef,
        #[case] expected: Scalar,
    ) {
        assert_eq!(
            extract_primitive_scalar(array.as_ref(), 0).unwrap(),
            expected
        );
    }

    // ============================================================================
    // extract_primitive_scalar: timestamp variants
    // ============================================================================

    #[test]
    fn test_extract_primitive_scalar_timestamp_without_tz_returns_timestamp_ntz() {
        let array = TimestampMicrosecondArray::from(vec![1_000_000i64]);
        assert_eq!(
            extract_primitive_scalar(&array, 0).unwrap(),
            Scalar::TimestampNtz(1_000_000)
        );
    }

    // The Delta protocol defines Timestamp as microseconds since epoch in UTC. Arrow
    // arrays may carry any timezone label, but the raw microsecond value is the same.
    // We accept any timezone and extract the raw value without conversion.
    #[rstest]
    #[case::utc("UTC")]
    #[case::utc_lowercase("utc")]
    #[case::us_eastern("America/New_York")]
    #[case::offset("+05:30")]
    #[case::europe("Europe/Berlin")]
    fn test_extract_primitive_scalar_timestamp_any_tz_returns_timestamp(#[case] tz: &str) {
        let array = TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone(tz);
        assert_eq!(
            extract_primitive_scalar(&array, 0).unwrap(),
            Scalar::Timestamp(1_000_000)
        );
    }

    #[rstest]
    #[case::utc("UTC")]
    #[case::us_eastern("America/New_York")]
    #[case::offset("+05:30")]
    fn test_extract_primitive_scalar_null_timestamp_any_tz_returns_typed_null(#[case] tz: &str) {
        let array = TimestampMicrosecondArray::from(vec![None::<i64>]).with_timezone(tz);
        assert_eq!(
            extract_primitive_scalar(&array, 0).unwrap(),
            Scalar::Null(DataType::TIMESTAMP)
        );
    }

    // ============================================================================
    // extract_primitive_scalar: decimal
    // ============================================================================

    #[rstest]
    #[case::positive(12345i128, 10, 2)]
    #[case::zero_scale(42i128, 5, 0)]
    #[case::negative_value(-5i128, 3, 2)]
    fn test_extract_primitive_scalar_decimal_returns_correct_value(
        #[case] value: i128,
        #[case] precision: u8,
        #[case] scale: i8,
    ) {
        let array = Decimal128Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap();
        assert_eq!(
            extract_primitive_scalar(&array, 0).unwrap(),
            Scalar::decimal(value, precision, scale as u8).unwrap()
        );
    }

    // ============================================================================
    // extract_primitive_scalar: null values across all supported types
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
    fn test_extract_primitive_scalar_null_returns_typed_null(
        #[case] arrow_type: ArrowDataType,
        #[case] expected_kernel_type: DataType,
    ) {
        let array = new_null_array(&arrow_type, 1);
        assert_eq!(
            extract_primitive_scalar(array.as_ref(), 0).unwrap(),
            Scalar::Null(expected_kernel_type)
        );
    }

    #[test]
    fn test_extract_primitive_scalar_null_decimal_returns_typed_null() {
        let array = new_null_array(&ArrowDataType::Decimal128(10, 2), 1);
        assert_eq!(
            extract_primitive_scalar(array.as_ref(), 0).unwrap(),
            Scalar::Null(DataType::decimal(10, 2).unwrap())
        );
    }

    // ============================================================================
    // extract_primitive_scalar: multi-row arrays
    // ============================================================================

    #[test]
    fn test_extract_primitive_scalar_multi_row_selects_correct_index() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        assert_eq!(
            extract_primitive_scalar(&array, 0).unwrap(),
            Scalar::Integer(1)
        );
        assert_eq!(
            extract_primitive_scalar(&array, 1).unwrap(),
            Scalar::Null(DataType::INTEGER)
        );
        assert_eq!(
            extract_primitive_scalar(&array, 2).unwrap(),
            Scalar::Integer(3)
        );
    }

    // ============================================================================
    // extract_primitive_scalar: bounds checking
    // ============================================================================

    #[test]
    fn test_extract_primitive_scalar_out_of_bounds_returns_error() {
        let array = Int32Array::from(vec![42]);
        let result = extract_primitive_scalar(&array, 1);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("out of bounds"), "got: {msg}");
    }

    // ============================================================================
    // extract_primitive_scalar: unsupported types return error
    // ============================================================================

    #[test]
    fn test_extract_primitive_scalar_non_null_struct_returns_error() {
        let int_array = Arc::new(Int32Array::from(vec![42]));
        let fields = Fields::from(vec![Field::new("a", ArrowDataType::Int32, false)]);
        let array = StructArray::try_new(fields, vec![int_array], None).unwrap();
        let result = extract_primitive_scalar(&array, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_primitive_scalar_null_struct_returns_error() {
        let fields = Fields::from(vec![Field::new("a", ArrowDataType::Int32, false)]);
        let array = StructArray::new_null(fields, 1);
        let result = extract_primitive_scalar(&array, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_primitive_scalar_null_list_returns_error() {
        let list_type =
            ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true)));
        let array = new_null_array(&list_type, 1);
        let result = extract_primitive_scalar(array.as_ref(), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_primitive_scalar_null_map_returns_error() {
        let map_type = ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(Fields::from(vec![
                    Field::new("key", ArrowDataType::Utf8, false),
                    Field::new("value", ArrowDataType::Int32, true),
                ])),
                false,
            )),
            false,
        );
        let array = new_null_array(&map_type, 1);
        let result = extract_primitive_scalar(array.as_ref(), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_primitive_scalar_null_unsupported_timestamp_unit_returns_error() {
        let array = new_null_array(&ArrowDataType::Timestamp(TimeUnit::Second, None), 1);
        let result = extract_primitive_scalar(array.as_ref(), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_primitive_scalar_null_decimal_negative_scale_returns_error() {
        // new_null_array bypasses Arrow's scale validation, so negative scale is reachable
        // on the null path through arrow_primitive_to_kernel_type.
        let array = new_null_array(&ArrowDataType::Decimal128(10, -2), 1);
        let result = extract_primitive_scalar(array.as_ref(), 0);
        assert!(result.is_err());
    }

    // ============================================================================
    // Roundtrip: Arrow -> extract -> serialize -> parse_scalar -> compare
    // ============================================================================
    //
    // Validates the full connector pipeline: Arrow array value is extracted as a Scalar,
    // serialized to a partition value string, then parsed back. The result must match
    // the originally extracted Scalar.

    // Converts an Arrow data type to its kernel PrimitiveType for roundtrip testing.
    fn arrow_to_primitive_type(arrow_type: &ArrowDataType) -> PrimitiveType {
        DataType::try_from_arrow(arrow_type)
            .expect("supported Arrow type")
            .as_primitive_opt()
            .expect("expected primitive type")
            .clone()
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
    #[case::large_binary(Arc::new(LargeBinaryArray::from(vec![b"large".as_ref()])) as ArrayRef)]
    #[case::float_normal(Arc::new(Float32Array::from(vec![1.25f32])) as ArrayRef)]
    #[case::float_inf(Arc::new(Float32Array::from(vec![f32::INFINITY])) as ArrayRef)]
    #[case::float_neg_inf(Arc::new(Float32Array::from(vec![f32::NEG_INFINITY])) as ArrayRef)]
    #[case::double_normal(Arc::new(Float64Array::from(vec![99.99f64])) as ArrayRef)]
    #[case::double_inf(Arc::new(Float64Array::from(vec![f64::INFINITY])) as ArrayRef)]
    #[case::double_neg_inf(Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])) as ArrayRef)]
    fn test_roundtrip_extract_serialize_parse_returns_original_scalar(#[case] array: ArrayRef) {
        let scalar = extract_primitive_scalar(array.as_ref(), 0).unwrap();
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
        let scalar = extract_primitive_scalar(array.as_ref(), 0).unwrap();
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
