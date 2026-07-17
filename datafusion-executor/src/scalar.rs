//! Conversion from a kernel [`Scalar`] to a DataFusion [`ScalarValue`].
//!
//! Leaf scalars map one-to-one onto the matching `ScalarValue` variant -- one explicit match
//! arm per type, with no Arrow array in between. A wrong mapping is then a visible wrong arm
//! rather than an opaque round-trip failure, which is what makes this easy to debug.
//!
//! The three container variants (`Struct`, `Array`, `Map`) are unavoidably Arrow-backed:
//! DataFusion defines them as `Arc<StructArray>` / `Arc<ListArray>` / `Arc<MapArray>`, so there
//! is no Arrow-free target to map onto. We build those arrays through DataFusion's own
//! constructors ([`ScalarValue::new_list`], [`ScalarStructBuilder`], [`MapArray::try_new`]),
//! taking the Arrow *type* metadata each one needs from kernel's [`TryIntoArrow`] type
//! conversion (the `arrow-conversion` feature) rather than kernel's value-level
//! `Scalar::to_array`.
//!
//! An `impl TryFrom<&Scalar> for ScalarValue` is impossible here: both types are foreign to
//! this crate, so the orphan rule forbids it. Hence a free function.

use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef, MapArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::ScalarValue;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{ArrayData, MapData, Scalar, StructData};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Error};

/// Converts a kernel [`Scalar`] into the equivalent DataFusion [`ScalarValue`].
///
/// # Errors
///
/// Returns an error if the scalar's type has no Arrow representation (the interval types), or
/// if building the backing Arrow array for a nested container otherwise fails.
pub fn to_datafusion_scalar(scalar: &Scalar) -> DeltaResult<ScalarValue> {
    Ok(match scalar {
        Scalar::Integer(i) => ScalarValue::Int32(Some(*i)),
        Scalar::Long(i) => ScalarValue::Int64(Some(*i)),
        Scalar::Short(i) => ScalarValue::Int16(Some(*i)),
        Scalar::Byte(i) => ScalarValue::Int8(Some(*i)),
        Scalar::Float(f) => ScalarValue::Float32(Some(*f)),
        Scalar::Double(f) => ScalarValue::Float64(Some(*f)),
        Scalar::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Scalar::Boolean(b) => ScalarValue::Boolean(Some(*b)),
        // Kernel timestamps are UTC-adjusted microseconds; the NTZ variant carries no zone.
        Scalar::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into())),
        Scalar::TimestampNtz(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        Scalar::Date(d) => ScalarValue::Date32(Some(*d)),
        Scalar::Binary(b) => ScalarValue::Binary(Some(b.clone())),
        // scale() is 0..=38, so the i8 cast never truncates.
        Scalar::Decimal(d) => {
            ScalarValue::Decimal128(Some(d.bits()), d.precision(), d.scale() as i8)
        }
        Scalar::Struct(data) => struct_to_scalar(data)?,
        Scalar::Array(data) => array_to_scalar(data)?,
        Scalar::Map(data) => map_to_scalar(data)?,
        Scalar::Null(data_type) => null_to_scalar(data_type)?,
    })
}

/// Builds a typed-null `ScalarValue` from a kernel type. Mirrors what the null branch of
/// `ScalarValue::try_from_array` does: resolve the Arrow type, then let DataFusion produce its
/// canonical null for it. Interval types have no Arrow representation and surface as an error.
fn null_to_scalar(data_type: &DataType) -> DeltaResult<ScalarValue> {
    let arrow_type: ArrowDataType = data_type.try_into_arrow().map_err(Error::generic_err)?;
    ScalarValue::try_from(&arrow_type).map_err(Error::generic_err)
}

/// Builds a `ScalarValue::List` holding a single list row of the converted elements.
fn array_to_scalar(data: &ArrayData) -> DeltaResult<ScalarValue> {
    let elements = data
        .array_elements()
        .iter()
        .map(to_datafusion_scalar)
        .collect::<DeltaResult<Vec<_>>>()?;
    let element_type: ArrowDataType = data
        .array_type()
        .element_type()
        .try_into_arrow()
        .map_err(Error::generic_err)?;
    let list = ScalarValue::new_list(&elements, &element_type, data.array_type().contains_null());
    Ok(ScalarValue::List(list))
}

/// Builds a `ScalarValue::Struct` from the struct's fields and converted values.
fn struct_to_scalar(data: &StructData) -> DeltaResult<ScalarValue> {
    let mut builder = ScalarStructBuilder::new();
    for (field, value) in data.fields().iter().zip(data.values()) {
        let arrow_field: ArrowField = field.try_into_arrow().map_err(Error::generic_err)?;
        builder = builder.with_scalar(arrow_field, to_datafusion_scalar(value)?);
    }
    builder.build().map_err(Error::generic_err)
}

/// Builds a `ScalarValue::Map` holding a single map row of the converted key/value pairs.
///
/// DataFusion has no map builder, so we assemble the `MapArray` by hand: a length-1 offset
/// buffer over one `key_value` entries struct built from the key and value columns. The entries
/// field and its child types come from kernel's [`MapType`] conversion, keeping this in lockstep
/// with how the rest of the engine describes maps to Arrow.
///
/// [`MapType`]: delta_kernel::schema::MapType
fn map_to_scalar(data: &MapData) -> DeltaResult<ScalarValue> {
    let map_type = data.map_type();
    // Arrow `key_value` entries field: Struct { key (non-null), value (nullable per map) }.
    let entries_field: ArrowField = map_type.try_into_arrow().map_err(Error::generic_err)?;
    let ArrowDataType::Struct(kv_fields) = entries_field.data_type().clone() else {
        return Err(Error::generic("map entries type is not a struct"));
    };
    let key_type: ArrowDataType = map_type
        .key_type()
        .try_into_arrow()
        .map_err(Error::generic_err)?;
    let value_type: ArrowDataType = map_type
        .value_type()
        .try_into_arrow()
        .map_err(Error::generic_err)?;

    let mut keys = Vec::with_capacity(data.pairs().len());
    let mut values = Vec::with_capacity(data.pairs().len());
    for (key, value) in data.pairs() {
        keys.push(to_datafusion_scalar(key)?);
        values.push(to_datafusion_scalar(value)?);
    }
    let key_array = scalars_to_array(keys, &key_type)?;
    let value_array = scalars_to_array(values, &value_type)?;

    let entries = StructArray::try_new(kv_fields, vec![key_array, value_array], None)
        .map_err(Error::generic_err)?;
    let offsets = OffsetBuffer::from_lengths([data.pairs().len()]);
    let map_array = MapArray::try_new(Arc::new(entries_field), offsets, entries, None, false)
        .map_err(Error::generic_err)?;
    Ok(ScalarValue::Map(Arc::new(map_array)))
}

/// Collects converted scalars into a single Arrow column. [`ScalarValue::iter_to_array`] infers
/// the type from the first element, so an empty column falls back to `arrow_type`.
fn scalars_to_array(
    scalars: Vec<ScalarValue>,
    arrow_type: &ArrowDataType,
) -> DeltaResult<ArrayRef> {
    if scalars.is_empty() {
        Ok(new_empty_array(arrow_type))
    } else {
        ScalarValue::iter_to_array(scalars).map_err(Error::generic_err)
    }
}

#[cfg(test)]
mod tests {
    use delta_kernel::expressions::{ArrayData, MapData, StructData};
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField};
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::integer(Scalar::Integer(42), ScalarValue::Int32(Some(42)))]
    #[case::long(Scalar::Long(9_876_543_210), ScalarValue::Int64(Some(9_876_543_210)))]
    #[case::short(Scalar::Short(7), ScalarValue::Int16(Some(7)))]
    #[case::byte(Scalar::Byte(3), ScalarValue::Int8(Some(3)))]
    #[case::float(Scalar::Float(1.25), ScalarValue::Float32(Some(1.25)))]
    #[case::double(Scalar::Double(99.99), ScalarValue::Float64(Some(99.99)))]
    #[case::boolean(Scalar::Boolean(true), ScalarValue::Boolean(Some(true)))]
    #[case::string(Scalar::String("hi".into()), ScalarValue::Utf8(Some("hi".into())))]
    #[case::binary(Scalar::Binary(b"abc".to_vec()), ScalarValue::Binary(Some(b"abc".to_vec())))]
    #[case::date(Scalar::Date(20178), ScalarValue::Date32(Some(20178)))]
    #[case::timestamp(
        Scalar::Timestamp(1_000_000),
        ScalarValue::TimestampMicrosecond(Some(1_000_000), Some("UTC".into()))
    )]
    #[case::timestamp_ntz(
        Scalar::TimestampNtz(1_000_000),
        ScalarValue::TimestampMicrosecond(Some(1_000_000), None)
    )]
    #[case::decimal(
        Scalar::decimal(12345, 10, 2).unwrap(),
        ScalarValue::Decimal128(Some(12345), 10, 2)
    )]
    fn primitive_scalar_converts_to_matching_value(
        #[case] scalar: Scalar,
        #[case] expected: ScalarValue,
    ) {
        assert_eq!(to_datafusion_scalar(&scalar).unwrap(), expected);
    }

    #[rstest]
    #[case::integer(DataType::INTEGER, ScalarValue::Int32(None))]
    #[case::long(DataType::LONG, ScalarValue::Int64(None))]
    #[case::string(DataType::STRING, ScalarValue::Utf8(None))]
    #[case::boolean(DataType::BOOLEAN, ScalarValue::Boolean(None))]
    #[case::date(DataType::DATE, ScalarValue::Date32(None))]
    #[case::timestamp(
        DataType::TIMESTAMP,
        ScalarValue::TimestampMicrosecond(None, Some("UTC".into()))
    )]
    #[case::timestamp_ntz(DataType::TIMESTAMP_NTZ, ScalarValue::TimestampMicrosecond(None, None))]
    fn typed_null_scalar_converts_to_typed_null_value(
        #[case] data_type: DataType,
        #[case] expected: ScalarValue,
    ) {
        assert_eq!(
            to_datafusion_scalar(&Scalar::Null(data_type)).unwrap(),
            expected
        );
    }

    #[test]
    fn nan_and_infinity_are_preserved() {
        match to_datafusion_scalar(&Scalar::Double(f64::NAN)).unwrap() {
            ScalarValue::Float64(Some(v)) => assert!(v.is_nan()),
            other => panic!("expected Float64 NaN, got {other:?}"),
        }
        assert_eq!(
            to_datafusion_scalar(&Scalar::Float(f32::INFINITY)).unwrap(),
            ScalarValue::Float32(Some(f32::INFINITY))
        );
    }

    // The container arms build Arrow arrays; assert the produced variant rather than
    // hand-building the expected Arrow-backed value.
    #[test]
    fn array_scalar_converts_to_list_value() {
        let array = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, false),
            [Scalar::Integer(1), Scalar::Integer(2)],
        )
        .unwrap();
        let value = to_datafusion_scalar(&Scalar::Array(array)).unwrap();
        assert!(matches!(value, ScalarValue::List(_)), "got {value:?}");
    }

    #[test]
    fn struct_scalar_converts_to_struct_value() {
        let data = StructData::try_new(
            vec![
                StructField::not_null("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ],
            vec![Scalar::Integer(1), Scalar::String("x".into())],
        )
        .unwrap();
        let value = to_datafusion_scalar(&Scalar::Struct(data)).unwrap();
        assert!(matches!(value, ScalarValue::Struct(_)), "got {value:?}");
    }

    #[test]
    fn map_scalar_converts_to_map_value() {
        let data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            [(Scalar::String("k".into()), Scalar::Integer(1))],
        )
        .unwrap();
        let value = to_datafusion_scalar(&Scalar::Map(data)).unwrap();
        assert!(matches!(value, ScalarValue::Map(_)), "got {value:?}");
    }

    #[test]
    fn empty_map_scalar_converts_to_map_value() {
        let data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            Vec::<(Scalar, Scalar)>::new(),
        )
        .unwrap();
        let value = to_datafusion_scalar(&Scalar::Map(data)).unwrap();
        assert!(matches!(value, ScalarValue::Map(_)), "got {value:?}");
    }

    #[test]
    fn unrepresentable_type_returns_error() {
        // Interval types are not supported on the kernel-to-Arrow type conversion.
        to_datafusion_scalar(&Scalar::Null(DataType::INTERVAL_YEAR_MONTH)).unwrap_err();
    }
}
