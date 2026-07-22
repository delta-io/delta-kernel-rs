//! Conversion from a kernel [`Scalar`] to a DataFusion [`ScalarValue`].

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
/// Returns an error if the scalar's type has no Arrow representation (the interval types), or
/// if building the backing Arrow array for a nested container otherwise fails.
pub fn kernel_to_df_scalar(scalar: &Scalar) -> DeltaResult<ScalarValue> {
    Ok(match scalar {
        Scalar::Integer(i) => ScalarValue::Int32(Some(*i)),
        Scalar::Long(i) => ScalarValue::Int64(Some(*i)),
        Scalar::Short(i) => ScalarValue::Int16(Some(*i)),
        Scalar::Byte(i) => ScalarValue::Int8(Some(*i)),
        Scalar::Float(f) => ScalarValue::Float32(Some(*f)),
        Scalar::Double(f) => ScalarValue::Float64(Some(*f)),
        Scalar::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Scalar::Boolean(b) => ScalarValue::Boolean(Some(*b)),
        Scalar::Timestamp(v) => ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into())),
        Scalar::TimestampNtz(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        Scalar::Date(d) => ScalarValue::Date32(Some(*d)),
        Scalar::Binary(b) => ScalarValue::Binary(Some(b.clone())),
        // scale() is 0..=38, so the i8 cast never truncates.
        Scalar::Decimal(d) => {
            ScalarValue::Decimal128(Some(d.bits()), d.precision(), d.scale() as i8)
        }
        Scalar::Struct(data) => kernel_struct_to_df_scalar(data)?,
        Scalar::Array(data) => kernel_array_to_df_scalar(data)?,
        Scalar::Map(data) => kernel_map_to_df_scalar(data)?,
        Scalar::Null(data_type) => kernel_datatype_to_df_null_scalar(data_type)?,
    })
}

/// Builds a typed-null `ScalarValue` from a kernel type.
fn kernel_datatype_to_df_null_scalar(data_type: &DataType) -> DeltaResult<ScalarValue> {
    let arrow_type: ArrowDataType = data_type.try_into_arrow().map_err(Error::generic_err)?;
    ScalarValue::try_from(&arrow_type).map_err(Error::generic_err)
}

/// Builds a `ScalarValue::List` holding a single list row of the converted elements.
fn kernel_array_to_df_scalar(data: &ArrayData) -> DeltaResult<ScalarValue> {
    let elements = data
        .array_elements()
        .iter()
        .map(kernel_to_df_scalar)
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
fn kernel_struct_to_df_scalar(data: &StructData) -> DeltaResult<ScalarValue> {
    let mut builder = ScalarStructBuilder::new();
    for (field, value) in data.fields().iter().zip(data.values()) {
        let arrow_field: ArrowField = field.try_into_arrow().map_err(Error::generic_err)?;
        builder = builder.with_scalar(arrow_field, kernel_to_df_scalar(value)?);
    }
    builder.build().map_err(Error::generic_err)
}

/// Builds a `ScalarValue::Map` holding a single map row of the converted key/value pairs.
fn kernel_map_to_df_scalar(data: &MapData) -> DeltaResult<ScalarValue> {
    let map_type = data.map_type();
    let entries_field: ArrowField = map_type.try_into_arrow().map_err(Error::generic_err)?;
    let ArrowDataType::Struct(kv_fields) = entries_field.data_type().clone() else {
        return Err(Error::generic("map entries type is not a struct"));
    };
    let [key_field, value_field] = kv_fields.as_ref() else {
        return Err(Error::generic(
            "map entries struct must have exactly a key and value field",
        ));
    };

    let pairs = data.pairs();
    let mut keys = Vec::with_capacity(pairs.len());
    let mut values = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        keys.push(kernel_to_df_scalar(key)?);
        values.push(kernel_to_df_scalar(value)?);
    }
    let key_array = df_scalars_to_arrow_array(keys, key_field.data_type())?;
    let value_array = df_scalars_to_arrow_array(values, value_field.data_type())?;

    let entries = StructArray::try_new(kv_fields.clone(), vec![key_array, value_array], None)
        .map_err(Error::generic_err)?;
    let offsets = OffsetBuffer::from_lengths([pairs.len()]);
    let map_array = MapArray::try_new(Arc::new(entries_field), offsets, entries, None, false)
        .map_err(Error::generic_err)?;
    Ok(ScalarValue::Map(Arc::new(map_array)))
}

/// Collects converted scalars into a single Arrow column. [`ScalarValue::iter_to_array`] infers
/// the type from the first element, so an empty column falls back to `arrow_type`.
fn df_scalars_to_arrow_array(
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
        assert_eq!(kernel_to_df_scalar(&scalar).unwrap(), expected);
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
            kernel_to_df_scalar(&Scalar::Null(data_type)).unwrap(),
            expected
        );
    }

    #[test]
    fn nan_and_infinity_are_preserved() {
        match kernel_to_df_scalar(&Scalar::Double(f64::NAN)).unwrap() {
            ScalarValue::Float64(Some(v)) => assert!(v.is_nan()),
            other => panic!("expected Float64 NaN, got {other:?}"),
        }
        assert_eq!(
            kernel_to_df_scalar(&Scalar::Float(f32::INFINITY)).unwrap(),
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
        let value = kernel_to_df_scalar(&Scalar::Array(array)).unwrap();
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
        let value = kernel_to_df_scalar(&Scalar::Struct(data)).unwrap();
        assert!(matches!(value, ScalarValue::Struct(_)), "got {value:?}");
    }

    #[rstest]
    #[case::single(vec![(Scalar::String("k".into()), Scalar::Integer(1))])]
    #[case::empty(vec![])]
    fn map_scalar_converts_to_map_value(#[case] pairs: Vec<(Scalar, Scalar)>) {
        let data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            pairs,
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Map(data)).unwrap();
        assert!(matches!(value, ScalarValue::Map(_)), "got {value:?}");
    }

    #[test]
    fn unrepresentable_type_returns_error() {
        // A shredded (non-unshredded) variant has no Arrow representation in kernel's type
        // conversion, so a typed null of that type surfaces an error.
        let shredded_variant =
            DataType::variant_type([StructField::not_null("x", DataType::INTEGER)]).unwrap();
        kernel_to_df_scalar(&Scalar::Null(shredded_variant)).unwrap_err();
    }
}
