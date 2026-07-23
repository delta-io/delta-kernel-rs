//! Conversion from a kernel [`Scalar`] to a DataFusion [`ScalarValue`].

use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef, MapArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::utils::SingleRowListArrayBuilder;
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
    // Name the list's element field from kernel's own ArrayType->Arrow conversion
    let element_field: ArrowField = data
        .array_type()
        .try_into_arrow()
        .map_err(Error::generic_err)?;
    let element_array = df_scalars_to_arrow_array(elements, element_field.data_type())?;
    let list = SingleRowListArrayBuilder::new(element_array)
        .with_field(&element_field)
        .build_list_array();
    Ok(ScalarValue::List(Arc::new(list)))
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
    use datafusion::arrow::array::{Array, AsArray, Int32Array, ListArray};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use delta_kernel::expressions::{ArrayData, MapData, StructData};
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
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

    // The list's element field is named "element" (kernel's LIST_ARRAY_ROOT), not DataFusion's
    // default "item"; the expected value is built to match kernel's ArrayType->Arrow conversion.
    #[test]
    fn array_scalar_converts_to_list_with_matching_elements() {
        let array = ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, false),
            [Scalar::Integer(1), Scalar::Integer(2)],
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Array(array)).unwrap();
        let element_field = ArrowField::new("element", ArrowDataType::Int32, false);
        let list = ListArray::new(
            Arc::new(element_field),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        );
        let expected = ScalarValue::List(Arc::new(list));
        assert_eq!(value, expected);
    }

    // Field names and nullability are part of struct equality, so asserting against a hand-built
    // expected value pins them too.
    #[test]
    fn struct_scalar_converts_to_struct_with_matching_fields() {
        let data = StructData::try_new(
            vec![
                StructField::not_null("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ],
            vec![Scalar::Integer(1), Scalar::String("x".into())],
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Struct(data)).unwrap();
        let expected = ScalarStructBuilder::new()
            .with_scalar(
                ArrowField::new("a", ArrowDataType::Int32, false),
                ScalarValue::Int32(Some(1)),
            )
            .with_scalar(
                ArrowField::new("b", ArrowDataType::Utf8, true),
                ScalarValue::Utf8(Some("x".into())),
            )
            .build()
            .unwrap();
        assert_eq!(value, expected);
    }

    // No symmetric ScalarValue map constructor exists, so read the entries back directly rather
    // than asserting against a hand-built expected value.
    #[rstest]
    #[case::single(vec![(Scalar::String("k".into()), Scalar::Integer(1))], vec![("k", 1)])]
    #[case::empty(vec![], vec![])]
    fn map_scalar_converts_to_map_with_matching_pairs(
        #[case] pairs: Vec<(Scalar, Scalar)>,
        #[case] expected: Vec<(&str, i32)>,
    ) {
        let data = MapData::try_new(
            MapType::new(DataType::STRING, DataType::INTEGER, false),
            pairs,
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Map(data)).unwrap();
        let ScalarValue::Map(map) = &value else {
            panic!("expected Map, got {value:?}");
        };
        let keys = map.keys().as_string::<i32>();
        let values = map.values().as_primitive::<Int32Type>();
        let actual: Vec<(&str, i32)> = (0..keys.len())
            .map(|i| (keys.value(i), values.value(i)))
            .collect();
        assert_eq!(actual, expected);
    }

    fn assert_rendered(value: &ScalarValue, expected: &[&str]) {
        let table = pretty_format_columns("c", &[value.to_array().unwrap()])
            .unwrap()
            .to_string();
        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(
            actual, expected,
            "\nexpected:\n{expected:#?}\nactual:\n{actual:#?}"
        );
    }

    fn sample_struct_type() -> StructType {
        StructType::try_new([
            StructField::not_null("a", DataType::INTEGER),
            StructField::nullable("b", DataType::STRING),
        ])
        .unwrap()
    }

    fn sample_struct_scalar() -> Scalar {
        Scalar::Struct(
            StructData::try_new(
                sample_struct_type().fields().cloned().collect(),
                vec![Scalar::Integer(1), Scalar::String("x".into())],
            )
            .unwrap(),
        )
    }

    fn sample_map_type() -> MapType {
        MapType::new(DataType::STRING, DataType::INTEGER, false)
    }

    fn sample_map_scalar() -> Scalar {
        Scalar::Map(
            MapData::try_new(
                sample_map_type(),
                [(Scalar::String("k".into()), Scalar::Integer(1))],
            )
            .unwrap(),
        )
    }

    fn sample_int_array_scalar() -> Scalar {
        Scalar::Array(
            ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                [Scalar::Integer(1), Scalar::Integer(2)],
            )
            .unwrap(),
        )
    }

    #[rstest]
    #[case::array_of_structs(
        ArrayType::new(sample_struct_type(), false),
        vec![sample_struct_scalar()],
        &[
            "+----------------+",
            "| c              |",
            "+----------------+",
            "| [{a: 1, b: x}] |",
            "+----------------+",
        ]
    )]
    #[case::array_of_maps(
        ArrayType::new(sample_map_type(), false),
        vec![sample_map_scalar()],
        &[
            "+----------+",
            "| c        |",
            "+----------+",
            "| [{k: 1}] |",
            "+----------+",
        ]
    )]
    #[case::array_of_arrays(
        ArrayType::new(ArrayType::new(DataType::INTEGER, false), false),
        vec![sample_int_array_scalar()],
        &[
            "+----------+",
            "| c        |",
            "+----------+",
            "| [[1, 2]] |",
            "+----------+",
        ]
    )]
    fn nested_array_converts_to_list(
        #[case] array_type: ArrayType,
        #[case] elements: Vec<Scalar>,
        #[case] expected: &[&str],
    ) {
        let data = ArrayData::try_new(array_type, elements).unwrap();
        let value = kernel_to_df_scalar(&Scalar::Array(data)).unwrap();
        assert!(matches!(value, ScalarValue::List(_)), "got {value:?}");
        assert_rendered(&value, expected);
    }

    #[test]
    fn nested_struct_field_converts_to_struct() {
        let data = StructData::try_new(
            vec![StructField::not_null("inner", sample_struct_type())],
            vec![sample_struct_scalar()],
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Struct(data)).unwrap();
        assert!(matches!(value, ScalarValue::Struct(_)), "got {value:?}");
        assert_rendered(
            &value,
            &[
                "+-----------------------+",
                "| c                     |",
                "+-----------------------+",
                "| {inner: {a: 1, b: x}} |",
                "+-----------------------+",
            ],
        );
    }

    #[rstest]
    #[case::map_of_structs(
        MapType::new(sample_struct_type(), sample_struct_type(), false),
        vec![(sample_struct_scalar(), sample_struct_scalar())],
        &[
            "+------------------------------+",
            "| c                            |",
            "+------------------------------+",
            "| {{a: 1, b: x}: {a: 1, b: x}} |",
            "+------------------------------+",
        ]
    )]
    #[case::map_of_maps(
        MapType::new(sample_map_type(), sample_map_type(), false),
        vec![(sample_map_scalar(), sample_map_scalar())],
        &[
            "+------------------+",
            "| c                |",
            "+------------------+",
            "| {{k: 1}: {k: 1}} |",
            "+------------------+",
        ]
    )]
    fn nested_map_converts_to_map(
        #[case] map_type: MapType,
        #[case] pairs: Vec<(Scalar, Scalar)>,
        #[case] expected: &[&str],
    ) {
        let data = MapData::try_new(map_type, pairs).unwrap();
        let value = kernel_to_df_scalar(&Scalar::Map(data)).unwrap();
        assert!(matches!(value, ScalarValue::Map(_)), "got {value:?}");
        assert_rendered(&value, expected);
    }

    // Top-level null struct whose declared subfields are non-nullable.
    #[test]
    fn null_struct_with_non_null_subfields_converts_to_null_struct() {
        let struct_type = StructType::try_new([
            StructField::not_null("a", DataType::INTEGER),
            StructField::not_null("b", DataType::STRING),
        ])
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Null(struct_type.into())).unwrap();
        assert!(matches!(value, ScalarValue::Struct(_)), "got {value:?}");
        assert!(value.is_null(), "expected a null struct, got {value:?}");
    }

    // A present (non-null) struct that carries a null in a NULLABLE field.
    #[test]
    fn present_struct_with_null_nullable_subfield_converts() {
        let data = StructData::try_new(
            vec![
                StructField::not_null("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ],
            vec![Scalar::Integer(1), Scalar::Null(DataType::STRING)],
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Struct(data)).unwrap();
        let ScalarValue::Struct(array) = &value else {
            panic!("expected Struct, got {value:?}");
        };
        assert!(!value.is_null(), "struct itself is present, got {value:?}");
        // Subfield `b` must be an actual null
        let b = array.column_by_name("b").unwrap();
        assert!(b.is_null(0), "subfield b should be null, got {b:?}");
    }

    // A kernel array nested inside a map key/value or a struct field.
    #[test]
    fn map_of_arrays_converts_to_map() {
        let data = MapData::try_new(
            MapType::new(
                ArrayType::new(DataType::INTEGER, false),
                ArrayType::new(DataType::INTEGER, false),
                false,
            ),
            vec![(sample_int_array_scalar(), sample_int_array_scalar())],
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Map(data)).unwrap();
        assert_rendered(
            &value,
            &[
                "+------------------+",
                "| c                |",
                "+------------------+",
                "| {[1, 2]: [1, 2]} |",
                "+------------------+",
            ],
        );
    }

    #[test]
    fn struct_with_array_field_converts_to_struct() {
        let data = StructData::try_new(
            vec![StructField::not_null(
                "arr",
                ArrayType::new(DataType::INTEGER, false),
            )],
            vec![sample_int_array_scalar()],
        )
        .unwrap();
        let value = kernel_to_df_scalar(&Scalar::Struct(data)).unwrap();
        assert_rendered(
            &value,
            &[
                "+---------------+",
                "| c             |",
                "+---------------+",
                "| {arr: [1, 2]} |",
                "+---------------+",
            ],
        );
    }

    // A shredded (non-unshredded) variant has no Arrow representation in kernel's type conversion,
    // so a typed null of that type surfaces an error.
    #[test]
    fn unrepresentable_type_returns_error() {
        let shredded_variant =
            DataType::variant_type([StructField::not_null("x", DataType::INTEGER)]).unwrap();
        kernel_to_df_scalar(&Scalar::Null(shredded_variant)).unwrap_err();
    }
}
