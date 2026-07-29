//! Conversion from a kernel [`Scalar`](KernelScalar) to a DataFusion [`ScalarValue`].

use std::sync::Arc;

use datafusion::arrow::array::{new_empty_array, ArrayRef, MapArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::ScalarValue;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::expressions::{
    ArrayData as KernelArrayData, MapData as KernelMapData, Scalar as KernelScalar,
    StructData as KernelStructData,
};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Error};

/// Converts a kernel [`Scalar`](KernelScalar) into the equivalent DataFusion [`ScalarValue`].
///
/// # Errors
/// Returns an error for interval scalars, which are not yet supported; for a type with no Arrow
/// representation (e.g. a shredded variant); or if building the backing Arrow array for a nested
/// container otherwise fails.
pub fn to_df_scalar(scalar: &KernelScalar) -> DeltaResult<ScalarValue> {
    Ok(match scalar {
        KernelScalar::Integer(i) => ScalarValue::Int32(Some(*i)),
        KernelScalar::Long(i) => ScalarValue::Int64(Some(*i)),
        KernelScalar::Short(i) => ScalarValue::Int16(Some(*i)),
        KernelScalar::Byte(i) => ScalarValue::Int8(Some(*i)),
        KernelScalar::Float(f) => ScalarValue::Float32(Some(*f)),
        KernelScalar::Double(f) => ScalarValue::Float64(Some(*f)),
        KernelScalar::String(s) => ScalarValue::Utf8(Some(s.clone())),
        KernelScalar::Boolean(b) => ScalarValue::Boolean(Some(*b)),
        KernelScalar::Timestamp(v) => {
            ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into()))
        }
        KernelScalar::TimestampNtz(v) => ScalarValue::TimestampMicrosecond(Some(*v), None),
        KernelScalar::Date(d) => ScalarValue::Date32(Some(*d)),
        KernelScalar::Binary(b) => ScalarValue::Binary(Some(b.clone())),
        // scale() is 0..=38, so the i8 cast never truncates.
        KernelScalar::Decimal(d) => {
            ScalarValue::Decimal128(Some(d.bits()), d.precision(), d.scale() as i8)
        }
        KernelScalar::Struct(data) => struct_to_df_scalar(data)?,
        KernelScalar::Array(data) => array_to_df_scalar(data)?,
        KernelScalar::Map(data) => map_to_df_scalar(data)?,
        KernelScalar::IntervalYearMonth(_) | KernelScalar::IntervalDayTime(_) => {
            return Err(Error::unsupported(
                "interval scalars are not supported in the DataFusion executor",
            ))
        }
        KernelScalar::Null(data_type) => datatype_to_df_null_scalar(data_type)?,
    })
}

/// Builds a typed-null `ScalarValue` from a kernel type.
fn datatype_to_df_null_scalar(data_type: &DataType) -> DeltaResult<ScalarValue> {
    let arrow_type: ArrowDataType = data_type.try_into_arrow()?;
    arrow_type.try_into().map_err(Error::generic_err)
}

/// Builds a `ScalarValue::List` holding a single list row of the converted elements.
fn array_to_df_scalar(data: &KernelArrayData) -> DeltaResult<ScalarValue> {
    let elements: DeltaResult<Vec<ScalarValue>> =
        data.array_elements().iter().map(to_df_scalar).collect();
    // Name the list's element field from kernel's own ArrayType->Arrow conversion
    let element_field: ArrowField = data.array_type().try_into_arrow()?;
    let element_array = df_scalars_to_arrow_array(elements?, element_field.data_type())?;
    let list = SingleRowListArrayBuilder::new(element_array)
        .with_field(&element_field)
        .build_list_array();
    Ok(ScalarValue::List(Arc::new(list)))
}

/// Builds a `ScalarValue::Struct` from the struct's fields and converted values.
fn struct_to_df_scalar(data: &KernelStructData) -> DeltaResult<ScalarValue> {
    let mut builder = ScalarStructBuilder::new();
    for (field, value) in data.fields().iter().zip(data.values()) {
        let arrow_field: ArrowField = field.try_into_arrow()?;
        builder = builder.with_scalar(arrow_field, to_df_scalar(value)?);
    }
    builder.build().map_err(Error::generic_err)
}

/// Builds a `ScalarValue::Map` holding a single map row of the converted key/value pairs.
fn map_to_df_scalar(data: &KernelMapData) -> DeltaResult<ScalarValue> {
    let map_type = data.map_type();
    let entries_field: ArrowField = map_type.try_into_arrow()?;
    let ArrowDataType::Struct(kv_fields) = entries_field.data_type() else {
        return Err(Error::generic("map entries type is not a struct"));
    };
    let [key_field, value_field] = kv_fields.as_ref() else {
        return Err(Error::generic(
            "map entries struct must have exactly a key and value field",
        ));
    };

    let pairs = data.pairs();
    // Convert each pair once; collect fans the results out into parallel key/value columns,
    // short-circuiting on the first conversion error.
    let converted: DeltaResult<(Vec<ScalarValue>, Vec<ScalarValue>)> = pairs
        .iter()
        .map(|(key, value)| Ok((to_df_scalar(key)?, to_df_scalar(value)?)))
        .collect();
    let (keys, values) = converted?;
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
    use delta_kernel::schema::{ArrayType, DataType, MapType, StructField, StructType};
    use rstest::rstest;

    use super::*;

    // === Shared helpers ===

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

    fn sample_struct_scalar() -> KernelScalar {
        KernelScalar::Struct(
            KernelStructData::try_new(
                sample_struct_type().fields().cloned().collect(),
                vec![KernelScalar::Integer(1), KernelScalar::String("x".into())],
            )
            .unwrap(),
        )
    }

    fn sample_map_type() -> MapType {
        MapType::new(DataType::STRING, DataType::INTEGER, false)
    }

    fn sample_map_scalar() -> KernelScalar {
        KernelScalar::Map(
            KernelMapData::try_new(
                sample_map_type(),
                [(KernelScalar::String("k".into()), KernelScalar::Integer(1))],
            )
            .unwrap(),
        )
    }

    fn sample_int_array_scalar() -> KernelScalar {
        KernelScalar::Array(
            KernelArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                [KernelScalar::Integer(1), KernelScalar::Integer(2)],
            )
            .unwrap(),
        )
    }

    // === Directly-converted primitive arms ===

    mod primitives {
        use super::*;

        #[rstest]
        #[case::integer(KernelScalar::Integer(42), ScalarValue::Int32(Some(42)))]
        #[case::long(
            KernelScalar::Long(9_876_543_210),
            ScalarValue::Int64(Some(9_876_543_210))
        )]
        #[case::short(KernelScalar::Short(7), ScalarValue::Int16(Some(7)))]
        #[case::byte(KernelScalar::Byte(3), ScalarValue::Int8(Some(3)))]
        #[case::float(KernelScalar::Float(1.25), ScalarValue::Float32(Some(1.25)))]
        #[case::double(KernelScalar::Double(99.99), ScalarValue::Float64(Some(99.99)))]
        #[case::boolean(KernelScalar::Boolean(true), ScalarValue::Boolean(Some(true)))]
        #[case::string(KernelScalar::String("hi".into()), ScalarValue::Utf8(Some("hi".into())))]
        #[case::binary(KernelScalar::Binary(b"abc".to_vec()), ScalarValue::Binary(Some(b"abc".to_vec())))]
        #[case::date(KernelScalar::Date(20178), ScalarValue::Date32(Some(20178)))]
        #[case::timestamp(
            KernelScalar::Timestamp(1_000_000),
            ScalarValue::TimestampMicrosecond(Some(1_000_000), Some("UTC".into()))
        )]
        #[case::timestamp_ntz(
            KernelScalar::TimestampNtz(1_000_000),
            ScalarValue::TimestampMicrosecond(Some(1_000_000), None)
        )]
        #[case::decimal(
            KernelScalar::decimal(12345, 10, 2).unwrap(),
            ScalarValue::Decimal128(Some(12345), 10, 2)
        )]
        fn primitive_scalar_converts_to_matching_value(
            #[case] scalar: KernelScalar,
            #[case] expected: ScalarValue,
        ) {
            assert_eq!(to_df_scalar(&scalar).unwrap(), expected);
        }

        #[test]
        fn nan_and_infinity_are_preserved() {
            match to_df_scalar(&KernelScalar::Double(f64::NAN)).unwrap() {
                ScalarValue::Float64(Some(v)) => assert!(v.is_nan()),
                other => panic!("expected Float64 NaN, got {other:?}"),
            }
            assert_eq!(
                to_df_scalar(&KernelScalar::Float(f32::INFINITY)).unwrap(),
                ScalarValue::Float32(Some(f32::INFINITY))
            );
        }
    }

    // === Typed nulls: datatype_to_df_null_scalar ===

    mod nulls {
        use super::*;

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
        #[case::timestamp_ntz(
            DataType::TIMESTAMP_NTZ,
            ScalarValue::TimestampMicrosecond(None, None)
        )]
        fn typed_null_scalar_converts_to_typed_null_value(
            #[case] data_type: DataType,
            #[case] expected: ScalarValue,
        ) {
            assert_eq!(
                to_df_scalar(&KernelScalar::Null(data_type)).unwrap(),
                expected
            );
        }

        #[test]
        fn null_struct_with_non_null_subfields_converts_to_null_struct() {
            let struct_type = StructType::try_new([
                StructField::not_null("a", DataType::INTEGER),
                StructField::not_null("b", DataType::STRING),
            ])
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Null(struct_type.into())).unwrap();
            assert!(matches!(value, ScalarValue::Struct(_)), "got {value:?}");
            assert!(value.is_null(), "expected a null struct, got {value:?}");
        }

        // A shredded (non-unshredded) variant has no Arrow representation in kernel's type
        // conversion, so a typed null of that type surfaces an error.
        #[test]
        fn unrepresentable_type_returns_error() {
            let shredded_variant =
                DataType::variant_type([StructField::not_null("x", DataType::INTEGER)]).unwrap();
            to_df_scalar(&KernelScalar::Null(shredded_variant)).unwrap_err();
        }
    }

    // === Arrays: array_to_df_scalar ===

    mod arrays {
        use super::*;

        // The list's element field is named "element" (kernel's LIST_ARRAY_ROOT), not DataFusion's
        // default "item"; the expected value is built to match kernel's ArrayType->Arrow
        // conversion.
        #[test]
        fn array_scalar_converts_to_list_with_matching_elements() {
            let array = KernelArrayData::try_new(
                ArrayType::new(DataType::INTEGER, false),
                [KernelScalar::Integer(1), KernelScalar::Integer(2)],
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Array(array)).unwrap();
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
            #[case] elements: Vec<KernelScalar>,
            #[case] expected: &[&str],
        ) {
            let data = KernelArrayData::try_new(array_type, elements).unwrap();
            let value = to_df_scalar(&KernelScalar::Array(data)).unwrap();
            assert!(matches!(value, ScalarValue::List(_)), "got {value:?}");
            assert_rendered(&value, expected);
        }
    }

    // === Structs: struct_to_df_scalar ===

    mod structs {
        use super::*;

        // Field names and nullability are part of struct equality, so asserting against a
        // hand-built expected value pins them too.
        #[test]
        fn struct_scalar_converts_to_struct_with_matching_fields() {
            let data = KernelStructData::try_new(
                vec![
                    StructField::not_null("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ],
                vec![KernelScalar::Integer(1), KernelScalar::String("x".into())],
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Struct(data)).unwrap();
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

        #[test]
        fn nested_struct_field_converts_to_struct() {
            let data = KernelStructData::try_new(
                vec![StructField::not_null("inner", sample_struct_type())],
                vec![sample_struct_scalar()],
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Struct(data)).unwrap();
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

        // A present (non-null) struct that carries a null in a NULLABLE field.
        #[test]
        fn present_struct_with_null_nullable_subfield_converts() {
            let data = KernelStructData::try_new(
                vec![
                    StructField::not_null("a", DataType::INTEGER),
                    StructField::nullable("b", DataType::STRING),
                ],
                vec![
                    KernelScalar::Integer(1),
                    KernelScalar::Null(DataType::STRING),
                ],
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Struct(data)).unwrap();
            let ScalarValue::Struct(array) = &value else {
                panic!("expected Struct, got {value:?}");
            };
            assert!(!value.is_null(), "struct itself is present, got {value:?}");
            // Subfield `b` must be an actual null
            let b = array.column_by_name("b").unwrap();
            assert!(b.is_null(0), "subfield b should be null, got {b:?}");
        }

        #[test]
        fn struct_with_array_field_converts_to_struct() {
            let data = KernelStructData::try_new(
                vec![StructField::not_null(
                    "arr",
                    ArrayType::new(DataType::INTEGER, false),
                )],
                vec![sample_int_array_scalar()],
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Struct(data)).unwrap();
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
    }

    // === Maps: map_to_df_scalar ===

    mod maps {
        use super::*;

        // No symmetric ScalarValue map constructor exists, so read the entries back directly
        // rather than asserting against a hand-built expected value.
        #[rstest]
        #[case::single(vec![(KernelScalar::String("k".into()), KernelScalar::Integer(1))], vec![("k", 1)])]
        #[case::empty(vec![], vec![])]
        fn map_scalar_converts_to_map_with_matching_pairs(
            #[case] pairs: Vec<(KernelScalar, KernelScalar)>,
            #[case] expected: Vec<(&str, i32)>,
        ) {
            let data = KernelMapData::try_new(
                MapType::new(DataType::STRING, DataType::INTEGER, false),
                pairs,
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Map(data)).unwrap();
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
            #[case] pairs: Vec<(KernelScalar, KernelScalar)>,
            #[case] expected: &[&str],
        ) {
            let data = KernelMapData::try_new(map_type, pairs).unwrap();
            let value = to_df_scalar(&KernelScalar::Map(data)).unwrap();
            assert!(matches!(value, ScalarValue::Map(_)), "got {value:?}");
            assert_rendered(&value, expected);
        }

        #[test]
        fn map_of_arrays_converts_to_map() {
            let data = KernelMapData::try_new(
                MapType::new(
                    ArrayType::new(DataType::INTEGER, false),
                    ArrayType::new(DataType::INTEGER, false),
                    false,
                ),
                vec![(sample_int_array_scalar(), sample_int_array_scalar())],
            )
            .unwrap();
            let value = to_df_scalar(&KernelScalar::Map(data)).unwrap();
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
    }
}
