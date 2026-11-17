use std::io::Cursor;
use std::sync::Arc;

use rstest::rstest;
use serde_json::json;

use super::*;
use crate::arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int64Array,
    LargeStringArray, StringViewArray, TimestampMicrosecondArray,
};
use crate::arrow::buffer::NullBuffer;
use crate::arrow::datatypes::Field;
use crate::arrow::json::ReaderBuilder;
use crate::schema::schema;
use crate::unit_test_utils::assert_result_error_with_message;

fn encode_bound(values: ArrayRef, data_type: DataType) -> KernelResult<StringArray> {
    let field = Arc::new(Field::new("bound", values.data_type().clone(), true));
    let values = StructArray::from(vec![(field, values)]);
    let schema = schema! { nullable "bound": (data_type) };
    serialize_stats(&values, &schema)
}

#[test]
fn nested_stats_preserve_physical_names_null_counts_and_tight_bounds() {
    let schema = schema! {
        nullable "numRecords": LONG,
        nullable "nullCount": { nullable "physical.a": { nullable "x": LONG } },
        nullable "minValues": { nullable "physical.a": { nullable "x": STRING } },
        nullable "tightBounds": BOOLEAN,
    };
    let first = concat!(
        r#"{"numRecords":10,"nullCount":{"physical.a":{"x":0}},"#,
        r#""minValues":{"physical.a":{"x":"\"\\\n"}},"tightBounds":true}"#,
    );
    let input = format!(
        "{first}\n{}\n{}",
        concat!(
            r#"{"numRecords":12,"nullCount":null,"minValues":{"physical.a":{"x":null}},"#,
            r#""tightBounds":false}"#,
        ),
        r#"{"numRecords":0}"#,
    );
    let mut reader = ReaderBuilder::new(Arc::new((&schema).try_into_arrow().unwrap()))
        .build(Cursor::new(&input))
        .unwrap();
    let stats = StructArray::from(reader.next().unwrap().unwrap());
    let encoded = serialize_stats(&stats, &schema).unwrap();
    let parsed: Vec<serde_json::Value> = encoded
        .iter()
        .map(|s| serde_json::from_str(s.unwrap()).unwrap())
        .collect();
    assert_eq!(
        parsed[0],
        serde_json::from_str::<serde_json::Value>(first).unwrap()
    );
    assert_eq!(
        parsed[1],
        json!({"numRecords":12,"minValues":{"physical.a":{}},"tightBounds":false})
    );
    assert_eq!(parsed[2], json!({"numRecords":0}));
}

#[rstest]
#[case::nan(f64::NAN, "NaN")]
#[case::positive_infinity(f64::INFINITY, "Infinity")]
#[case::negative_infinity(f64::NEG_INFINITY, "-Infinity")]
fn non_finite_floats_encode_as_strings(#[case] value: f64, #[case] expected: &str) {
    for (array, data_type) in [
        (
            Arc::new(Float32Array::from(vec![value as f32])) as ArrayRef,
            DataType::FLOAT,
        ),
        (
            Arc::new(Float64Array::from(vec![value])) as ArrayRef,
            DataType::DOUBLE,
        ),
    ] {
        let encoded = encode_bound(array, data_type).unwrap();
        assert_eq!(encoded.value(0), format!(r#"{{"bound":"{expected}"}}"#));
    }
}

#[test]
fn finite_floats_remain_numbers_alongside_non_finite_and_null_values() {
    let values = Float64Array::from(vec![Some(1.25), Some(f64::NAN), None]);
    let encoded = encode_bound(Arc::new(values), DataType::DOUBLE).unwrap();
    assert_eq!(encoded.value(0), r#"{"bound":1.25}"#);
    assert_eq!(encoded.value(1), r#"{"bound":"NaN"}"#);
    assert_eq!(encoded.value(2), "{}");
}

#[rstest]
#[case::utf8(Arc::new(StringArray::from(vec![Some("a\"\\\n"), None])) as ArrayRef)]
#[case::large_utf8(Arc::new(LargeStringArray::from(vec![Some("a\"\\\n"), None])) as ArrayRef)]
#[case::utf8_view(Arc::new(StringViewArray::from(vec![Some("a\"\\\n"), None])) as ArrayRef)]
fn compatible_string_encodings_preserve_values_and_nulls(#[case] values: ArrayRef) {
    let encoded = encode_bound(values, DataType::STRING).unwrap();
    assert_eq!(encoded.value(0), r#"{"bound":"a\"\\\n"}"#);
    assert_eq!(encoded.value(1), "{}");
}

#[rstest]
fn equivalent_utc_annotations_produce_utc_timestamp_bounds(
    #[values("UTC", "Etc/UTC", "+00:00", "-00:00")] timezone: &str,
) {
    let values =
        TimestampMicrosecondArray::from(vec![1_783_007_755_999_900]).with_timezone(timezone);
    let encoded = encode_bound(Arc::new(values), DataType::TIMESTAMP).unwrap();
    assert_eq!(encoded.value(0), r#"{"bound":"2026-07-02T15:55:55.999Z"}"#);
}

#[rstest]
fn non_utc_annotations_return_errors(
    #[values("+01:00", "America/Los_Angeles", "Europe/London")] timezone: &str,
) {
    let values = TimestampMicrosecondArray::from(vec![0]).with_timezone(timezone);
    assert_result_error_with_message(
        encode_bound(Arc::new(values), DataType::TIMESTAMP),
        "statistics type",
    );
}

#[test]
fn decimals_and_large_integers_retain_exact_digits() {
    let value = 12345678901234567890123456789012345678i128;
    let array = Decimal128Array::from(vec![value])
        .with_precision_and_scale(38, 10)
        .unwrap();
    let encoded = encode_bound(Arc::new(array), DataType::decimal(38, 10).unwrap()).unwrap();
    assert_eq!(
        encoded.value(0),
        r#"{"bound":1234567890123456789012345678.9012345678}"#
    );
    let encoded = encode_bound(Arc::new(Int64Array::from(vec![i64::MAX])), DataType::LONG).unwrap();
    assert_eq!(encoded.value(0), r#"{"bound":9223372036854775807}"#);
}

#[rstest]
#[case::after_epoch(1_783_007_755_999_900, "2026-07-02T15:55:55.999")]
#[case::before_epoch(-1_500, "1969-12-31T23:59:59.998")]
fn timestamps_floor_to_milliseconds(#[case] micros: i64, #[case] expected: &str) {
    let values = TimestampMicrosecondArray::from(vec![micros]);
    for (array, data_type, suffix) in [
        (
            Arc::new(values.clone()) as ArrayRef,
            DataType::TIMESTAMP_NTZ,
            "",
        ),
        (
            Arc::new(values.with_timezone("UTC")) as ArrayRef,
            DataType::TIMESTAMP,
            "Z",
        ),
    ] {
        let encoded = encode_bound(array, data_type).unwrap();
        assert_eq!(
            encoded.value(0),
            format!(r#"{{"bound":"{expected}{suffix}"}}"#)
        );
    }
    let encoded = encode_bound(Arc::new(Date32Array::from(vec![-1])), DataType::DATE).unwrap();
    assert_eq!(encoded.value(0), r#"{"bound":"1969-12-31"}"#);
}

#[rstest]
#[case::aligned(vec![0, 1], "00001")]
#[case::one_byte_of_padding(vec![0, 1, 0, 0, 0], "0000100000")]
#[case::two_bytes_of_padding(vec![0, 1, 0, 0], "0000100000")]
#[case::three_bytes_of_padding(vec![0, 1, 0], "0000100000")]
fn variant_concatenates_metadata_and_value_then_zero_pads(
    #[case] value: Vec<u8>,
    #[case] expected: &str,
    #[values(false, true)] reverse_fields: bool,
) {
    let mut fields = vec![
        (
            Arc::new(Field::new("metadata", ArrowDataType::Binary, false)),
            Arc::new(BinaryArray::from_vec(vec![&[0, 0]])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", ArrowDataType::Binary, false)),
            Arc::new(BinaryArray::from_vec(vec![&value])) as ArrayRef,
        ),
    ];
    if reverse_fields {
        fields.reverse();
    }
    let array = StructArray::from(fields);
    let encoded = encode_bound(Arc::new(array), DataType::unshredded_variant()).unwrap();
    assert_eq!(encoded.value(0), format!(r#"{{"bound":"{expected}"}}"#));
}

#[rstest]
fn variant_requires_non_null_children_only_when_the_parent_is_valid(
    #[values(false, true)] valid_parent: bool,
) {
    let values = StructArray::new(
        vec![
            Arc::new(Field::new("metadata", ArrowDataType::Binary, true)),
            Arc::new(Field::new("value", ArrowDataType::Binary, true)),
        ]
        .into(),
        vec![
            Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
            Arc::new(BinaryArray::from_vec(vec![&[0, 1]])),
        ],
        Some(NullBuffer::from(vec![valid_parent])),
    );
    let result = encode_bound(Arc::new(values), DataType::unshredded_variant());
    if valid_parent {
        assert_result_error_with_message(result, "Null VARIANT statistics");
    } else {
        assert_eq!(result.unwrap().value(0), "{}");
    }
}

#[test]
fn null_parent_structs_and_empty_batches_are_preserved() {
    let schema = schema! { nullable "numRecords": LONG };
    let stats = StructArray::new(
        vec![Arc::new(Field::new(
            "numRecords",
            ArrowDataType::Int64,
            true,
        ))]
        .into(),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
        Some(NullBuffer::from(vec![false, true])),
    );
    let encoded = serialize_stats(&stats, &schema).unwrap();
    assert!(encoded.is_null(0));
    assert_eq!(encoded.value(1), r#"{"numRecords":20}"#);
    assert!(
        serialize_stats(&stats.slice(0, 0), &schema)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn unsupported_or_mismatched_types_return_errors() {
    let values = Arc::new(Int64Array::from(vec![1]));
    for data_type in [
        DataType::STRING,
        DataType::BINARY,
        DataType::unshredded_variant(),
    ] {
        assert_result_error_with_message(
            encode_bound(values.clone(), data_type),
            "statistics type",
        );
    }
}

#[rstest]
#[case::extra_field(vec!["numRecords", "extra"], "do not match schema")]
#[case::missing_field(vec![], "do not match schema")]
#[case::renamed_field(vec!["other"], "Missing statistics field numRecords")]
fn mismatched_struct_fields_return_errors(#[case] field_names: Vec<&str>, #[case] expected: &str) {
    let schema = schema! { nullable "numRecords": LONG };
    let fields = field_names
        .into_iter()
        .map(|name| {
            (
                Arc::new(Field::new(name, ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            )
        })
        .collect::<Vec<_>>();
    let values = if fields.is_empty() {
        StructArray::new_empty_fields(1, None)
    } else {
        StructArray::from(fields)
    };
    assert_result_error_with_message(serialize_stats(&values, &schema), expected);
}

#[rstest]
#[case::extra_variant_child(vec!["metadata", "value", "extra"], false, false)]
#[case::missing_variant_child(vec!["metadata"], false, false)]
#[case::renamed_variant_child(vec!["metadata", "other"], false, false)]
#[case::non_binary_variant_child(vec!["metadata", "value"], true, false)]
#[case::shredded_variant(vec!["metadata", "value"], false, true)]
fn invalid_variant_layouts_return_errors(
    #[case] field_names: Vec<&str>,
    #[case] non_binary_value: bool,
    #[case] shredded: bool,
) {
    let fields = field_names
        .into_iter()
        .map(|name| {
            let values: ArrayRef = if name == "value" && non_binary_value {
                Arc::new(StringArray::from(vec!["invalid"]))
            } else {
                Arc::new(BinaryArray::from_vec(vec![b"ab".as_slice()]))
            };
            (
                Arc::new(Field::new(name, values.data_type().clone(), true)),
                values,
            )
        })
        .collect::<Vec<_>>();
    let data_type = if shredded {
        DataType::variant_type(
            schema! {
                not_null "metadata": BINARY,
                nullable "value": BINARY,
                nullable "typed_value": LONG,
            }
            .into_fields(),
        )
        .unwrap()
    } else {
        DataType::unshredded_variant()
    };
    assert_result_error_with_message(
        encode_bound(Arc::new(StructArray::from(fields)), data_type),
        "statistics type",
    );
}

#[test]
fn binary_bounds_are_rejected_even_with_a_matching_arrow_type() {
    let values = Arc::new(BinaryArray::from_vec(vec![b"ab".as_slice()]));
    assert_result_error_with_message(encode_bound(values, DataType::BINARY), "statistics type");
}

#[rstest]
#[case::date(Arc::new(Date32Array::from(vec![i32::MAX])) as ArrayRef, DataType::DATE)]
#[case::timestamp(Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX])) as ArrayRef, DataType::TIMESTAMP_NTZ)]
fn out_of_range_temporal_bounds_return_errors(
    #[case] values: ArrayRef,
    #[case] data_type: DataType,
) {
    assert_result_error_with_message(encode_bound(values, data_type), "Failed to convert");
}

#[cfg(feature = "geo-type-in-dev")]
#[rstest]
#[case::coordinates(None)]
#[case::little_endian_wkb(Some(true))]
#[case::big_endian_wkb(Some(false))]
fn geometry_uses_wkt_while_coordinate_structs_remain_objects(#[case] little_endian: Option<bool>) {
    let coordinates = StructArray::from(vec![
        (
            Arc::new(Field::new("x", ArrowDataType::Float64, true)),
            Arc::new(Float64Array::from(vec![1.25])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", ArrowDataType::Float64, true)),
            Arc::new(Float64Array::from(vec![-2.5])) as ArrayRef,
        ),
    ]);
    let geometry: ArrayRef = if let Some(little_endian) = little_endian {
        let mut wkb = vec![u8::from(little_endian)];
        wkb.extend(if little_endian {
            1u32.to_le_bytes()
        } else {
            1u32.to_be_bytes()
        });
        for n in [1.25f64, -2.5] {
            wkb.extend(if little_endian {
                n.to_le_bytes()
            } else {
                n.to_be_bytes()
            });
        }
        Arc::new(BinaryArray::from_vec(vec![&wkb]))
    } else {
        Arc::new(coordinates.clone())
    };
    let encoded =
        encode_bound(geometry, crate::unit_test_utils::geometry_type("EPSG:4326")).unwrap();
    assert_eq!(encoded.value(0), r#"{"bound":"POINT (1.25 -2.5)"}"#);
    let data_type = schema! { nullable "x": DOUBLE, nullable "y": DOUBLE };
    let encoded = encode_bound(Arc::new(coordinates), data_type.into()).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(encoded.value(0)).unwrap(),
        json!({"bound":{"x":1.25,"y":-2.5}})
    );
}

#[cfg(feature = "geo-type-in-dev")]
#[rstest]
#[case::truncated(vec![1, 1, 0, 0, 0])]
#[case::invalid_byte_order(vec![2; 21])]
#[case::not_a_point(vec![0; 21])]
fn invalid_geometry_bounds_return_errors(#[case] wkb: Vec<u8>) {
    let values = Arc::new(BinaryArray::from_vec(vec![&wkb]));
    assert_result_error_with_message(
        encode_bound(values, crate::unit_test_utils::geometry_type("EPSG:4326")),
        "2D point",
    );
}

#[cfg(feature = "geo-type-in-dev")]
#[rstest]
#[case::null_x(vec!["x", "y"], None, false)]
#[case::missing_y(vec!["x"], Some(1.0), false)]
#[case::renamed_y(vec!["x", "other"], Some(1.0), false)]
#[case::extra_coordinate(vec!["x", "y", "z"], Some(1.0), false)]
#[case::float32_coordinates(vec!["x", "y"], Some(1.0), true)]
fn invalid_geometry_coordinate_structs_return_errors(
    #[case] field_names: Vec<&str>,
    #[case] x: Option<f64>,
    #[case] float32: bool,
) {
    let fields = field_names
        .into_iter()
        .map(|name| {
            let value = if name == "x" { x } else { Some(2.0) };
            let values: ArrayRef = if float32 {
                Arc::new(Float32Array::from(vec![value.map(|v| v as f32)]))
            } else {
                Arc::new(Float64Array::from(vec![value]))
            };
            (
                Arc::new(Field::new(name, values.data_type().clone(), true)),
                values,
            )
        })
        .collect::<Vec<_>>();
    assert_result_error_with_message(
        encode_bound(
            Arc::new(StructArray::from(fields)),
            crate::unit_test_utils::geometry_type("EPSG:4326"),
        ),
        "2D point",
    );
}

#[cfg(feature = "geo-type-in-dev")]
#[rstest]
fn non_finite_geometry_coordinates_return_errors(
    #[values(f64::NAN, f64::INFINITY, f64::NEG_INFINITY)] coordinate: f64,
    #[values(false, true)] non_finite_y: bool,
) {
    let mut wkb = vec![1, 1, 0, 0, 0];
    let coordinates = if non_finite_y {
        [1.0, coordinate]
    } else {
        [coordinate, 2.0]
    };
    for value in coordinates {
        wkb.extend(value.to_le_bytes());
    }
    assert_result_error_with_message(
        encode_bound(
            Arc::new(BinaryArray::from_vec(vec![&wkb])),
            crate::unit_test_utils::geometry_type("EPSG:4326"),
        ),
        "2D point",
    );
}
