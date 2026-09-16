//! Conversion between the two forms a Delta log carries a shredded VARIANT column's statistics in.
//!
//! The statistic is one VARIANT value per file, an object keyed by the normalized JSON path of each
//! shredded leaf. A checkpoint's `stats_parsed` holds it as the variant's physical struct, but the
//! stats JSON has no binary type, so there the metadata and value are concatenated and the pair is
//! stored Z85-encoded in a single string. Reading the JSON form means splitting that string apart;
//! writing it means joining the halves back together.

use std::sync::Arc;

use delta_kernel_derive::internal_api;

use crate::arrow::array::{
    new_null_array, Array as ArrowArray, ArrayRef as ArrowArrayRef, AsArray, BinaryBuilder,
    NullBufferBuilder, RecordBatch, RecordBatchOptions, StringBuilder, StructArray,
};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Fields as ArrowFields, Schema as ArrowSchema,
};
use crate::engine::arrow_conversion::TryFromKernel as _;
use crate::parquet::variant::{Variant, VariantMetadata};
use crate::schema::variant_utils::schema_contains_variant_type;
use crate::schema::{DataType, Schema, StructField};
use crate::DeltaResult;

/// The `metadata` field of a variant's physical struct.
const VARIANT_METADATA_FIELD: &str = "metadata";

/// The `value` field of a variant's physical struct.
const VARIANT_VALUE_FIELD: &str = "value";

// === Decoding: stats JSON -> variant struct ===

/// Rewrites every VARIANT leaf of a JSON-decoded stats batch from the Z85 string the log stores
/// into the variant's physical struct, leaving all other leaves as they arrived.
///
/// `schema` is the stats schema being decoded to and names which leaves are VARIANT; the batch
/// itself arrives with those leaves as strings, the only form the typed JSON decoder can read. A
/// row whose statistic is absent or does not decode becomes NULL, which every skipping predicate
/// reads as "no statistic, keep the file".
pub(crate) fn decode_variant_stats(
    batch: RecordBatch,
    schema: &Schema,
) -> DeltaResult<RecordBatch> {
    if !schema_contains_variant_type(schema) {
        return Ok(batch);
    }
    let (arrow_schema, columns, num_rows) = batch.into_parts();
    let (fields, columns) = rewrite_fields(arrow_schema.fields(), columns, schema, false)?;
    Ok(RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

/// Decodes a column of Z85-encoded VARIANT statistics into the physical variant encoding described
/// by `fields`. The children are matched by name, so either field order works.
///
/// A row whose statistic is absent, is not a string, or does not decode to a well-formed variant
/// becomes NULL.
#[internal_api]
pub(crate) fn decode_z85_variant_stats(
    array: &dyn ArrowArray,
    fields: &ArrowFields,
) -> DeltaResult<ArrowArrayRef> {
    let num_rows = array.len();
    let Some(strings) = array.as_string_opt::<i32>() else {
        return Ok(Arc::new(StructArray::new_null(fields.clone(), num_rows)));
    };

    let mut metadatas = BinaryBuilder::new();
    let mut values = BinaryBuilder::new();
    let mut nulls = NullBufferBuilder::new(num_rows);
    for row in 0..num_rows {
        let decoded = if strings.is_null(row) {
            None
        } else {
            decode_z85_variant(strings.value(row))
        };
        match decoded {
            Some((metadata, value)) => {
                metadatas.append_value(metadata);
                values.append_value(value);
                nulls.append_non_null();
            }
            None => {
                // The variant children are declared non-nullable, so a NULL row still needs a value
                // in each child; the struct's own null buffer is what marks it absent.
                metadatas.append_value([]);
                values.append_value([]);
                nulls.append_null();
            }
        }
    }

    let metadata_column = Arc::new(metadatas.finish()) as ArrowArrayRef;
    let value_column = Arc::new(values.finish()) as ArrowArrayRef;
    let columns = fields
        .iter()
        .map(|field| {
            if field.name() == VARIANT_METADATA_FIELD {
                Arc::clone(&metadata_column)
            } else {
                Arc::clone(&value_column)
            }
        })
        .collect();
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        nulls.finish(),
    )?))
}

/// Splits one Z85-encoded statistic into its variant metadata and value bytes, the inverse of
/// [`encode_z85_variant`].
///
/// The raw bytes are the concatenation of the metadata and value, zero-filled up to the four-byte
/// group Z85 encodes. Each half declares its own length, so both are trimmed to it and the fill
/// bytes between the value's end and the group boundary are dropped.
fn decode_z85_variant(encoded: &str) -> Option<(Vec<u8>, Vec<u8>)> {
    let decoded = z85::decode(encoded).ok()?;
    let metadata = VariantMetadata::try_new(&decoded).ok()?;
    let metadata_size = metadata.size();
    let value = decoded.get(metadata_size..)?;
    // A statistic is an object keyed by shredded path. Any other shape is not one this reads, and
    // only an object carries a length the trim can use.
    let value_size = match Variant::try_new_with_metadata(metadata, value).ok()? {
        Variant::Object(object) => object.value.len(),
        _ => return None,
    };
    Some((
        decoded.get(..metadata_size)?.to_vec(),
        value.get(..value_size)?.to_vec(),
    ))
}

// === Encoding: variant struct -> stats JSON ===

/// Rewrites every VARIANT leaf of `array` from the variant's physical struct into the single Z85
/// string the stats JSON format carries, the inverse of [`decode_variant_stats`].
///
/// `schema` is the kernel schema of `array` and names which leaves are VARIANT. Returns `None` when
/// `schema` holds no VARIANT, so a caller can keep the array it already has. A row whose statistic
/// is absent, or whose physical struct is not the expected pair of binaries, becomes NULL.
pub(crate) fn encode_variant_stats(
    array: &StructArray,
    schema: &Schema,
) -> DeltaResult<Option<StructArray>> {
    if !schema_contains_variant_type(schema) {
        return Ok(None);
    }
    let nulls = array.nulls().cloned();
    let (arrow_fields, columns, _) = array.clone().into_parts();
    let (arrow_fields, columns) = rewrite_fields(&arrow_fields, columns, schema, true)?;
    Ok(Some(StructArray::try_new(arrow_fields, columns, nulls)?))
}

/// Encodes a column of VARIANT statistics held as the physical variant struct into the Z85 strings
/// the stats JSON format carries. The children are matched by name, so either field order works.
///
/// A row that is NULL, or whose `metadata`/`value` children are missing, not binary, or NULL,
/// becomes NULL.
fn encode_z85_variant_stats(array: &dyn ArrowArray) -> ArrowArrayRef {
    let num_rows = array.len();
    let null_column = || new_null_array(&ArrowDataType::Utf8, num_rows);
    let Some(struct_array) = array.as_struct_opt() else {
        return null_column();
    };
    let binary_child = |name| {
        struct_array
            .column_by_name(name)
            .and_then(|column| column.as_binary_opt::<i32>())
    };
    let (Some(metadatas), Some(values)) = (
        binary_child(VARIANT_METADATA_FIELD),
        binary_child(VARIANT_VALUE_FIELD),
    ) else {
        return null_column();
    };

    let mut encoded = StringBuilder::new();
    for row in 0..num_rows {
        if struct_array.is_null(row) || metadatas.is_null(row) || values.is_null(row) {
            encoded.append_null();
        } else {
            encoded.append_value(encode_z85_variant(metadatas.value(row), values.value(row)));
        }
    }
    Arc::new(encoded.finish())
}

/// Joins one statistic's variant metadata and value into the Z85 string the stats JSON format
/// carries, the inverse of [`decode_z85_variant`].
///
/// The halves are concatenated and zero-filled up to the four-byte group Z85 encodes. `z85::encode`
/// alone would not do: it marks an unaligned tail with `#` rather than filling it, which no Delta
/// reader expects.
fn encode_z85_variant(metadata: &[u8], value: &[u8]) -> String {
    let mut combined = Vec::with_capacity(metadata.len() + value.len());
    combined.extend_from_slice(metadata);
    combined.extend_from_slice(value);
    combined.resize(combined.len().next_multiple_of(4), 0);
    z85::encode(&combined)
}

// === Shared traversal ===

/// Rewrites the VARIANT leaves of one struct level, pairing each Arrow column with the kernel field
/// that says whether it is one. `encode` picks the direction.
///
/// Passes the level through untouched when `columns`, `arrow_fields`, and `schema` disagree on
/// width. That never happens for data matching `schema`, and it is safer than zipping the two and
/// silently dropping the surplus.
fn rewrite_fields(
    arrow_fields: &ArrowFields,
    columns: Vec<ArrowArrayRef>,
    schema: &Schema,
    encode: bool,
) -> DeltaResult<(ArrowFields, Vec<ArrowArrayRef>)> {
    if columns.len() != schema.num_fields() || columns.len() != arrow_fields.len() {
        return Ok((arrow_fields.clone(), columns));
    }
    let columns = columns
        .into_iter()
        .zip(schema.fields())
        .map(|(column, field)| rewrite_field(column, field, encode))
        .collect::<DeltaResult<Vec<_>>>()?;
    let fields = arrow_fields
        .iter()
        .zip(columns.iter())
        .map(|(arrow_field, column)| -> ArrowFieldRef {
            Arc::new(ArrowField::clone(arrow_field).with_data_type(column.data_type().clone()))
        })
        .collect();
    Ok((fields, columns))
}

/// Rewrites the VARIANT leaves under one field, rebuilding the structs above them.
fn rewrite_field(
    array: ArrowArrayRef,
    field: &StructField,
    encode: bool,
) -> DeltaResult<ArrowArrayRef> {
    match field.data_type() {
        DataType::Variant(_) if encode => Ok(encode_z85_variant_stats(array.as_ref())),
        DataType::Variant(_) => {
            let ArrowDataType::Struct(fields) = ArrowDataType::try_from_kernel(field.data_type())?
            else {
                return Ok(array);
            };
            decode_z85_variant_stats(array.as_ref(), &fields)
        }
        DataType::Struct(stype) if schema_contains_variant_type(stype) => {
            let Some(struct_array) = array.as_struct_opt() else {
                return Ok(array);
            };
            let nulls = struct_array.nulls().cloned();
            let (arrow_fields, columns, _) = struct_array.clone().into_parts();
            let (arrow_fields, columns) = rewrite_fields(&arrow_fields, columns, stype, encode)?;
            Ok(Arc::new(StructArray::try_new(
                arrow_fields,
                columns,
                nulls,
            )?))
        }
        _ => Ok(array),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{BinaryArray, Int64Array, StringArray};
    use crate::parquet::variant::VariantBuilder;
    use crate::schema::schema;

    /// A one-key statistic object, built by the variant writer so its bytes are well-formed.
    fn variant_object() -> (Vec<u8>, Vec<u8>) {
        let mut builder = VariantBuilder::new();
        let mut object = builder.new_object();
        object.insert("$.a", 1i32);
        object.finish();
        builder.finish()
    }

    fn struct_array(fields: Vec<(&str, ArrowDataType, ArrowArrayRef)>) -> ArrowArrayRef {
        let fields: Vec<(ArrowFieldRef, ArrowArrayRef)> = fields
            .into_iter()
            .map(|(name, data_type, column)| {
                (
                    Arc::new(ArrowField::new(name, data_type, true)) as ArrowFieldRef,
                    column,
                )
            })
            .collect();
        Arc::new(StructArray::from(fields))
    }

    #[test]
    fn test_decode_z85_variant_drops_the_encoders_fill_bytes() {
        let (metadata, value) = variant_object();
        assert_ne!(
            (metadata.len() + value.len()) % 4,
            0,
            "fixture must be unaligned, or it never exercises the fill bytes"
        );

        let (decoded_metadata, decoded_value) =
            decode_z85_variant(&encode_z85_variant(&metadata, &value))
                .expect("well-formed variant");

        assert_eq!(decoded_metadata, metadata);
        assert_eq!(decoded_value, value);
    }

    #[test]
    fn test_decode_variant_stats_rewrites_only_variant_leaves() {
        let stats_schema = schema! {
            nullable "minValues": {
                nullable "id": LONG,
                nullable "payload": (DataType::unshredded_variant()),
            },
        };
        let (metadata, value) = variant_object();
        let encoded = encode_z85_variant(&metadata, &value);
        let min_values = struct_array(vec![
            (
                "id",
                ArrowDataType::Int64,
                Arc::new(Int64Array::from(vec![Some(1)])) as ArrowArrayRef,
            ),
            (
                "payload",
                ArrowDataType::Utf8,
                Arc::new(StringArray::from(vec![Some(encoded.as_str())])) as ArrowArrayRef,
            ),
        ]);
        let relaxed = RecordBatch::try_from_iter(vec![("minValues", min_values)]).unwrap();

        let decoded = decode_variant_stats(relaxed, &stats_schema).unwrap();
        let min_values = decoded.column(0).as_struct();
        assert_eq!(min_values.column(0).data_type(), &ArrowDataType::Int64);
        let payload = min_values.column(1).as_struct();
        assert_eq!(payload.null_count(), 0);
        assert_eq!(
            payload
                .column_by_name(VARIANT_METADATA_FIELD)
                .unwrap()
                .as_binary::<i32>()
                .value(0),
            metadata
        );
        assert_eq!(
            payload
                .column_by_name(VARIANT_VALUE_FIELD)
                .unwrap()
                .as_binary::<i32>()
                .value(0),
            value
        );
    }

    /// The two directions are inverses over the wire form: the fill bytes the encoder adds are
    /// exactly what the decoder trims, so a statistic survives a full round trip unchanged.
    #[test]
    fn test_variant_stats_round_trip_through_both_directions() {
        let stats_schema = schema! {
            nullable "minValues": {
                nullable "id": LONG,
                nullable "payload": (DataType::unshredded_variant()),
            },
        };
        let (metadata, value) = variant_object();
        let encoded = encode_z85_variant(&metadata, &value);
        let min_values = struct_array(vec![
            (
                "id",
                ArrowDataType::Int64,
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrowArrayRef,
            ),
            (
                "payload",
                ArrowDataType::Utf8,
                Arc::new(StringArray::from(vec![Some(encoded.as_str()), None])) as ArrowArrayRef,
            ),
        ]);
        let json_form = RecordBatch::try_from_iter(vec![("minValues", min_values)]).unwrap();

        let struct_form = decode_variant_stats(json_form.clone(), &stats_schema).unwrap();
        let round_tripped =
            encode_variant_stats(&StructArray::from(struct_form.clone()), &stats_schema)
                .unwrap()
                .expect("schema holds a variant");

        assert_eq!(RecordBatch::from(round_tripped), json_form);
    }

    #[test]
    fn test_encode_variant_stats_rewrites_only_variant_leaves() {
        let stats_schema = schema! {
            nullable "minValues": {
                nullable "id": LONG,
                nullable "payload": (DataType::unshredded_variant()),
            },
        };
        let (metadata, value) = variant_object();
        let payload = struct_array(vec![
            (
                VARIANT_METADATA_FIELD,
                ArrowDataType::Binary,
                Arc::new(BinaryArray::from(vec![Some(metadata.as_slice()), None])) as ArrowArrayRef,
            ),
            (
                VARIANT_VALUE_FIELD,
                ArrowDataType::Binary,
                Arc::new(BinaryArray::from(vec![Some(value.as_slice()), None])) as ArrowArrayRef,
            ),
        ]);
        let min_values = struct_array(vec![
            (
                "id",
                ArrowDataType::Int64,
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrowArrayRef,
            ),
            ("payload", payload.data_type().clone(), payload),
        ]);
        let input = StructArray::from(vec![(
            Arc::new(ArrowField::new(
                "minValues",
                min_values.data_type().clone(),
                true,
            )) as ArrowFieldRef,
            min_values,
        )]);

        let encoded = encode_variant_stats(&input, &stats_schema)
            .unwrap()
            .expect("schema holds a variant");
        let min_values = encoded.column(0).as_struct();
        assert_eq!(min_values.column(0).data_type(), &ArrowDataType::Int64);
        let payload = min_values.column(1).as_string::<i32>();
        assert_eq!(payload.value(0), encode_z85_variant(&metadata, &value));
        assert!(
            payload.is_null(1),
            "a NULL statistic encodes as a NULL string"
        );
    }

    /// A schema with no VARIANT means there is nothing to rewrite, so the caller keeps its array.
    #[test]
    fn test_encode_variant_stats_skips_a_schema_without_variant() {
        let stats_schema = schema! { nullable "id": LONG };
        let input = StructArray::from(vec![(
            Arc::new(ArrowField::new("id", ArrowDataType::Int64, true)) as ArrowFieldRef,
            Arc::new(Int64Array::from(vec![Some(1)])) as ArrowArrayRef,
        )]);

        assert!(encode_variant_stats(&input, &stats_schema)
            .unwrap()
            .is_none());
    }

    /// Every shape the split rejects lands on the same fail-open path: a NULL statistic, which
    /// every skipping predicate reads as "keep the file".
    #[test]
    fn test_decode_variant_stats_nulls_an_undecodable_statistic() {
        let stats_schema = schema! {
            nullable "minValues": {
                nullable "payload": (DataType::unshredded_variant()),
            },
        };
        let (metadata, value) = variant_object();
        let truncated_metadata = encode_z85_variant(&metadata[..metadata.len() - 1], &[]);
        let bad_version = {
            let mut metadata = metadata.clone();
            metadata[0] = (metadata[0] & 0xF0) | 0x02;
            encode_z85_variant(&metadata, &value)
        };
        let not_an_object = encode_z85_variant(&metadata, &[0x0c]);
        let min_values = struct_array(vec![(
            "payload",
            ArrowDataType::Utf8,
            Arc::new(StringArray::from(vec![
                Some("not z85"),
                Some(truncated_metadata.as_str()),
                Some(bad_version.as_str()),
                Some(not_an_object.as_str()),
                None,
            ])) as ArrowArrayRef,
        )]);
        let relaxed = RecordBatch::try_from_iter(vec![("minValues", min_values)]).unwrap();

        let decoded = decode_variant_stats(relaxed, &stats_schema).unwrap();
        let payload = decoded.column(0).as_struct().column(0).as_struct();
        assert_eq!(payload.null_count(), 5);
    }
}
