//! Delta file-statistics JSON encoding for Arrow-based engines.

use crate::arrow::array::{
    Array, AsArray, BinaryArray, NullBufferBuilder, StringArray, StructArray,
};
use crate::arrow::buffer::OffsetBuffer;
use crate::arrow::datatypes::{DataType as ArrowDataType, Float32Type, Float64Type, TimeUnit};
use crate::arrow::util::display::{ArrayFormatter, FormatOptions};
use crate::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use crate::engine::arrow_expression::evaluate_expression::{
    STATS_TIMESTAMP_NTZ_FORMAT, STATS_TIMESTAMP_TZ_FORMAT,
};
use crate::schema::{DataType, PrimitiveType, StructType};
use crate::{KernelError, KernelResult};

/// Encodes structured file statistics as Delta `add.stats` JSON strings.
///
/// `physical_stats_schema` describes the supplied statistics, using physical column names and
/// retaining VARIANT and Geometry type identities. Use the physical schema returned by
/// [`ScanBuilder::stats_output_schemas`](crate::scan::ScanBuilder::stats_output_schemas) when
/// serializing retained scan statistics. Only the supplied statistics are encoded; this function
/// does not collect or recompute bounds.
///
/// Null fields are omitted, and a null statistics struct produces a null string. Decimal precision
/// is preserved, non-finite floats become `"NaN"`, `"Infinity"`, or `"-Infinity"`, and timestamps
/// are truncated down to milliseconds. Unshredded VARIANT bounds encode as padded Z85 strings.
/// Geometry bounds encode as WKT points; they may be supplied as 2D Point WKB or as structs with
/// FLOAT64 `x` and `y` fields. Ordinary structs with those field names remain JSON objects.
///
/// Strings may use Arrow's UTF-8, large UTF-8, or UTF-8 view representation. Timestamps use
/// microseconds; timezone-aware arrays must be annotated as `UTC`, `Etc/UTC`, or a zero fixed
/// offset. Other primitive arrays must match the schema's Arrow representation.
///
/// `numRecords` and `tightBounds` are preserved. When passing the result to
/// [`update_deletion_vectors`],
/// keep the physical row count: the transaction validates it and widens the replacement Add's
/// bounds. Preserve the original `tightBounds` for the paired Remove action.
///
/// Returns an error if the Arrow data does not match the supplied schema or contains an unsupported
/// statistics type, shredded VARIANT, or invalid Geometry bound.
///
/// ```
/// use std::sync::Arc;
/// use delta_kernel::arrow::array::{ArrayRef, Int64Array, StructArray};
/// use delta_kernel::arrow::datatypes::{DataType, Field};
/// use delta_kernel::engine::arrow_stats::serialize_stats;
/// use delta_kernel::schema::schema;
///
/// let stats = StructArray::from(vec![(
///     Arc::new(Field::new("numRecords", DataType::Int64, true)),
///     Arc::new(Int64Array::from(vec![10])) as ArrayRef,
/// )]);
/// let schema = schema! { nullable "numRecords": LONG };
/// let json = serialize_stats(&stats, &schema)?;
/// assert_eq!(json.value(0), r#"{"numRecords":10}"#);
/// # Ok::<(), delta_kernel::KernelError>(())
/// ```
///
/// [`update_deletion_vectors`]: crate::transaction::Transaction::update_deletion_vectors
pub fn serialize_stats(
    stats: &StructArray,
    physical_stats_schema: &StructType,
) -> KernelResult<StringArray> {
    let options = FormatOptions::default()
        .with_timestamp_format(Some(STATS_TIMESTAMP_NTZ_FORMAT))
        .with_timestamp_tz_format(Some(STATS_TIMESTAMP_TZ_FORMAT));
    let encoder = StatsEncoder::struct_encoder(stats, physical_stats_schema, &options)?;
    let mut out = String::new();
    let mut offsets = Vec::with_capacity(stats.len() + 1);
    let mut nulls = NullBufferBuilder::new(stats.len());
    offsets.push(0);
    for row in 0..stats.len() {
        if stats.is_null(row) {
            nulls.append_null();
        } else {
            encoder.write(row, &mut out)?;
            nulls.append_non_null();
        }
        offsets.push(i32::try_from(out.len()).map_err(|_| {
            KernelError::generic("Statistics JSON exceeds the Arrow string offset limit")
        })?);
    }
    Ok(StringArray::try_new(
        OffsetBuffer::new(offsets.into()),
        out.into_bytes().into(),
        nulls.finish(),
    )?)
}

struct StatsEncoder<'a> {
    values: &'a dyn Array,
    kind: StatsKind<'a>,
}

enum StatsKind<'a> {
    Struct(Vec<(&'a str, StatsEncoder<'a>)>),
    Variant(&'a BinaryArray, &'a BinaryArray),
    #[cfg(feature = "geo-type-in-dev")]
    Geometry,
    Primitive(&'a PrimitiveType, ArrayFormatter<'a>),
}

impl<'a> StatsEncoder<'a> {
    fn struct_encoder(
        values: &'a StructArray,
        schema: &'a StructType,
        options: &FormatOptions<'a>,
    ) -> KernelResult<Self> {
        if values.num_columns() != schema.fields().len() {
            return Err(KernelError::generic(
                "Statistics fields do not match schema",
            ));
        }
        let fields = schema
            .fields()
            .map(|field| {
                let array = values.column_by_name(field.name()).ok_or_else(|| {
                    KernelError::generic(format!("Missing statistics field {}", field.name()))
                })?;
                Ok((
                    field.name().as_str(),
                    Self::new(array.as_ref(), field.data_type(), options)?,
                ))
            })
            .collect::<KernelResult<_>>()?;
        Ok(Self {
            values,
            kind: StatsKind::Struct(fields),
        })
    }

    fn new(
        values: &'a dyn Array,
        data_type: &'a DataType,
        options: &FormatOptions<'a>,
    ) -> KernelResult<Self> {
        let kind = match data_type {
            DataType::Struct(schema) => {
                let array = values
                    .as_struct_opt()
                    .ok_or_else(|| type_error(data_type))?;
                return Self::struct_encoder(array, schema, options);
            }
            DataType::Variant(_) if data_type == &DataType::unshredded_variant() => {
                let array = values
                    .as_struct_opt()
                    .ok_or_else(|| type_error(data_type))?;
                if array.num_columns() != 2 {
                    return Err(type_error(data_type));
                }
                let binary = |name| {
                    array
                        .column_by_name(name)
                        .and_then(|a| a.as_binary_opt::<i32>())
                        .ok_or_else(|| type_error(data_type))
                };
                StatsKind::Variant(binary("metadata")?, binary("value")?)
            }
            #[cfg(feature = "geo-type-in-dev")]
            DataType::Primitive(PrimitiveType::Geometry(_)) => StatsKind::Geometry,
            DataType::Primitive(primitive) => {
                let expected: ArrowDataType = data_type.try_into_arrow()?;
                let compatible = match (primitive, values.data_type()) {
                    (PrimitiveType::String, actual) => actual
                        .try_into_kernel()
                        .is_ok_and(|actual: DataType| actual == DataType::STRING),
                    (
                        PrimitiveType::Timestamp,
                        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                    ) => {
                        matches!(timezone.as_ref(), "UTC" | "Etc/UTC")
                            || timezone
                                .parse::<chrono::FixedOffset>()
                                .is_ok_and(|offset| offset.local_minus_utc() == 0)
                    }
                    _ => values.data_type() == &expected,
                };
                if !compatible {
                    return Err(type_error(data_type));
                }
                match primitive {
                    PrimitiveType::Boolean
                    | PrimitiveType::Byte
                    | PrimitiveType::Short
                    | PrimitiveType::Integer
                    | PrimitiveType::Long
                    | PrimitiveType::Float
                    | PrimitiveType::Double
                    | PrimitiveType::Decimal(_)
                    | PrimitiveType::String
                    | PrimitiveType::Date
                    | PrimitiveType::Timestamp
                    | PrimitiveType::TimestampNtz => {
                        StatsKind::Primitive(primitive, ArrayFormatter::try_new(values, options)?)
                    }
                    _ => return Err(type_error(data_type)),
                }
            }
            _ => return Err(type_error(data_type)),
        };
        Ok(Self { values, kind })
    }

    fn write(&self, row: usize, out: &mut String) -> KernelResult<()> {
        match &self.kind {
            StatsKind::Struct(fields) => {
                out.push('{');
                let mut first = true;
                for (name, encoder) in fields {
                    if encoder.values.is_null(row) {
                        continue;
                    }
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    write_string(name, out)?;
                    out.push(':');
                    encoder.write(row, out)?;
                }
                out.push('}');
            }
            StatsKind::Variant(metadata, value) => {
                if metadata.is_null(row) || value.is_null(row) {
                    return Err(KernelError::generic(
                        "Null VARIANT statistics metadata or value",
                    ));
                }
                let mut bytes = [metadata.value(row), value.value(row)].concat();
                bytes.resize(bytes.len().next_multiple_of(4), 0);
                write_string(&z85::encode(&bytes), out)?;
            }
            #[cfg(feature = "geo-type-in-dev")]
            StatsKind::Geometry => write_string(&geometry_point(self.values, row)?, out)?,
            StatsKind::Primitive(primitive, formatter) => {
                let float = match primitive {
                    PrimitiveType::Float => Some(f64::from(
                        self.values.as_primitive::<Float32Type>().value(row),
                    )),
                    PrimitiveType::Double => {
                        Some(self.values.as_primitive::<Float64Type>().value(row))
                    }
                    _ => None,
                };
                if let Some(value) = float.filter(|v| !v.is_finite()) {
                    let name = if value.is_nan() {
                        "NaN"
                    } else if value.is_sign_positive() {
                        "Infinity"
                    } else {
                        "-Infinity"
                    };
                    write_string(name, out)?;
                } else if matches!(
                    primitive,
                    PrimitiveType::String
                        | PrimitiveType::Date
                        | PrimitiveType::Timestamp
                        | PrimitiveType::TimestampNtz
                ) {
                    let mut value = String::new();
                    formatter.value(row).write(&mut value)?;
                    write_string(&value, out)?;
                } else {
                    // Writing directly avoids rounding decimal and large integer bounds through
                    // f64.
                    formatter.value(row).write(out)?;
                }
            }
        }
        Ok(())
    }
}

fn write_string(value: &str, out: &mut String) -> KernelResult<()> {
    out.push_str(&serde_json::to_string(value)?);
    Ok(())
}

fn type_error(data_type: &DataType) -> KernelError {
    KernelError::generic(format!(
        "Unsupported or mismatched statistics type: {data_type}"
    ))
}

#[cfg(feature = "geo-type-in-dev")]
fn geometry_point(values: &dyn Array, row: usize) -> KernelResult<String> {
    let invalid = || KernelError::generic("Geometry statistics require a non-null 2D point");
    let (x, y) = if let Some(array) = values.as_struct_opt() {
        if array.num_columns() != 2 {
            return Err(invalid());
        }
        let coordinate = |name| {
            let array = array
                .column_by_name(name)
                .and_then(|a| a.as_primitive_opt::<Float64Type>())
                .ok_or_else(invalid)?;
            if array.is_null(row) {
                return Err(invalid());
            }
            Ok(array.value(row))
        };
        (coordinate("x")?, coordinate("y")?)
    } else {
        let bytes = values
            .as_binary_opt::<i32>()
            .ok_or_else(invalid)?
            .value(row);
        let [order, a, b, c, d, coordinates @ ..] = bytes else {
            return Err(invalid());
        };
        let (little_endian, tag) = match order {
            0 => (false, u32::from_be_bytes([*a, *b, *c, *d])),
            1 => (true, u32::from_le_bytes([*a, *b, *c, *d])),
            _ => return Err(invalid()),
        };
        if tag != 1 || coordinates.len() != 16 {
            return Err(invalid());
        }
        let coordinate = |bytes: &[u8]| -> KernelResult<f64> {
            let bytes = bytes.try_into().map_err(|_| invalid())?;
            Ok(if little_endian {
                f64::from_le_bytes(bytes)
            } else {
                f64::from_be_bytes(bytes)
            })
        };
        (
            coordinate(&coordinates[..8])?,
            coordinate(&coordinates[8..])?,
        )
    };
    if !x.is_finite() || !y.is_finite() {
        return Err(invalid());
    }
    Ok(format!("POINT ({x} {y})"))
}

#[cfg(test)]
mod tests;
