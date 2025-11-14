//! Helper module for reading stats_parsed from checkpoint Parquet files

use std::collections::HashMap;

use crate::engine_data::{GetData, TypedGetData};
use crate::expressions::Scalar;
use crate::schema::{DataType, PrimitiveType, StructType};
use crate::statistics::StatsParsed;
use crate::DeltaResult;

/// Parse stats_parsed struct from checkpoint Parquet data
///
/// Reads the complete stats_parsed structure from Parquet checkpoints, including:
/// - numRecords (required)
/// - minValues (nested struct by column)
/// - maxValues (nested struct by column)
/// - nullCount (nested struct by column)
/// - tightBounds (optional boolean)
pub(crate) fn parse_stats_parsed_from_getters<'a>(
    row_index: usize,
    stats_parsed_getter: &'a dyn GetData<'a>,
    table_schema: &StructType,
) -> DeltaResult<Option<StatsParsed>> {
    // Try to get numRecords - if it fails, stats_parsed doesn't exist
    let num_records = match stats_parsed_getter.get_long(row_index, "add.statsParsed.numRecords")? {
        Some(n) => n,
        None => return Ok(None), // stats_parsed column doesn't exist
    };

    // Parse minValues for each column in the table
    let min_values = parse_stat_values_from_parquet(
        row_index,
        stats_parsed_getter,
        table_schema,
        "add.statsParsed.minValues",
    )?;

    // Parse maxValues for each column in the table
    let max_values = parse_stat_values_from_parquet(
        row_index,
        stats_parsed_getter,
        table_schema,
        "add.statsParsed.maxValues",
    )?;

    // Parse nullCount for each column in the table
    let null_count = parse_null_counts_from_parquet(
        row_index,
        stats_parsed_getter,
        table_schema,
        "add.statsParsed.nullCount",
    )?;

    // Parse tightBounds (optional)
    let tight_bounds: Option<bool> =
        stats_parsed_getter.get_opt(row_index, "add.statsParsed.tightBounds")?;

    Ok(Some(StatsParsed {
        num_records,
        min_values,
        max_values,
        null_count,
        tight_bounds,
    }))
}

/// Parse min/max values from Parquet for all columns in the schema
fn parse_stat_values_from_parquet<'a>(
    row_index: usize,
    getter: &'a dyn GetData<'a>,
    schema: &StructType,
    base_path: &str, // e.g., "add.statsParsed.minValues"
) -> DeltaResult<HashMap<String, Option<Scalar>>> {
    let mut result = HashMap::new();

    // Iterate through all fields in the table schema
    for field in schema.fields() {
        let physical_name = field.physical_name();
        let field_path = format!("{}.{}", base_path, physical_name);

        // Try to read the value based on the field's data type
        let value_opt =
            read_scalar_from_parquet(row_index, getter, &field_path, field.data_type())?;

        // Only include stats-eligible columns
        if crate::stats_schema::is_stats_eligible_type(field.data_type()) {
            result.insert(physical_name.to_string(), value_opt);
        }
    }

    Ok(result)
}

/// Parse null counts from Parquet for all columns in the schema
fn parse_null_counts_from_parquet<'a>(
    row_index: usize,
    getter: &'a dyn GetData<'a>,
    schema: &StructType,
    base_path: &str, // e.g., "add.statsParsed.nullCount"
) -> DeltaResult<HashMap<String, i64>> {
    let mut result = HashMap::new();

    // Null counts exist for all columns (converted to LONG)
    for field in schema.fields() {
        let physical_name = field.physical_name();
        let field_path = format!("{}.{}", base_path, physical_name);

        // Null counts are always i64
        let count: Option<i64> = getter.get_opt(row_index, &field_path)?;
        if let Some(count) = count {
            result.insert(physical_name.to_string(), count);
        }
    }

    Ok(result)
}

/// Read a scalar value from Parquet based on its data type
fn read_scalar_from_parquet<'a>(
    row_index: usize,
    getter: &'a dyn GetData<'a>,
    field_path: &str,
    data_type: &DataType,
) -> DeltaResult<Option<Scalar>> {
    use DataType::Primitive;

    match data_type {
        Primitive(PrimitiveType::Boolean) => {
            let val: Option<bool> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::Boolean))
        }
        Primitive(PrimitiveType::Byte) => {
            let val: Option<i32> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(|v| Scalar::Byte(v as i8)))
        }
        Primitive(PrimitiveType::Short) => {
            let val: Option<i32> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(|v| Scalar::Short(v as i16)))
        }
        Primitive(PrimitiveType::Integer) => {
            let val: Option<i32> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::Integer))
        }
        Primitive(PrimitiveType::Long) => {
            let val: Option<i64> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::Long))
        }
        Primitive(PrimitiveType::Float) => {
            // Parquet stores floats as f64, need to convert
            let val: Option<i64> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(|bits| Scalar::Float(f32::from_bits(bits as u32))))
        }
        Primitive(PrimitiveType::Double) => {
            // Try to read as i64 (bit representation)
            let val: Option<i64> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(|bits| Scalar::Double(f64::from_bits(bits as u64))))
        }
        Primitive(PrimitiveType::String) => {
            let val: Option<String> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::String))
        }
        Primitive(PrimitiveType::Binary) => {
            // Binary values as byte vectors
            // Note: GetData doesn't have get_binary, so we'd need to extend the trait
            // For now, return None for binary fields
            Ok(None)
        }
        Primitive(PrimitiveType::Date) => {
            let val: Option<i32> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::Date))
        }
        Primitive(PrimitiveType::Timestamp) => {
            let val: Option<i64> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::Timestamp))
        }
        Primitive(PrimitiveType::TimestampNtz) => {
            let val: Option<i64> = getter.get_opt(row_index, field_path)?;
            Ok(val.map(Scalar::TimestampNtz))
        }
        Primitive(PrimitiveType::Decimal(_)) => {
            // Decimal handling is complex, skip for now
            // TODO: Implement decimal reading
            Ok(None)
        }
        DataType::Struct(nested_schema) => {
            // For nested structs, we'd need to recursively parse
            // For now, return None
            // TODO: Implement recursive struct parsing
            Ok(None)
        }
        _ => {
            // Unsupported type (arrays, maps, etc.)
            Ok(None)
        }
    }
}
