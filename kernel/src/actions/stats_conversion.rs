//! Conversion utilities between JSON stats and parsed stats formats

use std::collections::HashMap;

use serde_json::{json, Value};

use crate::expressions::Scalar;
use crate::schema::{DataType, StructType};
use crate::statistics::StatsParsed;
use crate::{DeltaResult, Error};

/// Convert JSON stats string to StatsParsed
///
/// This is used when writing checkpoints to generate stats_parsed from JSON stats
pub fn parse_json_stats_to_parsed(
    json_stats: &str,
    table_schema: &StructType,
) -> DeltaResult<StatsParsed> {
    let value: Value = serde_json::from_str(json_stats)
        .map_err(|e| Error::generic(format!("Failed to parse stats JSON: {}", e)))?;

    let num_records = value["numRecords"]
        .as_i64()
        .ok_or_else(|| Error::generic("Missing numRecords in stats"))?;

    let min_values = parse_json_stat_values(&value["minValues"], table_schema)?;
    let max_values = parse_json_stat_values(&value["maxValues"], table_schema)?;
    let null_count = parse_json_null_counts(&value["nullCount"], table_schema)?;

    let tight_bounds = value["tightBounds"].as_bool();

    Ok(StatsParsed {
        num_records,
        min_values,
        max_values,
        null_count,
        tight_bounds,
    })
}

/// Parse min/max values from JSON
fn parse_json_stat_values(
    values: &Value,
    table_schema: &StructType,
) -> DeltaResult<HashMap<String, Option<Scalar>>> {
    let mut result = HashMap::new();

    if let Some(obj) = values.as_object() {
        for (key, value) in obj {
            // Find the field in the schema to determine the correct type
            if let Some(field) = table_schema.field(key) {
                let stat_value = json_value_to_stat_value(value, field.data_type())?;
                result.insert(field.physical_name().to_string(), Some(stat_value));
            }
        }
    }

    Ok(result)
}

/// Parse null counts from JSON
fn parse_json_null_counts(
    values: &Value,
    table_schema: &StructType,
) -> DeltaResult<HashMap<String, i64>> {
    let mut result = HashMap::new();

    if let Some(obj) = values.as_object() {
        for (key, value) in obj {
            if let Some(field) = table_schema.field(key) {
                let count = value
                    .as_i64()
                    .ok_or_else(|| Error::generic(format!("Invalid null count for {}", key)))?;
                result.insert(field.physical_name().to_string(), count);
            }
        }
    }

    Ok(result)
}

/// Convert a JSON value to StatValue based on the expected data type
fn json_value_to_stat_value(value: &Value, data_type: &DataType) -> DeltaResult<Scalar> {
    use crate::schema::PrimitiveType;

    match (value, data_type) {
        (Value::Bool(b), DataType::Primitive(PrimitiveType::Boolean)) => Ok(Scalar::Boolean(*b)),
        (Value::Number(n), DataType::Primitive(PrimitiveType::Byte)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid byte value"))?;
            Ok(Scalar::Byte(v as i8))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Short)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid short value"))?;
            Ok(Scalar::Short(v as i16))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Integer)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid integer value"))?;
            Ok(Scalar::Integer(v as i32))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Long)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid long value"))?;
            Ok(Scalar::Long(v))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Float)) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Error::generic("Invalid float value"))?;
            Ok(Scalar::Float(v as f32))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Double)) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Error::generic("Invalid double value"))?;
            Ok(Scalar::Double(v))
        }
        (Value::String(s), DataType::Primitive(PrimitiveType::String)) => {
            Ok(Scalar::String(s.clone()))
        }
        (Value::String(s), DataType::Primitive(PrimitiveType::Binary)) => {
            // For now, treat binary as a string
            // TODO: Implement proper base64 encoding/decoding
            Ok(Scalar::Binary(s.as_bytes().to_vec()))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Date)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid date value"))?;
            Ok(Scalar::Date(v as i32))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Timestamp)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid timestamp value"))?;
            Ok(Scalar::Timestamp(v))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::TimestampNtz)) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::generic("Invalid timestamp_ntz value"))?;
            Ok(Scalar::TimestampNtz(v))
        }
        (Value::String(s), DataType::Primitive(PrimitiveType::Date)) => {
            // Try to parse date string
            let days = s
                .parse::<i32>()
                .map_err(|_| Error::generic("Invalid date string"))?;
            Ok(Scalar::Date(days))
        }
        (Value::String(s), DataType::Primitive(PrimitiveType::Timestamp)) => {
            // Try to parse timestamp string
            let micros = s
                .parse::<i64>()
                .map_err(|_| Error::generic("Invalid timestamp string"))?;
            Ok(Scalar::Timestamp(micros))
        }
        (Value::String(s), DataType::Primitive(PrimitiveType::TimestampNtz)) => {
            // Try to parse timestamp string
            let micros = s
                .parse::<i64>()
                .map_err(|_| Error::generic("Invalid timestamp string"))?;
            Ok(Scalar::TimestampNtz(micros))
        }
        (Value::Number(n), DataType::Primitive(PrimitiveType::Decimal(_))) => {
            // For decimal, use the number as is (could be int or float)
            if let Some(v) = n.as_i64() {
                Ok(Scalar::Long(v))
            } else if let Some(v) = n.as_f64() {
                Ok(Scalar::Double(v))
            } else {
                Err(Error::generic("Invalid decimal value"))
            }
        }
        _ => Err(Error::generic(format!(
            "Type mismatch: cannot convert {:?} to {:?}",
            value, data_type
        ))),
    }
}

/// Convert StatsParsed to JSON stats string
///
/// This is used as a fallback when only parsed stats are available
/// but JSON stats are needed for compatibility
pub fn serialize_stats_to_json(parsed: &StatsParsed) -> DeltaResult<String> {
    let mut obj = serde_json::Map::new();

    // Add numRecords
    obj.insert("numRecords".to_string(), json!(parsed.num_records));

    // Add minValues
    if !parsed.min_values.is_empty() {
        let mut min_obj = serde_json::Map::new();
        for (key, value) in &parsed.min_values {
            if let Some(stat_value) = value {
                min_obj.insert(key.clone(), stat_value_to_json(stat_value));
            }
        }
        if !min_obj.is_empty() {
            obj.insert("minValues".to_string(), Value::Object(min_obj));
        }
    }

    // Add maxValues
    if !parsed.max_values.is_empty() {
        let mut max_obj = serde_json::Map::new();
        for (key, value) in &parsed.max_values {
            if let Some(stat_value) = value {
                max_obj.insert(key.clone(), stat_value_to_json(stat_value));
            }
        }
        if !max_obj.is_empty() {
            obj.insert("maxValues".to_string(), Value::Object(max_obj));
        }
    }

    // Add nullCount
    if !parsed.null_count.is_empty() {
        let mut null_obj = serde_json::Map::new();
        for (key, count) in &parsed.null_count {
            null_obj.insert(key.clone(), json!(*count));
        }
        obj.insert("nullCount".to_string(), Value::Object(null_obj));
    }

    // Add tightBounds if present
    if let Some(tight_bounds) = parsed.tight_bounds {
        obj.insert("tightBounds".to_string(), json!(tight_bounds));
    }

    serde_json::to_string(&Value::Object(obj))
        .map_err(|e| Error::generic(format!("Failed to serialize stats to JSON: {}", e)))
}

/// Convert StatValue to JSON Value
fn stat_value_to_json(value: &Scalar) -> Value {
    match value {
        Scalar::Boolean(b) => json!(*b),
        Scalar::Byte(v) => json!(*v),
        Scalar::Short(v) => json!(*v),
        Scalar::Integer(v) => json!(*v),
        Scalar::Long(v) => json!(*v),
        Scalar::Float(v) => json!(*v),
        Scalar::Double(v) => json!(*v),
        Scalar::String(s) => json!(s),
        Scalar::Binary(bytes) => {
            // For now, convert to string
            // TODO: Implement proper base64 encoding
            json!(String::from_utf8_lossy(bytes))
        }
        Scalar::Date(days) => json!(*days),
        Scalar::Timestamp(micros) => json!(*micros),
        Scalar::TimestampNtz(micros) => json!(*micros),
        Scalar::Decimal(decimal) => {
            // Convert decimal to JSON number using bits
            json!(decimal.bits())
        }
        Scalar::Null(_) => Value::Null,
        Scalar::Struct(_) => {
            // Struct stats are not supported in min/max values
            Value::Null
        }
        Scalar::Array(_) => {
            // Array stats are not supported in min/max values
            Value::Null
        }
        Scalar::Map(_) => {
            // Map stats are not supported in min/max values
            Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    #[test]
    fn test_json_to_parsed_conversion() {
        let table_schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("name", DataType::STRING, true),
            StructField::new("value", DataType::DOUBLE, true),
        ]);

        let json_stats = r#"{
            "numRecords": 100,
            "minValues": {"id": 1, "name": "Alice", "value": 10.5},
            "maxValues": {"id": 100, "name": "Zoe", "value": 99.9},
            "nullCount": {"id": 0, "name": 5, "value": 2},
            "tightBounds": true
        }"#;

        let parsed = parse_json_stats_to_parsed(json_stats, &table_schema).unwrap();

        assert_eq!(parsed.num_records, 100);
        assert_eq!(parsed.min_values.get("id"), Some(&Some(Scalar::Integer(1))));
        assert_eq!(
            parsed.min_values.get("name"),
            Some(&Some(Scalar::String("Alice".to_string())))
        );
        assert_eq!(
            parsed.max_values.get("id"),
            Some(&Some(Scalar::Integer(100)))
        );
        assert_eq!(parsed.null_count.get("id"), Some(&0));
        assert_eq!(parsed.null_count.get("name"), Some(&5));
        assert_eq!(parsed.tight_bounds, Some(true));
    }

    #[test]
    fn test_parsed_to_json_conversion() {
        let mut min_values = HashMap::new();
        min_values.insert("id".to_string(), Some(Scalar::Integer(1)));
        min_values.insert(
            "name".to_string(),
            Some(Scalar::String("Alice".to_string())),
        );

        let mut max_values = HashMap::new();
        max_values.insert("id".to_string(), Some(Scalar::Integer(100)));
        max_values.insert("name".to_string(), Some(Scalar::String("Zoe".to_string())));

        let mut null_count = HashMap::new();
        null_count.insert("id".to_string(), 0);
        null_count.insert("name".to_string(), 5);

        let parsed = StatsParsed {
            num_records: 100,
            min_values,
            max_values,
            null_count,
            tight_bounds: Some(true),
        };

        let json = serialize_stats_to_json(&parsed).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["numRecords"], 100);
        assert_eq!(value["minValues"]["id"], 1);
        assert_eq!(value["minValues"]["name"], "Alice");
        assert_eq!(value["maxValues"]["id"], 100);
        assert_eq!(value["nullCount"]["id"], 0);
        assert_eq!(value["tightBounds"], true);
    }

    #[test]
    fn test_round_trip_conversion() {
        let table_schema = StructType::new_unchecked(vec![
            StructField::new("id", DataType::LONG, false),
            StructField::new("active", DataType::BOOLEAN, false),
        ]);

        let original_json = r#"{
            "numRecords": 50,
            "minValues": {"id": 10, "active": false},
            "maxValues": {"id": 60, "active": true},
            "nullCount": {"id": 1, "active": 0}
        }"#;

        // Convert JSON to parsed
        let parsed = parse_json_stats_to_parsed(original_json, &table_schema).unwrap();

        // Convert back to JSON
        let new_json = serialize_stats_to_json(&parsed).unwrap();

        // Parse both JSONs to compare
        let original_value: Value = serde_json::from_str(original_json).unwrap();
        let new_value: Value = serde_json::from_str(&new_json).unwrap();

        assert_eq!(original_value["numRecords"], new_value["numRecords"]);
        assert_eq!(original_value["minValues"], new_value["minValues"]);
        assert_eq!(original_value["maxValues"], new_value["maxValues"]);
        assert_eq!(original_value["nullCount"], new_value["nullCount"]);
    }
}
