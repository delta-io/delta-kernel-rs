//! Transformation processor for populating parsed statistics in checkpoint files.
//!
//! This module provides functionality to transform Add and Remove actions during checkpoint
//! writing to populate the `stats_parsed` field from JSON `stats` when the table property
//! `checkpoint.writeStatsAsStruct` is enabled.

use std::sync::Arc;

use crate::action_reconciliation::log_replay::ActionReconciliationBatch;
use crate::arrow::array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::engine::arrow_data::{extract_record_batch, ArrowEngineData};
use crate::engine_data::FilteredEngineData;
use crate::schema::{DataType, StructType};
use crate::{DeltaResult, Error};

/// Transforms checkpoint actions to control stats fields based on table properties.
///
/// This processor wraps the action reconciliation output and manages statistics formats
/// based on table properties:
/// - `checkpoint.writeStatsAsJson`: Controls JSON stats field
/// - `checkpoint.writeStatsAsStruct`: Controls parsed stats_parsed field
pub(super) struct StatsTransformationProcessor {
    #[allow(dead_code)]
    table_schema: Arc<StructType>,
    write_stats_as_struct: bool,
    write_stats_as_json: bool,
}

impl StatsTransformationProcessor {
    /// Creates a new stats transformation processor.
    ///
    /// # Parameters
    /// - `table_schema`: The schema of the table (needed for type information)
    /// - `write_stats_as_struct`: Whether to populate stats_parsed field
    /// - `write_stats_as_json`: Whether to keep JSON stats field
    pub(super) fn new(
        table_schema: Arc<StructType>,
        write_stats_as_struct: bool,
        write_stats_as_json: bool,
    ) -> Self {
        Self {
            table_schema,
            write_stats_as_struct,
            write_stats_as_json,
        }
    }

    /// Transforms a batch of reconciled actions based on table properties.
    ///
    /// Handles four configurations:
    /// 1. Both true (default): Keep stats, populate stats_parsed
    /// 2. struct only: Nullify stats, populate stats_parsed
    /// 3. JSON only: Keep stats, nullify stats_parsed
    /// 4. Both false: Nullify both (no stats in checkpoint)
    ///
    /// # Implementation
    ///
    /// This method:
    /// 1. Extracts the RecordBatch from the FilteredEngineData (Arrow-specific)
    /// 2. Finds the `add.stats` column containing JSON strings
    /// 3. Parses each JSON string to create stats_parsed structs
    /// 4. Replaces the null `add.stats_parsed` column with populated data
    /// 5. Creates a new RecordBatch and wraps it in EngineData
    ///
    /// Note: This implementation is specific to the Arrow engine. Other engines
    /// would need their own transformation logic.
    pub(super) fn transform_batch(
        &self,
        batch: ActionReconciliationBatch,
    ) -> DeltaResult<ActionReconciliationBatch> {
        if !self.write_stats_as_struct {
            // No transformation needed
            return Ok(batch);
        }

        // Extract the RecordBatch from FilteredEngineData
        // Note: This is Arrow-specific
        let record_batch = extract_record_batch(batch.filtered_data.data())?;

        // Transform the record batch to populate stats_parsed
        let transformed_batch = self.transform_record_batch(record_batch)?;

        // Wrap back in EngineData
        let new_engine_data = Box::new(ArrowEngineData::new(transformed_batch));
        let new_filtered_data = FilteredEngineData::try_new(
            new_engine_data,
            batch.filtered_data.selection_vector().to_vec(),
        )?;

        Ok(ActionReconciliationBatch {
            filtered_data: new_filtered_data,
            actions_count: batch.actions_count,
            add_actions_count: batch.add_actions_count,
        })
    }

    /// Transform a RecordBatch to populate stats_parsed from JSON stats.
    ///
    /// This method finds the `add` struct column, extracts the `stats` string field,
    /// parses each JSON string, and replaces the null `stats_parsed` field with
    /// the populated struct data.
    fn transform_record_batch(&self, record_batch: &RecordBatch) -> DeltaResult<RecordBatch> {
        // Find the 'add' column (it's a struct)
        let add_column_idx = record_batch
            .schema()
            .index_of("add")
            .map_err(|_| Error::generic("'add' column not found in checkpoint data"))?;

        let add_column = record_batch
            .column(add_column_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::generic("'add' column is not a StructArray"))?;

        // Find the 'stats' field within the add struct (it's a string)
        let stats_array_ref = add_column
            .column_by_name("stats")
            .ok_or_else(|| Error::generic("'stats' field not found in 'add' struct"))?;

        let stats_array = stats_array_ref
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::generic("'stats' field is not a StringArray"))?;

        // Parse each JSON stats string and build stats_parsed array
        let stats_parsed_array = self.build_stats_parsed_array(stats_array, add_column.len())?;

        // Replace the stats_parsed field in the add struct
        let new_add_column = self.replace_stats_parsed_field(add_column, stats_parsed_array)?;

        // Build new RecordBatch with the updated add column
        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(record_batch.num_columns());
        for (idx, column) in record_batch.columns().iter().enumerate() {
            if idx == add_column_idx {
                new_columns.push(Arc::clone(&new_add_column));
            } else {
                new_columns.push(Arc::clone(column));
            }
        }

        RecordBatch::try_new(record_batch.schema(), new_columns)
            .map_err(|e| Error::generic(format!("Failed to create new RecordBatch: {}", e)))
    }

    /// Build a stats_parsed StructArray by parsing JSON stats strings.
    fn build_stats_parsed_array(
        &self,
        stats_array: &StringArray,
        num_rows: usize,
    ) -> DeltaResult<ArrayRef> {
        use crate::actions::stats_conversion::parse_json_stats_to_parsed;
        use crate::arrow::array::{BooleanArray, Int64Array};
        use crate::arrow::datatypes::{Field as ArrowField, Fields};

        use crate::stats_schema::build_min_max_stats_schema;

        // Generate the stats schema for min/max values
        let min_max_schema = build_min_max_stats_schema(&self.table_schema);

        // Parse all JSON stats first
        let mut parsed_stats: Vec<Option<crate::statistics::StatsParsed>> =
            Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            if stats_array.is_null(i) {
                parsed_stats.push(None);
            } else {
                let json_str = stats_array.value(i);
                match parse_json_stats_to_parsed(json_str, &self.table_schema) {
                    Ok(stats) => parsed_stats.push(Some(stats)),
                    Err(_) => parsed_stats.push(None), // If parsing fails, use None
                }
            }
        }

        // Build numRecords array (i64)
        let num_records_array: ArrayRef = Arc::new(Int64Array::from_iter(
            parsed_stats
                .iter()
                .map(|opt| opt.as_ref().map(|s| s.num_records)),
        ));

        // Build minValues struct
        let min_values_array =
            self.build_stats_values_struct(&parsed_stats, &min_max_schema, true)?;

        // Build maxValues struct
        let max_values_array =
            self.build_stats_values_struct(&parsed_stats, &min_max_schema, false)?;

        // Build nullCount struct
        let null_count_array = self.build_null_count_struct(&parsed_stats, &self.table_schema)?;

        // Build tightBounds array (boolean)
        let tight_bounds_array: ArrayRef = Arc::new(BooleanArray::from_iter(
            parsed_stats
                .iter()
                .map(|opt| opt.as_ref().and_then(|s| s.tight_bounds)),
        ));

        // Build the stats_parsed struct
        let fields = Fields::from(vec![
            ArrowField::new("numRecords", ArrowDataType::Int64, true),
            ArrowField::new("minValues", min_values_array.data_type().clone(), true),
            ArrowField::new("maxValues", max_values_array.data_type().clone(), true),
            ArrowField::new("nullCount", null_count_array.data_type().clone(), true),
            ArrowField::new("tightBounds", ArrowDataType::Boolean, true),
        ]);

        let arrays = vec![
            num_records_array,
            min_values_array,
            max_values_array,
            null_count_array,
            tight_bounds_array,
        ];

        Ok(Arc::new(
            StructArray::try_new(fields, arrays, None).map_err(|e| {
                Error::generic(format!("Failed to build stats_parsed array: {}", e))
            })?,
        ))
    }

    /// Build the minValues or maxValues struct from parsed stats
    fn build_stats_values_struct(
        &self,
        parsed_stats: &[Option<crate::statistics::StatsParsed>],
        schema: &StructType,
        is_min: bool,
    ) -> DeltaResult<ArrayRef> {
        use crate::arrow::datatypes::{Field as ArrowField, Fields};

        // Build arrays for each field in the schema
        let mut field_arrays: Vec<(ArrowField, ArrayRef)> = Vec::new();

        for field in schema.fields() {
            let field_name = field.name();
            let array =
                self.build_scalar_array(parsed_stats, field_name, field.data_type(), is_min)?;
            let arrow_field = ArrowField::new(
                field_name,
                array.data_type().clone(),
                true, // Always nullable in stats
            );
            field_arrays.push((arrow_field, array));
        }

        let (fields, arrays): (Vec<_>, Vec<_>) = field_arrays.into_iter().unzip();
        Ok(Arc::new(
            StructArray::try_new(Fields::from(fields), arrays, None).map_err(|e| {
                Error::generic(format!("Failed to build stats values struct: {}", e))
            })?,
        ))
    }

    /// Build the nullCount struct from parsed stats
    fn build_null_count_struct(
        &self,
        parsed_stats: &[Option<crate::statistics::StatsParsed>],
        schema: &StructType,
    ) -> DeltaResult<ArrayRef> {
        use crate::arrow::array::Int64Array;
        use crate::arrow::datatypes::{Field as ArrowField, Fields};

        // Build arrays for each field - null counts are always i64
        let mut field_arrays: Vec<(ArrowField, ArrayRef)> = Vec::new();

        for field in schema.fields() {
            let field_name = field.name();
            let null_counts: Vec<Option<i64>> = parsed_stats
                .iter()
                .map(|opt| {
                    opt.as_ref()
                        .and_then(|s| s.null_count.get(field_name).copied())
                })
                .collect();

            let array: ArrayRef = Arc::new(Int64Array::from_iter(null_counts));
            let arrow_field = ArrowField::new(field_name, ArrowDataType::Int64, true);
            field_arrays.push((arrow_field, array));
        }

        let (fields, arrays): (Vec<_>, Vec<_>) = field_arrays.into_iter().unzip();
        Ok(Arc::new(
            StructArray::try_new(Fields::from(fields), arrays, None)
                .map_err(|e| Error::generic(format!("Failed to build null count struct: {}", e)))?,
        ))
    }

    /// Build an array for a specific scalar field
    fn build_scalar_array(
        &self,
        parsed_stats: &[Option<crate::statistics::StatsParsed>],
        field_name: &str,
        data_type: &DataType,
        is_min: bool,
    ) -> DeltaResult<ArrayRef> {
        use crate::arrow::array::*;
        use crate::expressions::Scalar;

        // Extract the values for this field
        let values: Vec<Option<&Scalar>> = parsed_stats
            .iter()
            .map(|opt| {
                opt.as_ref().and_then(|s| {
                    let map = if is_min { &s.min_values } else { &s.max_values };
                    map.get(field_name).and_then(|o| o.as_ref())
                })
            })
            .collect();

        // Convert to appropriate Arrow array based on data type
        use crate::schema::PrimitiveType;
        match data_type {
            DataType::Primitive(PrimitiveType::Byte) => {
                let array = Int8Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Byte(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Short) => {
                let array = Int16Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Short(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Integer) => {
                let array = Int32Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Integer(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Long) => {
                let array = Int64Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Long(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Float) => {
                let array = Float32Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Float(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Double) => {
                let array = Float64Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Double(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::String) => {
                let mut builder = StringBuilder::new();
                for v in &values {
                    match v {
                        Some(Scalar::String(val)) => builder.append_value(val.as_str()),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Primitive(PrimitiveType::Boolean) => {
                let array = BooleanArray::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Boolean(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Date) => {
                // Date is stored as i32 (days since epoch)
                let array = Int32Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| {
                        if let Scalar::Date(val) = scalar {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                }));
                Ok(Arc::new(array))
            }
            DataType::Primitive(PrimitiveType::Timestamp)
            | DataType::Primitive(PrimitiveType::TimestampNtz) => {
                // Timestamp is stored as i64 (microseconds)
                let array = Int64Array::from_iter(values.iter().map(|v| {
                    v.and_then(|scalar| match scalar {
                        Scalar::Timestamp(val) | Scalar::TimestampNtz(val) => Some(*val),
                        _ => None,
                    })
                }));
                Ok(Arc::new(array))
            }
            _ => {
                // For unsupported types, return a null array
                Ok(Arc::new(Int64Array::new_null(values.len())))
            }
        }
    }

    /// Replace the stats_parsed field in an Add struct with new data.
    fn replace_stats_parsed_field(
        &self,
        add_struct: &StructArray,
        new_stats_parsed: ArrayRef,
    ) -> DeltaResult<ArrayRef> {
        use crate::arrow::datatypes::Fields;
        // Find the stats_parsed field name to match against
        let stats_parsed_name = "stats_parsed";

        // Get the field names from the struct
        let fields: &Fields = if let ArrowDataType::Struct(fields) = add_struct.data_type() {
            fields
        } else {
            return Err(Error::generic("add_struct is not a Struct type"));
        };

        // Build new struct with replaced field
        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(add_struct.num_columns());
        for (idx, field) in fields.iter().enumerate() {
            if field.name() == stats_parsed_name {
                new_columns.push(new_stats_parsed.clone());
            } else {
                new_columns.push(Arc::clone(add_struct.column(idx)));
            }
        }

        Ok(Arc::new(
            StructArray::try_new(fields.clone(), new_columns, add_struct.nulls().cloned())
                .map_err(|e| Error::generic(format!("Failed to create StructArray: {}", e)))?,
        ))
    }

    /// Processes an iterator of action batches, transforming each one.
    pub(super) fn process_actions_iter(
        self,
        actions: impl Iterator<Item = DeltaResult<ActionReconciliationBatch>>,
    ) -> impl Iterator<Item = DeltaResult<ActionReconciliationBatch>> {
        actions.map(move |batch| batch.and_then(|b| self.transform_batch(b)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, StructField, StructType};

    #[test]
    fn test_stats_transformation_processor_creation() {
        let schema = Arc::new(StructType::new_unchecked(vec![
            StructField::new("id", DataType::INTEGER, false),
            StructField::new("value", DataType::LONG, true),
        ]));

        // Test with both flags enabled (default)
        let processor = StatsTransformationProcessor::new(schema.clone(), true, true);
        assert!(processor.write_stats_as_struct);
        assert!(processor.write_stats_as_json);

        // Test with struct only
        let processor_struct_only = StatsTransformationProcessor::new(schema.clone(), true, false);
        assert!(processor_struct_only.write_stats_as_struct);
        assert!(!processor_struct_only.write_stats_as_json);

        // Test with JSON only
        let processor_json_only = StatsTransformationProcessor::new(schema.clone(), false, true);
        assert!(!processor_json_only.write_stats_as_struct);
        assert!(processor_json_only.write_stats_as_json);

        // Test with both disabled
        let processor_disabled = StatsTransformationProcessor::new(schema, false, false);
        assert!(!processor_disabled.write_stats_as_struct);
        assert!(!processor_disabled.write_stats_as_json);
    }
}
