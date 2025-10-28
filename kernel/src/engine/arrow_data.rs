use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::types::{Int32Type, Int64Type};
use crate::arrow::array::{
    Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch, StructArray,
};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef, Schema as ArrowSchema,
};
use crate::engine::arrow_conversion::TryIntoArrow as _;
use crate::engine_data::{EngineData, EngineList, EngineMap, GetData, RowVisitor};
use crate::expressions::ArrayData;
use crate::schema::{ColumnName, DataType, SchemaRef};
use crate::{DeltaResult, Error};

pub use crate::engine::arrow_utils::fix_nested_null_masks;

/// ArrowEngineData holds an Arrow `RecordBatch`, implements `EngineData` so the kernel can extract from it.
///
/// WARNING: Row visitors require that all leaf columns of the record batch have correctly computed
/// NULL masks. The arrow parquet reader is known to produce incomplete NULL masks, for
/// example. When in doubt, call [`fix_nested_null_masks`] first.
pub struct ArrowEngineData {
    data: RecordBatch,
}

/// Helper function to extract a RecordBatch from EngineData, ensuring it's ArrowEngineData
pub(crate) fn extract_record_batch(engine_data: &dyn EngineData) -> DeltaResult<&RecordBatch> {
    let Some(arrow_data) = engine_data.any_ref().downcast_ref::<ArrowEngineData>() else {
        return Err(Error::engine_data_type("ArrowEngineData"));
    };
    Ok(arrow_data.record_batch())
}

/// unshredded variant arrow type: struct of two non-nullable binary fields 'metadata' and 'value'
#[allow(dead_code)]
pub(crate) fn unshredded_variant_arrow_type() -> ArrowDataType {
    let metadata_field = ArrowField::new("metadata", ArrowDataType::Binary, false);
    let value_field = ArrowField::new("value", ArrowDataType::Binary, false);
    let fields = vec![metadata_field, value_field];
    ArrowDataType::Struct(fields.into())
}

impl ArrowEngineData {
    /// Create a new `ArrowEngineData` from a `RecordBatch`
    pub fn new(data: RecordBatch) -> Self {
        ArrowEngineData { data }
    }

    /// Utility constructor to get a `Box<ArrowEngineData>` out of a `Box<dyn EngineData>`
    pub fn try_from_engine_data(engine_data: Box<dyn EngineData>) -> DeltaResult<Box<Self>> {
        engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| Error::engine_data_type("ArrowEngineData"))
    }

    /// Get a reference to the `RecordBatch` this `ArrowEngineData` is wrapping
    pub fn record_batch(&self) -> &RecordBatch {
        &self.data
    }
}

impl From<RecordBatch> for ArrowEngineData {
    fn from(value: RecordBatch) -> Self {
        ArrowEngineData::new(value)
    }
}

impl From<StructArray> for ArrowEngineData {
    fn from(value: StructArray) -> Self {
        ArrowEngineData::new(value.into())
    }
}

impl From<ArrowEngineData> for RecordBatch {
    fn from(value: ArrowEngineData) -> Self {
        value.data
    }
}

impl From<Box<ArrowEngineData>> for RecordBatch {
    fn from(value: Box<ArrowEngineData>) -> Self {
        value.data
    }
}

impl<OffsetSize> EngineList for GenericListArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn len(&self, row_index: usize) -> usize {
        self.value(row_index).len()
    }

    fn get(&self, row_index: usize, index: usize) -> String {
        let arry = self.value(row_index);
        let sarry = arry.as_string::<i32>();
        sarry.value(index).to_string()
    }

    fn materialize(&self, row_index: usize) -> Vec<String> {
        let mut result = vec![];
        for i in 0..EngineList::len(self, row_index) {
            result.push(self.get(row_index, i));
        }
        result
    }
}

impl EngineMap for MapArray {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str> {
        let offsets = self.offsets();
        let start_offset = offsets[row_index] as usize;
        let count = offsets[row_index + 1] as usize - start_offset;
        let keys = self.keys().as_string::<i32>();
        for (idx, map_key) in keys.iter().enumerate().skip(start_offset).take(count) {
            if let Some(map_key) = map_key {
                if key == map_key {
                    // found the item
                    let vals = self.values().as_string::<i32>();
                    return Some(vals.value(idx));
                }
            }
        }
        None
    }

    fn materialize(&self, row_index: usize) -> HashMap<String, String> {
        let mut ret = HashMap::new();
        let map_val = self.value(row_index);
        let keys = map_val.column(0).as_string::<i32>();
        let values = map_val.column(1).as_string::<i32>();
        for (key, value) in keys.iter().zip(values.iter()) {
            if let (Some(key), Some(value)) = (key, value) {
                ret.insert(key.into(), value.into());
            }
        }
        ret
    }
}

/// Helper trait that provides uniform access to columns and fields, so that our row visitor can use
/// the same code to drill into a `RecordBatch` (initial case) or `StructArray` (nested case).
trait ProvidesColumnsAndFields {
    fn columns(&self) -> &[ArrayRef];
    fn fields(&self) -> &[FieldRef];
}

impl ProvidesColumnsAndFields for RecordBatch {
    fn columns(&self) -> &[ArrayRef] {
        self.columns()
    }
    fn fields(&self) -> &[FieldRef] {
        self.schema_ref().fields()
    }
}

impl ProvidesColumnsAndFields for StructArray {
    fn columns(&self) -> &[ArrayRef] {
        self.columns()
    }
    fn fields(&self) -> &[FieldRef] {
        self.fields()
    }
}

impl EngineData for ArrowEngineData {
    fn len(&self) -> usize {
        self.data.num_rows()
    }

    fn visit_rows(
        &self,
        leaf_columns: &[ColumnName],
        visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()> {
        // Make sure the caller passed the correct number of column names
        let leaf_types = visitor.selected_column_names_and_types().1;
        if leaf_types.len() != leaf_columns.len() {
            return Err(Error::MissingColumn(format!(
                "Visitor expected {} column names, but caller passed {}",
                leaf_types.len(),
                leaf_columns.len()
            ))
            .with_backtrace());
        }

        // Collect the names of all leaf columns we want to extract, along with their parents, to
        // guide our depth-first extraction. If the list contains any non-leaf, duplicate, or
        // missing column references, the extracted column list will be too short (error out below).
        let mut mask = HashSet::new();
        for column in leaf_columns {
            for i in 0..column.len() {
                mask.insert(&column[..i + 1]);
            }
        }
        debug!("Column mask for selected columns {leaf_columns:?} is {mask:#?}");

        let mut getters = vec![];
        Self::extract_columns(&mut vec![], &mut getters, leaf_types, &mask, &self.data)?;
        if getters.len() != leaf_columns.len() {
            return Err(Error::MissingColumn(format!(
                "Visitor expected {} leaf columns, but only {} were found in the data",
                leaf_columns.len(),
                getters.len()
            )));
        }
        visitor.visit(self.len(), &getters)
    }

    fn append_columns(
        &self,
        schema: SchemaRef,
        columns: Vec<ArrayData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // Combine existing and new schema fields
        let schema: ArrowSchema = schema.as_ref().try_into_arrow()?;
        let mut combined_fields = self.data.schema().fields().to_vec();
        combined_fields.extend_from_slice(schema.fields());
        let combined_schema = Arc::new(ArrowSchema::new(combined_fields));

        // Combine existing and new columns
        let new_columns: Vec<ArrayRef> = columns
            .into_iter()
            .map(|array_data| array_data.to_arrow())
            .try_collect()?;
        let mut combined_columns = self.data.columns().to_vec();
        combined_columns.extend(new_columns);

        // Create a new ArrowEngineData with the combined schema and columns
        let data = RecordBatch::try_new(combined_schema, combined_columns)?;
        Ok(Box::new(ArrowEngineData { data }))
    }
}

impl ArrowEngineData {
    fn extract_columns<'a>(
        path: &mut Vec<String>,
        getters: &mut Vec<&'a dyn GetData<'a>>,
        leaf_types: &[DataType],
        column_mask: &HashSet<&[String]>,
        data: &'a dyn ProvidesColumnsAndFields,
    ) -> DeltaResult<()> {
        for (column, field) in data.columns().iter().zip(data.fields()) {
            path.push(field.name().to_string());
            if column_mask.contains(&path[..]) {
                if let Some(struct_array) = column.as_struct_opt() {
                    debug!(
                        "Recurse into a struct array for {}",
                        ColumnName::new(path.iter())
                    );
                    Self::extract_columns(path, getters, leaf_types, column_mask, struct_array)?;
                } else if column.data_type() == &ArrowDataType::Null {
                    debug!("Pushing a null array for {}", ColumnName::new(path.iter()));
                    getters.push(&());
                } else {
                    let data_type = &leaf_types[getters.len()];
                    let getter = Self::extract_leaf_column(path, data_type, column)?;
                    getters.push(getter);
                }
            } else {
                debug!("Skipping unmasked path {}", ColumnName::new(path.iter()));
            }
            path.pop();
        }
        Ok(())
    }

    fn extract_leaf_column<'a>(
        path: &[String],
        data_type: &DataType,
        col: &'a dyn Array,
    ) -> DeltaResult<&'a dyn GetData<'a>> {
        use ArrowDataType::Utf8;
        let col_as_list = || {
            if let Some(array) = col.as_list_opt::<i32>() {
                (array.value_type() == Utf8).then_some(array as _)
            } else if let Some(array) = col.as_list_opt::<i64>() {
                (array.value_type() == Utf8).then_some(array as _)
            } else {
                None
            }
        };
        let col_as_map = || {
            col.as_map_opt().and_then(|array| {
                (array.key_type() == &Utf8 && array.value_type() == &Utf8).then_some(array as _)
            })
        };
        let result: Result<&'a dyn GetData<'a>, _> = match data_type {
            &DataType::BOOLEAN => {
                debug!("Pushing boolean array for {}", ColumnName::new(path));
                col.as_boolean_opt().map(|a| a as _).ok_or("bool")
            }
            &DataType::STRING => {
                debug!("Pushing string array for {}", ColumnName::new(path));
                col.as_string_opt().map(|a| a as _).ok_or("string")
            }
            &DataType::INTEGER => {
                debug!("Pushing int32 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int32Type>()
                    .map(|a| a as _)
                    .ok_or("int")
            }
            &DataType::LONG => {
                debug!("Pushing int64 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int64Type>()
                    .map(|a| a as _)
                    .ok_or("long")
            }
            DataType::Array(_) => {
                debug!("Pushing list for {}", ColumnName::new(path));
                col_as_list().ok_or("array<string>")
            }
            DataType::Map(_) => {
                debug!("Pushing map for {}", ColumnName::new(path));
                col_as_map().ok_or("map<string, string>")
            }
            data_type => {
                return Err(Error::UnexpectedColumnType(format!(
                    "On {}: Unsupported type {data_type}",
                    ColumnName::new(path)
                )));
            }
        };
        result.map_err(|type_name| {
            Error::UnexpectedColumnType(format!(
                "Type mismatch on {}: expected {}, got {}",
                ColumnName::new(path),
                type_name,
                col.data_type()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::actions::{get_commit_schema, Metadata, Protocol};
    use crate::arrow::array::types::Int32Type;
    use crate::arrow::array::{Array, AsArray, Int32Array, RecordBatch, StringArray};
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::engine::sync::SyncEngine;
    use crate::expressions::ArrayData;
    use crate::schema::{ArrayType, DataType, StructField, StructType};
    use crate::table_features::TableFeature;
    use crate::utils::test_utils::{assert_result_error_with_message, string_array_to_engine_data};
    use crate::{DeltaResult, Engine as _, EngineData as _};

    use super::{extract_record_batch, ArrowEngineData};

    #[test]
    fn test_md_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.json_handler();
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
        ]
        .into();
        let output_schema = get_commit_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let metadata = Metadata::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(metadata.id(), "aff5cb91-8cd9-4195-aef9-446908507302");
        assert_eq!(metadata.created_time(), Some(1670892997849));
        assert_eq!(*metadata.partition_columns(), vec!("c1", "c2"));
        Ok(())
    }

    #[test]
    fn test_protocol_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.json_handler();
        let json_strings: StringArray = vec![
            r#"{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["rw1"], "writerFeatures": ["rw1", "w2"]}}"#,
        ]
        .into();
        let output_schema = get_commit_schema().project(&["protocol"])?;
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let protocol = Protocol::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(
            protocol.reader_features(),
            Some([TableFeature::unknown("rw1")].as_slice())
        );
        assert_eq!(
            protocol.writer_features(),
            Some([TableFeature::unknown("rw1"), TableFeature::unknown("w2")].as_slice())
        );
        Ok(())
    }

    #[test]
    fn test_append_columns() -> DeltaResult<()> {
        // Create initial ArrowEngineData with 2 rows and 2 columns
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            ],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Create new columns as ArrayData
        let new_columns = vec![
            ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, true),
                vec![Some(25), None],
            )?,
            ArrayData::try_new(ArrayType::new(DataType::BOOLEAN, false), vec![true, false])?,
        ];

        // Create schema for the new columns
        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("age", DataType::INTEGER, true),
            StructField::new("active", DataType::BOOLEAN, false),
        ]));

        // Test the append_columns method
        let arrow_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(arrow_data.as_ref())?;

        // Verify the result
        assert_eq!(result_batch.num_columns(), 4);
        assert_eq!(result_batch.num_rows(), 2);

        let schema = result_batch.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "age");
        assert_eq!(schema.field(3).name(), "active");

        assert_eq!(schema.field(0).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(3).data_type(), &ArrowDataType::Boolean);

        let id_column = result_batch.column(0).as_primitive::<Int32Type>();
        let name_column = result_batch.column(1).as_string::<i32>();
        let age_column = result_batch.column(2).as_primitive::<Int32Type>();
        let active_column = result_batch.column(3).as_boolean();

        assert_eq!(id_column.values(), &[1, 2]);
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert_eq!(age_column.value(0), 25);
        assert!(age_column.is_null(1));
        assert!(active_column.value(0));
        assert!(!active_column.value(1));

        Ok(())
    }

    #[test]
    fn test_append_columns_row_mismatch() -> DeltaResult<()> {
        // Create initial ArrowEngineData with 2 rows
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = super::ArrowEngineData::new(initial_batch);

        // Create new column with wrong number of rows (3 instead of 2)
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::INTEGER, false),
            vec![25, 30, 35],
        )?];

        let new_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "age",
            DataType::INTEGER,
            true,
        )]));

        let result = arrow_data.append_columns(new_schema, new_columns);
        assert_result_error_with_message(
            result,
            "all columns in a record batch must have the same length",
        );

        Ok(())
    }

    #[test]
    fn test_append_columns_schema_field_count_mismatch() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Schema has 2 fields but only 1 column provided
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            vec![Some("Alice".to_string()), Some("Bob".to_string())],
        )?];

        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("name", DataType::STRING, true),
            StructField::new("email", DataType::STRING, true), // Extra field in schema
        ]));

        let result = arrow_data.append_columns(new_schema, new_columns);
        assert_result_error_with_message(
            result,
            "number of columns(2) must match number of fields(3)",
        );

        Ok(())
    }

    #[test]
    fn test_append_columns_empty_existing_data() -> DeltaResult<()> {
        // Create empty ArrowEngineData with schema but no rows
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Create empty new columns
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::STRING, true),
            Vec::<Option<String>>::new(),
        )?];
        let new_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "name",
            DataType::STRING,
            true,
        )]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 2);
        assert_eq!(result_batch.num_rows(), 0);
        assert_eq!(result_batch.schema().field(0).name(), "id");
        assert_eq!(result_batch.schema().field(1).name(), "name");

        Ok(())
    }

    #[test]
    fn test_append_columns_empty_new_columns() -> DeltaResult<()> {
        // Create ArrowEngineData with some data
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Create empty schema and columns
        let new_columns = vec![];
        let new_schema = Arc::new(StructType::new_unchecked([]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        // Should be identical to original
        assert_eq!(result_batch.num_columns(), 1);
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.schema().field(0).name(), "id");

        Ok(())
    }

    #[test]
    fn test_append_columns_with_nulls() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        let new_columns = vec![
            ArrayData::try_new(
                ArrayType::new(DataType::STRING, true),
                vec![Some("Alice".to_string()), None, Some("Charlie".to_string())],
            )?,
            ArrayData::try_new(
                ArrayType::new(DataType::INTEGER, true),
                vec![Some(25), Some(30), None],
            )?,
        ];

        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("name", DataType::STRING, true),
            StructField::new("age", DataType::INTEGER, true),
        ]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 3);
        assert_eq!(result_batch.num_rows(), 3);

        // Verify nullable columns work correctly
        assert!(!result_batch.schema().field(0).is_nullable());
        assert!(result_batch.schema().field(1).is_nullable());
        assert!(result_batch.schema().field(2).is_nullable());

        Ok(())
    }

    #[test]
    fn test_append_columns_various_data_types() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let initial_batch =
            RecordBatch::try_new(initial_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        let new_columns = vec![
            ArrayData::try_new(
                ArrayType::new(DataType::LONG, false),
                vec![1000_i64, 2000_i64],
            )?,
            ArrayData::try_new(
                ArrayType::new(DataType::DOUBLE, true),
                vec![Some(3.87), Some(2.71)],
            )?,
            ArrayData::try_new(ArrayType::new(DataType::BOOLEAN, false), vec![true, false])?,
        ];

        let new_schema = Arc::new(StructType::new_unchecked([
            StructField::new("big_number", DataType::LONG, false),
            StructField::new("pi", DataType::DOUBLE, true),
            StructField::new("flag", DataType::BOOLEAN, false),
        ]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 4);
        assert_eq!(result_batch.num_rows(), 2);

        // Check data types
        let schema = result_batch.schema();
        assert_eq!(schema.field(0).data_type(), &ArrowDataType::Int32);
        assert_eq!(schema.field(1).data_type(), &ArrowDataType::Int64);
        assert_eq!(schema.field(2).data_type(), &ArrowDataType::Float64);
        assert_eq!(schema.field(3).data_type(), &ArrowDataType::Boolean);

        Ok(())
    }

    #[test]
    fn test_append_single_column() -> DeltaResult<()> {
        let initial_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let initial_batch = RecordBatch::try_new(
            initial_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("Alice"),
                    Some("Bob"),
                    Some("Charlie"),
                ])),
            ],
        )?;
        let arrow_data = ArrowEngineData::new(initial_batch);

        // Append just one column
        let new_columns = vec![ArrayData::try_new(
            ArrayType::new(DataType::BOOLEAN, false),
            vec![true, false, true],
        )?];

        let new_schema = Arc::new(StructType::new_unchecked([StructField::new(
            "active",
            DataType::BOOLEAN,
            false,
        )]));

        let result_data = arrow_data.append_columns(new_schema, new_columns)?;
        let result_batch = extract_record_batch(result_data.as_ref())?;

        assert_eq!(result_batch.num_columns(), 3);
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.schema().field(2).name(), "active");

        Ok(())
    }
}
