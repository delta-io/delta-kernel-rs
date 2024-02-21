use crate::engine_data::{DataItemList, DataItemMap, EngineData, GetData, TypeTag};
use crate::schema::{DataType, PrimitiveType, Schema, SchemaRef, StructField};
use crate::{DeltaResult, Error};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, GenericListArray, MapArray, RecordBatch, StructArray};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};
use url::Url;

use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

#[derive(Debug)]
pub struct SimpleDataTypeTag;
impl TypeTag for SimpleDataTypeTag {}

/// SimpleData holds a RecordBatch
pub struct SimpleData {
    data: RecordBatch,
}

impl SimpleData {
    /// Create a new SimpleData from a RecordBatch
    pub fn new(data: RecordBatch) -> Self {
        SimpleData { data }
    }

    /// Utility constructor to get a `Box<SimpleData>` out of a `Box<dyn EngineData>`
    pub fn try_from_engine_data(engine_data: Box<dyn EngineData>) -> DeltaResult<Box<Self>> {
        engine_data
            .into_any()
            .downcast::<SimpleData>()
            .map_err(|_| Error::EngineDataType("SimpleData".into()))
    }

    pub fn into_record_batch(self) -> RecordBatch {
        self.data
    }

    pub fn record_batch(&self) -> &RecordBatch {
        &self.data
    }
}

impl EngineData for SimpleData {
    fn type_tag(&self) -> &dyn TypeTag {
        &SimpleDataTypeTag
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

trait ProvidesColumnByName {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>>;
}

impl ProvidesColumnByName for RecordBatch {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

impl ProvidesColumnByName for StructArray {
    fn column_by_name(&self, name: &str) -> Option<&Arc<dyn Array>> {
        self.column_by_name(name)
    }
}

impl DataItemList for GenericListArray<i32> {
    fn len(&self, row_index: usize) -> usize {
        self.value(row_index).len()
    }

    fn get(&self, row_index: usize, index: usize) -> String {
        let arry = self.value(row_index);
        let sarry = arry.as_string::<i32>();
        sarry.value(index).to_string()
    }
}

impl DataItemMap for MapArray {
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

    fn materialize(&self, row_index: usize) -> HashMap<String, Option<String>> {
        let mut ret = HashMap::new();
        let map_val = self.value(row_index);
        let keys = map_val.column(0).as_string::<i32>();
        let values = map_val.column(1).as_string::<i32>();
        for (key, value) in keys.iter().zip(values.iter()) {
            if let Some(key) = key {
                ret.insert(key.into(), value.map(|v| v.into()));
            }
        }
        ret
    }
}

impl SimpleData {
    pub fn try_create_from_json(schema: SchemaRef, location: Url) -> DeltaResult<Self> {
        let arrow_schema: ArrowSchema = (&*schema).try_into()?;
        debug!("Reading {:#?} with schema: {:#?}", location, arrow_schema);
        // todo: Check scheme of url
        let file = File::open(
            location
                .to_file_path()
                .map_err(|_| Error::Generic("can only read local files".to_string()))?,
        )?;
        let mut json =
            arrow_json::ReaderBuilder::new(Arc::new(arrow_schema)).build(BufReader::new(file))?;
        let data = json.next().ok_or(Error::Generic(
            "No data found reading json file".to_string(),
        ))?;
        Ok(SimpleData::new(data?))
    }

    // todo: fix all the unwrapping
    pub fn try_create_from_parquet(_schema: SchemaRef, location: Url) -> DeltaResult<Self> {
        let file = File::open(
            location
                .to_file_path()
                .map_err(|_| Error::Generic("can only read local files".to_string()))?,
        )?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let data = reader.next().ok_or(Error::Generic(
            "No data found reading parquet file".to_string(),
        ))?;
        Ok(SimpleData::new(data?))
    }

    pub fn extract_columns<'a>(
        &'a self,
        out_col_array: &mut Vec<&dyn GetData<'a>>,
        schema: &Schema,
    ) -> DeltaResult<()> {
        debug!("Extracting column getters for {:#?}", schema);
        SimpleData::extract_columns_from_array(out_col_array, schema, Some(&self.data))
    }

    /// Extracts an exploded schema (all leaf values), in schema order
    fn extract_columns_from_array<'a>(
        out_col_array: &mut Vec<&dyn GetData<'a>>,
        schema: &Schema,
        array: Option<&'a dyn ProvidesColumnByName>,
    ) -> DeltaResult<()> {
        for field in schema.fields.iter() {
            let col = array
                .and_then(|a| a.column_by_name(&field.name))
                .filter(|a| *a.data_type() != ArrowDataType::Null);
            // Note: if col is None we have either:
            //   a) encountered a column that is all nulls or,
            //   b) recursed into a struct that was all null.
            // So below if the field is allowed to be null, we push that, otherwise we error out.
            if let Some(col) = col {
                Self::extract_column(out_col_array, field, col)?;
            } else if field.is_nullable() {
                if let DataType::Struct(_) = field.data_type() {
                    Self::extract_columns_from_array(out_col_array, schema, None)?;
                } else {
                    debug!("Pushing a null field for {}", field.name);
                    out_col_array.push(&());
                }
            } else {
                return Err(Error::Generic(format!(
                    "Found required field {}, but it's null",
                    field.name
                )));
            }
        }
        Ok(())
    }

    fn extract_column<'a>(
        out_col_array: &mut Vec<&dyn GetData<'a>>,
        field: &StructField,
        col: &'a dyn Array,
    ) -> DeltaResult<()> {
        match (col.data_type(), &field.data_type) {
            (&ArrowDataType::Struct(_), DataType::Struct(fields)) => {
                // both structs, so recurse into col
                let struct_array = col.as_struct();
                SimpleData::extract_columns_from_array(out_col_array, fields, Some(struct_array))?;
            }
            (&ArrowDataType::Boolean, &DataType::Primitive(PrimitiveType::Boolean)) => {
                debug!("Pushing boolean array for {}", field.name);
                out_col_array.push(col.as_boolean());
            }
            (&ArrowDataType::Utf8, &DataType::Primitive(PrimitiveType::String)) => {
                debug!("Pushing string array for {}", field.name);
                out_col_array.push(col.as_string::<i32>());
            }
            (&ArrowDataType::Int32, &DataType::Primitive(PrimitiveType::Integer)) => {
                debug!("Pushing int32 array for {}", field.name);
                out_col_array.push(col.as_primitive::<Int32Type>());
            }
            (&ArrowDataType::Int64, &DataType::Primitive(PrimitiveType::Long)) => {
                debug!("Pushing int64 array for {}", field.name);
                out_col_array.push(col.as_primitive::<Int64Type>());
            }
            (ArrowDataType::List(_arrow_field), DataType::Array(_array_type)) => {
                // TODO(nick): validate the element types match
                debug!("Pushing list for {}", field.name);
                out_col_array.push(col.as_list());
            }
            (&ArrowDataType::Map(_, _), &DataType::Map(_)) => {
                debug!("Pushing map for {}", field.name);
                out_col_array.push(col.as_map());
            }
            (arrow_data_type, data_type) => {
                warn!(
                    "Can't extract {}. Arrow Type: {arrow_data_type}\n Kernel Type: {data_type}",
                    field.name
                );
                return Err(get_error_for_types(data_type, arrow_data_type, &field.name));
            }
        }
        Ok(())
    }

    pub fn length(&self) -> usize {
        self.data.num_rows()
    }
}

fn get_error_for_types(
    data_type: &DataType,
    arrow_data_type: &ArrowDataType,
    field_name: &str,
) -> Error {
    let expected_type: Result<ArrowDataType, ArrowError> = data_type.try_into();
    match expected_type {
        Ok(expected_type) => {
            if expected_type == *arrow_data_type {
                Error::Generic(format!(
                    "On {field_name}: Don't know how to extract something of type {data_type}",
                ))
            } else {
                Error::Generic(format!(
                    "Type mismatch on {field_name}: expected {data_type}, got {arrow_data_type}",
                ))
            }
        }
        Err(e) => Error::Generic(format!(
            "On {field_name}: Unsupported data type {data_type}: {e}",
        )),
    }
}

impl From<RecordBatch> for SimpleData {
    fn from(value: RecordBatch) -> Self {
        SimpleData::new(value)
    }
}

// test disabled because creating a record batch is tricky :)

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::actions::action_definitions::Metadata;
    use crate::{
        actions::schemas::log_schema,
        simple_client::{data::SimpleData, SimpleClient},
        EngineClient, EngineData,
    };

    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(SimpleData::new(batch))
    }

    #[test]
    fn test_md_extract() {
        let client = SimpleClient::new();
        let handler = client.get_json_handler();
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
        ]
        .into();
        let output_schema = Arc::new(log_schema().clone());
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let metadata = Metadata::try_new_from_data(&client, parsed.as_ref());
        assert!(metadata.is_ok());
        let metadata = metadata.unwrap();
        assert_eq!(metadata.id, "aff5cb91-8cd9-4195-aef9-446908507302");
        assert_eq!(metadata.created_time, Some(1670892997849));
        assert_eq!(metadata.partition_columns, vec!("c1", "c2"))
    }
}
