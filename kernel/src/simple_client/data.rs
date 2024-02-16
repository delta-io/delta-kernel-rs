use crate::engine_data::{
    DataItem, DataItemList, DataItemMap, DataVisitor, EngineData, GetDataItem, ListItem, MapItem,
    TypeTag,
};
use crate::schema::{DataType, PrimitiveType, Schema, SchemaRef};
use crate::{DeltaResult, Error};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, GenericListArray, MapArray, NullArray, RecordBatch, StructArray};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, error, warn};
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
        schema: &Schema,
        col_array: &mut Vec<&dyn GetDataItem<'a>>,
    ) -> DeltaResult<()> {
        SimpleData::extract_columns_from_array(Some(&self.data), schema, col_array)?;
        Ok(())
    }

    /// Extracts an exploded schema (all leaf values), in schema order
    fn extract_columns_from_array<'a>(
        array: Option<&'a dyn ProvidesColumnByName>,
        schema: &Schema,
        col_array: &mut Vec<&dyn GetDataItem<'a>>,
    ) -> DeltaResult<()> {
        for field in schema.fields.iter() {
            //println!("Looking at {:#?}", field);
            if array.is_none() {
                // we have recursed into a struct that was all null. if the field is allowed to be
                // null, push that, otherwise error out.
                if field.is_nullable() {
                    match &field.data_type() {
                        &DataType::Struct(ref fields) => {
                            // keep recursing
                            SimpleData::extract_columns_from_array(None, fields, col_array)?;
                        }
                        _ => col_array.push(&()),
                    }
                    continue;
                } else {
                    return Err(Error::Generic(format!(
                        "Found required field {}, but it's null",
                        field.name
                    )));
                }
            }
            // unwrap here is safe as we checked above
            // TODO(nick): refactor to `match` to make idiomatic
            let col = array.unwrap().column_by_name(&field.name);
            let data_type = col.map_or(&ArrowDataType::Null, |c| c.data_type());
            match (col, data_type, &field.data_type) {
                (_, &ArrowDataType::Null, &DataType::Struct(ref fields)) => {
                    // We always explode structs even if null/missing, so recurse on
                    // on each field.
                    SimpleData::extract_columns_from_array(None, fields, col_array)?;
                }
                // TODO: Is this actually the right place to enforce nullability? We
                // will anyway have to null-check the value for each row? Tho I guess we
                // could early-out if we find an all-null or missing column forwhen a
                // non-nullable field was requested, and could also simplify the checks
                // in case the underlying column is non-nullable.
                (_, &ArrowDataType::Null, _) if field.is_nullable() => col_array.push(&()),
                (_, &ArrowDataType::Null, _) => {
                    return Err(Error::Generic(
                        "Got a null column for something required in passed schema".to_string(),
                    ))
                }
                (Some(col), &ArrowDataType::Struct(_), &DataType::Struct(ref fields)) => {
                    // both structs, so recurse into col
                    let struct_array = col.as_struct();
                    SimpleData::extract_columns_from_array(Some(struct_array), fields, col_array)?;
                }
                (
                    Some(col),
                    &ArrowDataType::Boolean,
                    &DataType::Primitive(PrimitiveType::Boolean),
                ) => {
                    col_array.push(col.as_boolean());
                }
                (Some(col), &ArrowDataType::Utf8, &DataType::Primitive(PrimitiveType::String)) => {
                    col_array.push(col.as_string::<i32>());
                }
                (
                    Some(col),
                    &ArrowDataType::Int32,
                    &DataType::Primitive(PrimitiveType::Integer),
                ) => {
                    col_array.push(col.as_primitive::<Int32Type>());
                }
                (Some(col), &ArrowDataType::Int64, &DataType::Primitive(PrimitiveType::Long)) => {
                    col_array.push(col.as_primitive::<Int64Type>());
                }
                (
                    Some(col),
                    &ArrowDataType::List(ref _arrow_field),
                    &DataType::Array(ref _array_type),
                ) => {
                    // TODO(nick): validate the element types match
                    col_array.push(col.as_list());
                }
                (Some(col), &ArrowDataType::Map(_, _), &DataType::Map(_)) => {
                    col_array.push(col.as_map());
                }
                (Some(_), arrow_data_type, data_type) => {
                    warn!("Can't extract {}. Arrow Type: {arrow_data_type}\n Kernel Type: {data_type}", field.name);
                    let expected_type: Result<ArrowDataType, ArrowError> = data_type.try_into();
                    return Err(match expected_type {
                        Ok(expected_type) => {
                            if expected_type == *arrow_data_type {
                                Error::Generic(format!("On {}: Don't know how to extract something of type {data_type}", field.name))
                            } else {
                                Error::Generic(format!(
                                    "Type mismatch on {}: expected {data_type}, got {arrow_data_type}",
                                    field.name
                                ))
                            }
                        }
                        Err(e) => Error::Generic(format!(
                            "On {}: Unsupported data type {data_type}: {e}",
                            field.name
                        )),
                    });
                }
                (_, arrow_data_type, _) => {
                    return Err(Error::Generic(format!(
                        "Need a column to extract field {} of type {arrow_data_type}, but got none",
                        field.name
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn length(&self) -> usize {
        self.data.num_rows()
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
