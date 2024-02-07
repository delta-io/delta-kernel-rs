use crate::engine_data::{DataItem, DataVisitor, EngineData, ListItem, MapItem, TypeTag};
use crate::schema::{Schema, SchemaRef};
use crate::{DeltaResult, Error};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, GenericListArray, MapArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, error};
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

    /// Utility constructor to get a Box<SimpleData> out of a Box<dyn EngineData>
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

impl ListItem for GenericListArray<i32> {
    fn len(&self, row_index: usize) -> usize {
        self.value(row_index).len()
    }

    fn get(&self, row_index: usize, index: usize) -> String {
        let arry = self.value(row_index);
        let sarry = arry.as_string::<i32>();
        sarry.value(index).to_string()
    }
}

// TODO: This is likely wrong and needs to only scan the correct row
impl MapItem for MapArray {
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
        let cols = map_val.columns();
        let keys = cols[0].as_string::<i32>();
        let values = cols[1].as_string::<i32>();
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
        let file = File::open(location.to_file_path().unwrap()).unwrap(); // todo: fix to_file_path.unwrap()
        let mut json = arrow_json::ReaderBuilder::new(Arc::new(arrow_schema))
            .build(BufReader::new(file))
            .unwrap();
        let data = json.next().unwrap().unwrap();
        Ok(SimpleData { data })
    }

    // todo: fix all the unwrapping
    pub fn try_create_from_parquet(_schema: SchemaRef, location: Url) -> DeltaResult<Self> {
        let file = File::open(location.to_file_path().unwrap()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let data = reader.next().unwrap().unwrap();
        Ok(SimpleData { data })
    }

    /// extract a row of data. will recurse into struct types
    fn extract_row<'a>(
        array: &'a dyn ProvidesColumnByName,
        schema: &Schema,
        row: usize,
        had_data: &mut bool,
        res_arry: &mut Vec<Option<DataItem<'a>>>,
    ) {
        // check each requested column in the row
        for field in schema.fields.iter() {
            match array.column_by_name(&field.name) {
                None => {
                    // check if this is nullable or not
                    if field.nullable {
                        debug!("Pushing None since column not present for {}", field.name);
                        // TODO(nick): This is probably wrong if there is a nullable struct type. we
                        // just need a helper that can recurse the kernel schema type and push Nones
                        res_arry.push(None);
                    } else {
                        panic!("Didn't find non-nullable column: {}", field.name);
                    }
                }
                Some(col) => {
                    // check first if a struct and just recurse no matter what
                    if let DataType::Struct(_arrow_fields) = col.data_type() {
                        match &field.data_type {
                            crate::schema::DataType::Struct(field_struct) => {
                                debug!(
                                    "Recurse into {} with schema {:#?}",
                                    field.name, field_struct
                                );
                                let struct_array = col.as_struct();
                                SimpleData::extract_row(
                                    struct_array,
                                    field_struct,
                                    row,
                                    had_data,
                                    res_arry,
                                );
                            }
                            _ => panic!("schema mismatch"),
                        }
                    }
                    if col.is_null(row) {
                        debug!("Pushing None for {}", field.name);
                        res_arry.push(None);
                    } else {
                        *had_data = true;
                        match col.data_type() {
                            DataType::Struct(_) => {} // handled above
                            DataType::Boolean => {
                                let val = col.as_boolean().value(row);
                                debug!("For {} pushing: {}", field.name, val);
                                res_arry.push(Some(DataItem::Bool(val)));
                            }
                            DataType::Int32 => {
                                let val = col.as_primitive::<Int32Type>().value(row);
                                debug!("For {} pushing: {}", field.name, val);
                                res_arry.push(Some(DataItem::I32(val)));
                            }
                            DataType::Int64 => {
                                let val = col.as_primitive::<Int64Type>().value(row);
                                debug!("For {} pushing: {}", field.name, val);
                                res_arry.push(Some(DataItem::I64(val)));
                            }
                            DataType::Utf8 => {
                                let val = col.as_string::<i32>().value(row);
                                debug!("For {} pushing: {}", field.name, val);
                                res_arry.push(Some(DataItem::Str(val)));
                            }
                            DataType::List(_) => {
                                res_arry.push(Some(DataItem::List(col.as_list::<i32>())));
                            }
                            DataType::Map(_, _) => {
                                res_arry.push(Some(DataItem::Map(col.as_map())));
                            }
                            typ => {
                                error!("CAN'T EXTRACT: {}", typ);
                                unimplemented!()
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn extract(&self, schema: SchemaRef, visitor: &mut dyn DataVisitor) {
        for row in 0..self.data.num_rows() {
            debug!("Extracting row: {}", row);
            let mut res_arry: Vec<Option<DataItem<'_>>> = vec![];
            let mut had_data = false;
            SimpleData::extract_row(&self.data, &schema, row, &mut had_data, &mut res_arry);
            if had_data {
                visitor.visit(row, &res_arry);
            }
        }
    }

    pub fn length(&self) -> usize {
        self.data.num_rows()
    }
}

impl From<RecordBatch> for SimpleData {
    fn from(value: RecordBatch) -> Self {
        SimpleData { data: value }
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
