use crate::engine_data::{DataItem, DataVisitor, EngineData, ListItem, MapItem, TypeTag};
use crate::schema::{Schema, SchemaRef};
use crate::DeltaResult;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, GenericListArray, MapArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, error, warn};
use url::Url;

use std::any::Any;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub struct SimpleDataTypeTag;
impl TypeTag for SimpleDataTypeTag {}

/// SimpleData holds a RecordBatch
pub struct SimpleData {
    data: RecordBatch,
}

impl EngineData for SimpleData {
    fn type_tag(&self) -> &dyn TypeTag {
        &SimpleDataTypeTag
    }

    fn as_any(&self) -> &dyn Any {
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

    fn get<'a>(&'a self, row_index: usize, index: usize) -> String {
        let arry = self.value(row_index);
        let sarry = arry.as_string::<i32>();
        sarry.value(index).to_string()
    }
}

// TODO: This is likely wrong and needs to only scan the correct row
impl MapItem for MapArray {
    fn get<'a>(&'a self, key: &str) -> Option<&'a str> {
        let keys = self.keys().as_string::<i32>();
        let mut idx = 0;
        for map_key in keys.iter() {
            if let Some(map_key) = map_key {
                if key == map_key {
                    // found the item
                    let vals = self.values().as_string::<i32>();
                    return Some(vals.value(idx));
                }
            }
            idx += 1;
        }
        None
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
        &'a self,
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
                                self.extract_row(
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
                            typ @ _ => {
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
            self.extract_row(&self.data, &schema, row, &mut had_data, &mut res_arry);
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use arrow_array::{Int64Array, StringArray, ListArray, builder::{StringBuilder, MapBuilder}};
//     use arrow_schema::{DataType, Field, Fields, Schema};

//     fn create_metadata_batch(metadata_schema: Schema) -> RecordBatch {
//         let id_array = StringArray::from(vec![Some("id")]);
//         let ct_array = Int64Array::from(vec![1]);

//         let prov_array = StringArray::from(vec![Some("parquet")]);
//         let schema_array = StringArray::from(vec![Some("schema!")]);

//         let format_key_builder = StringBuilder::new();
//         let format_val_builder = StringBuilder::new();
//         let mut format_builder = MapBuilder::new(None, format_key_builder, format_val_builder);
//         format_builder.keys().append_value("conf_key");
//         format_builder.values().append_value("conf_val");
//         format_builder.append(true).unwrap();
//         let format_config_array = format_builder.finish();

//         let format_fields = Fields::from(vec![
//             Field::new("provider", DataType::Utf8, false),
//             Field::new("configuration", format_config_array.data_type().clone(), true),
//         ]);
//         let format_array = StructArray::new(
//             format_fields,
//             vec![
//                 Arc::new(prov_array),
//                 Arc::new(format_config_array)
//             ],
//             None
//         );

//         let partition_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec!(
//             Some(vec![Some(0)]),
//         ));

//         let key_builder = StringBuilder::new();
//         let val_builder = StringBuilder::new();
//         let mut builder = MapBuilder::new(None, key_builder, val_builder);
//         builder.keys().append_value("conf_key");
//         builder.values().append_value("conf_val");
//         builder.append(true).unwrap();
//         let config_array = builder.finish();

//         RecordBatch::try_new(
//             Arc::new(metadata_schema),
//             vec![
//                 Arc::new(id_array),
//                 Arc::new(StringArray::new_null(1)), // name
//                 Arc::new(StringArray::new_null(1)), // desc
//                 Arc::new(format_array),
//                 Arc::new(schema_array), // schemaString
//                 Arc::new(partition_array), // partitionColumns
//                 Arc::new(ct_array),
//                 Arc::new(config_array), // configuration
//             ],
//         )
//         .unwrap()
//     }

//     #[test]
//     fn test_md_extract() {
//         use crate::schema::{DataType, PrimitiveType, StructField, StructType};
//         let metadata_schema = crate::actions::schemas::METADATA_FIELDS.clone();
//         let s = SimpleData {
//             data: create_metadata_batch(
//                 crate::actions::schemas::METADATA_SCHEMA.as_ref().try_into().unwrap()
//             ),
//         };
//         let mut metadata_visitor = crate::actions::action_definitions::MetadataVisitor::default();
//         s.extract(Arc::new(metadata_schema), &mut metadata_visitor);

//         println!("Got: {:?}", metadata_visitor.extracted);

//         assert!(metadata_visitor.extracted.is_some());
//         let metadata = metadata_visitor.extracted.unwrap();
//         assert!(metadata.id == "id");
//         assert!(metadata.created_time == Some(1));
//     }
// }
