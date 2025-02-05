use crate::engine_data::{EngineData, EngineList, EngineMap, GetData, RowVisitor};
use crate::schema::{ColumnName, DataType};
use crate::{DeltaResult, Error};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch, StructArray,
};
use arrow_schema::{DataType as ArrowDataType, FieldRef};
use tracing::debug;

use std::collections::{HashMap, HashSet};

/// ArrowEngineData holds an Arrow RecordBatch, implements `EngineData` so the kernel can extract from it.
pub struct ArrowEngineData {
    data: RecordBatch,
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

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::{
        actions::{get_log_schema, Metadata, Protocol},
        engine::sync::SyncEngine,
        DeltaResult, Engine, EngineData,
    };

    use super::ArrowEngineData;

    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    #[test]
    fn test_md_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.get_json_handler();
        let json_strings: StringArray = vec![
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
        ]
        .into();
        let output_schema = get_log_schema().clone();
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let metadata = Metadata::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(metadata.id, "aff5cb91-8cd9-4195-aef9-446908507302");
        assert_eq!(metadata.created_time, Some(1670892997849));
        assert_eq!(metadata.partition_columns, vec!("c1", "c2"));
        Ok(())
    }

    #[test]
    fn test_protocol_extract() -> DeltaResult<()> {
        let engine = SyncEngine::new();
        let handler = engine.get_json_handler();
        let json_strings: StringArray = vec![
            r#"{"protocol": {"minReaderVersion": 3, "minWriterVersion": 7, "readerFeatures": ["rw1"], "writerFeatures": ["rw1", "w2"]}}"#,
        ]
        .into();
        let output_schema = get_log_schema().project(&["protocol"])?;
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let protocol = Protocol::try_new_from_data(parsed.as_ref())?.unwrap();
        assert_eq!(protocol.min_reader_version(), 3);
        assert_eq!(protocol.min_writer_version(), 7);
        assert_eq!(protocol.reader_features(), Some(["rw1".into()].as_slice()));
        assert_eq!(
            protocol.writer_features(),
            Some(["rw1".into(), "w2".into()].as_slice())
        );
        Ok(())
    }
}
