use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::types::{Int32Type, Int64Type};
use crate::arrow::array::{
    Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch, RunArray,
    StructArray,
};
use crate::arrow::compute::filter_record_batch;
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

/// A trait to allow easy conversion from [`EngineData`] to an arrow [``RecordBatch`]. Returns an
/// error if called on an `EngineData` that is not an `ArrowEngineData`.
pub trait EngineDataArrowExt {
    fn try_into_record_batch(self) -> DeltaResult<RecordBatch>;
}

impl EngineDataArrowExt for Box<dyn EngineData> {
    fn try_into_record_batch(self) -> DeltaResult<RecordBatch> {
        Ok(self
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into())
    }
}

impl EngineDataArrowExt for DeltaResult<Box<dyn EngineData>> {
    fn try_into_record_batch(self) -> DeltaResult<RecordBatch> {
        Ok(self?
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into())
    }
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
        let len = EngineList::len(self, row_index);
        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            result.push(self.get(row_index, i));
        }
        result
    }
}

impl EngineMap for MapArray {
    fn get<'a>(&'a self, row_index: usize, key: &str) -> Option<&'a str> {
        // Check if the map element itself is null
        if self.is_null(row_index) {
            return None;
        }

        let offsets = self.offsets();
        let start_offset = offsets[row_index] as usize;
        let end_offset = offsets[row_index + 1] as usize;
        let keys = self.keys().as_string::<i32>();
        let vals = self.values().as_string::<i32>();

        // Iterate backwards for potential cache locality benefits
        for idx in (start_offset..end_offset).rev() {
            let map_key = keys.value(idx);
            if key == map_key {
                return vals.is_valid(idx).then(|| vals.value(idx));
            }
        }
        None
    }

    fn materialize(&self, row_index: usize) -> HashMap<String, String> {
        // Check if the map element itself is null
        if self.is_null(row_index) {
            return HashMap::new();
        }

        let offsets = self.offsets();
        let start_offset = offsets[row_index] as usize;
        let end_offset = offsets[row_index + 1] as usize;
        let keys = self.keys().as_string::<i32>();
        let vals = self.values().as_string::<i32>();
        let mut ret = HashMap::with_capacity(end_offset - start_offset);

        // Use direct array access for better performance vs Arrow's high-level API
        for idx in start_offset..end_offset {
            if vals.is_valid(idx) {
                // Arrow maps always have non-null keys.
                let key = keys.value(idx);
                let value = vals.value(idx);
                ret.insert(key.to_string(), value.to_string());
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
        let mask_capacity: usize = leaf_columns.iter().map(|c| c.len()).sum();
        let mut mask = HashSet::with_capacity(mask_capacity);
        for column in leaf_columns {
            for i in 0..column.len() {
                mask.insert(&column[..i + 1]);
            }
        }
        debug!("Column mask for selected columns {leaf_columns:?} is {mask:#?}");

        let mut getters = Vec::with_capacity(leaf_columns.len());
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
        let existing_schema = self.data.schema();
        let existing_fields = existing_schema.fields();
        let new_fields = schema.fields();
        let mut combined_fields = Vec::with_capacity(existing_fields.len() + new_fields.len());
        combined_fields.extend_from_slice(existing_fields);
        combined_fields.extend_from_slice(new_fields);
        let combined_schema = Arc::new(ArrowSchema::new(combined_fields));

        // Combine existing and new columns
        let new_columns: Vec<ArrayRef> = columns
            .into_iter()
            .map(|array_data| array_data.to_arrow())
            .try_collect()?;
        let existing_columns = self.data.columns();
        let mut combined_columns = Vec::with_capacity(existing_columns.len() + new_columns.len());
        combined_columns.extend_from_slice(existing_columns);
        combined_columns.extend(new_columns);

        // Create a new ArrowEngineData with the combined schema and columns
        let data = RecordBatch::try_new(combined_schema, combined_columns)?;
        Ok(Box::new(ArrowEngineData { data }))
    }

    fn apply_selection_vector(
        self: Box<Self>,
        mut selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        selection_vector.resize(self.len(), true);
        let filtered = filter_record_batch(&self.data, &selection_vector.into())?;
        Ok(Box::new(Self::new(filtered)))
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

    /// Helper function to extract a column, supporting both direct arrays and REE-encoded (RunEndEncoded) arrays.
    /// This reduces boilerplate by handling the common pattern of trying direct access first,
    /// then falling back to RunArray if the column is REE-encoded.
    fn try_extract_with_ree<'a>(col: &'a dyn Array) -> Option<&'a dyn GetData<'a>> {
        match col.data_type() {
            ArrowDataType::RunEndEncoded(_, _) => col
                .as_any()
                .downcast_ref::<RunArray<Int64Type>>()
                .map(|run_array| run_array as &'a dyn GetData<'a>),
            _ => None,
        }
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
                col.as_boolean_opt()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_ree(col))
                    .ok_or("bool")
            }
            &DataType::STRING => {
                debug!("Pushing string array for {}", ColumnName::new(path));
                col.as_string_opt()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_ree(col))
                    .ok_or("string")
            }
            &DataType::BINARY => {
                debug!("Pushing binary array for {}", ColumnName::new(path));
                col.as_binary_opt()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_ree(col))
                    .ok_or("binary")
            }
            &DataType::INTEGER => {
                debug!("Pushing int32 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int32Type>()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_ree(col))
                    .ok_or("int")
            }
            &DataType::LONG => {
                debug!("Pushing int64 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int64Type>()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_ree(col))
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
    use crate::arrow::array::types::{Int32Type, Int64Type};
    use crate::arrow::array::{
        Array, AsArray, BinaryArray, BooleanArray, Int32Array, Int64Array, MapArray, RecordBatch,
        RunArray, StringArray, StructArray,
    };
    use crate::arrow::buffer::OffsetBuffer;
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::{EngineMap, GetData};
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

    #[test]
    fn test_binary_column_extraction() -> DeltaResult<()> {
        use crate::arrow::array::BinaryArray;
        use crate::engine_data::{GetData, RowVisitor};
        use crate::schema::ColumnName;
        use std::sync::LazyLock;

        // Create a RecordBatch with binary data
        let binary_data: Vec<Option<&[u8]>> = vec![
            Some(b"hello"),
            Some(b"world"),
            None,
            Some(b"\x00\x01\x02\x03"),
        ];
        let binary_array = BinaryArray::from(binary_data.clone());

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "data",
            ArrowDataType::Binary,
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(binary_array)])?;
        let arrow_data = ArrowEngineData::new(batch);

        // Create a visitor to extract binary data
        struct BinaryVisitor {
            values: Vec<Option<Vec<u8>>>,
        }

        impl RowVisitor for BinaryVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> =
                    LazyLock::new(|| vec![ColumnName::new(["data"])]);
                static TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| vec![DataType::BINARY]);
                (&NAMES, &TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                assert_eq!(getters.len(), 1);
                let getter = getters[0];

                for i in 0..row_count {
                    self.values
                        .push(getter.get_binary(i, "data")?.map(|b| b.to_vec()));
                }
                Ok(())
            }
        }

        let mut visitor = BinaryVisitor { values: vec![] };
        arrow_data.visit_rows(&[ColumnName::new(["data"])], &mut visitor)?;

        // Verify the extracted values
        assert_eq!(visitor.values.len(), 4);
        assert_eq!(visitor.values[0].as_deref(), Some(b"hello".as_ref()));
        assert_eq!(visitor.values[1].as_deref(), Some(b"world".as_ref()));
        assert_eq!(visitor.values[2], None);
        assert_eq!(
            visitor.values[3].as_deref(),
            Some(b"\x00\x01\x02\x03".as_ref())
        );

        Ok(())
    }

    #[test]
    fn test_binary_column_extraction_type_mismatch() -> DeltaResult<()> {
        use crate::engine_data::{GetData, RowVisitor};
        use crate::schema::ColumnName;
        use std::sync::LazyLock;

        // Create a RecordBatch with Int32 data (not binary)
        let data: Vec<Option<i32>> = vec![Some(123)];
        let int_array = Int32Array::from(data);

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "data",
            ArrowDataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array)])?;
        let arrow_data = ArrowEngineData::new(batch);

        // Create a visitor that tries to extract binary data from an int column
        struct BinaryVisitor {
            values: Vec<Option<Vec<u8>>>,
        }

        impl RowVisitor for BinaryVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static NAMES: LazyLock<Vec<ColumnName>> =
                    LazyLock::new(|| vec![ColumnName::new(["data"])]);
                static TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| vec![DataType::BINARY]);
                (&NAMES, &TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                assert_eq!(getters.len(), 1);
                let getter = getters[0];

                for i in 0..row_count {
                    self.values
                        .push(getter.get_binary(i, "data")?.map(|b| b.to_vec()));
                }
                Ok(())
            }
        }

        let mut visitor = BinaryVisitor { values: vec![] };
        let result = arrow_data.visit_rows(&[ColumnName::new(["data"])], &mut visitor);

        // Verify that we get a type mismatch error
        assert_result_error_with_message(
            result,
            "Type mismatch on data: expected binary, got Int32",
        );

        Ok(())
    }

    #[test]
    fn test_run_array_out_of_bounds_errors() -> DeltaResult<()> {
        // Test that out of bounds errors include field name for all types
        let run_ends = Int64Array::from(vec![2]);

        // Test str
        let str_array =
            RunArray::<Int64Type>::try_new(&run_ends, &StringArray::from(vec!["test"]))?;
        let err_msg = str_array.get_str(2, "str_field").unwrap_err().to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("str_field"));

        // Test int
        let int_array = RunArray::<Int64Type>::try_new(&run_ends, &Int32Array::from(vec![42]))?;
        let err_msg = int_array.get_int(5, "int_field").unwrap_err().to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("int_field"));

        // Test long
        let long_array =
            RunArray::<Int64Type>::try_new(&run_ends, &Int64Array::from(vec![100i64]))?;
        let err_msg = long_array
            .get_long(3, "long_field")
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("long_field"));

        // Test bool
        let bool_array =
            RunArray::<Int64Type>::try_new(&run_ends, &BooleanArray::from(vec![true]))?;
        let err_msg = bool_array
            .get_bool(2, "bool_field")
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("bool_field"));

        // Test binary
        let binary_array = RunArray::<Int64Type>::try_new(
            &run_ends,
            &BinaryArray::from(vec![Some(b"data".as_ref())]),
        )?;
        let err_msg = binary_array
            .get_binary(4, "binary_field")
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("binary_field"));

        Ok(())
    }

    #[test]
    fn test_run_array_extraction_via_visitor() -> DeltaResult<()> {
        use crate::engine_data::RowVisitor;
        use crate::schema::ColumnName;
        use std::sync::LazyLock;

        // Create RunArray columns with pattern: [val1, val1, null, null, val2]
        // Per Arrow spec: nulls are encoded as runs in the values child array
        let run_ends = Int64Array::from(vec![2, 4, 5]);
        let mk_field = |name, dt| {
            ArrowField::new(
                name,
                ArrowDataType::RunEndEncoded(
                    Arc::new(ArrowField::new("run_ends", ArrowDataType::Int64, false)),
                    Arc::new(ArrowField::new("values", dt, true)),
                ),
                true,
            )
        };

        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(RunArray::<Int64Type>::try_new(
                &run_ends,
                &StringArray::from(vec![Some("a"), None, Some("b")]),
            )?),
            Arc::new(RunArray::<Int64Type>::try_new(
                &run_ends,
                &Int32Array::from(vec![Some(1), None, Some(2)]),
            )?),
            Arc::new(RunArray::<Int64Type>::try_new(
                &run_ends,
                &Int64Array::from(vec![Some(10i64), None, Some(20)]),
            )?),
            Arc::new(RunArray::<Int64Type>::try_new(
                &run_ends,
                &BooleanArray::from(vec![Some(true), None, Some(false)]),
            )?),
            Arc::new(RunArray::<Int64Type>::try_new(
                &run_ends,
                &BinaryArray::from(vec![Some(b"x".as_ref()), None, Some(b"y".as_ref())]),
            )?),
        ];

        let schema = Arc::new(ArrowSchema::new(vec![
            mk_field("s", ArrowDataType::Utf8),
            mk_field("i", ArrowDataType::Int32),
            mk_field("l", ArrowDataType::Int64),
            mk_field("b", ArrowDataType::Boolean),
            mk_field("bin", ArrowDataType::Binary),
        ]));

        let arrow_data = ArrowEngineData::new(RecordBatch::try_new(schema, columns)?);

        type Row = (
            Option<String>,
            Option<i32>,
            Option<i64>,
            Option<bool>,
            Option<Vec<u8>>,
        );

        struct TestVisitor {
            data: Vec<Row>,
        }

        impl RowVisitor for TestVisitor {
            fn selected_column_names_and_types(
                &self,
            ) -> (&'static [ColumnName], &'static [DataType]) {
                static COLUMNS: LazyLock<[ColumnName; 5]> = LazyLock::new(|| {
                    [
                        ColumnName::new(["s"]),
                        ColumnName::new(["i"]),
                        ColumnName::new(["l"]),
                        ColumnName::new(["b"]),
                        ColumnName::new(["bin"]),
                    ]
                });
                static TYPES: &[DataType] = &[
                    DataType::STRING,
                    DataType::INTEGER,
                    DataType::LONG,
                    DataType::BOOLEAN,
                    DataType::BINARY,
                ];
                (&*COLUMNS, TYPES)
            }

            fn visit<'a>(
                &mut self,
                row_count: usize,
                getters: &[&'a dyn GetData<'a>],
            ) -> DeltaResult<()> {
                for i in 0..row_count {
                    self.data.push((
                        getters[0].get_str(i, "s")?.map(|s| s.to_string()),
                        getters[1].get_int(i, "i")?,
                        getters[2].get_long(i, "l")?,
                        getters[3].get_bool(i, "b")?,
                        getters[4].get_binary(i, "bin")?.map(|b| b.to_vec()),
                    ));
                }
                Ok(())
            }
        }

        let mut visitor = TestVisitor { data: vec![] };
        visitor.visit_rows_of(&arrow_data)?;

        // Verify decompression including nulls: [val1, val1, null, null, val2]
        let expected = vec![
            (
                Some("a".into()),
                Some(1),
                Some(10),
                Some(true),
                Some(b"x".to_vec()),
            ),
            (
                Some("a".into()),
                Some(1),
                Some(10),
                Some(true),
                Some(b"x".to_vec()),
            ),
            (None, None, None, None, None),
            (None, None, None, None, None),
            (
                Some("b".into()),
                Some(2),
                Some(20),
                Some(false),
                Some(b"y".to_vec()),
            ),
        ];
        assert_eq!(visitor.data, expected);

        Ok(())
    }

    /// Helper to create a MapArray from key-value pairs for materialize tests
    fn create_map_array(entries: Vec<Vec<(&str, Option<&str>)>>) -> MapArray {
        let mut all_keys = vec![];
        let mut all_values = vec![];
        let mut offsets = vec![0i32];

        for entry_group in entries {
            for (key, value) in entry_group {
                all_keys.push(Some(key));
                all_values.push(value);
            }
            offsets.push(all_keys.len() as i32);
        }

        let keys_array =
            Arc::new(StringArray::from(all_keys)) as Arc<dyn crate::arrow::array::Array>;
        let values_array =
            Arc::new(StringArray::from(all_values)) as Arc<dyn crate::arrow::array::Array>;

        let entries_struct = StructArray::try_new(
            vec![
                Arc::new(ArrowField::new("keys", ArrowDataType::Utf8, false)),
                Arc::new(ArrowField::new("values", ArrowDataType::Utf8, true)),
            ]
            .into(),
            vec![keys_array, values_array],
            None,
        )
        .unwrap();

        let offsets_buffer = OffsetBuffer::new(offsets.into());
        MapArray::try_new(
            Arc::new(ArrowField::new_struct(
                "entries",
                vec![
                    Arc::new(ArrowField::new("keys", ArrowDataType::Utf8, false)),
                    Arc::new(ArrowField::new("values", ArrowDataType::Utf8, true)),
                ],
                false,
            )),
            offsets_buffer,
            entries_struct,
            None,
            false,
        )
        .unwrap()
    }

    #[test]
    fn test_materialize_matches_get() -> DeltaResult<()> {
        // Create MapArray with various keys
        let map_array = create_map_array(vec![vec![
            ("key1", Some("value1")),
            ("key2", Some("value2")),
            ("key3", Some("value3")),
        ]]);

        let materialized = map_array.materialize(0);

        // Verify that get(key) matches materialize()[key] for all keys
        for (key, value) in &materialized {
            let get_result = map_array.get(0, key);
            assert_eq!(get_result, Some(value.as_str()));
        }

        // Verify count matches
        assert_eq!(materialized.len(), 3);
        Ok(())
    }

    #[test]
    fn test_materialize_handles_nulls() -> DeltaResult<()> {
        // Create MapArray with null values
        let map_array =
            create_map_array(vec![vec![("a", Some("1")), ("b", None), ("c", Some("3"))]]);

        let result = map_array.materialize(0);

        // Null values should be excluded from materialized map
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("a"), Some(&"1".to_string()));
        assert_eq!(result.get("b"), None);
        assert_eq!(result.get("c"), Some(&"3".to_string()));
        Ok(())
    }

    #[test]
    fn test_materialize_empty_map() -> DeltaResult<()> {
        // Create MapArray with empty map
        let map_array = create_map_array(vec![vec![]]);

        let result = map_array.materialize(0);

        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[test]
    fn test_materialize_multiple_rows() -> DeltaResult<()> {
        // Create MapArray with multiple rows
        let map_array = create_map_array(vec![
            vec![("a", Some("1")), ("b", Some("2"))],
            vec![("x", Some("10")), ("y", Some("20"))],
        ]);

        let result0 = map_array.materialize(0);
        assert_eq!(result0.len(), 2);
        assert_eq!(result0.get("a"), Some(&"1".to_string()));
        assert_eq!(result0.get("b"), Some(&"2".to_string()));

        let result1 = map_array.materialize(1);
        assert_eq!(result1.len(), 2);
        assert_eq!(result1.get("x"), Some(&"10".to_string()));
        assert_eq!(result1.get("y"), Some(&"20".to_string()));
        Ok(())
    }

    #[test]
    fn test_get_vs_materialize_consistency_with_duplicates() -> DeltaResult<()> {
        // Test that materialize() handles duplicate keys correctly (last wins)
        // and that get() returns the same value as materialize() for duplicate keys
        let map_array = create_map_array(vec![vec![
            ("a", Some("1")),
            ("b", Some("2")),
            ("a", Some("3")), // Duplicate 'a' - should override first
            ("c", Some("4")),
            ("a", Some("5")), // Another duplicate 'a' - should be final value
        ]]);

        let materialized = map_array.materialize(0);

        // Verify materialize() handles duplicates correctly (last wins)
        assert_eq!(materialized.len(), 3); // Only 3 unique keys
        assert_eq!(materialized.get("a"), Some(&"5".to_string())); // Last 'a' wins
        assert_eq!(materialized.get("b"), Some(&"2".to_string()));
        assert_eq!(materialized.get("c"), Some(&"4".to_string()));

        // Verify get() and materialize() return same values
        assert_eq!(map_array.get(0, "a"), Some("5")); // Matches materialized
        assert_eq!(map_array.get(0, "b"), Some("2"));
        assert_eq!(map_array.get(0, "c"), Some("4"));

        Ok(())
    }

    #[test]
    fn test_materialize_null_map() -> DeltaResult<()> {
        // Create MapArray with 3 elements: 2 entries in first, 1 entry in second (null), 1 entry in third
        let keys_array = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"), // First element (2 entries)
            Some("c"), // Second element (1 entry, but element is null)
            Some("d"), // Third element (1 entry)
        ])) as Arc<dyn crate::arrow::array::Array>;

        let values_array = Arc::new(StringArray::from(vec![
            Some("1"),
            Some("2"), // First element values
            Some("3"), // Second element value (but element is null)
            Some("4"), // Third element value
        ])) as Arc<dyn crate::arrow::array::Array>;

        let entries_struct = StructArray::try_new(
            vec![
                Arc::new(ArrowField::new("keys", ArrowDataType::Utf8, false)),
                Arc::new(ArrowField::new("values", ArrowDataType::Utf8, true)),
            ]
            .into(),
            vec![keys_array, values_array],
            None,
        )
        .unwrap();

        // Offsets: [0, 2, 3, 4] - first has 2 entries, second has 1, third has 1
        let offsets_buffer = OffsetBuffer::new(vec![0i32, 2, 3, 4].into());

        // Create null buffer with second element (index 1) null
        let null_buffer = Some(crate::arrow::buffer::NullBuffer::from(vec![
            true, false, true,
        ]));

        let map_array = MapArray::try_new(
            Arc::new(ArrowField::new_struct(
                "entries",
                vec![
                    Arc::new(ArrowField::new("keys", ArrowDataType::Utf8, false)),
                    Arc::new(ArrowField::new("values", ArrowDataType::Utf8, true)),
                ],
                false,
            )),
            offsets_buffer,
            entries_struct,
            null_buffer,
            false,
        )
        .unwrap();

        // First element should have 2 entries
        let result0 = map_array.materialize(0);
        assert_eq!(result0.len(), 2);
        assert_eq!(result0.get("a"), Some(&"1".to_string()));
        assert_eq!(result0.get("b"), Some(&"2".to_string()));

        // Second element is null, should return empty HashMap
        let result1 = map_array.materialize(1);
        assert_eq!(result1.len(), 0);

        // get() on null element should return None, even for key that exists in underlying data
        assert_eq!(map_array.get(1, "c"), None); // "c" exists in data but element is null

        // Third element should have 1 entry
        let result2 = map_array.materialize(2);
        assert_eq!(result2.len(), 1);
        assert_eq!(result2.get("d"), Some(&"4".to_string()));

        Ok(())
    }
}
