use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use tracing::debug;

use crate::arrow::array::cast::AsArray;
use crate::arrow::array::types::{Int32Type, Int64Type};
use crate::arrow::array::{
    Array, ArrayRef, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch, StructArray,
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

    fn apply_selection_vector(
        self: Box<Self>,
        mut selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        selection_vector.resize(self.len(), true);
        let filtered = filter_record_batch(&self.data, &selection_vector.into())?;
        Ok(Box::new(Self::new(filtered)))
    }
}

/// Validates row index and returns physical index into the values array.
///
/// Per Arrow spec, REE parent array has no validity bitmap (null_count = 0).
/// Nulls are encoded in the values child array, so null checking must be done
/// on the values array in each get_* method, not here on the parent array.
fn validate_and_get_physical_index(
    run_array: &crate::arrow::array::RunArray<Int64Type>,
    row_index: usize,
    field_name: &str,
) -> DeltaResult<usize> {
    if row_index >= run_array.len() {
        return Err(Error::generic(format!(
            "Row index {} out of bounds for field '{}'",
            row_index, field_name
        )));
    }

    let physical_idx = run_array.run_ends().get_physical_index(row_index);
    Ok(physical_idx)
}

/// Implement GetData for RunArray directly, so we can return it as a trait object
/// without needing a wrapper struct or Box::leak.
///
/// This implementation supports multiple value types (strings, integers, booleans, etc.)
/// by runtime downcasting of the values array.
impl<'a> GetData<'a> for crate::arrow::array::RunArray<Int64Type> {
    fn get_str(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<&'a str>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_any()
            .downcast_ref::<crate::arrow::array::StringArray>()
            .ok_or_else(|| Error::generic("Expected StringArray values in RunArray"))?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_int(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<i32>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| Error::generic("Expected Int32Array values in RunArray"))?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_long(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<i64>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| Error::generic("Expected Int64Array values in RunArray"))?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_bool(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<bool>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_boolean_opt()
            .ok_or_else(|| Error::generic("Expected BooleanArray values in RunArray"))?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
    }

    fn get_binary(&'a self, row_index: usize, field_name: &str) -> DeltaResult<Option<&'a [u8]>> {
        let physical_idx = validate_and_get_physical_index(self, row_index, field_name)?;
        let values = self
            .values()
            .as_any()
            .downcast_ref::<crate::arrow::array::GenericByteArray<
                crate::arrow::array::types::GenericBinaryType<i32>,
            >>()
            .ok_or_else(|| Error::generic("Expected BinaryArray values in RunArray"))?;

        Ok((!values.is_null(physical_idx)).then(|| values.value(physical_idx)))
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

    /// Helper function to extract a column, supporting both direct arrays and RLE-encoded (RunEndEncoded) arrays.
    /// This reduces boilerplate by handling the common pattern of trying direct access first,
    /// then falling back to RunArray if the column is RLE-encoded.
    fn try_extract_with_rle<'a>(col: &'a dyn Array) -> Option<&'a dyn GetData<'a>> {
        use crate::arrow::array::RunArray;
        use crate::arrow::datatypes::DataType as ArrowDataType;

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
                    .or_else(|| Self::try_extract_with_rle(col))
                    .ok_or("bool")
            }
            &DataType::STRING => {
                debug!("Pushing string array for {}", ColumnName::new(path));
                col.as_string_opt()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_rle(col))
                    .ok_or("string")
            }
            &DataType::BINARY => {
                debug!("Pushing binary array for {}", ColumnName::new(path));
                col.as_binary_opt()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_rle(col))
                    .ok_or("binary")
            }
            &DataType::INTEGER => {
                debug!("Pushing int32 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int32Type>()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_rle(col))
                    .ok_or("int")
            }
            &DataType::LONG => {
                debug!("Pushing int64 array for {}", ColumnName::new(path));
                col.as_primitive_opt::<Int64Type>()
                    .map(|a| a as _)
                    .or_else(|| Self::try_extract_with_rle(col))
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
        Array, AsArray, BinaryArray, BooleanArray, Int32Array, Int64Array, RecordBatch, RunArray,
        StringArray,
    };
    use crate::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use crate::engine::sync::SyncEngine;
    use crate::engine_data::GetData;
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
    fn test_run_array_get_str() -> DeltaResult<()> {
        // Create a RunArray with string values: ["a", "a", "a", "b", "b"]
        let run_ends = Int64Array::from(vec![3, 5]);
        let values = StringArray::from(vec!["a", "b"]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        // Test accessing values
        let results: Vec<_> = (0..5)
            .map(|i| run_array.get_str(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(
            results,
            vec![Some("a"), Some("a"), Some("a"), Some("b"), Some("b")]
        );

        // Test out of bounds
        let err = run_array.get_str(5, "test_field").unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("test_field"));

        Ok(())
    }

    #[test]
    fn test_run_array_get_str_with_nulls() -> DeltaResult<()> {
        // Create a RunArray with nulls in child values: ["a", "a", null, null, "b"]
        // Per Arrow spec: REE parent has no nulls; nulls only in child values array
        let run_ends = Int64Array::from(vec![2, 4, 5]);
        let values = StringArray::from(vec![Some("a"), None, Some("b")]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..5)
            .map(|i| run_array.get_str(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(results, vec![Some("a"), Some("a"), None, None, Some("b")]);

        Ok(())
    }

    #[test]
    fn test_run_array_get_int() -> DeltaResult<()> {
        // Create a RunArray with int values: [10, 10, 20, 20, 20]
        let run_ends = Int64Array::from(vec![2, 5]);
        let values = Int32Array::from(vec![10, 20]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..5)
            .map(|i| run_array.get_int(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(
            results,
            vec![Some(10), Some(10), Some(20), Some(20), Some(20)]
        );

        Ok(())
    }

    #[test]
    fn test_run_array_get_int_with_nulls() -> DeltaResult<()> {
        // Create a RunArray with nulls in child values: [1, null, null, 2]
        // Per Arrow spec: REE parent has no nulls; nulls only in child values array
        let run_ends = Int64Array::from(vec![1, 3, 4]);
        let values = Int32Array::from(vec![Some(1), None, Some(2)]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..4)
            .map(|i| run_array.get_int(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(results, vec![Some(1), None, None, Some(2)]);

        Ok(())
    }

    #[test]
    fn test_run_array_get_long() -> DeltaResult<()> {
        // Create a RunArray with long values: [100, 100, 100, 200]
        let run_ends = Int64Array::from(vec![3, 4]);
        let values = Int64Array::from(vec![100i64, 200i64]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..4)
            .map(|i| run_array.get_long(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(results, vec![Some(100), Some(100), Some(100), Some(200)]);

        Ok(())
    }

    #[test]
    fn test_run_array_get_bool() -> DeltaResult<()> {
        // Create a RunArray with bool values: [true, true, false, false, true]
        let run_ends = Int64Array::from(vec![2, 4, 5]);
        let values = BooleanArray::from(vec![true, false, true]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..5)
            .map(|i| run_array.get_bool(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(
            results,
            vec![Some(true), Some(true), Some(false), Some(false), Some(true)]
        );

        Ok(())
    }

    #[test]
    fn test_run_array_get_bool_with_nulls() -> DeltaResult<()> {
        // Create a RunArray with nulls in child values: [true, null, false]
        // Per Arrow spec: REE parent has no nulls; nulls only in child values array
        let run_ends = Int64Array::from(vec![1, 2, 3]);
        let values = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..3)
            .map(|i| run_array.get_bool(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(results, vec![Some(true), None, Some(false)]);

        Ok(())
    }

    #[test]
    fn test_run_array_out_of_bounds_errors() -> DeltaResult<()> {
        let run_ends = Int64Array::from(vec![2]);
        let values = StringArray::from(vec!["test"]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        // Test that out of bounds errors include field name
        let err_msg = run_array.get_str(2, "my_field").unwrap_err().to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("my_field"));

        let err_msg = run_array
            .get_int(5, "another_field")
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("out of bounds") && err_msg.contains("another_field"));

        Ok(())
    }

    #[test]
    fn test_run_array_get_binary() -> DeltaResult<()> {
        // Create a RunArray with binary values
        let run_ends = Int64Array::from(vec![2, 4, 5]);
        let values = BinaryArray::from(vec![
            Some(b"hello".as_ref()),
            Some(b"world".as_ref()),
            Some(b"\x00\x01\x02".as_ref()),
        ]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..5)
            .map(|i| run_array.get_binary(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(
            results,
            vec![
                Some(b"hello".as_ref()),
                Some(b"hello".as_ref()),
                Some(b"world".as_ref()),
                Some(b"world".as_ref()),
                Some(b"\x00\x01\x02".as_ref())
            ]
        );

        Ok(())
    }

    #[test]
    fn test_run_array_get_binary_with_nulls() -> DeltaResult<()> {
        // Create a RunArray with nulls in child values: [data, null, more]
        // Per Arrow spec: REE parent has no nulls; nulls only in child values array
        let run_ends = Int64Array::from(vec![1, 2, 3]);
        let values = BinaryArray::from(vec![Some(b"data".as_ref()), None, Some(b"more".as_ref())]);
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, &values)?;

        let results: Vec<_> = (0..3)
            .map(|i| run_array.get_binary(i, "field"))
            .collect::<DeltaResult<_>>()?;
        assert_eq!(
            results,
            vec![Some(b"data".as_ref()), None, Some(b"more".as_ref())]
        );

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
}
