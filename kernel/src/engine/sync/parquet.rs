use std::fs::File;
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use crate::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

use super::read_files;
use crate::engine::arrow_conversion::TryFromArrow as _;
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::arrow_utils::{
    fixup_parquet_read, generate_mask, get_requested_indices, ordering_needs_row_indexes,
    RowIndexBuilder,
};
use crate::engine::parquet_row_group_skipping::ParquetRowGroupSkipping;
use crate::schema::{SchemaRef, StructType};
use crate::{
    DeltaResult, Error, FileDataReadResultIterator, FileMeta, ParquetHandler, PredicateRef,
};

pub(crate) struct SyncParquetHandler;

fn try_create_from_parquet(
    file: File,
    schema: SchemaRef,
    _arrow_schema: ArrowSchemaRef,
    predicate: Option<PredicateRef>,
) -> DeltaResult<impl Iterator<Item = DeltaResult<ArrowEngineData>>> {
    let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
    let parquet_schema = metadata.schema();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let (indices, requested_ordering) = get_requested_indices(&schema, parquet_schema)?;
    if let Some(mask) = generate_mask(&schema, parquet_schema, builder.parquet_schema(), &indices) {
        builder = builder.with_projection(mask);
    }

    // Only create RowIndexBuilder if row indexes are actually needed
    let mut row_indexes = ordering_needs_row_indexes(&requested_ordering)
        .then(|| RowIndexBuilder::new(builder.metadata().row_groups()));

    // Filter row groups and row indexes if a predicate is provided
    if let Some(predicate) = predicate {
        builder = builder.with_row_group_filter(predicate.as_ref(), row_indexes.as_mut());
    }

    let mut row_indexes = row_indexes.map(|rb| rb.build()).transpose()?;
    let stream = builder.build()?;
    Ok(stream.map(move |rbr| fixup_parquet_read(rbr?, &requested_ordering, row_indexes.as_mut())))
}

impl ParquetHandler for SyncParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        read_files(files, schema, predicate, try_create_from_parquet)
    }

    fn get_parquet_schema(&self, file: &FileMeta) -> DeltaResult<SchemaRef> {
        // Convert URL to file path
        let path = file
            .location
            .to_file_path()
            .map_err(|_| Error::generic("SyncEngine can only read local files"))?;

        // Open the file
        let file = File::open(path)?;

        // Load metadata from the file
        let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
        let arrow_schema = metadata.schema();

        // Convert Arrow schema to Kernel schema
        StructType::try_from_arrow(arrow_schema.as_ref())
            .map(Arc::new)
            .map_err(Error::Arrow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use url::Url;

    #[test]
    fn test_sync_get_parquet_schema() -> DeltaResult<()> {
        use crate::schema::{DataType, PrimitiveType};

        let handler = SyncParquetHandler;

        // Use a checkpoint parquet file.
        // Note: This test file does not have Parquet field IDs (column mapping is not enabled).
        // When field IDs are present, they are preserved in StructField metadata under
        // "PARQUET:field_id" and can be accessed via ColumnMetadataKey::ParquetFieldId.
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        ))?;
        let file_size = std::fs::metadata(&path)?.len();
        let url = Url::from_file_path(path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
        };

        // Get the schema
        let schema = handler.get_parquet_schema(&file_meta)?;

        // Helper to get a field by name from a struct type
        let get_field = |struct_type: &crate::schema::StructType, name: &str| {
            struct_type
                .fields()
                .find(|f| f.name() == name)
                .unwrap_or_else(|| panic!("Field '{}' not found", name))
                .clone()
        };

        // Verify top-level checkpoint action fields exist and are structs
        let top_level_fields = ["txn", "add", "remove", "metaData", "protocol"];
        for field_name in top_level_fields {
            let field = get_field(&schema, field_name);
            assert!(
                matches!(field.data_type(), DataType::Struct(_)),
                "Field '{}' should be a struct type",
                field_name
            );
        }

        // Verify 'add' struct has expected nested fields with correct types
        let add_field = get_field(&schema, "add");
        let add_struct = match add_field.data_type() {
            DataType::Struct(s) => s,
            _ => panic!("'add' should be a struct"),
        };
        assert_eq!(
            get_field(add_struct, "path").data_type(),
            &DataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            get_field(add_struct, "size").data_type(),
            &DataType::Primitive(PrimitiveType::Long)
        );
        assert!(
            matches!(
                get_field(add_struct, "partitionValues").data_type(),
                DataType::Map(_)
            ),
            "'partitionValues' should be a map type"
        );

        // Verify 'metaData' struct has nested 'format' struct
        let metadata_field = get_field(&schema, "metaData");
        let metadata_struct = match metadata_field.data_type() {
            DataType::Struct(s) => s,
            _ => panic!("'metaData' should be a struct"),
        };
        let format_field = get_field(metadata_struct, "format");
        let format_struct = match format_field.data_type() {
            DataType::Struct(s) => s,
            _ => panic!("'format' should be a struct"),
        };
        assert_eq!(
            get_field(format_struct, "provider").data_type(),
            &DataType::Primitive(PrimitiveType::String)
        );

        // Verify 'protocol' struct has correct primitive types
        let protocol_field = get_field(&schema, "protocol");
        let protocol_struct = match protocol_field.data_type() {
            DataType::Struct(s) => s,
            _ => panic!("'protocol' should be a struct"),
        };
        assert_eq!(
            get_field(protocol_struct, "minReaderVersion").data_type(),
            &DataType::Primitive(PrimitiveType::Integer)
        );
        assert_eq!(
            get_field(protocol_struct, "minWriterVersion").data_type(),
            &DataType::Primitive(PrimitiveType::Integer)
        );

        Ok(())
    }

    #[test]
    fn test_sync_get_parquet_schema_invalid_file() {
        let handler = SyncParquetHandler;

        // Test with a non-existent file
        let mut temp_path = std::env::temp_dir();
        temp_path.push("non_existent_file_for_sync_test.parquet");
        let url = Url::from_file_path(temp_path).unwrap();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        let result = handler.get_parquet_schema(&file_meta);
        assert!(result.is_err(), "Should error on non-existent file");
    }
}
