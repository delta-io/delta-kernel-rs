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
        let handler = SyncParquetHandler;

        // Use a checkpoint parquet file
        let path = std::fs::canonicalize(PathBuf::from(
            "./tests/data/with_checkpoint_no_last_checkpoint/_delta_log/00000000000000000002.checkpoint.parquet",
        ))?;
        let url = Url::from_file_path(path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        // Get the schema
        let schema = handler.get_parquet_schema(&file_meta)?;

        println!("schema: {:?}", schema);

        // Verify the schema has fields
        assert!(
            schema.fields().count() > 0,
            "Schema should have at least one field"
        );

        // Verify this is a checkpoint schema with expected fields
        let field_names: Vec<&str> = schema.fields().map(|f| f.name()).collect();
        assert!(
            field_names.contains(&"txn"),
            "Checkpoint should have 'txn' field"
        );
        assert!(
            field_names.contains(&"add"),
            "Checkpoint should have 'add' field"
        );
        assert!(
            field_names.contains(&"remove"),
            "Checkpoint should have 'remove' field"
        );
        assert!(
            field_names.contains(&"metaData"),
            "Checkpoint should have 'metaData' field"
        );
        assert!(
            field_names.contains(&"protocol"),
            "Checkpoint should have 'protocol' field"
        );

        // Verify we can access field properties
        for field in schema.fields() {
            assert!(!field.name().is_empty(), "Field name should not be empty");
            let _data_type = field.data_type(); // Should not panic
        }

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
