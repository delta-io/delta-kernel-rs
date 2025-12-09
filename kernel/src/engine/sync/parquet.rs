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
    DeltaResult, Error, FileDataReadResultIterator, FileMeta, ParquetFooter, ParquetHandler,
    PredicateRef,
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

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        let path = file
            .location
            .to_file_path()
            .map_err(|_| Error::generic("SyncEngine can only read local files"))?;
        let file = File::open(path)?;
        let metadata = ArrowReaderMetadata::load(&file, Default::default())?;
        let schema = StructType::try_from_arrow(metadata.schema().as_ref())
            .map(Arc::new)
            .map_err(Error::Arrow)?;
        Ok(ParquetFooter { schema })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use url::Url;

    use super::*;
    use crate::arrow::array::{Int64Array, RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use crate::parquet::arrow::arrow_writer::ArrowWriter;
    use crate::parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    #[test]
    fn test_sync_read_parquet_footer() -> DeltaResult<()> {
        let handler = SyncParquetHandler;
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

        let footer = handler.read_parquet_footer(&file_meta)?;
        crate::utils::test_utils::validate_checkpoint_schema(&footer.schema);

        Ok(())
    }

    #[test]
    fn test_sync_read_parquet_footer_invalid_file() {
        let handler = SyncParquetHandler;

        let mut temp_path = std::env::temp_dir();
        temp_path.push("non_existent_file_for_sync_test.parquet");
        let url = Url::from_file_path(temp_path).unwrap();
        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        let result = handler.read_parquet_footer(&file_meta);
        assert!(result.is_err(), "Should error on non-existent file");
    }

    #[test]
    fn test_sync_read_parquet_footer_preserves_field_ids() {
        // Create Arrow schema with field IDs in metadata
        let field_with_id = Field::new("id", ArrowDataType::Int64, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let field_with_id_2 = Field::new("name", ArrowDataType::Utf8, true).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field_with_id, field_with_id_2]));

        // Write a parquet file with this schema
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test_field_ids.parquet");

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read footer and verify schema
        let handler = SyncParquetHandler;
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let url = Url::from_file_path(&file_path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: file_size,
        };

        let footer = handler.read_parquet_footer(&file_meta).unwrap();

        // Verify field IDs are preserved
        let id_field = footer.schema.fields().find(|f| f.name() == "id").unwrap();
        assert_eq!(
            id_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".into())
        );

        let name_field = footer.schema.fields().find(|f| f.name() == "name").unwrap();
        assert_eq!(
            name_field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".into())
        );
    }
}
