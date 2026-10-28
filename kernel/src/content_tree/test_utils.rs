//! Test helpers for building AMT content-tree entries and writing them as manifest files.

use std::iter;
use std::sync::Arc;

use url::Url;

use crate::content_tree::{
    AMT_FORMAT_VERSION, ContentTreeNodeEntry, DataContentType, DataFileFormat, ManifestInfo,
    TrackingInfo, TrackingStatus,
};
use crate::expressions::{Scalar, StructData};
use crate::schema::ToSchema as _;
use crate::{Engine, EngineData, FileMeta};

/// A minimal-root `Data`/`Added` entry with the given path, size, and row-tracking numbers.
pub(crate) fn added_data_entry(
    path: &str,
    size: i64,
    num_records: i64,
    base_row_id: i64,
    commit_version: i64,
) -> ContentTreeNodeEntry {
    ContentTreeNodeEntry {
        content_type: DataContentType::Data,
        location: path.to_string(),
        file_format: DataFileFormat::Parquet,
        tracking: TrackingInfo {
            status: TrackingStatus::Added,
            snapshot_id: Some(1),
            modified_snapshot_id: None,
            sequence_number: Some(commit_version),
            file_sequence_number: Some(commit_version),
            first_row_id: Some(base_row_id),
            deleted_positions: None,
            replaced_positions: None,
        },
        deletion_vector: None,
        spec_id: Some(0),
        partition: None,
        sort_order_id: None,
        record_count: num_records,
        file_size_in_bytes: size,
        content_stats: None,
        manifest_info: None,
        key_metadata: None,
        split_offsets: None,
        column_files: None,
        tags: None,
    }
}

/// A `DataManifest` entry referencing the leaf manifest at `location` with the given size and
/// tracking status.
pub(crate) fn data_manifest_entry(
    location: &str,
    size: i64,
    status: TrackingStatus,
) -> ContentTreeNodeEntry {
    let mut entry = added_data_entry(location, size, 0, 0, 1);
    entry.content_type = DataContentType::DataManifest;
    entry.tracking.status = status;
    entry.manifest_info = Some(ManifestInfo {
        format_version: AMT_FORMAT_VERSION,
        ..Default::default()
    });
    entry
}

/// Builds a content-tree entry batch from explicit entries, with the [`ContentTreeNodeEntry`]
/// schema (no `partition` or `content_stats` columns).
pub(crate) fn entry_batch(
    engine: &dyn Engine,
    entries: &[ContentTreeNodeEntry],
) -> Box<dyn EngineData> {
    let structs: Vec<StructData> = entries.iter().cloned().map(Into::into).collect();
    struct_batch(engine, &structs)
}

/// Builds a content-tree entry batch from already-encoded entry rows, so tests can inject
/// on-disk values the typed [`ContentTreeNodeEntry`] cannot express.
pub(crate) fn struct_batch(engine: &dyn Engine, entries: &[StructData]) -> Box<dyn EngineData> {
    let rows: Vec<Vec<Scalar>> = entries.iter().map(|s| s.values().to_vec()).collect();
    engine
        .evaluation_handler()
        .create_many(Arc::new(ContentTreeNodeEntry::to_schema()), rows)
        .unwrap()
}

/// Returns `data` with the (possibly nested) field at `path` replaced by `value`.
pub(crate) fn with_field(data: &StructData, path: &[&str], value: Scalar) -> StructData {
    let idx = data
        .fields()
        .iter()
        .position(|f| f.name() == path[0])
        .unwrap();
    let mut values = data.values().to_vec();
    values[idx] = match &path[1..] {
        [] => value,
        rest => match &values[idx] {
            Scalar::Struct(child) => Scalar::Struct(with_field(child, rest, value)),
            other => panic!("field '{}' is not a struct: {other:?}", path[0]),
        },
    };
    StructData::try_new(data.fields().to_vec(), values).unwrap()
}

/// Writes `entries` as a single-batch manifest parquet file at `location` and returns its
/// [`FileMeta`], whose size is the written file size. The manifest has the [`entry_batch`] schema.
pub(crate) fn write_manifest(
    engine: &dyn Engine,
    location: Url,
    entries: &[ContentTreeNodeEntry],
) -> FileMeta {
    let data = entry_batch(engine, entries);
    let size = engine
        .parquet_handler()
        .write_parquet_file(location.clone(), Box::new(iter::once(Ok(data))))
        .unwrap();
    FileMeta {
        location,
        last_modified: 0,
        size,
    }
}

mod tests {
    use std::slice;

    use super::*;
    use crate::arrow::array::RecordBatch;
    use crate::arrow::compute::{cast, concat_batches};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::{ColumnMetadataKey, DataType, MetadataValue, StructField, StructType};

    fn manifest_entries() -> Vec<ContentTreeNodeEntry> {
        vec![
            added_data_entry("a.parquet", 100, 10, 0, 5),
            added_data_entry("b.parquet", 200, 20, 10, 5),
            data_manifest_entry("leaf.parquet", 300, TrackingStatus::Existing),
        ]
    }

    /// Writes [`manifest_entries`] to an in-memory store and returns the engine and file.
    fn write_test_manifest() -> (SyncEngine, FileMeta) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let location = Url::parse("memory:///metadata/manifest.parquet").unwrap();
        let file = write_manifest(&engine, location, &manifest_entries());
        (engine, file)
    }

    /// Reads `file` back with `schema`, concatenating its batches.
    fn read_back(engine: &SyncEngine, file: &FileMeta, schema: StructType) -> RecordBatch {
        let batches: Vec<_> = engine
            .parquet_handler()
            .read_parquet_files(slice::from_ref(file), Arc::new(schema), None)
            .unwrap()
            .map(|batch| batch.unwrap().try_into_record_batch().unwrap())
            .collect();
        concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    /// `(field path, Parquet field id)` for every field of `schema`, descending into structs.
    fn field_ids(schema: &StructType, prefix: &str) -> Vec<(String, Option<MetadataValue>)> {
        schema
            .fields()
            .flat_map(|field| {
                let path = format!("{prefix}{}", field.name());
                let id = field
                    .get_config_value(&ColumnMetadataKey::ParquetFieldId)
                    .cloned();
                let mut ids = vec![(path.clone(), id)];
                if let DataType::Struct(child) = field.data_type() {
                    ids.extend(field_ids(child, &format!("{path}.")));
                }
                ids
            })
            .collect()
    }

    /// Renames every field of `schema`, recursing into structs, keeping each field's metadata (and
    /// so its field id).
    fn renamed(schema: &StructType) -> StructType {
        StructType::new_unchecked(schema.fields().map(|field| {
            let data_type = match field.data_type() {
                DataType::Struct(child) => DataType::from(renamed(child)),
                other => other.clone(),
            };
            StructField::new(
                format!("renamed_{}", field.name()),
                data_type,
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone())
        }))
    }

    #[test]
    fn write_manifest_round_trips_entries_with_field_ids() {
        let (engine, file) = write_test_manifest();

        let schema = ContentTreeNodeEntry::to_schema();
        let footer = engine.parquet_handler().read_parquet_footer(&file).unwrap();
        assert_eq!(field_ids(&footer.schema, ""), field_ids(&schema, ""));

        let read = read_back(&engine, &file, schema);
        let expected = entry_batch(&engine, &manifest_entries());
        assert_eq!(read, expected.try_into_record_batch().unwrap());
    }

    #[test]
    fn manifest_read_matches_columns_by_field_id() {
        let (engine, file) = write_test_manifest();

        let read = read_back(&engine, &file, renamed(&ContentTreeNodeEntry::to_schema()));
        let expected = entry_batch(&engine, &manifest_entries())
            .try_into_record_batch()
            .unwrap();
        // Compare values only: a by-id read drops nested list-element field-id metadata.
        for (actual, expected) in read.columns().iter().zip(expected.columns()) {
            assert_eq!(&cast(actual, expected.data_type()).unwrap(), expected);
        }
    }
}
