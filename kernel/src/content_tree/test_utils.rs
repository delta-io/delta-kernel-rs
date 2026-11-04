//! Test helpers for building AMT content-tree entries and writing them as manifest files.

use std::iter;
use std::sync::Arc;

use url::Url;

/// Encodes manifest deletion vector positions as a Mumbling bitmap.
pub(crate) use crate::content_tree::mumbling::tests::encode_positions as mumbling_bitmap;
use crate::content_tree::skipping::content_stats_read_field_for_predicate;
use crate::content_tree::stats::{field_with_id, stats_schema};
use crate::content_tree::{
    AMT_FORMAT_VERSION, CONTENT_STATS_FIELD_NAME, ContentTreeNodeEntry, DataContentType,
    DataFileFormat, DeletionVectorInfo, ManifestInfo, TrackingInfo, TrackingStatus,
};
use crate::expressions::{ArrayData, Scalar, StructData, col, lit};
use crate::schema::{ArrayType, DataType, StructField, StructType, ToSchema as _};
use crate::{Engine, EngineData, FileMeta};

/// A valid relative AMT deletion vector, convertible to a `DeletionVectorDescriptor`.
pub(crate) fn test_dv_info() -> DeletionVectorInfo {
    DeletionVectorInfo {
        location: "dv.bin".to_string(),
        offset: 1,
        size_in_bytes: 48,
        cardinality: 6,
        key_metadata: None,
    }
}

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
        tight_stats: None,
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
    write_batch(engine, location, entry_batch(engine, entries))
}

/// Writes `entries` like [`write_manifest`], followed by a `content_stats` column (described by
/// `content_stats`) holding `stats[i]` for entry `i`.
pub(crate) fn write_manifest_with_stats(
    engine: &dyn Engine,
    location: Url,
    entries: &[ContentTreeNodeEntry],
    content_stats: &StructField,
    stats: Vec<Scalar>,
) -> FileMeta {
    let column_type = ArrayType::new(content_stats.data_type().clone(), true);
    let column = ArrayData::try_new(column_type, stats).unwrap();
    let schema = Arc::new(StructType::new_unchecked([content_stats.clone()]));
    let data = entry_batch(engine, entries)
        .append_columns(schema, vec![column])
        .unwrap();
    write_batch(engine, location, data)
}

/// The full `content_stats` field of a table whose only column `x` (field ID 1) is a LONG, and
/// the read field of a skipping predicate on `x`'s max, which holds only `x`'s upper bound.
pub(crate) fn x_content_stats() -> (StructField, StructField) {
    let table = StructType::new_unchecked([field_with_id("x", DataType::LONG, true, 1)]);
    let stats = stats_schema(&table).unwrap();
    let skipping = col!("stats_parsed.maxValues.x").gt(lit(5i64));
    let read = content_stats_read_field_for_predicate(&table, &skipping)
        .unwrap()
        .unwrap();
    let full = field_with_id(CONTENT_STATS_FIELD_NAME, stats.into(), true, 146);
    (full, read)
}

/// A `content_stats` value of `field`'s type whose single leaf holds `values`, in field order.
pub(crate) fn x_stats(field: &StructField, values: &[i64]) -> Scalar {
    let DataType::Struct(stats) = field.data_type() else {
        panic!("content_stats must be a struct");
    };
    let leaf_field = stats.fields().next().unwrap();
    let DataType::Struct(leaf) = leaf_field.data_type() else {
        panic!("leaf stats must be a struct");
    };
    let leaf_values = values.iter().copied().map(Scalar::Long).collect();
    let leaf_stats = StructData::try_new(leaf.fields().cloned().collect(), leaf_values).unwrap();
    let fields = vec![leaf_field.clone()];
    Scalar::Struct(StructData::try_new(fields, vec![Scalar::Struct(leaf_stats)]).unwrap())
}

/// Writes `data` as a single-batch parquet file at `location` and returns its [`FileMeta`].
fn write_batch(engine: &dyn Engine, location: Url, data: Box<dyn EngineData>) -> FileMeta {
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
    use crate::arrow::array::{Array as _, RecordBatch};
    use crate::arrow::compute::{cast, concat_batches};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::schema::{ColumnMetadataKey, MetadataValue};

    /// Covers `tight_stats` as `Some(true)`, `Some(false)`, and `None`.
    fn manifest_entries() -> Vec<ContentTreeNodeEntry> {
        let mut tight = added_data_entry("a.parquet", 100, 10, 0, 5);
        tight.tight_stats = Some(true);
        let mut loose = added_data_entry("b.parquet", 200, 20, 10, 5);
        loose.tight_stats = Some(false);
        vec![
            tight,
            loose,
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

    /// The entry read schema followed by `content_stats`.
    fn with_content_stats(content_stats: &StructField) -> StructType {
        let schema = ContentTreeNodeEntry::to_schema();
        StructType::new_unchecked(schema.fields().cloned().chain([content_stats.clone()]))
    }

    /// Reading with a narrowed `content_stats` field reads only the requested stats, by field ID.
    #[test]
    fn manifest_read_projects_requested_content_stats() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let (full, read) = x_content_stats();
        let location = Url::parse("memory:///metadata/manifest.parquet").unwrap();
        let entries = [added_data_entry("a.parquet", 1, 10, 0, 1)];
        let stats = vec![x_stats(&full, &[1, 9, 10, 0])];
        let file = write_manifest_with_stats(&engine, location, &entries, &full, stats);

        let batch = read_back(&engine, &file, with_content_stats(&read));
        let expected = engine
            .evaluation_handler()
            .create_many(
                Arc::new(StructType::new_unchecked([read.clone()])),
                vec![vec![x_stats(&read, &[9])]],
            )
            .unwrap()
            .try_into_record_batch()
            .unwrap();
        assert_eq!(
            batch.column_by_name(CONTENT_STATS_FIELD_NAME).unwrap(),
            expected.column(0)
        );
    }

    #[test]
    fn manifest_read_of_missing_content_stats_is_null() {
        let (engine, file) = write_test_manifest();
        let (_, read) = x_content_stats();
        let batch = read_back(&engine, &file, with_content_stats(&read));
        let stats = batch.column_by_name(CONTENT_STATS_FIELD_NAME).unwrap();
        assert_eq!(stats.null_count(), batch.num_rows());
    }
}
