//! Write-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates Delta file write-metadata into content-tree entry [`EngineData`] -- the columnar
//! form of an AMT root manifest. This is the minimal blind-append path: it produces `Data`
//! entries only, with deletion vectors and tags left null and statistics, partition values, and
//! leaf-manifest information omitted from the schema.
//!
//! The translation is purely columnar: it builds a single expression that maps a write-metadata
//! batch to the entry schema and evaluates it, without materializing any [`ContentTreeNodeEntry`]
//! Rust values.

use std::sync::Arc;

use crate::actions::NUM_RECORDS;
use crate::content_tree::{
    ContentTreeNodeEntry, DataContentType, DataFileFormat, TrackingInfo, TrackingStatus,
    CONTENT_TYPE, FILE_FORMAT, FILE_SEQUENCE_NUMBER, FILE_SIZE_IN_BYTES, FIRST_ROW_ID,
    FORMAT_VERSION, LOCATION, PARTITION_SPEC_ID, RECORD_COUNT, SEQUENCE_NUMBER, TRACKING,
    TRACKING_SNAPSHOT_ID, TRACKING_STATUS,
};
use crate::engine_data::EngineData;
use crate::expressions::{lit, Expression};
use crate::scan::log_replay::{BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME};
use crate::schema::{DataType, SchemaRef, StructField, StructType, ToSchema as _};
use crate::{DeltaResult, Engine};

/// Column names of the write-metadata input consumed by [`write_metadata_to_entry_batch`]. These
/// are a projection of the add-file write-metadata schema (`Transaction::add_files_schema`): the
/// `path`/`size` fields plus the nested `stats.numRecords` statistic that carries the file's row
/// count.
pub(crate) const WRITE_METADATA_PATH: &str = "path";
pub(crate) const WRITE_METADATA_SIZE: &str = "size";
pub(crate) const WRITE_METADATA_STATS: &str = "stats";

/// The AMT/Iceberg adaptive-metadata format version stamped onto each written entry: Iceberg
/// format version 4 (the "V4 adaptive metadata tree").
const AMT_FORMAT_VERSION: i32 = 4;

/// Translates a Delta file write-metadata batch into a content-tree entry [`EngineData`] batch
/// (one `Data` entry per input row), suitable for serializing as an AMT root manifest.
///
/// Each input row is a newly added file: the produced entry has content type `Data`, `fileFormat`
/// `parquet`, `specId` 0 (this path assumes an unpartitioned table), `formatVersion` 4, and
/// tracking status [`TrackingStatus::Added`]. `location`, `fileSizeInBytes`, `recordCount`, and
/// `firstRowId` come from the input `path`, `size`, `stats.numRecords`, and `baseRowId` columns
/// respectively. Both `sequenceNumber` and `fileSequenceNumber` come from the input
/// `defaultRowCommitVersion` column (the AMT data/file sequence number). `snapshotId` is set to
/// `snapshot_id`. Deletion vectors and tags are left null, and statistics and partition values are
/// omitted from the output schema.
///
/// This path is only valid for AMT tables, which always have row tracking enabled, so every input
/// row carries an assigned `baseRowId` and `defaultRowCommitVersion`.
///
/// # Parameters
/// - `engine`: provides the [`crate::EvaluationHandler`] used to evaluate the transform.
/// - `write_metadata`: input batch with the schema `{path: string, size: long, stats: {numRecords:
///   long}, baseRowId: long, defaultRowCommitVersion: long}` -- a projection of the row-tracking-
///   augmented add-file write-metadata schema.
/// - `snapshot_id`: the AMT snapshot id the files are added in; stored in each entry's tracking.
///
/// # Returns
/// An [`EngineData`] batch matching [`ContentTreeNodeEntry::to_schema`].
///
/// # Errors
/// Returns an error if the evaluator cannot be constructed or the transform fails to evaluate
/// (including when a row's required `stats.numRecords`, `baseRowId`, or `defaultRowCommitVersion`
/// is null).
pub(crate) fn write_metadata_to_entry_batch(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
) -> DeltaResult<Box<dyn EngineData>> {
    let output_schema = ContentTreeNodeEntry::to_schema();

    let projections = ContentTreeEntryProjections {
        status: TrackingStatus::Added,
        snapshot_id,
        location: Expression::column([WRITE_METADATA_PATH]),
        file_size_in_bytes: Expression::column([WRITE_METADATA_SIZE]),
        sequence_number: Expression::column([DEFAULT_ROW_COMMIT_VERSION_NAME]),
        record_count: Expression::column([WRITE_METADATA_STATS, NUM_RECORDS]),
        first_row_id: Expression::column([BASE_ROW_ID_NAME]),
    };

    let expr = build_content_tree_entry_expression(&output_schema, &projections);
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        write_metadata_input_schema(),
        Arc::new(expr),
        DataType::from(output_schema),
    )?;
    evaluator.evaluate(write_metadata)
}

// === Helpers ===

/// The write-metadata input schema consumed by [`write_metadata_to_entry_batch`]:
/// `{path: string, size: long, stats: {numRecords: long}, baseRowId: long,
/// defaultRowCommitVersion: long}`. All fields are required (not null): AMT tables always have row
/// tracking enabled, so `baseRowId`/`defaultRowCommitVersion` are always assigned, and each entry
/// carries the file's physical row count -- a null in any of these fails evaluation.
fn write_metadata_input_schema() -> SchemaRef {
    Arc::new(StructType::new_unchecked([
        StructField::not_null(WRITE_METADATA_PATH, DataType::STRING),
        StructField::not_null(WRITE_METADATA_SIZE, DataType::LONG),
        StructField::not_null(
            WRITE_METADATA_STATS,
            StructType::new_unchecked([StructField::not_null(NUM_RECORDS, DataType::LONG)]),
        ),
        StructField::not_null(BASE_ROW_ID_NAME, DataType::LONG),
        StructField::not_null(DEFAULT_ROW_COMMIT_VERSION_NAME, DataType::LONG),
    ]))
}

/// Per-field expressions driving [`build_content_tree_entry_expression`].
struct ContentTreeEntryProjections {
    status: TrackingStatus,
    snapshot_id: i64,
    location: Expression,
    file_size_in_bytes: Expression,
    /// The `defaultRowCommitVersion` of the added files, used for both `sequenceNumber` and
    /// `fileSequenceNumber`.
    sequence_number: Expression,
    record_count: Expression,
    /// The `baseRowId` of the added files, used for `firstRowId`.
    first_row_id: Expression,
}

/// Builds the expression mapping a write-metadata row to a [`ContentTreeNodeEntry`]-shaped struct.
fn build_content_tree_entry_expression(
    output_schema: &StructType,
    projections: &ContentTreeEntryProjections,
) -> Expression {
    struct_expr_from_schema(output_schema, |name| match name {
        CONTENT_TYPE => Some(lit(DataContentType::Data)),
        LOCATION => Some(projections.location.clone()),
        FILE_FORMAT => Some(lit(DataFileFormat::Parquet)),
        TRACKING => Some(build_tracking_expression(projections)),
        PARTITION_SPEC_ID => Some(lit(0i32)),
        RECORD_COUNT => Some(projections.record_count.clone()),
        FILE_SIZE_IN_BYTES => Some(projections.file_size_in_bytes.clone()),
        FORMAT_VERSION => Some(lit(AMT_FORMAT_VERSION)),
        _ => None,
    })
}

/// Builds the `tracking` sub-struct for an added `Data` entry.
fn build_tracking_expression(projections: &ContentTreeEntryProjections) -> Expression {
    struct_expr_from_schema(&TrackingInfo::to_schema(), |name| match name {
        TRACKING_STATUS => Some(lit(projections.status)),
        TRACKING_SNAPSHOT_ID => Some(lit(projections.snapshot_id)),
        SEQUENCE_NUMBER | FILE_SEQUENCE_NUMBER => Some(projections.sequence_number.clone()),
        FIRST_ROW_ID => Some(projections.first_row_id.clone()),
        _ => None,
    })
}

/// Builds a struct expression matching `schema` field-for-field. `project` supplies the expression
/// for a named field; unmatched fields (those returning `None`) become typed null literals, so the
/// result matches the schema in field order and type.
fn struct_expr_from_schema(
    schema: &StructType,
    project: impl Fn(&str) -> Option<Expression>,
) -> Expression {
    Expression::struct_from(schema.fields().map(|field| {
        project(field.name().as_str())
            .unwrap_or_else(|| Expression::null_literal(field.data_type().clone()))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::{Array, Int32Array, Int64Array, StringArray, StructArray};
    use crate::content_tree::{DV_INFO, TAGS};
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{Scalar, StructData};
    use crate::Engine;

    /// Builds a write-metadata input batch (`{path, size, stats: {numRecords}, baseRowId,
    /// defaultRowCommitVersion}`) from `(path, size, num_records, base_row_id, commit_version)`
    /// tuples.
    fn write_metadata_input(
        engine: &dyn Engine,
        files: &[(&str, i64, i64, i64, i64)],
    ) -> Box<dyn EngineData> {
        let rows: Vec<Vec<Scalar>> = files
            .iter()
            .map(|(path, size, num_records, base_row_id, commit_version)| {
                let stats = StructData::try_new(
                    vec![StructField::not_null(NUM_RECORDS, DataType::LONG)],
                    vec![Scalar::Long(*num_records)],
                )
                .unwrap();
                vec![
                    Scalar::from(*path),
                    Scalar::from(*size),
                    Scalar::Struct(stats),
                    Scalar::Long(*base_row_id),
                    Scalar::Long(*commit_version),
                ]
            })
            .collect();
        let row_refs: Vec<&[Scalar]> = rows.iter().map(Vec::as_slice).collect();
        engine
            .evaluation_handler()
            .create_many(write_metadata_input_schema(), &row_refs)
            .unwrap()
    }

    fn int32<'a>(batch: &'a StructArray, name: &str) -> &'a Int32Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
    }

    fn int64<'a>(batch: &'a StructArray, name: &str) -> &'a Int64Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    }

    #[test]
    fn write_metadata_to_entry_batch_produces_added_data_entries() {
        let engine = SyncEngine::new();
        // (path, size, numRecords, baseRowId, defaultRowCommitVersion)
        let input = write_metadata_input(
            &engine,
            &[("a.parquet", 100, 10, 0, 5), ("b.parquet", 200, 20, 10, 5)],
        );
        let snapshot_id = 42;

        let out = write_metadata_to_entry_batch(&engine, input.as_ref(), snapshot_id)
            .unwrap()
            .try_into_record_batch()
            .unwrap();
        let entries = StructArray::from(out);

        assert_eq!(entries.len(), 2);
        assert_eq!(int32(&entries, CONTENT_TYPE).values(), &[0, 0]); // Data
        assert_eq!(int32(&entries, PARTITION_SPEC_ID).values(), &[0, 0]);
        assert_eq!(int64(&entries, FILE_SIZE_IN_BYTES).values(), &[100, 200]);
        assert_eq!(int64(&entries, RECORD_COUNT).values(), &[10, 20]);
        assert_eq!(int32(&entries, FORMAT_VERSION).values(), &[4, 4]);

        let location = entries
            .column_by_name(LOCATION)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(location.value(0), "a.parquet");
        assert_eq!(location.value(1), "b.parquet");

        let file_format = entries
            .column_by_name(FILE_FORMAT)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(file_format.value(0), "parquet");
        assert_eq!(file_format.value(1), "parquet");

        // Deferred entry fields are null for every row.
        assert_eq!(entries.column_by_name(DV_INFO).unwrap().null_count(), 2);
        assert_eq!(entries.column_by_name(TAGS).unwrap().null_count(), 2);

        let tracking = entries
            .column_by_name(TRACKING)
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(int32(tracking, TRACKING_STATUS).values(), &[1, 1]); // Added
                                                                        // sequenceNumber and fileSequenceNumber come from the defaultRowCommitVersion column.
        assert_eq!(int64(tracking, SEQUENCE_NUMBER).values(), &[5, 5]);
        assert_eq!(int64(tracking, FILE_SEQUENCE_NUMBER).values(), &[5, 5]);
        assert_eq!(int64(tracking, TRACKING_SNAPSHOT_ID).values(), &[42, 42]);
        // firstRowId comes from the baseRowId column.
        assert_eq!(int64(tracking, FIRST_ROW_ID).values(), &[0, 10]);
        // Every deferred tracking field is null for every row.
        for field in ["dvSnapshotId", "deletedPositions", "replacedPositions"] {
            assert_eq!(tracking.column_by_name(field).unwrap().null_count(), 2);
        }
    }

    #[test]
    fn write_metadata_output_schema_matches_entry_schema() {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[("a.parquet", 1, 1, 0, 0)]);
        let out = write_metadata_to_entry_batch(&engine, input.as_ref(), 0)
            .unwrap()
            .try_into_record_batch()
            .unwrap();

        let expected = (&ContentTreeNodeEntry::to_schema())
            .try_into_arrow()
            .unwrap();
        assert_eq!(out.schema().as_ref(), &expected);
    }

    #[test]
    fn write_metadata_to_entry_batch_empty_input_yields_empty_batch() {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[]);
        let out = write_metadata_to_entry_batch(&engine, input.as_ref(), 0).unwrap();
        assert_eq!(out.len(), 0);
    }
}
