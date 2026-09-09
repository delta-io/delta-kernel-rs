//! Write-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates Delta file write-metadata into content-tree entry [`EngineData`] -- the columnar
//! form of an AMT root manifest. This is the minimal blind-append path: it produces `Data`
//! entries only, with deletion vectors, tags, and leaf-manifest information left null, and
//! statistics and partition values omitted from the schema.
//!
//! The translation is purely columnar: it builds a single expression that maps a write-metadata
//! batch to the entry schema and evaluates it, without materializing any [`ContentTreeNodeEntry`]
//! Rust values.

use std::sync::{Arc, LazyLock};

use crate::actions::NUM_RECORDS;
use crate::content_tree::{
    ContentTreeNodeEntry, DataContentType, DataFileFormat, TrackingInfo, TrackingStatus,
    CONTENT_TYPE, FILE_FORMAT, FILE_SEQUENCE_NUMBER, FILE_SIZE_IN_BYTES, FIRST_ROW_ID,
    FORMAT_VERSION, LOCATION, PARTITION_SPEC_ID, RECORD_COUNT, SEQUENCE_NUMBER, TRACKING,
    TRACKING_SNAPSHOT_ID, TRACKING_STATUS,
};
use crate::engine_data::{EngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, null_lit, ColumnName, Expression};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
    ToSchema as _,
};
use crate::transaction::{with_row_tracking_cols, BASE_ADD_FILES_SCHEMA};
use crate::{DeltaResult, Engine, Error};

/// The add-file write-metadata `stats` column name. Unlike `path`/`size`/`baseRowId`/
/// `defaultRowCommitVersion`, `stats` has no shared name constant, so it is defined here.
const STATS_NAME: &str = "stats";

/// The AMT/Iceberg adaptive-metadata format version stamped onto each written entry: Iceberg
/// format version 4 (the "V4 adaptive metadata tree").
const AMT_FORMAT_VERSION: i32 = 4;

/// The write-metadata leaf columns this path requires to be non-null on every row, in leaf order:
/// `stats.numRecords`, `baseRowId`, `defaultRowCommitVersion`. See [`RequiredFieldsNonNull`].
static REQUIRED_NON_NULL_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    StructType::new_unchecked([
        StructField::nullable(
            STATS_NAME,
            StructType::new_unchecked([StructField::nullable(NUM_RECORDS, DataType::LONG)]),
        ),
        StructField::nullable(BASE_ROW_ID_NAME, DataType::LONG),
        StructField::nullable(DEFAULT_ROW_COMMIT_VERSION_NAME, DataType::LONG),
    ])
    .leaves(None)
});

/// Translates a Delta file write-metadata batch into a content-tree entry [`EngineData`] batch
/// (one `Data` entry per input row), suitable for serializing as an AMT root manifest.
///
/// Each input row is a newly added file: the produced entry has content type `Data`, `fileFormat`
/// `parquet`, `specId` 0 (this path assumes an unpartitioned table), `formatVersion` 4, and
/// tracking status [`TrackingStatus::Added`]. `location`, `fileSizeInBytes`, `recordCount`, and
/// `firstRowId` come from the input `path`, `size`, `stats.numRecords`, and `baseRowId` columns
/// respectively. Both `sequenceNumber` and `fileSequenceNumber` come from the input
/// `defaultRowCommitVersion` column (the AMT data/file sequence number). `snapshotId` is set to
/// `snapshot_id`. All entry fields other than these are left null; statistics and partition values
/// are omitted from the output schema.
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
/// Returns an error if the input schema cannot be derived, if a row's required `stats.numRecords`,
/// `baseRowId`, or `defaultRowCommitVersion` is null, or if the evaluator cannot be constructed or
/// fails to evaluate.
pub(crate) fn write_metadata_to_entry_batch(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
) -> DeltaResult<Box<dyn EngineData>> {
    // Row tracking guarantees these are assigned, but the evaluator does not enforce the input
    // schema's non-nullability, so a missing assignment would otherwise emit a root entry with a
    // null sequence/firstRowId (invalid for a root manifest). Reject it up front.
    let mut validator = RequiredFieldsNonNull;
    validator.visit_rows_of(write_metadata)?;

    let output_schema = ContentTreeNodeEntry::to_schema();

    let projections = ContentTreeEntryProjections {
        status: TrackingStatus::Added,
        snapshot_id,
        // TODO(C1): AMT `location` (Iceberg field id 100) is expected to be the percent-decoded
        // data-file path, but `path` is the raw RFC-2396-encoded `AddFile.path`. A decode step is
        // needed once kernel has an expression-level percent-decode op.
        location: Expression::column([PATH_NAME]),
        file_size_in_bytes: Expression::column([SIZE_NAME]),
        sequence_number: Expression::column([DEFAULT_ROW_COMMIT_VERSION_NAME]),
        record_count: Expression::column([STATS_NAME, NUM_RECORDS]),
        first_row_id: Expression::column([BASE_ROW_ID_NAME]),
    };

    let expr = build_content_tree_entry_expression(&output_schema, &projections);
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        write_metadata_input_schema()?,
        Arc::new(expr),
        DataType::from(output_schema),
    )?;
    evaluator.evaluate(write_metadata)
}

// === Helpers ===

/// The write-metadata input schema consumed by [`write_metadata_to_entry_batch`]; see that
/// function's docs for field meanings. Derived from the canonical row-tracking-augmented add-file
/// schema (`Transaction::add_files_schema`) so its field identities and types cannot drift, then
/// narrowed to the fields this path reads: `path`, `size`, `stats.numRecords`, `baseRowId`, and
/// `defaultRowCommitVersion`.
fn write_metadata_input_schema() -> DeltaResult<SchemaRef> {
    let augmented = with_row_tracking_cols(&BASE_ADD_FILES_SCHEMA)?;
    let projected = augmented.project(&[
        PATH_NAME,
        SIZE_NAME,
        STATS_NAME,
        BASE_ROW_ID_NAME,
        DEFAULT_ROW_COMMIT_VERSION_NAME,
    ])?;
    let narrowed_stats = match projected.field(STATS_NAME).map(StructField::data_type) {
        Some(DataType::Struct(stats)) => stats.project_as_struct(&[NUM_RECORDS])?,
        _ => {
            return Err(Error::generic(
                "add-file write-metadata schema is missing the `stats` struct",
            ))
        }
    };
    let narrowed = SchemaStructPatchBuilder::new()
        .replace(
            STATS_NAME,
            StructField::nullable(STATS_NAME, narrowed_stats),
        )
        .build(&projected)?;
    Ok(Arc::new(narrowed))
}

/// Rejects any write-metadata row whose required row-tracking/statistic fields are null. The
/// evaluator ignores the input schema's non-nullability, so this is the only guard that upholds the
/// contract of [`write_metadata_to_entry_batch`].
struct RequiredFieldsNonNull;

impl RowVisitor for RequiredFieldsNonNull {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        REQUIRED_NON_NULL_COLUMNS.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // Column order matches REQUIRED_NON_NULL_COLUMNS: stats.numRecords, baseRowId,
        // defaultRowCommitVersion.
        for row in 0..row_count {
            require_non_null(getters[0], row, "stats.numRecords")?;
            require_non_null(getters[1], row, BASE_ROW_ID_NAME)?;
            require_non_null(getters[2], row, DEFAULT_ROW_COMMIT_VERSION_NAME)?;
        }
        Ok(())
    }
}

/// Errors if `getter` holds a null at `row`, naming `field` in the message.
fn require_non_null<'a>(getter: &'a dyn GetData<'a>, row: usize, field: &str) -> DeltaResult<()> {
    let value: Option<i64> = getter.get_opt(row, field)?;
    match value {
        Some(_) => Ok(()),
        None => Err(Error::missing_data(format!(
            "AMT content-tree write metadata has a null required field '{field}'"
        ))),
    }
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
        project(field.name().as_str()).unwrap_or_else(|| {
            // A missing projection must only ever fall back to null for a nullable field; a
            // required field with no projection would silently become a null of a non-nullable
            // type.
            debug_assert!(
                field.is_nullable(),
                "no projection for required field {}",
                field.name()
            );
            null_lit(field.data_type().clone())
        })
    }))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::{Scalar, StructData};
    use crate::Engine;

    /// A row of write-metadata input: `(path, size, numRecords, baseRowId,
    /// defaultRowCommitVersion)`, where any field may be null (to exercise null rejection).
    type InputRow = (
        Option<&'static str>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    );

    fn opt_long(value: Option<i64>) -> Scalar {
        value
            .map(Scalar::Long)
            .unwrap_or(Scalar::Null(DataType::LONG))
    }

    /// Builds a write-metadata input batch from fully-populated `(path, size, num_records,
    /// base_row_id, commit_version)` tuples.
    fn write_metadata_input(
        engine: &dyn Engine,
        files: &[(&'static str, i64, i64, i64, i64)],
    ) -> Box<dyn EngineData> {
        let rows: Vec<InputRow> = files
            .iter()
            .map(|&(p, s, n, b, c)| (Some(p), Some(s), Some(n), Some(b), Some(c)))
            .collect();
        write_metadata_input_nullable(engine, &rows)
    }

    /// Builds a write-metadata input batch where any field may be null.
    fn write_metadata_input_nullable(
        engine: &dyn Engine,
        rows: &[InputRow],
    ) -> Box<dyn EngineData> {
        let scalars: Vec<Vec<Scalar>> = rows
            .iter()
            .map(|&(path, size, num_records, base_row_id, commit_version)| {
                let stats = StructData::try_new(
                    vec![StructField::nullable(NUM_RECORDS, DataType::LONG)],
                    vec![opt_long(num_records)],
                )
                .unwrap();
                vec![
                    path.map(Scalar::from)
                        .unwrap_or(Scalar::Null(DataType::STRING)),
                    opt_long(size),
                    Scalar::Struct(stats),
                    opt_long(base_row_id),
                    opt_long(commit_version),
                ]
            })
            .collect();
        let row_refs: Vec<&[Scalar]> = scalars.iter().map(Vec::as_slice).collect();
        engine
            .evaluation_handler()
            .create_many(write_metadata_input_schema().unwrap(), &row_refs)
            .unwrap()
    }

    /// Builds the expected content-tree entry batch for `files`, so tests assert the whole
    /// transform output by equality rather than inspecting individual Arrow columns. Values and
    /// typed nulls are derived from `ContentTreeNodeEntry::to_schema`, so this tracks the schema.
    fn expected_entries(
        engine: &dyn Engine,
        files: &[(&'static str, i64, i64, i64, i64)],
        snapshot_id: i64,
    ) -> Box<dyn EngineData> {
        let output_schema = Arc::new(ContentTreeNodeEntry::to_schema());
        let rows: Vec<Vec<Scalar>> = files
            .iter()
            .map(|&(path, size, num_records, base_row_id, commit_version)| {
                entry_row(
                    &output_schema,
                    path,
                    size,
                    num_records,
                    base_row_id,
                    commit_version,
                    snapshot_id,
                )
            })
            .collect();
        let row_refs: Vec<&[Scalar]> = rows.iter().map(Vec::as_slice).collect();
        engine
            .evaluation_handler()
            .create_many(output_schema, &row_refs)
            .unwrap()
    }

    /// One expected entry row as scalars, in `schema` field order; unmatched fields are typed
    /// nulls.
    fn entry_row(
        schema: &StructType,
        path: &str,
        size: i64,
        num_records: i64,
        base_row_id: i64,
        commit_version: i64,
        snapshot_id: i64,
    ) -> Vec<Scalar> {
        let tracking_schema = TrackingInfo::to_schema();
        let tracking_values = tracking_schema
            .fields()
            .map(|f| match f.name().as_str() {
                TRACKING_STATUS => Scalar::from(TrackingStatus::Added),
                TRACKING_SNAPSHOT_ID => Scalar::Long(snapshot_id),
                SEQUENCE_NUMBER | FILE_SEQUENCE_NUMBER => Scalar::Long(commit_version),
                FIRST_ROW_ID => Scalar::Long(base_row_id),
                _ => Scalar::Null(f.data_type().clone()),
            })
            .collect();
        let tracking = Scalar::Struct(
            StructData::try_new(tracking_schema.fields().cloned().collect(), tracking_values)
                .unwrap(),
        );

        schema
            .fields()
            .map(|f| match f.name().as_str() {
                CONTENT_TYPE => Scalar::from(DataContentType::Data),
                LOCATION => Scalar::from(path),
                FILE_FORMAT => Scalar::from(DataFileFormat::Parquet),
                TRACKING => tracking.clone(),
                PARTITION_SPEC_ID => Scalar::Integer(0),
                RECORD_COUNT => Scalar::Long(num_records),
                FILE_SIZE_IN_BYTES => Scalar::Long(size),
                FORMAT_VERSION => Scalar::Integer(AMT_FORMAT_VERSION),
                _ => Scalar::Null(f.data_type().clone()),
            })
            .collect()
    }

    #[test]
    fn write_metadata_to_entry_batch_produces_added_data_entries() {
        let engine = SyncEngine::new();
        // (path, size, numRecords, baseRowId, defaultRowCommitVersion)
        let files = [("a.parquet", 100, 10, 0, 5), ("b.parquet", 200, 20, 10, 5)];
        let snapshot_id = 42;

        let out = write_metadata_to_entry_batch(
            &engine,
            write_metadata_input(&engine, &files).as_ref(),
            snapshot_id,
        )
        .unwrap();
        let expected = expected_entries(&engine, &files, snapshot_id);

        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    #[case::spaced("a b.parquet")]
    #[case::percent_encoded("a%20b.parquet")]
    fn write_metadata_to_entry_batch_location_is_verbatim(#[case] path: &'static str) {
        // Pins the current (deferred-decode) contract: `location` carries the raw path byte-for-
        // byte. When AMT `location` decoding lands (C1), the expected value here changes.
        let engine = SyncEngine::new();
        let files = [(path, 1, 1, 0, 0)];
        let out = write_metadata_to_entry_batch(
            &engine,
            write_metadata_input(&engine, &files).as_ref(),
            0,
        )
        .unwrap();
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected_entries(&engine, &files, 0)
                .try_into_record_batch()
                .unwrap()
        );
    }

    #[rstest]
    #[case::num_records((Some("a.parquet"), Some(1), None, Some(0), Some(0)), "stats.numRecords")]
    #[case::base_row_id((Some("a.parquet"), Some(1), Some(1), None, Some(0)), BASE_ROW_ID_NAME)]
    #[case::commit_version((Some("a.parquet"), Some(1), Some(1), Some(0), None), DEFAULT_ROW_COMMIT_VERSION_NAME)]
    fn write_metadata_to_entry_batch_rejects_null_required_field(
        #[case] row: InputRow,
        #[case] field: &str,
    ) {
        let engine = SyncEngine::new();
        let input = write_metadata_input_nullable(&engine, &[row]);
        let err = write_metadata_to_entry_batch(&engine, input.as_ref(), 0)
            .err()
            .expect("null required field should be rejected");
        assert!(
            err.to_string().contains(field),
            "expected error to name {field:?}, got: {err}"
        );
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
