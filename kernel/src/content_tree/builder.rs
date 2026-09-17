//! Write-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates Delta file write-metadata into content-tree entry [`EngineData`] -- the columnar
//! form of an AMT root or leaf manifest. This is the minimal blind-append path: it produces `Data`
//! entries only, with deletion vectors, tags, and leaf-manifest information left null, and
//! statistics and partition values omitted from the schema. A [`ManifestKind`] selects whether the
//! manifest-inherited tracking fields (`sequenceNumber`, `fileSequenceNumber`, `firstRowId`) are
//! written explicitly (root) or left null to be inherited/assigned from the parent manifest (leaf).

use std::sync::{Arc, LazyLock};

use crate::actions::NUM_RECORDS;
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, DataFileFormat, TrackingInfo,
    TrackingStatus, CONTENT_TYPE, FILE_FORMAT, FILE_SEQUENCE_NUMBER, FILE_SIZE_IN_BYTES,
    FIRST_ROW_ID, FORMAT_VERSION, LOCATION, PARTITION_SPEC_ID, RECORD_COUNT, SEQUENCE_NUMBER,
    TRACKING, TRACKING_SNAPSHOT_ID, TRACKING_STATUS,
};
use crate::engine_data::{EngineData, FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, ColumnName, Expression};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, PATH_NAME, SIZE_NAME, STATS_NAME,
};
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
    ToSchema as _,
};
use crate::transaction::{with_row_tracking_cols, BASE_ADD_FILES_SCHEMA};
use crate::{DeltaResult, Engine, Error};

/// The AMT/Iceberg adaptive-metadata format version stamped onto each written entry: Iceberg
/// format version 4 (the "V4 adaptive metadata tree").
const AMT_FORMAT_VERSION: i32 = 4;

/// Which AMT manifest level a batch of `Data` entries belongs to.
///
/// Governs the manifest-inherited tracking fields: a [`ManifestKind::Root`] entry writes
/// `sequenceNumber`, `fileSequenceNumber`, and `firstRowId` explicitly; a [`ManifestKind::Leaf`]
/// entry leaves them null so they are inherited (sequence numbers) or assigned (firstRowId) from
/// its parent manifest entry. `snapshotId` is written explicitly in both.
#[derive(Debug, Clone, Copy)]
enum ManifestKind {
    Root,
    Leaf,
}

/// The write-metadata input schema consumed by [`convert_append_metadata_to_entry_batch`]: the
/// canonical row-tracking-augmented add-file write-metadata schema (`BASE_ADD_FILES_SCHEMA`
/// extended with the row-tracking columns), projected to the columns this path reads and with
/// `stats` narrowed to `numRecords`. Derived from that single source of truth so it cannot drift.
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

/// The write-metadata leaf columns the ROOT path requires to be non-null on every row, in leaf
/// order: `stats.numRecords`, `baseRowId`, `defaultRowCommitVersion` (the root path writes
/// `firstRowId` from `baseRowId` and both sequence numbers from `defaultRowCommitVersion`). See
/// [`RequiredFieldsNonNullVisitor`]. Nullability is only used to name the selected leaves; the
/// non-null contract is enforced by the visitor, not this schema.
static ROOT_REQUIRED_NON_NULL_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    StructType::new_unchecked([
        StructField::not_null(
            STATS_NAME,
            StructType::new_unchecked([StructField::not_null(NUM_RECORDS, DataType::LONG)]),
        ),
        StructField::not_null(BASE_ROW_ID_NAME, DataType::LONG),
        StructField::not_null(DEFAULT_ROW_COMMIT_VERSION_NAME, DataType::LONG),
    ])
    .leaves(None)
});

/// The write-metadata leaf columns the LEAF path requires to be non-null on every row: only
/// `stats.numRecords`. `baseRowId` and `defaultRowCommitVersion` are unused because a leaf entry
/// leaves `firstRowId` and both sequence numbers null, to be inherited/assigned from its parent
/// manifest entry. See [`RequiredFieldsNonNullVisitor`].
static LEAF_REQUIRED_NON_NULL_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    StructType::new_unchecked([StructField::not_null(
        STATS_NAME,
        StructType::new_unchecked([StructField::not_null(NUM_RECORDS, DataType::LONG)]),
    )])
    .leaves(None)
});

/// Translates a Delta file write-metadata batch into ROOT-manifest content-tree `Data` entries
/// (one entry per input row).
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
/// A [`FilteredEngineData`] whose batch matches [`ContentTreeNodeEntry::to_schema`], with every row
/// selected (each input file produces exactly one live entry). The selection is carried explicitly
/// so this path is symmetric with the AMT read path ([`super::reader`]), which drops rows.
///
/// # Errors
/// Returns an error if a row's required `stats.numRecords`, `baseRowId`, or
/// `defaultRowCommitVersion` is null, or if the evaluator cannot be constructed or fails to
/// evaluate.
pub(crate) fn convert_append_metadata_to_entry_batch(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
) -> DeltaResult<FilteredEngineData> {
    convert_append_metadata_to_entry_batch_impl(
        engine,
        write_metadata,
        snapshot_id,
        ManifestKind::Root,
    )
}

/// Translates a Delta file write-metadata batch into LEAF-manifest content-tree `Data` entries
/// (one entry per input row).
///
/// Identical to [`convert_append_metadata_to_entry_batch`] except the manifest-inherited tracking
/// fields are left null: `sequenceNumber`/`fileSequenceNumber` are inherited from, and `firstRowId`
/// is assigned by, the parent manifest entry that references this leaf. `snapshotId` is still
/// written. Consequently only `stats.numRecords` is required non-null on each input row; the input
/// `baseRowId` and `defaultRowCommitVersion` columns are ignored.
///
/// See [`convert_append_metadata_to_entry_batch`] for the shared parameter, return, and other
/// field semantics.
pub(crate) fn convert_append_metadata_to_leaf_entry_batch(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
) -> DeltaResult<FilteredEngineData> {
    convert_append_metadata_to_entry_batch_impl(
        engine,
        write_metadata,
        snapshot_id,
        ManifestKind::Leaf,
    )
}

/// Shared body of the root and leaf append-to-entry paths; `kind` selects which
/// manifest-inherited tracking fields are written (see [`ManifestKind`]).
fn convert_append_metadata_to_entry_batch_impl(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
    kind: ManifestKind,
) -> DeltaResult<FilteredEngineData> {
    // Row tracking guarantees these are assigned, but the evaluator does not enforce the input
    // schema's non-nullability, so a missing assignment would otherwise emit an entry with a null
    // required field. Reject it up front. The required set depends on which fields `kind` writes.
    let required_columns = match kind {
        ManifestKind::Root => &*ROOT_REQUIRED_NON_NULL_COLUMNS,
        ManifestKind::Leaf => &*LEAF_REQUIRED_NON_NULL_COLUMNS,
    };
    let mut validator = RequiredFieldsNonNullVisitor {
        columns: required_columns,
    };
    validator.visit_rows_of(write_metadata)?;

    let output_schema = ContentTreeNodeEntry::to_schema();

    // Root writes the sequence numbers (from `defaultRowCommitVersion`) and `firstRowId` (from
    // `baseRowId`) explicitly; leaf leaves them null so they are inherited/assigned from the parent
    // manifest entry.
    let (sequence_number, first_row_id) = match kind {
        ManifestKind::Root => (
            Some(Expression::column([DEFAULT_ROW_COMMIT_VERSION_NAME])),
            Some(Expression::column([BASE_ROW_ID_NAME])),
        ),
        ManifestKind::Leaf => (None, None),
    };

    let projections = ContentTreeEntryProjections {
        status: TrackingStatus::Added,
        snapshot_id,
        // TODO(#3319): AMT `location` (Iceberg field id 100) is expected to be the
        // percent-decoded data-file path, but `path` is the raw RFC-2396-encoded `AddFile.path`.
        // A decode step is needed once kernel has an expression-level percent-decode op.
        location: Expression::column([PATH_NAME]),
        file_size_in_bytes: Expression::column([SIZE_NAME]),
        sequence_number,
        record_count: Expression::column([STATS_NAME, NUM_RECORDS]),
        first_row_id,
    };

    let expr = build_content_tree_entry_expression(&output_schema, &projections)?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        write_metadata_input_schema()?,
        Arc::new(expr),
        DataType::from(output_schema),
    )?;
    let entries = evaluator.evaluate(write_metadata)?;
    Ok(FilteredEngineData::with_all_rows_selected(entries))
}

// === Helpers ===

/// Rejects any write-metadata row whose required row-tracking/statistic fields (per the manifest
/// kind, via `columns`) are null.
struct RequiredFieldsNonNullVisitor {
    columns: &'static ColumnNamesAndTypes,
}

impl RowVisitor for RequiredFieldsNonNullVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        // Getters align with the selected column names; check each selected column on every row.
        let (names, _) = self.columns.as_ref();
        for (getter, name) in getters.iter().zip(names) {
            let name = name.to_string();
            for row in 0..row_count {
                require_non_null(*getter, row, &name)?;
            }
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
    /// `snapshotId`, written explicitly for both root and leaf entries.
    snapshot_id: i64,
    location: Expression,
    file_size_in_bytes: Expression,
    /// The `defaultRowCommitVersion` of the added files, used for both `sequenceNumber` and
    /// `fileSequenceNumber`. `None` (leaf) emits typed nulls so both are inherited from the parent
    /// manifest entry.
    sequence_number: Option<Expression>,
    record_count: Expression,
    /// The `baseRowId` of the added files, used for `firstRowId`. `None` (leaf) emits a typed null
    /// so `firstRowId` is assigned from the parent manifest entry.
    first_row_id: Option<Expression>,
}

/// Builds the expression mapping a write-metadata row to a [`ContentTreeNodeEntry`]-shaped struct.
fn build_content_tree_entry_expression(
    output_schema: &StructType,
    projections: &ContentTreeEntryProjections,
) -> DeltaResult<Expression> {
    let tracking = build_tracking_expression(projections)?;
    struct_expr_from_schema(output_schema, |name| match name {
        CONTENT_TYPE => Some(lit(DataContentType::Data)),
        LOCATION => Some(projections.location.clone()),
        FILE_FORMAT => Some(lit(DataFileFormat::Parquet)),
        TRACKING => Some(tracking.clone()),
        // TODO(#3320): `specId` 0 is only correct for unpartitioned tables. A partitioned table's
        // spec 0 is its real (non-empty) partition spec, so this must carry the table's actual
        // spec id once the write path supports partitioned AMT tables.
        PARTITION_SPEC_ID => Some(lit(0i32)),
        RECORD_COUNT => Some(projections.record_count.clone()),
        FILE_SIZE_IN_BYTES => Some(projections.file_size_in_bytes.clone()),
        FORMAT_VERSION => Some(lit(AMT_FORMAT_VERSION)),
        _ => None,
    })
}

/// Builds the `tracking` sub-struct for an added `Data` entry.
fn build_tracking_expression(projections: &ContentTreeEntryProjections) -> DeltaResult<Expression> {
    struct_expr_from_schema(&TrackingInfo::to_schema(), |name| match name {
        TRACKING_STATUS => Some(lit(projections.status)),
        TRACKING_SNAPSHOT_ID => Some(lit(projections.snapshot_id)),
        SEQUENCE_NUMBER | FILE_SEQUENCE_NUMBER => projections.sequence_number.clone(),
        FIRST_ROW_ID => projections.first_row_id.clone(),
        _ => None,
    })
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

    /// A row of write-metadata input, where any field may be null (to exercise null rejection).
    #[derive(Clone, Copy)]
    struct InputRow {
        path: Option<&'static str>,
        size: Option<i64>,
        num_records: Option<i64>,
        base_row_id: Option<i64>,
        commit_version: Option<i64>,
    }

    impl From<(&'static str, i64, i64, i64, i64)> for InputRow {
        fn from(
            (path, size, num_records, base_row_id, commit_version): (
                &'static str,
                i64,
                i64,
                i64,
                i64,
            ),
        ) -> Self {
            InputRow {
                path: Some(path),
                size: Some(size),
                num_records: Some(num_records),
                base_row_id: Some(base_row_id),
                commit_version: Some(commit_version),
            }
        }
    }

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
        let rows: Vec<InputRow> = files.iter().map(|&f| f.into()).collect();
        write_metadata_input_nullable(engine, &rows)
    }

    /// Builds a write-metadata input batch where any field may be null.
    fn write_metadata_input_nullable(
        engine: &dyn Engine,
        rows: &[InputRow],
    ) -> Box<dyn EngineData> {
        let scalars: Vec<Vec<Scalar>> = rows
            .iter()
            .map(
                |&InputRow {
                     path,
                     size,
                     num_records,
                     base_row_id,
                     commit_version,
                 }| {
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
                },
            )
            .collect();
        engine
            .evaluation_handler()
            .create_many(write_metadata_input_schema().unwrap(), scalars)
            .unwrap()
    }

    /// Dispatches to the root or leaf append-to-entry path per `kind`.
    fn convert(
        engine: &dyn Engine,
        write_metadata: &dyn EngineData,
        snapshot_id: i64,
        kind: ManifestKind,
    ) -> DeltaResult<FilteredEngineData> {
        match kind {
            ManifestKind::Root => {
                convert_append_metadata_to_entry_batch(engine, write_metadata, snapshot_id)
            }
            ManifestKind::Leaf => {
                convert_append_metadata_to_leaf_entry_batch(engine, write_metadata, snapshot_id)
            }
        }
    }

    /// Builds the expected content-tree entry batch for `files` from explicit
    /// [`ContentTreeNodeEntry`] values, for the given manifest `kind`.
    fn expected_entries(
        engine: &dyn Engine,
        files: &[(&'static str, i64, i64, i64, i64)],
        snapshot_id: i64,
        kind: ManifestKind,
    ) -> Box<dyn EngineData> {
        let entries: Vec<StructData> = files
            .iter()
            .map(|&(path, size, num_records, base_row_id, commit_version)| {
                expected_entry(
                    path,
                    size,
                    num_records,
                    base_row_id,
                    commit_version,
                    snapshot_id,
                    kind,
                )
                .into()
            })
            .collect();
        let rows: Vec<Vec<Scalar>> = entries.iter().map(|e| e.values().to_vec()).collect();
        engine
            .evaluation_handler()
            .create_many(Arc::new(ContentTreeNodeEntry::to_schema()), rows)
            .unwrap()
    }

    /// The expected `Added` `Data` entry for one input file. Root entries carry explicit sequence
    /// numbers (from `commit_version`) and `firstRowId` (from `base_row_id`); leaf entries leave
    /// those null so they are inherited/assigned from the parent manifest. `snapshotId` is set in
    /// both.
    fn expected_entry(
        path: &str,
        size: i64,
        num_records: i64,
        base_row_id: i64,
        commit_version: i64,
        snapshot_id: i64,
        kind: ManifestKind,
    ) -> ContentTreeNodeEntry {
        let (sequence_number, first_row_id) = match kind {
            ManifestKind::Root => (Some(commit_version), Some(base_row_id)),
            ManifestKind::Leaf => (None, None),
        };
        ContentTreeNodeEntry {
            content_type: DataContentType::Data,
            location: path.to_string(),
            file_format: DataFileFormat::Parquet,
            tracking: TrackingInfo {
                status: TrackingStatus::Added,
                snapshot_id: Some(snapshot_id),
                dv_snapshot_id: None,
                sequence_number,
                file_sequence_number: sequence_number,
                first_row_id,
                deleted_positions: None,
                replaced_positions: None,
            },
            deletion_vector: None,
            spec_id: 0,
            partition: None,
            sort_order_id: None,
            record_count: num_records,
            file_size_in_bytes: size,
            content_stats: None,
            manifest_info: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            format_version: AMT_FORMAT_VERSION,
            tags: None,
        }
    }

    /// Unwraps a [`FilteredEngineData`] to its batch, asserting the write path selected every row.
    fn all_selected_data(filtered: FilteredEngineData) -> Box<dyn EngineData> {
        let (data, selection) = filtered.into_parts();
        assert!(
            selection.iter().all(|&selected| selected),
            "write path must select every entry, got {selection:?}"
        );
        data
    }

    #[rstest]
    #[case::root(ManifestKind::Root)]
    #[case::leaf(ManifestKind::Leaf)]
    fn convert_append_metadata_to_entry_batch_produces_added_data_entries(
        #[case] kind: ManifestKind,
    ) {
        let engine = SyncEngine::new();
        // (path, size, numRecords, baseRowId, defaultRowCommitVersion)
        let files = [("a.parquet", 100, 10, 0, 5), ("b.parquet", 200, 20, 10, 5)];
        let snapshot_id = 42;

        let out = all_selected_data(
            convert(
                &engine,
                write_metadata_input(&engine, &files).as_ref(),
                snapshot_id,
                kind,
            )
            .unwrap(),
        );
        let expected = expected_entries(&engine, &files, snapshot_id, kind);

        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    fn convert_append_metadata_to_entry_batch_location_is_verbatim(
        #[values("a b.parquet", "a%20b.parquet")] path: &'static str,
        #[values(ManifestKind::Root, ManifestKind::Leaf)] kind: ManifestKind,
    ) {
        // Pins the current contract: `location` carries the raw path byte-for-byte.
        let engine = SyncEngine::new();
        let files = [(path, 1, 1, 0, 0)];
        let out = all_selected_data(
            convert(
                &engine,
                write_metadata_input(&engine, &files).as_ref(),
                0,
                kind,
            )
            .unwrap(),
        );
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected_entries(&engine, &files, 0, kind)
                .try_into_record_batch()
                .unwrap()
        );
    }

    // Root requires all three fields non-null; leaf requires only `stats.numRecords` and ignores a
    // null `baseRowId` / `defaultRowCommitVersion` (it leaves the fields they feed null).
    // `expected` is `Ok(())` for accepted rows or `Err(field)` naming the field the error must
    // mention.
    #[rstest]
    #[case::root_num_records(
        ManifestKind::Root,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: None, base_row_id: Some(0), commit_version: Some(0) },
        Err("stats.numRecords")
    )]
    #[case::root_base_row_id(
        ManifestKind::Root,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: Some(1), base_row_id: None, commit_version: Some(0) },
        Err(BASE_ROW_ID_NAME)
    )]
    #[case::root_commit_version(
        ManifestKind::Root,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: Some(1), base_row_id: Some(0), commit_version: None },
        Err(DEFAULT_ROW_COMMIT_VERSION_NAME)
    )]
    #[case::leaf_num_records(
        ManifestKind::Leaf,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: None, base_row_id: Some(0), commit_version: Some(0) },
        Err("stats.numRecords")
    )]
    #[case::leaf_null_base_row_id_accepted(
        ManifestKind::Leaf,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: Some(1), base_row_id: None, commit_version: None },
        Ok(())
    )]
    fn convert_append_metadata_to_entry_batch_required_field_validation(
        #[case] kind: ManifestKind,
        #[case] row: InputRow,
        #[case] expected: Result<(), &str>,
    ) {
        let engine = SyncEngine::new();
        let input = write_metadata_input_nullable(&engine, &[row]);
        let result = convert(&engine, input.as_ref(), 0, kind);
        match expected {
            Ok(()) => {
                result.expect("row with only unused fields null should be accepted");
            }
            Err(field) => {
                let err = result
                    .err()
                    .expect("null required field should be rejected");
                assert!(
                    err.to_string().contains(field),
                    "expected error to name {field:?}, got: {err}"
                );
            }
        }
    }

    #[rstest]
    #[case::root(ManifestKind::Root)]
    #[case::leaf(ManifestKind::Leaf)]
    fn write_metadata_output_schema_matches_entry_schema(#[case] kind: ManifestKind) {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[("a.parquet", 1, 1, 0, 0)]);
        let out = all_selected_data(convert(&engine, input.as_ref(), 0, kind).unwrap())
            .try_into_record_batch()
            .unwrap();

        let expected = (&ContentTreeNodeEntry::to_schema())
            .try_into_arrow()
            .unwrap();
        assert_eq!(out.schema().as_ref(), &expected);
    }

    #[rstest]
    #[case::root(ManifestKind::Root)]
    #[case::leaf(ManifestKind::Leaf)]
    fn convert_append_metadata_to_entry_batch_empty_input_yields_empty_batch(
        #[case] kind: ManifestKind,
    ) {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[]);
        let out = all_selected_data(convert(&engine, input.as_ref(), 0, kind).unwrap());
        assert_eq!(out.len(), 0);
    }
}
