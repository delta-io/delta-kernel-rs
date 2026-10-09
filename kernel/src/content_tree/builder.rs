//! Write-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates Delta file write-metadata into content-tree entry [`EngineData`] -- the columnar
//! form of an AMT manifest. This is the minimal blind-append path: it produces `Data` entries only,
//! leaving content statistics, partition values, deletion vectors, tags, and leaf-manifest
//! information null in the output. The narrowed input it reads carries only `stats.numRecords` and
//! no partition values.

use std::sync::{Arc, LazyLock};

use crate::actions::NUM_RECORDS;
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, DataFileFormat, TrackingInfo,
    TrackingStatus, CONTENT_TYPE, FILE_FORMAT, FILE_SEQUENCE_NUMBER, FILE_SIZE_IN_BYTES,
    FIRST_ROW_ID, FORMAT_VERSION, LOCATION, PARTITION_SPEC_ID, RECORD_COUNT, SEQUENCE_NUMBER,
    TRACKING, TRACKING_SNAPSHOT_ID, TRACKING_STATUS,
};
use crate::engine_data::{EngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, ColumnName, Expression};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, PATH_NAME, SIZE_NAME, STATS_NAME,
};
use crate::schema::{
    ColumnNamesAndTypes, DataType, SchemaRef, SchemaStructPatchBuilder, StructField, StructType,
    ToSchema as _,
};
use crate::transaction::augmented_write_metadata_schema;
use crate::{Engine, KernelError, KernelResult};

/// The Iceberg format version stamped onto each written entry: 4 (the V4 adaptive metadata tree).
const AMT_FORMAT_VERSION: i32 = 4;

/// The write-metadata input schema consumed by [`convert_append_metadata_to_root_entry_batch`]: the
/// [`augmented_write_metadata_schema`] projected to the columns this path reads and with `stats`
/// narrowed to `numRecords`. Derived from that single source so it cannot drift.
fn write_metadata_input_schema() -> KernelResult<SchemaRef> {
    let augmented = augmented_write_metadata_schema()?;
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
            return Err(KernelError::generic(
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
/// Each input row is a newly added file, producing an [`TrackingStatus::Added`] `Data` entry
/// (`fileFormat` parquet, `formatVersion` 4, `specId` 0 -- unpartitioned tables only). Both
/// `sequenceNumber` and `fileSequenceNumber` take the input `defaultRowCommitVersion`; remaining
/// entry fields (including content statistics and partition values) are left null in the output.
///
/// This path is only valid for AMT tables, which always have row tracking enabled, so every input
/// row carries an assigned `baseRowId` and `defaultRowCommitVersion`.
///
/// # Parameters
/// - `engine`: provides the [`crate::EvaluationHandler`] used to evaluate the transform.
/// - `write_metadata`: input batch matching [`write_metadata_input_schema`].
/// - `snapshot_id`: the AMT snapshot id the files are added in; stored in each entry's tracking.
/// - `partition_columns`: the table's partition columns; must be empty (partitioned tables are
///   rejected).
///
/// # Returns
/// An [`EngineData`] batch matching [`ContentTreeNodeEntry::to_schema`] (one `Data` entry per
/// input row).
///
/// # Errors
/// Returns an error if `partition_columns` is non-empty, if a row's required `stats.numRecords`,
/// `baseRowId`, or `defaultRowCommitVersion` is null, or if the evaluator cannot be constructed or
/// fails to evaluate.
pub(crate) fn convert_append_metadata_to_root_entry_batch(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
    partition_columns: &[String],
) -> KernelResult<Box<dyn EngineData>> {
    // TODO(#2866): This method doesn't allow for caching standard checks and expressions.
    // we should have some sort of struct wrapper to avoid reconstruction.

    // `specId` 0 below is only correct for an unpartitioned table (a partitioned table's spec 0 is
    // its real partition spec), so reject partitioned tables until this path supports them.
    if !partition_columns.is_empty() {
        return Err(KernelError::unsupported(format!(
            "AMT root writes do not yet support partitioned tables; partition columns: \
             {partition_columns:?}"
        )));
    }
    convert_append_metadata_to_entry_batch_impl(
        engine,
        write_metadata,
        snapshot_id,
        AppendEntrySpec::root(),
    )
}

/// Translates a Delta file write-metadata batch into LEAF-manifest content-tree `Data` entries
/// (one entry per input row).
///
/// Identical to [`convert_append_metadata_to_root_entry_batch`] except the manifest-inherited
/// tracking fields are left null: `sequenceNumber`/`fileSequenceNumber` are inherited from, and
/// `firstRowId` is assigned by, the parent manifest entry that references this leaf. `snapshotId`
/// is still written. Consequently only `stats.numRecords` is required non-null on each input row;
/// the input `baseRowId` and `defaultRowCommitVersion` columns are ignored.
///
/// See [`convert_append_metadata_to_root_entry_batch`] for the shared parameter, return, and other
/// field semantics.
pub(crate) fn convert_append_metadata_to_leaf_entry_batch(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
) -> KernelResult<Box<dyn EngineData>> {
    convert_append_metadata_to_entry_batch_impl(
        engine,
        write_metadata,
        snapshot_id,
        AppendEntrySpec::leaf(),
    )
}

/// Shared body of the root and leaf append-to-entry paths; `spec` supplies the per-manifest-level
/// differences (see [`AppendEntrySpec`]).
fn convert_append_metadata_to_entry_batch_impl(
    engine: &dyn Engine,
    write_metadata: &dyn EngineData,
    snapshot_id: i64,
    spec: AppendEntrySpec,
) -> KernelResult<Box<dyn EngineData>> {
    // Row tracking guarantees these are assigned, but the evaluator does not enforce the input
    // schema's non-nullability, so a missing assignment would otherwise emit an entry with a null
    // required field. Reject it up front.
    let mut validator = RequiredFieldsNonNullVisitor {
        columns: spec.required_non_null_columns,
    };
    validator.visit_rows_of(write_metadata)?;

    let output_schema = ContentTreeNodeEntry::to_schema();

    let projections = ContentTreeEntryProjections {
        status: TrackingStatus::Added,
        snapshot_id,
        // TODO(#3319): AMT `location` (Iceberg field id 100) is expected to be the
        // percent-decoded data-file path, but `path` is the raw RFC-2396-encoded `AddFile.path`.
        // A decode step is needed once kernel has an expression-level percent-decode op.
        location: Expression::column([PATH_NAME]),
        file_size_in_bytes: Expression::column([SIZE_NAME]),
        sequence_number: spec.sequence_number,
        record_count: Expression::column([STATS_NAME, NUM_RECORDS]),
        first_row_id: spec.first_row_id,
    };

    let expr = build_content_tree_entry_expression(&output_schema, &projections)?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        write_metadata_input_schema()?,
        Arc::new(expr),
        DataType::from(output_schema),
    )?;
    evaluator.evaluate(write_metadata)
}

// === Helpers ===

/// Per-manifest-level inputs for the append-to-entry path: which input columns must be non-null and
/// how the manifest-inherited tracking fields are filled. All other entry fields are identical
/// across levels.
struct AppendEntrySpec {
    /// Columns required non-null on every input row for this level.
    required_non_null_columns: &'static ColumnNamesAndTypes,
    /// Source for `sequenceNumber`/`fileSequenceNumber`; `None` emits null (inherited).
    sequence_number: Option<Expression>,
    /// Source for `firstRowId`; `None` emits null (assigned by the parent manifest).
    first_row_id: Option<Expression>,
}

impl AppendEntrySpec {
    /// Root manifest: sequence numbers (from `defaultRowCommitVersion`) and `firstRowId` (from
    /// `baseRowId`) are written explicitly.
    fn root() -> Self {
        Self {
            required_non_null_columns: &ROOT_REQUIRED_NON_NULL_COLUMNS,
            sequence_number: Some(Expression::column([DEFAULT_ROW_COMMIT_VERSION_NAME])),
            first_row_id: Some(Expression::column([BASE_ROW_ID_NAME])),
        }
    }

    /// Leaf manifest: sequence numbers and `firstRowId` are left null to be inherited/assigned from
    /// the parent manifest entry.
    fn leaf() -> Self {
        Self {
            required_non_null_columns: &LEAF_REQUIRED_NON_NULL_COLUMNS,
            sequence_number: None,
            first_row_id: None,
        }
    }
}

/// Rejects any write-metadata row whose required row-tracking/statistic fields (via `columns`) are
/// null.
struct RequiredFieldsNonNullVisitor {
    columns: &'static ColumnNamesAndTypes,
}

impl RowVisitor for RequiredFieldsNonNullVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        self.columns.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
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
fn require_non_null<'a>(getter: &'a dyn GetData<'a>, row: usize, field: &str) -> KernelResult<()> {
    let value: Option<i64> = getter.get_opt(row, field)?;
    match value {
        Some(_) => Ok(()),
        None => Err(KernelError::missing_data(format!(
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
) -> KernelResult<Expression> {
    let tracking = build_tracking_expression(projections)?;
    struct_expr_from_schema(output_schema, |name| match name {
        CONTENT_TYPE => Some(lit(DataContentType::Data)),
        LOCATION => Some(projections.location.clone()),
        FILE_FORMAT => Some(lit(DataFileFormat::Parquet)),
        TRACKING => Some(tracking.clone()),
        // `specId` 0 is safe here because the caller rejects partitioned tables. TODO(#3320): carry
        // the table's actual spec id once the write path supports partitioned AMT tables.
        PARTITION_SPEC_ID => Some(lit(0i32)),
        RECORD_COUNT => Some(projections.record_count.clone()),
        FILE_SIZE_IN_BYTES => Some(projections.file_size_in_bytes.clone()),
        FORMAT_VERSION => Some(lit(AMT_FORMAT_VERSION)),
        _ => None,
    })
}

/// Builds the `tracking` sub-struct for an added `Data` entry.
fn build_tracking_expression(
    projections: &ContentTreeEntryProjections,
) -> KernelResult<Expression> {
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

    /// An append-to-entry entry point under test.
    type ConvertFn = fn(&dyn Engine, &dyn EngineData, i64) -> KernelResult<Box<dyn EngineData>>;

    /// Root entry point adapted to [`ConvertFn`]: the root path is unpartitioned, so it takes no
    /// partition columns.
    fn convert_root(
        engine: &dyn Engine,
        write_metadata: &dyn EngineData,
        snapshot_id: i64,
    ) -> KernelResult<Box<dyn EngineData>> {
        convert_append_metadata_to_root_entry_batch(engine, write_metadata, snapshot_id, &[])
    }

    /// One append path under test, as data: the entry point plus the tracking versions it is
    /// expected to produce for an added file with a given `(base_row_id, commit_version)`. Root
    /// writes both explicitly; leaf leaves them null.
    #[derive(Clone, Copy)]
    struct EntryPath {
        convert: ConvertFn,
        expected_versions: fn(base_row_id: i64, commit_version: i64) -> (Option<i64>, Option<i64>),
    }

    const ROOT_PATH: EntryPath = EntryPath {
        convert: convert_root,
        expected_versions: |base_row_id, commit_version| (Some(commit_version), Some(base_row_id)),
    };

    const LEAF_PATH: EntryPath = EntryPath {
        convert: convert_append_metadata_to_leaf_entry_batch,
        expected_versions: |_, _| (None, None),
    };

    /// Builds the expected content-tree entry batch for `files` from explicit
    /// [`ContentTreeNodeEntry`] values, using `expected_versions` for the per-level tracking
    /// fields.
    fn expected_entries(
        engine: &dyn Engine,
        files: &[(&'static str, i64, i64, i64, i64)],
        snapshot_id: i64,
        expected_versions: fn(i64, i64) -> (Option<i64>, Option<i64>),
    ) -> Box<dyn EngineData> {
        let entries: Vec<StructData> = files
            .iter()
            .map(|&(path, size, num_records, base_row_id, commit_version)| {
                let (sequence_number, first_row_id) =
                    expected_versions(base_row_id, commit_version);
                expected_entry(
                    path,
                    size,
                    num_records,
                    snapshot_id,
                    sequence_number,
                    first_row_id,
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

    /// The expected `Added` `Data` entry for one input file, with the given tracking
    /// `sequence_number` (used for both `sequenceNumber` and `fileSequenceNumber`) and
    /// `first_row_id`. `snapshotId` is always set.
    fn expected_entry(
        path: &str,
        size: i64,
        num_records: i64,
        snapshot_id: i64,
        sequence_number: Option<i64>,
        first_row_id: Option<i64>,
    ) -> ContentTreeNodeEntry {
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
            spec_id: Some(0),
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

    #[rstest]
    #[case::root(ROOT_PATH)]
    #[case::leaf(LEAF_PATH)]
    fn convert_append_metadata_to_entry_batch_produces_added_data_entries(
        #[case] entry_path: EntryPath,
    ) {
        let engine = SyncEngine::new();
        // (path, size, numRecords, baseRowId, defaultRowCommitVersion)
        let files = [("a.parquet", 100, 10, 0, 5), ("b.parquet", 200, 20, 10, 5)];
        let snapshot_id = 42;

        let out = (entry_path.convert)(
            &engine,
            write_metadata_input(&engine, &files).as_ref(),
            snapshot_id,
        )
        .unwrap();
        let expected = expected_entries(&engine, &files, snapshot_id, entry_path.expected_versions);

        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    fn convert_append_metadata_to_entry_batch_location_is_verbatim(
        #[values("a b.parquet", "a%20b.parquet")] path: &'static str,
        #[values(ROOT_PATH, LEAF_PATH)] entry_path: EntryPath,
    ) {
        // Pins the current contract: `location` carries the raw path byte-for-byte.
        let engine = SyncEngine::new();
        let files = [(path, 1, 1, 0, 0)];
        let out = (entry_path.convert)(&engine, write_metadata_input(&engine, &files).as_ref(), 0)
            .unwrap();
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected_entries(&engine, &files, 0, entry_path.expected_versions)
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
        convert_root,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: None, base_row_id: Some(0), commit_version: Some(0) },
        Err("stats.numRecords")
    )]
    #[case::root_base_row_id(
        convert_root,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: Some(1), base_row_id: None, commit_version: Some(0) },
        Err(BASE_ROW_ID_NAME)
    )]
    #[case::root_commit_version(
        convert_root,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: Some(1), base_row_id: Some(0), commit_version: None },
        Err(DEFAULT_ROW_COMMIT_VERSION_NAME)
    )]
    #[case::leaf_num_records(
        convert_append_metadata_to_leaf_entry_batch,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: None, base_row_id: Some(0), commit_version: Some(0) },
        Err("stats.numRecords")
    )]
    #[case::leaf_null_base_row_id_accepted(
        convert_append_metadata_to_leaf_entry_batch,
        InputRow { path: Some("a.parquet"), size: Some(1), num_records: Some(1), base_row_id: None, commit_version: None },
        Ok(())
    )]
    fn convert_append_metadata_to_entry_batch_required_field_validation(
        #[case] convert: ConvertFn,
        #[case] row: InputRow,
        #[case] expected: Result<(), &str>,
    ) {
        let engine = SyncEngine::new();
        let input = write_metadata_input_nullable(&engine, &[row]);
        let result = convert(&engine, input.as_ref(), 0);
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

    #[test]
    fn convert_append_metadata_to_entry_batch_rejects_partitioned_table() {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[("a.parquet", 1, 1, 0, 0)]);
        let partition_columns = ["part_col".to_string()];
        let err = convert_append_metadata_to_root_entry_batch(
            &engine,
            input.as_ref(),
            0,
            &partition_columns,
        )
        .err()
        .expect("a partitioned table should be rejected");
        assert!(
            err.to_string().contains("part_col"),
            "expected error to name the partition column, got: {err}"
        );
    }

    #[rstest]
    #[case::root(ROOT_PATH)]
    #[case::leaf(LEAF_PATH)]
    fn write_metadata_output_schema_matches_entry_schema(#[case] entry_path: EntryPath) {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[("a.parquet", 1, 1, 0, 0)]);
        let out = (entry_path.convert)(&engine, input.as_ref(), 0)
            .unwrap()
            .try_into_record_batch()
            .unwrap();

        let expected = (&ContentTreeNodeEntry::to_schema())
            .try_into_arrow()
            .unwrap();
        assert_eq!(out.schema().as_ref(), &expected);
    }

    #[rstest]
    #[case::root(ROOT_PATH)]
    #[case::leaf(LEAF_PATH)]
    fn convert_append_metadata_to_entry_batch_empty_input_yields_empty_batch(
        #[case] entry_path: EntryPath,
    ) {
        let engine = SyncEngine::new();
        let input = write_metadata_input(&engine, &[]);
        let out = (entry_path.convert)(&engine, input.as_ref(), 0).unwrap();
        assert_eq!(out.len(), 0);
    }
}
