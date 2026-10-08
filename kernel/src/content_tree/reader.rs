//! Read-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates the content-tree entries of an AMT root manifest into `Add` file actions -- the
//! inverse of [`super::builder`]. This is the minimal read path: it surfaces live `Data` entries as
//! `Add` actions and drops everything else. Statistics, partition values, tags, and deletion
//! vectors are not yet carried across (see the per-field TODOs); the only entry data that flows
//! into the action is the file location, size, and row-tracking numbers. `modificationTime` and
//! `dataChange` have no AMT source and are supplied by the caller via [`ReadContext`].

use std::sync::{Arc, LazyLock};

use crate::actions::{ADD_NAME, ADD_SCHEMA, LOG_ADD_SCHEMA};
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, DataFileFormat, TrackingStatus,
    CONTENT_TYPE, DV_INFO, DV_SNAPSHOT_ID, FILE_FORMAT, FILE_SIZE_IN_BYTES, FIRST_ROW_ID, LOCATION,
    PARTITION_SPEC_ID, SEQUENCE_NUMBER, TRACKING, TRACKING_STATUS,
};
use crate::engine_data::{EngineData, FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, ColumnName, Expression, MapData, Scalar};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DATA_CHANGE_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, MODIFICATION_TIME_NAME,
    PARTITION_VALUES_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, StructField, ToSchema as _};
use crate::{Engine, KernelError, KernelResult};

/// Values the read path emits into every `Add` action but cannot derive from an AMT entry, so the
/// caller must supply them.
///
/// The AMT entry carries neither the data file's modification time nor a `dataChange` flag, so
/// without this context the reader would have to invent both. Supplying them here keeps the
/// function signature stable once a real source (e.g. the commit timestamp) is threaded through.
pub(crate) struct ReadContext {
    /// Emitted as `Add.modificationTime`.
    pub(crate) modification_time: i64,
    /// Emitted as `Add.dataChange`.
    pub(crate) data_change: bool,
}

/// Translates an AMT root manifest's content-tree entry batch into an `Add`-action batch, keeping
/// only the rows that read as live data files.
///
/// An entry becomes an `Add` when its `contentType` is [`DataContentType::Data`] and its tracking
/// status is [live](TrackingStatus::is_live); every other entry is dropped via the returned
/// selection vector. [`DataContentType::DataManifest`] entries are dropped too: expanding a leaf
/// manifest into its data files is the caller's responsibility.
///
/// # Parameters
/// - `engine`: provides the [`crate::EvaluationHandler`] used to evaluate the transform.
/// - `entries`: a content-tree entry batch matching [`ContentTreeNodeEntry::to_schema`] (the
///   columnar form produced by [`super::builder`]).
/// - `ctx`: the per-read values the entry cannot supply (see [`ReadContext`]).
///
/// # Returns
/// A [`FilteredEngineData`] over an `Add`-action batch (one row per input entry, schema
/// [`crate::actions::LOG_ADD_SCHEMA`]), whose selection vector keeps only the entries that read as
/// live data files. The selection is returned rather than applied so the caller can fold it into
/// its own selection vector.
///
/// # Errors
/// Returns an error if an entry carries an unknown content-type or tracking-status value, if a
/// delete entry (position/equality deletes or a delete manifest) is live, if a selected (live
/// `Data`) entry is not Parquet, has a non-zero partition spec id, or carries a deletion vector
/// (none yet supported by the read path), if the evaluator cannot be constructed or fails to
/// evaluate, or if the selection vector length exceeds the batch.
pub(crate) fn convert_root_entries_to_add_actions(
    engine: &dyn Engine,
    entries: &dyn EngineData,
    ctx: &ReadContext,
) -> KernelResult<FilteredEngineData> {
    // TODO(#2866): cache the expression/evaluator so repeated calls don't rebuild them.
    let mut selector = AddSelectionVisitor::default();
    selector.visit_rows_of(entries)?;

    let input_schema = Arc::new(ContentTreeNodeEntry::to_schema());
    let output_type = DataType::from(LOG_ADD_SCHEMA.as_ref().clone());
    let expr = build_entry_to_add_expression(ctx)?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        input_schema,
        Arc::new(expr),
        output_type,
    )?;
    let actions = evaluator.evaluate(entries)?;
    FilteredEngineData::try_new(actions, selector.selection)
}

// === Helpers ===

/// Builds the transform mapping a [`ContentTreeNodeEntry`] row to a `{ add: Add }` struct matching
/// [`crate::actions::LOG_ADD_SCHEMA`].
///
/// `modificationTime` and `dataChange` have no AMT source and are taken from `ctx`; nullable fields
/// not listed here fall through to a typed null via [`struct_expr_from_schema`].
fn build_entry_to_add_expression(ctx: &ReadContext) -> KernelResult<Expression> {
    // TODO(#3320): read partition values from the entry's `partition` tuple once the read path
    // carries a partition spec; the AMT root written by the minimal blind-append path is
    // unpartitioned. The map type is taken from the action schema so its value-nullability
    // matches `Add.partitionValues`.
    let empty_partition_values = lit(Scalar::Map(MapData::try_new(
        partition_values_map_type()?,
        Vec::<(Scalar, Scalar)>::new(),
    )?));

    // `struct_expr_from_schema` fills every unmatched (nullable) field with a typed null, so
    // `stats`, `tags`, `deletionVector`, and `clusteringProvider` fall through to null until the
    // read path carries statistics, tags, and inline deletion-vector info across from the entry.
    let add = struct_expr_from_schema(&ADD_SCHEMA, |name| {
        Some(match name {
            // TODO(#3319): `location` is the raw path stored in the AMT root; resolving it to a
            // table-relative `Add.path` (percent-decoding, and relativizing against a manifest
            // location for non-root entries) is not yet done -- it flows through verbatim, which
            // round-trips only the minimal-root case that stored the raw path.
            PATH_NAME => Expression::column([LOCATION]),
            // TODO(#3320): read partition values from the entry's `partition` tuple once the read
            // path carries a partition spec.
            PARTITION_VALUES_NAME => empty_partition_values.clone(),
            SIZE_NAME => Expression::column([FILE_SIZE_IN_BYTES]),
            // The AMT entry carries neither of these; both come from the caller's `ReadContext`.
            MODIFICATION_TIME_NAME => lit(ctx.modification_time),
            DATA_CHANGE_NAME => lit(ctx.data_change),
            BASE_ROW_ID_NAME => Expression::column([TRACKING, FIRST_ROW_ID]),
            DEFAULT_ROW_COMMIT_VERSION_NAME => Expression::column([TRACKING, SEQUENCE_NUMBER]),
            _ => return None,
        })
    })?;

    // LOG_ADD_SCHEMA is a single `add` field wrapping the action struct.
    struct_expr_from_schema(&LOG_ADD_SCHEMA, |name| match name {
        ADD_NAME => Some(add.clone()),
        _ => None,
    })
}

/// The [`MapType`] of `Add.partitionValues`, read from the action schema so callers match its
/// declared value-nullability.
fn partition_values_map_type() -> KernelResult<MapType> {
    match ADD_SCHEMA
        .field(PARTITION_VALUES_NAME)
        .map(StructField::data_type)
    {
        Some(DataType::Map(map)) => Ok(map.as_ref().clone()),
        other => Err(KernelError::generic(format!(
            "Add schema `{PARTITION_VALUES_NAME}` field is not a map: {other:?}"
        ))),
    }
}

/// Builds the selection vector picking the entries that become `Add` actions: a live
/// ([`TrackingStatus::is_live`]) [`DataContentType::Data`] entry is selected; any other entry is
/// not.
///
/// Fails closed on entries the read path cannot yet represent as an `Add`, rather than silently
/// dropping or mis-translating them:
/// - a live delete entry (position/equality deletes or a delete manifest), since dropping it would
///   read the rows it deletes back as live;
/// - a selected entry that is not Parquet or has a non-zero partition spec id;
/// - a selected entry that carries a deletion vector: the read path does not yet populate
///   `Add.deletionVector`, so a DV-less `Add` would read its logically deleted rows back as live.
#[derive(Default)]
struct AddSelectionVisitor {
    selection: Vec<bool>,
}

impl RowVisitor for AddSelectionVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    ColumnName::new([CONTENT_TYPE]),
                    ColumnName::new([TRACKING, TRACKING_STATUS]),
                    ColumnName::new([LOCATION]),
                    ColumnName::new([FILE_FORMAT]),
                    ColumnName::new([PARTITION_SPEC_ID]),
                    // `deletionVector.location` is a required field of the DV sub-struct, so it
                    // reads null iff the `deletionVector` struct itself is null.
                    ColumnName::new([DV_INFO, LOCATION]),
                    ColumnName::new([TRACKING, DV_SNAPSHOT_ID]),
                ],
                vec![
                    DataType::INTEGER,
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::STRING,
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::LONG,
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        self.selection.reserve(row_count);
        for row in 0..row_count {
            let content_type = DataContentType::try_from_repr(getters[0].get(row, CONTENT_TYPE)?)?;
            let status = TrackingStatus::try_from_repr(getters[1].get(row, TRACKING_STATUS)?)?;
            let location: &str = getters[2].get(row, LOCATION)?;
            let selected = match content_type {
                DataContentType::Data => status.is_live(),
                DataContentType::DataManifest => false,
                DataContentType::PositionDeletes
                | DataContentType::EqualityDeletes
                | DataContentType::DeleteManifest => {
                    if status.is_live() {
                        return Err(KernelError::unsupported(format!(
                            "AMT content-tree read path does not yet support live \
                             {content_type:?} entry '{location}'"
                        )));
                    }
                    false
                }
            };
            if selected {
                check_selected_entry(row, location, getters)?;
            }
            self.selection.push(selected);
        }
        Ok(())
    }
}

/// Rejects a selected (live `Data`) entry carrying a field the read path cannot yet translate into
/// its `Add`. `getters` is [`AddSelectionVisitor`]'s column order.
fn check_selected_entry<'a>(
    row: usize,
    location: &str,
    getters: &[&'a dyn GetData<'a>],
) -> KernelResult<()> {
    let file_format: &str = getters[3].get(row, FILE_FORMAT)?;
    let parquet = DataFileFormat::Parquet.name();
    if !file_format.eq_ignore_ascii_case(parquet) {
        return Err(KernelError::unsupported(format!(
            "AMT content-tree read path only supports {parquet} data files; entry '{location}' \
             has file format '{file_format}'"
        )));
    }
    let spec_id: Option<i32> = getters[4].get_opt(row, PARTITION_SPEC_ID)?;
    if let Some(spec_id) = spec_id.filter(|&id| id != 0) {
        return Err(KernelError::unsupported(format!(
            "AMT content-tree read path does not yet support partition specs; entry \
             '{location}' has {PARTITION_SPEC_ID} {spec_id}"
        )));
    }
    let dv_location: Option<&str> = getters[5].get_opt(row, LOCATION)?;
    let dv_snapshot_id: Option<i64> = getters[6].get_opt(row, DV_SNAPSHOT_ID)?;
    if let Some(field) = dv_location
        .map(|_| DV_INFO)
        .or_else(|| dv_snapshot_id.map(|_| DV_SNAPSHOT_ID))
    {
        return Err(KernelError::unsupported(format!(
            "AMT content-tree read path does not yet support a live entry with a deletion vector \
             (entry '{location}' has non-null '{field}')"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::content_tree::{DeletionVectorInfo, ManifestInfo, TrackingInfo};
    use crate::engine::arrow_conversion::TryIntoArrow as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::StructData;
    use crate::unit_test_utils::assert_result_error_with_message;

    /// AMT/Iceberg format version stamped on entries; irrelevant to the `Add` output but required
    /// to build a well-formed [`ContentTreeNodeEntry`].
    const AMT_FORMAT_VERSION: i32 = 4;

    /// The [`ReadContext`] the tests read against; its values match [`expected_add_row`]'s
    /// `modificationTime` (`i64::MAX`) and `dataChange` (`true`).
    fn read_ctx() -> ReadContext {
        ReadContext {
            modification_time: i64::MAX,
            data_change: true,
        }
    }

    /// A minimal-root `Data`/`Added` entry with the given path, size, and row-tracking numbers.
    fn added_data_entry(
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
                dv_snapshot_id: None,
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
            equality_ids: None,
            format_version: AMT_FORMAT_VERSION,
            tags: None,
        }
    }

    /// Builds a content-tree entry batch (input to the read path) from explicit entries.
    fn entry_batch(engine: &dyn Engine, entries: &[ContentTreeNodeEntry]) -> Box<dyn EngineData> {
        let structs: Vec<StructData> = entries.iter().cloned().map(Into::into).collect();
        struct_batch(engine, &structs)
    }

    /// Builds a content-tree entry batch from already-encoded entry rows, so tests can inject
    /// on-disk values the typed [`ContentTreeNodeEntry`] cannot express.
    fn struct_batch(engine: &dyn Engine, entries: &[StructData]) -> Box<dyn EngineData> {
        let rows: Vec<Vec<Scalar>> = entries.iter().map(|s| s.values().to_vec()).collect();
        engine
            .evaluation_handler()
            .create_many(Arc::new(ContentTreeNodeEntry::to_schema()), rows)
            .unwrap()
    }

    /// Returns `data` with the (possibly nested) field at `path` replaced by `value`.
    fn with_field(data: &StructData, path: &[&str], value: Scalar) -> StructData {
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

    /// Runs the read path over `entries`.
    fn convert(entries: &[ContentTreeNodeEntry]) -> KernelResult<FilteredEngineData> {
        let engine = SyncEngine::new();
        convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, entries).as_ref(),
            &read_ctx(),
        )
    }

    /// Runs the read path over `entries` and returns its selection vector.
    fn selection_of(entries: &[ContentTreeNodeEntry]) -> Vec<bool> {
        convert(entries).unwrap().selection_vector().to_vec()
    }

    /// Runs the read path over a single already-encoded entry row.
    fn convert_struct(entry: StructData) -> KernelResult<FilteredEngineData> {
        let engine = SyncEngine::new();
        convert_root_entries_to_add_actions(
            &engine,
            struct_batch(&engine, &[entry]).as_ref(),
            &read_ctx(),
        )
    }

    /// A deletion vector descriptor for tests that attach one to an entry.
    fn test_dv() -> DeletionVectorInfo {
        DeletionVectorInfo {
            location: "dv.bin".to_string(),
            offset: 0,
            size_in_bytes: 1,
            cardinality: 1,
        }
    }

    /// The expected `{ add: Add }` row for one surviving entry, mirroring
    /// [`build_entry_to_add_expression`].
    fn expected_add_row(
        path: &str,
        size: i64,
        base_row_id: i64,
        commit_version: i64,
    ) -> Vec<Scalar> {
        let add = StructData::try_new(
            ADD_SCHEMA.fields().cloned().collect(),
            vec![
                Scalar::from(path),
                Scalar::Map(
                    MapData::try_new(
                        partition_values_map_type().unwrap(),
                        Vec::<(Scalar, Scalar)>::new(),
                    )
                    .unwrap(),
                ),
                Scalar::Long(size),
                Scalar::Long(i64::MAX),
                Scalar::Boolean(true),
                null_of("stats"),
                null_of("tags"),
                null_of("deletionVector"),
                Scalar::Long(base_row_id),
                Scalar::Long(commit_version),
                null_of("clusteringProvider"),
                null_of("backReference"),
            ],
        )
        .unwrap();
        vec![Scalar::Struct(add)]
    }

    /// A typed null [`Scalar`] for the named `Add` field, so expected values match the schema types
    /// the transform's null fall-through produces.
    fn null_of(field: &str) -> Scalar {
        Scalar::Null(ADD_SCHEMA.field(field).unwrap().data_type().clone())
    }

    fn expected_batch(engine: &dyn Engine, rows: &[Vec<Scalar>]) -> Box<dyn EngineData> {
        engine
            .evaluation_handler()
            .create_many(LOG_ADD_SCHEMA.clone(), rows.to_vec())
            .unwrap()
    }

    #[test]
    fn converts_added_data_entries_to_add_actions() {
        let engine = SyncEngine::new();
        let entries = [
            added_data_entry("a.parquet", 100, 10, 0, 5),
            added_data_entry("b.parquet", 200, 20, 10, 5),
        ];
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &entries).as_ref(),
                &read_ctx(),
            )
            .unwrap(),
        );

        let expected = expected_batch(
            &engine,
            &[
                expected_add_row("a.parquet", 100, 0, 5),
                expected_add_row("b.parquet", 200, 10, 5),
            ],
        );
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn output_schema_matches_log_add_schema() {
        let engine = SyncEngine::new();
        let entries = [added_data_entry("a.parquet", 1, 1, 0, 0)];
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &entries).as_ref(),
                &read_ctx(),
            )
            .unwrap(),
        )
        .try_into_record_batch()
        .unwrap();
        let expected = LOG_ADD_SCHEMA.as_ref().try_into_arrow().unwrap();
        assert_eq!(out.schema().as_ref(), &expected);
    }

    #[test]
    fn drops_tombstone_and_non_data_entries() {
        let engine = SyncEngine::new();
        let mut deleted = added_data_entry("deleted.parquet", 1, 1, 0, 0);
        deleted.tracking.status = TrackingStatus::Deleted;
        let mut manifest = added_data_entry("manifest.parquet", 1, 1, 0, 0);
        manifest.content_type = DataContentType::DataManifest;
        manifest.manifest_info = Some(ManifestInfo::default());
        let live = added_data_entry("live.parquet", 42, 7, 3, 9);

        let filtered = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[deleted, manifest, live]).as_ref(),
            &read_ctx(),
        )
        .unwrap();
        assert_eq!(filtered.selection_vector(), &[false, false, true]);

        let out = filtered_to_batch(filtered);
        let expected = expected_batch(&engine, &[expected_add_row("live.parquet", 42, 3, 9)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    #[case(TrackingStatus::Existing, true)]
    #[case(TrackingStatus::Added, true)]
    #[case(TrackingStatus::Deleted, false)]
    #[case(TrackingStatus::Replaced, false)]
    fn selects_only_live_data_entries(#[case] status: TrackingStatus, #[case] kept: bool) {
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        entry.tracking.status = status;
        assert_eq!(selection_of(&[entry]), [kept]);
    }

    #[rstest]
    fn live_data_manifest_entry_is_dropped(
        #[values(TrackingStatus::Existing, TrackingStatus::Added)] status: TrackingStatus,
    ) {
        let mut entry = added_data_entry("manifest.parquet", 1, 1, 0, 0);
        entry.content_type = DataContentType::DataManifest;
        entry.manifest_info = Some(ManifestInfo::default());
        entry.tracking.status = status;
        assert_eq!(selection_of(&[entry]), [false]);
    }

    #[rstest]
    fn live_delete_entry_errors(
        #[values(
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
            DataContentType::DeleteManifest
        )]
        content_type: DataContentType,
        #[values(TrackingStatus::Existing, TrackingStatus::Added)] status: TrackingStatus,
    ) {
        let mut entry = added_data_entry("deletes.parquet", 1, 1, 0, 0);
        entry.content_type = content_type;
        entry.tracking.status = status;
        let result = convert(&[entry]);
        assert_result_error_with_message(result, &format!("live {content_type:?} entry"));
    }

    /// A dropped entry is not validated: an unsupported file format, partition spec, or deletion
    /// vector on it must not fail the read.
    #[rstest]
    fn dropped_entry_with_unsupported_fields_is_not_rejected(
        #[values(
            DataContentType::Data,
            DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes,
            DataContentType::DataManifest,
            DataContentType::DeleteManifest
        )]
        content_type: DataContentType,
        #[values(TrackingStatus::Deleted, TrackingStatus::Replaced)] status: TrackingStatus,
    ) {
        let mut entry = added_data_entry("dropped.parquet", 1, 1, 0, 0);
        entry.content_type = content_type;
        entry.tracking.status = status;
        entry.file_format = DataFileFormat::Puffin;
        entry.spec_id = Some(7);
        entry.deletion_vector = Some(test_dv());
        entry.tracking.dv_snapshot_id = Some(1);
        assert_eq!(selection_of(&[entry]), [false]);
    }

    #[rstest]
    #[case::content_type(&[CONTENT_TYPE], "Invalid AMT content type value: 5")]
    #[case::tracking_status(&[TRACKING, TRACKING_STATUS], "Invalid AMT tracking status value: 5")]
    fn unknown_enum_value_errors(#[case] path: &[&str], #[case] message: &str) {
        let entry: StructData = added_data_entry("f.parquet", 1, 1, 0, 0).into();
        let result = convert_struct(with_field(&entry, path, Scalar::Integer(5)));
        assert_result_error_with_message(result, message);
    }

    #[rstest]
    fn live_data_entry_accepts_supported_file_format_and_spec_id(
        #[values("parquet", "PARQUET")] file_format: &str,
        #[values(None, Some(0))] spec_id: Option<i32>,
    ) {
        let mut entry = added_data_entry("f.parquet", 1, 1, 0, 0);
        entry.spec_id = spec_id;
        let entry = with_field(&entry.into(), &[FILE_FORMAT], Scalar::from(file_format));
        assert_eq!(convert_struct(entry).unwrap().selection_vector(), &[true]);
    }

    #[rstest]
    #[case::puffin_file_format(DataFileFormat::Puffin, Some(0), "has file format 'puffin'")]
    #[case::non_zero_spec_id(DataFileFormat::Parquet, Some(1), "has specId 1")]
    fn live_data_entry_with_unsupported_field_errors(
        #[case] file_format: DataFileFormat,
        #[case] spec_id: Option<i32>,
        #[case] message: &str,
    ) {
        let mut entry = added_data_entry("f.parquet", 1, 1, 0, 0);
        entry.file_format = file_format;
        entry.spec_id = spec_id;
        let result = convert(&[entry]);
        assert_result_error_with_message(result, message);
    }

    #[rstest]
    #[case::deletion_vector_struct(true, false)]
    #[case::dv_snapshot_id(false, true)]
    fn live_entry_with_deletion_vector_errors(
        #[case] set_deletion_vector: bool,
        #[case] set_dv_snapshot_id: bool,
    ) {
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        if set_deletion_vector {
            entry.deletion_vector = Some(test_dv());
        }
        if set_dv_snapshot_id {
            entry.tracking.dv_snapshot_id = Some(1);
        }
        let result = convert(&[entry]);
        assert_result_error_with_message(result, "deletion vector");
    }

    #[test]
    fn empty_input_yields_empty_batch() {
        let engine = SyncEngine::new();
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[]).as_ref(),
                &read_ctx(),
            )
            .unwrap(),
        );
        assert_eq!(out.len(), 0);
    }

    /// Materializes a [`FilteredEngineData`] by applying its selection vector, so tests can compare
    /// against the surviving `Add` rows.
    fn filtered_to_batch(filtered: FilteredEngineData) -> Box<dyn EngineData> {
        filtered.apply_selection_vector().unwrap()
    }
}
