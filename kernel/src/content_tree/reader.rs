//! Read-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates the content-tree entries of an AMT root manifest into `Add` file actions -- the
//! inverse of [`super::builder`]. This is the minimal read path: it surfaces live `Data` entries as
//! `Add` actions and drops everything else. Statistics, partition values, tags, deletion vectors,
//! and the data-file modification time are not yet carried across (see the per-field TODOs); the
//! only entry data that flows into the action is the file location, size, and row-tracking numbers.

use std::sync::{Arc, LazyLock};

use crate::actions::{ADD_NAME, ADD_SCHEMA, LOG_ADD_SCHEMA};
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, TrackingStatus, CONTENT_TYPE,
    FILE_SIZE_IN_BYTES, FIRST_ROW_ID, LOCATION, SEQUENCE_NUMBER, TRACKING, TRACKING_STATUS,
};
use crate::engine_data::{EngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, ColumnName, Expression, MapData, Scalar};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, PARTITION_VALUES_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::{ColumnNamesAndTypes, DataType, MapType, StructField, ToSchema as _};
use crate::{DeltaResult, Engine, Error};

/// Name of the `Add` action's `modificationTime` field (no dedicated constant in `log_replay`).
const MODIFICATION_TIME: &str = "modificationTime";
/// Name of the `Add` action's `dataChange` field (no dedicated constant in `log_replay`).
const DATA_CHANGE: &str = "dataChange";

/// Translates an AMT root manifest's content-tree entry batch into an `Add`-action batch, keeping
/// only the rows that read as live data files.
///
/// An entry becomes an `Add` when its `contentType` is [`DataContentType::Data`] and its tracking
/// status is [live](TrackingStatus::is_live); every other entry (manifest references, tombstones)
/// is dropped. The produced batch matches [`crate::actions::LOG_ADD_SCHEMA`] (`{ add: Add }`), so
/// it can flow into log replay exactly like an `Add` parsed from a JSON commit.
///
/// Field mapping, per surviving row: `add.path` <- `location`, `add.size` <- `fileSizeInBytes`
/// (0 when null), `add.baseRowId` <- `tracking.firstRowId`, and `add.defaultRowCommitVersion` <-
/// `tracking.sequenceNumber`. `add.dataChange` is `true`. Fields the AMT root does not yet carry
/// are left null (or a placeholder); see the per-field TODOs in [`build_entry_to_add_expression`].
///
/// # Parameters
/// - `engine`: provides the [`crate::EvaluationHandler`] used to evaluate the transform.
/// - `entries`: a content-tree entry batch matching [`ContentTreeNodeEntry::to_schema`] (the
///   columnar form produced by [`super::builder`]).
///
/// # Returns
/// An [`EngineData`] batch of `Add` actions, containing one row per surviving entry.
///
/// # Errors
/// Returns an error if a row carries an unknown tracking-status value, if the evaluator cannot be
/// constructed or fails to evaluate, or if the selection vector cannot be applied.
pub(crate) fn convert_root_entries_to_add_actions(
    engine: &dyn Engine,
    entries: &dyn EngineData,
) -> DeltaResult<Box<dyn EngineData>> {
    let mut selector = AddSelectionVisitor::default();
    selector.visit_rows_of(entries)?;

    let input_schema = Arc::new(ContentTreeNodeEntry::to_schema());
    let output_type = DataType::from(LOG_ADD_SCHEMA.as_ref().clone());
    let expr = build_entry_to_add_expression()?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        input_schema,
        Arc::new(expr),
        output_type,
    )?;
    let actions = evaluator.evaluate(entries)?;
    actions.apply_selection_vector(selector.selection)
}

// === Helpers ===

/// Builds the transform mapping a [`ContentTreeNodeEntry`] row to a `{ add: Add }` struct matching
/// [`crate::actions::LOG_ADD_SCHEMA`].
///
/// Non-null `Add` fields with no AMT source get a placeholder; nullable fields not listed here fall
/// through to a typed null via [`struct_expr_from_schema`].
fn build_entry_to_add_expression() -> DeltaResult<Expression> {
    // TODO: read partition values from the entry's `partition` tuple once the read path carries a
    // partition spec; the AMT root written by the minimal blind-append path is unpartitioned. The
    // map type is taken from the action schema so its value-nullability matches
    // `Add.partitionValues`.
    let empty_partition_values = lit(Scalar::Map(MapData::try_new(
        partition_values_map_type()?,
        Vec::<(Scalar, Scalar)>::new(),
    )?));

    // `log_replay` field names are `static`, so they cannot be `match` patterns; compare via
    // guards. `struct_expr_from_schema` fills every unmatched (nullable) field with a typed null,
    // so `stats`, `tags`, `deletionVector`, and `clusteringProvider` fall through to null until the
    // read path carries statistics, tags, and inline deletion-vector info across from the entry.
    let add = struct_expr_from_schema(&ADD_SCHEMA, |name| {
        Some(match name {
            // TODO: `location` is the raw path stored in the AMT root; resolving it to a
            // table-relative `Add.path` (percent-decoding, and relativizing against a manifest
            // location for non-root entries) is not yet done -- it flows through verbatim, which
            // round-trips only the minimal-root case that stored the raw path.
            n if n == PATH_NAME => Expression::column([LOCATION]),
            // TODO: read partition values from the entry's `partition` tuple once the read path
            // carries a partition spec.
            n if n == PARTITION_VALUES_NAME => empty_partition_values.clone(),
            n if n == SIZE_NAME => {
                Expression::coalesce([Expression::column([FILE_SIZE_IN_BYTES]), lit(0i64)])
            }
            // TODO: the AMT entry does not carry the data file's modification time; emit a
            // placeholder until a source (e.g. an entry field or the commit timestamp) is threaded
            // through.
            n if n == MODIFICATION_TIME => lit(i64::MIN),
            n if n == DATA_CHANGE => lit(true),
            n if n == BASE_ROW_ID_NAME => Expression::column([TRACKING, FIRST_ROW_ID]),
            n if n == DEFAULT_ROW_COMMIT_VERSION_NAME => {
                Expression::column([TRACKING, SEQUENCE_NUMBER])
            }
            _ => return None,
        })
    });

    // LOG_ADD_SCHEMA is a single non-null `add` field wrapping the action struct.
    Ok(struct_expr_from_schema(
        &LOG_ADD_SCHEMA,
        |name| match name {
            ADD_NAME => Some(add.clone()),
            _ => None,
        },
    ))
}

/// The [`MapType`] of `Add.partitionValues`, read from the action schema so callers match its
/// declared value-nullability.
fn partition_values_map_type() -> DeltaResult<MapType> {
    match ADD_SCHEMA
        .field(PARTITION_VALUES_NAME)
        .map(StructField::data_type)
    {
        Some(DataType::Map(map)) => Ok(map.as_ref().clone()),
        other => Err(Error::generic(format!(
            "Add schema `{PARTITION_VALUES_NAME}` field is not a map: {other:?}"
        ))),
    }
}

/// Builds the selection vector picking the entries that become `Add` actions: a live
/// ([`TrackingStatus::is_live`]) [`DataContentType::Data`] entry is selected; any other entry is
/// not.
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
                ],
                vec![DataType::INTEGER, DataType::INTEGER],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        const DATA: i32 = DataContentType::Data as i32;
        self.selection.reserve(row_count);
        for row in 0..row_count {
            let content_type: i32 = getters[0].get(row, CONTENT_TYPE)?;
            let selected = if content_type == DATA {
                let status: i32 = getters[1].get(row, TRACKING_STATUS)?;
                TrackingStatus::try_from_repr(status)?.is_live()
            } else {
                false
            };
            self.selection.push(selected);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_tree::{DataFileFormat, ManifestInfo, TrackingInfo};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::StructData;

    /// AMT/Iceberg format version stamped on entries; irrelevant to the `Add` output but required
    /// to build a well-formed [`ContentTreeNodeEntry`].
    const AMT_FORMAT_VERSION: i32 = 4;

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
            location: Some(path.to_string()),
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
            spec_id: 0,
            partition: None,
            sort_order_id: None,
            record_count: num_records,
            file_size_in_bytes: Some(size),
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
        let rows: Vec<StructData> = entries.iter().cloned().map(Into::into).collect();
        let row_refs: Vec<&[Scalar]> = rows.iter().map(StructData::values).collect();
        engine
            .evaluation_handler()
            .create_many(Arc::new(ContentTreeNodeEntry::to_schema()), &row_refs)
            .unwrap()
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
                Scalar::Long(i64::MIN),
                Scalar::Boolean(true),
                null_of("stats"),
                null_of("tags"),
                null_of("deletionVector"),
                Scalar::Long(base_row_id),
                Scalar::Long(commit_version),
                null_of("clusteringProvider"),
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
        let row_refs: Vec<&[Scalar]> = rows.iter().map(Vec::as_slice).collect();
        engine
            .evaluation_handler()
            .create_many(LOG_ADD_SCHEMA.clone(), &row_refs)
            .unwrap()
    }

    #[test]
    fn converts_added_data_entries_to_add_actions() {
        let engine = SyncEngine::new();
        let entries = [
            added_data_entry("a.parquet", 100, 10, 0, 5),
            added_data_entry("b.parquet", 200, 20, 10, 5),
        ];
        let out =
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &entries).as_ref())
                .unwrap();

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
        use crate::engine::arrow_conversion::TryIntoArrow as _;
        let engine = SyncEngine::new();
        let entries = [added_data_entry("a.parquet", 1, 1, 0, 0)];
        let out =
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &entries).as_ref())
                .unwrap()
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

        let out = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[deleted, manifest, live]).as_ref(),
        )
        .unwrap();

        let expected = expected_batch(&engine, &[expected_add_row("live.parquet", 42, 3, 9)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn null_file_size_becomes_zero() {
        let engine = SyncEngine::new();
        let mut entry = added_data_entry("a.parquet", 0, 1, 0, 0);
        entry.file_size_in_bytes = None;
        let out =
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &[entry]).as_ref())
                .unwrap();
        let expected = expected_batch(&engine, &[expected_add_row("a.parquet", 0, 0, 0)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn empty_input_yields_empty_batch() {
        let engine = SyncEngine::new();
        let out = convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &[]).as_ref())
            .unwrap();
        assert_eq!(out.len(), 0);
    }
}
