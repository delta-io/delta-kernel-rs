//! Read-side translation for the Adaptive Metadata Tree (AMT): content-tree entries -> `Add` file
//! actions, the inverse of [`super::builder`].
//!
//! [`convert_root_entries_to_add_actions`] surfaces live `Data` entries as `Add` actions and drops
//! everything else. [`LeafReadContext`] adapts a leaf manifest to that path, first materializing
//! the tracking fields a leaf entry inherits from its parent.
//!
//! This is the minimal read path: statistics, partition values, tags, deletion vectors, and the
//! data-file modification time are not yet carried across (see the per-field TODOs).

use std::sync::{Arc, LazyLock};

use crate::actions::{
    ADD_NAME, ADD_SCHEMA, DATA_CHANGE_NAME, LOG_ADD_SCHEMA, MODIFICATION_TIME_NAME,
};
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, TrackingInfo, TrackingStatus,
    CONTENT_TYPE, DV_INFO, DV_SNAPSHOT_ID, FILE_SEQUENCE_NUMBER, FILE_SIZE_IN_BYTES, FIRST_ROW_ID,
    LOCATION, RECORD_COUNT, SEQUENCE_NUMBER, TRACKING, TRACKING_SNAPSHOT_ID, TRACKING_STATUS,
};
use crate::engine_data::{EngineData, FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, ArrayData, ColumnName, Expression, MapData, Scalar};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, PARTITION_VALUES_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::{
    ArrayType, ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType,
    ToSchema as _,
};
use crate::{DeltaResult, Engine, Error};

/// Helper column name carrying the prefix-summed `firstRowId` values while normalizing a leaf
/// entry batch. It is appended to the entry batch, consumed by the normalize expression, and
/// dropped from the output.
const FIRST_ROW_ID_HELPER: &str = "_firstRowId";

/// Schema of the single [`FIRST_ROW_ID_HELPER`] column appended to a leaf entry batch.
static FIRST_ROW_ID_HELPER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        FIRST_ROW_ID_HELPER,
        DataType::LONG,
    )]))
});

/// Input schema for the leaf normalize expression: the entry schema plus the appended
/// [`FIRST_ROW_ID_HELPER`] column.
static LEAF_NORMALIZE_INPUT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields: Vec<StructField> = ContentTreeNodeEntry::to_schema()
        .fields()
        .cloned()
        .collect();
    fields.push(StructField::nullable(FIRST_ROW_ID_HELPER, DataType::LONG));
    Arc::new(StructType::new_unchecked(fields))
});

/// Translates an AMT root manifest's content-tree entry batch into an `Add`-action batch, keeping
/// only the rows that read as live data files.
///
/// An entry becomes an `Add` when its `contentType` is [`DataContentType::Data`] and its tracking
/// status is [live](TrackingStatus::is_live); every other entry is dropped via the returned
/// selection vector. Leaf-manifest batches also reach this path, once
/// [`LeafReadContext::convert_leaf_entries_to_add_actions`] has materialized their inherited
/// tracking fields so they meet the same expectations as a root batch.
///
/// # Parameters
/// - `engine`: provides the [`crate::EvaluationHandler`] used to evaluate the transform.
/// - `entries`: a content-tree entry batch matching [`ContentTreeNodeEntry::to_schema`] (the
///   columnar form produced by [`super::builder`]).
///
/// # Returns
/// A [`FilteredEngineData`] over an `Add`-action batch (one row per input entry, schema
/// [`crate::actions::LOG_ADD_SCHEMA`]), whose selection vector keeps only the entries that read as
/// live data files. The selection is carried rather than applied so this path is symmetric with the
/// AMT write path ([`super::builder`]).
///
/// # Errors
/// Returns an error if a row carries an unknown tracking-status value, if a selected (live `Data`)
/// entry carries a deletion vector (not yet supported by the read path), if the evaluator cannot be
/// constructed or fails to evaluate, or if the selection vector length exceeds the batch.
pub(crate) fn convert_root_entries_to_add_actions(
    engine: &dyn Engine,
    entries: &dyn EngineData,
) -> DeltaResult<FilteredEngineData> {
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
    FilteredEngineData::try_new(actions, selector.selection)
}

/// Applies the tracking-field inheritance a leaf entry defers to its parent `DataManifest` entry,
/// converting leaf-manifest batches into `Add` actions.
///
/// A leaf entry leaves `snapshotId`/`sequenceNumber`/`fileSequenceNumber` null (inherited from the
/// parent) and `firstRowId` null (assigned by a prefix sum of `recordCount` seeded from the
/// parent's `firstRowId`). The prefix-sum cursor is carried across
/// [`Self::convert_leaf_entries_to_add_actions`] calls so successive batches of one leaf continue
/// the sum.
pub(crate) struct LeafReadContext {
    /// Next unassigned `firstRowId`, advanced across batches.
    next_first_row_id: i64,
    /// Transform that materializes the inherited tracking fields, built once from the parent.
    normalize_expr: Arc<Expression>,
}

impl LeafReadContext {
    /// Builds the inheritance context from a parent `DataManifest` entry's tracking info. Errors if
    /// any field a leaf entry inherits (`snapshotId`, `sequenceNumber`, `fileSequenceNumber`,
    /// `firstRowId`) is null on the parent: the protocol requires the root entry to carry these.
    pub(crate) fn new(parent: &TrackingInfo) -> DeltaResult<Self> {
        let require = |value: Option<i64>, field: &str| {
            value.ok_or_else(|| {
                Error::missing_data(format!(
                    "AMT parent DataManifest entry is missing required tracking field '{field}'"
                ))
            })
        };
        let normalize_expr = build_leaf_normalize_expression(
            require(parent.snapshot_id, TRACKING_SNAPSHOT_ID)?,
            require(parent.sequence_number, SEQUENCE_NUMBER)?,
            require(parent.file_sequence_number, FILE_SEQUENCE_NUMBER)?,
        )?;
        Ok(Self {
            next_first_row_id: require(parent.first_row_id, FIRST_ROW_ID)?,
            normalize_expr: Arc::new(normalize_expr),
        })
    }

    /// Converts one batch of leaf entries into an `Add`-action batch. Call once per batch of the
    /// same leaf manifest, in order: the `firstRowId` cursor carried on `self` continues across
    /// calls.
    ///
    /// # Parameters
    /// - `engine`: provides the [`crate::EvaluationHandler`] used to evaluate the transforms.
    /// - `entries`: a leaf-manifest entry batch matching [`ContentTreeNodeEntry::to_schema`].
    ///
    /// # Returns
    /// A [`FilteredEngineData`] over an `Add`-action batch, as
    /// [`convert_root_entries_to_add_actions`].
    ///
    /// # Errors
    /// Returns an error if a row has a null `recordCount`, or for any error surfaced by
    /// [`convert_root_entries_to_add_actions`].
    pub(crate) fn convert_leaf_entries_to_add_actions(
        &mut self,
        engine: &dyn Engine,
        entries: &dyn EngineData,
    ) -> DeltaResult<FilteredEngineData> {
        // The visitor assigns a `firstRowId` per row over the full batch in entry order (before
        // selection drops any rows), so the prefix sum stays aligned with the entries.
        let mut visitor = FirstRowIdVisitor::new(self.next_first_row_id);
        visitor.visit_rows_of(entries)?;
        self.next_first_row_id = visitor.next_first_row_id;

        let helper_column =
            ArrayData::try_new(ArrayType::new(DataType::LONG, true), visitor.first_row_ids)?;
        let augmented =
            entries.append_columns(FIRST_ROW_ID_HELPER_SCHEMA.clone(), vec![helper_column])?;

        let evaluator = engine.evaluation_handler().new_expression_evaluator(
            LEAF_NORMALIZE_INPUT_SCHEMA.clone(),
            self.normalize_expr.clone(),
            DataType::from(ContentTreeNodeEntry::to_schema()),
        )?;
        let normalized = evaluator.evaluate(augmented.as_ref())?;
        // The normalized batch has every inherited tracking field materialized, so it now meets the
        // expectations of the root read path.
        convert_root_entries_to_add_actions(engine, normalized.as_ref())
    }
}

// === Helpers ===

/// Builds the transform rewriting a leaf entry (augmented with the [`FIRST_ROW_ID_HELPER`] column)
/// into a normalized [`ContentTreeNodeEntry`]: the inherited tracking fields take the parent's
/// literals (when null), `firstRowId` takes the prefix-sum column, and every other field passes
/// through by column.
fn build_leaf_normalize_expression(
    parent_snapshot_id: i64,
    parent_sequence_number: i64,
    parent_file_sequence_number: i64,
) -> DeltaResult<Expression> {
    // A null inherited field falls back to the parent's literal; a present value wins.
    let inherit = |field: &str, parent: i64| {
        Expression::coalesce([Expression::column([TRACKING, field]), lit(parent)])
    };
    // Every field passes through by column; only the inherited tracking fields are rewritten.
    // Unlike the write path, no field may fall through to a typed null -- that would erase real
    // entry data.
    let tracking = struct_expr_from_schema(&TrackingInfo::to_schema(), |name| {
        Some(match name {
            TRACKING_SNAPSHOT_ID => inherit(TRACKING_SNAPSHOT_ID, parent_snapshot_id),
            SEQUENCE_NUMBER => inherit(SEQUENCE_NUMBER, parent_sequence_number),
            FILE_SEQUENCE_NUMBER => inherit(FILE_SEQUENCE_NUMBER, parent_file_sequence_number),
            // The visitor already copied existing `firstRowId`s into the helper column, so this
            // takes it directly rather than coalescing against the entry's own column.
            FIRST_ROW_ID => Expression::column([FIRST_ROW_ID_HELPER]),
            other => Expression::column([TRACKING, other]),
        })
    })?;
    struct_expr_from_schema(&ContentTreeNodeEntry::to_schema(), |name| {
        Some(match name {
            TRACKING => tracking.clone(),
            other => Expression::column([other]),
        })
    })
}

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
            n if n == SIZE_NAME => Expression::column([FILE_SIZE_IN_BYTES]),
            // TODO: the AMT entry does not carry the data file's modification time; emit a
            // placeholder until a source (e.g. an entry field or the commit timestamp) is threaded
            // through.
            n if n == MODIFICATION_TIME_NAME => lit(i64::MAX),
            // TODO: `dataChange` is hard-coded true; carry the real value once the entry (or the
            // commit context) provides it.
            n if n == DATA_CHANGE_NAME => lit(true),
            n if n == BASE_ROW_ID_NAME => Expression::column([TRACKING, FIRST_ROW_ID]),
            n if n == DEFAULT_ROW_COMMIT_VERSION_NAME => {
                Expression::column([TRACKING, SEQUENCE_NUMBER])
            }
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
///
/// Errors on a selected entry that carries a deletion vector: the read path does not yet populate
/// `Add.deletionVector`, so emitting such an entry as a DV-less `Add` would read its logically
/// deleted rows back as live. Rejecting is conservative until the read path threads DV info across.
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
                    // `deletionVector.location` is a required field of the DV sub-struct, so it
                    // reads null iff the `deletionVector` struct itself is null.
                    ColumnName::new([DV_INFO, LOCATION]),
                    ColumnName::new([TRACKING, DV_SNAPSHOT_ID]),
                ],
                vec![
                    DataType::INTEGER,
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::LONG,
                ],
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
            if selected {
                let dv_location: Option<&str> = getters[2].get_opt(row, LOCATION)?;
                let dv_snapshot_id: Option<i64> = getters[3].get_opt(row, DV_SNAPSHOT_ID)?;
                if let Some(field) = dv_location
                    .map(|_| DV_INFO)
                    .or_else(|| dv_snapshot_id.map(|_| DV_SNAPSHOT_ID))
                {
                    return Err(Error::unsupported(format!(
                        "AMT content-tree read path does not yet support a live entry with a \
                         deletion vector (non-null '{field}')"
                    )));
                }
            }
            self.selection.push(selected);
        }
        Ok(())
    }
}

/// Assigns a `firstRowId` to each row of a leaf entry batch: an existing value is preserved,
/// otherwise the next value from the cursor is assigned and the cursor advances by `recordCount`.
/// The cursor persists across batches when the visitor is re-seeded from its final value.
struct FirstRowIdVisitor {
    /// Next unassigned `firstRowId`; seeded from the parent and advanced per fresh assignment.
    next_first_row_id: i64,
    /// Assigned `firstRowId` per row, in entry order.
    first_row_ids: Vec<i64>,
}

impl FirstRowIdVisitor {
    fn new(next_first_row_id: i64) -> Self {
        Self {
            next_first_row_id,
            first_row_ids: Vec::new(),
        }
    }
}

impl RowVisitor for FirstRowIdVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    ColumnName::new([RECORD_COUNT]),
                    ColumnName::new([TRACKING, FIRST_ROW_ID]),
                ],
                vec![DataType::LONG, DataType::LONG],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        self.first_row_ids.reserve(row_count);
        for row in 0..row_count {
            let record_count: i64 = getters[0].get_opt(row, RECORD_COUNT)?.ok_or_else(|| {
                Error::missing_data(format!(
                    "AMT content-tree leaf entry has a null required field '{RECORD_COUNT}'"
                ))
            })?;
            // Preserve an existing assignment (and leave the cursor untouched); otherwise take the
            // next range and advance the cursor by this entry's row count.
            let assigned = match getters[1].get_opt(row, FIRST_ROW_ID)? {
                Some(existing) => existing,
                None => {
                    let assigned = self.next_first_row_id;
                    self.next_first_row_id += record_count;
                    assigned
                }
            };
            self.first_row_ids.push(assigned);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::content_tree::{DataFileFormat, DeletionVectorInfo, ManifestInfo};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::StructData;
    use crate::unit_test_utils::assert_result_error_with_message;

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

    /// Builds a content-tree entry batch (input to the read path) from explicit entries.
    fn entry_batch(engine: &dyn Engine, entries: &[ContentTreeNodeEntry]) -> Box<dyn EngineData> {
        let structs: Vec<StructData> = entries.iter().cloned().map(Into::into).collect();
        let rows: Vec<Vec<Scalar>> = structs.iter().map(|s| s.values().to_vec()).collect();
        engine
            .evaluation_handler()
            .create_many(Arc::new(ContentTreeNodeEntry::to_schema()), rows)
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
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &entries).as_ref())
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
        use crate::engine::arrow_conversion::TryIntoArrow as _;
        let engine = SyncEngine::new();
        let entries = [added_data_entry("a.parquet", 1, 1, 0, 0)];
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &entries).as_ref())
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

        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[deleted, manifest, live]).as_ref(),
            )
            .unwrap(),
        );

        let expected = expected_batch(&engine, &[expected_add_row("live.parquet", 42, 3, 9)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    #[case(TrackingStatus::Existing, true)]
    #[case(TrackingStatus::Added, true)]
    #[case(TrackingStatus::Modified, true)]
    #[case(TrackingStatus::Deleted, false)]
    #[case(TrackingStatus::Replaced, false)]
    fn selects_only_live_data_entries(#[case] status: TrackingStatus, #[case] kept: bool) {
        let engine = SyncEngine::new();
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        entry.tracking.status = status;
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &[entry]).as_ref())
                .unwrap(),
        );
        assert_eq!(out.len(), usize::from(kept));
    }

    #[rstest]
    #[case::deletion_vector_struct(true, false)]
    #[case::dv_snapshot_id(false, true)]
    fn live_entry_with_deletion_vector_errors(
        #[case] set_deletion_vector: bool,
        #[case] set_dv_snapshot_id: bool,
    ) {
        let engine = SyncEngine::new();
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        if set_deletion_vector {
            entry.deletion_vector = Some(DeletionVectorInfo {
                location: "dv.bin".to_string(),
                offset: 0,
                size_in_bytes: 1,
                cardinality: 1,
            });
        }
        if set_dv_snapshot_id {
            entry.tracking.dv_snapshot_id = Some(1);
        }
        let result =
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &[entry]).as_ref());
        assert_result_error_with_message(result, "deletion vector");
    }

    #[test]
    fn empty_input_yields_empty_batch() {
        let engine = SyncEngine::new();
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(&engine, entry_batch(&engine, &[]).as_ref())
                .unwrap(),
        );
        assert_eq!(out.len(), 0);
    }

    // === Leaf read path ===

    /// A leaf `Data`/`Added` entry whose inherited tracking fields (`snapshotId`,
    /// `sequenceNumber`/`fileSequenceNumber`, `firstRowId`) may be left null to exercise
    /// inheritance. `sequence_number` sets both sequence fields.
    fn leaf_entry(
        path: &str,
        size: i64,
        num_records: i64,
        snapshot_id: Option<i64>,
        sequence_number: Option<i64>,
        first_row_id: Option<i64>,
    ) -> ContentTreeNodeEntry {
        let mut entry = added_data_entry(path, size, num_records, 0, 0);
        entry.tracking.snapshot_id = snapshot_id;
        entry.tracking.sequence_number = sequence_number;
        entry.tracking.file_sequence_number = sequence_number;
        entry.tracking.first_row_id = first_row_id;
        entry
    }

    /// A parent `DataManifest` entry's tracking info supplying the values leaf entries inherit.
    fn parent_tracking(
        snapshot_id: Option<i64>,
        sequence_number: Option<i64>,
        file_sequence_number: Option<i64>,
        first_row_id: Option<i64>,
    ) -> TrackingInfo {
        TrackingInfo {
            status: TrackingStatus::Added,
            snapshot_id,
            dv_snapshot_id: None,
            sequence_number,
            file_sequence_number,
            first_row_id,
            deleted_positions: None,
            replaced_positions: None,
        }
    }

    /// A fully-populated parent (`snapshotId` 7, sequence 5, `firstRowId` 100) for the common case.
    fn valid_parent() -> TrackingInfo {
        parent_tracking(Some(7), Some(5), Some(5), Some(100))
    }

    #[test]
    fn leaf_inherits_sequence_and_assigns_first_row_id() {
        let engine = SyncEngine::new();
        let entries = [
            leaf_entry("a.parquet", 100, 10, None, None, None),
            leaf_entry("b.parquet", 200, 20, None, None, None),
        ];
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &entries).as_ref(),
            )
            .unwrap(),
        );
        // baseRowId is prefix-summed from the parent's 100 by recordCount; defaultRowCommitVersion
        // inherits the parent's sequence number 5.
        let expected = expected_batch(
            &engine,
            &[
                expected_add_row("a.parquet", 100, 100, 5),
                expected_add_row("b.parquet", 200, 110, 5),
            ],
        );
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn leaf_preserves_its_own_non_null_inherited_fields() {
        let engine = SyncEngine::new();
        // The entry carries its own sequence 99 and firstRowId 500, which win over the parent's.
        let entries = [leaf_entry(
            "a.parquet",
            100,
            10,
            Some(3),
            Some(99),
            Some(500),
        )];
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &entries).as_ref(),
            )
            .unwrap(),
        );
        let expected = expected_batch(&engine, &[expected_add_row("a.parquet", 100, 500, 99)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn existing_first_row_id_is_preserved_and_does_not_advance_cursor() {
        let engine = SyncEngine::new();
        // The first entry keeps its own firstRowId 500 and must NOT advance the cursor, so the
        // second (was-null) entry is still assigned the seeded 100.
        let entries = [
            leaf_entry("a.parquet", 1, 7, None, None, Some(500)),
            leaf_entry("b.parquet", 1, 10, None, None, None),
        ];
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &entries).as_ref(),
            )
            .unwrap(),
        );
        let expected = expected_batch(
            &engine,
            &[
                expected_add_row("a.parquet", 1, 500, 5),
                expected_add_row("b.parquet", 1, 100, 5),
            ],
        );
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn first_row_id_cursor_forwards_across_batches() {
        let engine = SyncEngine::new();
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();

        let batch1 = [
            leaf_entry("a.parquet", 1, 10, None, None, None),
            leaf_entry("b.parquet", 1, 20, None, None, None),
        ];
        let out1 = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &batch1).as_ref(),
            )
            .unwrap(),
        );
        let expected1 = expected_batch(
            &engine,
            &[
                expected_add_row("a.parquet", 1, 100, 5),
                expected_add_row("b.parquet", 1, 110, 5),
            ],
        );
        assert_eq!(
            out1.try_into_record_batch().unwrap(),
            expected1.try_into_record_batch().unwrap()
        );

        // The cursor persists on the context: 100 + 10 + 20 = 130.
        let batch2 = [leaf_entry("c.parquet", 1, 5, None, None, None)];
        let out2 = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &batch2).as_ref(),
            )
            .unwrap(),
        );
        let expected2 = expected_batch(&engine, &[expected_add_row("c.parquet", 1, 130, 5)]);
        assert_eq!(
            out2.try_into_record_batch().unwrap(),
            expected2.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn deleted_leaf_entry_is_dropped_and_keeps_prefix_sum_ordinal() {
        let engine = SyncEngine::new();
        let a = leaf_entry("a.parquet", 1, 10, None, None, None);
        // A dropped Deleted entry carries its own firstRowId, so it neither reassigns nor advances.
        let mut deleted = leaf_entry("del.parquet", 1, 7, None, None, Some(999));
        deleted.tracking.status = TrackingStatus::Deleted;
        let c = leaf_entry("c.parquet", 1, 20, None, None, None);

        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[a, deleted, c]).as_ref(),
            )
            .unwrap(),
        );
        // `a` -> 100; the Deleted entry is dropped and does not advance; `c` -> 110.
        let expected = expected_batch(
            &engine,
            &[
                expected_add_row("a.parquet", 1, 100, 5),
                expected_add_row("c.parquet", 1, 110, 5),
            ],
        );
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    #[case::snapshot(None, Some(5), Some(5), Some(100), TRACKING_SNAPSHOT_ID)]
    #[case::sequence(Some(7), None, Some(5), Some(100), SEQUENCE_NUMBER)]
    #[case::file_sequence(Some(7), Some(5), None, Some(100), FILE_SEQUENCE_NUMBER)]
    #[case::first_row_id(Some(7), Some(5), Some(5), None, FIRST_ROW_ID)]
    fn leaf_read_context_rejects_null_parent_field(
        #[case] snapshot_id: Option<i64>,
        #[case] sequence_number: Option<i64>,
        #[case] file_sequence_number: Option<i64>,
        #[case] first_row_id: Option<i64>,
        #[case] expected_field: &str,
    ) {
        let result = LeafReadContext::new(&parent_tracking(
            snapshot_id,
            sequence_number,
            file_sequence_number,
            first_row_id,
        ));
        assert_result_error_with_message(result, expected_field);
    }

    #[test]
    fn leaf_live_entry_with_deletion_vector_errors() {
        let engine = SyncEngine::new();
        let mut entry = leaf_entry("f.parquet", 10, 5, None, None, None);
        entry.deletion_vector = Some(DeletionVectorInfo {
            location: "dv.bin".to_string(),
            offset: 0,
            size_in_bytes: 1,
            cardinality: 1,
        });
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let result = ctx
            .convert_leaf_entries_to_add_actions(&engine, entry_batch(&engine, &[entry]).as_ref());
        assert_result_error_with_message(result, "deletion vector");
    }

    #[test]
    fn leaf_empty_batch_yields_empty_and_preserves_cursor() {
        let engine = SyncEngine::new();
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let empty = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(&engine, entry_batch(&engine, &[]).as_ref())
                .unwrap(),
        );
        assert_eq!(empty.len(), 0);

        // The seeded cursor is untouched by the empty batch, so the next entry still starts at 100.
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[leaf_entry("a.parquet", 1, 10, None, None, None)]).as_ref(),
            )
            .unwrap(),
        );
        let expected = expected_batch(&engine, &[expected_add_row("a.parquet", 1, 100, 5)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    /// Materializes a [`FilteredEngineData`] by applying its selection vector, so tests can compare
    /// against the surviving `Add` rows.
    fn filtered_to_batch(filtered: FilteredEngineData) -> Box<dyn EngineData> {
        filtered.apply_selection_vector().unwrap()
    }
}
