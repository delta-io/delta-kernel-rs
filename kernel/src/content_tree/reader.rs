//! Read-side translation for the Adaptive Metadata Tree (AMT): content-tree entries -> `Add` file
//! actions, the inverse of [`super::builder`].
//!
//! [`convert_root_entries_to_add_actions`] surfaces live `Data` entries as `Add` actions and drops
//! everything else. [`LeafReadContext`] adapts a leaf manifest to that path, first materializing
//! the tracking fields a leaf entry inherits from its parent.
//!
//! This is the minimal read path: statistics, partition values, tags, and deletion vectors are not
//! yet carried across (see the per-field TODOs). `modificationTime` and `dataChange` have no AMT
//! source and are supplied by the caller via [`ReadContext`].

use std::sync::{Arc, LazyLock};

use crate::actions::{
    ADD_NAME, ADD_SCHEMA, DATA_CHANGE_NAME, LOG_ADD_SCHEMA, MODIFICATION_TIME_NAME,
};
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, TrackingInfo, TrackingStatus,
    CONTENT_TYPE, DV_INFO, DV_SNAPSHOT_ID, FILE_SIZE_IN_BYTES, FIRST_ROW_ID, LOCATION,
    RECORD_COUNT, SEQUENCE_NUMBER, TRACKING, TRACKING_STATUS,
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

/// Caller-supplied expressions for the two `Add` fields the read path derives from an entry's
/// `tracking` sub-struct. The root path ([`AddFieldSources::root`]) reads the entry columns
/// directly; other callers (e.g. a leaf manifest applying inheritance) can substitute their own
/// expressions without the root path having to know about them.
struct AddFieldSources {
    /// Expression producing `Add.baseRowId`.
    base_row_id: Expression,
    /// Expression producing `Add.defaultRowCommitVersion`.
    default_row_commit_version: Expression,
}

impl AddFieldSources {
    /// Root-manifest sources: read `tracking.firstRowId` / `tracking.sequenceNumber` straight off
    /// the entry (a root entry carries both).
    fn root() -> Self {
        Self {
            base_row_id: Expression::column([TRACKING, FIRST_ROW_ID]),
            default_row_commit_version: Expression::column([TRACKING, SEQUENCE_NUMBER]),
        }
    }
}

/// Helper column name carrying the assigned `firstRowId` values for a leaf entry batch. It is
/// appended to the entry batch and read by the entry->`Add` transform as `Add.baseRowId`.
const FIRST_ROW_ID_HELPER: &str = "_firstRowId";

/// Schema of the single [`FIRST_ROW_ID_HELPER`] column appended to a leaf entry batch.
static FIRST_ROW_ID_HELPER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        FIRST_ROW_ID_HELPER,
        DataType::LONG,
    )]))
});

/// Input schema the leaf entry->`Add` transform evaluates against: the entry schema plus the
/// appended [`FIRST_ROW_ID_HELPER`] column.
static LEAF_INPUT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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
/// selection vector.
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
/// Returns an error if a `Data` entry carries an unknown tracking-status value, if a selected
/// (live `Data`) entry carries a deletion vector (not yet supported by the read path), if the
/// evaluator cannot be constructed or fails to evaluate, or if the selection vector length exceeds
/// the batch.
pub(crate) fn convert_root_entries_to_add_actions(
    engine: &dyn Engine,
    entries: &dyn EngineData,
    ctx: &ReadContext,
) -> DeltaResult<FilteredEngineData> {
    convert_entries_with(
        engine,
        entries,
        ctx,
        &AddFieldSources::root(),
        Arc::new(ContentTreeNodeEntry::to_schema()),
    )
}

// === Helpers ===

/// Shared body of the read path: build the entry->`Add` transform (with the caller's
/// [`AddFieldSources`]), evaluate it over `entries`, and pair the result with the live-`Data`
/// selection vector.
///
/// `input_schema` is the schema the transform evaluates against -- the entry schema for a root
/// batch, or an augmented schema when a caller appends helper columns.
fn convert_entries_with(
    engine: &dyn Engine,
    entries: &dyn EngineData,
    ctx: &ReadContext,
    sources: &AddFieldSources,
    input_schema: SchemaRef,
) -> DeltaResult<FilteredEngineData> {
    let mut selector = AddSelectionVisitor::default();
    selector.visit_rows_of(entries)?;

    let output_type = DataType::from(LOG_ADD_SCHEMA.as_ref().clone());
    let expr = build_entry_to_add_expression(ctx, sources)?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        input_schema,
        Arc::new(expr),
        output_type,
    )?;
    let actions = evaluator.evaluate(entries)?;
    FilteredEngineData::try_new(actions, selector.selection)
}

/// Converts leaf-manifest entry batches into `Add` actions, applying the inheritance a leaf entry
/// defers to its parent `DataManifest` entry: an `Added` entry's null `firstRowId` is assigned from
/// a cursor seeded by the parent, and a null `sequenceNumber` inherits the parent's. The cursor is
/// carried across [`Self::convert_leaf_entries_to_add_actions`] calls so successive batches of one
/// leaf continue the sequence.
///
/// Only `firstRowId` and `sequenceNumber` are materialized here, because they are the only tracking
/// fields that feed an `Add` (as `baseRowId` / `defaultRowCommitVersion`).
pub(crate) struct LeafReadContext {
    /// Next unassigned `firstRowId`, advanced across batches.
    next_first_row_id: i64,
    /// `Add` row-tracking field sources applying this leaf's inheritance, built once from the
    /// parent.
    sources: AddFieldSources,
}

impl LeafReadContext {
    /// Builds the inheritance context from a parent `DataManifest` entry's tracking info. Errors if
    /// the parent is missing a field a leaf entry inherits: `sequenceNumber` (the fallback for
    /// `Add.defaultRowCommitVersion`) or `firstRowId` (the row-id seed).
    pub(crate) fn new(parent: &TrackingInfo) -> DeltaResult<Self> {
        let require = |value: Option<i64>, field: &str| {
            value.ok_or_else(|| {
                Error::missing_data(format!(
                    "AMT parent DataManifest entry is missing required tracking field '{field}'"
                ))
            })
        };
        let parent_sequence_number = require(parent.sequence_number, SEQUENCE_NUMBER)?;
        Ok(Self {
            next_first_row_id: require(parent.first_row_id, FIRST_ROW_ID)?,
            // `baseRowId` reads the assigned-firstRowId helper column; `defaultRowCommitVersion`
            // takes the entry's own `sequenceNumber`, falling back to the parent's when null.
            sources: AddFieldSources {
                base_row_id: Expression::column([FIRST_ROW_ID_HELPER]),
                default_row_commit_version: Expression::coalesce([
                    Expression::column([TRACKING, SEQUENCE_NUMBER]),
                    lit(parent_sequence_number),
                ]),
            },
        })
    }

    /// Converts one batch of leaf entries into an `Add`-action batch. Call once per batch of the
    /// same leaf manifest, in order: the `firstRowId` cursor carried on `self` continues across
    /// calls.
    ///
    /// The parent manifest's deletion vector is not applied here; the caller must apply it to the
    /// returned selection (after row-id assignment) so invalidated files do not resurface.
    ///
    /// # Errors
    /// Returns an error if a row has a null or negative `recordCount`, if the `firstRowId` cursor
    /// overflows, if a live non-`Added` entry has a null `firstRowId`, or for any error surfaced by
    /// [`convert_root_entries_to_add_actions`].
    pub(crate) fn convert_leaf_entries_to_add_actions(
        &mut self,
        engine: &dyn Engine,
        entries: &dyn EngineData,
        ctx: &ReadContext,
    ) -> DeltaResult<FilteredEngineData> {
        // Assign a `firstRowId` per row over the full batch in entry order (before selection drops
        // any rows) so the prefix sum stays aligned with the entries. The cursor is committed to
        // `self` only after the whole conversion succeeds, so a failed/retried batch is consistent.
        let (first_row_ids, next_first_row_id) =
            assign_first_row_ids(entries, self.next_first_row_id)?;

        let helper_column =
            ArrayData::try_new(ArrayType::new(DataType::LONG, true), first_row_ids)?;
        let augmented =
            entries.append_columns(FIRST_ROW_ID_HELPER_SCHEMA.clone(), vec![helper_column])?;

        let filtered = convert_entries_with(
            engine,
            augmented.as_ref(),
            ctx,
            &self.sources,
            LEAF_INPUT_SCHEMA.clone(),
        )?;
        self.next_first_row_id = next_first_row_id;
        Ok(filtered)
    }
}

// === Helpers ===

/// Assigns a `firstRowId` to each leaf entry over the full batch in entry order, returning the
/// per-row values (the [`FIRST_ROW_ID_HELPER`] column) and the next cursor.
///
/// An entry that already carries a `firstRowId` keeps it (the cursor does not move). An `Added`
/// entry with a null `firstRowId` is assigned the next range and the cursor advances by its
/// `recordCount`. A live non-`Added` entry with a null `firstRowId` is an error (it should carry
/// the value assigned when it was first added); a dropped (not-live) entry's `firstRowId` is
/// irrelevant and left null.
///
/// # Errors
/// Returns an error on an unknown tracking status, a null/negative `recordCount` on an `Added`
/// entry needing assignment, a cursor overflow, a live non-`Added` entry with a null `firstRowId`,
/// or a live non-`Added` entry with a null `sequenceNumber`.
fn assign_first_row_ids(entries: &dyn EngineData, seed: i64) -> DeltaResult<(Vec<Scalar>, i64)> {
    let mut visitor = FirstRowIdVisitor::new(seed);
    visitor.visit_rows_of(entries)?;
    Ok((visitor.first_row_ids, visitor.next_first_row_id))
}

/// Builds the transform mapping a [`ContentTreeNodeEntry`] row to a `{ add: Add }` struct matching
/// [`crate::actions::LOG_ADD_SCHEMA`].
///
/// `modificationTime` and `dataChange` have no AMT source and are taken from `ctx`; `baseRowId` and
/// `defaultRowCommitVersion` are taken from `sources` (so a caller can apply inheritance); nullable
/// fields not listed here fall through to a typed null via [`struct_expr_from_schema`].
fn build_entry_to_add_expression(
    ctx: &ReadContext,
    sources: &AddFieldSources,
) -> DeltaResult<Expression> {
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
            // The AMT entry carries neither of these; both come from the caller's `ReadContext`.
            n if n == MODIFICATION_TIME_NAME => lit(ctx.modification_time),
            n if n == DATA_CHANGE_NAME => lit(ctx.data_change),
            n if n == BASE_ROW_ID_NAME => sources.base_row_id.clone(),
            n if n == DEFAULT_ROW_COMMIT_VERSION_NAME => sources.default_row_commit_version.clone(),
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

/// Assigns the `firstRowId` for each leaf entry (see [`assign_first_row_ids`]). An entry that
/// already carries a value keeps it (the cursor does not move); an `Added` entry with a null
/// `firstRowId` takes the next range and advances the cursor by `recordCount`. A live non-`Added`
/// entry with a null `firstRowId` is rejected; a dropped entry's is left null. The cursor persists
/// across batches when the visitor is re-seeded from its final value.
struct FirstRowIdVisitor {
    /// Next unassigned `firstRowId`; seeded from the parent and advanced per fresh assignment.
    next_first_row_id: i64,
    /// Assigned `firstRowId` per row, in entry order, as the [`Scalar`]s of the helper column.
    first_row_ids: Vec<Scalar>,
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
                    ColumnName::new([TRACKING, TRACKING_STATUS]),
                    ColumnName::new([TRACKING, SEQUENCE_NUMBER]),
                ],
                vec![
                    DataType::LONG,
                    DataType::LONG,
                    DataType::INTEGER,
                    DataType::LONG,
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        self.first_row_ids.reserve(row_count);
        for row in 0..row_count {
            let status = TrackingStatus::try_from_repr(getters[2].get(row, TRACKING_STATUS)?)?;
            // Only `Added` entries inherit `sequenceNumber`; a live non-`Added` entry must carry
            // its own, or the parent's would be silently filled into `Add.defaultRowCommitVersion`.
            let sequence_number: Option<i64> = getters[3].get_opt(row, SEQUENCE_NUMBER)?;
            if status.is_live() && status != TrackingStatus::Added && sequence_number.is_none() {
                return Err(Error::missing_data(format!(
                    "AMT content-tree live non-Added leaf entry has a null '{SEQUENCE_NUMBER}'"
                )));
            }
            let assigned = match getters[1].get_opt(row, FIRST_ROW_ID)? {
                // An already-materialized `firstRowId` is kept and does not move the cursor.
                Some(existing) => Scalar::Long(existing),
                // Only `Added` entries are assigned a fresh range; the cursor advances by the
                // entry's row count. See [`assign_first_row_ids`] for the status rules.
                None if status == TrackingStatus::Added => {
                    let record_count: i64 =
                        getters[0].get_opt(row, RECORD_COUNT)?.ok_or_else(|| {
                            Error::missing_data(format!(
                                "AMT content-tree leaf entry has a null required field \
                                 '{RECORD_COUNT}'"
                            ))
                        })?;
                    if record_count < 0 {
                        return Err(Error::generic(format!(
                            "AMT content-tree leaf entry has a negative '{RECORD_COUNT}': \
                             {record_count}"
                        )));
                    }
                    let assigned = self.next_first_row_id;
                    self.next_first_row_id =
                        assigned.checked_add(record_count).ok_or_else(|| {
                            Error::generic(format!(
                                "AMT content-tree '{FIRST_ROW_ID}' assignment overflowed i64 at \
                             {assigned} + {record_count}"
                            ))
                        })?;
                    Scalar::Long(assigned)
                }
                // A live non-`Added` entry must carry its own `firstRowId` (assigned when it was
                // first added); a null here is malformed.
                None if status.is_live() => {
                    return Err(Error::missing_data(format!(
                        "AMT content-tree live non-Added leaf entry has a null '{FIRST_ROW_ID}'"
                    )));
                }
                // A dropped (not-live) entry's `firstRowId` is irrelevant; leave it null.
                None => Scalar::Null(DataType::LONG),
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
        use crate::engine::arrow_conversion::TryIntoArrow as _;
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

        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[deleted, manifest, live]).as_ref(),
                &read_ctx(),
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
    #[case(TrackingStatus::Deleted, false)]
    #[case(TrackingStatus::Replaced, false)]
    fn selects_only_live_data_entries(#[case] status: TrackingStatus, #[case] kept: bool) {
        let engine = SyncEngine::new();
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        entry.tracking.status = status;
        let out = filtered_to_batch(
            convert_root_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[entry]).as_ref(),
                &read_ctx(),
            )
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
        let result = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[entry]).as_ref(),
            &read_ctx(),
        );
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
                &read_ctx(),
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
                &read_ctx(),
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
                &read_ctx(),
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
                &read_ctx(),
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
                &read_ctx(),
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
    fn leaf_dropped_entry_with_own_first_row_id_does_not_advance_cursor() {
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
                &read_ctx(),
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

    #[test]
    fn leaf_existing_entry_with_own_ids_does_not_advance_cursor() {
        let engine = SyncEngine::new();
        // An Existing entry carries its own firstRowId/sequenceNumber (assigned when it was added),
        // so it keeps them and does not advance the cursor; the following Added entry gets the
        // seed.
        let mut existing = leaf_entry("e.parquet", 1, 7, Some(3), Some(9), Some(500));
        existing.tracking.status = TrackingStatus::Existing;
        let added = leaf_entry("a.parquet", 1, 10, None, None, None);
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[existing, added]).as_ref(),
                &read_ctx(),
            )
            .unwrap(),
        );
        // Existing keeps firstRowId 500 and its own sequence 9; Added takes the seed 100 and
        // inherits the parent's sequence 5.
        let expected = expected_batch(
            &engine,
            &[
                expected_add_row("e.parquet", 1, 500, 9),
                expected_add_row("a.parquet", 1, 100, 5),
            ],
        );
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    #[case::null_first_row_id(None, Some(9), FIRST_ROW_ID)]
    #[case::null_sequence(Some(500), None, SEQUENCE_NUMBER)]
    fn leaf_live_non_added_entry_with_null_inherited_field_errors(
        #[case] first_row_id: Option<i64>,
        #[case] sequence_number: Option<i64>,
        #[case] expected_field: &str,
    ) {
        // A live Existing entry must carry its own firstRowId and sequenceNumber -- only Added
        // inherits them, so a null here is rejected rather than silently filled from the parent.
        let engine = SyncEngine::new();
        let mut existing = leaf_entry("e.parquet", 1, 7, Some(3), sequence_number, first_row_id);
        existing.tracking.status = TrackingStatus::Existing;
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let result = ctx.convert_leaf_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[existing]).as_ref(),
            &read_ctx(),
        );
        assert_result_error_with_message(result, expected_field);
    }

    #[rstest]
    #[case::negative(-1, RECORD_COUNT)]
    #[case::overflow(i64::MAX, "overflow")]
    fn leaf_added_entry_bad_record_count_errors(
        #[case] record_count: i64,
        #[case] expected_message: &str,
    ) {
        // An Added entry needing firstRowId assignment rejects a negative recordCount and a cursor
        // overflow (seed 100 + i64::MAX).
        let engine = SyncEngine::new();
        let entry = leaf_entry("a.parquet", 1, record_count, None, None, None);
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let result = ctx.convert_leaf_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[entry]).as_ref(),
            &read_ctx(),
        );
        assert_result_error_with_message(result, expected_message);
    }

    #[test]
    fn leaf_dropped_entry_with_null_first_row_id_does_not_advance_cursor() {
        let engine = SyncEngine::new();
        // A dropped Deleted entry may carry a null firstRowId; it is left null and does not advance
        // the cursor, so the following Added entry still gets the next seed.
        let a = leaf_entry("a.parquet", 1, 10, None, None, None);
        let mut deleted = leaf_entry("del.parquet", 1, 7, None, None, None);
        deleted.tracking.status = TrackingStatus::Deleted;
        let c = leaf_entry("c.parquet", 1, 20, None, None, None);
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[a, deleted, c]).as_ref(),
                &read_ctx(),
            )
            .unwrap(),
        );
        // `a` -> 100 (advance to 110); the Deleted entry is dropped and does not advance; `c` ->
        // 110.
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
    #[case::sequence(Some(7), None, Some(5), Some(100), SEQUENCE_NUMBER)]
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
        let result = ctx.convert_leaf_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[entry]).as_ref(),
            &read_ctx(),
        );
        assert_result_error_with_message(result, "deletion vector");
    }

    #[test]
    fn leaf_empty_batch_yields_empty_and_preserves_cursor() {
        let engine = SyncEngine::new();
        let mut ctx = LeafReadContext::new(&valid_parent()).unwrap();
        let empty = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[]).as_ref(),
                &read_ctx(),
            )
            .unwrap(),
        );
        assert_eq!(empty.len(), 0);

        // The seeded cursor is untouched by the empty batch, so the next entry still starts at 100.
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
                &engine,
                entry_batch(&engine, &[leaf_entry("a.parquet", 1, 10, None, None, None)]).as_ref(),
                &read_ctx(),
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
