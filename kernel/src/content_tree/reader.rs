//! Read-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates the content-tree entries of an AMT root or leaf manifest into `Add` file actions --
//! the inverse of [`super::builder`]. This is the minimal read path: a root manifest's live `Data`
//! entries become `Add` actions and every other entry is dropped; a leaf manifest may contain only
//! `Data` entries and errors otherwise. Statistics, partition values, tags, and deletion vectors
//! are not yet carried across (see the per-field TODOs); the only entry data that flows into the
//! action is the file location, size, and row-tracking numbers. `modificationTime` and
//! `dataChange` have no AMT source and are supplied by the caller via [`ReadContext`].

use std::sync::{Arc, LazyLock};

use crate::actions::{ADD_NAME, ADD_SCHEMA, LOG_ADD_SCHEMA};
use crate::content_tree::{
    struct_expr_from_schema, ContentTreeNodeEntry, DataContentType, DataFileFormat, TrackingInfo,
    TrackingStatus, CONTENT_TYPE, DV_INFO, DV_SNAPSHOT_ID, FILE_FORMAT, FILE_SIZE_IN_BYTES,
    FIRST_ROW_ID, LOCATION, PARTITION_SPEC_ID, RECORD_COUNT, SEQUENCE_NUMBER, TRACKING,
    TRACKING_STATUS,
};
use crate::engine_data::{EngineData, FilteredEngineData, GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{lit, ArrayData, ColumnName, Expression, MapData, Scalar};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DATA_CHANGE_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, MODIFICATION_TIME_NAME,
    PARTITION_VALUES_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::{
    ArrayType, ColumnNamesAndTypes, DataType, MapType, SchemaRef, StructField, StructType,
    ToSchema as _,
};
use crate::{Engine, ExpressionEvaluator, KernelError, KernelResult};

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

/// Expressions for the `Add` row-tracking fields whose source differs between root and leaf
/// manifests (a leaf entry inherits them from its parent).
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

/// Column appended to a leaf entry batch holding each entry's resolved `firstRowId`; read as
/// `Add.baseRowId`.
const FIRST_ROW_ID_HELPER: &str = "_firstRowId";

/// Schema of the appended [`FIRST_ROW_ID_HELPER`] column.
static FIRST_ROW_ID_HELPER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new_unchecked([StructField::nullable(
        FIRST_ROW_ID_HELPER,
        DataType::LONG,
    )]))
});

/// Leaf converter input schema: the entry schema plus [`FIRST_ROW_ID_HELPER`].
static LEAF_INPUT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut fields: Vec<StructField> = ContentTreeNodeEntry::to_schema()
        .fields()
        .cloned()
        .collect();
    fields.push(StructField::nullable(FIRST_ROW_ID_HELPER, DataType::LONG));
    Arc::new(StructType::new_unchecked(fields))
});

/// Converts content-tree entry batches into `Add`-action batches. The entry->`Add` evaluator is
/// built once, so one converter can be reused across every batch of a manifest.
pub(crate) struct EntryConverter {
    evaluator: Arc<dyn ExpressionEvaluator>,
}

impl EntryConverter {
    /// Translates a content-tree entry batch into an `Add`-action batch, keeping only the rows that
    /// read as live data files.
    ///
    /// An entry becomes an `Add` when its `contentType` is [`DataContentType::Data`] and its
    /// tracking status is [live](TrackingStatus::is_live); every other entry is dropped via the
    /// returned selection vector. [`DataContentType::DataManifest`] entries are dropped too:
    /// expanding a leaf manifest into its data files is the caller's responsibility.
    ///
    /// # Parameters
    /// - `entries`: a content-tree entry batch matching the converter's input schema (for a root
    ///   converter, [`ContentTreeNodeEntry::to_schema`], the columnar form produced by
    ///   [`super::builder`]).
    ///
    /// # Returns
    /// A [`FilteredEngineData`] over an `Add`-action batch (one row per input entry, schema
    /// [`crate::actions::LOG_ADD_SCHEMA`]), whose selection vector keeps only the entries that read
    /// as live data files.
    ///
    /// # Errors
    /// Returns an error if an entry carries an unknown content-type or tracking-status value, if a
    /// delete entry (position/equality deletes or a delete manifest) is live, if a selected (live
    /// `Data`) entry is not Parquet, has a non-zero partition spec id, or carries a deletion vector
    /// (none yet supported by the read path), if the evaluator fails to evaluate, or if the
    /// selection vector length exceeds the batch.
    pub(crate) fn convert(&self, entries: &dyn EngineData) -> KernelResult<FilteredEngineData> {
        let mut selector = AddSelectionVisitor::default();
        selector.visit_rows_of(entries)?;
        let actions = self.evaluator.evaluate(entries)?;
        FilteredEngineData::try_new(actions, selector.selection)
    }
}

/// Builds an [`EntryConverter`] for an AMT root manifest's content-tree entries.
///
/// # Parameters
/// - `engine`: provides the [`crate::EvaluationHandler`] used to build the evaluator.
/// - `ctx`: the per-read values the entry cannot supply (see [`ReadContext`]).
///
/// # Errors
/// Returns an error if the evaluator cannot be constructed.
pub(crate) fn make_root_entry_converter(
    engine: &dyn Engine,
    ctx: &ReadContext,
) -> KernelResult<EntryConverter> {
    make_entry_converter(
        engine,
        ctx,
        &AddFieldSources::root(),
        Arc::new(ContentTreeNodeEntry::to_schema()),
    )
}

/// Converts the entry batches of one leaf manifest into `Add` actions, filling null tracking
/// fields from the root `DataManifest` entry that references the leaf.
pub(crate) struct LeafReadContext {
    /// Next unassigned `firstRowId`.
    next_first_row_id: i64,
    converter: EntryConverter,
}

impl LeafReadContext {
    /// Creates a context for the leaf referenced by a root `DataManifest` entry.
    ///
    /// # Parameters
    /// - `engine`: provides the evaluation handler for the entry->`Add` transform.
    /// - `parent`: tracking info of the root `DataManifest` entry referencing the leaf.
    /// - `ctx`: the per-read values the entry cannot supply (see [`ReadContext`]).
    ///
    /// # Errors
    /// Returns an error if `parent` has a null `sequenceNumber` or `firstRowId`, or if the
    /// evaluator cannot be constructed.
    pub(crate) fn new(
        engine: &dyn Engine,
        parent: &TrackingInfo,
        ctx: &ReadContext,
    ) -> KernelResult<Self> {
        let require = |value: Option<i64>, field: &str| {
            value.ok_or_else(|| {
                KernelError::missing_data(format!(
                    "AMT parent DataManifest entry is missing required tracking field '{field}'"
                ))
            })
        };
        let parent_sequence_number = require(parent.sequence_number, SEQUENCE_NUMBER)?;
        let sources = AddFieldSources {
            base_row_id: Expression::column([FIRST_ROW_ID_HELPER]),
            default_row_commit_version: Expression::coalesce([
                Expression::column([TRACKING, SEQUENCE_NUMBER]),
                lit(parent_sequence_number),
            ]),
        };
        Ok(Self {
            next_first_row_id: require(parent.first_row_id, FIRST_ROW_ID)?,
            converter: make_entry_converter(engine, ctx, &sources, LEAF_INPUT_SCHEMA.clone())?,
        })
    }

    /// Converts one batch of leaf entries into an `Add`-action batch, like
    /// [`EntryConverter::convert`]. Batches must be passed in manifest order, since null
    /// `firstRowId`s are assigned sequentially across them.
    ///
    /// The parent's manifest deletion vector is not applied; the caller must apply it to the
    /// returned selection.
    ///
    /// # Errors
    /// Returns an error if an entry is malformed for a leaf (see [`FirstRowIdVisitor`]), or any
    /// error from [`EntryConverter::convert`].
    pub(crate) fn convert_leaf_entries_to_add_actions(
        &mut self,
        entries: &dyn EngineData,
    ) -> KernelResult<FilteredEngineData> {
        // Only update `self` once the whole batch succeeds, so a retried batch gets the same IDs.
        let (first_row_ids, next_first_row_id) =
            assign_first_row_ids(entries, self.next_first_row_id)?;

        let helper_column =
            ArrayData::try_new(ArrayType::new(DataType::LONG, true), first_row_ids)?;
        let augmented =
            entries.append_columns(FIRST_ROW_ID_HELPER_SCHEMA.clone(), vec![helper_column])?;

        let filtered = self.converter.convert(augmented.as_ref())?;
        self.next_first_row_id = next_first_row_id;
        Ok(filtered)
    }
}

// === Helpers ===

/// Shared constructor for [`EntryConverter`]: builds the entry->`Add` transform (with the caller's
/// [`AddFieldSources`]) and its evaluator.
///
/// `input_schema` is the schema the transform evaluates against -- the entry schema for a root
/// batch, or an augmented schema when a caller appends helper columns.
fn make_entry_converter(
    engine: &dyn Engine,
    ctx: &ReadContext,
    sources: &AddFieldSources,
    input_schema: SchemaRef,
) -> KernelResult<EntryConverter> {
    let output_type = DataType::from(LOG_ADD_SCHEMA.as_ref().clone());
    let expr = build_entry_to_add_expression(ctx, sources)?;
    let evaluator = engine.evaluation_handler().new_expression_evaluator(
        input_schema,
        Arc::new(expr),
        output_type,
    )?;
    Ok(EntryConverter { evaluator })
}

/// Resolves each leaf entry's `firstRowId`, starting from `seed`. Returns the per-row values and
/// the next unassigned row ID.
///
/// An entry with a `firstRowId` keeps it. An `Added` entry without one gets the next unassigned
/// ID, which then advances by the entry's `recordCount`.
///
/// # Errors
/// See [`FirstRowIdVisitor`].
fn assign_first_row_ids(entries: &dyn EngineData, seed: i64) -> KernelResult<(Vec<Scalar>, i64)> {
    let mut visitor = FirstRowIdVisitor::new(seed);
    visitor.visit_rows_of(entries)?;
    Ok((visitor.first_row_ids, visitor.next_first_row_id))
}

/// Builds the transform mapping a [`ContentTreeNodeEntry`] row to a `{ add: Add }` struct matching
/// [`crate::actions::LOG_ADD_SCHEMA`].
///
/// `ctx` supplies the fields the AMT entry cannot; `sources` supplies the row-tracking fields
/// whose derivation differs between root and leaf entries.
fn build_entry_to_add_expression(
    ctx: &ReadContext,
    sources: &AddFieldSources,
) -> KernelResult<Expression> {
    // TODO(#3320): read partition values from the entry's `partition` tuple once the read path
    // carries a partition spec; the AMT root written by the minimal blind-append path is
    // unpartitioned. The map type is taken from the action schema so its value-nullability
    // matches `Add.partitionValues`.
    let empty_partition_values = lit(Scalar::Map(MapData::try_new(
        partition_values_map_type()?,
        Vec::<(Scalar, Scalar)>::new(),
    )?));

    // `struct_expr_from_schema` fills every unmatched (nullable) field with a typed null, so
    // `stats`, `tags`, `deletionVector`, and `clusteringProvider` fall through to null.
    // TODO(#2866): carry statistics (including `numRecords` from `recordCount`), tags, and inline
    // deletion-vector info across from the entry.
    let add = struct_expr_from_schema(&ADD_SCHEMA, |name| {
        Some(match name {
            // TODO(#3319): `location` is the raw path stored in the AMT root; resolving it to a
            // table-relative `Add.path` (percent-decoding, and relativizing against a manifest
            // location for non-root entries) is not yet done -- it flows through verbatim, which
            // round-trips only the minimal-root case that stored the raw path.
            n if n == PATH_NAME => Expression::column([LOCATION]),
            // TODO(#3320): read partition values from the entry's `partition` tuple once the read
            // path carries a partition spec.
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

/// Visitor behind [`assign_first_row_ids`].
///
/// Errors if a leaf entry is not `Data`, has an unknown status, is live and non-`Added` with a
/// null `sequenceNumber` or `firstRowId`, or needs a `firstRowId` but has a negative
/// `recordCount`, or if assignment overflows `i64`.
struct FirstRowIdVisitor {
    /// Next unassigned `firstRowId`.
    next_first_row_id: i64,
    /// Resolved `firstRowId` per row, in entry order.
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
                    ColumnName::new([CONTENT_TYPE]),
                    ColumnName::new([LOCATION]),
                ],
                vec![
                    DataType::LONG,
                    DataType::LONG,
                    DataType::INTEGER,
                    DataType::LONG,
                    DataType::INTEGER,
                    DataType::STRING,
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        self.first_row_ids.reserve(row_count);
        for row in 0..row_count {
            // A leaf manifest holds only data file entries.
            let content_type = DataContentType::try_from_repr(getters[4].get(row, CONTENT_TYPE)?)?;
            if content_type != DataContentType::Data {
                let location: &str = getters[5].get(row, LOCATION)?;
                return Err(KernelError::generic(format!(
                    "AMT leaf manifest entry '{location}' has content type {content_type:?}; \
                     leaf manifests may only contain Data entries"
                )));
            }
            let status = TrackingStatus::try_from_repr(getters[2].get(row, TRACKING_STATUS)?)?;
            // Only `Added` entries may inherit `sequenceNumber`. Dropped entries never become an
            // `Add`, so they are not checked.
            let sequence_number: Option<i64> = getters[3].get_opt(row, SEQUENCE_NUMBER)?;
            if status.is_live() && status != TrackingStatus::Added && sequence_number.is_none() {
                return Err(KernelError::missing_data(format!(
                    "AMT content-tree live non-Added leaf entry has a null '{SEQUENCE_NUMBER}'"
                )));
            }
            let assigned = match getters[1].get_opt(row, FIRST_ROW_ID)? {
                // An already-materialized `firstRowId` is kept and does not consume row IDs.
                Some(existing) => Scalar::Long(existing),
                // Only `Added` entries are assigned a fresh range of `recordCount` row IDs.
                None if status == TrackingStatus::Added => {
                    let record_count: i64 = getters[0].get(row, RECORD_COUNT)?;
                    if record_count < 0 {
                        return Err(KernelError::generic(format!(
                            "AMT content-tree leaf entry has a negative '{RECORD_COUNT}': \
                             {record_count}"
                        )));
                    }
                    let assigned = self.next_first_row_id;
                    self.next_first_row_id =
                        assigned.checked_add(record_count).ok_or_else(|| {
                            KernelError::generic(format!(
                                "AMT content-tree '{FIRST_ROW_ID}' assignment overflowed i64 at \
                             {assigned} + {record_count}"
                            ))
                        })?;
                    Scalar::Long(assigned)
                }
                None if status.is_live() => {
                    return Err(KernelError::missing_data(format!(
                        "AMT content-tree live non-Added leaf entry has a null '{FIRST_ROW_ID}'"
                    )));
                }
                // Dropped entries never become an `Add`.
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
    use crate::content_tree::test_utils::{
        added_data_entry, entry_batch, struct_batch, with_field,
    };
    use crate::content_tree::{DeletionVectorInfo, ManifestInfo};
    use crate::engine::arrow_conversion::TryFromArrow as _;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::StructData;
    use crate::schema::StructType;
    use crate::unit_test_utils::assert_result_error_with_message;

    /// The [`ReadContext`] the tests read against; [`expected_add_row`] reads its values back.
    /// Non-default values so a hard-coded `modificationTime` / `dataChange` would fail.
    fn read_ctx() -> ReadContext {
        ReadContext {
            modification_time: 1234,
            data_change: false,
        }
    }

    /// Builds a root converter and runs it over a single batch.
    fn convert_root_entries_to_add_actions(
        engine: &dyn Engine,
        entries: &dyn EngineData,
        ctx: &ReadContext,
    ) -> KernelResult<FilteredEngineData> {
        make_root_entry_converter(engine, ctx)?.convert(entries)
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
                Scalar::Long(read_ctx().modification_time),
                Scalar::Boolean(read_ctx().data_change),
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
    fn root_entry_converter_is_reusable_across_batches() {
        let engine = SyncEngine::new();
        let converter = make_root_entry_converter(&engine, &read_ctx()).unwrap();
        let mut deleted = added_data_entry("deleted.parquet", 1, 1, 0, 0);
        deleted.tracking.status = TrackingStatus::Deleted;
        let batches = [
            vec![added_data_entry("a.parquet", 100, 10, 0, 5)],
            vec![deleted, added_data_entry("b.parquet", 200, 20, 10, 6)],
        ];
        let expected = [
            (vec![true], expected_add_row("a.parquet", 100, 0, 5)),
            (vec![false, true], expected_add_row("b.parquet", 200, 10, 6)),
        ];

        for (entries, (selection, row)) in batches.iter().zip(expected) {
            let filtered = converter
                .convert(entry_batch(&engine, entries).as_ref())
                .unwrap();
            assert_eq!(filtered.selection_vector(), selection.as_slice());
            assert_eq!(
                filtered_to_batch(filtered).try_into_record_batch().unwrap(),
                expected_batch(&engine, &[row])
                    .try_into_record_batch()
                    .unwrap()
            );
        }
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
        let schema = StructType::try_from_arrow(out.schema().as_ref()).unwrap();
        assert_eq!(&schema, LOG_ADD_SCHEMA.as_ref());
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
    #[case(TrackingStatus::Modified, true)]
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
    #[case::added_dv_struct(TrackingStatus::Added, true, false)]
    #[case::added_dv_snapshot_id(TrackingStatus::Added, false, true)]
    #[case::modified_dv_struct(TrackingStatus::Modified, true, false)]
    fn live_entry_with_deletion_vector_errors(
        #[case] status: TrackingStatus,
        #[case] set_deletion_vector: bool,
        #[case] set_dv_snapshot_id: bool,
    ) {
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        entry.tracking.status = status;
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

    // === Leaf read path ===

    /// A leaf `Data`/`Added` entry; `sequence_number` sets both sequence fields.
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

    /// Returns `entry` with its tracking status set to `status`.
    fn with_status(
        mut entry: ContentTreeNodeEntry,
        status: TrackingStatus,
    ) -> ContentTreeNodeEntry {
        entry.tracking.status = status;
        entry
    }

    /// Tracking info for a root `DataManifest` entry referencing a leaf.
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
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &entries).as_ref())
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
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &entries).as_ref())
                .unwrap(),
        );
        let expected = expected_batch(&engine, &[expected_add_row("a.parquet", 100, 500, 99)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    /// `preceding` entries need no `firstRowId` assignment, so the trailing `Added` null-id entry
    /// is still assigned the parent's seed 100. `expected_preceding` lists the surviving
    /// `(path, baseRowId, defaultRowCommitVersion)` of `preceding`.
    #[rstest]
    #[case::added_own_id(
        vec![leaf_entry("p.parquet", 1, 7, None, None, Some(500))],
        vec![("p.parquet", 500, 5)]
    )]
    #[case::existing_own_ids(
        vec![with_status(leaf_entry("p.parquet", 1, 7, Some(3), Some(9), Some(500)), TrackingStatus::Existing)],
        vec![("p.parquet", 500, 9)]
    )]
    #[case::modified_own_ids(
        vec![with_status(leaf_entry("p.parquet", 1, 7, Some(3), Some(9), Some(500)), TrackingStatus::Modified)],
        vec![("p.parquet", 500, 9)]
    )]
    #[case::deleted_own_id(
        vec![with_status(leaf_entry("p.parquet", 1, 7, None, None, Some(999)), TrackingStatus::Deleted)],
        vec![]
    )]
    #[case::deleted_null_id(
        vec![with_status(leaf_entry("p.parquet", 1, 7, None, None, None), TrackingStatus::Deleted)],
        vec![]
    )]
    #[case::replaced_null_id(
        vec![with_status(leaf_entry("p.parquet", 1, 7, None, None, None), TrackingStatus::Replaced)],
        vec![]
    )]
    #[case::mixed_order(
        vec![
            with_status(leaf_entry("e.parquet", 1, 7, Some(3), Some(9), Some(500)), TrackingStatus::Existing),
            with_status(leaf_entry("d.parquet", 1, 7, None, None, None), TrackingStatus::Deleted),
        ],
        vec![("e.parquet", 500, 9)]
    )]
    fn leaf_entry_not_needing_assignment_does_not_advance_next_row_id(
        #[case] preceding: Vec<ContentTreeNodeEntry>,
        #[case] expected_preceding: Vec<(&str, i64, i64)>,
    ) {
        let engine = SyncEngine::new();
        let mut entries = preceding;
        entries.push(leaf_entry("a.parquet", 1, 10, None, None, None));
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &entries).as_ref())
                .unwrap(),
        );
        let expected_rows: Vec<_> = expected_preceding
            .into_iter()
            .chain([("a.parquet", 100, 5)])
            .map(|(path, base_row_id, version)| expected_add_row(path, 1, base_row_id, version))
            .collect();
        let expected = expected_batch(&engine, &expected_rows);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn first_row_id_assignment_continues_across_batches() {
        let engine = SyncEngine::new();
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();

        let batch1 = [
            leaf_entry("a.parquet", 1, 10, None, None, None),
            leaf_entry("b.parquet", 1, 20, None, None, None),
        ];
        let out1 = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &batch1).as_ref())
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

        // Assignment continues from the previous batch: 100 + 10 + 20 = 130.
        let batch2 = [leaf_entry("c.parquet", 1, 5, None, None, None)];
        let out2 = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &batch2).as_ref())
                .unwrap(),
        );
        let expected2 = expected_batch(&engine, &[expected_add_row("c.parquet", 1, 130, 5)]);
        assert_eq!(
            out2.try_into_record_batch().unwrap(),
            expected2.try_into_record_batch().unwrap()
        );
    }

    #[rstest]
    #[case::null_first_row_id(None, Some(9), FIRST_ROW_ID)]
    #[case::null_sequence(Some(500), None, SEQUENCE_NUMBER)]
    fn leaf_live_non_added_entry_with_null_inherited_field_errors(
        #[case] first_row_id: Option<i64>,
        #[case] sequence_number: Option<i64>,
        #[case] expected_field: &str,
        #[values(TrackingStatus::Existing, TrackingStatus::Modified)] status: TrackingStatus,
    ) {
        // Only Added entries inherit, so a live non-Added entry with a null field is rejected.
        let engine = SyncEngine::new();
        let entry = with_status(
            leaf_entry("e.parquet", 1, 7, Some(3), sequence_number, first_row_id),
            status,
        );
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let result =
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &[entry]).as_ref());
        assert_result_error_with_message(result, expected_field);
    }

    #[rstest]
    #[case::negative(-1, "negative")]
    #[case::overflow(i64::MAX, "overflow")]
    fn leaf_added_entry_bad_record_count_errors(
        #[case] record_count: i64,
        #[case] expected_message: &str,
    ) {
        // An Added entry needing firstRowId assignment rejects a negative recordCount and a row-ID
        // overflow (seed 100 + i64::MAX).
        let engine = SyncEngine::new();
        let entry = leaf_entry("a.parquet", 1, record_count, None, None, None);
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let result =
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &[entry]).as_ref());
        assert_result_error_with_message(result, expected_message);
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
        let engine = SyncEngine::new();
        let parent = parent_tracking(
            snapshot_id,
            sequence_number,
            file_sequence_number,
            first_row_id,
        );
        let result = LeafReadContext::new(&engine, &parent, &read_ctx());
        assert_result_error_with_message(result, expected_field);
    }

    // Rejected only until the read path supports mapping deletion vectors into `Add`.
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
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let result =
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &[entry]).as_ref());
        assert_result_error_with_message(result, "deletion vector");
    }

    #[rstest]
    #[case::data_manifest(DataContentType::DataManifest)]
    #[case::position_deletes(DataContentType::PositionDeletes)]
    #[case::equality_deletes(DataContentType::EqualityDeletes)]
    #[case::delete_manifest(DataContentType::DeleteManifest)]
    fn leaf_non_data_entry_errors(#[case] content_type: DataContentType) {
        let engine = SyncEngine::new();
        let mut entry = leaf_entry("f.parquet", 10, 5, None, None, None);
        entry.content_type = content_type;
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let result =
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &[entry]).as_ref());
        assert_result_error_with_message(result, &format!("content type {content_type:?}"));
    }

    #[test]
    fn leaf_empty_batch_yields_empty_and_preserves_next_row_id() {
        let engine = SyncEngine::new();
        let mut ctx = LeafReadContext::new(&engine, &valid_parent(), &read_ctx()).unwrap();
        let empty = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(entry_batch(&engine, &[]).as_ref())
                .unwrap(),
        );
        assert_eq!(empty.len(), 0);

        // The empty batch consumes no row IDs, so the next entry still starts at 100.
        let out = filtered_to_batch(
            ctx.convert_leaf_entries_to_add_actions(
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
