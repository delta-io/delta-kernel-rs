//! Read-side translation for the Adaptive Metadata Tree (AMT).
//!
//! Translates the content-tree entries of an AMT root or leaf manifest into `Add` file actions --
//! the inverse of [`super::builder`]. This is the minimal read path: a root manifest's live `Data`
//! entries become `Add` actions, its live `DataManifest` entries are expanded into the `Add`
//! actions of the leaf manifests they reference (minus the leaf entries removed by the root entry's
//! manifest deletion vector), and every other root entry is dropped. A leaf manifest may contain
//! only `Data` entries and errors otherwise. Statistics, partition values, and tags are not yet
//! carried across (see the per-field TODOs); the only entry data that flows into the action is the
//! file location, size, row-tracking numbers, and deletion vector.
//! `modificationTime` and `dataChange` have no AMT source and are supplied by the caller via
//! [`ReadContext`].

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, LazyLock};
use std::{iter, slice};

use bytes::Bytes;
use url::Url;

use crate::actions::{ADD_NAME, ADD_SCHEMA, CheckpointAction, LOG_ADD_SCHEMA};
use crate::amt_path_util::amt_manifest_filemeta;
use crate::cancellation::CancellationTokenRef;
use crate::content_tree::dv_conversion::{
    ADD_DV_HELPER_SCHEMA, AddDeletionVectorColumns, add_deletion_vector_expression,
};
use crate::content_tree::mumbling::MumblingBitmap;
use crate::content_tree::{
    AMT_FORMAT_VERSION, COLUMN_FILES, CONTENT_TYPE, ColumnFile, ContentTreeNodeEntry,
    DV_INFO_COLUMNS, DataContentType, DataFileFormat, FILE_FORMAT, FILE_SIZE_IN_BYTES,
    FIRST_ROW_ID, FORMAT_VERSION, LOCATION, MANIFEST_DV, MANIFEST_INFO, MODIFIED_SNAPSHOT_ID,
    PARTITION_SPEC_ID, RECORD_COUNT, SEQUENCE_NUMBER, TRACKING, TRACKING_COLUMNS, TRACKING_STATUS,
    TrackingInfo, TrackingStatus, struct_expr_from_schema,
};
use crate::engine_data::{
    EngineData, GetData, RowVisitor, StructList, TypedGetData as _, filter_by_predicate,
};
use crate::expressions::{
    ArrayData, ColumnName, Expression, ExpressionStructPatchBuilder, MapData, Predicate, Scalar,
    expr, lit, null_lit,
};
use crate::scan::log_replay::{
    BASE_ROW_ID_NAME, DATA_CHANGE_NAME, DEFAULT_ROW_COMMIT_VERSION_NAME, DELETION_VECTOR_NAME,
    MODIFICATION_TIME_NAME, PARTITION_VALUES_NAME, PATH_NAME, SIZE_NAME,
};
use crate::schema::{
    ArrayType, ColumnNamesAndTypes, DataType, MapType, MetadataColumnSpec, SchemaRef, StructField,
    StructType, ToSchema as _, lazy_schema_ref,
};
use crate::{
    Engine, ExpressionEvaluator, FileDataReadResultIterator, FileMeta, KernelError, KernelResult,
    KernelResultIterator, PredicateEvaluator,
};

/// Reads the `Add` actions for every live data file in an AMT checkpoint's content tree.
///
/// Reads the root manifest eagerly and returns the `Add` actions of its live `Data` entries and of
/// every live leaf manifest it references.
///
/// # Parameters
/// - `engine`: provides the parquet and evaluation handlers.
/// - `checkpoint`: the AMT `checkpoint` action whose content tree to read.
/// - `ctx`: the per-read values the entries cannot supply (see [`ReadContext`]).
/// - `cancellation_token`: when cancelled, stops the manifest reads with
///   [`KernelError::Cancelled`].
///
/// # Returns
/// An iterator of `Add`-action batches (schema [`LOG_ADD_SCHEMA`]) holding only live data files.
///
/// # Errors
/// Returns an error if a manifest cannot be read or holds a malformed or unsupported entry. Leaf
/// manifest errors may instead surface while iterating, after which every remaining leaf batch
/// also errors.
pub(crate) fn read_add_actions_from_amt(
    engine: &dyn Engine,
    checkpoint: &CheckpointAction,
    ctx: &ReadContext,
    cancellation_token: Option<&CancellationTokenRef>,
) -> KernelResult<Box<dyn KernelResultIterator<Box<dyn EngineData>>>> {
    let table_root = &ctx.table_root;
    let root = checkpoint.root_filemeta(table_root)?;
    let schemas = ManifestReadSchema::new();
    let converter = make_root_entry_converter(engine, ctx, schemas.root.clone())?;
    let mut root_actions = Vec::new();
    let mut leaves = LeafRefVisitor::new(table_root);
    let batches = engine
        .parquet_handler()
        .read_parquet_files_with_cancellation(
            slice::from_ref(&root),
            schemas.root.clone(),
            None,
            cancellation_token.cloned(),
        )?;
    for batch in batches {
        let batch = batch?;
        leaves.visit_rows_of(batch.as_ref())?;
        root_actions.push(converter.convert(batch.as_ref(), |_| Ok(vec![]))?);
    }
    let leaf_actions =
        LeafToAddActions::try_new(engine, leaves.leaves, schemas.leaf, ctx, cancellation_token)?;
    Ok(Box::new(
        root_actions.into_iter().map(Ok).chain(leaf_actions),
    ))
}

/// Values the read path needs but cannot derive from an AMT entry, so the caller must supply them.
///
/// The context carries the table root that relative entry locations resolve against. The AMT entry
/// also lacks the data file's modification time and a `dataChange` flag, so without this context
/// the reader would have to invent both. Supplying them here keeps the function signature
/// stable once a real source (e.g. the commit timestamp) is threaded through.
pub(crate) struct ReadContext {
    /// The table root URL relative manifest locations resolve against.
    pub(crate) table_root: Url,
    /// Emitted as `Add.modificationTime`.
    pub(crate) modification_time: i64,
    /// Emitted as `Add.dataChange`.
    pub(crate) data_change: bool,
}

/// A leaf manifest referenced by a live root `DataManifest` entry: where to read it, and the
/// parent values its entries inherit or are filtered by.
struct LeafRef {
    /// The resolved leaf manifest file to read.
    file_meta: FileMeta,
    /// Tracking info of the referencing root `DataManifest` entry.
    parent_info: TrackingInfo,
    /// The root entry's `manifestInfo.dv`: the leaf entry positions that are no longer live.
    /// `None` when absent or empty.
    manifest_dv: Option<ManifestDv>,
    /// The root entry's `manifestInfo.formatVersion`: the format version the leaf was written
    /// with.
    format_version: i32,
    /// Whether the root entry carries a non-empty `columnFiles` list: column updates to the leaf.
    has_column_files: bool,
}

/// The columns [`LeafRef::try_from_getters`] reads, in getter order: the root entry's own fields
/// followed by [`TRACKING_COLUMNS`].
static LEAF_REF_COLUMNS: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
    let (tracking_names, tracking_types) = TRACKING_COLUMNS.as_ref();
    let names = [
        ColumnName::new([LOCATION]),
        ColumnName::new([FILE_SIZE_IN_BYTES]),
        ColumnName::new([MANIFEST_INFO, MANIFEST_DV]),
        ColumnName::new([MANIFEST_INFO, FORMAT_VERSION]),
        ColumnName::new([COLUMN_FILES]),
    ];
    let types = [
        DataType::STRING,
        DataType::LONG,
        DataType::BINARY,
        DataType::INTEGER,
        ArrayType::new(ColumnFile::to_schema(), false).into(),
    ];
    let names = names.into_iter().chain(tracking_names.iter().cloned());
    let types = types.into_iter().chain(tracking_types.iter().cloned());
    (names.collect(), types.collect()).into()
});

impl LeafRef {
    /// Parses one root `DataManifest` row from `getters`, which must follow [`LEAF_REF_COLUMNS`]
    /// order. The raw `location` resolves against `table_root` when relative.
    fn try_from_getters<'a>(
        row: usize,
        getters: &[&'a dyn GetData<'a>],
        table_root: &Url,
    ) -> KernelResult<Self> {
        let expected = LEAF_REF_COLUMNS.as_ref().0.len();
        if getters.len() != expected {
            return Err(KernelError::internal_error(format!(
                "Expected {expected} AMT leaf reference getters, got {}",
                getters.len()
            )));
        }
        let (fields, tracking) = getters.split_at(tracking_start(getters));
        let location: &str = fields[0].get(row, LOCATION)?;
        let size: i64 = fields[1].get(row, FILE_SIZE_IN_BYTES)?;
        let file_meta = amt_manifest_filemeta(location, size, table_root, "AMT leaf manifest")?;
        let manifest_dv: Option<&[u8]> = fields[2].get_opt(row, MANIFEST_DV)?;
        let manifest_dv = manifest_dv
            .map(ManifestDv::try_new)
            .transpose()
            .map_err(|e| {
                KernelError::invalid_checkpoint(format!(
                    "DataManifest entry '{}' has a malformed {MANIFEST_INFO}.{MANIFEST_DV}: {e}",
                    file_meta.location
                ))
            })?
            .flatten();
        Ok(Self {
            file_meta,
            parent_info: TrackingInfo::try_from_getters(row, tracking)?,
            manifest_dv,
            format_version: fields[3].get(row, FORMAT_VERSION)?,
            has_column_files: has_column_files(fields[4].get_struct_list(row, COLUMN_FILES)?)?,
        })
    }

    /// Rejects a leaf the read path cannot yet read: one written before Iceberg v4, or one whose
    /// parent has column files, which the read path cannot yet apply to the leaf's entries.
    fn check_readable(&self) -> KernelResult<()> {
        let location = &self.file_meta.location;
        if self.format_version != AMT_FORMAT_VERSION {
            return Err(KernelError::unsupported(format!(
                "AMT read path only supports format version {AMT_FORMAT_VERSION} leaf \
                 manifests; DataManifest entry '{location}' has {MANIFEST_INFO}.{FORMAT_VERSION} {}",
                self.format_version
            )));
        }
        if self.has_column_files {
            return Err(KernelError::unsupported(format!(
                "AMT read path does not yet support column files on a leaf manifest; \
                 DataManifest entry '{location}' has {COLUMN_FILES} set"
            )));
        }
        Ok(())
    }
}

/// A non-empty manifest deletion vector: serialized for [`Predicate::manifest_dv_contains`], plus
/// the largest position it removes, which [`LeafReadContext::finish`] checks against the leaf.
struct ManifestDv {
    bytes: Vec<u8>,
    last_position: u32,
}

impl ManifestDv {
    /// Parses `bytes`, returning `None` for a deletion vector that removes nothing.
    fn try_new(bytes: &[u8]) -> KernelResult<Option<Self>> {
        let positions = MumblingBitmap::deserialize(Bytes::copy_from_slice(bytes))?;
        Ok(positions.iter().max().map(|last_position| Self {
            bytes: bytes.to_vec(),
            last_position,
        }))
    }
}

/// Name of the leaf read's `_file` metadata column, which tags each batch with the leaf it came
/// from.
const LEAF_FILE_PATH_COLUMN: &str = "_file";

/// Name of the leaf read's row-index metadata column: each entry's position within its leaf, the
/// position a manifest deletion vector or backreference refers to.
const LEAF_ROW_INDEX_COLUMN: &str = "_row_index";

/// The schemas AMT root and leaf manifests are read with.
struct ManifestReadSchema {
    /// Root manifest read schema: [`ContentTreeNodeEntry::to_schema`].
    root: SchemaRef,
    /// Leaf manifest read schema: [`Self::root`] plus the [`LEAF_FILE_PATH_COLUMN`] and
    /// [`LEAF_ROW_INDEX_COLUMN`] metadata columns. Root entries need neither: they are never
    /// addressed by position.
    leaf: SchemaRef,
}

impl ManifestReadSchema {
    /// Builds the manifest read schemas.
    fn new() -> Self {
        let entry_schema = Arc::new(ContentTreeNodeEntry::to_schema());
        let metadata = [
            StructField::create_metadata_column(
                LEAF_FILE_PATH_COLUMN,
                MetadataColumnSpec::FilePath,
            ),
            StructField::create_metadata_column(
                LEAF_ROW_INDEX_COLUMN,
                MetadataColumnSpec::RowIndex,
            ),
        ];
        let leaf = StructType::new_unchecked(entry_schema.fields().cloned().chain(metadata));
        Self {
            root: entry_schema,
            leaf: Arc::new(leaf),
        }
    }
}

/// What differs between converting root and leaf manifest entries to `Add` actions: the source of
/// `Add.baseRowId` (a leaf entry may need one assigned), and the helper columns it reads.
struct AddFieldSources {
    /// Expression producing `Add.baseRowId`.
    base_row_id: Expression,
    /// Columns the caller of [`EntryConverter::convert`] supplies for `base_row_id`.
    helpers: SchemaRef,
}

impl AddFieldSources {
    /// Root-manifest sources: read `tracking.firstRowId` straight off the entry, since a root entry
    /// always carries it.
    fn root() -> Self {
        Self {
            base_row_id: Expression::column([TRACKING, FIRST_ROW_ID]),
            helpers: Arc::new(StructType::new_unchecked([])),
        }
    }

    /// Leaf-manifest sources: read the `firstRowId` the reader resolves for each entry.
    fn leaf() -> Self {
        Self {
            base_row_id: Expression::column([FIRST_ROW_ID_HELPER]),
            helpers: FIRST_ROW_ID_HELPER_SCHEMA.clone(),
        }
    }
}

/// Column appended to a leaf entry batch holding each entry's resolved `firstRowId`; read as
/// `Add.baseRowId`.
const FIRST_ROW_ID_HELPER: &str = "_firstRowId";

/// Schema of the appended [`FIRST_ROW_ID_HELPER`] column.
static FIRST_ROW_ID_HELPER_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable FIRST_ROW_ID_HELPER: LONG,
};

/// Column holding whether the manifest deletion vector of the entry's leaf removes the entry.
/// Always false for a root entry.
const REMOVED_COLUMN: &str = "_removed";

/// Column holding the `sequenceNumber` a leaf entry inherits from the root entry referencing its
/// leaf. Null for a root entry.
const PARENT_SEQUENCE_NUMBER_COLUMN: &str = "_parentSequenceNumber";

/// Schema of the columns [`EntryConverter::convert`] appends from a manifest's
/// [`ManifestValues`], before anything else reads the batch.
static MANIFEST_VALUES_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable REMOVED_COLUMN: BOOLEAN,
    nullable PARENT_SEQUENCE_NUMBER_COLUMN: LONG,
};

/// Column holding whether an entry becomes an `Add` (see [`selected_predicate`]).
const SELECTED_COLUMN: &str = "_selected";

/// Schema of the appended [`SELECTED_COLUMN`].
static SELECTED_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable SELECTED_COLUMN: BOOLEAN,
};

/// Values shared by every entry of one manifest, which [`EntryConverter::convert`] appends to each
/// entry batch as the [`MANIFEST_VALUES_SCHEMA`] columns.
struct ManifestValues {
    /// Whether an entry is removed, becoming the [`REMOVED_COLUMN`] value.
    removed: Predicate,
    /// Becomes the [`PARENT_SEQUENCE_NUMBER_COLUMN`] value.
    parent_sequence_number: Option<i64>,
}

impl ManifestValues {
    /// Root entries are never removed and have no parent.
    fn root() -> Self {
        Self {
            removed: Predicate::FALSE,
            parent_sequence_number: None,
        }
    }

    /// The struct patch appending these values to an entry batch.
    fn into_patch(self) -> KernelResult<Expression> {
        let parent_sequence_number = match self.parent_sequence_number {
            Some(value) => lit(value),
            None => null_lit(DataType::LONG),
        };
        Expression::struct_patch(
            ExpressionStructPatchBuilder::new()
                .append(Arc::new(Expression::from(self.removed)))
                .append(Arc::new(parent_sequence_number)),
        )
    }
}

/// The evaluators that convert the entry batches of one kind of manifest (root or leaf) into
/// `Add`-action batches. They are built once per read and shared by every manifest of that kind,
/// whose own values each [`EntryConverter`] appends.
struct EntryTransform {
    /// The schema entry batches are read with.
    read_schema: SchemaRef,
    /// [`Self::read_schema`] followed by the [`MANIFEST_VALUES_SCHEMA`] columns.
    marked_schema: SchemaRef,
    /// Appends [`SELECTED_COLUMN`] to a batch with the [`MANIFEST_VALUES_SCHEMA`] columns.
    selector: Arc<dyn ExpressionEvaluator>,
    /// Keeps the rows of a batch whose [`SELECTED_COLUMN`] is true.
    selection_filter: Arc<dyn PredicateEvaluator>,
    /// The caller's [`AddFieldSources::helpers`] followed by the [`ADD_DV_HELPER_SCHEMA`] columns
    /// `Add.deletionVector` is built from.
    helper_schema: SchemaRef,
    /// Evaluates an entry batch with [`SELECTED_COLUMN`] and the [`Self::helper_schema`] columns
    /// (see [`add_deletion_vector_expression`]).
    evaluator: Arc<dyn ExpressionEvaluator>,
}

impl EntryTransform {
    /// Builds the evaluators for entry batches read with `read_schema`.
    fn try_new(
        engine: &dyn Engine,
        ctx: &ReadContext,
        sources: AddFieldSources,
        read_schema: SchemaRef,
    ) -> KernelResult<Arc<Self>> {
        let handler = engine.evaluation_handler();
        let marked_schema = schema_with(&read_schema, [MANIFEST_VALUES_SCHEMA.as_ref()]);
        let selected_schema = schema_with(&marked_schema, [SELECTED_SCHEMA.as_ref()]);
        let selector = handler.new_expression_evaluator(
            marked_schema.clone(),
            Arc::new(Expression::struct_patch(
                ExpressionStructPatchBuilder::new()
                    .append(Arc::new(Expression::from(selected_predicate()))),
            )?),
            selected_schema.as_ref().clone().into(),
        )?;
        let helper_schema = schema_with(&sources.helpers, [ADD_DV_HELPER_SCHEMA.as_ref()]);
        let augmented_schema = schema_with(&selected_schema, [helper_schema.as_ref()]);
        let selection_filter = handler.new_predicate_evaluator(
            augmented_schema.clone(),
            Arc::new(Predicate::from_expr(Expression::column([SELECTED_COLUMN]))),
        )?;
        let evaluator = handler.new_expression_evaluator(
            augmented_schema,
            Arc::new(build_entry_to_add_expression(ctx, &sources)?),
            DataType::from(LOG_ADD_SCHEMA.as_ref().clone()),
        )?;
        Ok(Arc::new(Self {
            read_schema,
            marked_schema,
            selector,
            selection_filter,
            helper_schema,
            evaluator,
        }))
    }

    /// Builds the converter for one manifest whose entries share `values`.
    fn converter(
        self: &Arc<Self>,
        engine: &dyn Engine,
        values: ManifestValues,
    ) -> KernelResult<EntryConverter> {
        let marker = engine.evaluation_handler().new_expression_evaluator(
            self.read_schema.clone(),
            Arc::new(values.into_patch()?),
            self.marked_schema.as_ref().clone().into(),
        )?;
        Ok(EntryConverter {
            marker,
            transform: self.clone(),
        })
    }
}

/// Converts the content-tree entry batches of one manifest into `Add`-action batches. One
/// converter can be reused across every batch of the manifest.
struct EntryConverter {
    /// Appends the manifest's [`ManifestValues`] to an entry batch.
    marker: Arc<dyn ExpressionEvaluator>,
    transform: Arc<EntryTransform>,
}

impl EntryConverter {
    /// Translates a content-tree entry batch, read with the converter's read schema, into an
    /// `Add`-action batch holding only the entries that read as live data files. `helper_columns`
    /// computes the caller's [`AddFieldSources::helpers`] from the batch with the
    /// [`MANIFEST_VALUES_SCHEMA`] and [`SELECTED_COLUMN`] columns appended.
    ///
    /// An entry becomes an `Add` when its `contentType` is [`DataContentType::Data`], its tracking
    /// status is [live](TrackingStatus::is_live), and it is not removed; every other entry is
    /// dropped. [`DataContentType::DataManifest`] entries are dropped too: expanding a leaf
    /// manifest into its data files is the caller's responsibility. Removed rows skip the
    /// `Add`-specific checks. The result has schema [`crate::actions::LOG_ADD_SCHEMA`].
    ///
    /// # Errors
    /// Returns an error if an entry carries an unknown content-type or tracking-status value, if a
    /// delete manifest entry is live, if a selected (live `Data`) entry is not Parquet, has a
    /// non-zero partition spec id, carries column files, a `modifiedSnapshotId`, or a deletion
    /// vector with no `Add` equivalent, or if an evaluator fails to evaluate.
    fn convert(
        &self,
        entries: &dyn EngineData,
        helper_columns: impl FnOnce(&dyn EngineData) -> KernelResult<Vec<ArrayData>>,
    ) -> KernelResult<Box<dyn EngineData>> {
        let transform = &self.transform;
        let marked = self.marker.evaluate(entries)?;
        let selected = transform.selector.evaluate(marked.as_ref())?;
        let mut columns = helper_columns(selected.as_ref())?;
        let mut visitor = AddSelectionVisitor::default();
        visitor.visit_rows_of(selected.as_ref())?;
        columns.extend(visitor.deletion_vectors.into_columns()?);
        let augmented = selected.append_columns(transform.helper_schema.clone(), columns)?;
        let adds = filter_by_predicate(transform.selection_filter.as_ref(), augmented)?;
        transform.evaluator.evaluate(adds.as_ref())
    }
}

/// Builds an [`EntryConverter`] for an AMT root manifest's content-tree entries, read with
/// `read_schema` (see [`ManifestReadSchema::root`]).
///
/// # Errors
/// Returns an error if an evaluator cannot be constructed.
fn make_root_entry_converter(
    engine: &dyn Engine,
    ctx: &ReadContext,
    read_schema: SchemaRef,
) -> KernelResult<EntryConverter> {
    EntryTransform::try_new(engine, ctx, AddFieldSources::root(), read_schema)?
        .converter(engine, ManifestValues::root())
}

/// Converts the entry batches of one leaf manifest into `Add` actions, filling null tracking
/// fields from the root `DataManifest` entry that references the leaf and dropping the entries its
/// manifest deletion vector removes.
struct LeafReadContext {
    /// Next unassigned `firstRowId`.
    next_first_row_id: i64,
    converter: EntryConverter,
    /// The largest position the parent's manifest deletion vector removes from this leaf.
    last_removed_position: Option<u32>,
    /// Number of entries converted so far, which is also the `_row_index` the next entry must
    /// have.
    entry_count: usize,
}

impl LeafReadContext {
    /// Creates a context for the leaf referenced by a root `DataManifest` entry with tracking info
    /// `parent` and manifest deletion vector `manifest_dv`, converting with `transform` (built with
    /// [`AddFieldSources::leaf`]).
    ///
    /// # Errors
    /// Returns an error if `parent` has a null `sequenceNumber` or `firstRowId`, or if the
    /// evaluator cannot be constructed.
    fn new(
        engine: &dyn Engine,
        transform: &Arc<EntryTransform>,
        parent: &TrackingInfo,
        manifest_dv: Option<ManifestDv>,
    ) -> KernelResult<Self> {
        // Root entries always carry explicit tracking values since nothing above them can be
        // inherited from, so a null here is a malformed root rather than a value to inherit.
        let require = |value: Option<i64>, field: &str| {
            value.ok_or_else(|| {
                KernelError::missing_data(format!(
                    "AMT parent DataManifest entry is missing required tracking field '{field}'"
                ))
            })
        };
        let parent_sequence_number = require(parent.sequence_number, SEQUENCE_NUMBER)?;
        let next_first_row_id = require(parent.first_row_id, FIRST_ROW_ID)?;
        let last_removed_position = manifest_dv.as_ref().map(|dv| dv.last_position);
        let removed = match manifest_dv {
            Some(dv) => Predicate::manifest_dv_contains(
                lit(Scalar::Binary(dv.bytes)),
                Expression::column([LEAF_ROW_INDEX_COLUMN]),
            ),
            None => Predicate::FALSE,
        };
        let values = ManifestValues {
            removed,
            parent_sequence_number: Some(parent_sequence_number),
        };
        Ok(Self {
            next_first_row_id,
            converter: transform.converter(engine, values)?,
            last_removed_position,
            entry_count: 0,
        })
    }

    /// Converts one batch of leaf entries into an `Add`-action batch, like
    /// [`EntryConverter::convert`]. Batches must be passed in manifest order, since null
    /// `firstRowId`s are assigned sequentially across them.
    ///
    /// Entries the manifest deletion vector removes are dropped, but they still take part in
    /// `firstRowId` assignment, so a surviving entry's row IDs do not change when an earlier entry
    /// is removed. They are validated like any other leaf entry, except that their `Add`-specific
    /// checks are skipped.
    ///
    /// # Errors
    /// Returns an error if an entry is malformed for a leaf (see [`LeafEntryVisitor`]), or any
    /// error from [`EntryConverter::convert`].
    fn convert_leaf_entries_to_add_actions(
        &mut self,
        entries: &dyn EngineData,
    ) -> KernelResult<Box<dyn EngineData>> {
        // Only update `self` once the whole batch succeeds, so a failed batch leaves
        // `next_first_row_id` and `entry_count` unchanged.
        let mut next_first_row_id = self.next_first_row_id;
        let adds = self.converter.convert(entries, |marked| {
            let (first_row_ids, next) = validate_and_assign_first_row_ids(
                marked,
                self.next_first_row_id,
                self.entry_count,
            )?;
            next_first_row_id = next;
            Ok(vec![ArrayData::try_new(
                ArrayType::new(DataType::LONG, true),
                first_row_ids,
            )?])
        })?;
        self.next_first_row_id = next_first_row_id;
        self.entry_count += entries.len();
        Ok(adds)
    }

    /// Checks, once all of the leaf's batches are converted, that the manifest deletion vector
    /// removes no position at or past the leaf's entry count.
    ///
    /// # Errors
    /// Returns an `InvalidCheckpoint` error naming `location` if the deletion vector has such a
    /// position, since it then does not describe this leaf.
    fn finish(&self, location: &str) -> KernelResult<()> {
        match self.last_removed_position {
            Some(pos) if usize::try_from(pos).is_ok_and(|pos| pos >= self.entry_count) => {
                Err(KernelError::invalid_checkpoint(format!(
                    "AMT leaf manifest '{location}' has {} entries, but its manifestInfo.dv \
                     removes position {pos}",
                    self.entry_count
                )))
            }
            _ => Ok(()),
        }
    }
}

// === Helpers ===

/// `schema` followed by the fields of each of `extra`.
fn schema_with<'a>(
    schema: &'a StructType,
    extra: impl IntoIterator<Item = &'a StructType>,
) -> SchemaRef {
    let extra_fields = extra.into_iter().flat_map(StructType::fields);
    Arc::new(StructType::new_unchecked(
        schema.fields().chain(extra_fields).cloned(),
    ))
}

/// Index of the first of the [`TRACKING_COLUMNS`] getters that end `getters`.
fn tracking_start(getters: &[&dyn GetData<'_>]) -> usize {
    getters
        .len()
        .saturating_sub(TRACKING_COLUMNS.as_ref().0.len())
}

/// Iterator of `Add`-action batches for all live leaf manifests, read in a single request. The
/// engine is expected to return each leaf's batches together and in request order, each tagged
/// with the leaf's requested location in `_file`.
struct LeafToAddActions {
    /// Leaves whose reads are still in progress, in request order, each with its location and
    /// context. The front is the leaf currently being read.
    pending: VecDeque<(String, LeafReadContext)>,
    /// Batches of all leaves; `None` when there are no leaves.
    batches: Option<FileDataReadResultIterator>,
}

impl LeafToAddActions {
    /// Validates and reads all the leaves, with `read_schema` (see [`ManifestReadSchema::leaf`]),
    /// stopping when `cancellation_token` is cancelled.
    ///
    /// # Errors
    /// Returns an error if two leaves share a location (their batches could not be told apart), if
    /// a leaf cannot yet be read (see [`LeafRef::check_readable`]), if a leaf's
    /// context cannot be built (see [`LeafReadContext::new`]), or if the engine fails to start the
    /// read.
    fn try_new(
        engine: &dyn Engine,
        leaves: Vec<LeafRef>,
        read_schema: SchemaRef,
        ctx: &ReadContext,
        cancellation_token: Option<&CancellationTokenRef>,
    ) -> KernelResult<Self> {
        let mut seen = HashSet::new();
        if let Some(dup) = leaves.iter().find(|l| !seen.insert(&l.file_meta.location)) {
            return Err(KernelError::invalid_checkpoint(format!(
                "AMT root manifest references leaf manifest '{}' more than once",
                dup.file_meta.location
            )));
        }
        if leaves.is_empty() {
            return Ok(Self {
                pending: VecDeque::new(),
                batches: None,
            });
        }
        let transform =
            EntryTransform::try_new(engine, ctx, AddFieldSources::leaf(), read_schema.clone())?;
        let (pending, files): (VecDeque<_>, Vec<_>) = leaves
            .into_iter()
            .map(|leaf| {
                leaf.check_readable()?;
                let location = leaf.file_meta.location.to_string();
                let context =
                    LeafReadContext::new(engine, &transform, &leaf.parent_info, leaf.manifest_dv)
                        .map_err(|e| match e {
                        KernelError::MissingData(msg) => {
                            KernelError::missing_data(format!("{msg} (leaf manifest '{location}')"))
                        }
                        e => e,
                    })?;
                Ok(((location, context), leaf.file_meta))
            })
            .collect::<KernelResult<_>>()?;
        let batches = engine
            .parquet_handler()
            .read_parquet_files_with_cancellation(
                &files,
                read_schema,
                None,
                cancellation_token.cloned(),
            )?;
        Ok(Self {
            pending,
            batches: Some(batches),
        })
    }

    /// Converts one leaf batch with the context of the leaf named by its `_file` value. Leaves
    /// before that one produced no further batches, so they are finished and dropped.
    fn convert_to_add_actions(
        &mut self,
        batch: &dyn EngineData,
    ) -> KernelResult<Box<dyn EngineData>> {
        let file = file_path_of(batch)?;
        while self
            .pending
            .front()
            .is_some_and(|(location, _)| *location != file)
        {
            // The engine returns each leaf's batches together and in request order, so a leaf
            // ahead of this batch's file has no batches left.
            if let Some((location, context)) = self.pending.pop_front() {
                context.finish(&location)?;
            }
        }
        let Some((_, context)) = self.pending.front_mut() else {
            return Err(KernelError::internal_error(format!(
                "AMT leaf manifest batch from '{file}' matches no remaining leaf manifest"
            )));
        };
        context.convert_leaf_entries_to_add_actions(batch)
    }
}

impl Iterator for LeafToAddActions {
    type Item = KernelResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(next) = self.batches.as_mut()?.next() else {
                // Every remaining leaf is finished. Dropping the batches fuses the iterator.
                self.batches = None;
                let result = self
                    .pending
                    .drain(..)
                    .try_for_each(|(location, context)| context.finish(&location));
                return result.err().map(Err);
            };
            let result = match next {
                // An empty batch carries no `_file` to group by, and no actions.
                Ok(batch) if batch.is_empty() => continue,
                Ok(batch) => self.convert_to_add_actions(batch.as_ref()),
                Err(e) => Err(e),
            };
            // Fail every later batch rather than convert it with stale `firstRowId`s.
            if result.is_err() {
                self.pending.clear();
            }
            return Some(result);
        }
    }
}

/// Reads the `_file` value of `batch`'s first row. A batch never spans files, so this names the
/// file of every row.
///
/// # Errors
/// Returns an error if `batch` is empty or its `_file` is null.
fn file_path_of(batch: &dyn EngineData) -> KernelResult<String> {
    let mut visitor = FilePathVisitor::default();
    visitor.visit_rows_of(batch)?;
    visitor
        .file_path
        .ok_or_else(|| KernelError::missing_data("AMT leaf manifest batch has no `_file` value"))
}

/// Captures the first row's [`LEAF_FILE_PATH_COLUMN`] value; see [`file_path_of`].
#[derive(Default)]
struct FilePathVisitor {
    file_path: Option<String>,
}

impl RowVisitor for FilePathVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![ColumnName::new([LEAF_FILE_PATH_COLUMN])],
                vec![DataType::STRING],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        if row_count > 0 && self.file_path.is_none() {
            self.file_path = getters[0].get_opt(0, LEAF_FILE_PATH_COLUMN)?;
        }
        Ok(())
    }
}

/// The entries that become `Add` actions: live, not removed `Data` entries.
fn selected_predicate() -> Predicate {
    let status = || Expression::column([TRACKING, TRACKING_STATUS]);
    let live = TrackingStatus::LIVE.map(|live| Predicate::eq(status(), lit(live)));
    Predicate::and_from([
        Predicate::eq(
            Expression::column([CONTENT_TYPE]),
            lit(DataContentType::Data),
        ),
        Predicate::or_from(live),
        Predicate::not(Predicate::from_expr(Expression::column([REMOVED_COLUMN]))),
    ])
}

/// Validates each leaf entry and resolves its `firstRowId`, starting from `seed`. Returns the
/// per-row values and the next unassigned row ID. `first_position` is the number of the leaf's
/// entries in earlier batches, the `_row_index` the batch's first entry must have.
///
/// An entry with a `firstRowId` keeps it. An `Added` entry without one gets the next unassigned
/// ID, which then advances by the entry's `recordCount`. Removed entries (see [`REMOVED_COLUMN`])
/// are assigned IDs the same way, and may have a null `firstRowId` whatever their status.
///
/// # Errors
/// See [`LeafEntryVisitor`].
fn validate_and_assign_first_row_ids(
    marked: &dyn EngineData,
    seed: i64,
    first_position: usize,
) -> KernelResult<(Vec<Scalar>, i64)> {
    let mut visitor = LeafEntryVisitor::new(seed, first_position);
    visitor.visit_rows_of(marked)?;
    Ok((visitor.first_row_ids, visitor.next_first_row_id))
}

/// Builds the transform mapping a [`ContentTreeNodeEntry`] row to a `{ add: Add }` struct matching
/// [`crate::actions::LOG_ADD_SCHEMA`].
///
/// `ctx` supplies the fields the AMT entry cannot; `sources` supplies `Add.baseRowId`, whose
/// derivation differs between root and leaf entries. A leaf entry with a null `sequenceNumber`
/// inherits its parent's (see [`PARENT_SEQUENCE_NUMBER_COLUMN`]).
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
    // `stats`, `tags`, and `clusteringProvider` fall through to null.
    // TODO(#2866): carry statistics (including `numRecords` from `recordCount`) and tags across
    // from the entry.
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
            n if n == DELETION_VECTOR_NAME => add_deletion_vector_expression(),
            n if n == BASE_ROW_ID_NAME => sources.base_row_id.clone(),
            n if n == DEFAULT_ROW_COMMIT_VERSION_NAME => expr!(coalesce(
                path(TRACKING, SEQUENCE_NUMBER),
                path(PARENT_SEQUENCE_NUMBER_COLUMN),
            )),
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

/// Validates the entries of a batch with [`SELECTED_COLUMN`] appended, and builds the
/// `Add.deletionVector` helper columns of the selected entries.
///
/// Fails closed on the entries the read path cannot yet represent as an `Add`, rather than silently
/// dropping or mis-translating them:
/// - an entry with an unknown content-type or tracking-status value;
/// - a live delete manifest entry, since dropping it would read the rows it deletes back as live;
/// - a selected entry that is not Parquet or has a non-zero partition spec id;
/// - a selected entry that carries column files, whose updated values a plain `Add` would drop;
/// - a selected entry whose deletion vector has no
///   [`DeletionVectorDescriptor`](crate::actions::deletion_vector::DeletionVectorDescriptor)
///   equivalent.
#[derive(Default)]
struct AddSelectionVisitor {
    /// The helper columns `Add.deletionVector` is built from, with a row for every entry: only a
    /// selected entry's deletion vector is kept.
    deletion_vectors: AddDeletionVectorColumns,
}

/// Index of the first [`DV_INFO_COLUMNS`] getter in [`AddSelectionVisitor`]'s getters, which end
/// with them.
const ADD_SELECTION_DV_START: usize = 8;

impl RowVisitor for AddSelectionVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let names: [ColumnName; ADD_SELECTION_DV_START] = [
                ColumnName::new([CONTENT_TYPE]),
                ColumnName::new([TRACKING, TRACKING_STATUS]),
                ColumnName::new([LOCATION]),
                ColumnName::new([FILE_FORMAT]),
                ColumnName::new([PARTITION_SPEC_ID]),
                ColumnName::new([TRACKING, MODIFIED_SNAPSHOT_ID]),
                ColumnName::new([COLUMN_FILES]),
                ColumnName::new([SELECTED_COLUMN]),
            ];
            let types: [DataType; ADD_SELECTION_DV_START] = [
                DataType::INTEGER,
                DataType::INTEGER,
                DataType::STRING,
                DataType::STRING,
                DataType::INTEGER,
                DataType::LONG,
                ArrayType::new(ColumnFile::to_schema(), false).into(),
                DataType::BOOLEAN,
            ];
            let (dv_names, dv_types) = DV_INFO_COLUMNS.as_ref();
            let names = names.into_iter().chain(dv_names.iter().cloned());
            let types = types.into_iter().chain(dv_types.iter().cloned());
            (names.collect(), types.collect()).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        for row in 0..row_count {
            let content_type = DataContentType::try_from_repr(getters[0].get(row, CONTENT_TYPE)?)?;
            let status = TrackingStatus::try_from_repr(getters[1].get(row, TRACKING_STATUS)?)?;
            let location: &str = getters[2].get(row, LOCATION)?;
            if content_type == DataContentType::DeleteManifest && status.is_live() {
                return Err(KernelError::unsupported(format!(
                    "AMT read path does not yet support live {content_type:?} entry '{location}'"
                )));
            }
            if getters[7].get(row, SELECTED_COLUMN)? {
                check_selected_entry(row, location, getters, &mut self.deletion_vectors)?;
            } else {
                self.deletion_vectors.push_null();
            }
        }
        Ok(())
    }
}

/// Collects a [`LeafRef`] for every live `DataManifest` entry of the root-manifest entry batches
/// it visits, in row order across batches. Non-live `DataManifest` entries and all other entries
/// are skipped.
struct LeafRefVisitor<'u> {
    /// The table root URL relative leaf locations resolve against.
    table_root: &'u Url,
    leaves: Vec<LeafRef>,
}

impl<'u> LeafRefVisitor<'u> {
    fn new(table_root: &'u Url) -> Self {
        Self {
            table_root,
            leaves: Vec::new(),
        }
    }
}

impl RowVisitor for LeafRefVisitor<'_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            let (names, types) = LEAF_REF_COLUMNS.as_ref();
            let names = iter::once(ColumnName::new([CONTENT_TYPE])).chain(names.iter().cloned());
            let types = iter::once(DataType::INTEGER).chain(types.iter().cloned());
            (names.collect(), types.collect()).into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        // `tracking.status` is the first tracking column; check it before parsing the rest of the
        // row.
        let status_index = tracking_start(getters);
        for row in 0..row_count {
            let content_type = DataContentType::try_from_repr(getters[0].get(row, CONTENT_TYPE)?)?;
            if content_type != DataContentType::DataManifest {
                continue;
            }
            let status = getters[status_index].get(row, TRACKING_STATUS)?;
            if !TrackingStatus::try_from_repr(status)?.is_live() {
                continue;
            }
            let leaf = LeafRef::try_from_getters(row, &getters[1..], self.table_root)?;
            self.leaves.push(leaf);
        }
        Ok(())
    }
}

/// Rejects a selected (live `Data`) entry carrying a field the read path cannot yet translate into
/// its `Add`, and appends its deletion vector to `deletion_vectors`. `getters` is
/// [`AddSelectionVisitor`]'s column order.
fn check_selected_entry<'a>(
    row: usize,
    location: &str,
    getters: &[&'a dyn GetData<'a>],
    deletion_vectors: &mut AddDeletionVectorColumns,
) -> KernelResult<()> {
    let file_format: &str = getters[3].get(row, FILE_FORMAT)?;
    let parquet = DataFileFormat::Parquet.name();
    if !file_format.eq_ignore_ascii_case(parquet) {
        return Err(KernelError::unsupported(format!(
            "AMT read path only supports {parquet} data files; entry '{location}' \
             has file format '{file_format}'"
        )));
    }
    let spec_id: Option<i32> = getters[4].get_opt(row, PARTITION_SPEC_ID)?;
    if let Some(spec_id) = spec_id.filter(|&id| id != 0) {
        return Err(KernelError::unsupported(format!(
            "AMT read path does not yet support partition specs; entry \
             '{location}' has {PARTITION_SPEC_ID} {spec_id}"
        )));
    }
    let has_deletion_vector = deletion_vectors.try_push(row, &getters[ADD_SELECTION_DV_START..])?;
    let unsupported_modification = |field: &str| {
        KernelError::unsupported(format!(
            "AMT read path does not yet support a live entry with column files or an \
             unrecognized modification (entry '{location}' has non-null '{field}')"
        ))
    };
    if has_column_files(getters[6].get_struct_list(row, COLUMN_FILES)?)? {
        return Err(unsupported_modification(COLUMN_FILES));
    }
    // A modification is the only reason to set `modifiedSnapshotId`, and a deletion vector is the
    // only one this path recognizes, so a DV-less entry carrying one has an unrecognized
    // modification.
    let modified_snapshot_id: Option<i64> = getters[5].get_opt(row, MODIFIED_SNAPSHOT_ID)?;
    if modified_snapshot_id.is_some() && !has_deletion_vector {
        return Err(unsupported_modification(MODIFIED_SNAPSHOT_ID));
    }
    Ok(())
}

/// Whether `column_files` holds at least one column file: an empty list carries no column updates.
fn has_column_files(column_files: Option<StructList<'_>>) -> KernelResult<bool> {
    let Some(column_files) = column_files else {
        return Ok(false);
    };
    let mut counter = ColumnFileCounter::default();
    column_files.visit_with(&mut counter)?;
    Ok(counter.count > 0)
}

/// Counts the elements of a [`ColumnFile`] list; see [`has_column_files`].
#[derive(Default)]
struct ColumnFileCounter {
    count: usize,
}

impl RowVisitor for ColumnFileCounter {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> =
            LazyLock::new(|| (vec![ColumnName::new([LOCATION])], vec![DataType::STRING]).into());
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, _: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        self.count += row_count;
        Ok(())
    }
}

/// Visitor behind [`validate_and_assign_first_row_ids`].
///
/// Errors if a leaf entry is not `Data`, has an unknown status, is live and non-`Added` with a null
/// `sequenceNumber`, or needs a `firstRowId` but has a negative `recordCount`, or if assignment
/// overflows `i64`. An entry the manifest deletion vector does not remove also errors if it is live
/// and non-`Added` with a null `firstRowId`.
struct LeafEntryVisitor {
    /// Next unassigned `firstRowId`.
    next_first_row_id: i64,
    /// The `_row_index` of the batch's first entry.
    first_position: usize,
    /// Resolved `firstRowId` per row, in entry order.
    first_row_ids: Vec<Scalar>,
}

impl LeafEntryVisitor {
    fn new(next_first_row_id: i64, first_position: usize) -> Self {
        Self {
            next_first_row_id,
            first_position,
            first_row_ids: Vec::new(),
        }
    }
}

impl RowVisitor for LeafEntryVisitor {
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
                    ColumnName::new([REMOVED_COLUMN]),
                    ColumnName::new([LEAF_ROW_INDEX_COLUMN]),
                ],
                vec![
                    DataType::LONG,
                    DataType::LONG,
                    DataType::INTEGER,
                    DataType::LONG,
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::BOOLEAN,
                    DataType::LONG,
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> KernelResult<()> {
        self.first_row_ids.reserve(row_count);
        for row in 0..row_count {
            // Removal and the deletion vector's end check both assume an entry's `_row_index` is
            // its position in the leaf, so a reader that numbers rows differently fails
            // here instead of removing the wrong entries.
            let row_index: i64 = getters[7].get(row, LEAF_ROW_INDEX_COLUMN)?;
            let position = self.first_position + row;
            if usize::try_from(row_index).ok() != Some(position) {
                let location: &str = getters[5].get(row, LOCATION)?;
                return Err(KernelError::internal_error(format!(
                    "AMT leaf manifest entry '{location}' has {LEAF_ROW_INDEX_COLUMN} {row_index}, \
                     but it is entry {position} of its leaf"
                )));
            }
            let removed: bool = getters[6].get(row, REMOVED_COLUMN)?;
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
            // Unlike the `sequenceNumber` check above, which validates the leaf entry itself, a
            // missing `firstRowId` only matters for the `baseRowId` of the `Add` the entry becomes.
            // A removed entry never becomes one, so it may lack a `firstRowId`.
            let needs_first_row_id = status.is_live() && !removed;
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
                None if needs_first_row_id => {
                    return Err(KernelError::missing_data(format!(
                        "AMT content-tree live non-Added leaf entry has a null '{FIRST_ROW_ID}'"
                    )));
                }
                // Entries that are not live, or are removed, never become an `Add`.
                None => Scalar::Null(DataType::LONG),
            };
            self.first_row_ids.push(assigned);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::slice;

    use rstest::rstest;
    use uuid::Uuid;

    use super::*;
    use crate::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorPath, DeletionVectorStorageType,
    };
    use crate::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };
    use crate::actions::visitors::AddVisitor;
    use crate::arrow::array::{AsArray as _, RecordBatch};
    use crate::arrow::compute::concat_batches;
    use crate::arrow::datatypes::Int64Type;
    use crate::content_tree::dv_conversion::extract_deletion_vector_content;
    use crate::content_tree::test_utils::{
        added_data_entry, data_manifest_entry, entry_batch, mumbling_bitmap, struct_batch,
        test_dv_info, with_field, write_manifest,
    };
    use crate::content_tree::{DeletionVectorInfo, ManifestInfo};
    use crate::engine::arrow_conversion::TryFromArrow as _;
    use crate::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt as _};
    use crate::engine::sync::SyncEngine;
    use crate::engine::test_delegating::DelegatingEngine;
    use crate::expressions::StructData;
    use crate::object_store::memory::InMemory;
    use crate::scan::state::DvInfo;
    use crate::schema::{StructType, schema_ref};
    use crate::unit_test_utils::adaptive_metadata_fixtures::checkpoint_action_with_root;
    use crate::unit_test_utils::{TestCancellationToken, assert_result_error_with_message};
    use crate::{FileSize, ParquetFooter, ParquetHandler, PredicateRef};

    /// The [`ReadContext`] the tests read against; [`expected_add_row`] reads its values back.
    /// Non-default values so a hard-coded `modificationTime` / `dataChange` would fail.
    fn read_ctx() -> ReadContext {
        ReadContext {
            table_root: table_root(),
            modification_time: 1234,
            data_change: false,
        }
    }

    /// The schema of the batches [`leaf_batch`] builds: entries plus [`LEAF_ROW_INDEX_COLUMN`].
    fn leaf_schema() -> SchemaRef {
        let row_index = StructField::not_null(LEAF_ROW_INDEX_COLUMN, DataType::LONG);
        schema_with(
            &ContentTreeNodeEntry::to_schema(),
            [&StructType::new_unchecked([row_index])],
        )
    }

    /// The leaf [`EntryTransform`] for batches built by [`leaf_batch`].
    fn leaf_transform(engine: &dyn Engine) -> Arc<EntryTransform> {
        EntryTransform::try_new(engine, &read_ctx(), AddFieldSources::leaf(), leaf_schema())
            .unwrap()
    }

    /// A leaf context under [`valid_parent`] for batches built by [`leaf_batch`].
    fn leaf_ctx(engine: &dyn Engine, manifest_dv: Option<ManifestDv>) -> LeafReadContext {
        LeafReadContext::new(
            engine,
            &leaf_transform(engine),
            &valid_parent(),
            manifest_dv,
        )
        .unwrap()
    }

    /// [`entry_batch`] plus the [`LEAF_ROW_INDEX_COLUMN`] a leaf read adds, numbering the entries
    /// from `first_row_index`.
    fn leaf_batch(
        engine: &dyn Engine,
        entries: &[ContentTreeNodeEntry],
        first_row_index: i64,
    ) -> Box<dyn EngineData> {
        let row_indexes = (first_row_index..).take(entries.len()).map(Scalar::Long);
        leaf_batch_with_row_indexes(engine, entries, row_indexes)
    }

    /// [`entry_batch`] plus a [`LEAF_ROW_INDEX_COLUMN`] holding `row_indexes`.
    fn leaf_batch_with_row_indexes(
        engine: &dyn Engine,
        entries: &[ContentTreeNodeEntry],
        row_indexes: impl IntoIterator<Item = Scalar>,
    ) -> Box<dyn EngineData> {
        let column =
            ArrayData::try_new(ArrayType::new(DataType::LONG, false), row_indexes).unwrap();
        let schema = schema_ref! { not_null LEAF_ROW_INDEX_COLUMN: LONG };
        entry_batch(engine, entries)
            .append_columns(schema, vec![column])
            .unwrap()
    }

    /// Builds a root converter and runs it over a single batch.
    fn convert_root_entries_to_add_actions(
        engine: &dyn Engine,
        entries: &dyn EngineData,
        ctx: &ReadContext,
    ) -> KernelResult<Box<dyn EngineData>> {
        let converter =
            make_root_entry_converter(engine, ctx, Arc::new(ContentTreeNodeEntry::to_schema()))?;
        converter.convert(entries, |_| Ok(vec![]))
    }

    /// Runs the read path over `entries`.
    fn convert(entries: &[ContentTreeNodeEntry]) -> KernelResult<Box<dyn EngineData>> {
        let engine = SyncEngine::new();
        convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, entries).as_ref(),
            &read_ctx(),
        )
    }

    /// Runs the read path over `entries` and returns how many `Add` actions it produced.
    fn add_count(entries: &[ContentTreeNodeEntry]) -> usize {
        convert(entries).unwrap().len()
    }

    /// Runs the read path over a single already-encoded entry row.
    fn convert_struct(entry: StructData) -> KernelResult<Box<dyn EngineData>> {
        let engine = SyncEngine::new();
        convert_root_entries_to_add_actions(
            &engine,
            struct_batch(&engine, &[entry]).as_ref(),
            &read_ctx(),
        )
    }

    /// A column file descriptor for tests that attach one to an entry.
    fn test_column_file() -> ColumnFile {
        ColumnFile {
            location: "cols.parquet".to_string(),
            field_ids: vec![1],
            file_format: DataFileFormat::Parquet,
            file_size_in_bytes: 1,
            key_metadata: None,
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

    /// Collects the leaf references of `batches`, visiting each as its own root entry batch.
    fn collect_leaf_refs(batches: &[&[ContentTreeNodeEntry]]) -> KernelResult<Vec<LeafRef>> {
        let engine = SyncEngine::new();
        let table_root = Url::parse("memory:///table/").unwrap();
        let mut visitor = LeafRefVisitor::new(&table_root);
        for entries in batches {
            visitor.visit_rows_of(entry_batch(&engine, entries).as_ref())?;
        }
        Ok(visitor.leaves)
    }

    #[rstest]
    #[case::relative("metadata/leaf.parquet", "memory:///table/metadata/leaf.parquet")]
    #[case::absolute("s3://bucket/leaf.parquet", "s3://bucket/leaf.parquet")]
    fn leaf_ref_resolves_location_against_table_root(
        #[case] location: &str,
        #[case] expected: &str,
    ) {
        let entry = data_manifest_entry(location, 42, TrackingStatus::Existing);
        let leaves = collect_leaf_refs(&[&[entry]]).unwrap();
        assert_eq!(leaves[0].file_meta.location.as_str(), expected);
        assert_eq!(leaves[0].file_meta.size, 42);
    }

    #[rstest]
    #[case::no_dv(None, None)]
    #[case::empty_dv(Some(mumbling_bitmap(&[])), None)]
    #[case::dv(Some(mumbling_bitmap(&[2, 17, 300])), Some(300))]
    fn leaf_ref_carries_parent_fields(
        #[case] dv: Option<Bytes>,
        #[case] expected_last_position: Option<u32>,
    ) {
        let mut entry = data_manifest_entry("leaf.parquet", 1, TrackingStatus::Existing);
        entry.manifest_info = Some(ManifestInfo {
            format_version: 9,
            dv,
            ..Default::default()
        });
        let leaf = collect_leaf_refs(&[&[entry.clone()]])
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(leaf.parent_info, entry.tracking);
        let last_position = leaf.manifest_dv.map(|dv| dv.last_position);
        assert_eq!(last_position, expected_last_position);
        assert_eq!(leaf.format_version, 9);
    }

    #[test]
    fn leaf_ref_rejects_wrong_getter_count() {
        let table_root = Url::parse("memory:///table/").unwrap();
        assert_result_error_with_message(
            LeafRef::try_from_getters(0, &[], &table_root),
            "AMT leaf reference getters, got 0",
        );
    }

    #[test]
    fn collect_leaf_refs_returns_live_data_manifests_in_order_across_batches() {
        let first = [
            added_data_entry("data.parquet", 1, 1, 0, 0),
            data_manifest_entry("added.parquet", 10, TrackingStatus::Added),
            data_manifest_entry("deleted.parquet", 2, TrackingStatus::Deleted),
        ];
        let second = [
            data_manifest_entry("existing.parquet", 30, TrackingStatus::Existing),
            data_manifest_entry("replaced.parquet", 40, TrackingStatus::Replaced),
            data_manifest_entry("modified.parquet", 50, TrackingStatus::Modified),
        ];
        let expected: Vec<_> = [&first[1], &second[0], &second[2]]
            .iter()
            .map(|e| {
                let location = format!("memory:///table/{}", e.location);
                (location, e.file_size_in_bytes as u64, e.tracking.clone())
            })
            .collect();

        let leaves: Vec<_> = collect_leaf_refs(&[&first, &[], &second])
            .unwrap()
            .into_iter()
            .map(|l| {
                (
                    l.file_meta.location.to_string(),
                    l.file_meta.size,
                    l.parent_info,
                )
            })
            .collect();
        assert_eq!(leaves, expected);
    }

    #[rstest]
    #[case::unknown_content_type(&[CONTENT_TYPE], Scalar::Integer(9), "content type value: 9")]
    #[case::unknown_status(&[TRACKING, TRACKING_STATUS], Scalar::Integer(9), "status value: 9")]
    #[case::unresolvable_location(
        &[LOCATION],
        Scalar::from("s3://bucket/a/../leaf.parquet"),
        "s3://bucket/a/../leaf.parquet"
    )]
    #[case::negative_size(
        &[FILE_SIZE_IN_BYTES],
        Scalar::Long(-1),
        "AMT leaf manifest 'leaf.parquet' size -1"
    )]
    fn collect_leaf_refs_rejects_malformed_entry(
        #[case] path: &[&str],
        #[case] value: Scalar,
        #[case] message: &str,
    ) {
        let engine = SyncEngine::new();
        let table_root = Url::parse("memory:///table/").unwrap();
        let entry = data_manifest_entry("leaf.parquet", 1, TrackingStatus::Existing);
        let batch = struct_batch(&engine, &[with_field(&entry.into(), path, value)]);
        let mut visitor = LeafRefVisitor::new(&table_root);
        assert_result_error_with_message(visitor.visit_rows_of(batch.as_ref()), message);
    }

    // === Manifest read schemas ===

    #[test]
    fn manifest_read_schema_adds_leaf_metadata_columns() {
        let entry_schema = Arc::new(ContentTreeNodeEntry::to_schema());
        let schemas = ManifestReadSchema::new();

        assert_eq!(schemas.root, entry_schema);
        let leaf: Vec<_> = schemas.leaf.fields().collect();
        let (entries, metadata) = leaf.split_at(leaf.len() - 2);
        assert_eq!(entries, entry_schema.fields().collect::<Vec<_>>());
        let specs: Vec<_> = metadata
            .iter()
            .map(|f| f.get_metadata_column_spec())
            .collect();
        assert_eq!(
            specs,
            [
                Some(MetadataColumnSpec::FilePath),
                Some(MetadataColumnSpec::RowIndex)
            ]
        );
    }

    #[test]
    fn leaf_read_schema_reads_file_and_row_index() {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let location = Url::parse("memory:///table/metadata/leaf.parquet").unwrap();
        let entries = [
            added_data_entry("a.parquet", 1, 1, 0, 0),
            added_data_entry("b.parquet", 1, 1, 1, 0),
        ];
        let file = write_manifest(&engine, location.clone(), &entries);
        let schemas = ManifestReadSchema::new();

        let batches: Vec<RecordBatch> = engine
            .parquet_handler()
            .read_parquet_files(slice::from_ref(&file), schemas.leaf.clone(), None)
            .unwrap()
            .map(|batch| batch.unwrap().try_into_record_batch().unwrap())
            .collect();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        let column = |name: &str| batch.column_by_name(name).unwrap().clone();
        let files: Vec<_> = column(LEAF_FILE_PATH_COLUMN)
            .as_string::<i32>()
            .iter()
            .map(|file| file.unwrap().to_owned())
            .collect();
        assert_eq!(files, vec![location.to_string(); 2]);
        let rows = column(LEAF_ROW_INDEX_COLUMN);
        assert_eq!(rows.as_primitive::<Int64Type>().values(), &[0, 1]);
    }

    // === Content-tree read ===

    const TABLE_ROOT: &str = "memory:///table/";
    const ROOT_PATH: &str = "metadata/root.parquet";

    fn table_root() -> Url {
        Url::parse(TABLE_ROOT).unwrap()
    }

    /// An engine over an empty in-memory store.
    fn memory_engine() -> SyncEngine {
        SyncEngine::new_with_store(Arc::new(InMemory::new()))
    }

    /// How [`ShapedParquetHandler`] reshapes its inner handler's reads.
    #[derive(Clone, Copy, Debug)]
    enum ReadShape {
        /// Forward reads unchanged.
        AsIs,
        /// Emit a zero-row batch, then one batch per row, for every inner batch.
        Split,
    }

    /// A test [`ParquetHandler`] that reshapes `inner`'s reads per [`ReadShape`].
    struct ShapedParquetHandler {
        inner: Arc<dyn ParquetHandler>,
        shape: ReadShape,
    }

    impl ParquetHandler for ShapedParquetHandler {
        fn read_parquet_files(
            &self,
            files: &[FileMeta],
            schema: SchemaRef,
            predicate: Option<PredicateRef>,
        ) -> KernelResult<FileDataReadResultIterator> {
            match self.shape {
                ReadShape::AsIs => self.inner.read_parquet_files(files, schema, predicate),
                ReadShape::Split => Ok(Box::new(
                    self.inner
                        .read_parquet_files(files, schema, predicate)?
                        .flat_map(split_batch),
                )),
            }
        }

        fn read_parquet_footer(&self, file: &FileMeta) -> KernelResult<ParquetFooter> {
            self.inner.read_parquet_footer(file)
        }

        fn write_parquet_file(
            &self,
            location: Url,
            data: Box<dyn KernelResultIterator<Box<dyn EngineData>>>,
        ) -> KernelResult<FileSize> {
            self.inner.write_parquet_file(location, data)
        }
    }

    /// Splits one read batch into a zero-row batch followed by one batch per row.
    fn split_batch(
        batch: KernelResult<Box<dyn EngineData>>,
    ) -> Vec<KernelResult<Box<dyn EngineData>>> {
        let batch = match batch.and_then(ArrowEngineData::try_from_engine_data) {
            Ok(batch) => batch.record_batch().clone(),
            Err(e) => return vec![Err(e)],
        };
        iter::once(batch.slice(0, 0))
            .chain((0..batch.num_rows()).map(|row| batch.slice(row, 1)))
            .map(|slice| Ok(Box::new(ArrowEngineData::new(slice)) as Box<dyn EngineData>))
            .collect()
    }

    /// An in-memory engine whose parquet reads are reshaped per `shape`.
    fn shaped_engine(shape: ReadShape) -> DelegatingEngine {
        let sync = Arc::new(SyncEngine::new_with_store(Arc::new(InMemory::new())));
        let shaped = Arc::new(ShapedParquetHandler {
            inner: sync.parquet_handler(),
            shape,
        });
        DelegatingEngine::new(sync).with_parquet_handler(shaped)
    }

    /// Writes `entries` as the root manifest and returns a checkpoint action referencing it.
    fn write_root(engine: &dyn Engine, entries: &[ContentTreeNodeEntry]) -> CheckpointAction {
        let root = write_manifest(engine, table_root().join(ROOT_PATH).unwrap(), entries);
        checkpoint_action_with_root(ROOT_PATH, root.size as i64, 1).unwrap()
    }

    /// Writes `entries` as the leaf manifest at `path` and returns a live root `DataManifest`
    /// entry referencing it, with the given parent `sequenceNumber` and `firstRowId`.
    fn write_leaf(
        engine: &dyn Engine,
        path: &str,
        entries: &[ContentTreeNodeEntry],
        sequence_number: i64,
        first_row_id: i64,
    ) -> ContentTreeNodeEntry {
        let leaf = write_manifest(engine, table_root().join(path).unwrap(), entries);
        let mut entry = data_manifest_entry(path, leaf.size as i64, TrackingStatus::Added);
        entry.tracking = parent_tracking(
            Some(1),
            Some(sequence_number),
            Some(sequence_number),
            Some(first_row_id),
        );
        entry
    }

    /// Reads `checkpoint`'s content tree, returning each batch's selected `Add` rows.
    fn read_tree(
        engine: &dyn Engine,
        checkpoint: &CheckpointAction,
    ) -> KernelResult<Vec<RecordBatch>> {
        read_add_actions_from_amt(engine, checkpoint, &read_ctx(), None)?
            .map(|batch| batch?.try_into_record_batch())
            .collect()
    }

    /// The expected `Add` batch for `rows` of `(path, size, baseRowId, defaultRowCommitVersion)`.
    fn expected_adds(engine: &dyn Engine, rows: &[(&str, i64, i64, i64)]) -> RecordBatch {
        let rows: Vec<_> = rows
            .iter()
            .map(|&(path, size, base_row_id, version)| {
                expected_add_row(path, size, base_row_id, version)
            })
            .collect();
        expected_batch(engine, &rows)
            .try_into_record_batch()
            .unwrap()
    }

    #[test]
    fn read_add_actions_from_amt_reads_root_data_entries() {
        let engine = memory_engine();
        let deleted = with_status(
            added_data_entry("deleted.parquet", 1, 1, 0, 0),
            TrackingStatus::Deleted,
        );
        let checkpoint = write_root(
            &engine,
            &[
                added_data_entry("a.parquet", 100, 10, 0, 5),
                deleted,
                added_data_entry("b.parquet", 200, 20, 10, 5),
            ],
        );

        let actual = read_tree(&engine, &checkpoint).unwrap();
        let expected = expected_adds(
            &engine,
            &[("a.parquet", 100, 0, 5), ("b.parquet", 200, 10, 5)],
        );
        assert_eq!(actual, vec![expected]);
    }

    /// A live entry's deletion vector reaches its `Add` as a descriptor for the writer's DV file:
    /// `r` with the raw path when the content tree stores its location relative, else `p`. The
    /// deleted rows read back through it, and a DV-less entry in the same batch keeps a null
    /// `Add.deletionVector`. A `Modified` entry also carries the `modifiedSnapshotId` of the DV
    /// change.
    #[rstest]
    fn read_add_actions_from_amt_carries_deletion_vector_into_add(
        #[values(false, true)] in_leaf: bool,
        #[values(TrackingStatus::Added, TrackingStatus::Modified)] status: TrackingStatus,
        #[values(false, true)] absolute_location: bool,
    ) {
        let engine = memory_engine();
        let dv_path = DeletionVectorPath::new_with_uuid(table_root(), "ab".into(), Uuid::nil());
        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
        let mut deleted_rows = KernelDeletionVector::new();
        deleted_rows.add_deleted_row_indexes([1, 5, 10]);
        let written = writer.write_deletion_vector(deleted_rows).unwrap();
        writer.finalize().unwrap();
        let dv_url = dv_path.absolute_path().unwrap();
        engine
            .storage_handler()
            .put(&dv_url, buffer.into(), false)
            .unwrap();

        let (mut entry, no_dv_entry) = if in_leaf {
            (
                leaf_entry("a.parquet", 100, 20, Some(1), Some(5), Some(0)),
                leaf_entry("b.parquet", 100, 20, Some(1), Some(5), Some(20)),
            )
        } else {
            (
                added_data_entry("a.parquet", 100, 20, 0, 5),
                added_data_entry("b.parquet", 100, 20, 20, 5),
            )
        };
        entry.tracking.status = status;
        if status == TrackingStatus::Modified {
            entry.tracking.modified_snapshot_id = Some(2);
        }
        let descriptor = written.to_descriptor(&dv_path);
        let mut dv_info = extract_deletion_vector_content(&descriptor).unwrap();
        if absolute_location {
            dv_info.location = dv_url.to_string();
        }
        entry.deletion_vector = Some(dv_info);
        let entries = [entry, no_dv_entry];
        let checkpoint = if in_leaf {
            let leaf = write_leaf(&engine, "metadata/leaf.parquet", &entries, 5, 0);
            write_root(&engine, &[leaf])
        } else {
            write_root(&engine, &entries)
        };

        let mut adds = AddVisitor::default();
        for batch in read_tree(&engine, &checkpoint).unwrap() {
            adds.visit_rows_of(&ArrowEngineData::new(batch)).unwrap();
        }
        let [add, no_dv_add] = adds.adds.as_slice() else {
            panic!("expected two Adds, got {:?}", adds.adds);
        };
        assert_eq!(no_dv_add.deletion_vector, None);
        let dv = add.deletion_vector.clone().unwrap();
        let expected = if absolute_location {
            DeletionVectorDescriptor {
                storage_type: DeletionVectorStorageType::PersistedAbsolute,
                path_or_inline_dv: dv_url.to_string(),
                ..descriptor
            }
        } else {
            DeletionVectorDescriptor {
                storage_type: DeletionVectorStorageType::PersistedUnencodedRelative,
                path_or_inline_dv: descriptor.relative_path().unwrap(),
                ..descriptor
            }
        };
        assert_eq!(dv, expected);
        let row_indexes = DvInfo::from(dv)
            .get_row_indexes(&engine, &table_root())
            .unwrap();
        assert_eq!(row_indexes, Some(vec![1, 5, 10]));
    }

    #[rstest]
    fn read_add_actions_from_amt_reads_all_live_leaves(
        #[values(ReadShape::AsIs, ReadShape::Split)] shape: ReadShape,
    ) {
        let engine = shaped_engine(shape);
        // Entries with an explicit `firstRowId` keep it and consume no assigned IDs.
        let existing = |path, first_row_id, sequence_number| {
            with_status(
                leaf_entry(
                    path,
                    1,
                    1,
                    Some(1),
                    Some(sequence_number),
                    Some(first_row_id),
                ),
                TrackingStatus::Existing,
            )
        };
        let leaf1 = write_leaf(
            &engine,
            "metadata/leaf1.parquet",
            &[
                leaf_entry("a.parquet", 100, 10, None, None, None),
                existing("e.parquet", 4000, 3),
                leaf_entry("b.parquet", 200, 20, None, None, None),
                leaf_entry("d.parquet", 400, 30, None, None, None),
            ],
            5,
            1000,
        );
        let empty_leaf = write_leaf(&engine, "metadata/empty.parquet", &[], 6, 5000);
        let mut leaf3 = write_leaf(
            &engine,
            "metadata/leaf3.parquet",
            &[
                leaf_entry("c.parquet", 300, 5, None, None, None),
                existing("f.parquet", 2000, 2),
            ],
            7,
            9000,
        );
        // An empty `columnFiles` list carries no column updates, so the leaf still reads.
        leaf3.column_files = Some(vec![]);
        // Non-live leaves are never read, so their files need not exist.
        let deleted = data_manifest_entry("metadata/gone.parquet", 1, TrackingStatus::Deleted);
        let replaced =
            data_manifest_entry("metadata/replaced.parquet", 1, TrackingStatus::Replaced);
        let checkpoint = write_root(
            &engine,
            &[
                added_data_entry("root.parquet", 50, 1, 600, 4),
                leaf1,
                deleted,
                empty_leaf,
                replaced,
                leaf3,
            ],
        );

        let actual = read_tree(&engine, &checkpoint).unwrap();
        // Null leaf `firstRowId`s are assigned from their parent's, and null sequence numbers
        // inherit the parent's, across batches.
        let expected = expected_adds(
            &engine,
            &[
                ("root.parquet", 50, 600, 4),
                ("a.parquet", 100, 1000, 5),
                ("e.parquet", 1, 4000, 3),
                ("b.parquet", 200, 1010, 5),
                ("d.parquet", 400, 1030, 5),
                ("c.parquet", 300, 9000, 7),
                ("f.parquet", 1, 2000, 2),
            ],
        );
        assert_eq!(
            concat_batches(&expected.schema(), &actual).unwrap(),
            expected
        );
    }

    #[rstest]
    #[case::unexpected_file(
        Scalar::from("memory:///table/other.parquet"),
        "batch from 'memory:///table/other.parquet' matches no remaining leaf manifest"
    )]
    #[case::null_file(Scalar::Null(DataType::STRING), "batch has no `_file` value")]
    fn leaf_actions_rejects_batch_without_matching_file(
        #[case] file: Scalar,
        #[case] message: &str,
    ) {
        let engine = SyncEngine::new();
        let leaf = "memory:///table/leaf.parquet".to_string();
        let mut actions = LeafToAddActions {
            pending: VecDeque::from([(leaf, leaf_ctx(&engine, None))]),
            batches: None,
        };
        let schema = Arc::new(StructType::new_unchecked([StructField::nullable(
            LEAF_FILE_PATH_COLUMN,
            DataType::STRING,
        )]));
        let batch = engine
            .evaluation_handler()
            .create_many(schema, vec![vec![file]])
            .unwrap();
        assert_result_error_with_message(actions.convert_to_add_actions(batch.as_ref()), message);
    }

    #[rstest]
    #[case::parent_missing_sequence_number(
        |leaf: &mut ContentTreeNodeEntry| leaf.tracking.sequence_number = None,
        vec![],
        "missing required tracking field 'sequenceNumber' (leaf manifest \
         'memory:///table/metadata/leaf.parquet')"
    )]
    #[case::data_manifest_in_leaf(
        |_: &mut ContentTreeNodeEntry| {},
        vec![data_manifest_entry("nested.parquet", 1, TrackingStatus::Added)],
        "AMT leaf manifest entry 'nested.parquet' has content type DataManifest"
    )]
    fn read_add_actions_from_amt_rejects_malformed_leaf(
        #[case] edit_parent: fn(&mut ContentTreeNodeEntry),
        #[case] leaf_entries: Vec<ContentTreeNodeEntry>,
        #[case] message: &str,
    ) {
        let engine = memory_engine();
        let mut leaf = write_leaf(&engine, "metadata/leaf.parquet", &leaf_entries, 5, 100);
        edit_parent(&mut leaf);
        let checkpoint = write_root(&engine, &[leaf]);
        assert_result_error_with_message(read_tree(&engine, &checkpoint), message);
    }

    #[rstest]
    #[case::pre_v4_leaf(
        |leaf: &mut ContentTreeNodeEntry| leaf.manifest_info.as_mut().unwrap().format_version = 0,
        "has manifestInfo.formatVersion 0"
    )]
    #[case::column_files(
        |leaf: &mut ContentTreeNodeEntry| leaf.column_files = Some(vec![test_column_file()]),
        "has columnFiles set"
    )]
    fn read_add_actions_from_amt_rejects_unreadable_leaf(
        #[case] edit_parent: fn(&mut ContentTreeNodeEntry),
        #[case] message: &str,
    ) {
        let engine = memory_engine();
        let mut leaf = write_leaf(&engine, "metadata/leaf.parquet", &[], 5, 100);
        edit_parent(&mut leaf);
        let checkpoint = write_root(&engine, &[leaf]);
        let err = read_tree(&engine, &checkpoint).unwrap_err().to_string();
        assert!(err.contains(message), "{err}");
        assert!(
            err.contains("'memory:///table/metadata/leaf.parquet'"),
            "{err}"
        );
    }

    /// Sets `positions` as `leaf`'s manifest deletion vector.
    fn with_manifest_dv(mut leaf: ContentTreeNodeEntry, positions: &[u32]) -> ContentTreeNodeEntry {
        leaf.manifest_info.as_mut().unwrap().dv = Some(mumbling_bitmap(positions));
        leaf
    }

    #[rstest]
    fn read_add_actions_from_amt_skips_manifest_dv_positions_keeping_row_ids(
        #[values(ReadShape::AsIs, ReadShape::Split)] shape: ReadShape,
    ) {
        let engine = shaped_engine(shape);
        let with_dv = write_leaf(
            &engine,
            "metadata/with_dv.parquet",
            &[
                leaf_entry("a.parquet", 100, 10, None, None, None),
                leaf_entry("b.parquet", 200, 20, None, None, None),
                leaf_entry("c.parquet", 300, 30, None, None, None),
                leaf_entry("d.parquet", 400, 40, None, None, None),
            ],
            5,
            1000,
        );
        // Positions restart at 0 for every leaf, so this deletion vector removes `e.parquet`.
        let second_with_dv = write_leaf(
            &engine,
            "metadata/second_with_dv.parquet",
            &[
                leaf_entry("e.parquet", 500, 5, None, None, None),
                leaf_entry("f.parquet", 600, 6, None, None, None),
            ],
            6,
            9000,
        );
        let without_dv = write_leaf(
            &engine,
            "metadata/without_dv.parquet",
            &[leaf_entry("g.parquet", 700, 7, None, None, None)],
            7,
            20000,
        );
        let checkpoint = write_root(
            &engine,
            &[
                with_manifest_dv(with_dv, &[1, 3]),
                with_manifest_dv(second_with_dv, &[0]),
                without_dv,
            ],
        );

        let actual = read_tree(&engine, &checkpoint).unwrap();
        // The removed `b.parquet` still consumes its 20 row IDs, so `c.parquet` keeps the
        // `baseRowId` it had before the deletion vector was set.
        let expected = expected_adds(
            &engine,
            &[
                ("a.parquet", 100, 1000, 5),
                ("c.parquet", 300, 1030, 5),
                ("f.parquet", 600, 9005, 6),
                ("g.parquet", 700, 20000, 7),
            ],
        );
        assert_eq!(
            concat_batches(&expected.schema(), &actual).unwrap(),
            expected
        );
    }

    #[rstest]
    #[case::unrepresentable_deletion_vector(|e: &mut ContentTreeNodeEntry| {
        e.deletion_vector = Some(DeletionVectorInfo {
            size_in_bytes: 7,
            ..test_dv_info()
        });
    })]
    #[case::column_files(
        |e: &mut ContentTreeNodeEntry| e.column_files = Some(vec![test_column_file()])
    )]
    #[case::partition_spec(|e: &mut ContentTreeNodeEntry| e.spec_id = Some(3))]
    #[case::modified_snapshot_id(
        |e: &mut ContentTreeNodeEntry| e.tracking.modified_snapshot_id = Some(3)
    )]
    #[case::existing_null_first_row_id(|e: &mut ContentTreeNodeEntry| {
        e.tracking.status = TrackingStatus::Existing;
        e.tracking.sequence_number = Some(1);
    })]
    fn read_add_actions_from_amt_does_not_validate_manifest_dv_removed_entry(
        #[case] edit_removed: fn(&mut ContentTreeNodeEntry),
    ) {
        let engine = memory_engine();
        let mut removed = leaf_entry("removed.parquet", 1, 1, None, None, None);
        edit_removed(&mut removed);
        let kept = with_status(
            leaf_entry("kept.parquet", 1, 1, Some(1), Some(4), Some(500)),
            TrackingStatus::Existing,
        );
        let leaf = write_leaf(&engine, "metadata/leaf.parquet", &[removed, kept], 5, 100);
        let checkpoint = write_root(&engine, &[with_manifest_dv(leaf, &[0])]);

        let actual = read_tree(&engine, &checkpoint).unwrap();
        let expected = expected_adds(&engine, &[("kept.parquet", 1, 500, 4)]);
        assert_eq!(
            concat_batches(&expected.schema(), &actual).unwrap(),
            expected
        );
    }

    #[rstest]
    #[case::data_manifest(
        |e: &mut ContentTreeNodeEntry| e.content_type = DataContentType::DataManifest,
        "has content type DataManifest"
    )]
    #[case::existing_null_sequence_number(
        |e: &mut ContentTreeNodeEntry| e.tracking.status = TrackingStatus::Existing,
        "live non-Added leaf entry has a null 'sequenceNumber'"
    )]
    #[case::added_negative_record_count(
        |e: &mut ContentTreeNodeEntry| {
            e.tracking.first_row_id = None;
            e.record_count = -1;
        },
        "negative 'recordCount'"
    )]
    fn read_add_actions_from_amt_rejects_malformed_manifest_dv_removed_entry(
        #[case] edit_removed: fn(&mut ContentTreeNodeEntry),
        #[case] message: &str,
    ) {
        let engine = memory_engine();
        let mut removed = leaf_entry("removed.parquet", 1, 1, Some(1), None, Some(1));
        edit_removed(&mut removed);
        let leaf = write_leaf(&engine, "metadata/leaf.parquet", &[removed], 5, 100);
        let checkpoint = write_root(&engine, &[with_manifest_dv(leaf, &[0])]);
        assert_result_error_with_message(read_tree(&engine, &checkpoint), message);
    }

    #[rstest]
    #[case::malformed(
        Bytes::from_static(b"dv"),
        "'memory:///table/metadata/leaf.parquet' has a malformed manifestInfo.dv"
    )]
    fn read_add_actions_from_amt_rejects_invalid_manifest_dv(
        #[case] dv: Bytes,
        #[case] message: &str,
    ) {
        let engine = memory_engine();
        let mut leaf = write_leaf(&engine, "metadata/leaf.parquet", &[], 5, 100);
        leaf.manifest_info.as_mut().unwrap().dv = Some(dv);
        let checkpoint = write_root(&engine, &[leaf]);
        assert_result_error_with_message(read_tree(&engine, &checkpoint), message);
    }

    #[rstest]
    #[case::first_leaf(&[0, 2], &[], "'memory:///table/metadata/first.parquet' has 2 entries")]
    #[case::last_leaf(&[], &[1], "'memory:///table/metadata/last.parquet' has 1 entries")]
    fn read_add_actions_from_amt_rejects_manifest_dv_position_past_leaf_end(
        #[values(ReadShape::AsIs, ReadShape::Split)] shape: ReadShape,
        #[case] first_dv: &[u32],
        #[case] last_dv: &[u32],
        #[case] message: &str,
    ) {
        let engine = shaped_engine(shape);
        let first = write_leaf(
            &engine,
            "metadata/first.parquet",
            &[
                leaf_entry("a.parquet", 100, 10, None, None, None),
                leaf_entry("b.parquet", 200, 20, None, None, None),
            ],
            5,
            1000,
        );
        let last = write_leaf(
            &engine,
            "metadata/last.parquet",
            &[leaf_entry("c.parquet", 300, 30, None, None, None)],
            6,
            2000,
        );
        let checkpoint = write_root(
            &engine,
            &[
                with_manifest_dv(first, first_dv),
                with_manifest_dv(last, last_dv),
            ],
        );
        assert_result_error_with_message(read_tree(&engine, &checkpoint), message);
    }

    #[test]
    fn read_add_actions_from_amt_rejects_manifest_dv_on_empty_leaf() {
        let engine = memory_engine();
        let leaf = write_leaf(&engine, "metadata/leaf.parquet", &[], 5, 100);
        let checkpoint = write_root(&engine, &[with_manifest_dv(leaf, &[0])]);
        assert_result_error_with_message(
            read_tree(&engine, &checkpoint),
            "'memory:///table/metadata/leaf.parquet' has 0 entries, but its manifestInfo.dv \
             removes position 0",
        );
    }

    #[test]
    fn read_add_actions_from_amt_errors_on_every_batch_after_first_leaf_error() {
        let engine = shaped_engine(ReadShape::Split);
        let malformed = write_leaf(
            &engine,
            "metadata/malformed.parquet",
            &[data_manifest_entry(
                "nested.parquet",
                1,
                TrackingStatus::Added,
            )],
            5,
            100,
        );
        let valid = write_leaf(
            &engine,
            "metadata/valid.parquet",
            &[leaf_entry("a.parquet", 100, 10, None, None, None)],
            6,
            200,
        );
        let checkpoint = write_root(&engine, &[malformed, valid]);
        let results: Vec<_> = read_add_actions_from_amt(&engine, &checkpoint, &read_ctx(), None)
            .unwrap()
            .collect();
        let first_error = results.iter().position(Result::is_err).unwrap();
        let after = &results[first_error + 1..];
        assert!(!after.is_empty());
        assert!(after.iter().all(Result::is_err));
    }

    #[test]
    fn read_add_actions_from_amt_errors_on_missing_leaf() {
        let engine = memory_engine();
        let missing = data_manifest_entry("metadata/missing.parquet", 1, TrackingStatus::Added);
        let checkpoint = write_root(&engine, &[missing]);
        assert_result_error_with_message(
            read_tree(&engine, &checkpoint),
            "metadata/missing.parquet",
        );
    }

    #[test]
    fn read_add_actions_from_amt_rejects_duplicate_leaf() {
        let engine = memory_engine();
        let leaf = write_leaf(&engine, "metadata/leaf.parquet", &[], 5, 100);
        let checkpoint = write_root(&engine, &[leaf.clone(), leaf]);
        assert_result_error_with_message(
            read_tree(&engine, &checkpoint),
            "references leaf manifest 'memory:///table/metadata/leaf.parquet' more than once",
        );
    }

    #[test]
    fn read_add_actions_from_amt_stops_when_cancelled() {
        let engine = memory_engine();
        let leaf = write_leaf(
            &engine,
            "metadata/leaf.parquet",
            &[leaf_entry("b.parquet", 100, 10, None, None, None)],
            5,
            100,
        );
        let checkpoint = write_root(
            &engine,
            &[added_data_entry("a.parquet", 100, 10, 0, 5), leaf],
        );
        let read = |token: CancellationTokenRef| {
            read_add_actions_from_amt(&engine, &checkpoint, &read_ctx(), Some(&token))
        };

        let cancelled = Arc::new(TestCancellationToken::cancelled());
        assert!(matches!(read(cancelled), Err(KernelError::Cancelled)));

        let token = Arc::new(TestCancellationToken::default());
        let mut batches = read(token.clone()).unwrap();
        token.cancel();
        // The root manifest is read eagerly, so only the leaf read is stopped.
        assert!(batches.next().unwrap().is_ok());
        assert!(matches!(batches.next(), Some(Err(KernelError::Cancelled))));
    }

    #[test]
    fn read_add_actions_from_amt_errors_on_missing_root() {
        let engine = memory_engine();
        let checkpoint = checkpoint_action_with_root(ROOT_PATH, 1, 1).unwrap();
        assert_result_error_with_message(read_tree(&engine, &checkpoint), ROOT_PATH);
    }

    #[test]
    fn converts_added_data_entries_to_add_actions() {
        let engine = SyncEngine::new();
        let entries = [
            added_data_entry("a.parquet", 100, 10, 0, 5),
            added_data_entry("b.parquet", 200, 20, 10, 5),
        ];
        let out = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &entries).as_ref(),
            &read_ctx(),
        )
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
    fn root_entry_converter_is_reusable_across_batches() {
        let engine = SyncEngine::new();
        let root_schema = Arc::new(ContentTreeNodeEntry::to_schema());
        let converter = make_root_entry_converter(&engine, &read_ctx(), root_schema).unwrap();
        let mut deleted = added_data_entry("deleted.parquet", 1, 1, 0, 0);
        deleted.tracking.status = TrackingStatus::Deleted;
        let batches = [
            vec![added_data_entry("a.parquet", 100, 10, 0, 5)],
            vec![deleted, added_data_entry("b.parquet", 200, 20, 10, 6)],
        ];
        let expected = [
            expected_add_row("a.parquet", 100, 0, 5),
            expected_add_row("b.parquet", 200, 10, 6),
        ];

        for (entries, row) in batches.iter().zip(expected) {
            let adds = converter
                .convert(entry_batch(&engine, entries).as_ref(), |_| Ok(vec![]))
                .unwrap();
            assert_eq!(
                adds.try_into_record_batch().unwrap(),
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
        let out = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &entries).as_ref(),
            &read_ctx(),
        )
        .unwrap()
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

        let out = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[deleted, manifest, live]).as_ref(),
            &read_ctx(),
        )
        .unwrap();
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
        assert_eq!(add_count(&[entry]), usize::from(kept));
    }

    #[rstest]
    fn live_data_manifest_entry_is_dropped(
        #[values(TrackingStatus::Existing, TrackingStatus::Added)] status: TrackingStatus,
    ) {
        let mut entry = added_data_entry("manifest.parquet", 1, 1, 0, 0);
        entry.content_type = DataContentType::DataManifest;
        entry.manifest_info = Some(ManifestInfo::default());
        entry.tracking.status = status;
        assert_eq!(add_count(&[entry]), 0);
    }

    #[rstest]
    fn live_delete_manifest_entry_errors(
        #[values(TrackingStatus::Existing, TrackingStatus::Added)] status: TrackingStatus,
    ) {
        let mut entry = added_data_entry("deletes.parquet", 1, 1, 0, 0);
        entry.content_type = DataContentType::DeleteManifest;
        entry.tracking.status = status;
        let result = convert(&[entry]);
        assert_result_error_with_message(result, "live DeleteManifest entry");
    }

    /// A dropped entry is not validated: an unsupported file format, partition spec, or deletion
    /// vector on it must not fail the read.
    #[rstest]
    fn dropped_entry_with_unsupported_fields_is_not_rejected(
        #[values(
            DataContentType::Data,
            DataContentType::DataManifest,
            DataContentType::DeleteManifest
        )]
        content_type: DataContentType,
        #[values(TrackingStatus::Deleted, TrackingStatus::Replaced)] status: TrackingStatus,
    ) {
        let mut entry = added_data_entry("dropped.parquet", 1, 1, 0, 0);
        entry.content_type = content_type;
        entry.tracking.status = status;
        entry.spec_id = Some(7);
        entry.deletion_vector = Some(test_dv_info());
        entry.tracking.modified_snapshot_id = Some(1);
        entry.column_files = Some(vec![test_column_file()]);
        let entry = with_field(&entry.into(), &[FILE_FORMAT], Scalar::from("orc"));
        assert_eq!(convert_struct(entry).unwrap().len(), 0);
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
    fn live_data_entry_accepts_supported_fields(
        #[values("parquet", "PARQUET")] file_format: &str,
        #[values(None, Some(0))] spec_id: Option<i32>,
        #[values(None, Some(vec![]))] column_files: Option<Vec<ColumnFile>>,
    ) {
        let mut entry = added_data_entry("f.parquet", 1, 1, 0, 0);
        entry.spec_id = spec_id;
        entry.column_files = column_files;
        let entry = with_field(&entry.into(), &[FILE_FORMAT], Scalar::from(file_format));
        assert_eq!(convert_struct(entry).unwrap().len(), 1);
    }

    #[rstest]
    #[case::orc_file_format("orc", Some(0), "has file format 'orc'")]
    #[case::non_zero_spec_id("parquet", Some(1), "has specId 1")]
    fn live_data_entry_with_unsupported_field_errors(
        #[case] file_format: &str,
        #[case] spec_id: Option<i32>,
        #[case] message: &str,
    ) {
        let mut entry = added_data_entry("f.parquet", 1, 1, 0, 0);
        entry.spec_id = spec_id;
        let entry = with_field(&entry.into(), &[FILE_FORMAT], Scalar::from(file_format));
        assert_result_error_with_message(convert_struct(entry), message);
    }

    #[rstest]
    #[case::added_modified_snapshot_id(
        TrackingStatus::Added,
        false,
        false,
        true,
        MODIFIED_SNAPSHOT_ID
    )]
    #[case::modified_column_files(TrackingStatus::Modified, false, true, true, COLUMN_FILES)]
    #[case::modified_dv_and_column_files(TrackingStatus::Modified, true, true, true, COLUMN_FILES)]
    fn live_entry_with_modification_errors(
        #[case] status: TrackingStatus,
        #[case] set_deletion_vector: bool,
        #[case] set_column_files: bool,
        #[case] set_modified_snapshot_id: bool,
        #[case] field: &str,
    ) {
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        entry.tracking.status = status;
        if set_deletion_vector {
            entry.deletion_vector = Some(test_dv_info());
        }
        if set_column_files {
            entry.column_files = Some(vec![test_column_file()]);
        }
        if set_modified_snapshot_id {
            entry.tracking.modified_snapshot_id = Some(1);
        }
        let result = convert(&[entry]);
        assert_result_error_with_message(result, &format!("has non-null '{field}'"));
    }

    // The conversion's error cases are covered in `dv_conversion`; this checks one propagates.
    #[test]
    fn live_entry_with_unrepresentable_deletion_vector_errors() {
        let mut entry = added_data_entry("f.parquet", 10, 5, 0, 1);
        entry.deletion_vector = Some(DeletionVectorInfo {
            size_in_bytes: 7,
            ..test_dv_info()
        });
        assert_result_error_with_message(convert(&[entry]), "not a valid Iceberg blob size");
    }

    #[test]
    fn empty_input_yields_empty_batch() {
        let engine = SyncEngine::new();
        let out = convert_root_entries_to_add_actions(
            &engine,
            entry_batch(&engine, &[]).as_ref(),
            &read_ctx(),
        )
        .unwrap();
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
            modified_snapshot_id: None,
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
        let mut ctx = leaf_ctx(&engine, None);
        let out = ctx
            .convert_leaf_entries_to_add_actions(leaf_batch(&engine, &entries, 0).as_ref())
            .unwrap();
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
        let mut ctx = leaf_ctx(&engine, None);
        let out = ctx
            .convert_leaf_entries_to_add_actions(leaf_batch(&engine, &entries, 0).as_ref())
            .unwrap();
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
        let mut ctx = leaf_ctx(&engine, None);
        let out = ctx
            .convert_leaf_entries_to_add_actions(leaf_batch(&engine, &entries, 0).as_ref())
            .unwrap();
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
        let mut ctx = leaf_ctx(&engine, None);

        let batch1 = [
            leaf_entry("a.parquet", 1, 10, None, None, None),
            leaf_entry("b.parquet", 1, 20, None, None, None),
        ];
        let out1 = ctx
            .convert_leaf_entries_to_add_actions(leaf_batch(&engine, &batch1, 0).as_ref())
            .unwrap();
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
        let out2 = ctx
            .convert_leaf_entries_to_add_actions(leaf_batch(&engine, &batch2, 2).as_ref())
            .unwrap();
        let expected2 = expected_batch(&engine, &[expected_add_row("c.parquet", 1, 130, 5)]);
        assert_eq!(
            out2.try_into_record_batch().unwrap(),
            expected2.try_into_record_batch().unwrap()
        );
    }

    #[test]
    fn manifest_dv_removes_entries_by_row_index_across_batches() {
        let engine = SyncEngine::new();
        let dv = ManifestDv::try_new(&mumbling_bitmap(&[1, 3])).unwrap();
        let mut ctx = leaf_ctx(&engine, dv);
        let mut next_row_index = 0;
        let mut convert = |entries: &[ContentTreeNodeEntry]| {
            let batch = leaf_batch(&engine, entries, next_row_index);
            next_row_index += entries.len() as i64;
            ctx.convert_leaf_entries_to_add_actions(batch.as_ref())
                .map(|out| out.try_into_record_batch().unwrap())
        };
        let expected = |rows: &[(&str, i64)]| {
            let rows: Vec<_> = rows
                .iter()
                .map(|&(path, base_row_id)| expected_add_row(path, 1, base_row_id, 5))
                .collect();
            expected_batch(&engine, &rows)
                .try_into_record_batch()
                .unwrap()
        };

        // Positions 0..=2: `b.parquet` (position 1) is removed but still consumes its row IDs.
        let out = convert(&[
            leaf_entry("a.parquet", 1, 10, None, None, None),
            leaf_entry("b.parquet", 1, 20, None, None, None),
            leaf_entry("c.parquet", 1, 30, None, None, None),
        ])
        .unwrap();
        assert_eq!(out, expected(&[("a.parquet", 100), ("c.parquet", 130)]));

        // Positions 3..=4: `d.parquet` (position 3) is removed.
        let out = convert(&[
            leaf_entry("d.parquet", 1, 40, None, None, None),
            leaf_entry("e.parquet", 1, 50, None, None, None),
        ])
        .unwrap();
        assert_eq!(out, expected(&[("e.parquet", 200)]));
    }

    #[rstest]
    #[case::offset_start(&[3, 4])]
    #[case::gap(&[0, 2])]
    #[case::repeated(&[0, 0])]
    fn leaf_entry_row_index_must_be_its_position(#[case] row_indexes: &[i64]) {
        let engine = SyncEngine::new();
        let mut ctx = leaf_ctx(&engine, None);
        let entries = [
            leaf_entry("a.parquet", 1, 10, None, None, None),
            leaf_entry("b.parquet", 1, 20, None, None, None),
        ];
        let batch = leaf_batch_with_row_indexes(
            &engine,
            &entries,
            row_indexes.iter().copied().map(Scalar::Long),
        );
        assert_result_error_with_message(
            ctx.convert_leaf_entries_to_add_actions(batch.as_ref()),
            "but it is entry",
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
        let mut ctx = leaf_ctx(&engine, None);
        let result =
            ctx.convert_leaf_entries_to_add_actions(leaf_batch(&engine, &[entry], 0).as_ref());
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
        let mut ctx = leaf_ctx(&engine, None);
        let result =
            ctx.convert_leaf_entries_to_add_actions(leaf_batch(&engine, &[entry], 0).as_ref());
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
        let result = LeafReadContext::new(&engine, &leaf_transform(&engine), &parent, None);
        assert_result_error_with_message(result, expected_field);
    }

    #[rstest]
    #[case::data_manifest(DataContentType::DataManifest)]
    #[case::delete_manifest(DataContentType::DeleteManifest)]
    fn leaf_non_data_entry_errors(#[case] content_type: DataContentType) {
        let engine = SyncEngine::new();
        let mut entry = leaf_entry("f.parquet", 10, 5, None, None, None);
        entry.content_type = content_type;
        let mut ctx = leaf_ctx(&engine, None);
        let result =
            ctx.convert_leaf_entries_to_add_actions(leaf_batch(&engine, &[entry], 0).as_ref());
        assert_result_error_with_message(result, &format!("content type {content_type:?}"));
    }

    #[test]
    fn leaf_empty_batch_yields_empty_and_preserves_next_row_id() {
        let engine = SyncEngine::new();
        let mut ctx = leaf_ctx(&engine, None);
        let empty = ctx
            .convert_leaf_entries_to_add_actions(leaf_batch(&engine, &[], 0).as_ref())
            .unwrap();
        assert_eq!(empty.len(), 0);

        // The empty batch consumes no row IDs, so the next entry still starts at 100.
        let out = ctx
            .convert_leaf_entries_to_add_actions(
                leaf_batch(
                    &engine,
                    &[leaf_entry("a.parquet", 1, 10, None, None, None)],
                    0,
                )
                .as_ref(),
            )
            .unwrap();
        let expected = expected_batch(&engine, &[expected_add_row("a.parquet", 1, 100, 5)]);
        assert_eq!(
            out.try_into_record_batch().unwrap(),
            expected.try_into_record_batch().unwrap()
        );
    }
}
