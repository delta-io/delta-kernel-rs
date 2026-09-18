//! Reads an AMT content tree (the Iceberg V4 manifest tree referenced by a checkpoint's
//! `contentRoot`) into Delta `add` actions for the scan read path.
//!
//! A root manifest holds a mix of leaf data-file entries and references to child data manifests.
//! [`read_content_tree_add_actions`] starts at the `contentRoot`, walks every reachable manifest,
//! and turns each live `Data` entry into an `add` action so the normal scan log-replay
//! ([`crate::scan::log_replay`]) can consume it. Anything the read path cannot yet represent is
//! rejected rather than silently dropped: entries with a deletion vector, entries with tracked
//! position deletes/replaces, entries whose `firstRowId` would be inherited from the parent
//! `DataManifest` (null), manifests with a manifest-level deletion vector, and delete-oriented
//! entries (`PositionDeletes`/`EqualityDeletes`/`DeleteManifest`), all error. Per-file statistics
//! are not yet emitted either (data skipping over AMT scans is a follow-up).
//!
//! Partitioned tables are not yet supported here: the reader emits empty partition values, so the
//! scan layer must reject partitioned AMT tables until the dynamic `partition` column is wired in.

use std::sync::{Arc, LazyLock};

use url::Url;

use crate::actions::{Add, ContentRoot, LOG_ADD_SCHEMA};
use crate::content_tree::{resolve_amt_location, DataContentType, TrackingStatus};
use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
use crate::expressions::{
    column_expr_ref, lit, null_lit, Expression, ExpressionRef, MapData, Scalar,
};
use crate::schema::{
    column_name, ColumnName, ColumnNamesAndTypes, DataType, MapType, SchemaRef, ToSchema,
};
use crate::{DeltaResult, Engine, EngineData, Error, FileMeta};

/// Physical read schema for a manifest file: the full [`ContentTreeNodeEntry`] schema, which
/// carries the Parquet field IDs the reader matches on. It omits the dynamic `partition` and
/// `content_stats` columns (both `#[skip_schema]`), which the MVP does not read.
///
/// [`ContentTreeNodeEntry`]: super::ContentTreeNodeEntry
static MANIFEST_READ_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(super::ContentTreeNodeEntry::to_schema()));

/// Reads the content tree rooted at `content_root` and returns its live data files as `add`-action
/// [`EngineData`] batches (one row per file, under a top-level `add` column matching
/// [`LOG_ADD_SCHEMA`]).
///
/// Walks the manifest tree depth-first from the root manifest, following `DataManifest` entries to
/// child manifests. Each manifest batch is transformed to `add` actions columnar via a reused
/// [`ExpressionEvaluator`] (see [`add_transform_expr`]); `ManifestEntryVisitor` supplies the
/// per-row selection so only live `Data` rows survive (`Deleted` data rows and `DataManifest`
/// pointers are filtered out).
///
/// `add.path` is the manifest `location` verbatim (relative), resolved later by the scan against
/// the table root, matching how Delta `add.path` is normally handled.
///
/// Returns an empty vector when the tree contains no live data files. Errors if a manifest cannot
/// be read, or if an entry cannot yet be represented as an `add` action: a data entry carrying a
/// deletion vector, a live data entry with a null `firstRowId` (baseRowId inheritance is not yet
/// wired in), any live entry with tracked position deletes/replaces, a manifest with a
/// manifest-level deletion vector, or a `PositionDeletes`/`EqualityDeletes`/`DeleteManifest` entry.
///
/// [`ExpressionEvaluator`]: crate::ExpressionEvaluator
pub(crate) fn read_content_tree_add_actions(
    engine: &dyn Engine,
    table_root: &Url,
    content_root: &ContentRoot,
) -> DeltaResult<Vec<Box<dyn EngineData>>> {
    // Built once and reused across every manifest batch, avoiding a per-row scalar round-trip.
    let add_transform = engine.evaluation_handler().new_expression_evaluator(
        MANIFEST_READ_SCHEMA.clone(),
        add_transform_expr()?,
        LOG_ADD_SCHEMA.clone().into(),
    )?;

    // Each pending manifest carries whether it is the root: `DataManifest` entries are only
    // allowed in the root, so tracking this both enforces the spec and prevents cycles (only the
    // once-processed root can reference child manifests, so the walk always terminates).
    let mut pending_manifests = vec![(content_root.to_filemeta(table_root)?, true)];
    let mut add_batches: Vec<Box<dyn EngineData>> = Vec::new();

    // TODO: Switch to async processing
    while let Some((manifest, is_root)) = pending_manifests.pop() {
        for batch in engine.parquet_handler().read_parquet_files(
            std::slice::from_ref(&manifest),
            MANIFEST_READ_SCHEMA.clone(),
            None,
        )? {
            let batch = batch?;
            let mut visitor = ManifestEntryVisitor::new(table_root, is_root);
            visitor.visit_rows_of(batch.as_ref())?;
            // Child manifests are never root, so any nested `DataManifest` they contain is
            // rejected.
            pending_manifests.extend(visitor.child_manifests.into_iter().map(|m| (m, false)));
            // Transform the whole batch columnar, then keep only the live data-file rows.
            if visitor.selection.iter().any(|&selected| selected) {
                let adds = add_transform.evaluate(batch.as_ref())?;
                add_batches.push(adds.apply_selection_vector(visitor.selection)?);
            }
        }
    }
    Ok(add_batches)
}

/// The reusable transform mapping a manifest batch (read under [`MANIFEST_READ_SCHEMA`]) to
/// `add`-action [`EngineData`] matching [`LOG_ADD_SCHEMA`], columnar. Manifest columns feed the
/// `add` fields directly (`location` -> `path`, `fileSizeInBytes` -> `size`,
/// `tracking.firstRowId` -> `baseRowId`); constant and not-yet-supported fields become broadcast
/// literals. Built by iterating [`Add::to_schema`] so field order and the feature-gated
/// `backReference` field stay correct.
fn add_transform_expr() -> DeltaResult<ExpressionRef> {
    // Empty, value-nullable string map, matching `Add::partition_values`.
    let empty_partition_values = Scalar::Map(MapData::try_new(
        MapType::new(DataType::STRING, DataType::STRING, true),
        Vec::<(String, String)>::new(),
    )?);
    let add_fields: Vec<ExpressionRef> = Add::to_schema()
        .fields()
        .map(|field| match field.name().as_str() {
            "path" => column_expr_ref!("location"),
            "size" => column_expr_ref!("fileSizeInBytes"),
            "baseRowId" => column_expr_ref!("tracking.firstRowId"),
            // TODO: Wire in partition
            "partitionValues" => Arc::new(lit(empty_partition_values.clone())),
            // AMT entries carry no modification time; scans do not use it.
            "modificationTime" => Arc::new(lit(0i64)),
            // Checkpoint-derived adds describe reconciled state, not a change in this version.
            "dataChange" => Arc::new(lit(false)),
            // Everything else (stats, tags, deletionVector, ...) is a typed null for now.
            // TODO: carry AMT deletion vectors and stats through; DV-bearing entries are rejected
            // during parsing until then.
            _ => Arc::new(null_lit(field.data_type().clone())),
        })
        .collect();
    // Wrap the `add` struct in the top-level `{ add: ... }` of `LOG_ADD_SCHEMA`.
    Ok(Arc::new(Expression::struct_from([Arc::new(
        Expression::struct_from(add_fields),
    )])))
}

/// Resolves a child `DataManifest` entry's `location` and size to a [`FileMeta`] to recurse into.
fn manifest_filemeta(
    location: &str,
    file_size_in_bytes: i64,
    table_root: &Url,
) -> DeltaResult<FileMeta> {
    Ok(FileMeta {
        location: resolve_amt_location(location, table_root)?,
        last_modified: i64::MAX,
        size: u64::try_from(file_size_in_bytes)
            .map_err(|_| Error::generic("manifest file size does not fit in u64"))?,
    })
}

/// Classifies the rows of one manifest node in a single lightweight pass, producing the per-row
/// `selection` the columnar transform is filtered by, plus the child manifest [`FileMeta`]s to
/// recurse into. `Deleted`-status rows are unselected (not live). Anything the read path cannot
/// yet represent is rejected with an error rather than dropped: a live data entry carrying a
/// deletion vector, a live data entry with a null `firstRowId`, any live entry with tracked
/// position deletes/replaces, a manifest with a manifest-level deletion vector, and
/// `PositionDeletes`/`EqualityDeletes`/`DeleteManifest` entries.
struct ManifestEntryVisitor {
    /// Base URL for resolving `DataManifest` locations into child [`FileMeta`]s.
    table_root: Url,
    /// Whether the manifest being visited is the root. `DataManifest` entries are only allowed in
    /// the root; encountering one in a non-root manifest is rejected.
    is_root: bool,
    /// One entry per manifest row: `true` for a live `Data` file (emitted as an `add`), `false`
    /// for non-live data rows (`Deleted`/`Replaced`) and `DataManifest` pointers. Aligns with the
    /// batch the transform evaluates, so it can drive [`EngineData::apply_selection_vector`].
    selection: Vec<bool>,
    /// The child `DataManifest` entries to recurse into, resolved to [`FileMeta`]s.
    child_manifests: Vec<FileMeta>,
}

impl ManifestEntryVisitor {
    fn new(table_root: &Url, is_root: bool) -> Self {
        Self {
            table_root: table_root.clone(),
            is_root,
            selection: Vec::new(),
            child_manifests: Vec::new(),
        }
    }
}

impl RowVisitor for ManifestEntryVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES_AND_TYPES: LazyLock<ColumnNamesAndTypes> = LazyLock::new(|| {
            (
                vec![
                    column_name!("contentType"),
                    column_name!("location"),
                    column_name!("tracking.status"),
                    column_name!("deletionVector.location"),
                    column_name!("fileSizeInBytes"),
                    column_name!("tracking.deletedPositions"),
                    column_name!("tracking.replacedPositions"),
                    column_name!("manifestInfo.dv"),
                    column_name!("tracking.firstRowId"),
                ],
                vec![
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::INTEGER,
                    DataType::STRING,
                    DataType::LONG,
                    DataType::BINARY,
                    DataType::BINARY,
                    DataType::BINARY,
                    DataType::LONG,
                ],
            )
                .into()
        });
        NAMES_AND_TYPES.as_ref()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        require_getters(getters.len())?;
        for i in 0..row_count {
            // Only `Added` and `Existing` entries are live. `Deleted` and `Replaced` are not, so
            // drop them without inspecting their content; any other status is unrecognized and
            // rejected rather than silently treated as live.
            let status: i32 = getters[2].get(i, "tracking.status")?;
            if status == TrackingStatus::Deleted as i32 || status == TrackingStatus::Replaced as i32
            {
                self.selection.push(false);
                continue;
            }
            if status != TrackingStatus::Added as i32 && status != TrackingStatus::Existing as i32 {
                return Err(Error::generic(format!(
                    "content tree entry has unrecognized tracking status {status}"
                )));
            }
            let content_type: i32 = getters[0].get(i, "contentType")?;
            let location: String = getters[1].get(i, "location")?;

            // Per-commit position deletes/replaces (serialized `RoaringBitmapArray`) on a live
            // entry describe deletes we cannot yet represent, so reject rather than drop them.
            // TODO: carry AMT tracked position deletes/replaces through to the scan.
            let deleted_positions: Option<&[u8]> =
                getters[5].get_opt(i, "tracking.deletedPositions")?;
            let replaced_positions: Option<&[u8]> =
                getters[6].get_opt(i, "tracking.replacedPositions")?;
            if deleted_positions.is_some() || replaced_positions.is_some() {
                return Err(Error::unsupported(format!(
                    "content tree entry {location:?} has tracked position deletes/replaces, which \
                     the AMT scan read path does not yet support"
                )));
            }

            if content_type == DataContentType::Data as i32 {
                // `deletionVector.location` is a required leaf of the DV struct, so it is non-null
                // iff the whole (nullable) `deletionVector` struct is present. Deletion vectors are
                // not yet carried through, so reject rather than drop them (which would return the
                // file as if it had no deletes).
                let dv_location: Option<String> =
                    getters[3].get_opt(i, "deletionVector.location")?;
                if dv_location.is_some() {
                    return Err(Error::unsupported(format!(
                        "content tree data file {location:?} has a deletion vector, which the AMT \
                         scan read path does not yet support"
                    )));
                }
                // adaptiveMetadata tables always enable row tracking, so a live data file must
                // carry a `firstRowId`. A null one is inherited from the parent `DataManifest`
                // entry; without that inheritance the emitted `add.baseRowId` would be silently
                // wrong, so reject rather than drop it.
                // TODO: inherit firstRowId/defaultRowCommitVersion from the parent DataManifest.
                let first_row_id: Option<i64> = getters[8].get_opt(i, "tracking.firstRowId")?;
                if first_row_id.is_none() {
                    return Err(Error::unsupported(format!(
                        "content tree data file {location:?} has no firstRowId; baseRowId \
                         inheritance from the parent DataManifest entry is not yet supported by \
                         the AMT scan read path"
                    )));
                }
                self.selection.push(true);
            } else if content_type == DataContentType::DataManifest as i32 {
                // `DataManifest` entries are only allowed in the root manifest. Rejecting a nested
                // one enforces the spec and guarantees the walk terminates (no manifest cycles).
                if !self.is_root {
                    return Err(Error::unsupported(format!(
                        "content tree references a nested DataManifest {location:?}, but \
                         DataManifest entries are only allowed in the root manifest"
                    )));
                }
                // A manifest-level deletion vector marks child entries as deleted; we cannot yet
                // apply it, so reject rather than return the child files as if none were deleted.
                // TODO: carry AMT manifest-level deletion vectors through to the scan.
                let manifest_dv: Option<&[u8]> = getters[7].get_opt(i, "manifestInfo.dv")?;
                if manifest_dv.is_some() {
                    return Err(Error::unsupported(format!(
                        "content tree manifest {location:?} has a manifest-level deletion vector, \
                         which the AMT scan read path does not yet support"
                    )));
                }
                let file_size_in_bytes: i64 = getters[4].get(i, "fileSizeInBytes")?;
                self.child_manifests.push(manifest_filemeta(
                    &location,
                    file_size_in_bytes,
                    &self.table_root,
                )?);
                self.selection.push(false);
            } else {
                // PositionDeletes / EqualityDeletes / DeleteManifest: delete-oriented content the
                // AMT scan read path does not handle. Reject rather than swallow it.
                return Err(Error::unsupported(format!(
                    "content tree entry {location:?} has unsupported contentType {content_type}"
                )));
            }
        }
        Ok(())
    }
}

fn require_getters(len: usize) -> DeltaResult<()> {
    if len == 9 {
        Ok(())
    } else {
        Err(Error::InternalError(format!(
            "ManifestEntryVisitor expects 9 getters, got {len}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::actions::visitors::AddVisitor;
    use crate::content_tree::{ContentTreeNodeEntry, DeletionVectorInfo, TrackingInfo};
    use crate::engine::sync::SyncEngine;
    use crate::expressions::StructData;
    use crate::schema::{schema_ref, StructField, StructType};
    use crate::Engine;

    /// Minimal manifest schema exposing exactly the columns [`ManifestEntryVisitor`] projects, so a
    /// test can synthesize a manifest batch without building the full entry schema.
    fn manifest_schema() -> SchemaRef {
        schema_ref! {
            not_null "contentType": INTEGER,
            not_null "location": STRING,
            not_null "tracking": {
                not_null "status": INTEGER,
                nullable "firstRowId": LONG,
                nullable "deletedPositions": BINARY,
                nullable "replacedPositions": BINARY,
            },
            nullable "deletionVector": {
                not_null "location": STRING,
                not_null "offset": LONG,
                not_null "sizeInBytes": LONG,
                not_null "cardinality": LONG,
            },
            not_null "fileSizeInBytes": LONG,
            nullable "manifestInfo": {
                nullable "dv": BINARY,
            },
        }
    }

    /// A `Scalar` for `Option<&[u8]>`: `Scalar::Binary` when present, typed null otherwise.
    fn binary_or_null(bytes: Option<&[u8]>) -> Scalar {
        bytes.map_or(Scalar::Null(DataType::BINARY), |b| {
            Scalar::Binary(b.to_vec())
        })
    }

    fn tracking(
        status: TrackingStatus,
        first_row_id: Option<i64>,
        deleted_positions: Option<&[u8]>,
        replaced_positions: Option<&[u8]>,
    ) -> Scalar {
        tracking_raw(
            status as i32,
            first_row_id,
            deleted_positions,
            replaced_positions,
        )
    }

    /// A `tracking` struct scalar with all fields null except `status`, set to a raw `i32` (so a
    /// test can inject a value outside the defined [`TrackingStatus`] range).
    fn tracking_with_status_value(status: i32) -> Scalar {
        tracking_raw(status, None, None, None)
    }

    fn tracking_raw(
        status: i32,
        first_row_id: Option<i64>,
        deleted_positions: Option<&[u8]>,
        replaced_positions: Option<&[u8]>,
    ) -> Scalar {
        Scalar::Struct(
            StructData::try_new(
                vec![
                    StructField::not_null("status", DataType::INTEGER),
                    StructField::nullable("firstRowId", DataType::LONG),
                    StructField::nullable("deletedPositions", DataType::BINARY),
                    StructField::nullable("replacedPositions", DataType::BINARY),
                ],
                vec![
                    Scalar::from(status),
                    first_row_id.map_or(Scalar::Null(DataType::LONG), Scalar::from),
                    binary_or_null(deleted_positions),
                    binary_or_null(replaced_positions),
                ],
            )
            .unwrap(),
        )
    }

    /// A `manifestInfo` struct scalar exposing just the `dv` leaf the visitor reads.
    fn manifest_info(dv: Option<&[u8]>) -> Scalar {
        Scalar::Struct(
            StructData::try_new(
                vec![StructField::nullable("dv", DataType::BINARY)],
                vec![binary_or_null(dv)],
            )
            .unwrap(),
        )
    }

    fn dv_info_scalar(dv: Option<&DeletionVectorInfo>) -> Scalar {
        let fields = vec![
            StructField::not_null("location", DataType::STRING),
            StructField::not_null("offset", DataType::LONG),
            StructField::not_null("sizeInBytes", DataType::LONG),
            StructField::not_null("cardinality", DataType::LONG),
        ];
        match dv {
            None => Scalar::Null(DataType::from(StructType::new_unchecked(fields))),
            Some(dv) => Scalar::Struct(
                StructData::try_new(
                    fields,
                    vec![
                        Scalar::from(dv.location.clone()),
                        Scalar::from(dv.offset),
                        Scalar::from(dv.size_in_bytes),
                        Scalar::from(dv.cardinality),
                    ],
                )
                .unwrap(),
            ),
        }
    }

    /// Builds a manifest row with no position deletes/replaces and no manifest-level DV.
    fn row(
        content_type: DataContentType,
        location: &str,
        status: TrackingStatus,
        first_row_id: Option<i64>,
        dv: Option<&DeletionVectorInfo>,
        file_size: i64,
    ) -> Vec<Scalar> {
        row_with_tracking_positions(
            content_type,
            location,
            status,
            first_row_id,
            dv,
            file_size,
            None,
            None,
        )
    }

    /// Like [`row`], but sets `tracking.deletedPositions` / `tracking.replacedPositions`.
    #[allow(clippy::too_many_arguments)]
    fn row_with_tracking_positions(
        content_type: DataContentType,
        location: &str,
        status: TrackingStatus,
        first_row_id: Option<i64>,
        dv: Option<&DeletionVectorInfo>,
        file_size: i64,
        deleted_positions: Option<&[u8]>,
        replaced_positions: Option<&[u8]>,
    ) -> Vec<Scalar> {
        vec![
            Scalar::from(content_type as i32),
            Scalar::from(location.to_string()),
            tracking(status, first_row_id, deleted_positions, replaced_positions),
            dv_info_scalar(dv),
            Scalar::from(file_size),
            manifest_info(None),
        ]
    }

    /// Builds a `DataManifest` row whose `manifestInfo.dv` is `dv`.
    fn manifest_row_with_dv(dv: Option<&[u8]>) -> Vec<Scalar> {
        vec![
            Scalar::from(DataContentType::DataManifest as i32),
            Scalar::from("metadata/leaf-1.parquet".to_string()),
            tracking(TrackingStatus::Existing, None, None, None),
            dv_info_scalar(None),
            Scalar::from(2048i64),
            manifest_info(dv),
        ]
    }

    /// Table root child `DataManifest` locations resolve against in the visitor tests.
    const TEST_TABLE_ROOT: &str = "file:///tmp/table/";

    /// Runs [`ManifestEntryVisitor`] over `rows` as the root manifest, returning the visitor or the
    /// visit error.
    fn try_visit(rows: Vec<Vec<Scalar>>) -> DeltaResult<ManifestEntryVisitor> {
        try_visit_at(rows, true)
    }

    /// Like [`try_visit`], but lets the caller choose whether the manifest is the root.
    fn try_visit_at(rows: Vec<Vec<Scalar>>, is_root: bool) -> DeltaResult<ManifestEntryVisitor> {
        let table_root = Url::parse(TEST_TABLE_ROOT).unwrap();
        let data = SyncEngine::new()
            .evaluation_handler()
            .create_many(manifest_schema(), rows)?;
        let mut visitor = ManifestEntryVisitor::new(&table_root, is_root);
        visitor.visit_rows_of(data.as_ref())?;
        Ok(visitor)
    }

    /// Like [`try_visit`], asserting the visit succeeds.
    fn visit(rows: Vec<Vec<Scalar>>) -> ManifestEntryVisitor {
        try_visit(rows).unwrap()
    }

    #[test]
    fn visitor_collects_live_data_and_child_manifests() {
        let visitor = visit(vec![
            row(
                DataContentType::Data,
                "data/f1.parquet",
                TrackingStatus::Added,
                Some(100),
                None,
                1024,
            ),
            row(
                DataContentType::DataManifest,
                "metadata/leaf-1.parquet",
                TrackingStatus::Existing,
                None,
                None,
                2048,
            ),
        ]);
        // The live data row is selected; the child-manifest pointer is not (it is recursed into).
        assert_eq!(visitor.selection, vec![true, false]);
        assert_eq!(visitor.child_manifests.len(), 1);
        assert_eq!(
            visitor.child_manifests[0].location.as_str(),
            "file:///tmp/table/metadata/leaf-1.parquet"
        );
        assert_eq!(visitor.child_manifests[0].size, 2048);
    }

    #[rstest]
    #[case::deleted(TrackingStatus::Deleted)]
    #[case::replaced(TrackingStatus::Replaced)]
    fn visitor_skips_non_live_data_entries(#[case] status: TrackingStatus) {
        // Only `Added`/`Existing` are live; `Deleted` and `Replaced` are legitimately not live, so
        // they are unselected without error.
        let visitor = visit(vec![row(
            DataContentType::Data,
            "data/non-live.parquet",
            status,
            None,
            None,
            1,
        )]);
        assert_eq!(visitor.selection, vec![false]);
        assert!(visitor.child_manifests.is_empty());
    }

    #[test]
    fn visitor_rejects_unrecognized_tracking_status() {
        // A status outside the defined set cannot be classified as live or not, so reject it
        // rather than guess. `TrackingStatus` defines 0..=3; 99 is unrecognized.
        let mut row = row(
            DataContentType::Data,
            "data/f1.parquet",
            TrackingStatus::Added,
            None,
            None,
            1,
        );
        row[2] = tracking_with_status_value(99);
        let result = try_visit(vec![row]);
        assert_result_error_with_message(result, "unrecognized tracking status");
    }

    #[test]
    fn visitor_rejects_nested_data_manifest() {
        // `DataManifest` entries are root-only; a non-root manifest containing one is rejected
        // (this is also what guarantees the manifest walk cannot cycle).
        let result = try_visit_at(vec![manifest_row_with_dv(None)], false);
        assert_result_error_with_message(result, "nested DataManifest");
    }

    #[rstest]
    #[case::position_deletes(DataContentType::PositionDeletes)]
    #[case::equality_deletes(DataContentType::EqualityDeletes)]
    #[case::delete_manifest(DataContentType::DeleteManifest)]
    fn visitor_rejects_delete_content_types(#[case] content_type: DataContentType) {
        let result = try_visit(vec![row(
            content_type,
            "data/delete-content.parquet",
            TrackingStatus::Added,
            None,
            None,
            1,
        )]);
        assert_result_error_with_message(result, "unsupported contentType");
    }

    #[test]
    fn visitor_rejects_data_entry_with_deletion_vector() {
        let dv = DeletionVectorInfo {
            location: "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: 4,
            size_in_bytes: 48,
            cardinality: 6,
        };
        let result = try_visit(vec![row(
            DataContentType::Data,
            "data/f1.parquet",
            TrackingStatus::Existing,
            None,
            Some(&dv),
            1024,
        )]);
        assert_result_error_with_message(result, "has a deletion vector");
    }

    #[test]
    fn visitor_rejects_live_data_entry_with_null_first_row_id() {
        // A live data file with no `firstRowId` would inherit it from the parent DataManifest; that
        // inheritance is not wired in, so emitting a null `baseRowId` would be silently wrong.
        let result = try_visit(vec![row(
            DataContentType::Data,
            "data/f1.parquet",
            TrackingStatus::Added,
            None,
            None,
            1024,
        )]);
        assert_result_error_with_message(result, "has no firstRowId");
    }

    #[rstest]
    // A live entry with either tracked positions column set is rejected.
    #[case::deleted(Some(&[1u8, 2, 3][..]), None)]
    #[case::replaced(None, Some(&[4u8, 5][..]))]
    #[case::both(Some(&[1u8][..]), Some(&[2u8][..]))]
    fn visitor_rejects_tracked_position_deletes(
        #[case] deleted_positions: Option<&[u8]>,
        #[case] replaced_positions: Option<&[u8]>,
    ) {
        let result = try_visit(vec![row_with_tracking_positions(
            DataContentType::Data,
            "data/f1.parquet",
            TrackingStatus::Added,
            None,
            None,
            1024,
            deleted_positions,
            replaced_positions,
        )]);
        assert_result_error_with_message(result, "tracked position deletes/replaces");
    }

    #[test]
    fn visitor_rejects_manifest_with_manifest_level_deletion_vector() {
        let result = try_visit(vec![manifest_row_with_dv(Some(&[1u8, 2, 3]))]);
        assert_result_error_with_message(result, "manifest-level deletion vector");
    }

    #[test]
    fn visitor_accepts_manifest_with_null_manifest_info_dv() {
        // A `DataManifest` whose `manifestInfo.dv` is null is a normal child manifest to recurse.
        let visitor = visit(vec![manifest_row_with_dv(None)]);
        assert_eq!(visitor.selection, vec![false]);
        assert_eq!(visitor.child_manifests.len(), 1);
    }

    // === End-to-end manifest-tree read (real Parquet round-trip) ===

    /// Builds a full [`ContentTreeNodeEntry`] row (all schema fields, in schema order) from the few
    /// values the read path cares about, nulling every other nullable field. Building by schema
    /// order keeps the row aligned with `ContentTreeNodeEntry::to_schema()` for `create_many`.
    ///
    /// [`ContentTreeNodeEntry`]: super::ContentTreeNodeEntry
    fn full_entry_row(
        content_type: DataContentType,
        location: &str,
        status: TrackingStatus,
        first_row_id: Option<i64>,
        file_size: i64,
    ) -> Vec<Scalar> {
        let tracking = Scalar::from(TrackingInfo {
            status,
            snapshot_id: None,
            dv_snapshot_id: None,
            sequence_number: None,
            file_sequence_number: None,
            first_row_id,
            deleted_positions: None,
            replaced_positions: None,
        });
        ContentTreeNodeEntry::to_schema()
            .fields()
            .map(|field| match field.name().as_str() {
                "contentType" => Scalar::from(content_type as i32),
                "location" => Scalar::from(location.to_string()),
                "fileFormat" => Scalar::from("parquet".to_string()),
                "tracking" => tracking.clone(),
                "specId" => Scalar::from(0i32),
                "recordCount" => Scalar::from(0i64),
                "fileSizeInBytes" => Scalar::from(file_size),
                "formatVersion" => Scalar::from(1i32),
                other => {
                    assert!(
                        field.is_nullable(),
                        "entry field {other:?} is non-nullable but not set by the test helper"
                    );
                    Scalar::Null(field.data_type().clone())
                }
            })
            .collect()
    }

    /// Writes `rows` as a manifest Parquet file at `path` and returns its size in bytes.
    fn write_manifest(engine: &dyn Engine, path: &Url, rows: Vec<Vec<Scalar>>) -> u64 {
        let data = engine
            .evaluation_handler()
            .create_many(Arc::new(ContentTreeNodeEntry::to_schema()), rows)
            .unwrap();
        let iter: crate::DeltaResultIteratorStatic<Box<dyn EngineData>> =
            Box::new(std::iter::once(Ok(data)));
        engine
            .parquet_handler()
            .write_parquet_file(path.clone(), iter)
            .unwrap();
        std::fs::metadata(path.to_file_path().unwrap())
            .unwrap()
            .len()
    }

    /// Reads all `add` actions produced by [`read_content_tree_add_actions`], sorted by path.
    fn collect_adds(batches: &[Box<dyn EngineData>]) -> DeltaResult<Vec<Add>> {
        let mut visitor = AddVisitor::default();
        for batch in batches {
            visitor.visit_rows_of(batch.as_ref())?;
        }
        let mut adds = visitor.adds;
        adds.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(adds)
    }

    #[test]
    fn read_content_tree_walks_root_and_leaf_manifests() -> DeltaResult<()> {
        let temp = tempfile::tempdir().unwrap();
        let table_root = Url::from_directory_path(temp.path()).unwrap();
        let engine = SyncEngine::new();

        // Leaf manifest: one live data file.
        let leaf_url = table_root.join("metadata/leaf-1.parquet").unwrap();
        let leaf_size = write_manifest(
            &engine,
            &leaf_url,
            vec![full_entry_row(
                DataContentType::Data,
                "data/leaf-f1.parquet",
                TrackingStatus::Added,
                Some(200),
                2048,
            )],
        );

        // Root manifest: a live data file, a pointer to the leaf manifest, and a deleted data file
        // (which must not surface as an add action).
        let root_url = table_root.join("metadata/root.parquet").unwrap();
        let root_size = write_manifest(
            &engine,
            &root_url,
            vec![
                full_entry_row(
                    DataContentType::Data,
                    "data/root-f1.parquet",
                    TrackingStatus::Added,
                    Some(100),
                    1024,
                ),
                full_entry_row(
                    DataContentType::DataManifest,
                    "metadata/leaf-1.parquet",
                    TrackingStatus::Existing,
                    None,
                    leaf_size as i64,
                ),
                full_entry_row(
                    DataContentType::Data,
                    "data/deleted.parquet",
                    TrackingStatus::Deleted,
                    None,
                    512,
                ),
            ],
        );

        let content_root =
            ContentRoot::new("metadata/root.parquet".to_string(), root_size as i64, 0);
        let batches = read_content_tree_add_actions(&engine, &table_root, &content_root)?;
        let adds = collect_adds(&batches)?;

        // The two live data files (leaf + root) surface; the deleted one does not.
        let paths: Vec<&str> = adds.iter().map(|a| a.path.as_str()).collect();
        assert_eq!(paths, vec!["data/leaf-f1.parquet", "data/root-f1.parquet"]);

        // `add.path` is the manifest `location` verbatim (relative); `size` and `baseRowId` come
        // from the entry, while `modificationTime`/`dataChange` are broadcast constants. Assert the
        // full remap, not just the path.
        let leaf = &adds[0];
        assert_eq!(leaf.size, 2048);
        assert_eq!(leaf.base_row_id, Some(200));
        assert_eq!(leaf.modification_time, 0);
        assert!(!leaf.data_change);

        let root = &adds[1];
        assert_eq!(root.size, 1024);
        assert_eq!(root.base_row_id, Some(100));
        assert_eq!(root.modification_time, 0);
        assert!(!root.data_change);
        Ok(())
    }
}
