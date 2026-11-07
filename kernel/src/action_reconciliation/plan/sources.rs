//! Physical action reads using the caller's requested schema.

use std::sync::LazyLock;

use url::Url;

use super::VERSION;
use crate::actions::{
    ADD_NAME, ActionSet, Add, LOG_ADD_SCHEMA, REMOVE_FIELD, SIDECAR_FIELD, SIDECAR_NAME,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{Expression as Expr, col, column_name, lit, null_lit};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::schema::{DataType, SchemaRef, StructField, lazy_schema_ref};
use crate::{KernelError, KernelResult, PlanBuilder, Version};

const STATS: &str = "stats";

/// Reads selected checkpoint actions. Non-content metadata reads use the root without sidecar
/// discovery.
/// Supplies `version`; absent checkpoints and empty selections return an empty relation.
pub(crate) fn checkpoint_actions(
    segment: &LogSegment,
    shape: Option<&CheckpointShape>,
    actions: ActionSet,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let Some((file_type, parts)) = segment.checkpoint_version_tagged_scan_files()? else {
        // The plan builder eliminates this empty relation from replay unions.
        return PlanBuilder::values(schema, vec![]);
    };
    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    match (
        actions.contains_content_actions(),
        shape.map(|shape| &shape.checkpoint_type),
    ) {
        (false, _) if !actions.contains_non_content_actions() => {
            PlanBuilder::values(schema, vec![])
        }
        (false, _) | (true, Some(CheckpointType::Leaf)) => scan(parts, &[VERSION], schema),
        (true, None) => Err(KernelError::schema("File reads require a checkpoint shape")),
        (true, Some(CheckpointType::None)) => PlanBuilder::values(schema, vec![]),
        (true, Some(CheckpointType::Manifest)) => {
            let files = match segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema.clone()),
                // Without a complete hint, load the sidecars referenced by the manifest.
                None => {
                    sidecar_actions(file_type, parts.clone(), schema.clone(), &segment.log_root)
                }
            }?;
            if !actions.contains_non_content_actions() {
                return Ok(files);
            }
            PlanBuilder::union_all([
                files.filter(actions.content_predicate())?,
                scan(parts, &[VERSION], schema)?.filter(actions.non_content_predicate())?,
            ])
        }
    }
}

/// Build add rows from CRC files with the supplied `version`.
/// Requested remove columns are NULL because CRCs contain only live adds.
pub(crate) fn crc_arm(
    files: Vec<Add>,
    version: Version,
    includes_removes: bool,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let version = crate::version_as_i64(version)?;
    PlanBuilder::values(
        LOG_ADD_SCHEMA.clone(),
        files.into_iter().map(|add| vec![add.into()]).collect(),
    )?
    .project_patch(|mut patch| {
        if includes_removes {
            patch = patch.append(
                REMOVE_FIELD.clone(),
                null_lit(REMOVE_FIELD.data_type().clone()),
            );
        }
        let patch = patch.append(StructField::nullable(VERSION, DataType::LONG), lit(version));
        if schema.field_at(&column_name!(ADD_NAME, STATS)).is_ok() {
            patch
        } else {
            patch.drop_at([ADD_NAME], STATS)
        }
    })
}

/// Build the commit JSON source with file versions in `version`.
/// When `base_version` is set, only commits newer than that base are read.
pub(crate) fn commit_arm(
    log_segment: &LogSegment,
    base_version: Option<Version>,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let commit_files = match base_version {
        Some(version) => log_segment
            .segment_after_version(version)
            .commit_cover_version_tagged_scan_files()?,
        None => log_segment.commit_cover_version_tagged_scan_files()?,
    };
    PlanBuilder::scan_json(commit_files, &[VERSION], schema)
}

/// Read actions from V2 checkpoint sidecars.
fn sidecar_actions(
    file_type: FileType,
    root_parts: Vec<ScanFile>,
    action_schema: SchemaRef,
    log_root: &Url,
) -> KernelResult<PlanBuilder> {
    const FILE_PATH: &str = "path";
    const FILE_SIZE: &str = "size";
    const FILE_MOD: &str = "filemod";
    const SIDECAR_SIZE: &str = "sizeInBytes";
    const SIDECAR_FILE_MOD: &str = "modificationTime";

    static SIDECAR_FILE_META_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        not_null FILE_PATH: STRING,
        not_null FILE_SIZE: LONG,
        not_null FILE_MOD: LONG,
        nullable VERSION: LONG,
    };

    static SIDECAR_READ_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
        (&SIDECAR_FIELD),
        nullable VERSION: LONG,
    };

    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    let sidecar_files = scan(root_parts, &[VERSION], SIDECAR_READ_SCHEMA.clone())?
        .filter(col!(SIDECAR_NAME, FILE_PATH).is_not_null())?
        .project(
            Expr::struct_from([
                col!(SIDECAR_NAME, FILE_PATH),
                col!(SIDECAR_NAME, SIDECAR_SIZE),
                col!(SIDECAR_NAME, SIDECAR_FILE_MOD),
                col!(VERSION),
            ]),
            SIDECAR_FILE_META_SCHEMA.clone(),
        )?;

    let dynamic_scan = DynamicScan::try_new(
        &SIDECAR_FILE_META_SCHEMA,
        action_schema,
        FileType::Parquet,
        log_root.join("_sidecars/")?,
        [VERSION],
        column_name!(FILE_PATH),
        column_name!(FILE_SIZE),
        column_name!(FILE_MOD),
        None,
    )?;

    sidecar_files.dynamic_scan(dynamic_scan)
}
