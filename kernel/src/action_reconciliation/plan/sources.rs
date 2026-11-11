use std::sync::LazyLock;

use url::Url;

use super::VERSION;
use crate::action_reconciliation::file_metadata::STATS_FIELD as STATS;
use crate::actions::{ADD_NAME, Add, LOG_ADD_SCHEMA, SIDECAR_FIELD, SIDECAR_NAME};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{Expression as Expr, col, column_name};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::schema::{SchemaRef, lazy_schema_ref};
use crate::{KernelResult, PlanBuilder, Version};

/// Build add rows from CRC files.
pub(crate) fn crc_arm(files: Vec<Add>, schema: SchemaRef) -> KernelResult<PlanBuilder> {
    PlanBuilder::values(
        LOG_ADD_SCHEMA.clone(),
        files.into_iter().map(|add| vec![add.into()]).collect(),
    )?
    .project_patch(|patch| {
        // Canonical CRC adds can only omit JSON stats from the read schema.
        if schema.field_at(&column_name!(ADD_NAME, STATS)).is_ok() {
            patch
        } else {
            patch.drop_at([ADD_NAME], STATS)
        }
    })
}

/// Build a leaf-checkpoint or V2 sidecar source with file versions in `version`.
/// Returns an empty relation when no checkpoint exists.
pub(crate) fn checkpoint_arm(
    log_segment: &LogSegment,
    shape: &CheckpointShape,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;
    match (&shape.checkpoint_type, checkpoint) {
        (CheckpointType::Leaf, Some((FileType::Parquet, parts))) => {
            PlanBuilder::scan_parquet(parts, &[VERSION], schema)
        }
        (CheckpointType::Leaf, Some((FileType::Json, parts))) => {
            PlanBuilder::scan_json(parts, &[VERSION], schema)
        }
        (CheckpointType::Manifest, Some((file_type, parts))) => {
            match log_segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
                Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
                // Without a complete hint, load the sidecars referenced by the
                // manifest.
                None => sidecar_actions(file_type, parts, schema, &log_segment.log_root),
            }
        }
        (CheckpointType::None, _) | (_, None) => PlanBuilder::values(schema, vec![]),
    }
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
