//! Reads checkpoint roots and their optional V2 sidecars as one action relation.

use std::sync::LazyLock;

use url::Url;

use super::VERSION;
use crate::actions::{ADD_NAME, REMOVE_NAME, SIDECAR_FIELD, SIDECAR_NAME, action_presence_leaf};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{Expression as Expr, Predicate, col, column_name};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::schema::{SchemaRef, lazy_schema_ref};
use crate::{KernelResult, PlanBuilder};

/// Reads inline actions, or file actions from sidecars plus non-file actions from the root.
pub(crate) fn checkpoint_actions(
    segment: &LogSegment,
    shape: &CheckpointShape,
    schema: SchemaRef,
) -> KernelResult<PlanBuilder> {
    let Some((file_type, parts)) = segment.checkpoint_version_tagged_scan_files()? else {
        return PlanBuilder::values(schema, vec![]);
    };
    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    if shape.checkpoint_type != CheckpointType::Manifest {
        return scan(parts, &[VERSION], schema);
    }
    let files = match segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
        Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema.clone()),
        None => sidecar_actions(file_type, parts.clone(), schema.clone(), &segment.log_root),
    }?;
    let non_file_presence = Predicate::or_from(schema.fields().filter_map(|field| {
        if [ADD_NAME, REMOVE_NAME, SIDECAR_NAME, "checkpointMetadata"]
            .contains(&field.name().as_str())
        {
            return None;
        }
        action_presence_leaf(field.name())
            .map(|leaf| Expr::column([field.name().as_str(), leaf]).is_not_null())
    }));
    // File-only scans do not need the manifest's non-file actions.
    if non_file_presence == Predicate::FALSE {
        return Ok(files);
    }
    PlanBuilder::union_all([
        files,
        scan(parts, &[VERSION], schema)?.filter(non_file_presence)?,
    ])
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
