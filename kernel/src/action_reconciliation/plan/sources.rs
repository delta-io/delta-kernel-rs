//! Physical action reads with a shared plan/options contract.

use std::sync::LazyLock;

use url::Url;

use super::{ActionOptions, ActionPlan, VERSION, presence_predicate, read_source};
use crate::actions::{
    ADD_NAME, ActionKind, Add, LOG_ADD_SCHEMA, REMOVE_NAME, SIDECAR_FIELD, SIDECAR_NAME,
    get_all_actions_schema,
};
use crate::checkpoint::{CheckpointShape, CheckpointType};
use crate::expressions::{Expression as Expr, col, column_name, lit};
use crate::log_segment::LogSegment;
use crate::plans::ir::nodes::{DynamicScan, FileType, ScanFile};
use crate::schema::{DataType, SchemaRef, StructField, lazy_schema_ref};
use crate::table_configuration::TableConfiguration;
use crate::{KernelError, KernelResult, PlanBuilder, Version, version_as_i64};

/// Reads CRC live adds; retained tombstones and non-file actions are unavailable.
pub(crate) fn crc_actions(
    version: Version,
    files: Vec<Add>,
    options: &ActionOptions,
) -> KernelResult<ActionPlan> {
    let version = version_as_i64(version)?;
    let options = ActionOptions {
        actions: vec![ActionKind::FileActions {
            includes_removes: false,
        }],
        ..options.clone()
    };
    read_source(None, &options, &LOG_ADD_SCHEMA, |schema| {
        PlanBuilder::values(
            LOG_ADD_SCHEMA.clone(),
            files.into_iter().map(|add| vec![add.into()]).collect(),
        )?
        .project_patch(|patch| {
            let patch = if schema.field_at(&column_name!(ADD_NAME, "stats")).is_ok() {
                patch
            } else {
                patch.drop_at([ADD_NAME], "stats")
            };
            patch.append(StructField::not_null(VERSION, DataType::LONG), lit(version))
        })
    })
}

/// Reads selected checkpoint actions. Non-file reads use the root without sidecar discovery.
/// File reads require a checkpoint shape; parsed metadata types come from table configuration.
pub(crate) fn checkpoint_actions(
    segment: &LogSegment,
    shape: Option<&CheckpointShape>,
    table_config: Option<&TableConfiguration>,
    options: &ActionOptions,
) -> KernelResult<ActionPlan> {
    let selection = CheckpointSelection::for_options(options);
    read_source(
        table_config,
        options,
        shape
            .and_then(|shape| shape.leaf_checkpoint_schema.as_deref())
            .unwrap_or(get_all_actions_schema()),
        |schema| {
            read_checkpoint(segment, shape, schema.clone(), selection)?
                .filter(presence_predicate(schema.fields()))
        },
    )
}

/// Reads selected JSON commits strictly newer than the base, with raw metadata for later parsing.
pub(crate) fn commit_actions(
    segment: &LogSegment,
    after_version: Option<Version>,
    options: &ActionOptions,
) -> KernelResult<ActionPlan> {
    let files = match after_version {
        Some(version) => segment
            .segment_after_version(version)
            .commit_cover_version_tagged_scan_files()?,
        None => segment.commit_cover_version_tagged_scan_files()?,
    };
    read_source(None, options, get_all_actions_schema(), |schema| {
        PlanBuilder::scan_json(files, &[VERSION], schema.clone())?
            .filter(presence_predicate(schema.fields()))
    })
}

/// Requested portions of a checkpoint; content means add/remove actions stored in V2 sidecars.
#[derive(Clone, Copy)]
enum CheckpointSelection {
    ContentOnly,
    NonContentOnly,
    Mixed,
}

impl CheckpointSelection {
    fn for_options(options: &ActionOptions) -> Self {
        let content = options
            .actions
            .iter()
            .any(|kind| matches!(kind, ActionKind::FileActions { .. }));
        let non_content = options
            .actions
            .iter()
            .any(|kind| !matches!(kind, ActionKind::FileActions { .. }));
        match (content, non_content) {
            (true, false) => Self::ContentOnly,
            (false, _) => Self::NonContentOnly,
            (true, true) => Self::Mixed,
        }
    }
}

/// Reads inline actions, or file actions from sidecars plus non-file actions from the root.
fn read_checkpoint(
    segment: &LogSegment,
    shape: Option<&CheckpointShape>,
    schema: SchemaRef,
    selection: CheckpointSelection,
) -> KernelResult<PlanBuilder> {
    let Some((file_type, parts)) = segment.checkpoint_version_tagged_scan_files()? else {
        return PlanBuilder::values(schema, vec![]);
    };
    let scan = match file_type {
        FileType::Json => PlanBuilder::scan_json,
        FileType::Parquet => PlanBuilder::scan_parquet,
    };
    if matches!(
        selection,
        CheckpointSelection::ContentOnly | CheckpointSelection::Mixed
    ) {
        let shape =
            shape.ok_or_else(|| KernelError::schema("File reads require a checkpoint shape"))?;
        // V1 and inline V2 keep file and non-file actions in the same root read.
        if shape.checkpoint_type != CheckpointType::Manifest {
            return scan(parts, &[VERSION], schema);
        }
    }
    let read_content =
        |parts, schema| match segment.checkpoint_hint_version_tagged_sidecar_scan_files()? {
            Some(sidecars) => PlanBuilder::scan_parquet(sidecars, &[VERSION], schema),
            None => sidecar_actions(file_type, parts, schema, &segment.log_root),
        };
    match selection {
        CheckpointSelection::ContentOnly => read_content(parts, schema),
        CheckpointSelection::NonContentOnly => scan(parts, &[VERSION], schema),
        CheckpointSelection::Mixed => {
            let non_file_presence = presence_predicate(
                schema
                    .fields()
                    .filter(|field| !matches!(field.name().as_str(), ADD_NAME | REMOVE_NAME)),
            );
            PlanBuilder::union_all([
                read_content(parts.clone(), schema.clone())?,
                scan(parts, &[VERSION], schema)?.filter(non_file_presence)?,
            ])
        }
    }
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
