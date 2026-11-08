use super::metadata_planner::{FILE_ACTION_KEY_FIELD, file_action_key_expr};
use super::*;

impl Scan {
    pub(super) fn crc_arm(
        &self,
        files: Vec<Add>,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<PlanBuilder> {
        planner
            .build_metadata_scan(&LOG_ADD_SCHEMA, false, |schema| {
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
            })?
            .project_patch(|patch| {
                patch.append(
                    FILE_ACTION_KEY_FIELD.clone(),
                    file_action_key_expr(|col| joined_column_expr!("add", col)),
                )
            })
    }

    /// Build normalized checkpoint adds. Returns an empty relation when no checkpoint exists.
    ///
    /// ## SQL equivalent:
    //
    /// SELECT STRUCT(
    ///          add.* EXCEPT (
    ///            stats_parsed, partitionValues_parsed
    ///          ),
    ///          add.stats_parsed AS stats_parsed,
    ///          add.partitionValues_parsed AS partitionValues_parsed
    ///        ) AS add,
    ///        file_key(add) AS key
    /// FROM checkpoint_actions
    /// WHERE add.path IS NOT NULL
    ///
    /// When the checkpoint lacks native parsed metadata, `FROM_JSON(add.stats, physical_stats)`
    /// and `MAP_TO_STRUCT(add.partitionValues, physical_partitions)` replace the corresponding
    /// fields above. A parsed field is omitted when its schema is absent.
    pub(super) fn checkpoint_arm(
        &self,
        shape: &CheckpointShape,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let available_file_schema = shape
            .leaf_checkpoint_schema
            .as_deref()
            .unwrap_or(get_all_actions_schema());
        let checkpoint = log_segment.checkpoint_version_tagged_scan_files()?;
        let source = |schema| match (&shape.checkpoint_type, checkpoint) {
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
        };
        planner
            .build_metadata_scan(available_file_schema, false, |schema| {
                source(schema)?.filter(Predicate::from_expr(expr!(is_not_null(col("add.path")))))
            })?
            .project_patch(|patch| {
                patch
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| joined_column_expr!("add", col)),
                    )
                    .drop(VERSION)
            })
    }

    /// Build the normalized commit JSON arm.
    /// When `base_version` is set, only commits newer than that base are read.
    ///
    /// ## SQL equivalent:
    ///
    /// SELECT STRUCT(
    ///          add.* EXCEPT (stats_parsed, partitionValues_parsed),
    ///          FROM_JSON(add.stats, physical_stats) AS stats_parsed,
    ///          MAP_TO_STRUCT(add.partitionValues, physical_partitions) AS partitionValues_parsed
    ///        ) AS add,
    ///        version,
    ///        file_key(COALESCE(add, remove)) AS key
    /// FROM json_commits
    /// WHERE add.path IS NOT NULL OR remove.path IS NOT NULL
    ///
    /// A parsed field is omitted when its schema is absent.
    pub(super) fn commit_arm(
        &self,
        base_version: Option<Version>,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<PlanBuilder> {
        let log_segment = self.snapshot.log_segment();
        let commit_files = match base_version {
            Some(version) => log_segment
                .segment_after_version(version)
                .commit_cover_version_tagged_scan_files()?,
            None => log_segment.commit_cover_version_tagged_scan_files()?,
        };
        planner
            .build_metadata_scan(get_all_actions_schema(), true, |schema| {
                PlanBuilder::scan_json(commit_files, &[VERSION], schema)?.filter(
                    Predicate::from_expr(expr!(or(
                        is_not_null(col("add.path")),
                        is_not_null(col("remove.path")),
                    ))),
                )
            })?
            .project_patch(|patch| {
                patch
                    .append(
                        FILE_ACTION_KEY_FIELD.clone(),
                        file_action_key_expr(|col| {
                            Expr::coalesce([
                                joined_column_expr!("add", col),
                                joined_column_expr!("remove", col),
                            ])
                        }),
                    )
                    .drop(REMOVE_NAME)
            })
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
