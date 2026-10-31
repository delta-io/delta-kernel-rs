//! Declarative metadata scan plans.
//!
//! [`Scan::declarative_metadata_scan_plan`](super::Scan::declarative_metadata_scan_plan) reconciles
//! checkpoint and commit actions into live adds, applying metadata pruning before
//! newest-action-wins replay.

use std::borrow::Cow;
#[cfg(test)]
use std::sync::Arc;

use super::data_skipping::as_sql_data_skipping_predicate_with_stats_columns;
use super::state_info::StateInfo;
use super::{PhysicalPredicate, Scan};
#[cfg(test)]
use crate::KernelError;
use crate::KernelResult;
use crate::action_reconciliation::file_metadata::PARTITION_VALUES_PARSED_FIELD as PARTITION_VALUES_PARSED;
#[cfg(test)]
use crate::action_reconciliation::file_metadata::STATS_FIELD as STATS;
pub(super) use crate::action_reconciliation::plan::MetadataPlanner;
use crate::action_reconciliation::plan::VERSION;
use crate::action_reconciliation::plan::action_set::ActionSet;
use crate::actions::{ADD_NAME, STATS_PARSED};
#[cfg(test)]
use crate::actions::{ADD_SCHEMA, REMOVE_NAME};
#[cfg(test)]
use crate::checkpoint::CheckpointShape;
use crate::expressions::{ColumnName, Predicate, lit};
#[cfg(test)]
use crate::expressions::{Expression as Expr, expr};
use crate::plans::ir::plan::Plan;
#[cfg(test)]
use crate::schema::{
    DataType, SchemaRef, SchemaStructPatchBuilder, StructField, schema, schema_ref,
};
use crate::transforms::{ExpressionTransform, transform_output_type};
#[cfg(test)]
use crate::utils::FoldWithOption as _;

mod metadata_replay_base;

pub(super) use metadata_replay_base::MetadataReplayBase;
// === Internal column names ===

#[cfg(test)]
const IS_ADD: &str = "is_add";

impl<'a> MetadataPlanner<'a> {
    pub(super) fn try_from_scan(scan: &'a Scan) -> KernelResult<Self> {
        let enable_metadata_filter =
            !scan.stats.synthesize_json || scan.state_info.physical_stats_output_schema().is_some();
        let stats_filter = enable_metadata_filter
            .then(|| stats_skipping_predicate(&scan.state_info))
            .flatten();
        let need_partition_schema =
            stats_filter.is_some() || scan.partition_values_options.parsed_struct;
        let parsed_partition_schema = need_partition_schema
            .then(|| scan.state_info.physical_partition_schema.as_ref())
            .flatten();

        let adds_actions = ActionSet::live_adds();
        Ok(Self::new(scan.snapshot.log_segment(), adds_actions)
            .with_stats(scan.stats.clone())
            .with_stats_schemas(
                scan.stats
                    .json_synthesis_schema(scan.snapshot.table_configuration())?
                    .or_else(|| scan.state_info.physical_stats_read_schema().cloned()),
                scan.state_info.physical_stats_output_schema().cloned(),
            )
            .with_partition_values(
                scan.partition_values_options.clone(),
                parsed_partition_schema.cloned(),
            )
            .with_read_collations(scan.needs_collation_stats_schema())
            .with_stats_filter(stats_filter))
    }
}

impl Scan {
    /// Build the live-add metadata plan from checkpoint and commit actions.
    ///
    /// Returns `None` for an empty result.
    #[tracing::instrument(
        name = "scan_plan.build_metadata_scan_plan",
        skip_all,
        fields(enable_call_frame),
        err
    )]
    pub(super) fn build_metadata_scan_plan_with(
        &self,
        base: MetadataReplayBase,
        mut planner: MetadataPlanner<'_>,
    ) -> KernelResult<Option<Plan>> {
        let state = &self.state_info;
        if state.physical_predicate == PhysicalPredicate::StaticSkipAll {
            return Ok(None);
        }

        let base_version = base.version();
        let checkpoint = match base {
            MetadataReplayBase::Crc { version, files } => planner.crc_adds(version, files),
            MetadataReplayBase::Checkpoint { shape, .. } => {
                planner = planner.with_checkpoint_shape(shape);
                planner.checkpoint_actions()
            }
        }?;
        // Keep commit removes through replay so they cancel older adds before selecting live files.
        let commits = planner.commit_actions(base_version)?;
        planner
            .replay_actions(commits, checkpoint)?
            .project_patch(|patch| patch.drop(VERSION))?
            .build_opt()
    }

    #[cfg(test)]
    fn build_metadata_scan_plan(
        &self,
        shape: &CheckpointShape,
        planner: &MetadataPlanner<'_>,
    ) -> KernelResult<Option<Plan>> {
        let base = MetadataReplayBase::Checkpoint {
            version: self.snapshot.log_segment().checkpoint_version,
            shape: shape.clone(),
        };
        self.build_metadata_scan_plan_with(base, planner.clone())
    }
}
// === Helpers ===

/// Physical checkpoint schema used by plan-shape tests.
#[cfg(test)]
fn parquet_read_schema(
    physical_stats: Option<&SchemaRef>,
    physical_partitions: Option<&SchemaRef>,
) -> KernelResult<SchemaRef> {
    let add_patch = SchemaStructPatchBuilder::new()
        .fold_with(physical_stats, |patch, schema| {
            patch.append(StructField::nullable(STATS_PARSED, schema.as_ref().clone()))
        })
        .fold_with(physical_partitions, |patch, schema| {
            patch.append(StructField::nullable(
                PARTITION_VALUES_PARSED,
                schema.as_ref().clone(),
            ))
        });
    Ok(schema_ref! {
        nullable ADD_NAME: (add_patch.build(&ADD_SCHEMA)?),
        nullable VERSION: LONG,
    })
}

/// Build the metadata pruning predicate, or `None` when no pruning is possible.
fn stats_skipping_predicate(state: &StateInfo) -> Option<Predicate> {
    /// Re-roots metadata columns under `add`.
    struct MetadataSkippingColumnPrefixer;

    impl<'a> ExpressionTransform<'a> for MetadataSkippingColumnPrefixer {
        transform_output_type!(|'a, T| Cow<'a, T>);

        fn transform_expr_column(&mut self, name: &'a ColumnName) -> Cow<'a, ColumnName> {
            let path = name.path();
            let replacement_root = match path.first().map(String::as_str) {
                Some(STATS_PARSED) => [ADD_NAME, STATS_PARSED],
                Some(PARTITION_VALUES_PARSED) => [ADD_NAME, PARTITION_VALUES_PARSED],
                _ => return Cow::Borrowed(name),
            };
            Cow::Owned(ColumnName::new(
                replacement_root
                    .into_iter()
                    .map(str::to_string)
                    .chain(path.iter().skip(1).cloned()),
            ))
        }
    }

    let PhysicalPredicate::Some(pred, _) = &state.physical_predicate else {
        return None;
    };
    let partition_column_names = state
        .physical_partition_schema
        .iter()
        .flat_map(|s| s.fields().map(|f| ColumnName::new([f.name()])))
        .collect();
    let skipping = as_sql_data_skipping_predicate_with_stats_columns(
        pred,
        &partition_column_names,
        &state.eligible_physical_stats_columns,
    )?;
    // A null skipping verdict means the available metadata cannot prove the file is skippable.
    let skipping = Predicate::distinct(skipping, lit(false));
    let mut prefixer = MetadataSkippingColumnPrefixer;
    Some(prefixer.transform_pred(&skipping).into_owned())
}

#[cfg(test)]
#[path = "scan_plan/tests.rs"]
mod execution_tests;

#[cfg(test)]
mod tests {
    use ::test_utils::record_batch_to_bytes;
    use serde_json::json;
    use url::Url;

    use super::execution_tests::{declarative_metadata, metadata_row_count};
    use super::*;
    use crate::actions::LOG_ADD_SCHEMA;
    use crate::arrow::array::{AsArray as _, StringArray, StructArray};
    use crate::checkpoint::CheckpointType;
    use crate::checkpoint::collation_stats::source_collation_stats;
    use crate::crc::Crc;
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::arrow_utils::parse_json_impl;
    use crate::engine::sync::SyncEngine;
    use crate::log_segment::LogSegment;
    use crate::log_segment_files::LogSegmentFiles;
    use crate::object_store::ObjectStoreExt as _;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::plans::Operation as PlanOperation;
    use crate::plans::ir::nodes::{FileType, Operator};
    use crate::scan::data_skipping::stats_schema::expected_stats_schema;
    use crate::scan::{PartitionValuesOptions, StatsOptions};
    use crate::schema::StructType;
    use crate::snapshot::Snapshot;
    use crate::table_features::TableFeature;
    use crate::unit_test_utils::{
        MockProtocolBuilder, MockTableConfigurationBuilder, create_log_path,
    };
    use crate::{Engine as _, KernelResult};

    fn mock_snapshot(log_segment: LogSegment) -> KernelResult<Arc<Snapshot>> {
        let table_configuration = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema())
            .with_partition_columns(["p"])
            .with_protocol(MockProtocolBuilder::new().with_versions(2, 5).build())
            .with_table_root("memory:///")
            .try_build()?;
        Ok(Arc::new(Snapshot::new(log_segment, table_configuration)?))
    }

    fn partitioned_schema() -> SchemaRef {
        schema_ref! {
            nullable "x": LONG,
            nullable "p": STRING,
        }
    }

    fn log_root() -> Url {
        Url::parse("file:///_delta_log/").unwrap()
    }

    pub(super) fn log_segment(
        log_root: Url,
        commits: &[&str],
        checkpoint: Option<&str>,
    ) -> LogSegment {
        let ascending_commit_files: Vec<_> =
            commits.iter().map(|path| create_log_path(path)).collect();
        let checkpoint_parts: Vec<_> = checkpoint.into_iter().map(create_log_path).collect();
        let checkpoint_version = checkpoint_parts.first().map(|path| path.version);
        let latest_commit_file = ascending_commit_files.last().cloned();
        let end_version = latest_commit_file
            .as_ref()
            .map(|path| path.version)
            .or(checkpoint_version)
            .unwrap_or_default();
        LogSegment {
            end_version,
            checkpoint_version,
            log_root,
            listed: LogSegmentFiles {
                ascending_commit_files,
                checkpoint_parts,
                latest_commit_file,
                max_published_version: Some(end_version),
                ..Default::default()
            },
            last_checkpoint_metadata: None,
            checkpoint_kind: Default::default(),
            checkpoint_kind_hint: Default::default(),
        }
    }

    fn checkpoint_path(file_type: FileType) -> &'static str {
        match file_type {
            FileType::Json => concat!(
                "file:///_delta_log/00000000000000000000.checkpoint.",
                "11111111-1111-1111-1111-111111111111.json"
            ),
            FileType::Parquet => "file:///_delta_log/00000000000000000000.checkpoint.parquet",
        }
    }

    fn shape(checkpoint_type: CheckpointType, parsed_stats: Option<SchemaRef>) -> CheckpointShape {
        let leaf_checkpoint_schema = parsed_stats
            .as_ref()
            .map(|stats| parquet_read_schema(Some(stats), None).unwrap());
        CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema,
        }
    }

    fn no_checkpoint() -> CheckpointShape {
        shape(CheckpointType::None, None)
    }

    fn tags(plan: &Plan) -> Vec<String> {
        plan.nodes.iter().map(|node| node.op.to_string()).collect()
    }

    // Inspect the leaf read schema to verify collation bounds survive checkpoint projection.
    fn leaf_read_schema<'plan>(plan: &'plan Plan, shape: &CheckpointShape) -> &'plan SchemaRef {
        plan.nodes
            .iter()
            .find_map(|node| match &node.op {
                Operator::ScanParquet(scan) if shape.checkpoint_type == CheckpointType::Leaf => {
                    Some(&scan.schema)
                }
                Operator::DynamicScan(scan) => {
                    assert!(scan.dv_column.is_none(), "sidecar scan sets no dv column");
                    Some(&scan.schema)
                }
                _ => None,
            })
            .expect("checkpoint leaf scan")
    }

    fn add_struct(schema: &SchemaRef) -> &StructType {
        let DataType::Struct(add_struct) = schema
            .field(ADD_NAME)
            .expect("schema should contain add")
            .data_type()
        else {
            panic!("add should be a struct");
        };
        add_struct
    }

    // One add with JSON stats and no `stats_parsed`.
    fn write_parquet_checkpoint(store: &Arc<InMemory>, path: &str) -> KernelResult<()> {
        use crate::arrow::array::builder::{MapBuilder, MapFieldNames, StringBuilder};
        use crate::arrow::array::{
            Array, BooleanArray, Int64Array, RecordBatch, StringArray as SA,
        };
        use crate::arrow::datatypes::{DataType as ADT, Field, Fields, Schema};
        use crate::parquet::arrow::arrow_writer::ArrowWriter;

        // An empty (non-null) `partitionValues` map for the single row; the canonical add schema
        // requires the field, so the checkpoint file must physically carry it. The inner field
        // names must match kernel's map convention (`key_value` / `key` / `value`).
        let map_names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut map = MapBuilder::new(Some(map_names), StringBuilder::new(), StringBuilder::new());
        map.append(true).unwrap();
        let partition_values = map.finish();

        // The reader null-fills missing *nullable* add fields, but the canonical add schema's
        // non-null scalars (`path`, `size`, `modificationTime`, `dataChange`) and `partitionValues`
        // must be present in the file. `stats` carries the JSON string parsed in the
        // no-parsed-stats path; there is deliberately no `stats_parsed` column.
        let add_fields = Fields::from(vec![
            Field::new("path", ADT::Utf8, true),
            Field::new("stats", ADT::Utf8, true),
            Field::new(
                "partitionValues",
                partition_values.data_type().clone(),
                true,
            ),
            Field::new("size", ADT::Int64, true),
            Field::new("modificationTime", ADT::Int64, true),
            Field::new("dataChange", ADT::Boolean, true),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(ADD_NAME, ADT::Struct(add_fields.clone()), true),
            Field::new(VERSION, ADT::Int64, true),
        ]));
        let add = StructArray::new(
            add_fields,
            vec![
                Arc::new(SA::from(vec!["c.parquet"])),
                Arc::new(SA::from(vec![
                    r#"{"numRecords":1,"minValues":{"x":10},"maxValues":{"x":10}}"#,
                ])),
                Arc::new(partition_values),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(add), Arc::new(Int64Array::from(vec![0i64]))],
        )?;

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        futures::executor::block_on(store.put(&Path::from(path), buf.into()))?;
        Ok(())
    }

    fn struct_stats_schema() -> SchemaRef {
        let segment = log_segment(log_root(), &[], None);
        mock_snapshot(segment)
            .unwrap()
            .scan_builder()
            .with_predicate(Arc::new(Predicate::from_expr(expr!(col("x") > 5i64))))
            .with_stats(StatsOptions::all())
            .build()
            .unwrap()
            .state_info
            .physical_stats_read_schema()
            .cloned()
            .expect("stats schema")
    }

    // Commit-arm operator sequence without optional pruning or metadata transformations.
    const COMMIT_ARM_TAGS: &[&str] = &[
        "scan_json", // commits
        "filter",    // keep file actions
        "project",   // add replay columns
        "aggregate", // newest-action-per-key
        "filter",    // live commit adds
        "project",   // drop action key
    ];

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "semi_join", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "semi_join", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "semi_join", "project"])]
    #[case::checkpoint_without_add(
        CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: Some(schema_ref! {}),
        },
        FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "semi_join", "project"]
    )]
    fn metadata_plan_checkpoint_arm_shape(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> KernelResult<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            Some(checkpoint_path(file_type)),
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&shape, &MetadataPlanner::try_from_scan(&scan)?)?
            .expect("non-empty");

        let mut expected: Vec<&str> = COMMIT_ARM_TAGS.to_vec();
        expected.extend(checkpoint_arm_tags);
        expected.extend(["union_all", "project"]); // drop version
        assert_eq!(tags(&plan), expected);
        Ok(())
    }

    #[rstest::rstest]
    #[case::without_parsed_partitions(None, false)]
    #[case::compatible_partitions(Some(schema_ref! { nullable "p": STRING }), true)]
    // The table's partition column p is STRING, so native LONG values are incompatible.
    #[case::incompatible_partitions(Some(schema_ref! { nullable "p": LONG }), false)]
    fn metadata_plan_checkpoint_metadata_columns(
        #[case] parsed_partitions: Option<SchemaRef>,
        #[case] expect_native_partitions: bool,
        #[values(None, Some(struct_stats_schema()))] parsed_stats: Option<SchemaRef>,
        #[values(CheckpointType::Leaf, CheckpointType::Manifest)] checkpoint_type: CheckpointType,
        #[values(false, true)] include_tombstones: bool,
    ) -> KernelResult<()> {
        let stats = StatsOptions::all_struct();
        let partition_values = PartitionValuesOptions::with_struct();
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(Predicate::from_expr(expr!(col("x") > 5i64))))
            .with_partition_values(partition_values)
            .build()?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: Some(parquet_read_schema(
                parsed_stats.as_ref(),
                parsed_partitions.as_ref(),
            )?),
        };
        let mut planner = MetadataPlanner::try_from_scan(&scan)?;
        planner.actions.include_tombstones = include_tombstones;
        let plan = scan
            .build_metadata_scan_plan(&shape, &planner)?
            .expect("non-empty");

        let checkpoint_schema = leaf_read_schema(&plan, &shape);
        assert_eq!(checkpoint_schema.contains(REMOVE_NAME), include_tombstones);
        assert_eq!(
            add_struct(checkpoint_schema).field(STATS_PARSED).is_some(),
            parsed_stats.is_some(),
        );
        assert_eq!(
            add_struct(checkpoint_schema)
                .field(PARTITION_VALUES_PARSED)
                .is_some(),
            expect_native_partitions,
        );

        let normalization = plan
            .nodes
            .iter()
            .rev()
            .find_map(|node| match &node.op {
                Operator::Project(project) if project.schema.field(IS_ADD).is_some() => {
                    Some(project.expr.to_string())
                }
                _ => None,
            })
            .expect("checkpoint normalization project");
        assert_eq!(
            normalization.contains("MAP_TO_STRUCT"),
            !expect_native_partitions
        );
        assert!(!normalization.contains("COALESCE"));
        Ok(())
    }

    // On a struct-only checkpoint, only JSON requests synthesize stats and read collation bounds.
    #[rstest::rstest]
    #[case::json(StatsOptions::json_only(), true)]
    #[case::both(StatsOptions::all(), true)]
    #[case::struct_only(StatsOptions::all_struct(), false)]
    #[case::none(StatsOptions::none(), false)]
    fn metadata_plan_reads_full_collation_source_only_for_json(
        #[case] stats: StatsOptions,
        #[case] preserve_json: bool,
        #[values(CheckpointType::Leaf, CheckpointType::Manifest)] checkpoint_type: CheckpointType,
    ) -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema())
            .with_partition_columns(["p"])
            .with_protocol(
                MockProtocolBuilder::new()
                    .with_writer_features([TableFeature::Collations])
                    .build(),
            )
            .with_table_root("memory:///")
            .try_build()?;
        let scan = Arc::new(Snapshot::new(segment, config)?)
            .scan_builder()
            .with_stats(stats)
            .build()?;
        let source = Arc::new(SchemaStructPatchBuilder::new()
            .append(StructField::nullable("statsWithCollation", schema! {
                nullable "test.ASCII_CI.1": { nullable "minValues": { nullable "nested": { nullable "leaf": STRING } } },
                nullable "test.ASCII_CI.2": { nullable "maxValues": { nullable "nested": { nullable "leaf": STRING } } },
            }))
            .build(&struct_stats_schema())?);
        let checkpoint_schema = parquet_read_schema(Some(&source), None)?;
        let leaf_checkpoint_schema = SchemaStructPatchBuilder::new()
            .drop_at([ADD_NAME], STATS)
            .build(&checkpoint_schema)?;
        let shape = CheckpointShape {
            checkpoint_type,
            leaf_checkpoint_schema: Some(Arc::new(leaf_checkpoint_schema)),
        };
        let plan = scan
            .build_metadata_scan_plan(&shape, &MetadataPlanner::try_from_scan(&scan)?)?
            .unwrap();
        let read_schema = leaf_read_schema(&plan, &shape);
        assert_eq!(source_collation_stats(read_schema).is_some(), preserve_json);
        assert_eq!(
            plan.nodes.iter().any(|node| match &node.op {
                Operator::Project(project) => project.expr.to_string().contains("TO_JSON"),
                _ => false,
            }),
            preserve_json
        );
        Ok(())
    }

    #[test]
    fn metadata_plan_commits_only() -> KernelResult<()> {
        let segment = log_segment(
            log_root(),
            &["file:///_delta_log/00000000000000000001.json"],
            None,
        );
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&no_checkpoint(), &MetadataPlanner::try_from_scan(&scan)?)?
            .expect("non-empty");
        let mut expected = COMMIT_ARM_TAGS.to_vec();
        expected.push("project"); // drop version
        assert_eq!(tags(&plan), expected);
        Ok(())
    }

    #[rstest::rstest]
    #[case::leaf_parquet(shape(CheckpointType::Leaf, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project", "project"])]
    #[case::leaf_json(shape(CheckpointType::Leaf, None), FileType::Json,
        vec!["scan_json", "filter", "project", "project", "project"])]
    #[case::manifest(shape(CheckpointType::Manifest, None), FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "dynamic_scan", "filter", "project",
            "project", "project"])]
    #[case::checkpoint_without_add(
        CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: Some(schema_ref! {}),
        },
        FileType::Parquet,
        vec!["scan_parquet", "filter", "project", "project", "project"]
    )]
    fn metadata_plan_checkpoint_only(
        #[case] shape: CheckpointShape,
        #[case] file_type: FileType,
        #[case] checkpoint_arm_tags: Vec<&'static str>,
    ) -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(file_type)));
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        let plan = scan
            .build_metadata_scan_plan(&shape, &MetadataPlanner::try_from_scan(&scan)?)?
            .expect("non-empty");
        assert_eq!(tags(&plan), checkpoint_arm_tags);
        Ok(())
    }

    #[test]
    fn metadata_plan_empty_is_none() -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?.scan_builder().build()?;
        assert!(
            scan.build_metadata_scan_plan(
                &no_checkpoint(),
                &MetadataPlanner::try_from_scan(&scan)?
            )?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn metadata_plan_static_skip_all_is_none() -> KernelResult<()> {
        let segment = log_segment(log_root(), &[], None);
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_predicate(Arc::new(Predicate::FALSE))
            .build()?;
        assert_eq!(
            scan.state_info.physical_predicate,
            PhysicalPredicate::StaticSkipAll
        );
        assert!(
            scan.build_metadata_scan_plan(
                &shape(CheckpointType::Leaf, None),
                &MetadataPlanner::try_from_scan(&scan)?,
            )?
            .is_none()
        );
        Ok(())
    }

    #[rstest::rstest]
    #[case::no_dv(None, None, &["b.parquet"])]
    #[case::matching_dv(Some(0), Some(0), &["b.parquet"])]
    #[case::different_dv(Some(0), Some(1), &["a.parquet", "b.parquet"])]
    fn metadata_plan_executes_commit_dedup_with_sync_executor(
        #[case] add_offset: Option<i32>,
        #[case] remove_offset: Option<i32>,
        #[case] expected_paths: &[&str],
        #[values("commits", "crc", "checkpoint")] base: &str,
    ) -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        let dv = |offset: Option<i32>| {
            offset.map(|offset| {
                json!({
                    "storageType": "p", "pathOrInlineDv": "memory:///dv.bin", "offset": offset,
                    "sizeInBytes": 1, "cardinality": 1
                })
            })
        };
        let add = |path, offset| {
            json!({
                "path": path, "size": 1, "modificationTime": 1, "dataChange": true,
                "partitionValues": {}, "deletionVector": dv(offset)
            })
        };
        let adds = [add("a.parquet", add_offset), add("b.parquet", None)];
        let adds_json = adds
            .iter()
            .map(|add| json!({"add": add}).to_string())
            .collect::<Vec<_>>();
        futures::executor::block_on(async {
            store
                .put(
                    &Path::from("_delta_log/00000000000000000000.json"),
                    adds_json.join("\n").into(),
                )
                .await?;
            if base == "checkpoint" {
                let batch = parse_json_impl(&StringArray::from(adds_json), LOG_ADD_SCHEMA.clone())?;
                store
                    .put(
                        &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
                        record_batch_to_bytes(&batch).into(),
                    )
                    .await?;
            }
            store
                .put(
                    &Path::from("_delta_log/00000000000000000001.json"),
                    json!({"remove": {
                        "path": "a.parquet", "deletionTimestamp": 2, "dataChange": true,
                        "deletionVector": dv(remove_offset)
                    }})
                    .to_string()
                    .into(),
                )
                .await?;
            KernelResult::<()>::Ok(())
        })?;

        let commits = [
            "memory:///_delta_log/00000000000000000000.json",
            "memory:///_delta_log/00000000000000000001.json",
        ];
        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            if base == "checkpoint" {
                &commits[1..]
            } else {
                &commits
            },
            (base == "checkpoint")
                .then_some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let engine = SyncEngine::new_with_store(store);
        let features = [TableFeature::DeletionVectors];
        let config = MockTableConfigurationBuilder::new()
            .with_schema(partitioned_schema())
            .with_partition_columns(["p"])
            .with_protocol(MockProtocolBuilder::new().with_features(features).build())
            .with_table_root("memory:///")
            .with_version(1)
            .try_build()?;
        let all_files = serde_json::from_value(json!(adds))?;
        let crc = (base == "crc").then(|| {
            Arc::new(Crc {
                version: 0,
                all_files: Some(all_files),
                ..Default::default()
            })
        });
        let scan = Arc::new(Snapshot::new_with_crc(segment, config, crc, false, false)?)
            .scan_builder()
            .build()?;
        let plan = scan
            .declarative_metadata_scan_plan(&engine)?
            .expect("non-empty");
        assert_eq!(
            plan.nodes
                .iter()
                .any(|node| matches!(node.op, Operator::Values(_))),
            base == "crc"
        );
        let batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let mut actual_paths = vec![];
        for batch in batches {
            let batch = batch?.try_into_record_batch()?;
            let add = batch
                .column_by_name(ADD_NAME)
                .expect("add column")
                .as_struct();
            let paths = add
                .column_by_name("path")
                .expect("add.path")
                .as_string::<i32>();
            actual_paths.extend(paths.iter().map(|path| path.unwrap().to_owned()));
        }
        actual_paths.sort_unstable();
        assert_eq!(actual_paths, expected_paths);
        Ok(())
    }

    #[rstest::rstest]
    #[case::date(DataType::DATE, DataType::INTEGER, "1970-01-02", "1970-01-02")]
    #[case::timestamp(
        DataType::TIMESTAMP,
        DataType::LONG,
        "1970-01-01T00:00:00.000Z",
        "1970-01-01T00:00:00.000001Z"
    )]
    #[case::timestamp_ntz(
        DataType::TIMESTAMP_NTZ,
        DataType::LONG,
        "1970-01-01T00:00:00.000",
        "1970-01-01T00:00:00.000001"
    )]
    fn metadata_plan_synthesizes_json_with_table_stats_types(
        #[case] logical_type: DataType,
        #[case] native_type: DataType,
        #[case] expected: &str,
        #[case] expected_native: &str,
    ) -> KernelResult<()> {
        let features = [TableFeature::TimestampWithoutTimezone];
        let config = MockTableConfigurationBuilder::new()
            .with_schema(schema_ref! { nullable "x": (logical_type) })
            .with_protocol(MockProtocolBuilder::new().with_features(features).build())
            .try_build()?;
        let stats_schema = config.stats_schema_builder().build()?;
        let file_schema = schema_ref! {
            nullable ADD_NAME: {
                ..(ADD_SCHEMA.fields().filter(|field| field.name() != STATS)),
                nullable STATS_PARSED: (expected_stats_schema(
                    &schema! { nullable "x": (native_type) },
                    &Default::default(), None, None,
                )?),
            },
        };
        let stats = |value: serde_json::Value| {
            serde_json::json!({
                "numRecords": 1,
                "minValues": {"x": value},
                "maxValues": {"x": value},
                "nullCount": {"x": 0},
            })
        };
        let store = Arc::new(InMemory::new());
        let engine = SyncEngine::new_with_store(store.clone());
        let add = serde_json::json!({ "add": {
            "path": "a.parquet", "partitionValues": {}, "size": 1,
            "modificationTime": 1, "dataChange": true, "stats_parsed": stats(1.into()),
        }});
        let batch = parse_json_impl(&StringArray::from(vec![add.to_string()]), file_schema)?;
        futures::executor::block_on(store.put(
            &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
            record_batch_to_bytes(&batch).into(),
        ))?;
        let segment = log_segment(log_root(), &[], Some(checkpoint_path(FileType::Parquet)));
        let scan = Arc::new(Snapshot::new(segment, config)?)
            .scan_builder()
            .with_stats(StatsOptions::all())
            .build()?;
        let batches = declarative_metadata(&scan, &engine)?;
        assert_eq!(metadata_row_count(&batches), 1);
        let native = StringArray::from(vec![stats(expected_native.into()).to_string()]);
        let expected_native = StructArray::from(parse_json_impl(&native, stats_schema)?);
        for batch in batches {
            let json = batch.column_by_name(STATS).unwrap().as_string::<i32>();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(json.value(0))?,
                stats(expected.into())
            );
            assert_eq!(
                batch.column_by_name(STATS_PARSED).unwrap().as_struct(),
                &expected_native,
            );
        }
        Ok(())
    }

    #[rstest::rstest]
    fn metadata_plan_native_stats_need_no_projection(
        #[values(1, 100)] width: usize,
    ) -> KernelResult<()> {
        let config = MockTableConfigurationBuilder::new()
            .with_schema(StructType::new_unchecked((0..width).map(|index| {
                StructField::nullable(format!("c{index}"), DataType::LONG)
            })))
            .with_properties([("delta.dataSkippingNumIndexedCols", "-1")])
            .try_build()?;
        let scan = Arc::new(Snapshot::new(log_segment(log_root(), &[], None), config)?)
            .scan_builder()
            .with_stats(StatsOptions::all_struct())
            .build()?;
        let planner = MetadataPlanner::try_from_scan(&scan)?;
        let file_shape = shape(CheckpointType::Leaf, planner.stats_schema.clone());
        let plan = planner
            .with_checkpoint_shape(file_shape)
            .checkpoint_actions()?
            .build()?;
        assert_eq!(tags(&plan), ["values"]);
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(StatsOptions::all_struct(), 5, 1)]
    #[case::prunes_non_matching_file(StatsOptions::all_struct(), 20, 0)]
    #[case::json_ignores_predicate(StatsOptions::json_only(), 20, 1)]
    fn metadata_planner_falls_back_to_json_for_incompatible_stats(
        #[case] stats: StatsOptions,
        #[case] lower_bound: i64,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
        let stats_json =
            r#"{"numRecords":1,"minValues":{"x":10},"maxValues":{"x":10},"nullCount":{"x":0}}"#;
        let file_schema = schema_ref! {
            nullable ADD_NAME: {
                ..(ADD_SCHEMA.fields()),
                nullable STATS_PARSED: { nullable "minValues": { nullable "x": STRING } },
            },
        };
        let add = serde_json::json!({ "add": {
            "path": "a.parquet", "partitionValues": {}, "size": 1,
            "modificationTime": 1, "dataChange": true, "stats": stats_json,
            "stats_parsed": { "minValues": { "x": "incompatible" } },
        }});
        let json = StringArray::from(vec![add.to_string()]);
        let batch = parse_json_impl(&json, file_schema.clone())?;
        let store = Arc::new(InMemory::new());
        futures::executor::block_on(store.put(
            &Path::from("_delta_log/00000000000000000000.checkpoint.parquet"),
            record_batch_to_bytes(&batch).into(),
        ))?;
        let segment = log_segment(
            Url::parse("memory:///_delta_log/")?,
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(Predicate::from_expr(expr!(
                col("x") > lit(lower_bound)
            ))))
            .build()?;
        let file_shape = CheckpointShape {
            checkpoint_type: CheckpointType::Leaf,
            leaf_checkpoint_schema: Some(file_schema),
        };
        let plan = MetadataPlanner::try_from_scan(&scan)?
            .with_checkpoint_shape(file_shape.clone())
            .checkpoint_actions()?
            .build()?;
        assert_eq!(
            add_struct(leaf_read_schema(&plan, &file_shape)),
            &*ADD_SCHEMA
        );
        let batches = declarative_metadata(&scan, &SyncEngine::new_with_store(store))?;
        assert_eq!(metadata_row_count(&batches), expected_rows);
        for batch in batches {
            let paths = batch.column_by_name("path").unwrap().as_string::<i32>();
            assert_eq!(paths.value(0), "a.parquet");
            if let Some(schema) = scan.state_info.physical_stats_output_schema() {
                let json = StringArray::from(vec![stats_json]);
                let expected = StructArray::from(parse_json_impl(&json, schema.clone())?);
                assert_eq!(
                    batch.column_by_name(STATS_PARSED).unwrap().as_struct(),
                    &expected
                );
            } else {
                let json = batch.column_by_name(STATS).unwrap().as_string::<i32>();
                assert_eq!(json.value(0), stats_json);
            }
        }
        Ok(())
    }

    #[rstest::rstest]
    #[case::keeps_matching_file(StatsOptions::all(), 5, 1)]
    #[case::prunes_non_matching_file(StatsOptions::all(), 20, 0)]
    #[case::json_ignores_predicate(StatsOptions::json_only(), 20, 1)]
    fn metadata_plan_executes_leaf_without_stats_parsed(
        #[case] stats: StatsOptions,
        #[case] lower_bound: i64,
        #[case] expected_rows: usize,
    ) -> KernelResult<()> {
        let store = Arc::new(InMemory::new());
        // A single-row parquet checkpoint carrying an `add` with a JSON `stats` string but no
        // `stats_parsed` column.
        write_parquet_checkpoint(&store, "_delta_log/00000000000000000000.checkpoint.parquet")?;

        let segment = log_segment(
            Url::parse("memory:///_delta_log/").unwrap(),
            &[],
            Some("memory:///_delta_log/00000000000000000000.checkpoint.parquet"),
        );
        let scan = mock_snapshot(segment)?
            .scan_builder()
            .with_stats(stats)
            .with_predicate(Arc::new(Predicate::from_expr(expr!(
                col("x") > lit(lower_bound)
            ))))
            .build()?;
        let plan = scan
            .build_metadata_scan_plan(
                &shape(CheckpointType::Leaf, None),
                &MetadataPlanner::try_from_scan(&scan)?,
            )?
            .expect("non-empty");

        let engine = SyncEngine::new_with_store(store);
        let mut batches = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?;
        let actual_rows = batches.try_fold(0, |rows, batch| {
            Ok::<_, crate::KernelError>(rows + batch?.try_into_record_batch()?.num_rows())
        })?;
        assert_eq!(actual_rows, expected_rows);
        Ok(())
    }
}
