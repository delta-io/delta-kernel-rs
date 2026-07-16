use std::sync::Arc;

use ::test_utils::table_builder::{
    checkpoint_json_stats, checkpoint_struct_stats, no_checkpoint_stats, DataLayoutConfig,
    FeatureSet, LogState, TableConfig, TestTableBuilder,
};
use rstest::rstest;

use super::*;
use crate::arrow::array::{Array, ArrayRef, BooleanArray, StringArray, StructArray};
use crate::arrow::compute::filter_record_batch;
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty::pretty_format_batches;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::engine::test_delegating::DelegatingEngine;
use crate::expressions::{col, column_name, lit, Predicate as Pred};
use crate::plans::ir::nodes::Operator;
use crate::plans::Operation as PlanOperation;
use crate::scan::{PartitionValuesOptions, Scan, StatsOptions};
use crate::unit_test_utils::load_test_table;
use crate::{DeltaResult, Engine, PredicateRef, Snapshot};

// Normalizes metadata for comparison: the imperative path splits fields between the data batch
// and fileConstantValues, while the declarative path returns them in an add struct.
fn normalized_metadata_batch(
    field: impl Fn(&str) -> ArrayRef,
    json_stats: Option<ArrayRef>,
    stats_parsed: Option<ArrayRef>,
    partitions_parsed: Option<ArrayRef>,
) -> DeltaResult<RecordBatch> {
    let mut columns = vec![
        ("path", field("path")),
        ("size", field("size")),
        ("modificationTime", field("modificationTime")),
    ];
    if let Some(stats) = json_stats {
        columns.push(("stats", stats));
    }
    columns.extend([
        ("partitionValues", field("partitionValues")),
        ("deletionVector", field("deletionVector")),
        ("baseRowId", field("baseRowId")),
        ("defaultRowCommitVersion", field("defaultRowCommitVersion")),
        ("tags", field("tags")),
        ("clusteringProvider", field("clusteringProvider")),
    ]);
    if let Some(stats) = stats_parsed {
        columns.push(("stats_parsed", stats));
    }
    if let Some(partitions) = partitions_parsed {
        columns.push(("partitionValues_parsed", partitions));
    }
    Ok(RecordBatch::try_from_iter(columns)?)
}

fn imperative_metadata(scan: Scan, engine: &dyn Engine) -> DeltaResult<Vec<RecordBatch>> {
    let mut batches = vec![];
    for metadata in scan.scan_metadata(engine)? {
        let (data, selection) = metadata?.scan_files.into_parts();
        let batch = filter_record_batch(
            &data.try_into_record_batch()?,
            &BooleanArray::from(selection),
        )?;
        if batch.num_rows() == 0 {
            continue;
        }
        let constants = batch
            .column_by_name("fileConstantValues")
            .expect("file constants")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("file constants struct");
        batches.push(normalized_metadata_batch(
            |name| {
                batch
                    .column_by_name(name)
                    .or_else(|| constants.column_by_name(name))
                    .unwrap_or_else(|| panic!("metadata field {name}"))
                    .clone()
            },
            batch
                .column_by_name(STATS)
                .or_else(|| constants.column_by_name(STATS))
                .cloned(),
            batch.column_by_name("stats_parsed").cloned(),
            batch.column_by_name("partitionValues_parsed").cloned(),
        )?);
    }
    Ok(batches)
}

fn declarative_metadata(scan: &Scan, engine: &dyn Engine) -> DeltaResult<Vec<RecordBatch>> {
    let Some(plan) = scan.declarative_metadata_scan_plan(engine)? else {
        return Ok(vec![]);
    };
    let batches = engine
        .plan_executor()
        .unwrap()
        .execute_op(PlanOperation::QueryPlan(plan))?
        .into_data()?;

    let mut projected = vec![];
    for batch in batches {
        let batch = batch?.try_into_record_batch()?;
        if batch.num_rows() == 0 {
            continue;
        }
        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        projected.push(normalized_metadata_batch(
            |name| {
                add.column_by_name(name)
                    .unwrap_or_else(|| panic!("add.{name}"))
                    .clone()
            },
            add.column_by_name(STATS).cloned(),
            add.column_by_name(STATS_PARSED).cloned(),
            add.column_by_name(PARTITION_VALUES_PARSED).cloned(),
        )?);
    }
    Ok(projected)
}

fn metadata_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn assert_metadata_eq(
    actual: &[RecordBatch],
    expected: &[RecordBatch],
    context: &str,
) -> DeltaResult<()> {
    fn sorted_pretty_lines(batches: &[RecordBatch]) -> DeltaResult<Vec<String>> {
        let formatted = pretty_format_batches(batches)?.to_string();
        let mut lines: Vec<_> = formatted.lines().map(str::to_string).collect();
        let len = lines.len();
        if len > 3 {
            lines[2..len - 1].sort_unstable();
        }
        Ok(lines)
    }

    if let (Some(actual), Some(expected)) = (actual.first(), expected.first()) {
        assert_eq!(actual.schema(), expected.schema(), "{context}");
    }
    let actual = sorted_pretty_lines(actual)?;
    let expected = sorted_pretty_lines(expected)?;
    assert_eq!(actual, expected, "{context}");
    Ok(())
}

fn without_columns(batches: &[RecordBatch], excluded: &[&str]) -> DeltaResult<Vec<RecordBatch>> {
    batches
        .iter()
        .map(|batch| {
            RecordBatch::try_from_iter(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .filter(|(field, _)| !excluded.contains(&field.name().as_str()))
                    .map(|(field, column)| (field.name(), column.clone())),
            )
            .map_err(Into::into)
        })
        .collect()
}

fn leaf_paths(batches: &[RecordBatch]) -> Vec<String> {
    fn append(field: &crate::arrow::datatypes::Field, prefix: &str, paths: &mut Vec<String>) {
        let path = if prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{prefix}.{}", field.name())
        };
        if let ArrowDataType::Struct(fields) = field.data_type() {
            for field in fields {
                append(field, &path, paths);
            }
        } else {
            paths.push(path);
        }
    }

    let mut paths = vec![];
    let schema = batches.first().expect("metadata output").schema();
    for field in schema.fields() {
        append(field, "", &mut paths);
    }
    paths.sort_unstable();
    paths
}

#[rstest]
#[case::v2_parquet_manifest("v2-checkpoints-parquet-with-sidecars")]
#[case::v2_json_manifest("v2-checkpoints-json-with-sidecars")]
#[case::v2_parquet_leaf("v2-checkpoints-parquet-without-sidecars")]
#[case::v2_json_leaf("v2-checkpoints-json-without-sidecars")]
#[case::v1_single_part_struct_stats("v1-single-part-struct-stats-only")]
#[case::v1_multi_part_struct_stats("v1-multi-part-struct-stats-only")]
#[case::v1_multi_part_partitioned_struct_stats("v1-multi-part-partitioned-struct-stats-only")]
fn declarative_metadata_matches_imperative_scan(
    #[case] table: &str,
    #[values(
        None,
        Some(col!("id").gt(lit(3i64))),
        Some(col!("id").eq(lit(2i64))),
        Some(col!("id").le(lit(0i64))),
        Some(col!("id").is_not_null())
    )]
    predicate: Option<Pred>,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = crate::unit_test_utils::load_test_table(table)?;
    let predicate = predicate.map(Arc::new);

    let imperative_builder = snapshot
        .clone()
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct());
    let imperative_builder = match &predicate {
        Some(predicate) => imperative_builder.with_predicate(predicate.clone()),
        None => imperative_builder,
    };
    let expected = imperative_metadata(imperative_builder.build()?, engine.as_ref())?;

    let declarative_builder = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct());
    let declarative_builder = match predicate {
        Some(predicate) => declarative_builder.with_predicate(predicate),
        None => declarative_builder,
    };
    let scan = declarative_builder.build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;

    if table.contains("struct-stats-only") {
        assert_metadata_eq(
            &without_columns(&actual, &[STATS])?,
            &without_columns(&expected, &[STATS])?,
            &format!("table {table}"),
        )
    } else {
        assert_metadata_eq(&actual, &expected, &format!("table {table}"))
    }
}

#[rstest]
#[case::parquet_manifest("v2-checkpoints-parquet-with-sidecars")]
#[case::json_manifest("v2-checkpoints-json-with-sidecars")]
fn declarative_metadata_scans_sidecars_from_checkpoint_hint(
    #[case] table: &str,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = crate::unit_test_utils::load_test_table(table)?;
    let plan = snapshot
        .scan_builder()
        .build()?
        .declarative_metadata_scan_plan(engine.as_ref())?
        .expect("metadata plan");

    assert!(plan
        .nodes
        .iter()
        .all(|node| !matches!(&node.op, Operator::DynamicScan(_))));
    assert!(plan.nodes.iter().any(|node| {
        let Operator::ScanParquet(scan) = &node.op else {
            return false;
        };
        scan.files
            .iter()
            .any(|file| file.meta.location.path().contains("/_sidecars/"))
    }));
    Ok(())
}

#[rstest]
#[case::json_only(StatsOptions::json_only(), &[JSON_STATS_FIELDS])]
#[case::all_struct(StatsOptions::all_struct(), &[PARSED_STATS_TABLE_ALL_STATS_FIELDS])]
#[case::struct_columns(
    StatsOptions::struct_columns(vec![column_name!("id")]),
    &[ID_STATS_PARSED_FIELDS]
)]
#[case::empty_struct_columns(StatsOptions::struct_columns(vec![]), &[])]
#[case::all(
    StatsOptions::all(),
    &[PARSED_STATS_TABLE_ALL_STATS_FIELDS, JSON_STATS_FIELDS]
)]
#[case::none(StatsOptions::none(), &[])]
fn declarative_metadata_matches_imperative_across_stats_options(
    #[case] stats: StatsOptions,
    #[case] expected_stats_field_groups: &[&[&str]],
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = load_test_table("parsed-stats")?;
    let predicate: PredicateRef = col!("id").gt(lit(0i64)).into();
    let expected_builder = snapshot
        .clone()
        .scan_builder()
        .with_stats(stats.clone())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .with_predicate(predicate.clone());
    let expected = imperative_metadata(expected_builder.build()?, engine.as_ref())?;
    let builder = snapshot
        .scan_builder()
        .with_stats(stats.clone())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .with_predicate(predicate);
    let scan = builder.build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;
    let actual_fields = leaf_paths(&actual);
    let imperative_fields = leaf_paths(&expected);
    let unexpected_fields: Vec<_> = actual_fields
        .iter()
        .filter(|field| !imperative_fields.contains(field))
        .collect();
    assert!(
        unexpected_fields.is_empty(),
        "declarative metadata fields missing from imperative output: {unexpected_fields:?}"
    );
    let actual_stats_fields: Vec<_> = actual_fields
        .into_iter()
        .filter(|field| field == STATS || field.starts_with("stats_parsed."))
        .collect();
    let mut expected_stats_fields: Vec<_> = expected_stats_field_groups
        .iter()
        .flat_map(|fields| fields.iter())
        .map(|field| {
            field
                .strip_prefix("add.")
                .expect("expected add field")
                .to_string()
        })
        .collect();
    expected_stats_fields.sort_unstable();
    assert_eq!(actual_stats_fields, expected_stats_fields);
    let ignored_stats = if stats.emit_json {
        &[][..]
    } else {
        // The imperative scan row schema keeps a null JSON stats placeholder.
        &[STATS][..]
    };
    assert_metadata_eq(
        &actual,
        &without_columns(&expected, ignored_stats)?,
        "metadata output options",
    )
}

const ADD_FIELDS: &[&str] = &[
    "add.path",
    "add.size",
    "add.modificationTime",
    "add.dataChange",
    "add.partitionValues",
    "add.deletionVector.storageType",
    "add.deletionVector.pathOrInlineDv",
    "add.deletionVector.offset",
    "add.deletionVector.sizeInBytes",
    "add.deletionVector.cardinality",
    "add.baseRowId",
    "add.defaultRowCommitVersion",
    "add.tags",
    "add.clusteringProvider",
];
const ALL_STATS_PARSED_FIELDS: &[&str] = &[
    "add.stats_parsed.numRecords",
    "add.stats_parsed.nullCount.id",
    "add.stats_parsed.nullCount.value",
    "add.stats_parsed.minValues.id",
    "add.stats_parsed.minValues.value",
    "add.stats_parsed.maxValues.id",
    "add.stats_parsed.maxValues.value",
    "add.stats_parsed.tightBounds",
];
const ID_STATS_PARSED_FIELDS: &[&str] = &[
    "add.stats_parsed.numRecords",
    "add.stats_parsed.nullCount.id",
    "add.stats_parsed.minValues.id",
    "add.stats_parsed.maxValues.id",
    "add.stats_parsed.tightBounds",
];
const PARSED_STATS_TABLE_ALL_STATS_FIELDS: &[&str] = &[
    "add.stats_parsed.numRecords",
    "add.stats_parsed.nullCount.id",
    "add.stats_parsed.nullCount.name",
    "add.stats_parsed.nullCount.age",
    "add.stats_parsed.nullCount.salary",
    "add.stats_parsed.nullCount.ts_col",
    "add.stats_parsed.minValues.id",
    "add.stats_parsed.minValues.name",
    "add.stats_parsed.minValues.age",
    "add.stats_parsed.minValues.salary",
    "add.stats_parsed.minValues.ts_col",
    "add.stats_parsed.maxValues.id",
    "add.stats_parsed.maxValues.name",
    "add.stats_parsed.maxValues.age",
    "add.stats_parsed.maxValues.salary",
    "add.stats_parsed.maxValues.ts_col",
    "add.stats_parsed.tightBounds",
];
const JSON_STATS_FIELDS: &[&str] = &["add.stats"];
const PARTITION_PARSED_FIELDS: &[&str] = &["add.partitionValues_parsed.part"];

#[rstest]
#[should_panic(expected = "requested JSON stats must be populated")]
#[case::json_only_string_map(
    StatsOptions::json_only(),
    PartitionValuesOptions::string_map_only(),
    &[ADD_FIELDS, JSON_STATS_FIELDS]
)]
#[should_panic(expected = "requested JSON stats must be populated")]
#[case::json_only_with_struct(
    StatsOptions::json_only(),
    PartitionValuesOptions::with_struct(),
    &[ADD_FIELDS, JSON_STATS_FIELDS, PARTITION_PARSED_FIELDS]
)]
#[case::all_struct_string_map(
    StatsOptions::all_struct(),
    PartitionValuesOptions::string_map_only(),
    &[ADD_FIELDS, ALL_STATS_PARSED_FIELDS]
)]
#[case::all_struct_with_struct(
    StatsOptions::all_struct(),
    PartitionValuesOptions::with_struct(),
    &[ADD_FIELDS, ALL_STATS_PARSED_FIELDS, PARTITION_PARSED_FIELDS]
)]
#[case::struct_columns_string_map(
    StatsOptions::struct_columns(vec![column_name!("id")]),
    PartitionValuesOptions::string_map_only(),
    &[ADD_FIELDS, ID_STATS_PARSED_FIELDS]
)]
#[case::struct_columns_with_struct(
    StatsOptions::struct_columns(vec![column_name!("id")]),
    PartitionValuesOptions::with_struct(),
    &[ADD_FIELDS, ID_STATS_PARSED_FIELDS, PARTITION_PARSED_FIELDS]
)]
#[case::empty_struct_columns_string_map(
    StatsOptions::struct_columns(vec![]),
    PartitionValuesOptions::string_map_only(),
    &[ADD_FIELDS]
)]
#[case::empty_struct_columns_with_struct(
    StatsOptions::struct_columns(vec![]),
    PartitionValuesOptions::with_struct(),
    &[ADD_FIELDS, PARTITION_PARSED_FIELDS]
)]
#[should_panic(expected = "requested JSON stats must be populated")]
#[case::all_string_map(
    StatsOptions::all(),
    PartitionValuesOptions::string_map_only(),
    &[ADD_FIELDS, ALL_STATS_PARSED_FIELDS, JSON_STATS_FIELDS]
)]
#[should_panic(expected = "requested JSON stats must be populated")]
#[case::all_with_struct(
    StatsOptions::all(),
    PartitionValuesOptions::with_struct(),
    &[
        ADD_FIELDS,
        ALL_STATS_PARSED_FIELDS,
        JSON_STATS_FIELDS,
        PARTITION_PARSED_FIELDS,
    ]
)]
#[case::none_string_map(
    StatsOptions::none(),
    PartitionValuesOptions::string_map_only(),
    &[ADD_FIELDS]
)]
#[case::none_with_struct(
    StatsOptions::none(),
    PartitionValuesOptions::with_struct(),
    &[ADD_FIELDS, PARTITION_PARSED_FIELDS]
)]
fn declarative_metadata_has_exact_leaf_schema_across_output_options(
    #[case] stats: StatsOptions,
    #[case] partition_values: PartitionValuesOptions,
    #[case] expected_field_groups: &[&[&str]],
) {
    (|| -> DeltaResult<()> {
        let json_requested = stats.emit_json;
        let (engine, snapshot, _tempdir) =
            load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
        let scan = snapshot
            .scan_builder()
            .with_stats(stats)
            .with_partition_values(partition_values)
            .build()?;
        let plan = scan
            .declarative_metadata_scan_plan(engine.as_ref())?
            .expect("metadata plan");
        let actual = engine
            .plan_executor()
            .unwrap()
            .execute_op(PlanOperation::QueryPlan(plan))?
            .into_data()?
            .map(|batch| batch?.try_into_record_batch())
            .collect::<DeltaResult<Vec<_>>>()?;

        if json_requested {
            for batch in &actual {
                let add = batch
                    .column_by_name(ADD_NAME)
                    .expect("add column")
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("add struct");
                let stats = add
                    .column_by_name(STATS)
                    .expect("requested JSON stats")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("JSON stats");
                assert_eq!(
                    stats.null_count(),
                    0,
                    "requested JSON stats must be populated"
                );
            }
        }

        let mut expected: Vec<_> = expected_field_groups
            .iter()
            .flat_map(|fields| fields.iter())
            .map(|field| field.to_string())
            .collect();
        expected.sort_unstable();
        assert_eq!(leaf_paths(&actual), expected);
        Ok(())
    })()
    .unwrap()
}

#[test]
fn declarative_metadata_projects_nested_column_mapped_stats() -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = load_test_table("stats-writing-all-types/delta")?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::struct_columns(vec![column_name!(
            "nested_struct.inner_int"
        )]))
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;
    let parent = "col-481c7590-d3b8-4e9c-b40e-7b7128a972f4";
    let child = "col-7f2f94cf-7082-430c-bba7-852bc6c5215e";
    let stats_paths: Vec<_> = leaf_paths(&actual)
        .into_iter()
        .filter(|path| path.starts_with(STATS_PARSED))
        .collect();
    assert_eq!(
        stats_paths,
        [
            format!("stats_parsed.maxValues.{parent}.{child}"),
            format!("stats_parsed.minValues.{parent}.{child}"),
            format!("stats_parsed.nullCount.{parent}.{child}"),
            "stats_parsed.numRecords".to_string(),
            "stats_parsed.tightBounds".to_string(),
        ]
    );
    Ok(())
}

#[rstest]
#[case::json_commits(
    LogState::with_latest_version(2),
    FeatureSet::new(),
    checkpoint_json_stats(),
    StatsOptions::all()
)]
#[case::v1_checkpoint_json(
    LogState::with_latest_version(2).with_checkpoint_at([2]),
    FeatureSet::new(),
    checkpoint_json_stats(),
    StatsOptions::all()
)]
#[case::v1_checkpoint_struct(
    LogState::with_latest_version(2).with_checkpoint_at([2]),
    FeatureSet::new(),
    checkpoint_struct_stats(),
    StatsOptions::all_struct()
)]
#[case::v2_checkpoint_json(
    LogState::with_latest_version(2)
        .with_checkpoint_at([2])
        .with_sidecars_if_enabled(None),
    FeatureSet::new().v2_checkpoint(),
    checkpoint_json_stats(),
    StatsOptions::all()
)]
#[case::v2_mixed_struct(
    LogState::with_latest_version(2)
        .with_checkpoint_at([1])
        .with_sidecars_if_enabled(None),
    FeatureSet::new().v2_checkpoint(),
    checkpoint_struct_stats(),
    StatsOptions::all_struct()
)]
#[case::v2_checkpoint_without_stats(
    LogState::with_latest_version(2)
        .with_checkpoint_at([2])
        .with_sidecars_if_enabled(None),
    FeatureSet::new().v2_checkpoint(),
    no_checkpoint_stats(),
    StatsOptions::all_struct()
)]
fn declarative_metadata_output_options_across_log_shapes(
    #[case] log_state: LogState,
    #[case] features: FeatureSet,
    #[case] table_config: TableConfig,
    #[case] stats: StatsOptions,
) -> DeltaResult<()> {
    assert_metadata_output_options(log_state, features, table_config, stats)
}

#[rstest]
#[case::v1(
    LogState::with_latest_version(2).with_checkpoint_at([2]),
    FeatureSet::new()
)]
#[case::v2(
    LogState::with_latest_version(2)
        .with_checkpoint_at([2])
        .with_sidecars_if_enabled(None),
    FeatureSet::new().v2_checkpoint()
)]
// TODO: https://github.com/delta-io/delta-kernel-rs/issues/3040
#[should_panic(expected = "requested JSON stats must be populated")]
fn declarative_metadata_synthesizes_json_for_struct_only_checkpoints(
    #[case] log_state: LogState,
    #[case] features: FeatureSet,
    #[values(StatsOptions::json_only(), StatsOptions::all())] stats: StatsOptions,
) {
    assert_metadata_output_options(log_state, features, checkpoint_struct_stats(), stats).unwrap();
}

fn assert_metadata_output_options(
    log_state: LogState,
    features: FeatureSet,
    table_config: TableConfig,
    stats: StatsOptions,
) -> DeltaResult<()> {
    let json_requested = stats.emit_json;
    let table = TestTableBuilder::new()
        .with_log_state(log_state)
        .with_features(features)
        .with_table_config(table_config)
        .with_data_layout(DataLayoutConfig::PartitionedAllTypes)
        .build()
        .expect("build output-options table");
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_stats(stats.clone())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        &engine,
    )?;
    let scan = snapshot
        .scan_builder()
        .with_stats(stats)
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, &engine)?;

    if json_requested {
        for batch in &actual {
            let stats = batch.column_by_name(STATS).expect("requested JSON stats");
            let stats = stats
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("JSON stats");
            assert_eq!(
                stats.null_count(),
                0,
                "requested JSON stats must be populated"
            );
        }
    }

    if json_requested {
        assert_metadata_eq(
            &actual,
            &expected,
            "metadata output options across log shapes",
        )?;
    } else {
        // Imperative metadata exposes source JSON even when it was not requested.
        assert_metadata_eq(
            &actual,
            &without_columns(&expected, &[STATS])?,
            "metadata output options across log shapes",
        )?;
    }
    Ok(())
}

#[rstest]
#[case::gt_three(col!("id").gt(lit(3i64)), 2)]
#[case::eq_two(col!("id").eq(lit(2i64)), 1)]
#[case::le_zero(col!("id").le(lit(0i64)), 0)]
fn declarative_metadata_data_skipping(
    #[values(
        "v1-multi-part-struct-stats-only",
        "v2-parquet-sidecars-struct-stats-only",
        "v2-json-sidecars-struct-stats-only"
    )]
    table: &str,
    #[case] predicate: Pred,
    #[case] expected_count: usize,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = crate::unit_test_utils::load_test_table(table)?;
    let predicate = Arc::new(predicate);
    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        engine.as_ref(),
    )?;
    assert_eq!(metadata_row_count(&expected), expected_count);

    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;

    assert_metadata_eq(
        &without_columns(&actual, &[STATS])?,
        &without_columns(&expected, &[STATS])?,
        &format!("table {table}"),
    )
}

#[rstest]
#[case::part_zero(col!("part").eq(lit(0i32)), 1)]
#[case::part_one(col!("part").eq(lit(1i32)), 2)]
#[case::missing_part(col!("part").eq(lit(4i32)), 0)]
fn declarative_metadata_reconstructs_partition_values_for_pruning(
    #[case] predicate: Pred,
    #[case] expected_count: usize,
) -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) =
        crate::unit_test_utils::load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
    let predicate = Arc::new(predicate);
    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        engine.as_ref(),
    )?;
    assert_eq!(metadata_row_count(&expected), expected_count);

    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;

    assert_metadata_eq(
        &without_columns(&actual, &[STATS])?,
        &without_columns(&expected, &[STATS])?,
        "partition pruning",
    )
}

#[test]
fn declarative_metadata_partition_is_null_keeps_null_partition() -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = load_test_table("data-reader-timestamp_ntz")?;
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(col!("tsNtzPartition").is_null()))
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;
    let formatted = pretty_format_batches(&actual)?.to_string();

    assert_eq!(metadata_row_count(&actual), 1, "{formatted}");
    let batch = actual.first().expect("null partition metadata");
    let path = batch
        .column_by_name("path")
        .expect("add.path")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("add.path string");
    assert_eq!(
        path.value(0),
        "tsNtzPartition=__HIVE_DEFAULT_PARTITION__/\
         part-00001-53fd3b3b-7773-459a-921c-bb64bf0bbd03.c000.snappy.parquet",
        "{formatted}"
    );
    let partitions = batch
        .column_by_name(PARTITION_VALUES_PARSED)
        .expect("add.partitionValues_parsed")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("add.partitionValues_parsed struct");
    assert!(
        partitions
            .column_by_name("tsNtzPartition")
            .expect("add.partitionValues_parsed.tsNtzPartition")
            .is_null(0),
        "{formatted}"
    );
    Ok(())
}

#[test]
fn declarative_metadata_reconstructs_well_formed_stats_and_partitions() -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) =
        crate::unit_test_utils::load_test_table("v1-multi-part-partitioned-struct-stats-only")?;
    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let plan = scan
        .declarative_metadata_scan_plan(engine.as_ref())?
        .expect("metadata plan");
    let batches = engine
        .plan_executor()
        .unwrap()
        .execute_op(PlanOperation::QueryPlan(plan))?
        .into_data()?;

    let mut projected = vec![];
    for batch in batches {
        let batch = batch?.try_into_record_batch()?;
        let add = batch
            .column_by_name(ADD_NAME)
            .expect("add column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("add struct");
        projected.push(RecordBatch::try_from_iter([
            (
                "stats",
                add.column_by_name(STATS_PARSED)
                    .expect("add.stats_parsed")
                    .clone(),
            ),
            (
                "partitionValues",
                add.column_by_name(PARTITION_VALUES_PARSED)
                    .expect("add.partitionValues_parsed")
                    .clone(),
            ),
        ])?);
    }

    let formatted = pretty_format_batches(&projected)?.to_string();
    let mut actual_rows: Vec<_> = formatted
        .lines()
        .filter(|line| line.starts_with("| {numRecords:"))
        .collect();
    actual_rows.sort_unstable();
    let expected_rows = [
        expected_stats_row(1, 1),
        expected_stats_row(2, 2),
        expected_stats_row(3, 0),
        expected_stats_row(4, 1),
        expected_stats_row(5, 2),
    ];
    assert_eq!(actual_rows, expected_rows, "{formatted}");
    assert!(formatted.contains("| stats"));
    assert!(formatted.contains("| partitionValues |"));
    Ok(())
}

fn expected_stats_row(id: i64, partition: i32) -> String {
    format!(
        "| {{numRecords: 1, nullCount: {{id: 0, value: 0}}, minValues: \
         {{id: {id}, value: value_{id}}}, maxValues: {{id: {id}, value: value_{id}}}, \
         tightBounds: true}} | {{part: {partition}}}       |"
    )
}

#[test]
fn declarative_metadata_reconciles_checkpoint_with_later_commits() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(4).with_checkpoint_at([2]))
        .build()
        .expect("build checkpoint-plus-commits table");
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;

    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        &engine,
    )?;
    assert_eq!(metadata_row_count(&expected), 4);

    let scan = snapshot
        .scan_builder()
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, &engine)?;

    assert_metadata_eq(&actual, &expected, "checkpoint with later commits")
}

#[test]
fn declarative_metadata_pruning_keeps_remove_for_checkpoint_reconciliation() -> DeltaResult<()> {
    let (engine, snapshot, _tempdir) = load_test_table("with_checkpoint_no_last_checkpoint")?;
    let scan = snapshot
        .scan_builder()
        .with_predicate(Arc::new(col!("int").gt(lit(0i64))))
        .build()?;
    let actual = declarative_metadata(&scan, engine.as_ref())?;
    let formatted = pretty_format_batches(&actual)?.to_string();

    assert_eq!(metadata_row_count(&actual), 1, "{formatted}");
    let path = actual[0]
        .column_by_name("path")
        .expect("add.path")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("add.path string");
    assert_eq!(
        path.value(0),
        "part-00000-70b1dcdf-0236-4f63-a072-124cdbafd8a0-c000.snappy.parquet",
        "{formatted}"
    );
    Ok(())
}

#[rstest]
fn declarative_metadata_prunes_across_v1_log_states(
    #[values(
        LogState::with_latest_version(4),
        LogState::with_latest_version(4).with_checkpoint_at([4]),
        LogState::with_latest_version(4).with_checkpoint_at([2])
    )]
    log_state: LogState,
    #[values(
        (col!("value").gt(lit(2500i32)), 2),
        (
            col!("part_string").eq(lit("part_2000")),
            1
        )
    )]
    pruning: (Pred, usize),
) -> DeltaResult<()> {
    assert_declarative_metadata_matches_imperative(
        log_state,
        FeatureSet::new(),
        pruning.0,
        pruning.1,
    )
}

#[rstest]
fn declarative_metadata_partition_prunes_v2_checkpoints(
    #[values(2, 4)] checkpoint_version: u64,
) -> DeltaResult<()> {
    let log_state = LogState::with_latest_version(4)
        .with_checkpoint_at([checkpoint_version])
        .with_sidecars_if_enabled(None);
    assert_declarative_metadata_matches_imperative(
        log_state,
        FeatureSet::new().v2_checkpoint(),
        col!("part_string").eq(lit("part_2000")),
        1,
    )
}

fn assert_declarative_metadata_matches_imperative(
    log_state: LogState,
    features: FeatureSet,
    predicate: Pred,
    expected_count: usize,
) -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(log_state)
        .with_features(features)
        .with_data_layout(DataLayoutConfig::PartitionedAllTypes)
        .build()
        .expect("build partitioned table");
    let engine = SyncEngine::new_with_store(table.store().clone());
    let snapshot = Snapshot::builder_for(table.table_root()).build(&engine)?;
    let predicate = Arc::new(predicate);

    let expected = imperative_metadata(
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(predicate.clone())
            .with_stats(StatsOptions::all())
            .with_partition_values(PartitionValuesOptions::with_struct())
            .build()?,
        &engine,
    )?;
    assert_eq!(
        metadata_row_count(&expected),
        expected_count,
        "{}",
        table.description()
    );
    let scan = snapshot
        .scan_builder()
        .with_predicate(predicate)
        .with_stats(StatsOptions::all())
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()?;
    let actual = declarative_metadata(&scan, &engine)?;

    assert_metadata_eq(&actual, &expected, table.description())
}

#[test]
fn test_declarative_metadata_scan_plan_no_executor_returns_unsupported() -> DeltaResult<()> {
    let table = TestTableBuilder::new()
        .with_log_state(LogState::with_latest_version(4).with_checkpoint_at([2]))
        .build()
        .expect("build checkpoint-plus-commits table");
    let sync_engine = Arc::new(SyncEngine::new_with_store(table.store().clone()));
    let snapshot = Snapshot::builder_for(table.table_root()).build(sync_engine.as_ref())?;
    let scan = snapshot.scan_builder().build()?;

    let no_plan_engine = DelegatingEngine::new(sync_engine).without_plan_executor();
    let err = scan
        .declarative_metadata_scan_plan(&no_plan_engine)
        .unwrap_err();

    assert!(matches!(err, crate::Error::Unsupported(_)));
    Ok(())
}
