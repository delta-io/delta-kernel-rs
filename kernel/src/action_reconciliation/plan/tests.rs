use rstest::rstest;
use serde_json::{Value, json};
use url::Url;

use super::*;
use crate::actions::{ADD_SCHEMA, Protocol, STATS_PARSED};
use crate::arrow::json::WriterBuilder;
use crate::arrow::json::writer::JsonArray;
use crate::engine::arrow_data::EngineDataArrowExt as _;
use crate::engine::sync::SyncEngine;
use crate::log_segment::LogSegment;
use crate::log_segment_files::LogSegmentFiles;
use crate::plans::Operation;
use crate::schema::{SchemaStructPatchBuilder, schema_ref};
use crate::unit_test_utils::{MockTableConfigurationBuilder, create_log_path};
use crate::{Engine, Snapshot};

fn fixture(mut actions: Vec<Value>) -> KernelResult<(Snapshot, tempfile::TempDir)> {
    for action in &mut actions {
        if let Some(add) = action.get_mut(ADD_NAME).and_then(Value::as_object_mut) {
            add.entry("partitionValues").or_insert_with(|| json!({}));
            add.entry("size").or_insert(json!(1));
            add.entry("modificationTime").or_insert(json!(1));
            add.entry("dataChange").or_insert(json!(true));
        }
    }
    let temp = tempfile::tempdir()?;
    let log_root = Url::from_directory_path(temp.path()).unwrap();
    let commit = log_root.join("00000000000000000000.json")?;
    std::fs::write(
        commit.to_file_path().unwrap(),
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    let file = create_log_path(commit.as_str());
    let segment = LogSegment {
        end_version: 0,
        checkpoint_version: None,
        log_root,
        listed: LogSegmentFiles {
            ascending_commit_files: vec![file.clone()],
            latest_commit_file: Some(file),
            max_published_version: Some(0),
            ..Default::default()
        },
        last_checkpoint_metadata: None,
    };
    let config = MockTableConfigurationBuilder::new()
        .with_protocol(Protocol::try_new_modern(
            Vec::<&str>::new(),
            ["collations"],
        )?)
        .with_schema(schema_ref! { nullable "x": LONG, nullable "y": LONG })
        .with_properties([("delta.dataSkippingStatsColumns", "x")])
        .try_build()?;
    Ok((Snapshot::new(segment, config)?, temp))
}

fn file_options(json_stats: bool, parsed_stats: Option<SchemaRef>) -> ActionOptions {
    ActionOptions {
        actions: [ActionKind::FileActions {
            includes_removes: false,
        }]
        .into_iter()
        .collect(),
        json_stats,
        parsed_stats,
        parsed_partition_values: false,
    }
}

fn available_stats(stats: &SchemaRef) -> KernelResult<SchemaRef> {
    Ok(schema_ref! {
        nullable ADD_NAME: (SchemaStructPatchBuilder::new()
            .append(StructField::nullable(STATS_PARSED, stats.clone())).build(&ADD_SCHEMA)?),
        nullable VERSION: LONG,
    })
}

fn read_native(
    snapshot: &Snapshot,
    requested: &ActionOptions,
    available: &StructType,
) -> KernelResult<ActionPlan> {
    read_source(
        Some(snapshot.table_configuration()),
        requested,
        available,
        |schema| {
            PlanBuilder::scan_json(
                snapshot
                    .log_segment()
                    .commit_cover_version_tagged_scan_files()?,
                &[VERSION],
                schema,
            )
        },
    )
}

fn rows(plan: PlanBuilder) -> KernelResult<Vec<Value>> {
    let engine = SyncEngine::new();
    let batches = engine
        .require_plan_executor()?
        .execute_op(Operation::QueryPlan(plan.build()?))?
        .into_data()?
        .map(|batch| batch?.try_into_record_batch())
        .collect::<KernelResult<Vec<_>>>()?;
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    Ok(serde_json::from_slice(&writer.into_inner())?)
}

#[rstest]
fn wider_stats_request_fills_absent_fields_and_keeps_native_values(
    #[values(false, true)] request_wide_at_source: bool,
    #[values(false, true)] prefer_json: bool,
    #[values(false, true)] json_present: bool,
) -> KernelResult<()> {
    let native = schema_ref! {
        nullable "numRecords": LONG,
        nullable "minValues": { nullable "x": LONG },
        nullable "extension": STRING,
    };
    let wide = schema_ref! {
        nullable "numRecords": LONG,
        nullable "tightBounds": BOOLEAN,
        nullable "minValues": { nullable "x": LONG, nullable "y": LONG },
        nullable "extension": STRING,
    };
    let json = json_present.then(|| {
        json!({"numRecords":9,"tightBounds":true,
        "minValues":{"x":99,"y":42},"extension":"json"})
        .to_string()
    });
    let (snapshot, _temp) = fixture(vec![json!({"add":{"path":"a.parquet", "stats":json,
        "stats_parsed":{"numRecords":7,"minValues":{"x":null},"extension":"native"}}})])?;
    let source_options = file_options(
        false,
        Some(if request_wide_at_source {
            wide.clone()
        } else {
            native.clone()
        }),
    );
    let source = read_native(
        &snapshot,
        &source_options,
        available_stats(&native)?.as_ref(),
    )?;
    assert_eq!(source.options.parsed_stats.as_ref(), Some(&native));
    // An extension already carried by the relation survives a request that does not name it.
    let requested_schema = Arc::new(
        SchemaStructPatchBuilder::new()
            .drop("extension")
            .build(&wide)?,
    );
    let requested = file_options(false, Some(requested_schema));
    let result = parse_file_metadata(&snapshot, source, &requested, prefer_json)?;
    assert_eq!(result.options.parsed_stats.as_ref(), Some(&wide));
    let actual = rows(result.plan)?;
    let stats = &actual[0]["add"][STATS_PARSED];
    let from_json = prefer_json && json_present;
    assert_eq!(
        stats,
        &json!({
            "numRecords": if from_json { 9 } else { 7 },
            "tightBounds": json_present.then_some(true),
            "minValues": {"x": from_json.then_some(99), "y": json_present.then_some(42)},
            "extension": if from_json { "json" } else { "native" },
        })
    );
    Ok(())
}

#[rstest]
fn incompatible_stats_request_uses_json_or_errors(
    #[values(false, true)] keep_json: bool,
) -> KernelResult<()> {
    let native = schema_ref! { nullable "numRecords": STRING };
    let (snapshot, _temp) = fixture(vec![json!({"add":{"path":"a.parquet",
        "stats":"{\"numRecords\":9}","stats_parsed":{"numRecords":"bad"}}})])?;
    let mut source = read_native(
        &snapshot,
        &file_options(false, Some(native.clone())),
        available_stats(&native)?.as_ref(),
    )?;
    if !keep_json {
        source.plan = source
            .plan
            .project_patch(|patch| patch.drop_at([ADD_NAME], "stats"))?;
        source.options.json_stats = false;
    }
    let requested = file_options(false, Some(schema_ref! { nullable "numRecords": LONG }));
    let result = parse_file_metadata(&snapshot, source, &requested, false);
    if keep_json {
        assert_eq!(rows(result?.plan)?[0]["add"][STATS_PARSED]["numRecords"], 9);
    } else {
        assert!(result.is_err());
    }
    Ok(())
}

#[rstest]
#[case::unindexed(
    schema_ref! {
        nullable "numRecords": LONG,
        nullable "minValues": { nullable "x": LONG, nullable "y": LONG },
    },
    json!({"numRecords":1,"minValues":{"x":10,"y":20}}),
    "minValues", json!({"x":10,"y":20})
)]
#[case::collation(
    schema_ref! {
        nullable "numRecords": LONG,
        nullable "statsWithCollation": {
            nullable "ICU.en_US.72": { nullable "minValues": { nullable "s": STRING } },
        },
    },
    json!({"numRecords":7,"statsWithCollation":{"ICU.en_US.72":{"minValues":{"s":"a"}}}}),
    "statsWithCollation", json!({"ICU.en_US.72":{"minValues":{"s":"a"}}})
)]
fn source_preserves_native_stats_extensions(
    #[case] native: SchemaRef,
    #[case] stats: Value,
    #[case] extension: &str,
    #[case] expected: Value,
    #[values(false, true)] request_native: bool,
    #[values(false, true)] reuse_options: bool,
) -> KernelResult<()> {
    let (snapshot, _temp) = fixture(vec![json!({"add":{
        "path":"a.parquet", "stats_parsed":stats,
    }})])?;
    let available = available_stats(&native)?;
    let options = file_options(true, request_native.then_some(native.clone()));
    let mut source = read_native(&snapshot, &options, &available)?;
    if reuse_options {
        source = read_native(&snapshot, &source.options, &available)?;
    }
    assert_eq!(source.options.parsed_stats.as_ref(), Some(&native));
    let parsed = if request_native {
        let requested = file_options(
            false,
            Some(schema_ref! {
                nullable "numRecords": LONG, nullable "tightBounds": BOOLEAN,
            }),
        );
        parse_file_metadata(&snapshot, source, &requested, false)?
    } else {
        source
    };
    assert!(
        parsed
            .options
            .parsed_stats
            .as_ref()
            .unwrap()
            .contains(extension)
    );
    let actual = rows(synthesize_json_stats(parsed)?.plan)?;
    assert_eq!(actual[0]["add"][STATS_PARSED][extension], expected);
    let json: Value = serde_json::from_str(actual[0]["add"]["stats"].as_str().unwrap())?;
    assert_eq!(json[extension], expected);
    Ok(())
}
