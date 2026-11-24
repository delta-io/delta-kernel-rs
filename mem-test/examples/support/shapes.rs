use std::collections::VecDeque;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{ArrayType, DataType, MapType, MetadataValue, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::transaction::data_layout::DataLayout;
use delta_kernel::{KernelError, KernelResult, Snapshot, try_parse_uri};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::Fixture;

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaShape {
    Flat,
    #[default]
    Nested,
    Mixed,
    Deep,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FixtureOptions {
    pub width: usize,
    pub shape: SchemaShape,
    pub depth: usize,
    pub metadata_bytes: usize,
    pub schema_bytes: Option<usize>,
    pub initial_files: usize,
    pub tail_commits: u64,
    pub actions_per_commit: usize,
    pub removes_per_commit: usize,
    pub checkpoint: bool,
    pub parsed_stats: bool,
    pub checkpoint_json_stats: bool,
    pub stats_columns: i64,
    pub stats_fields: usize,
    pub stats_string_bytes: usize,
    pub stats_bytes: usize,
    pub partitioned: bool,
}

impl Default for FixtureOptions {
    fn default() -> Self {
        Self {
            width: 100,
            shape: SchemaShape::Nested,
            depth: 4,
            metadata_bytes: 0,
            schema_bytes: None,
            initial_files: 100,
            tail_commits: 100,
            actions_per_commit: 1,
            removes_per_commit: 0,
            checkpoint: true,
            parsed_stats: false,
            checkpoint_json_stats: true,
            stats_columns: 0,
            stats_fields: 0,
            stats_string_bytes: 16,
            stats_bytes: 0,
            partitioned: false,
        }
    }
}

impl Fixture {
    pub fn from_options(options: &FixtureOptions) -> Result<Self, Box<dyn std::error::Error>> {
        let o = options;
        if o.width == 0 || !(1..=64).contains(&o.depth) || o.initial_files == 0 {
            return Err("fixture requires positive width/files and depth in 1..=64".into());
        }
        let directory = tempfile::tempdir()?;
        let table_root =
            try_parse_uri(directory.path().to_str().ok_or("non-UTF-8 fixture path")?)?.to_string();
        let mut fields = (0..o.width)
            .map(|i| {
                let mut field = StructField::nullable(format!("col_{i}"), column_type(i, o)?);
                if o.metadata_bytes != 0 {
                    field.metadata.insert(
                        "comment".into(),
                        MetadataValue::String("m".repeat(o.metadata_bytes).into()),
                    );
                }
                Ok(field)
            })
            .collect::<KernelResult<Vec<_>>>()?;
        if o.partitioned {
            fields.push(StructField::nullable("p", DataType::LONG));
        }
        if let Some(target) = o.schema_bytes {
            fields[0]
                .metadata
                .insert("comment".into(), MetadataValue::String("".into()));
            let current = serde_json::to_vec(&StructType::try_new(fields.clone())?)?.len();
            let padding = target
                .checked_sub(current)
                .ok_or("schema padding overflow")?;
            fields[0].metadata.insert(
                "comment".into(),
                MetadataValue::String("x".repeat(padding).into()),
            );
        }
        let schema = Arc::new(StructType::try_new(fields)?);
        let schema_bytes = serde_json::to_vec(schema.as_ref())?.len();
        let schema_fields = count_fields(&schema);
        let stats = action_stats(&schema, o)?;
        let engine = SyncEngine::new();
        let mut properties = vec![(
            "delta.dataSkippingNumIndexedCols",
            o.stats_columns.to_string(),
        )];
        if o.parsed_stats {
            properties.push(("delta.checkpoint.writeStatsAsStruct", "true".into()));
        }
        if !o.checkpoint_json_stats {
            properties.push(("delta.checkpoint.writeStatsAsJson", "false".into()));
        }
        let builder = create_table(&table_root, schema, "metadata-memory-fixture", true)
            .with_table_properties(properties);
        let builder = if o.partitioned {
            builder.with_data_layout(DataLayout::partitioned(["p"]))
        } else {
            builder
        };
        builder
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_committed();
        let mut active = VecDeque::new();
        let initial = (0..o.initial_files)
            .map(|i| {
                let path = format!("part-{i}-v0.parquet");
                active.push_back(path.clone());
                add(path, i, 0, &stats, o.partitioned).to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        let store = LocalFileSystem::new();
        futures::executor::block_on(test_utils::add_commit(&table_root, &store, 1, initial))?;
        if o.checkpoint {
            Snapshot::builder_for(&table_root)
                .build(&engine)?
                .checkpoint(&engine, None)?;
        }
        let mut remove_actions = 0;
        for version in 2..=o.tail_commits + 1 {
            let mut actions = Vec::new();
            for _ in 0..o.removes_per_commit {
                if let Some(path) = active.pop_front() {
                    actions.push(
                        json!({"remove": {
                            "path": path, "deletionTimestamp": version, "dataChange": true
                        }})
                        .to_string(),
                    );
                    remove_actions += 1;
                }
            }
            for i in 0..o.actions_per_commit {
                let path = if o.actions_per_commit == 1 {
                    format!("tail-{version}.parquet")
                } else {
                    format!("tail-{version}-{i}.parquet")
                };
                active.push_back(path.clone());
                actions.push(add(path, i, version, &stats, o.partitioned).to_string());
            }
            if actions.is_empty() {
                actions.push(json!({"commitInfo": {"timestamp": version}}).to_string());
            }
            futures::executor::block_on(test_utils::add_commit(
                &table_root,
                &store,
                version,
                actions.join("\n"),
            ))?;
        }
        Ok(Self {
            directory,
            table_root,
            schema_bytes,
            schema_fields,
            width: o.width,
            initial_files: o.initial_files,
            tail_commits: o.tail_commits,
            stats_json_bytes: stats.len(),
            add_actions: o.initial_files + o.tail_commits as usize * o.actions_per_commit,
            remove_actions,
            checkpoints: usize::from(o.checkpoint),
        })
    }
}

fn nested() -> KernelResult<StructType> {
    StructType::try_new([
        StructField::nullable("x", DataType::LONG),
        StructField::nullable("y", DataType::STRING),
    ])
}

fn column_type(index: usize, o: &FixtureOptions) -> KernelResult<DataType> {
    Ok(match o.shape {
        SchemaShape::Flat => {
            if index % 4 == 1 {
                DataType::STRING
            } else {
                DataType::LONG
            }
        }
        SchemaShape::Nested => nested()?.into(),
        SchemaShape::Mixed => match index % 5 {
            0 => DataType::LONG,
            1 => DataType::STRING,
            2 => nested()?.into(),
            3 => ArrayType::new(nested()?, true).into(),
            _ => MapType::new(DataType::STRING, nested()?, true).into(),
        },
        SchemaShape::Deep => {
            let mut inner = nested()?;
            for level in 0..o.depth {
                inner =
                    StructType::try_new([StructField::nullable(format!("level_{level}"), inner)])?;
            }
            inner.into()
        }
    })
}

fn add(path: String, index: usize, version: u64, stats: &str, partitioned: bool) -> Value {
    let partitions = if partitioned {
        json!({"p": (index % 8).to_string()})
    } else {
        json!({})
    };
    json!({"add": {
        "path": path, "partitionValues": partitions, "size": 100,
        "modificationTime": version, "dataChange": true, "stats": stats
    }})
}

fn action_stats(schema: &StructType, o: &FixtureOptions) -> KernelResult<String> {
    let mut stats = json!({"numRecords": if o.stats_fields == 0 { 1 } else { 2 }});
    if o.stats_fields != 0 {
        for kind in ["minValues", "maxValues", "nullCount"] {
            let mut values = Map::new();
            for field in schema
                .fields()
                .filter(|f| f.name() != "p")
                .take(o.stats_fields)
            {
                if let Some(value) = stat_value(field.data_type(), kind, o.stats_string_bytes) {
                    values.insert(field.name().clone(), value);
                }
            }
            stats[kind] = Value::Object(values);
        }
        stats["tightBounds"] = json!(true);
    }
    if o.stats_bytes != 0 {
        stats["padding"] = json!("");
        let current = serde_json::to_vec(&stats)?.len();
        let padding = o
            .stats_bytes
            .checked_sub(current)
            .ok_or_else(|| KernelError::generic("stats size is too small"))?;
        stats["padding"] = json!("x".repeat(padding));
    }
    Ok(serde_json::to_string(&stats)?)
}

fn stat_value(data_type: &DataType, kind: &str, bytes: usize) -> Option<Value> {
    match data_type {
        DataType::Struct(inner) => Some(Value::Object(
            inner
                .fields()
                .filter_map(|field| {
                    stat_value(field.data_type(), kind, bytes)
                        .map(|value| (field.name().clone(), value))
                })
                .collect(),
        )),
        _ if kind == "nullCount" => Some(json!(0)),
        value if *value == DataType::LONG => {
            Some(json!(if kind == "minValues" { 0 } else { 100_000 }))
        }
        value if *value == DataType::STRING => Some(json!(
            if kind == "minValues" { "a" } else { "z" }.repeat(bytes)
        )),
        _ => None,
    }
}

fn count_fields(schema: &StructType) -> usize {
    schema
        .fields()
        .map(|field| {
            1 + match field.data_type() {
                DataType::Struct(inner) => count_fields(inner),
                DataType::Array(inner) => match inner.element_type() {
                    DataType::Struct(inner) => count_fields(inner),
                    _ => 0,
                },
                DataType::Map(inner) => match inner.value_type() {
                    DataType::Struct(inner) => count_fields(inner),
                    _ => 0,
                },
                _ => 0,
            }
        })
        .sum()
}
