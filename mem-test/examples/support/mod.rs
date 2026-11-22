use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::schema::{DataType, MetadataValue, StructField, StructType};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::{Snapshot, try_parse_uri};
use serde_json::json;

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub table_root: String,
    pub schema_bytes: usize,
    pub width: usize,
    pub initial_files: usize,
    pub tail_commits: u64,
}

impl Fixture {
    pub fn create(
        width: usize,
        initial_files: usize,
        tail_commits: u64,
        schema_bytes: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::create_with_action_size(width, initial_files, tail_commits, schema_bytes, 0)
    }

    pub fn create_with_action_size(
        width: usize,
        initial_files: usize,
        tail_commits: u64,
        schema_bytes: Option<usize>,
        stats_bytes: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::create_with_options(
            width,
            initial_files,
            tail_commits,
            schema_bytes,
            stats_bytes,
            0,
            false,
        )
    }

    pub fn create_with_options(
        width: usize,
        initial_files: usize,
        tail_commits: u64,
        schema_bytes: Option<usize>,
        stats_bytes: usize,
        stats_columns: i64,
        parsed_stats: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let table_root =
            try_parse_uri(directory.path().to_str().ok_or("non-UTF-8 fixture path")?)?.to_string();
        let mut fields = (0..width)
            .map(|i| {
                Ok(StructField::nullable(
                    format!("col_{i}"),
                    StructType::try_new([
                        StructField::nullable("x", DataType::LONG),
                        StructField::nullable("y", DataType::STRING),
                    ])?,
                ))
            })
            .collect::<delta_kernel::KernelResult<Vec<_>>>()?;
        if let Some(target) = schema_bytes {
            let current = serde_json::to_vec(&StructType::try_new(fields.clone())?)?.len();
            let padding = target
                .checked_sub(current + br#""comment":"""#.len())
                .ok_or("schema padding overflow")?;
            fields[0].metadata.insert(
                "comment".into(),
                MetadataValue::String("x".repeat(padding).into()),
            );
        }
        let schema = Arc::new(StructType::try_new(fields)?);
        let schema_bytes = serde_json::to_vec(schema.as_ref())?.len();
        let engine = SyncEngine::new();
        let mut properties = vec![(
            "delta.dataSkippingNumIndexedCols",
            stats_columns.to_string(),
        )];
        if parsed_stats {
            properties.push(("delta.checkpoint.writeStatsAsStruct", "true".into()));
        }
        create_table(&table_root, schema, "metadata-memory-fixture", true)
            .with_table_properties(properties)
            .build(&engine, Box::new(FileSystemCommitter::new()))?
            .commit(&engine)?
            .unwrap_committed();
        let store = LocalFileSystem::new();
        let stats = if stats_bytes == 0 {
            "{\"numRecords\":1}".to_owned()
        } else {
            let empty = "{\"numRecords\":1,\"padding\":\"\"}";
            let padding = stats_bytes
                .checked_sub(empty.len())
                .ok_or("stats size is too small")?;
            format!(
                "{{\"numRecords\":1,\"padding\":\"{}\"}}",
                "x".repeat(padding)
            )
        };
        let initial = (0..initial_files)
            .map(|i| {
                json!({"add": {
                    "path": format!("part-{i}-v0.parquet"), "partitionValues": {},
                    "size": 100, "modificationTime": 0, "dataChange": true,
                    "stats": stats.clone()
                }})
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        futures::executor::block_on(test_utils::add_commit(&table_root, &store, 1, initial))?;
        Snapshot::builder_for(&table_root)
            .build(&engine)?
            .checkpoint(&engine, None)?;
        for version in 2..=tail_commits + 1 {
            // New paths exercise retained deduplication state. No data scan is requested.
            let action = json!({"add": {
                "path": format!("tail-{version}.parquet"), "partitionValues": {},
                "size": 100, "modificationTime": version, "dataChange": true,
                "stats": stats.clone()
            }});
            futures::executor::block_on(test_utils::add_commit(
                &table_root,
                &store,
                version,
                action.to_string(),
            ))?;
        }
        Ok(Self {
            directory,
            table_root,
            schema_bytes,
            width,
            initial_files,
            tail_commits,
        })
    }
}
