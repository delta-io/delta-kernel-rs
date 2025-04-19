// use std::path::PathBuf;
// use std::sync::Arc;

// use delta_kernel::arrow::array::RecordBatch;
// use delta_kernel::arrow::datatypes::Schema;
// use delta_kernel::engine::arrow_data::ArrowEngineData;
// use delta_kernel::engine::sync::SyncEngine;
// use delta_kernel::{arrow, DeltaResult, Engine, Table};

// mod common;
// use common::{load_test_data, read_scan};
// use itertools::Itertools;
// use parquet_53::arrow::ArrowWriter;

// fn read_table(test_name: &str) -> DeltaResult<Vec<RecordBatch>> {
//     let path = std::fs::canonicalize(PathBuf::from(format!("./tests/data/{}/", test_name)))?;
//     let table = Table::try_from_uri(path.to_string_lossy().as_ref())?;
//     let engine = Arc::new(SyncEngine::new());
//     let snapshot = table.snapshot(engine.as_ref(), None)?;
//     let scan = snapshot.into_scan_builder().build()?;
//     let batches = read_scan(&scan, engine)?;

//     Ok(batches)
// }

// fn checkpoint_table(table_name: &str, checkpoint_version: u64) -> DeltaResult<()> {
//     let path = std::fs::canonicalize(PathBuf::from(format!("./tests/data/{}/", table_name)))?;
//     let table = Table::try_from_uri(path.to_string_lossy().as_ref())?;
//     let engine = SyncEngine::new();

//     let mut writer = table.checkpoint(&engine, Some(checkpoint_version))?;

//     // Get the checkpoint data
//     let checkpoint_data = writer.checkpoint_data(&engine)?;

//     // Convert URL to filesystem path for local implementation
//     let fs_path = std::path::Path::new(checkpoint_data.path.as_str())
//         .strip_prefix("file:/")
//         .unwrap();

//     // Create parent directories
//     if let Some(parent) = fs_path.parent() {
//         std::fs::create_dir_all(parent)?;
//     }

//     use std::fs::File;

//     // Create a Parquet writer with appropriate schema
//     // Note: In a real implementation, you would use the schema from the data
//     let schema = Schema::new(vec![]);
//     let file = File::create(fs_path)?;
//     let mut parquet_writer = ArrowWriter::try_new(file, schema.into(), None)?;

//     // Write the data to the Parquet file
//     let mut size_in_bytes: i64 = 0;
//     for data_result in checkpoint_data.data {
//         let filtered_data = data_result?;

//         // Only write data that matches the selection vector (true values)
//         if filtered_data
//             .selection_vector
//             .iter()
//             .any(|&selected| selected)
//         {
//             // Get the actual engine data
//             let data = filtered_data.data;

//             // Convert the engine data to a record batch and write it
//             if let Some(arrow_batch) = data
//                 .into_any()
//                 .downcast::<ArrowEngineData>()
//                 .unwrap()
//                 .into()
//             {
//                 // Write the batch to the Parquet file
//                 parquet_writer.write(arrow_batch.record_batch())?;
//             }
//         }
//     }

//     // Close the writer and flush to disk
//     parquet_writer.close()?;

//     Ok(())
// }

// fn test_table(table_name: &str, latest_version: u64) -> DeltaResult<()> {
//     let mut expected = read_table(table_name)?;
//     sort_lines!(expected);

//     for version in 0..latest_version {
//         // Checkpoint at each version
//         checkpoint_table(table_name, version)?;

//         let result = read_table(table_name)?;
//         assert_batches_sorted_eq!(expected, &result);
//     }

//     Ok(())
// }

// #[test]
// fn v2_checkpoints_json_with_sidecars() -> DeltaResult<()> {
//     test_table("app-txn-no-checkpoint", 1);
//     Ok(())
// }
