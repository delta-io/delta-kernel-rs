//! Integration tests for CRC (version checksum) file-based APIs on Snapshot.

use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::snapshot::{FileStats, Snapshot};
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use object_store::local::LocalFileSystem;
use test_utils::test_table_setup;

#[tokio::test]
async fn test_get_file_stats_from_crc() -> DeltaResult<()> {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/crc-full/")).unwrap();
    let table_root = url::Url::from_directory_path(path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let engine = DefaultEngineBuilder::new(store).build();

    let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
    assert_eq!(snapshot.version(), 0);

    let file_stats = snapshot.get_file_stats(&engine);
    let expected = FileStats {
        table_size_bytes: 5259,
        num_files: 10,
    };
    assert_eq!(file_stats, Some(expected));

    Ok(())
}

#[tokio::test]
async fn test_get_file_stats_no_crc() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let schema = Arc::new(StructType::try_new(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("value", DataType::STRING, true),
    ])?);

    let _ = create_table(&table_path, schema, "Test/1.0")
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let table_url = delta_kernel::try_parse_uri(&table_path)?;
    let snapshot = Snapshot::builder_for(table_url).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 0);

    let file_stats = snapshot.get_file_stats(engine.as_ref());
    assert_eq!(file_stats, None);

    Ok(())
}
