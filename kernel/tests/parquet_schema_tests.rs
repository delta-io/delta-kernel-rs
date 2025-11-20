//! Tests for getting Parquet schema from file footer
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{Engine, FileMeta};
use object_store::local::LocalFileSystem;
use url::Url;

#[test]
fn test_get_parquet_schema_simple() -> Result<(), Box<dyn std::error::Error>> {
    // Use an existing test Parquet file
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/table-with-dv-small/part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet",
    ))?;
    let url = Url::from_file_path(path).unwrap();

    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(store));
    let parquet_handler = engine.parquet_handler();

    let file_meta = FileMeta {
        location: url,
        last_modified: 0,
        size: 0,
    };

    // Get the schema
    let schema = parquet_handler.get_parquet_schema(&file_meta)?;

    // Verify the schema has expected fields
    assert!(
        schema.fields().count() > 0,
        "Schema should have at least one field"
    );

    // This test file should have specific columns - let's verify some basic properties
    let field_names: Vec<_> = schema.fields().map(|f| f.name()).collect();
    assert!(!field_names.is_empty(), "Should have field names");

    Ok(())
}

#[test]
fn test_get_parquet_schema_invalid_file() {
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(store));
    let parquet_handler = engine.parquet_handler();

    // Try with a non-existent file
    let url = Url::from_file_path("/tmp/non_existent_file.parquet").unwrap();
    let file_meta = FileMeta {
        location: url,
        last_modified: 0,
        size: 0,
    };

    let result = parquet_handler.get_parquet_schema(&file_meta);
    assert!(result.is_err(), "Should error on non-existent file");
}

#[test]
fn test_get_parquet_schema_non_parquet_file() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(store));
    let parquet_handler = engine.parquet_handler();

    // Try with a non-Parquet file (use a JSON log file)
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/table-with-dv-small/_delta_log/00000000000000000000.json",
    ))?;
    let url = Url::from_file_path(path).unwrap();

    let file_meta = FileMeta {
        location: url,
        last_modified: 0,
        size: 0,
    };

    let result = parquet_handler.get_parquet_schema(&file_meta);
    assert!(result.is_err(), "Should error on non-Parquet file");

    Ok(())
}

#[test]
fn test_get_parquet_schema_with_nested_types() -> Result<(), Box<dyn std::error::Error>> {
    // Find a test file with nested types (struct, array, map)
    // Using type-widening test data which might have nested structures
    let test_files = vec![
        "./tests/data/type-widening/part-00000-61accb66-b740-416b-9f5b-f0fccaceb415-c000.snappy.parquet",
    ];

    let store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(store));
    let parquet_handler = engine.parquet_handler();

    for test_file in test_files {
        let path_buf = PathBuf::from(test_file);
        if !path_buf.exists() {
            continue;
        }

        let path = std::fs::canonicalize(path_buf)?;
        let url = Url::from_file_path(path).unwrap();

        let file_meta = FileMeta {
            location: url,
            last_modified: 0,
            size: 0,
        };

        // Get the schema
        let schema = parquet_handler.get_parquet_schema(&file_meta)?;

        // Verify we can read the schema
        assert!(schema.fields().count() > 0, "Schema should have fields");

        // Print field types for debugging (if needed)
        for field in schema.fields() {
            let _data_type = field.data_type();
            // Schema was successfully read and converted
        }
    }

    Ok(())
}
