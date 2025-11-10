use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url_opts;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::{DeltaResult, Engine, Error};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use url::Url;

/// Integration test for copy_atomic with S3.
///
/// Tests atomic copy operations using different copy-if-not-exists strategies
/// (multipart or header) against real S3 storage.
///
/// Requires AWS credentials set via AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// S3 bucket name
    #[arg(long)]
    bucket: String,

    /// AWS region (default: us-east-1)
    #[arg(long, default_value = "us-east-1")]
    region: String,

    /// Optional S3-compatible endpoint (e.g., MinIO: http://localhost:9000)
    #[arg(long)]
    endpoint: Option<String>,

    /// Test prefix for file names
    #[arg(long, default_value = "copy-atomic-test")]
    prefix: String,
}

fn main() -> ExitCode {
    env_logger::init();
    match tokio::runtime::Runtime::new().unwrap().block_on(try_main()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e:#?}");
            ExitCode::FAILURE
        }
    }
}

async fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    println!("=== S3 copy_atomic Test ===");
    println!("Bucket: {}", cli.bucket);
    println!("Region: {}", cli.region);
    if let Some(endpoint) = &cli.endpoint {
        println!("Endpoint: {}", endpoint);
    }
    println!();

    let url = Url::parse(&format!("s3://{}", cli.bucket))
        .map_err(|e| Error::generic(format!("Invalid URL: {e}")))?;

    let mut options = HashMap::new();
    options.insert("region".to_string(), cli.region.clone());

    if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
        options.insert("aws_access_key_id".to_string(), access_key);
    }

    if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        options.insert("aws_secret_access_key".to_string(), secret_key);
    }

    if let Some(endpoint) = &cli.endpoint {
        options.insert("endpoint".to_string(), endpoint.clone());
        options.insert("allow_http".to_string(), "true".to_string());
    }

    let executor = Arc::new(TokioBackgroundExecutor::new());
    let object_store = store_from_url_opts(&url, options)?;
    let engine = DefaultEngine::new_with_executor(object_store, executor.clone());
    let handler = engine.storage_handler();

    // Get object store for direct operations
    let s3_store = engine
        .get_object_store_for_url(&url)
        .ok_or_else(|| Error::generic("Failed to get object store"))?;

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let source_key = format!("{}/source-{}.txt", cli.prefix, timestamp);
    let dest_key = format!("{}/dest-{}.txt", cli.prefix, timestamp);

    println!("Test files:");
    println!("  Source: {}", source_key);
    println!("  Dest:   {}", dest_key);
    println!();

    println!("Test 1: Creating source file...");
    let test_data = format!(
        "Test data from copy_atomic example - timestamp: {}",
        timestamp
    );
    let source_path = ObjectStorePath::from(source_key.clone());
    s3_store
        .put(&source_path, test_data.clone().into())
        .await
        .map_err(|e| Error::generic(format!("Failed to create source file: {e}")))?;
    println!("[OK] Source file created");
    println!();

    println!("Test 2: Copying to destination (should succeed)...");
    let source_url = Url::parse(&format!("s3://{}/{}", cli.bucket, source_key))
        .map_err(|e| Error::generic(format!("Invalid URL: {e}")))?;
    let dest_url = Url::parse(&format!("s3://{}/{}", cli.bucket, dest_key))
        .map_err(|e| Error::generic(format!("Invalid URL: {e}")))?;

    handler.copy_atomic(&source_url, &dest_url)?;
    println!("[OK] Copy succeeded");
    println!();

    println!("Test 3: Verifying destination content...");
    let dest_path = ObjectStorePath::from(dest_key.clone());
    let dest_data = s3_store
        .get(&dest_path)
        .await
        .map_err(|e| Error::generic(format!("Failed to read destination: {e}")))?
        .bytes()
        .await
        .map_err(|e| Error::generic(format!("Failed to read bytes: {e}")))?;
    let dest_string = String::from_utf8(dest_data.to_vec())
        .map_err(|e| Error::generic(format!("Invalid UTF-8: {e}")))?;

    if dest_string == test_data {
        println!("[OK] Destination content matches source");
    } else {
        return Err(Error::generic("Destination content doesn't match!"));
    }
    println!();

    println!("Test 4: Attempting duplicate copy (should fail)...");
    match handler.copy_atomic(&source_url, &dest_url) {
        Err(Error::FileAlreadyExists(_)) => {
            println!("[OK] Correctly failed with FileAlreadyExists");
        }
        Ok(_) => {
            return Err(Error::generic(
                "FAIL: Copy should have failed but succeeded!",
            ))
        }
        Err(e) => {
            return Err(Error::generic(format!(
                "FAIL: Expected FileAlreadyExists, got: {e}"
            )))
        }
    }
    println!();

    println!("Test 5: Copying from non-existent source (should fail)...");
    let missing_url = Url::parse(&format!("s3://{}/{}/missing.txt", cli.bucket, cli.prefix))
        .map_err(|e| Error::generic(format!("Invalid URL: {e}")))?;
    let dest2_url = Url::parse(&format!(
        "s3://{}/{}/dest2-{}.txt",
        cli.bucket, cli.prefix, timestamp
    ))
    .map_err(|e| Error::generic(format!("Invalid URL: {e}")))?;

    match handler.copy_atomic(&missing_url, &dest2_url) {
        Err(_) => {
            println!("[OK] Correctly failed when source doesn't exist");
        }
        Ok(_) => {
            return Err(Error::generic(
                "FAIL: Should have failed with missing source!",
            ))
        }
    }
    println!();

    println!("Cleanup: Deleting test files...");
    s3_store
        .delete(&source_path)
        .await
        .map_err(|e| Error::generic(format!("Failed to delete source: {e}")))?;
    s3_store
        .delete(&dest_path)
        .await
        .map_err(|e| Error::generic(format!("Failed to delete dest: {e}")))?;
    println!("[OK] Test files deleted");
    println!();

    println!("=== All Tests Passed! ===");
    println!();
    println!("Summary:");
    println!("  [OK] Source file creation");
    println!("  [OK] Atomic copy to destination");
    println!("  [OK] Content verification");
    println!("  [OK] Duplicate copy prevention (atomic if-not-exists)");
    println!("  [OK] Missing source error handling");
    println!();
    println!("The copy_atomic implementation works correctly on S3!");

    Ok(())
}
