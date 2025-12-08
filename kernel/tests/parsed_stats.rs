//! Integration tests for parsed stats (stats_parsed) support.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::expressions::{column_expr, Expression as Expr, Predicate};
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::{DeltaResult, Engine, ExpressionRef, Snapshot};
use url::Url;

fn test_table_url() -> Url {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // Go up from kernel to parsed-stats-all
    path.push("test_data/parsed_stats_table");
    Url::from_directory_path(path).unwrap()
}

/// Test that data skipping works with parsed stats.
/// The test table has:
/// - file 1: id 1-100, value 10-1000
/// - file 2: id 101-200, value 2000-3000
///
/// Run with RUST_LOG=info to see parsed stats logging.
#[test]
fn test_data_skipping_with_parsed_stats() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging so we can see parsed stats being used
    let _ = tracing_subscriber::fmt::try_init();

    let url = test_table_url();
    let engine = test_utils::create_default_engine(&url)?;

    let snapshot = Snapshot::builder_for(url).build(engine.as_ref())?;

    // Test 1: Predicate that should keep only file 1 (id < 50)
    let predicate: Arc<Predicate> =
        Arc::new(Predicate::lt(column_expr!("id"), Expr::literal(50i64)));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    let files = get_scan_files(&scan, engine.as_ref())?;
    assert_eq!(
        files.len(),
        1,
        "Should only scan file 1 with predicate id < 50, got {:?}",
        files
    );
    assert!(
        files[0].contains("part-00000"),
        "Should select first file, got {:?}",
        files
    );

    // Test 2: Predicate that should keep only file 2 (id > 150)
    let predicate: Arc<Predicate> =
        Arc::new(Predicate::gt(column_expr!("id"), Expr::literal(150i64)));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    let files = get_scan_files(&scan, engine.as_ref())?;
    assert_eq!(
        files.len(),
        1,
        "Should only scan file 2 with predicate id > 150, got {:?}",
        files
    );
    assert!(
        files[0].contains("part-00001"),
        "Should select second file, got {:?}",
        files
    );

    // Test 3: Predicate that should keep both files (value < 2500)
    let predicate: Arc<Predicate> =
        Arc::new(Predicate::lt(column_expr!("value"), Expr::literal(2500i64)));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    let files = get_scan_files(&scan, engine.as_ref())?;
    assert_eq!(
        files.len(),
        2,
        "Should scan both files with predicate value < 2500, got {:?}",
        files
    );

    // Test 4: Predicate that should skip all files (id > 1000)
    let predicate: Arc<Predicate> =
        Arc::new(Predicate::gt(column_expr!("id"), Expr::literal(1000i64)));
    let scan = snapshot
        .clone()
        .scan_builder()
        .with_predicate(predicate)
        .build()?;

    let files = get_scan_files(&scan, engine.as_ref())?;
    assert_eq!(
        files.len(),
        0,
        "Should skip all files with predicate id > 1000, got {:?}",
        files
    );

    Ok(())
}

fn get_scan_files(
    scan: &delta_kernel::scan::Scan,
    engine: &dyn Engine,
) -> DeltaResult<Vec<String>> {
    let scan_metadata_iter = scan.scan_metadata(engine)?;
    let mut files = Vec::new();

    fn callback(
        files: &mut Vec<String>,
        path: &str,
        _size: i64,
        _stats: Option<Stats>,
        _dv_info: DvInfo,
        _transform: Option<ExpressionRef>,
        _partition_values: HashMap<String, String>,
    ) {
        files.push(path.to_string());
    }

    for result in scan_metadata_iter {
        let metadata = result?;
        files = metadata.visit_scan_files(files, callback)?;
    }

    Ok(files)
}
