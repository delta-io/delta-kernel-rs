use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::engine::arrow_extensions::ScanExt;
use delta_kernel::engine::sync::SyncEngine;
use delta_kernel::Table;
use itertools::Itertools;

mod common;

#[test_log::test]
fn test_scan_metadata_arrow() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let table = Table::new(url);
    let snapshot = table.snapshot(engine.as_ref(), None).unwrap();
    let scan = snapshot.into_scan_builder().build().unwrap();
    let files: Vec<_> = scan
        .scan_metadata_arrow(engine.as_ref())
        .unwrap()
        .try_collect()
        .unwrap();

    assert_eq!(files.len(), 1);
    let num_rows = files[0].scan_files.num_rows();
    assert_eq!(num_rows, 1)
}

#[test_log::test]
fn test_execute_arrow() {
    let path =
        std::fs::canonicalize(PathBuf::from("./tests/data/table-without-dv-small/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let table = Table::new(url);
    let snapshot = table.snapshot(engine.as_ref(), None).unwrap();
    let scan = snapshot.into_scan_builder().build().unwrap();
    let files: Vec<_> = scan.execute_arrow(engine).unwrap().try_collect().unwrap();

    let expected = vec![
        "+-------+",
        "| value |",
        "+-------+",
        "| 0     |",
        "| 1     |",
        "| 2     |",
        "| 3     |",
        "| 4     |",
        "| 5     |",
        "| 6     |",
        "| 7     |",
        "| 8     |",
        "| 9     |",
        "+-------+",
    ];

    assert_batches_sorted_eq!(expected, &files);
}
