use std::path::PathBuf;
use std::sync::Arc;

use super::{IncrementalListing, IncrementalScanBuilder, IncrementalScanResult};
use crate::engine::sync::SyncEngine;
use crate::Snapshot;

fn unwrap_listing(result: IncrementalScanResult) -> IncrementalListing {
    match result {
        IncrementalScanResult::Listing(l) => l,
        other => panic!("expected listing, got {other:?}"),
    }
}

#[test]
fn rejects_when_base_equals_target() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let snapshot = Snapshot::builder_for(url)
        .at_version(1)
        .build(engine.as_ref())
        .unwrap();
    let target_version = snapshot.version();

    let res = IncrementalScanBuilder::new(snapshot, target_version)
        .build(engine.as_ref(), std::iter::empty::<&str>());
    let err = match res {
        Ok(_) => panic!("expected error when base_version == target_version"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("must be less than"), "{err}");
}

// `basic_partitioned` v0 has 3 files and v1 adds 3 more. With an empty base, every Add
// in (0, 1] is "new" (no duplicates). We should see one batch of 3 surviving adds and
// no duplicate paths or removes.
#[test]
fn empty_base_classifies_all_adds_as_new() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let target = Snapshot::builder_for(url)
        .at_version(1)
        .build(engine.as_ref())
        .unwrap();

    let result = IncrementalScanBuilder::new(target, 0)
        .build(engine.as_ref(), std::iter::empty::<&str>())
        .unwrap();
    let listing = unwrap_listing(result);

    assert_eq!(listing.base_version, 0);
    assert_eq!(listing.target_version, 1);
    assert!(listing.duplicate_add_paths.is_empty());
    assert!(listing.remove_files.is_empty());
    assert_eq!(listing.add_files.len(), 1);

    let total_adds: usize = listing
        .add_files
        .iter()
        .map(|f| f.selection_vector().iter().filter(|s| **s).count())
        .sum();
    assert_eq!(total_adds, 3, "expected 3 surviving adds at v1");
}

// When the consumer's base path set already contains the paths added by commits in the
// range, those adds are classified as duplicates (metadata-only re-adds). The Add rows
// stay in `add_files`; their paths get surfaced in `duplicate_add_paths`.
#[test]
fn populated_base_classifies_matching_paths_as_duplicates() {
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let target = Snapshot::builder_for(url)
        .at_version(1)
        .build(engine.as_ref())
        .unwrap();

    // The three paths v1 adds. Pretending the base already contains these forces them to
    // classify as duplicate_add_paths.
    let base_paths = [
        "letter=__HIVE_DEFAULT_PARTITION__/part-00000-8eb7f29a-e6a1-436e-a638-bbf0a7953f09.c000.snappy.parquet",
        "letter=a/part-00000-0dbe0cc5-e3bf-4fb0-b36a-b5fdd67fe843.c000.snappy.parquet",
        "letter=e/part-00000-847cf2d1-1247-4aa0-89ef-2f90c68ea51e.c000.snappy.parquet",
    ];

    let result = IncrementalScanBuilder::new(target, 0)
        .build(engine.as_ref(), base_paths.iter().copied())
        .unwrap();
    let listing = unwrap_listing(result);

    assert_eq!(listing.duplicate_add_paths.len(), 3);
    for p in base_paths {
        assert!(
            listing.duplicate_add_paths.contains(p),
            "expected {p} to be classified as duplicate"
        );
    }
    assert!(listing.remove_files.is_empty());
    let total_adds: usize = listing
        .add_files
        .iter()
        .map(|f| f.selection_vector().iter().filter(|s| **s).count())
        .sum();
    assert_eq!(total_adds, 3, "Add rows still survive in add_files");
}

#[test]
fn invalid_range_returns_error() {
    // base_version (999) >= target_version (1) is a caller error, surfaced as Err
    // (not CommitsUnavailable, which is reserved for log-retention scenarios).
    let path = std::fs::canonicalize(PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();
    let engine = Arc::new(SyncEngine::new());

    let target = Snapshot::builder_for(url)
        .at_version(1)
        .build(engine.as_ref())
        .unwrap();

    let res =
        IncrementalScanBuilder::new(target, 999).build(engine.as_ref(), std::iter::empty::<&str>());
    assert!(res.is_err());
}
