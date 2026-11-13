/// This module contains all of the miscellaneous acceptance tests for delta-kernel-rs
///
/// Since each new `.rs` file in this directory results in increased build and link time, it is
/// important to only add new files if absolutely necessary for code readability or test
/// performance.
use std::path::Path;

/// Guards against mistaking local-override results for the pinned release.
#[test]
fn acceptance_workloads_is_not_a_local_override() {
    let workloads = Path::new(env!("CARGO_MANIFEST_DIR")).join("workloads");
    let meta = std::fs::symlink_metadata(&workloads).expect("workloads/ should exist after build");
    assert!(
        !meta.file_type().is_symlink(),
        "acceptance/workloads is a symlink to a LOCAL corpus, so you are not testing the pinned \
         release. Unset DELTA_ACCEPTANCE_WORKLOADS_PATH and rebuild (or remove acceptance/workloads) \
         before trusting these results."
    );
}
