//! Integration tests exercising `delta_kernel::commit_range` via its public API.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use delta_kernel::arrow::array::RecordBatch;
use delta_kernel::commit_range::{CommitActions, CommitOrdering, CommitRange, DeltaAction};
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::{DeltaResult, Engine, Snapshot, Version};
use test_utils::create_default_engine;
use url::Url;

fn setup_test(rel_path: &str) -> Result<(Url, Arc<dyn Engine>), Box<dyn Error>> {
    let abs = std::fs::canonicalize(PathBuf::from(rel_path))?;
    let url =
        Url::from_directory_path(&abs).map_err(|()| format!("could not build URL from {abs:?}"))?;
    let engine = create_default_engine(&url)?;
    Ok((url, engine))
}

/// Count rows with a non-null entry in each requested action column for a single commit.
/// `actions` must match what was passed to `CommitRange::commits` so the field indices align
/// with the emitted top-level columns.
fn count_action_rows(commit: CommitActions, actions: &[DeltaAction]) -> DeltaResult<Vec<usize>> {
    let mut counts = vec![0usize; actions.len()];
    for batch_res in commit.into_actions() {
        let rb: RecordBatch = batch_res.try_into_record_batch()?;
        for (idx, count) in counts.iter_mut().enumerate() {
            let col = rb.column(idx);
            *count += col.len() - col.null_count();
        }
    }
    Ok(counts)
}

#[rstest::rstest]
#[case::ascending_default(None, 0, vec![(0, vec![1, 0]), (1, vec![1, 1])])]
#[case::descending(
    Some(CommitOrdering::DescendingOrder),
    1,
    vec![(1, vec![1, 1]), (0, vec![1, 0])],
)]
fn reads_all_commits_in_requested_order(
    #[case] ordering: Option<CommitOrdering>,
    #[case] snapshot_version: Version,
    #[case] expected_per_commit: Vec<(Version, Vec<usize>)>,
) -> Result<(), Box<dyn Error>> {
    let (url, engine) = setup_test("./tests/data/table-with-dv-small")?;

    let anchor_snapshot = Snapshot::builder_for(url.as_str())
        .at_version(snapshot_version)
        .build(engine.as_ref())?;

    let mut builder = CommitRange::builder_for(url.as_str(), 0).with_end_version(1);
    if let Some(order) = ordering {
        builder = builder.with_ordering(order);
    }
    let range = builder.build(engine.as_ref())?;

    assert_eq!(range.start_version(), 0);
    assert_eq!(range.end_version(), 1);

    let actions = [DeltaAction::Add, DeltaAction::Remove];
    let commits = range
        .commits(engine.as_ref(), anchor_snapshot, &actions)?
        .collect::<DeltaResult<Vec<_>>>()?;
    assert_eq!(commits.len(), expected_per_commit.len(), "commit count");

    for (commit, (expected_version, expected_counts)) in
        commits.into_iter().zip(expected_per_commit)
    {
        let version = commit.version();
        assert_eq!(version, expected_version, "version order");
        let counts = count_action_rows(commit, &actions)?;
        assert_eq!(counts, expected_counts, "v={version} action counts");
    }

    Ok(())
}
