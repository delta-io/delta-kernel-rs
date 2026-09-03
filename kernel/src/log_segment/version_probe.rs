use std::collections::{BTreeMap, BTreeSet};

use url::Url;

use crate::cancellation::CancellationTokenRef;
use crate::log_segment_files::{list_delta_log_from_storage, should_process_log_file};
use crate::path::{CheckpointInstance, LogPathFileType, ParsedLogPath};
use crate::{DeltaResult, StorageHandler, Version};

/// The reconstructability of all visible snapshot history after applying catalog precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotHistory {
    /// No commit, checkpoint, or known snapshot exists.
    NoHistory,
    /// At least one viable anchor reaches the latest visible state.
    Available {
        /// Earliest version in any reconstructable visible lineage.
        earliest: Version,
        /// Latest reconstructable visible version.
        latest: Version,
        /// Whether a commit or checkpoint exists at the requested target.
        target_artifact_present: bool,
    },
    /// The latest visible state has a proven gap after its last viable anchor.
    Gap {
        /// Earliest version in any reconstructable visible lineage.
        earliest: Version,
        /// Latest visible commit, checkpoint, or known snapshot version.
        latest: Version,
        /// Observed version immediately before the gap.
        start: Version,
        /// Observed version immediately after the gap.
        end: Version,
        /// Whether a commit or checkpoint exists at the requested target.
        target_artifact_present: bool,
    },
    /// Visible artifacts exist, but none belongs to a reconstructable lineage.
    Unrecreatable,
}

/// Probes the complete visible history after the normal snapshot listing could not satisfy a
/// request. A known snapshot is a trusted reconstruction anchor even if its source files are no
/// longer visible. The catalog tail has the same precedence as normal log-segment construction:
/// its first version suppresses filesystem commits from that version onward, while filesystem
/// checkpoints remain visible.
pub(crate) fn probe_snapshot_history(
    storage: &dyn StorageHandler,
    log_root: &Url,
    log_tail: &[ParsedLogPath],
    max_catalog_version: Option<Version>,
    target: Option<Version>,
    known_snapshot_version: Option<Version>,
    cancellation_token: Option<&CancellationTokenRef>,
) -> DeltaResult<SnapshotHistory> {
    let upper_bound = max_catalog_version
        .or_else(|| log_tail.last().map(|path| path.version))
        .unwrap_or(Version::MAX);
    let tail_start = log_tail.first().map(|path| path.version);
    let listing =
        list_delta_log_from_storage(storage, log_root, 0, upper_bound, cancellation_token)?;

    let mut commits = BTreeSet::new();
    let mut checkpoint_parts = BTreeMap::<(Version, CheckpointInstance), BTreeSet<u32>>::new();
    let mut saw_history_artifact = false;
    let mut target_artifact_present = false;

    for path in listing {
        let path = path?;
        saw_history_artifact |= path.is_commit() || path.is_checkpoint();
        if path.is_commit() && tail_start.is_some_and(|tail_start| path.version >= tail_start) {
            continue;
        }
        target_artifact_present |= target.is_some_and(|target| {
            target == path.version && (path.is_commit() || path.is_checkpoint())
        });
        record_path(path, &mut commits, &mut checkpoint_parts);
    }
    for path in log_tail {
        if path.version <= upper_bound && path.is_commit() {
            saw_history_artifact = true;
            target_artifact_present |= target == Some(path.version);
            commits.insert(path.version);
        }
    }

    let checkpoints = checkpoint_parts
        .into_iter()
        .filter_map(|((version, instance), parts)| {
            checkpoint_is_complete(&instance, &parts).then_some(version)
        })
        .collect::<BTreeSet<_>>();
    let mut state_versions = commits
        .union(&checkpoints)
        .copied()
        .collect::<BTreeSet<_>>();
    if let Some(known_snapshot_version) = known_snapshot_version {
        state_versions.insert(known_snapshot_version);
    }
    let Some(&latest) = state_versions.last() else {
        return Ok(if saw_history_artifact {
            SnapshotHistory::Unrecreatable
        } else {
            SnapshotHistory::NoHistory
        });
    };

    // Available-version bounds span every reconstructable lineage. A later checkpoint can start a
    // new lineage after cleaned history without making earlier versions rooted at commit 0
    // unavailable.
    let Some(earliest) = state_versions
        .iter()
        .copied()
        .find(|version| is_anchor(*version, &commits, &checkpoints, known_snapshot_version))
    else {
        return Ok(SnapshotHistory::Unrecreatable);
    };

    // No viable anchor reaches the newest state. Report the first proven hole after any viable
    // anchor; a checkpoint encountered later resets the chain and can establish a new one.
    let mut previous = None;
    let mut first_gap = None;
    for version in state_versions {
        if is_anchor(version, &commits, &checkpoints, known_snapshot_version) {
            previous = Some(version);
            first_gap = None;
            continue;
        }
        if let Some(prior) = previous {
            if prior.checked_add(1) != Some(version) {
                first_gap.get_or_insert((prior, version));
                previous = None;
            } else {
                previous = Some(version);
            }
        }
    }

    if previous == Some(latest) {
        Ok(SnapshotHistory::Available {
            earliest,
            latest,
            target_artifact_present,
        })
    } else if let Some((start, end)) = first_gap {
        Ok(SnapshotHistory::Gap {
            earliest,
            latest,
            start,
            end,
            target_artifact_present,
        })
    } else {
        Ok(SnapshotHistory::Unrecreatable)
    }
}

fn record_path(
    path: ParsedLogPath,
    commits: &mut BTreeSet<Version>,
    checkpoint_parts: &mut BTreeMap<(Version, CheckpointInstance), BTreeSet<u32>>,
) {
    match path.file_type {
        LogPathFileType::Commit | LogPathFileType::StagedCommit => {
            commits.insert(path.version);
        }
        LogPathFileType::ClassicCheckpoint | LogPathFileType::UuidCheckpoint
            if should_process_log_file(&path) =>
        {
            if let Some(instance) = CheckpointInstance::of(&path) {
                checkpoint_parts
                    .entry((path.version, instance))
                    .or_default()
                    .insert(1);
            }
        }
        LogPathFileType::MultiPartCheckpoint { part_num, .. } if should_process_log_file(&path) => {
            if let Some(instance) = CheckpointInstance::of(&path) {
                checkpoint_parts
                    .entry((path.version, instance))
                    .or_default()
                    .insert(part_num);
            }
        }
        LogPathFileType::ClassicCheckpoint
        | LogPathFileType::UuidCheckpoint
        | LogPathFileType::MultiPartCheckpoint { .. }
        | LogPathFileType::CompactedCommit { .. }
        | LogPathFileType::Crc
        | LogPathFileType::Unknown => {}
    }
}

fn checkpoint_is_complete(instance: &CheckpointInstance, parts: &BTreeSet<u32>) -> bool {
    let expected = instance.num_parts();
    parts.len() == expected
        && parts.first().copied() == Some(1)
        && parts.last().copied() == u32::try_from(expected).ok()
}

fn is_anchor(
    version: Version,
    commits: &BTreeSet<Version>,
    checkpoints: &BTreeSet<Version>,
    known_snapshot_version: Option<Version>,
) -> bool {
    (version == 0 && commits.contains(&version))
        || checkpoints.contains(&version)
        || known_snapshot_version == Some(version)
}
