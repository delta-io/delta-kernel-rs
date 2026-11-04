//! Selects the files retained by an updated snapshot and the state baseline they support.
//!
//! [`LogUpdate::discover`] lists storage and merges the catalog tail. [`LogUpdate::from_listing`]
//! then combines those paths with the existing segment without I/O. The returned segment has
//! validated version coverage; the caller still needs to resolve its protocol, metadata, and CRC.

use crate::log_segment::LogSegment;
use crate::log_segment_files::{CheckpointHandling, LogSegmentFiles};
use crate::path::ParsedLogPath;
use crate::{KernelError, KernelResult, StorageHandler, Version};

/// A validated file selection from discovery, ready for snapshot state resolution.
#[derive(Debug)]
pub(super) struct LogUpdate {
    /// Files retained for scans and later updates; may include commits below the existing version.
    pub(super) segment: LogSegment,
    pub(super) baseline: StateBaseline,
}

/// Whether retained history lets protocol and metadata start from the existing snapshot.
/// CRC baseline selection happens later, during state resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StateBaseline {
    /// Every commit after the existing snapshot remains available for incremental replay.
    ExistingSnapshot,
    /// A checkpoint ahead of the snapshot replaces commits needed to extend the existing state.
    NewCheckpoint,
}

impl LogUpdate {
    /// Discovers retained history for `target`, or the latest version when `target` is `None`.
    ///
    /// A supplied target must exceed the existing version. Returns `None` when the existing files
    /// can be reused; otherwise the caller resolves state using the returned baseline.
    pub(super) fn discover(
        existing: &LogSegment,
        log_tail: Vec<ParsedLogPath>,
        target: Option<Version>,
        checkpoint_handling: CheckpointHandling,
        storage: &dyn StorageHandler,
    ) -> KernelResult<Option<Self>> {
        let Some(start) = listing_start(existing, checkpoint_handling) else {
            return Ok(None);
        };
        let listed = LogSegmentFiles::list_with_checkpoint_handling(
            storage,
            &existing.log_root,
            log_tail,
            Some(start),
            target,
            checkpoint_handling,
        )?;
        Self::from_listing(existing, listed, target)
    }

    /// Merges discovered paths with retained history and validates coverage without I/O.
    ///
    /// `listed` must come from listing under the chosen checkpoint policy and target bound.
    /// Its commits may have gaps that the existing segment can fill. `target`, when set, must
    /// exceed the existing version and must be reached by the merged history.
    ///
    /// Returns `None` to preserve the existing segment. An empty listing permits this only for an
    /// unbounded latest request. A checkpoint at or below the existing version can change retained
    /// files even when the version stays equal.
    pub(super) fn from_listing(
        existing: &LogSegment,
        listed: LogSegmentFiles,
        target: Option<Version>,
    ) -> KernelResult<Option<Self>> {
        if listed.ascending_commit_files().is_empty() && listed.checkpoint_parts().is_empty() {
            return match target {
                Some(_) => Err(KernelError::MissingVersion(existing.end_version + 1)),
                None => Ok(None),
            };
        }
        let checkpoint = listed.checkpoint_parts.first().map(|path| path.version);
        let discovered_end = listed
            .ascending_commit_files
            .last()
            .map(|path| path.version)
            .max(checkpoint)
            .ok_or(KernelError::EmptyLog)?;
        if discovered_end < existing.end_version {
            return Err(KernelError::invalid_log_segment(format!(
                "Unexpected state: the newest version in the log {discovered_end} is \
                 older than the existing snapshot version {}",
                existing.end_version
            )));
        }
        if checkpoint.is_some_and(|v| v > existing.end_version) {
            // Listing discarded commits through this checkpoint. The checkpoint and its later
            // commits must therefore supply complete state without the existing snapshot.
            return Ok(Some(Self {
                segment: LogSegment::try_new(listed, existing.log_root.clone(), target, None)?,
                baseline: StateBaseline::NewCheckpoint,
            }));
        }
        if discovered_end == existing.end_version && checkpoint.is_none() && target.is_none() {
            return Ok(None);
        }
        // Publication can lag behind the cached snapshot: storage may end at v1, the snapshot at
        // v2, and the catalog tail start at v3. Merge cached v2 before validating continuity.
        Ok(Some(Self {
            segment: merge_files(existing, listed, target)?,
            baseline: StateBaseline::ExistingSnapshot,
        }))
    }
}

/// Chooses the first version to list under the checkpoint policy.
///
/// `Adopt` starts after the retained checkpoint so it can find checkpoints below the snapshot.
/// With no checkpoint it starts at v1, since the snapshot already covers v0. `Ignore` needs only
/// commits after the snapshot. Returns `None` when the version to list after is `Version::MAX`.
pub(super) fn listing_start(
    existing: &LogSegment,
    checkpoint_handling: CheckpointHandling,
) -> Option<Version> {
    let after = match checkpoint_handling {
        CheckpointHandling::Adopt => existing.checkpoint_version.unwrap_or(0),
        CheckpointHandling::Ignore => existing.end_version,
    };
    after.checked_add(1)
}

/// Keeps existing paths above the adopted checkpoint and appends only newly discovered versions.
///
/// Existing paths win in the overlap, including staged commits that have since been published.
/// `max_published_version` still advances from storage discovery even when staged paths are kept.
fn merge_files(
    existing: &LogSegment,
    mut discovered: LogSegmentFiles,
    target: Option<Version>,
) -> KernelResult<LogSegment> {
    let checkpoint = discovered.checkpoint_parts.first().map(|path| path.version);
    let crc_file = resolve_crc_file(&discovered, existing);
    let mut commits = files_after_checkpoint(&existing.listed.ascending_commit_files, checkpoint);
    commits.extend(
        discovered
            .ascending_commit_files
            .into_iter()
            .filter(|path| path.version > existing.end_version),
    );
    let mut compactions =
        files_after_checkpoint(&existing.listed.ascending_compaction_files, checkpoint);
    // Compactions crossing the existing version cannot replace just the new portion of history.
    compactions.extend(
        discovered
            .ascending_compaction_files
            .into_iter()
            .filter(|path| path.version > existing.end_version),
    );
    let checkpoint_hint = if checkpoint.is_some() {
        // Discovery does not read _last_checkpoint, so the adopted checkpoint has no cached hint.
        None
    } else {
        discovered.checkpoint_parts = existing.listed.checkpoint_parts.clone();
        existing.last_checkpoint_metadata.clone()
    };
    LogSegment::try_new(
        LogSegmentFiles {
            ascending_commit_files: commits,
            ascending_compaction_files: compactions,
            checkpoint_parts: discovered.checkpoint_parts,
            latest_crc_file: crc_file,
            latest_commit_file: discovered.latest_commit_file,
            max_published_version: discovered
                .max_published_version
                .max(existing.listed.max_published_version),
        },
        existing.log_root.clone(),
        target,
        checkpoint_hint,
    )
}

/// Prefers the discovered CRC path; keeps the existing one only if no checkpoint is ahead of it.
/// This keeps only a path; state resolution reads CRC contents and compares with the in-memory CRC.
fn resolve_crc_file(discovered: &LogSegmentFiles, existing: &LogSegment) -> Option<ParsedLogPath> {
    discovered.latest_crc_file.clone().or_else(|| {
        existing.listed.latest_crc_file.clone().filter(|crc| {
            discovered
                .checkpoint_parts
                .first()
                .is_none_or(|checkpoint| crc.version >= checkpoint.version)
        })
    })
}

fn files_after_checkpoint(
    files: &[ParsedLogPath],
    checkpoint: Option<Version>,
) -> Vec<ParsedLogPath> {
    match checkpoint {
        Some(version) => files
            .iter()
            .filter(|file| file.version > version)
            .cloned()
            .collect(),
        None => files.to_vec(),
    }
}
